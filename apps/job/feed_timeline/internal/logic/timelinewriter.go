package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"feedsystem-zero/apps/job/feed_timeline/internal/model"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/feedx"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

const (
	defaultTimelineBatchSize            = 100
	maxTimelineBatchSize                = 500
	defaultTimelineWorkerCount          = 4
	maxTimelineWorkerCount              = 16
	defaultFollowerQueryBatchSize       = 500
	maxFollowerQueryBatchSize           = 2000
	defaultGlobalTimelineMaxLen   int64 = 10000
	defaultUserTimelineMaxLen     int64 = 2000
	defaultFollowBackfillLimit          = 100
	defaultUserTimelineTTL              = 30 * 24 * time.Hour
	defaultAuthorOutboxMaxLen     int64 = 500
	defaultAuthorOutboxTTL              = 30 * 24 * time.Hour
	defaultTimelineRedisTimeout         = 3 * time.Second
	defaultTimelineDBTimeout            = 5 * time.Second
	defaultTimelineFlushInterval        = time.Second
	defaultProcessedEventTTLDays        = 14
	globalBuildLockTTL                  = 30 * time.Second
	globalBuildWait                     = 5 * time.Second
	globalBuildPollInterval             = 100 * time.Millisecond
	timelineTempWriteBatchSize          = 500
	globalBuildMaxAttempts              = 3
)

// errGlobalTimelineNotReady 表示全局 Timeline 在 Redis 中丢失（例如被 flush），
// 需要依赖 BootstrapGlobalTimeline 重建。事件处理层捕获该错误后只执行一次
// 带分布式锁的重建并重试当前事件，避免 Kafka 原地重试却永远无人恢复 ready 标记。
var errGlobalTimelineNotReady = errors.New("global timeline not ready, waiting for bootstrap")

const mutateGlobalTimelineScript = `
redis.call("INCR", KEYS[2])
if redis.call("EXISTS", KEYS[3]) == 0 then
    return 0
end
if ARGV[1] == "add" then
    redis.call("ZADD", KEYS[1], 0, ARGV[2])
    redis.call("SET", KEYS[4], ARGV[2])
else
    redis.call("ZREM", KEYS[1], ARGV[2])
    redis.call("DEL", KEYS[4])
end

local maxLen = tonumber(ARGV[3])
if maxLen and maxLen > 0 then
    redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -(maxLen + 1))
end
return 1
`

const mutateUserTimelineScript = `
redis.call("INCR", KEYS[2])
local ttl = tonumber(ARGV[3])
if ttl and ttl > 0 then
    redis.call("EXPIRE", KEYS[2], ttl)
end
if redis.call("EXISTS", KEYS[3]) == 0 then
    return 0
end

local action = ARGV[1]
for i = 4, #ARGV do
    if action == "add" then
        redis.call("ZADD", KEYS[1], 0, ARGV[i])
    else
        redis.call("ZREM", KEYS[1], ARGV[i])
    end
end

local maxLen = tonumber(ARGV[2])
if maxLen and maxLen > 0 then
    redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -(maxLen + 1))
end
if ttl and ttl > 0 then
    if redis.call("EXISTS", KEYS[1]) == 1 then
        redis.call("EXPIRE", KEYS[1], ttl)
    end
    redis.call("EXPIRE", KEYS[3], ttl)
end
return 1
`

// mutateAuthorOutboxScript 与用户 Timeline 脚本同构：
//   - 版本号无条件递增，冷启动检测到版本变化会走版本比较分支重试；
//   - outbox ZSet 只在 ready 标记存在时写入，避免部分写入产生脏数据；
//   - 每次写入都刷新 outbox / version / ready 三者的 TTL，保持一致的生命周期。
const mutateAuthorOutboxScript = `
redis.call("INCR", KEYS[2])
local ttl = tonumber(ARGV[3])
if ttl and ttl > 0 then
    redis.call("EXPIRE", KEYS[2], ttl)
end
if redis.call("EXISTS", KEYS[3]) == 0 then
    return 0
end

local action = ARGV[1]
for i = 4, #ARGV do
    if action == "add" then
        redis.call("ZADD", KEYS[1], 0, ARGV[i])
    else
        redis.call("ZREM", KEYS[1], ARGV[i])
    end
end

local maxLen = tonumber(ARGV[2])
if maxLen and maxLen > 0 then
    redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -(maxLen + 1))
end
if ttl and ttl > 0 then
    if redis.call("EXISTS", KEYS[1]) == 1 then
        redis.call("EXPIRE", KEYS[1], ttl)
    end
    redis.call("EXPIRE", KEYS[3], ttl)
end
return 1
`

const replaceGlobalTimelineScript = `
local current = redis.call("GET", KEYS[1])
if not current then
    current = "0"
end
if current ~= ARGV[1] then
    redis.call("DEL", KEYS[4])
    return 0
end

redis.call("DEL", KEYS[2])
if redis.call("EXISTS", KEYS[4]) == 1 then
    redis.call("RENAME", KEYS[4], KEYS[2])
end
redis.call("INCR", KEYS[1])
redis.call("SET", KEYS[3], "1")
return 1
`

const releaseGlobalBuildLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
`

// BootstrapGlobalTimeline 在 consumer 拉取 Kafka 前构建全局最新流。
// Redis 已有 ready 标记时直接复用；冷启动时使用锁、版本比较和临时 ZSet 原子替换。
func (c *TimelineConsumer) BootstrapGlobalTimeline(ctx context.Context) error {
	ready, err := c.globalTimelineReady(ctx)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	token, err := randomTimelineToken()
	if err != nil {
		return err
	}
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	locked, err := c.svcCtx.RedisCli.SetNX(
		redisCtx,
		rediskey.FeedGlobalTimelineBuildLockKey(),
		token,
		globalBuildLockTTL,
	).Result()
	cancel()
	if err != nil {
		return fmt.Errorf("acquire global timeline build lock failed: %w", err)
	}
	if !locked {
		return c.waitGlobalTimelineReady(ctx)
	}
	defer c.releaseGlobalBuildLock(ctx, token)

	for attempt := 1; attempt <= globalBuildMaxAttempts; attempt++ {
		version, err := c.globalTimelineVersion(ctx)
		if err != nil {
			return err
		}
		videos, err := c.loadLatestGlobalVideos(ctx)
		if err != nil {
			return err
		}
		members, err := membersFromVideos(videos)
		if err != nil {
			return err
		}

		tempKey := rediskey.FeedGlobalTimelineTempKey(token + ":" + strconv.Itoa(attempt))
		if err := c.writeTempTimeline(ctx, tempKey, members); err != nil {
			return err
		}
		applied, err := c.replaceGlobalTimeline(ctx, version, tempKey)
		if err != nil {
			return err
		}
		if applied {
			logx.WithContext(ctx).Infof("global feed timeline bootstrapped, videos:%d", len(members))
			return nil
		}
	}
	return errors.New("global timeline changed continuously during bootstrap")
}

func (c *TimelineConsumer) applyVideoEvent(ctx context.Context, event eventx.FeedVideoEvent) error {
	video, err := c.loadVideoFinalState(ctx, event.VideoID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 正常业务使用软删除；若数据被人工物理删除，则用发布时保存的 member 映射兜底清理。
			member, loadErr := c.loadStoredVideoMember(ctx, event.VideoID)
			if errors.Is(loadErr, redis.Nil) {
				return nil
			}
			if loadErr != nil {
				return loadErr
			}
			if err := c.mutateGlobalTimeline(ctx, "remove", event.VideoID, member); err != nil {
				return err
			}
			return c.dispatchAuthorTimeline(ctx, event.AuthorID, "remove", member)
		}
		return err
	}

	member, err := feedx.EncodeTimelineMember(video.CreatedAt.UnixMilli(), video.ID)
	if err != nil {
		return err
	}
	action := "remove"
	if video.Status == model.VideoStatusNormal && video.DeletedAt == nil {
		action = "add"
	}

	// 全局 Timeline 始终维护；用户 Timeline / 大 V outbox 根据 ready 标记按需写入。
	if err := c.mutateGlobalTimeline(ctx, action, video.ID, member); err != nil {
		return err
	}
	return c.dispatchAuthorTimeline(ctx, video.AuthorID, action, member)
}

// dispatchAuthorTimeline 根据作者是否为大 V 决定推 / 拉：
//   - 大 V：只写入作者自己的 outbox（1 次 Redis 写），跳过对海量粉丝的 fanout；
//   - 小 V：走原有 fanoutVideoToFollowers 流程，把 member 推送到每个粉丝的 inbox。
//
// 是否为大 V 由 accounts.is_big_v 只升不降标记位决定（social 模块在关注事务内维护），
// 一次主键查询即可完成判定，避免直接比较 follower_count 引发的阈值反向穿越。
// 如果 accounts 记录暂时缺失（例如账号被删除），保守按小 V 处理避免历史视频消失。
func (c *TimelineConsumer) dispatchAuthorTimeline(ctx context.Context, authorID uint64, action, member string) error {
	if authorID == 0 {
		return nil
	}
	isBigV, err := c.loadAuthorBigVFlag(ctx, authorID)
	if err != nil {
		return err
	}
	if feedx.IsBigCreator(isBigV) {
		return c.mutateAuthorOutbox(ctx, authorID, action, []string{member})
	}
	return c.fanoutVideoToFollowers(ctx, authorID, action, member)
}

// loadAuthorBigVFlag 读取作者的大 V 标记位用于推拉分离判定。
// 记录不存在时返回 false，让上游按小 V 处理避免误将新账号视频吞掉。
// 该字段只升不降，一旦为 true 就永久有效，掉粉不会回退，保证已入 outbox 的历史视频可被读侧 union。
func (c *TimelineConsumer) loadAuthorBigVFlag(ctx context.Context, authorID uint64) (bool, error) {
	var account model.Account
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	err := c.svcCtx.GormDB.WithContext(dbCtx).
		Select("id", "is_big_v").
		Where("id = ?", authorID).
		First(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load author big_v flag failed, author_id:%d: %w", authorID, err)
	}
	return account.IsBigV, nil
}

func (c *TimelineConsumer) applyFollowEvent(ctx context.Context, event eventx.FollowEvent) error {
	followed, err := c.loadFollowFinalState(ctx, event.FollowerID, event.FollowingID)
	if err != nil {
		return err
	}

	// 大 V 无需 inbox 回填 / 清理：读侧会自动 union / 剔除该作者的 outbox。
	// is_big_v 只升不降，即使该作者后续掉粉低于阈值，已经入 outbox 的历史视频仍可被读到，
	// 不会像直接比较 follower_count 那样出现阈值反向穿越导致视频消失。
	isBigV, err := c.loadAuthorBigVFlag(ctx, event.FollowingID)
	if err != nil {
		return err
	}
	if feedx.IsBigCreator(isBigV) {
		return nil
	}

	action := "remove"
	limit := int(c.userTimelineMaxLen())
	if followed {
		action = "add"
		limit = c.followBackfillLimit()
	}
	videos, err := c.loadAuthorVideos(ctx, event.FollowingID, followed, limit)
	if err != nil {
		return err
	}
	members, err := membersFromVideos(videos)
	if err != nil {
		return err
	}

	// 即使 Timeline 尚未构建也会先递增版本；并发冷启动检测到版本变化后会重新查 MySQL。
	return c.mutateUserTimeline(ctx, event.FollowerID, action, members)
}

func (c *TimelineConsumer) loadVideoFinalState(ctx context.Context, videoID uint64) (model.Video, error) {
	var video model.Video
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	err := c.svcCtx.GormDB.WithContext(dbCtx).
		Select("id", "author_id", "status", "deleted_at", "created_at").
		Where("id = ?", videoID).
		First(&video).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.Video{}, fmt.Errorf("video row missing for feed event, video_id:%d: %w", videoID, gorm.ErrRecordNotFound)
	}
	if err != nil {
		return model.Video{}, fmt.Errorf("load video final state failed, video_id:%d: %w", videoID, err)
	}
	return video, nil
}

func (c *TimelineConsumer) loadStoredVideoMember(ctx context.Context, videoID uint64) (string, error) {
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	defer cancel()
	return c.svcCtx.RedisCli.Get(redisCtx, rediskey.FeedVideoTimelineMemberKey(videoID)).Result()
}

func (c *TimelineConsumer) loadFollowFinalState(ctx context.Context, followerID, followingID uint64) (bool, error) {
	var follow model.Follow
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	err := c.svcCtx.GormDB.WithContext(dbCtx).
		Select("id", "follower_id", "following_id", "status", "updated_at", "deleted_at").
		Where("follower_id = ? AND following_id = ?", followerID, followingID).
		First(&follow).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load follow final state failed, follower_id:%d following_id:%d: %w", followerID, followingID, err)
	}
	return follow.Status == model.FollowStatusActive && follow.DeletedAt == nil, nil
}

func (c *TimelineConsumer) loadLatestGlobalVideos(ctx context.Context) ([]model.Video, error) {
	var videos []model.Video
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	err := c.svcCtx.GormDB.WithContext(dbCtx).
		Select("id", "author_id", "status", "deleted_at", "created_at").
		Where("status = ? AND deleted_at IS NULL", model.VideoStatusNormal).
		Order("created_at DESC, id DESC").
		Limit(int(c.globalTimelineMaxLen())).
		Find(&videos).Error
	return videos, err
}

func (c *TimelineConsumer) loadAuthorVideos(ctx context.Context, authorID uint64, normalOnly bool, limit int) ([]model.Video, error) {
	if limit <= 0 {
		return nil, nil
	}
	var videos []model.Video
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	query := c.svcCtx.GormDB.WithContext(dbCtx).
		Select("id", "author_id", "status", "deleted_at", "created_at").
		Where("author_id = ?", authorID)
	if normalOnly {
		query = query.Where("status = ? AND deleted_at IS NULL", model.VideoStatusNormal)
	}
	err := query.Order("created_at DESC, id DESC").Limit(limit).Find(&videos).Error
	return videos, err
}

func (c *TimelineConsumer) fanoutVideoToFollowers(ctx context.Context, authorID uint64, action, member string) error {
	batchSize := c.followerQueryBatchSize()
	var cursorID uint64
	for {
		var follows []model.Follow
		dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
		err := c.svcCtx.GormDB.WithContext(dbCtx).
			Select("id", "follower_id", "following_id", "status", "updated_at", "deleted_at").
			Where("following_id = ? AND status = ? AND deleted_at IS NULL AND id > ?", authorID, model.FollowStatusActive, cursorID).
			Order("id ASC").
			Limit(batchSize).
			Find(&follows).Error
		cancel()
		if err != nil {
			return fmt.Errorf("query video followers failed, author_id:%d: %w", authorID, err)
		}
		if len(follows) == 0 {
			return nil
		}

		userIDs := make([]uint64, 0, len(follows))
		for _, follow := range follows {
			userIDs = append(userIDs, follow.FollowerID)
			cursorID = follow.ID
		}
		if err := c.mutateUserTimelines(ctx, userIDs, action, []string{member}); err != nil {
			return err
		}
		if len(follows) < batchSize {
			return nil
		}
	}
}

func (c *TimelineConsumer) mutateGlobalTimeline(ctx context.Context, action string, videoID uint64, member string) error {
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	result, err := c.svcCtx.RedisCli.Eval(
		redisCtx,
		mutateGlobalTimelineScript,
		[]string{
			rediskey.FeedGlobalTimelineKey(),
			rediskey.FeedGlobalTimelineVersionKey(),
			rediskey.FeedGlobalTimelineReadyKey(),
			rediskey.FeedVideoTimelineMemberKey(videoID),
		},
		action,
		member,
		strconv.FormatInt(c.globalTimelineMaxLen(), 10),
	).Int64()
	cancel()
	if err != nil {
		return err
	}
	if result == 0 {
		// 全局 Timeline Ready 键不存在（可能被运维/故障 flush）。
		// 不能用单条事件伪造完整全局流，也不能在事件处理路径同步递归 bootstrap
		// （会阻塞当前批次、并可能与其它调用方并发 bootstrap）。
		// 记录 warning 后返回 sentinel error 让 Kafka 重投，由下一轮 poll 之前的
		// bootstrap 流程（Job 启动或后续心跳）负责重建。
		logx.WithContext(ctx).Errorf(
			"global timeline ready missing, waiting for bootstrap, action:%s video_id:%d",
			action, videoID,
		)
		return errGlobalTimelineNotReady
	}
	return nil
}

func (c *TimelineConsumer) mutateUserTimelines(ctx context.Context, userIDs []uint64, action string, members []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	defer cancel()
	pipe := c.svcCtx.RedisCli.Pipeline()
	for _, userID := range userIDs {
		args := c.userTimelineMutationArgs(action, members)
		pipe.Eval(
			redisCtx,
			mutateUserTimelineScript,
			[]string{
				rediskey.FeedTimelineKey(userID),
				rediskey.FeedTimelineVersionKey(userID),
				rediskey.FeedTimelineReadyKey(userID),
			},
			args...,
		)
	}
	_, err := pipe.Exec(redisCtx)
	return err
}

func (c *TimelineConsumer) mutateUserTimeline(ctx context.Context, userID uint64, action string, members []string) error {
	return c.mutateUserTimelines(ctx, []uint64{userID}, action, members)
}

// mutateAuthorOutbox 对大 V 的 outbox 进行 add / remove。
// ready 标记不存在时视为 outbox 尚未冷启动，只递增版本号并刷新 TTL；
// 版本变化会让 rpc 侧的懒加载在冷启动完成前重试，最终读到最新数据。
func (c *TimelineConsumer) mutateAuthorOutbox(ctx context.Context, authorID uint64, action string, members []string) error {
	if authorID == 0 || len(members) == 0 {
		return nil
	}
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	defer cancel()

	args := make([]any, 0, len(members)+3)
	args = append(args,
		action,
		strconv.FormatInt(c.authorOutboxMaxLen(), 10),
		strconv.FormatInt(int64(c.authorOutboxTTL()/time.Second), 10),
	)
	for _, member := range members {
		args = append(args, member)
	}
	_, err := c.svcCtx.RedisCli.Eval(
		redisCtx,
		mutateAuthorOutboxScript,
		[]string{
			rediskey.FeedAuthorOutboxKey(authorID),
			rediskey.FeedAuthorOutboxVersionKey(authorID),
			rediskey.FeedAuthorOutboxReadyKey(authorID),
		},
		args...,
	).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("mutate author outbox failed, author_id:%d: %w", authorID, err)
	}
	return nil
}

func (c *TimelineConsumer) userTimelineMutationArgs(action string, members []string) []any {
	args := make([]any, 0, len(members)+3)
	args = append(args,
		action,
		strconv.FormatInt(c.userTimelineMaxLen(), 10),
		strconv.FormatInt(int64(c.userTimelineTTL()/time.Second), 10),
	)
	for _, member := range members {
		args = append(args, member)
	}
	return args
}

func (c *TimelineConsumer) globalTimelineReady(ctx context.Context) (bool, error) {
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	defer cancel()
	count, err := c.svcCtx.RedisCli.Exists(redisCtx, rediskey.FeedGlobalTimelineReadyKey()).Result()
	return count > 0, err
}

func (c *TimelineConsumer) waitGlobalTimelineReady(ctx context.Context) error {
	deadline := time.NewTimer(globalBuildWait)
	defer deadline.Stop()
	ticker := time.NewTicker(globalBuildPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("wait global timeline bootstrap timeout")
		case <-ticker.C:
			ready, err := c.globalTimelineReady(ctx)
			if err != nil {
				return err
			}
			if ready {
				return nil
			}
		}
	}
}

func (c *TimelineConsumer) releaseGlobalBuildLock(ctx context.Context, token string) {
	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.redisTimeout())
	defer cancel()
	_ = c.svcCtx.RedisCli.Eval(
		redisCtx,
		releaseGlobalBuildLockScript,
		[]string{rediskey.FeedGlobalTimelineBuildLockKey()},
		token,
	).Err()
}

func (c *TimelineConsumer) globalTimelineVersion(ctx context.Context) (int64, error) {
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	defer cancel()
	version, err := c.svcCtx.RedisCli.Get(redisCtx, rediskey.FeedGlobalTimelineVersionKey()).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return version, err
}

func (c *TimelineConsumer) writeTempTimeline(ctx context.Context, key string, members []string) error {
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	defer cancel()
	if err := c.svcCtx.RedisCli.Del(redisCtx, key).Err(); err != nil {
		return err
	}
	for start := 0; start < len(members); start += timelineTempWriteBatchSize {
		end := min(start+timelineTempWriteBatchSize, len(members))
		values := make([]redis.Z, 0, end-start)
		for _, member := range members[start:end] {
			values = append(values, redis.Z{Score: 0, Member: member})
		}
		if err := c.svcCtx.RedisCli.ZAdd(redisCtx, key, values...).Err(); err != nil {
			return err
		}
	}
	if len(members) > 0 {
		return c.svcCtx.RedisCli.Expire(redisCtx, key, globalBuildLockTTL).Err()
	}
	return nil
}

func (c *TimelineConsumer) replaceGlobalTimeline(ctx context.Context, expectedVersion int64, tempKey string) (bool, error) {
	redisCtx, cancel := context.WithTimeout(ctx, c.redisTimeout())
	defer cancel()
	result, err := c.svcCtx.RedisCli.Eval(
		redisCtx,
		replaceGlobalTimelineScript,
		[]string{
			rediskey.FeedGlobalTimelineVersionKey(),
			rediskey.FeedGlobalTimelineKey(),
			rediskey.FeedGlobalTimelineReadyKey(),
			tempKey,
		},
		strconv.FormatInt(expectedVersion, 10),
	).Int64()
	return result == 1, err
}

func membersFromVideos(videos []model.Video) ([]string, error) {
	members := make([]string, 0, len(videos))
	for _, video := range videos {
		member, err := feedx.EncodeTimelineMember(video.CreatedAt.UnixMilli(), video.ID)
		if err != nil {
			return nil, fmt.Errorf("encode timeline member failed, video_id:%d: %w", video.ID, err)
		}
		members = append(members, member)
	}
	return members, nil
}

func randomTimelineToken() (string, error) {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func (c *TimelineConsumer) timelineBatchSize() int {
	size := c.svcCtx.Config.Timeline.BatchSize
	if size <= 0 {
		size = c.svcCtx.Config.Kafka.BatchSize
	}
	if size <= 0 {
		return defaultTimelineBatchSize
	}
	return min(size, maxTimelineBatchSize)
}

func (c *TimelineConsumer) timelineFlushInterval() time.Duration {
	milliseconds := c.svcCtx.Config.Timeline.FlushMs
	if milliseconds <= 0 {
		return defaultTimelineFlushInterval
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (c *TimelineConsumer) workerCount(groupCount int) int {
	if groupCount <= 0 {
		return 0
	}
	count := c.svcCtx.Config.Timeline.WorkerCount
	if count <= 0 {
		count = defaultTimelineWorkerCount
	}
	count = min(count, maxTimelineWorkerCount)
	return min(count, groupCount)
}

func (c *TimelineConsumer) followerQueryBatchSize() int {
	size := c.svcCtx.Config.Timeline.FollowerQueryBatchSize
	if size <= 0 {
		return defaultFollowerQueryBatchSize
	}
	return min(size, maxFollowerQueryBatchSize)
}

func (c *TimelineConsumer) globalTimelineMaxLen() int64 {
	value := c.svcCtx.Config.Timeline.GlobalTimelineMaxLen
	if value <= 0 {
		return defaultGlobalTimelineMaxLen
	}
	return value
}

func (c *TimelineConsumer) userTimelineMaxLen() int64 {
	value := c.svcCtx.Config.Timeline.UserTimelineMaxLen
	if value <= 0 {
		return defaultUserTimelineMaxLen
	}
	return value
}

func (c *TimelineConsumer) userTimelineTTL() time.Duration {
	seconds := c.svcCtx.Config.Timeline.UserTimelineTTLSeconds
	if seconds <= 0 {
		return defaultUserTimelineTTL
	}
	return time.Duration(seconds) * time.Second
}

func (c *TimelineConsumer) followBackfillLimit() int {
	value := c.svcCtx.Config.Timeline.FollowBackfillVideoLimit
	if value <= 0 {
		return defaultFollowBackfillLimit
	}
	return min(value, int(c.userTimelineMaxLen()))
}

// authorOutboxMaxLen 单个大 V outbox 的最大保留条数。
// 配置未提供时使用默认值；读侧对多份 outbox 做 union，因此长度不宜过大。
func (c *TimelineConsumer) authorOutboxMaxLen() int64 {
	value := c.svcCtx.Config.Timeline.AuthorOutboxMaxLen
	if value <= 0 {
		return defaultAuthorOutboxMaxLen
	}
	return value
}

// authorOutboxTTL 大 V outbox 的有效期。
// 有事件到达或读侧访问都会自动续期；长期无人访问的大 V 会随 TTL 淘汰。
func (c *TimelineConsumer) authorOutboxTTL() time.Duration {
	seconds := c.svcCtx.Config.Timeline.AuthorOutboxTTLSeconds
	if seconds <= 0 {
		return defaultAuthorOutboxTTL
	}
	return time.Duration(seconds) * time.Second
}

func (c *TimelineConsumer) redisTimeout() time.Duration {
	milliseconds := c.svcCtx.Config.Timeline.RedisOpTimeoutMs
	if milliseconds <= 0 {
		return defaultTimelineRedisTimeout
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (c *TimelineConsumer) dbTimeout() time.Duration {
	milliseconds := c.svcCtx.Config.Timeline.DBQueryTimeoutMs
	if milliseconds <= 0 {
		return defaultTimelineDBTimeout
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (c *TimelineConsumer) processedEventTTLDays() int {
	days := c.svcCtx.Config.Timeline.ProcessedEventTTLDays
	if days <= 0 {
		return defaultProcessedEventTTLDays
	}
	return days
}
