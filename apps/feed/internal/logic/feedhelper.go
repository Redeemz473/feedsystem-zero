package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"feedsystem-zero/apps/feed/feed"
	"feedsystem-zero/apps/feed/internal/model"
	"feedsystem-zero/apps/feed/internal/svc"
	"feedsystem-zero/common/feedx"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultFeedPageSize        int64 = 20
	maxFeedPageSize            int64 = 50
	defaultGlobalTimelineLimit int64 = 10000
	defaultUserTimelineLimit   int64 = 2000
	defaultUserTimelineTTL           = 30 * 24 * time.Hour
	defaultBuildLockTTL              = 15 * time.Second
	defaultBuildWait                 = 1500 * time.Millisecond
	defaultFeedRedisTimeout          = time.Second
	defaultFeedDBTimeout             = 3 * time.Second
	timelineBuildMaxAttempts         = 3
	timelineTempWriteBatchSize       = 500
	timelineReadBatchSize      int64 = 64
	buildWaitPollInterval            = 50 * time.Millisecond
)

const replaceTimelineIfVersionMatchScript = `
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

local ttl = tonumber(ARGV[2])
if ttl and ttl > 0 then
    if redis.call("EXISTS", KEYS[2]) == 1 then
        redis.call("EXPIRE", KEYS[2], ttl)
    end
    redis.call("EXPIRE", KEYS[3], ttl)
    redis.call("EXPIRE", KEYS[1], ttl)
end
return 1
`

const releaseTimelineBuildLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
`

var timelineBuildGroup = syncx.NewSingleFlight()

type timelinePage struct {
	Items   []*feed.FeedVideoItem
	HasMore bool
}

// normalizeFeedPageSize 统一两个 Feed 接口的页大小语义。
func normalizeFeedPageSize(svcCtx *svc.ServiceContext, pageSize int64) int64 {
	defaultSize := svcCtx.Config.Timeline.DefaultPageSize
	if defaultSize <= 0 {
		defaultSize = defaultFeedPageSize
	}
	maxSize := svcCtx.Config.Timeline.MaxPageSize
	if maxSize <= 0 {
		maxSize = maxFeedPageSize
	}
	if defaultSize > maxSize {
		defaultSize = maxSize
	}
	if pageSize <= 0 {
		return defaultSize
	}
	if pageSize > maxSize {
		return maxSize
	}
	return pageSize
}

// validateFeedCursor 要求复合游标成对出现，并拒绝未来时间戳等明显非法输入。
func validateFeedCursor(cursorPublishedAt int64, cursorVideoID uint64) error {
	if cursorPublishedAt == 0 && cursorVideoID == 0 {
		return nil
	}
	if cursorPublishedAt <= 0 || cursorVideoID == 0 {
		return status.Error(codes.InvalidArgument, "发布时间游标和视频ID游标必须同时提供")
	}
	// 允许最多五分钟时钟偏差，避免超大游标制造无意义 Redis 查询。
	if cursorPublishedAt > time.Now().Add(5*time.Minute).UnixMilli() {
		return status.Error(codes.InvalidArgument, "发布时间游标不合法")
	}
	return nil
}

// loadTimelinePage 按复合 member 的字典序倒序分页。
// 遇到损坏 member 会清理并继续向后读取，保证单个脏元素不会让整页失败。
func loadTimelinePage(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	timelineKey string,
	cursorPublishedAt int64,
	cursorVideoID uint64,
	pageSize int64,
) (timelinePage, error) {
	lexMax, err := feedx.TimelineLexMax(cursorPublishedAt, cursorVideoID)
	if err != nil {
		return timelinePage{}, status.Error(codes.InvalidArgument, err.Error())
	}

	need := pageSize + 1
	items := make([]*feed.FeedVideoItem, 0, need)
	invalidMembers := make([]any, 0)
	currentMax := lexMax

	redisCtx, cancel := context.WithTimeout(ctx, feedRedisTimeout(svcCtx))
	defer cancel()

	for int64(len(items)) < need {
		count := timelineReadBatchSize
		remaining := need - int64(len(items))
		if remaining > count {
			count = remaining
		}
		members, err := svcCtx.RedisCli.ZRevRangeByLex(redisCtx, timelineKey, &redis.ZRangeBy{
			Max:    currentMax,
			Min:    "-",
			Offset: 0,
			Count:  count,
		}).Result()
		if err != nil {
			return timelinePage{}, fmt.Errorf("读取Feed Timeline失败: %w", err)
		}
		if len(members) == 0 {
			break
		}

		// 就地更新 currentMax 到"当前处理到的这个 member"，避免内层 for 因 need 满而提前 break 时
		// 用整批最后一个 member 作为下一轮起点，进而跳过一段未消费的 member。
		lastMember := members[len(members)-1]
		for _, member := range members {
			lastMember = member
			publishedAt, videoID, decodeErr := feedx.DecodeTimelineMember(member)
			if decodeErr != nil {
				invalidMembers = append(invalidMembers, member)
				continue
			}
			items = append(items, &feed.FeedVideoItem{
				VideoId:     videoID,
				PublishedAt: publishedAt,
			})
			if int64(len(items)) >= need {
				break
			}
		}
		currentMax = "(" + lastMember
		if int64(len(members)) < count {
			break
		}
	}

	if len(invalidMembers) > 0 {
		// 清理失败不影响本次读取结果；后续 Job 重建仍能最终修复。
		_ = svcCtx.RedisCli.ZRem(redisCtx, timelineKey, invalidMembers...).Err()
	}

	hasMore := int64(len(items)) > pageSize
	if hasMore {
		items = items[:pageSize]
	}
	return timelinePage{Items: items, HasMore: hasMore}, nil
}

// ensureGlobalTimeline 只等待 Job 完成全局 Timeline 的 bootstrap，不再抢锁自建。
// 全局 Timeline 的单点建设由 apps/job/feed_timeline 负责，避免 rpc 与 job 争抢同一把
// 构建锁导致互相等待失败。
func ensureGlobalTimeline(ctx context.Context, svcCtx *svc.ServiceContext) error {
	return ensureTimeline(
		ctx,
		svcCtx,
		"global",
		rediskey.FeedGlobalTimelineReadyKey(),
		rediskey.FeedGlobalTimelineBuildLockKey(),
		rediskey.FeedGlobalTimelineVersionKey(),
		rediskey.FeedGlobalTimelineKey(),
		func(token string) string { return rediskey.FeedGlobalTimelineTempKey(token) },
		0,
		func(loadCtx context.Context) ([]string, error) {
			return loadGlobalTimelineMembers(loadCtx, svcCtx)
		},
		true,
	)
}

// ensureFollowingTimeline 在用户 Timeline 未初始化或已过期时，从 MySQL 关注关系构建完整快照。
func ensureFollowingTimeline(ctx context.Context, svcCtx *svc.ServiceContext, userID uint64) error {
	if userID == 0 {
		return status.Error(codes.Unauthenticated, "未登录")
	}
	return ensureTimeline(
		ctx,
		svcCtx,
		"user:"+strconv.FormatUint(userID, 10),
		rediskey.FeedTimelineReadyKey(userID),
		rediskey.FeedTimelineBuildLockKey(userID),
		rediskey.FeedTimelineVersionKey(userID),
		rediskey.FeedTimelineKey(userID),
		func(token string) string { return rediskey.FeedTimelineTempKey(userID, token) },
		feedUserTimelineTTL(svcCtx),
		func(loadCtx context.Context) ([]string, error) {
			return loadFollowingTimelineMembers(loadCtx, svcCtx, userID)
		},
		false,
	)
}

// refreshFollowingTimelineTTL 只刷新活跃用户 Timeline，不影响全局 Timeline。
func refreshFollowingTimelineTTL(ctx context.Context, svcCtx *svc.ServiceContext, userID uint64) {
	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), feedRedisTimeout(svcCtx))
	defer cancel()
	ttl := feedUserTimelineTTL(svcCtx)
	pipe := svcCtx.RedisCli.Pipeline()
	pipe.Expire(redisCtx, rediskey.FeedTimelineKey(userID), ttl)
	pipe.Expire(redisCtx, rediskey.FeedTimelineReadyKey(userID), ttl)
	pipe.Expire(redisCtx, rediskey.FeedTimelineVersionKey(userID), ttl)
	if _, err := pipe.Exec(redisCtx); err != nil && !errors.Is(err, redis.Nil) {
		// TTL 刷新属于保活优化，不应让已经读出的 Feed 失败。
		logx.WithContext(ctx).Errorf("refresh following timeline ttl failed, user_id:%d error:%v", userID, err)
		return
	}
}

func ensureTimeline(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	buildGroupKey string,
	readyKey string,
	lockKey string,
	versionKey string,
	timelineKey string,
	tempKey func(token string) string,
	ttl time.Duration,
	loadMembers func(context.Context) ([]string, error),
	passiveOnly bool,
) error {
	ready, err := timelineReady(ctx, svcCtx, readyKey)
	if err != nil {
		logx.WithContext(ctx).Errorf("check timeline ready failed, build_group:%s error:%v", buildGroupKey, err)
		return status.Error(codes.Unavailable, "Feed缓存暂时不可用")
	}
	if ready {
		return nil
	}

	// passiveOnly=true 表示当前 Timeline 由外部（Job）单点负责冷启动，rpc 侧只做等待。
	// 这样避免 rpc 与 job 争抢同一把 build lock 导致相互阻塞。
	if passiveOnly {
		if err := waitTimelineReady(ctx, svcCtx, readyKey); err != nil {
			logx.WithContext(ctx).Errorf("wait timeline ready failed, build_group:%s error:%v", buildGroupKey, err)
			return status.Error(codes.Unavailable, "Feed缓存正在初始化，请稍后重试")
		}
		return nil
	}

	_, err = timelineBuildGroup.Do(buildGroupKey, func() (any, error) {
		ready, err := timelineReady(ctx, svcCtx, readyKey)
		if err != nil || ready {
			return nil, err
		}

		lockToken, locked, err := acquireTimelineBuildLock(ctx, svcCtx, lockKey)
		if err != nil {
			return nil, err
		}
		if !locked {
			return nil, waitTimelineReady(ctx, svcCtx, readyKey)
		}
		defer releaseTimelineBuildLock(ctx, svcCtx, lockKey, lockToken)

		for attempt := 0; attempt < timelineBuildMaxAttempts; attempt++ {
			version, err := loadTimelineVersion(ctx, svcCtx, versionKey)
			if err != nil {
				return nil, err
			}

			dbCtx, cancel := context.WithTimeout(ctx, feedDBTimeout(svcCtx))
			members, err := loadMembers(dbCtx)
			cancel()
			if err != nil {
				return nil, err
			}

			token, err := randomTimelineToken()
			if err != nil {
				return nil, err
			}
			currentTempKey := tempKey(token)
			if err := writeTimelineTemp(ctx, svcCtx, currentTempKey, members); err != nil {
				return nil, err
			}

			applied, err := replaceTimelineIfVersionMatch(
				ctx, svcCtx, versionKey, timelineKey, readyKey, currentTempKey, version, ttl,
			)
			if err != nil {
				return nil, err
			}
			if applied {
				return nil, nil
			}
		}
		return nil, errors.New("Timeline构建期间持续发生变化，请稍后重试")
	})
	if err != nil {
		logx.WithContext(ctx).Errorf("ensure timeline failed, build_group:%s error:%v", buildGroupKey, err)
		return status.Error(codes.Unavailable, "Feed初始化失败，请稍后重试")
	}
	return nil
}

func loadGlobalTimelineMembers(ctx context.Context, svcCtx *svc.ServiceContext) ([]string, error) {
	limit := svcCtx.Config.Timeline.GlobalTimelineMaxLen
	if limit <= 0 {
		limit = defaultGlobalTimelineLimit
	}
	var videos []model.Video
	if err := svcCtx.GormDB.WithContext(ctx).
		Select("id", "author_id", "status", "deleted_at", "created_at").
		Where("status = ? AND deleted_at IS NULL", model.VideoStatusNormal).
		Order("created_at DESC, id DESC").
		Limit(int(limit)).
		Find(&videos).Error; err != nil {
		return nil, fmt.Errorf("查询全局Feed快照失败: %w", err)
	}
	return timelineMembersFromVideos(videos)
}

func loadFollowingTimelineMembers(ctx context.Context, svcCtx *svc.ServiceContext, userID uint64) ([]string, error) {
	limit := svcCtx.Config.Timeline.UserTimelineMaxLen
	if limit <= 0 {
		limit = defaultUserTimelineLimit
	}
	var videos []model.Video
	if err := svcCtx.GormDB.WithContext(ctx).
		Table("videos AS v").
		Select("v.id", "v.author_id", "v.status", "v.deleted_at", "v.created_at").
		Joins("JOIN follows AS f ON f.following_id = v.author_id").
		Where("f.follower_id = ? AND f.status = ? AND f.deleted_at IS NULL", userID, model.FollowStatusActive).
		Where("v.status = ? AND v.deleted_at IS NULL", model.VideoStatusNormal).
		Order("v.created_at DESC, v.id DESC").
		Limit(int(limit)).
		Find(&videos).Error; err != nil {
		return nil, fmt.Errorf("查询关注流快照失败: %w", err)
	}
	return timelineMembersFromVideos(videos)
}

func timelineMembersFromVideos(videos []model.Video) ([]string, error) {
	members := make([]string, 0, len(videos))
	for _, video := range videos {
		member, err := feedx.EncodeTimelineMember(video.CreatedAt.UnixMilli(), video.ID)
		if err != nil {
			return nil, fmt.Errorf("构造视频Timeline member失败, video_id:%d: %w", video.ID, err)
		}
		members = append(members, member)
	}
	return members, nil
}

func timelineReady(ctx context.Context, svcCtx *svc.ServiceContext, readyKey string) (bool, error) {
	redisCtx, cancel := context.WithTimeout(ctx, feedRedisTimeout(svcCtx))
	defer cancel()
	count, err := svcCtx.RedisCli.Exists(redisCtx, readyKey).Result()
	return count > 0, err
}

func acquireTimelineBuildLock(ctx context.Context, svcCtx *svc.ServiceContext, lockKey string) (string, bool, error) {
	token, err := randomTimelineToken()
	if err != nil {
		return "", false, err
	}
	redisCtx, cancel := context.WithTimeout(ctx, feedRedisTimeout(svcCtx))
	defer cancel()
	locked, err := svcCtx.RedisCli.SetNX(redisCtx, lockKey, token, feedBuildLockTTL(svcCtx)).Result()
	return token, locked, err
}

func releaseTimelineBuildLock(ctx context.Context, svcCtx *svc.ServiceContext, lockKey, token string) {
	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), feedRedisTimeout(svcCtx))
	defer cancel()
	_ = svcCtx.RedisCli.Eval(redisCtx, releaseTimelineBuildLockScript, []string{lockKey}, token).Err()
}

func waitTimelineReady(ctx context.Context, svcCtx *svc.ServiceContext, readyKey string) error {
	deadline := time.NewTimer(feedBuildWait(svcCtx))
	defer deadline.Stop()
	ticker := time.NewTicker(buildWaitPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("等待其他实例构建Timeline超时")
		case <-ticker.C:
			ready, err := timelineReady(ctx, svcCtx, readyKey)
			if err != nil {
				return err
			}
			if ready {
				return nil
			}
		}
	}
}

func loadTimelineVersion(ctx context.Context, svcCtx *svc.ServiceContext, versionKey string) (int64, error) {
	redisCtx, cancel := context.WithTimeout(ctx, feedRedisTimeout(svcCtx))
	defer cancel()
	version, err := svcCtx.RedisCli.Get(redisCtx, versionKey).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, errors.New("Timeline版本不能为负数")
	}
	return version, nil
}

func writeTimelineTemp(ctx context.Context, svcCtx *svc.ServiceContext, tempKey string, members []string) error {
	redisCtx, cancel := context.WithTimeout(ctx, feedRedisTimeout(svcCtx))
	defer cancel()
	if err := svcCtx.RedisCli.Del(redisCtx, tempKey).Err(); err != nil {
		return err
	}
	for start := 0; start < len(members); start += timelineTempWriteBatchSize {
		end := start + timelineTempWriteBatchSize
		if end > len(members) {
			end = len(members)
		}
		items := make([]redis.Z, 0, end-start)
		for _, member := range members[start:end] {
			items = append(items, redis.Z{Score: 0, Member: member})
		}
		if err := svcCtx.RedisCli.ZAdd(redisCtx, tempKey, items...).Err(); err != nil {
			return err
		}
	}
	if len(members) > 0 {
		return svcCtx.RedisCli.Expire(redisCtx, tempKey, feedBuildLockTTL(svcCtx)).Err()
	}
	return nil
}

func replaceTimelineIfVersionMatch(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	versionKey, timelineKey, readyKey, tempKey string,
	expectedVersion int64,
	ttl time.Duration,
) (bool, error) {
	redisCtx, cancel := context.WithTimeout(ctx, feedRedisTimeout(svcCtx))
	defer cancel()
	result, err := svcCtx.RedisCli.Eval(
		redisCtx,
		replaceTimelineIfVersionMatchScript,
		[]string{versionKey, timelineKey, readyKey, tempKey},
		strconv.FormatInt(expectedVersion, 10),
		strconv.FormatInt(int64(ttl/time.Second), 10),
	).Int64()
	return result == 1, err
}

func randomTimelineToken() (string, error) {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func feedUserTimelineTTL(svcCtx *svc.ServiceContext) time.Duration {
	seconds := svcCtx.Config.Timeline.UserTimelineTTLSeconds
	if seconds <= 0 {
		return defaultUserTimelineTTL
	}
	return time.Duration(seconds) * time.Second
}

func feedBuildLockTTL(svcCtx *svc.ServiceContext) time.Duration {
	seconds := svcCtx.Config.Timeline.BuildLockTTLSeconds
	if seconds <= 0 {
		return defaultBuildLockTTL
	}
	return time.Duration(seconds) * time.Second
}

func feedBuildWait(svcCtx *svc.ServiceContext) time.Duration {
	milliseconds := svcCtx.Config.Timeline.BuildWaitMs
	if milliseconds <= 0 {
		return defaultBuildWait
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func feedRedisTimeout(svcCtx *svc.ServiceContext) time.Duration {
	milliseconds := svcCtx.Config.Timeline.RedisOpTimeoutMs
	if milliseconds <= 0 {
		return defaultFeedRedisTimeout
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func feedDBTimeout(svcCtx *svc.ServiceContext) time.Duration {
	milliseconds := svcCtx.Config.Timeline.DBQueryTimeoutMs
	if milliseconds <= 0 {
		return defaultFeedDBTimeout
	}
	return time.Duration(milliseconds) * time.Millisecond
}
