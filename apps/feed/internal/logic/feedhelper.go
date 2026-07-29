package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
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
	defaultAuthorOutboxLimit   int64 = 500
	defaultAuthorOutboxTTL           = 30 * 24 * time.Hour
	defaultMaxBigCreatorFanIn  int   = 100
	defaultBuildLockTTL              = 15 * time.Second
	defaultBuildWait                 = 1500 * time.Millisecond
	defaultFeedRedisTimeout          = time.Second
	defaultFeedDBTimeout             = 3 * time.Second
	timelineBuildMaxAttempts         = 3
	timelineTempWriteBatchSize       = 500
	timelineReadBatchSize      int64 = 64
	buildWaitPollInterval            = 50 * time.Millisecond
)

const (
	defaultHotRankWindowMinutes        int64 = 60
	maxHotRankWindowMinutes            int64 = 1440
	defaultHotRankMaxSize              int64 = 1000
	maxHotRankMaxSize                  int64 = 10000
	defaultHotRankDecayHalfLifeMinutes int64 = 30
	defaultHotRankSnapshotTTL                = 30 * time.Minute
	defaultHotRankMaxSnapshotAge             = 30 * time.Minute
	defaultHotRankBuildLockTTL               = 10 * time.Second
	defaultHotRankBuildWait                  = 1200 * time.Millisecond
	defaultHotRankRedisTimeout               = time.Second
	defaultHotRankFutureTolerance            = 5 * time.Minute
	hotRankReadBatchSize               int64 = 64
	hotRankMinuteLayout                      = "200601021504"
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

// promoteHotRankSnapshotScript 只有在构建者仍持有分布式锁时，才允许用临时
// ZSet 替换正式快照。ready 保存实际成员数，因此空榜也能被识别为已成功构建。
const promoteHotRankSnapshotScript = `
if redis.call("GET", KEYS[3]) ~= ARGV[1] then
    redis.call("DEL", KEYS[4])
    return 0
end

redis.call("DEL", KEYS[1])
local size = 0
if redis.call("EXISTS", KEYS[4]) == 1 then
    redis.call("RENAME", KEYS[4], KEYS[1])
    size = redis.call("ZCARD", KEYS[1])
end

local ttl = tonumber(ARGV[2])
if not ttl or ttl <= 0 then
    return redis.error_reply("invalid hot rank snapshot ttl")
end
if size > 0 then
    redis.call("EXPIRE", KEYS[1], ttl)
end
redis.call("SET", KEYS[2], tostring(size), "EX", ttl)
return 1
`

var timelineBuildGroup = syncx.NewSingleFlight()
var hotRankSnapshotBuildGroup = syncx.NewSingleFlight()

type timelinePage struct {
	Items   []*feed.FeedVideoItem
	HasMore bool
}

type hotRankPage struct {
	Items   []*feed.HotFeedVideoItem
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

// ensureGlobalTimeline 等待 Job 完成全局 Timeline 的 bootstrap，不再抢锁自建。
// 全局 Timeline 的单点建设由 apps/job/feed_timeline 负责，避免 rpc 与 job 争抢同一把构建锁导致互相等待失败。
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

// 用户首次访问或 Timeline 过期时，通过本地 SingleFlight 和 Redis 分布式锁
// 从 MySQL 的 follows + videos 构建快照，并用版本号避免覆盖并发写入。
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

type hotFeedQuery struct {
	SnapshotAt int64
	Offset     int64
	PageSize   int64
}

// normalizeHotFeedQuery 一次性规范热榜的快照和分页参数，避免 Logic 漏掉
// “非首页必须携带 snapshot_at”这条稳定分页约束。
func normalizeHotFeedQuery(
	svcCtx *svc.ServiceContext,
	requestedSnapshotAt int64,
	offset int64,
	pageSize int64,
) (hotFeedQuery, error) {
	return normalizeHotFeedQueryAt(svcCtx, requestedSnapshotAt, offset, pageSize, time.Now())
}

func normalizeHotFeedQueryAt(
	svcCtx *svc.ServiceContext,
	requestedSnapshotAt int64,
	offset int64,
	pageSize int64,
	now time.Time,
) (hotFeedQuery, error) {
	if offset < 0 {
		return hotFeedQuery{}, status.Error(codes.InvalidArgument, "热榜offset不能为负数")
	}
	if offset > hotRankMaxSize(svcCtx) {
		return hotFeedQuery{}, status.Error(codes.InvalidArgument, "热榜offset超出可查询范围")
	}
	if offset > 0 && requestedSnapshotAt == 0 {
		return hotFeedQuery{}, status.Error(codes.InvalidArgument, "热榜翻页必须携带首次响应的snapshot_at")
	}

	snapshotAt, err := normalizeHotFeedSnapshotAt(svcCtx, requestedSnapshotAt, now)
	if err != nil {
		return hotFeedQuery{}, err
	}
	return hotFeedQuery{
		SnapshotAt: snapshotAt,
		Offset:     offset,
		PageSize:   normalizeFeedPageSize(svcCtx, pageSize),
	}, nil
}

func normalizeHotFeedSnapshotAt(
	svcCtx *svc.ServiceContext,
	requestedSnapshotAt int64,
	now time.Time,
) (int64, error) {
	now = now.UTC()
	currentMinute := now.Truncate(time.Minute)
	if requestedSnapshotAt == 0 {
		return currentMinute.Unix(), nil
	}
	if requestedSnapshotAt < 0 {
		return 0, status.Error(codes.InvalidArgument, "热榜snapshot_at不能为负数")
	}

	requested := time.Unix(requestedSnapshotAt, 0).UTC()
	if requested.After(now.Add(hotRankFutureTolerance(svcCtx))) {
		return 0, status.Error(codes.InvalidArgument, "热榜snapshot_at不能晚于当前时间")
	}

	snapshot := requested.Truncate(time.Minute)
	// 客户端时钟轻微超前时固定到服务端当前分钟，不创建未来快照。
	if snapshot.After(currentMinute) {
		snapshot = currentMinute
	}
	if currentMinute.Sub(snapshot) > hotRankMaxSnapshotAge(svcCtx) {
		return 0, status.Error(codes.InvalidArgument, "热榜快照已过期，请从首页重新获取")
	}
	return snapshot.Unix(), nil
}

// ensureHotRankSnapshot 保证 snapshot_at 对应的聚合榜已经构建。
// 同进程用 SingleFlight 合并请求，多实例用 Redis 锁只允许一个构建者。
// 只有 offset=0 的首页允许创建快照；翻页时快照若已丢失必须重新从首页开始，
// 不能用相同 snapshot_at 重新计算一份可能已经变化的榜单。
func ensureHotRankSnapshot(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	snapshotAt int64,
	offset int64,
) error {
	asOf := hotRankSnapshotAsOf(snapshotAt)
	readyKey := rediskey.HotVideoMergeReadyKey(asOf)
	snapshotKey := rediskey.HotVideoMergeKey(asOf)

	ready, err := hotRankSnapshotReady(ctx, svcCtx, readyKey, snapshotKey)
	if err != nil {
		logx.WithContext(ctx).Errorf("check hot rank snapshot ready failed, as_of:%s error:%v", asOf, err)
		return status.Error(codes.Unavailable, "热榜缓存暂时不可用")
	}
	if ready {
		return nil
	}
	if offset > 0 {
		return status.Error(codes.FailedPrecondition, "热榜快照已失效，请从首页重新获取")
	}

	_, err = hotRankSnapshotBuildGroup.Do("hotrank:"+asOf, func() (any, error) {
		ready, err := hotRankSnapshotReady(ctx, svcCtx, readyKey, snapshotKey)
		if err != nil || ready {
			return nil, err
		}

		lockKey := rediskey.HotVideoMergeBuildLockKey(asOf)
		lockToken, locked, err := acquireHotRankSnapshotBuildLock(ctx, svcCtx, lockKey)
		if err != nil {
			return nil, err
		}
		if !locked {
			return nil, waitHotRankSnapshotReady(ctx, svcCtx, readyKey, snapshotKey)
		}
		defer releaseHotRankSnapshotBuildLock(ctx, svcCtx, lockKey, lockToken)

		// 拿锁前可能已有其他构建者刚完成，再检查一次可避免重复 ZUNIONSTORE。
		ready, err = hotRankSnapshotReady(ctx, svcCtx, readyKey, snapshotKey)
		if err != nil || ready {
			return nil, err
		}

		applied, err := buildHotRankSnapshot(
			ctx,
			svcCtx,
			time.Unix(snapshotAt, 0).UTC(),
			asOf,
			lockKey,
			lockToken,
		)
		if err != nil {
			return nil, err
		}
		if !applied {
			return nil, waitHotRankSnapshotReady(ctx, svcCtx, readyKey, snapshotKey)
		}
		return nil, nil
	})
	if err != nil {
		logx.WithContext(ctx).Errorf("ensure hot rank snapshot failed, as_of:%s error:%v", asOf, err)
		return status.Error(codes.Unavailable, "热榜正在生成，请稍后重试")
	}
	return nil
}

// buildHotRankSnapshot 对最近 N 个分钟窗口做带衰减权重的求和，只保留正分 Top K。
// 正式榜单使用临时 Key + Lua 原子替换，读请求不会看到构建到一半的结果。
func buildHotRankSnapshot(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	snapshot time.Time,
	asOf string,
	lockKey string,
	lockToken string,
) (bool, error) {
	sourceKeys, weights := hotRankMergeSources(
		snapshot,
		hotRankWindowMinutes(svcCtx),
		hotRankDecayHalfLifeMinutes(svcCtx),
	)
	tempKey := rediskey.HotVideoMergeTempKey(asOf, lockToken)

	redisCtx, cancel := context.WithTimeout(ctx, hotRankRedisTimeout(svcCtx))
	pipe := svcCtx.RedisCli.Pipeline()
	pipe.ZUnionStore(redisCtx, tempKey, &redis.ZStore{
		Keys:      sourceKeys,
		Weights:   weights,
		Aggregate: "SUM",
	})
	pipe.ZRemRangeByScore(redisCtx, tempKey, "-inf", "0")
	// ZSet 为升序 rank，删除 [0, -(K+1)] 后只留下分数最高的 K 个成员。
	pipe.ZRemRangeByRank(redisCtx, tempKey, 0, -(hotRankMaxSize(svcCtx) + 1))
	pipe.Expire(redisCtx, tempKey, hotRankBuildLockTTL(svcCtx))
	_, err := pipe.Exec(redisCtx)
	cancel()
	if err != nil {
		deleteHotRankTempKey(ctx, svcCtx, tempKey)
		return false, fmt.Errorf("合并热榜分钟窗口失败: %w", err)
	}

	redisCtx, cancel = context.WithTimeout(ctx, hotRankRedisTimeout(svcCtx))
	defer cancel()
	applied, err := svcCtx.RedisCli.Eval(
		redisCtx,
		promoteHotRankSnapshotScript,
		[]string{
			rediskey.HotVideoMergeKey(asOf),
			rediskey.HotVideoMergeReadyKey(asOf),
			lockKey,
			tempKey,
		},
		lockToken,
		strconv.FormatInt(int64(hotRankSnapshotTTL(svcCtx)/time.Second), 10),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("发布热榜快照失败: %w", err)
	}
	return applied == 1, nil
}

func hotRankMergeSources(
	snapshot time.Time,
	windowMinutes int64,
	halfLifeMinutes int64,
) ([]string, []float64) {
	snapshot = snapshot.UTC().Truncate(time.Minute)
	keys := make([]string, 0, windowMinutes)
	weights := make([]float64, 0, windowMinutes)
	for age := int64(0); age < windowMinutes; age++ {
		minute := snapshot.Add(-time.Duration(age) * time.Minute)
		keys = append(keys, rediskey.HotVideoWindowKey(minute.Format(hotRankMinuteLayout)))
		// 半衰期模型比固定权重更平滑：age=halfLife 时，该分钟事件只保留一半权重。
		weight := math.Pow(0.5, float64(age)/float64(halfLifeMinutes))
		weights = append(weights, weight)
	}
	return keys, weights
}

// loadHotRankPage 从固定快照读取 pageSize+1 条，多出的一条只用于判断 has_more。
// 快照在生命周期内不再更新，因此 offset 分页不会因实时热度变化产生跳页。
func loadHotRankPage(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	snapshotAt int64,
	offset int64,
	pageSize int64,
) (hotRankPage, error) {
	asOf := hotRankSnapshotAsOf(snapshotAt)
	snapshotKey := rediskey.HotVideoMergeKey(asOf)
	need := pageSize + 1
	items := make([]*feed.HotFeedVideoItem, 0, need)
	invalidMembers := make([]any, 0)
	scanOffset := offset

	redisCtx, cancel := context.WithTimeout(ctx, hotRankRedisTimeout(svcCtx))
	defer cancel()

	for int64(len(items)) < need {
		count := hotRankReadBatchSize
		remaining := need - int64(len(items))
		if remaining > count {
			count = remaining
		}
		values, err := svcCtx.RedisCli.ZRevRangeWithScores(
			redisCtx,
			snapshotKey,
			scanOffset,
			scanOffset+count-1,
		).Result()
		if err != nil {
			return hotRankPage{}, fmt.Errorf("读取热榜快照失败: %w", err)
		}
		if len(values) == 0 {
			break
		}
		scanOffset += int64(len(values))

		for _, value := range values {
			videoID, decodeErr := decodeHotRankVideoID(value.Member)
			if decodeErr != nil || value.Score <= 0 || math.IsNaN(value.Score) || math.IsInf(value.Score, 0) {
				invalidMembers = append(invalidMembers, value.Member)
				continue
			}
			items = append(items, &feed.HotFeedVideoItem{
				VideoId:  videoID,
				HotScore: value.Score,
				Rank:     offset + int64(len(items)) + 1,
			})
			if int64(len(items)) >= need {
				break
			}
		}
		if int64(len(values)) < count {
			break
		}
	}

	if len(invalidMembers) > 0 {
		// 快照成员由 Job 校验后产生，正常不会进入这里；清理失败不影响本次合法结果。
		_ = svcCtx.RedisCli.ZRem(redisCtx, snapshotKey, invalidMembers...).Err()
	}

	hasMore := int64(len(items)) > pageSize
	if hasMore {
		items = items[:pageSize]
	}
	return hotRankPage{Items: items, HasMore: hasMore}, nil
}

func decodeHotRankVideoID(member any) (uint64, error) {
	var value string
	switch typed := member.(type) {
	case string:
		value = typed
	case []byte:
		value = string(typed)
	default:
		return 0, fmt.Errorf("热榜member类型不合法: %T", member)
	}
	videoID, err := strconv.ParseUint(value, 10, 64)
	if err != nil || videoID == 0 {
		return 0, fmt.Errorf("热榜video_id不合法: %q", value)
	}
	return videoID, nil
}

func hotRankSnapshotReady(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	readyKey string,
	snapshotKey string,
) (bool, error) {
	redisCtx, cancel := context.WithTimeout(ctx, hotRankRedisTimeout(svcCtx))
	defer cancel()

	value, err := svcCtx.RedisCli.Get(redisCtx, readyKey).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	size, err := strconv.ParseInt(value, 10, 64)
	if err != nil || size < 0 {
		_ = svcCtx.RedisCli.Del(redisCtx, readyKey).Err()
		return false, errors.New("热榜快照Ready标记损坏")
	}
	if size == 0 {
		return true, nil
	}

	exists, err := svcCtx.RedisCli.Exists(redisCtx, snapshotKey).Result()
	if err != nil {
		return false, err
	}
	if exists == 0 {
		// Redis 淘汰了 ZSet、但 Ready 仍存在时主动修复，避免把非空榜误报为空榜。
		_ = svcCtx.RedisCli.Del(redisCtx, readyKey).Err()
		return false, nil
	}
	return true, nil
}

func acquireHotRankSnapshotBuildLock(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	lockKey string,
) (string, bool, error) {
	token, err := randomTimelineToken()
	if err != nil {
		return "", false, err
	}
	redisCtx, cancel := context.WithTimeout(ctx, hotRankRedisTimeout(svcCtx))
	defer cancel()
	locked, err := svcCtx.RedisCli.SetNX(redisCtx, lockKey, token, hotRankBuildLockTTL(svcCtx)).Result()
	return token, locked, err
}

func releaseHotRankSnapshotBuildLock(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	lockKey string,
	lockToken string,
) {
	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hotRankRedisTimeout(svcCtx))
	defer cancel()
	if err := svcCtx.RedisCli.Eval(
		redisCtx,
		releaseTimelineBuildLockScript,
		[]string{lockKey},
		lockToken,
	).Err(); err != nil {
		logx.WithContext(ctx).Errorf("release hot rank snapshot lock failed, key:%s error:%v", lockKey, err)
	}
}

func waitHotRankSnapshotReady(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	readyKey string,
	snapshotKey string,
) error {
	deadline := time.NewTimer(hotRankBuildWait(svcCtx))
	defer deadline.Stop()
	ticker := time.NewTicker(buildWaitPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("等待热榜快照构建超时")
		case <-ticker.C:
			ready, err := hotRankSnapshotReady(ctx, svcCtx, readyKey, snapshotKey)
			if err != nil {
				return err
			}
			if ready {
				return nil
			}
		}
	}
}

func deleteHotRankTempKey(ctx context.Context, svcCtx *svc.ServiceContext, tempKey string) {
	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hotRankRedisTimeout(svcCtx))
	defer cancel()
	_ = svcCtx.RedisCli.Del(redisCtx, tempKey).Err()
}

func hotRankSnapshotAsOf(snapshotAt int64) string {
	return time.Unix(snapshotAt, 0).UTC().Truncate(time.Minute).Format(hotRankMinuteLayout)
}

func hotRankWindowMinutes(svcCtx *svc.ServiceContext) int64 {
	minutes := svcCtx.Config.HotRank.WindowMinutes
	if minutes <= 0 {
		return defaultHotRankWindowMinutes
	}
	if minutes > maxHotRankWindowMinutes {
		return maxHotRankWindowMinutes
	}
	return minutes
}

func hotRankMaxSize(svcCtx *svc.ServiceContext) int64 {
	size := svcCtx.Config.HotRank.MaxRankSize
	if size <= 0 {
		return defaultHotRankMaxSize
	}
	if size > maxHotRankMaxSize {
		return maxHotRankMaxSize
	}
	return size
}

func hotRankDecayHalfLifeMinutes(svcCtx *svc.ServiceContext) int64 {
	minutes := svcCtx.Config.HotRank.DecayHalfLifeMinutes
	if minutes <= 0 {
		return defaultHotRankDecayHalfLifeMinutes
	}
	return minutes
}

func hotRankSnapshotTTL(svcCtx *svc.ServiceContext) time.Duration {
	seconds := svcCtx.Config.HotRank.SnapshotTTLSeconds
	if seconds <= 0 {
		return defaultHotRankSnapshotTTL
	}
	return time.Duration(seconds) * time.Second
}

func hotRankMaxSnapshotAge(svcCtx *svc.ServiceContext) time.Duration {
	seconds := svcCtx.Config.HotRank.MaxSnapshotAgeSeconds
	if seconds <= 0 {
		return defaultHotRankMaxSnapshotAge
	}
	return time.Duration(seconds) * time.Second
}

func hotRankBuildLockTTL(svcCtx *svc.ServiceContext) time.Duration {
	seconds := svcCtx.Config.HotRank.BuildLockTTLSeconds
	if seconds <= 0 {
		return defaultHotRankBuildLockTTL
	}
	return time.Duration(seconds) * time.Second
}

func hotRankBuildWait(svcCtx *svc.ServiceContext) time.Duration {
	milliseconds := svcCtx.Config.HotRank.BuildWaitMs
	if milliseconds <= 0 {
		return defaultHotRankBuildWait
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func hotRankRedisTimeout(svcCtx *svc.ServiceContext) time.Duration {
	milliseconds := svcCtx.Config.HotRank.RedisOpTimeoutMs
	if milliseconds <= 0 {
		return defaultHotRankRedisTimeout
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func hotRankFutureTolerance(svcCtx *svc.ServiceContext) time.Duration {
	seconds := svcCtx.Config.HotRank.FutureToleranceSeconds
	if seconds <= 0 {
		return defaultHotRankFutureTolerance
	}
	return time.Duration(seconds) * time.Second
}

// feedAuthorOutboxTTL 大 V outbox 的有效期，写侧续期由 Job 负责，
// 读侧懒加载会在冷启动时使用同一 TTL 首次写入。
func feedAuthorOutboxTTL(svcCtx *svc.ServiceContext) time.Duration {
	seconds := svcCtx.Config.Timeline.AuthorOutboxTTLSeconds
	if seconds <= 0 {
		return defaultAuthorOutboxTTL
	}
	return time.Duration(seconds) * time.Second
}

// feedAuthorOutboxLimit 单个大 V outbox 保留的最大条数，用于冷启动查 MySQL。
func feedAuthorOutboxLimit(svcCtx *svc.ServiceContext) int64 {
	value := svcCtx.Config.Timeline.AuthorOutboxMaxLen
	if value <= 0 {
		return defaultAuthorOutboxLimit
	}
	return value
}

// feedMaxBigCreatorFanIn 单次读侧最多合并的大 V outbox 数量。
// viewer 关注的大 V 超过阈值时，只挑最近 N 个避免读放大。
func feedMaxBigCreatorFanIn(svcCtx *svc.ServiceContext) int {
	value := svcCtx.Config.Timeline.MaxBigCreatorFanIn
	if value <= 0 {
		return defaultMaxBigCreatorFanIn
	}
	return value
}

// ensureAuthorOutbox 懒加载单个大 V 的 outbox。
// 复用通用的 ensureTimeline 冷启动流程（分布式锁 + 版本号 CAS + 临时 ZSet 原子替换），
// passiveOnly=false 表示 rpc 侧自己抢锁构建，与 Job 侧写事件互不阻塞
// （Job 通过 mutateAuthorOutbox 的版本递增让并发构建重来）。
func ensureAuthorOutbox(ctx context.Context, svcCtx *svc.ServiceContext, authorID uint64) error {
	if authorID == 0 {
		return nil
	}
	return ensureTimeline(
		ctx,
		svcCtx,
		"author_outbox:"+strconv.FormatUint(authorID, 10),
		rediskey.FeedAuthorOutboxReadyKey(authorID),
		rediskey.FeedAuthorOutboxBuildLockKey(authorID),
		rediskey.FeedAuthorOutboxVersionKey(authorID),
		rediskey.FeedAuthorOutboxKey(authorID),
		func(token string) string { return rediskey.FeedAuthorOutboxTempKey(authorID, token) },
		feedAuthorOutboxTTL(svcCtx),
		func(loadCtx context.Context) ([]string, error) {
			return loadAuthorOutboxMembers(loadCtx, svcCtx, authorID)
		},
		false,
	)
}

// loadAuthorOutboxMembers 从 MySQL 读取该作者最近 N 条视频，用于冷启动 outbox。
// 只查发布态、未软删的视频；下沉/审核不通过的视频 Job 侧的事件会补上删除动作。
func loadAuthorOutboxMembers(ctx context.Context, svcCtx *svc.ServiceContext, authorID uint64) ([]string, error) {
	var videos []model.Video
	if err := svcCtx.GormDB.WithContext(ctx).
		Select("id", "author_id", "status", "deleted_at", "created_at").
		Where("author_id = ? AND status = ? AND deleted_at IS NULL", authorID, model.VideoStatusNormal).
		Order("created_at DESC, id DESC").
		Limit(int(feedAuthorOutboxLimit(svcCtx))).
		Find(&videos).Error; err != nil {
		return nil, fmt.Errorf("查询大V outbox 快照失败, author_id:%d: %w", authorID, err)
	}
	return timelineMembersFromVideos(videos)
}

// listFollowingBigCreators 返回 viewer 关注的所有大 V 作者 id。
// 通过 join accounts.is_big_v = 1 做筛选，避免应用层拉全表再过滤。
// 该标记位由 social 模块在关注事务内维护，只升不降，能天然覆盖两类场景：
//  1. 阈值反向穿越：大 V 掉粉后 is_big_v 仍为 1，历史 outbox 视频照常被 union，不会消失；
//  2. 阈值抖动：is_big_v 一次升级永久生效，避免读侧判定与写侧不一致。
//
// 结果按 accounts.follower_count DESC 排序，超过 fan-in 上限时优先保留粉丝多的作者
// 输出去掉了不必要的 status/deleted_at 判空，因为查询本身已经按有效关注过滤。
func listFollowingBigCreators(ctx context.Context, svcCtx *svc.ServiceContext, viewerID uint64) ([]uint64, error) {
	if viewerID == 0 {
		return nil, nil
	}
	var authorIDs []uint64
	err := svcCtx.GormDB.WithContext(ctx).
		Table("follows AS f").
		Select("f.following_id").
		Joins("JOIN accounts AS a ON a.id = f.following_id").
		Where("f.follower_id = ? AND f.status = ? AND f.deleted_at IS NULL", viewerID, model.FollowStatusActive).
		Where("a.is_big_v = ?", true).
		Order("a.follower_count DESC, f.following_id ASC").
		Limit(feedMaxBigCreatorFanIn(svcCtx)).
		Pluck("f.following_id", &authorIDs).Error
	if err != nil {
		return nil, fmt.Errorf("查询关注的大V失败, viewer_id:%d: %w", viewerID, err)
	}
	return authorIDs, nil
}

// loadFollowingFeedMerged 读侧推拉合并入口：
//  1. 读 viewer 自己的 inbox（小 V 推过来的） pageSize+1 条；
//  2. 并行懒加载/读取 viewer 关注的大 V 的 outbox，各取 pageSize+1 条；
//  3. 按 timeline member 字典序（等价 publishedAt DESC + videoID DESC）归并去重；
//  4. 截断到 pageSize，返回是否 hasMore。
//
// 复杂度：Redis 调用次数 = 1 (inbox) + N (大 V outbox)，N 最多为 MaxBigCreatorFanIn。
// 不再需要对大 V 的每次发布都做一次全量 fanout，写侧成本从 O(followers) 降至 O(1)。
func loadFollowingFeedMerged(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	viewerID uint64,
	cursorPublishedAt int64,
	cursorVideoID uint64,
	pageSize int64,
) (timelinePage, error) {
	// 1. 关注的大 V 列表
	dbCtx, cancel := context.WithTimeout(ctx, feedDBTimeout(svcCtx))
	bigCreatorIDs, err := listFollowingBigCreators(dbCtx, svcCtx, viewerID)
	cancel()
	if err != nil {
		return timelinePage{}, err
	}

	// 2. inbox（一定要读，即使没有大 V 关注也要读）
	inboxPage, err := loadTimelinePage(
		ctx, svcCtx,
		rediskey.FeedTimelineKey(viewerID),
		cursorPublishedAt, cursorVideoID,
		pageSize,
	)
	if err != nil {
		return timelinePage{}, err
	}
	if len(bigCreatorIDs) == 0 {
		return inboxPage, nil
	}

	// 3. 逐个懒加载 + 读大 V outbox
	// 顺序处理保持逻辑简单；后续如果 fan-in 变大可以改为并发。
	outboxItems := make([]*feed.FeedVideoItem, 0, len(bigCreatorIDs)*int(pageSize))
	outboxHasMore := false
	for _, authorID := range bigCreatorIDs {
		if err := ensureAuthorOutbox(ctx, svcCtx, authorID); err != nil {
			// 单个大 V outbox 失败不阻塞整个 feed，降级为该作者视频不出现在本页。
			// 下次访问 outbox ready 后会被读到，最坏情况用户会短暂看不到该作者最新视频。
			logx.WithContext(ctx).Errorf(
				"ensure author outbox failed, viewer_id:%d author_id:%d error:%v",
				viewerID, authorID, err,
			)
			continue
		}
		page, err := loadTimelinePage(
			ctx, svcCtx,
			rediskey.FeedAuthorOutboxKey(authorID),
			cursorPublishedAt, cursorVideoID,
			pageSize,
		)
		if err != nil {
			logx.WithContext(ctx).Errorf(
				"load author outbox page failed, viewer_id:%d author_id:%d error:%v",
				viewerID, authorID, err,
			)
			continue
		}
		outboxItems = append(outboxItems, page.Items...)
		outboxHasMore = outboxHasMore || page.HasMore
	}

	// 4. 归并去重
	merged := mergeFeedItems(inboxPage.Items, outboxItems, pageSize)
	hasMore := inboxPage.HasMore ||
		outboxHasMore ||
		int64(len(inboxPage.Items)+len(outboxItems)) > pageSize
	return timelinePage{Items: merged, HasMore: hasMore}, nil
}

// mergeFeedItems 合并 inbox 和多个 outbox 的结果，按 (publishedAt DESC, videoID DESC) 排序，
// 相同 videoID 去重（inbox 里如果历史推送过该大 V 视频，outbox 会覆盖）。
func mergeFeedItems(inbox, outbox []*feed.FeedVideoItem, pageSize int64) []*feed.FeedVideoItem {
	seen := make(map[uint64]struct{}, len(inbox)+len(outbox))
	all := make([]*feed.FeedVideoItem, 0, len(inbox)+len(outbox))
	for _, item := range inbox {
		if _, ok := seen[item.GetVideoId()]; ok {
			continue
		}
		seen[item.GetVideoId()] = struct{}{}
		all = append(all, item)
	}
	for _, item := range outbox {
		if _, ok := seen[item.GetVideoId()]; ok {
			continue
		}
		seen[item.GetVideoId()] = struct{}{}
		all = append(all, item)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].GetPublishedAt() != all[j].GetPublishedAt() {
			return all[i].GetPublishedAt() > all[j].GetPublishedAt()
		}
		return all[i].GetVideoId() > all[j].GetVideoId()
	})
	if int64(len(all)) > pageSize {
		all = all[:pageSize]
	}
	return all
}
