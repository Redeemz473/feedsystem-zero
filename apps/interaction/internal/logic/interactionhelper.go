package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	likeActionLockTTL       = 3 * time.Second
	likeStateTTL            = 7 * 24 * time.Hour
	commentIdempotencyTTL   = 24 * time.Hour
	likePopularityWeight    = eventx.LikePopularityWeight
	commentPopularityWeight = eventx.CommentPopularityWeight
)

// authStatsFields 是 VideoStatsAuthKey Hash 里的三个统计字段。
const (
	authFieldLikes      = "likes_count"
	authFieldComments   = "comments_count"
	authFieldPopularity = "popularity"
)

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func newEventID(prefix string) (string, error) {
	token, err := randomHex(12)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixNano(), token), nil
}

func likeAggregateID(videoID, userID uint64) string {
	// 点赞关系才是 like 聚合：同一用户对同一视频的 like/unlike 必须有序，
	// 不同用户之间可以并发投递。该值也同时作为 Kafka partition key。
	return fmt.Sprintf("%d:%d", videoID, userID)
}

// buildInteractionNotificationOutbox 构造专用通知事件。
// sourceEventID 指向同一事务里的点赞/评论领域事件，notificationEventID 则作为
// notification-job 的独立消费幂等 ID，二者不能复用。
func buildInteractionNotificationOutbox(
	notificationEventID string,
	sourceEventID string,
	receiverID uint64,
	actorID uint64,
	videoID uint64,
	commentID uint64,
	notificationType string,
	action string,
	occurredAt time.Time,
) (*model.OutboxEvent, error) {
	notificationEvent := eventx.NotificationEvent{
		EventID:          notificationEventID,
		SourceEventID:    sourceEventID,
		ReceiverID:       receiverID,
		ActorID:          actorID,
		VideoID:          videoID,
		CommentID:        commentID,
		NotificationType: notificationType,
		Action:           action,
		OccurredAt:       occurredAt.UnixMilli(),
	}
	envelope, aggregateID, err := eventx.BuildNotificationEnvelope(notificationEvent, "interaction-rpc")
	if err != nil {
		return nil, err
	}

	eventType := eventx.EventTypeNotificationCreate
	if action == eventx.NotificationActionDelete {
		eventType = eventx.EventTypeNotificationDelete
	}
	return &model.OutboxEvent{
		EventID:       notificationEventID,
		Topic:         eventx.TopicNotificationEvents,
		EventType:     eventType,
		AggregateType: eventx.AggregateNotification,
		AggregateID:   aggregateID,
		Payload:       string(envelope),
		Status:        model.OutboxStatusPending,
		CreatedAt:     occurredAt,
		UpdatedAt:     occurredAt,
	}, nil
}

func releaseRedisLock(ctx context.Context, redisCli *redis.Client, lockKey string, lockToken string) error {
	const unlockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`
	return redisCli.Eval(ctx, unlockScript, []string{lockKey}, lockToken).Err()
}

func redisHashField(id uint64) string {
	return strconv.FormatUint(id, 10)
}

func nonNegative(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

func newCommentListVersion() int64 {
	return time.Now().UnixMilli()
}

const bumpCommentListVersionScript = `
if redis.call("EXISTS", KEYS[1]) == 0 then
	return redis.call("SET", KEYS[1], ARGV[1])
end
return redis.call("INCR", KEYS[1])
`

// 用Lua更新评论的版本号
func queueBumpCommentListVersion(ctx context.Context, pipe redis.Pipeliner, videoID uint64) {
	pipe.Eval(ctx, bumpCommentListVersionScript, []string{rediskey.CommentListVersionKey(videoID)}, newCommentListVersion())
}

// loadLikeStateFromRedis 从 Redis 读取用户对视频的点赞覆盖状态。
// 返回值说明：
// - liked=true, hit=true：Redis 明确记录已点赞。
// - liked=false, hit=true：Redis 明确记录未点赞。
// - hit=false：Redis 没有状态，需要继续查 MySQL 兜底。
func loadLikeStateFromRedis(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64) (liked bool, hit bool, err error) {
	value, err := redisCli.Get(ctx, rediskey.LikeStateKey(videoID, userID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, false, nil
		}
		return false, false, err
	}

	switch value {
	case "1":
		return true, true, nil
	case "0":
		return false, true, nil
	default:
		return false, false, nil
	}
}

// loadLikeStateFromDB 查询 MySQL 中当前用户是否有效点赞了某个视频。
func loadLikeStateFromDB(ctx context.Context, db *gorm.DB, videoID uint64, userID uint64) (bool, error) {
	var count int64
	err := db.WithContext(ctx).
		Model(&model.Like{}).
		Where("video_id = ? AND user_id = ? AND status = ? AND deleted_at IS NULL", videoID, userID, model.LikeStatusActive).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// batchLoadLikeStates 批量查询用户对多个视频的点赞状态。
// 查询顺序：先用 Redis Pipeline 批量查覆盖缓存，未命中的 video_id 再批量查 MySQL，
// 最后把 MySQL 结果回填 Redis，避免视频列表页对 likes 表产生 N 次查询。
func batchLoadLikeStates(ctx context.Context, redisCli *redis.Client, db *gorm.DB, videoIDs []uint64, userID uint64) (map[uint64]bool, error) {
	result := make(map[uint64]bool, len(videoIDs))
	if len(videoIDs) == 0 {
		return result, nil
	}

	for _, videoID := range videoIDs {
		result[videoID] = false
	}

	if userID == 0 {
		return result, nil
	}

	pipe := redisCli.Pipeline()
	cmdMap := make(map[uint64]*redis.StringCmd, len(videoIDs))
	for _, videoID := range videoIDs {
		cmdMap[videoID] = pipe.Get(ctx, rediskey.LikeStateKey(videoID, userID))
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return batchLoadLikeStatesFromDB(ctx, redisCli, db, videoIDs, userID, result)
	}

	missVideoIDs := make([]uint64, 0)
	for videoID, cmd := range cmdMap {
		value, err := cmd.Result()
		if err == nil {
			result[videoID] = value == "1"
			continue
		}

		if errors.Is(err, redis.Nil) {
			missVideoIDs = append(missVideoIDs, videoID)
			continue
		}

		missVideoIDs = append(missVideoIDs, videoID)
	}

	if len(missVideoIDs) == 0 {
		return result, nil
	}

	return batchLoadLikeStatesFromDB(ctx, redisCli, db, missVideoIDs, userID, result)
}

func batchLoadLikeStatesFromDB(ctx context.Context, redisCli *redis.Client, db *gorm.DB, videoIDs []uint64, userID uint64, result map[uint64]bool) (map[uint64]bool, error) {
	var likedVideoIDs []uint64
	if err := db.WithContext(ctx).
		Model(&model.Like{}).
		Where("user_id = ? AND video_id IN ? AND status = ? AND deleted_at IS NULL", userID, videoIDs, model.LikeStatusActive).
		Pluck("video_id", &likedVideoIDs).Error; err != nil {
		return nil, err
	}

	likedSet := make(map[uint64]struct{}, len(likedVideoIDs))
	for _, videoID := range likedVideoIDs {
		likedSet[videoID] = struct{}{}
		result[videoID] = true
	}

	pipe := redisCli.Pipeline()
	for _, videoID := range videoIDs {
		value := "0"
		if _, ok := likedSet[videoID]; ok {
			value = "1"
		}
		pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), value, likeStateTTL)
	}
	_, _ = pipe.Exec(ctx)

	return result, nil
}

// fillLikedState 把 MySQL 中已经存在的点赞状态回填到 Redis。
func fillLikedState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64) error {
	pipe := redisCli.TxPipeline()
	pipe.SAdd(ctx, rediskey.LikeVideoUsersKey(videoID), strconv.FormatUint(userID, 10))
	pipe.SAdd(ctx, rediskey.LikeUserVideosKey(userID), strconv.FormatUint(videoID, 10))
	pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), "1", likeStateTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// fillUnlikedState 把"未点赞"状态回填到 Redis，避免短时间内反复查 MySQL。
func fillUnlikedState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64) error {
	pipe := redisCli.TxPipeline()
	pipe.SRem(ctx, rediskey.LikeVideoUsersKey(videoID), strconv.FormatUint(userID, 10))
	pipe.SRem(ctx, rediskey.LikeUserVideosKey(userID), strconv.FormatUint(videoID, 10))
	pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), "0", likeStateTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// bumpVideoStatsAuthScript 在在线写链路中乐观更新统计投影。
//
// 输入：
//
//	KEYS[1] = VideoStatsAuthKey(videoID)
//	ARGV[1] = base_likes（DB 冷备 likes_count，用于冷启动）
//	ARGV[2] = base_comments
//	ARGV[3] = base_popularity
//	ARGV[4] = base_stats_version
//	ARGV[5] = like_delta（本次操作对 likes_count 的增量）
//	ARGV[6] = comment_delta
//	ARGV[7] = popularity_delta
//	ARGV[8] = ttl_seconds
//
// 输出：{likes_count, comments_count, popularity}（本次操作后的权威值）
//
// 语义：
//  1. 若 auth key 不存在，先用 base_* 建立基准值（冷启动，不使用 HSetNX 单字段避免"部分字段存在"问题）。
//  2. 在基准上原子叠加本次的 delta 并 EXPIRE 续期。
//  3. 冷启动与叠加放在同一次 Eval 里，避免"两个并发线程都发现 EXISTS=0 → 各自 HSet 覆盖对方"造成的
//     "基准值丢增量"竞态。策略 X：不做严格单调，用户可见值允许短暂 -1/+1 抖动，由前端乐观 UI 掩盖。
const bumpVideoStatsAuthScript = `
if redis.call("EXISTS", KEYS[1]) == 0 then
	redis.call("HSET", KEYS[1],
		"likes_count", ARGV[1],
		"comments_count", ARGV[2],
		"popularity", ARGV[3],
		"stats_version", ARGV[4])
elseif not tonumber(redis.call("HGET", KEYS[1], "stats_version")) then
	-- 兼容升级前已经存在的三字段 Hash，不覆盖其中尚未消费的实时计数。
	redis.call("HSET", KEYS[1], "stats_version", ARGV[4])
end
local likes = redis.call("HINCRBY", KEYS[1], "likes_count", ARGV[5])
local comments = redis.call("HINCRBY", KEYS[1], "comments_count", ARGV[6])
local pop = redis.call("HINCRBY", KEYS[1], "popularity", ARGV[7])
local version = redis.call("HGET", KEYS[1], "stats_version")
redis.call("EXPIRE", KEYS[1], ARGV[8])
return {likes, comments, pop, version}
`

// videoStatsAuthResult 是 bumpVideoStatsAuthScript 的返回值封装。
type videoStatsAuthResult struct {
	LikesCount    int64
	CommentsCount int64
	Popularity    int64
	StatsVersion  uint64
}

// bumpVideoStatsAuth 原子地叠加 Redis 权威 Hash，并返回本次操作后的权威值。
// baseStats 是 DB 冷备值，只有 auth key 不存在时才使用；一旦 key 已存在，冷备值将被忽略。
func bumpVideoStatsAuth(
	ctx context.Context,
	redisCli *redis.Client,
	videoID uint64,
	baseStats videoStatsAuthResult,
	delta videoStatDelta,
) (videoStatsAuthResult, error) {
	values, err := redisCli.Eval(
		ctx,
		bumpVideoStatsAuthScript,
		[]string{rediskey.VideoStatsAuthKey(videoID)},
		strconv.FormatInt(baseStats.LikesCount, 10),
		strconv.FormatInt(baseStats.CommentsCount, 10),
		strconv.FormatInt(baseStats.Popularity, 10),
		strconv.FormatUint(baseStats.StatsVersion, 10),
		strconv.FormatInt(delta.LikeDelta, 10),
		strconv.FormatInt(delta.CommentDelta, 10),
		strconv.FormatInt(delta.PopularityDelta, 10),
		strconv.FormatInt(int64(rediskey.VideoStatsAuthTTL/time.Second), 10),
	).Slice()
	if err != nil {
		return videoStatsAuthResult{}, err
	}

	return parseVideoStatsAuthResult(values), nil
}

// parseVideoStatsAuthResult 解析 Lua 返回的 {likes, comments, popularity} 数组。
// Redis Lua 数字返回值在 go-redis 中可能是 int64 或 string，两种情况都需要兼容。
func parseVideoStatsAuthResult(values []any) videoStatsAuthResult {
	getAt := func(i int) int64 {
		if i < 0 || i >= len(values) {
			return 0
		}
		switch v := values[i].(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case string:
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		default:
			return 0
		}
	}
	return videoStatsAuthResult{
		LikesCount:    getAt(0),
		CommentsCount: getAt(1),
		Popularity:    getAt(2),
		StatsVersion:  uint64(nonNegative(getAt(3))),
	}
}

func parseVideoStatsAuthHash(values map[string]string) (videoStatsAuthResult, bool) {
	likesRaw, likesOK := values["likes_count"]
	commentsRaw, commentsOK := values["comments_count"]
	popularityRaw, popularityOK := values["popularity"]
	versionRaw, versionOK := values["stats_version"]
	if !likesOK || !commentsOK || !popularityOK || !versionOK {
		return videoStatsAuthResult{}, false
	}

	likes, err := strconv.ParseInt(likesRaw, 10, 64)
	if err != nil {
		return videoStatsAuthResult{}, false
	}
	comments, err := strconv.ParseInt(commentsRaw, 10, 64)
	if err != nil {
		return videoStatsAuthResult{}, false
	}
	popularity, err := strconv.ParseInt(popularityRaw, 10, 64)
	if err != nil {
		return videoStatsAuthResult{}, false
	}
	version, err := strconv.ParseUint(versionRaw, 10, 64)
	if err != nil {
		return videoStatsAuthResult{}, false
	}

	return videoStatsAuthResult{
		LikesCount:    likes,
		CommentsCount: comments,
		Popularity:    popularity,
		StatsVersion:  version,
	}, true
}

func parseLegacyVideoStatsAuthHash(values map[string]string) (videoStatsAuthResult, bool) {
	if _, hasVersion := values["stats_version"]; hasVersion {
		return videoStatsAuthResult{}, false
	}
	likes, err := strconv.ParseInt(values["likes_count"], 10, 64)
	if err != nil {
		return videoStatsAuthResult{}, false
	}
	comments, err := strconv.ParseInt(values["comments_count"], 10, 64)
	if err != nil {
		return videoStatsAuthResult{}, false
	}
	popularity, err := strconv.ParseInt(values["popularity"], 10, 64)
	if err != nil {
		return videoStatsAuthResult{}, false
	}
	return videoStatsAuthResult{
		LikesCount: likes, CommentsCount: comments, Popularity: popularity,
	}, true
}

// readVideoStatsAuthScript 读取统计投影。key 缺失、字段不完整或 DB 版本更新时，
// 用 MySQL 快照原子重建；命中时也刷新 TTL。
//
// 输入：
//
//	KEYS[1] = VideoStatsAuthKey(videoID)
//	ARGV[1..3] = base_likes/comments/popularity（冷启动初始值，来自 MySQL videos）
//	ARGV[4]   = base_stats_version
//	ARGV[5]   = ttl_seconds
//
// 输出：{likes_count, comments_count, popularity, stats_version}
const readVideoStatsAuthScript = `
local values = redis.call("HMGET", KEYS[1], "likes_count", "comments_count", "popularity", "stats_version")
local current_version = values[4]
local current_likes = tonumber(values[1])
local current_comments = tonumber(values[2])
local current_popularity = tonumber(values[3])
local current_version_number = tonumber(current_version)
if not current_version and current_likes and current_comments and current_popularity then
	-- 滚动升级兼容：旧三字段 Hash 可能包含尚未消费的在线增量，只补版本，不覆盖计数。
	redis.call("HSET", KEYS[1], "stats_version", ARGV[4])
elseif not current_version_number
		or not current_likes
		or not current_comments
		or not current_popularity
		or current_version_number < tonumber(ARGV[4]) then
	redis.call("HSET", KEYS[1],
		"likes_count", ARGV[1],
		"comments_count", ARGV[2],
		"popularity", ARGV[3],
		"stats_version", ARGV[4])
end
redis.call("EXPIRE", KEYS[1], ARGV[5])
return redis.call("HMGET", KEYS[1], "likes_count", "comments_count", "popularity", "stats_version")
`

// readVideoStatsAuthWithBase 读权威 Hash，miss 时用 DB 冷备做冷启动。
func readVideoStatsAuthWithBase(
	ctx context.Context,
	redisCli *redis.Client,
	videoID uint64,
	baseStats videoStatsAuthResult,
) (videoStatsAuthResult, error) {
	values, err := redisCli.Eval(
		ctx,
		readVideoStatsAuthScript,
		[]string{rediskey.VideoStatsAuthKey(videoID)},
		strconv.FormatInt(baseStats.LikesCount, 10),
		strconv.FormatInt(baseStats.CommentsCount, 10),
		strconv.FormatInt(baseStats.Popularity, 10),
		strconv.FormatUint(baseStats.StatsVersion, 10),
		strconv.FormatInt(int64(rediskey.VideoStatsAuthTTL/time.Second), 10),
	).Slice()
	if err != nil {
		return videoStatsAuthResult{}, err
	}

	return parseVideoStatsAuthResult(values), nil
}

// videoBaseStatsFromDB 把 model.Video 冷备字段转换为 Redis 冷启动基准。
func videoBaseStatsFromDB(video model.Video) videoStatsAuthResult {
	return videoStatsAuthResult{
		LikesCount:    nonNegative(video.LikesCount),
		CommentsCount: nonNegative(video.CommentsCount),
		Popularity:    nonNegative(video.Popularity),
		StatsVersion:  video.StatsVersion,
	}
}

// projectVideoStatsScript 由 Kafka Consumer 把 MySQL 持久快照投影到 Redis。
// 只有版本不旧于 Redis 当前版本时才覆盖，避免并发 Consumer 把新快照回滚成旧快照；
// 相同版本允许重复写，从而让 Kafka 重放修复上一次 Redis 写失败。
const projectVideoStatsScript = `
local current_version = redis.call("HGET", KEYS[1], "stats_version")
local current_version_number = tonumber(current_version)
if not current_version_number or tonumber(ARGV[1]) >= current_version_number then
	redis.call("HSET", KEYS[1],
		"stats_version", ARGV[1],
		"likes_count", ARGV[2],
		"comments_count", ARGV[3],
		"popularity", ARGV[4])
	redis.call("EXPIRE", KEYS[1], ARGV[5])
	return 1
end
return 0
`

type videoStatsProjection struct {
	VideoID uint64
	Stats   videoStatsAuthResult
}

// projectVideoStatsBatch 用一条 Redis Pipeline 投影整个 Flush 批次。
// 任一命令失败都会让 RPC 失败，Kafka 将重试同一批；processed_events 会跳过 DB 增量，
// 但 Flush 仍会重新读取并投影最新快照，因此不会重复计数。
func projectVideoStatsBatch(ctx context.Context, redisCli *redis.Client, projections []videoStatsProjection) error {
	if len(projections) == 0 {
		return nil
	}

	pipe := redisCli.Pipeline()
	for _, projection := range projections {
		stats := projection.Stats
		pipe.Eval(
			ctx,
			projectVideoStatsScript,
			[]string{rediskey.VideoStatsAuthKey(projection.VideoID)},
			strconv.FormatUint(stats.StatsVersion, 10),
			strconv.FormatInt(stats.LikesCount, 10),
			strconv.FormatInt(stats.CommentsCount, 10),
			strconv.FormatInt(stats.Popularity, 10),
			strconv.FormatInt(int64(rediskey.VideoStatsAuthTTL/time.Second), 10),
		)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// applyRedisLikeState 写点赞后的 Redis 实时状态，同时用 Lua 原子叠加权威 Hash。
// 返回值是操作后 Redis 权威点赞数（用户可见值）。
func applyRedisLikeState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64, video model.Video) (int64, error) {
	authStats, err := bumpVideoStatsAuth(ctx, redisCli, videoID, videoBaseStatsFromDB(video), videoStatDelta{
		LikeDelta:       1,
		PopularityDelta: likePopularityWeight,
	})
	if err != nil {
		return 0, err
	}

	field := redisHashField(videoID)
	pipe := redisCli.TxPipeline()
	pipe.SAdd(ctx, rediskey.LikeVideoUsersKey(videoID), strconv.FormatUint(userID, 10))
	pipe.SAdd(ctx, rediskey.LikeUserVideosKey(userID), strconv.FormatUint(videoID, 10))
	pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), "1", likeStateTTL)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), float64(likePopularityWeight), field)
	pipe.Incr(ctx, rediskey.LikeUserVideosListVersionKey(userID))
	if _, err := pipe.Exec(ctx); err != nil {
		return nonNegative(authStats.LikesCount), err
	}
	return nonNegative(authStats.LikesCount), nil
}

// applyRedisUnlikeState 写取消点赞后的 Redis 实时状态，同时用 Lua 原子扣减权威 Hash。
func applyRedisUnlikeState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64, video model.Video) (int64, error) {
	authStats, err := bumpVideoStatsAuth(ctx, redisCli, videoID, videoBaseStatsFromDB(video), videoStatDelta{
		LikeDelta:       -1,
		PopularityDelta: -likePopularityWeight,
	})
	if err != nil {
		return 0, err
	}

	field := redisHashField(videoID)
	pipe := redisCli.TxPipeline()
	pipe.SRem(ctx, rediskey.LikeVideoUsersKey(videoID), strconv.FormatUint(userID, 10))
	pipe.SRem(ctx, rediskey.LikeUserVideosKey(userID), strconv.FormatUint(videoID, 10))
	pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), "0", likeStateTTL)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), -float64(likePopularityWeight), field)
	pipe.Incr(ctx, rediskey.LikeUserVideosListVersionKey(userID))
	if _, err := pipe.Exec(ctx); err != nil {
		return nonNegative(authStats.LikesCount), err
	}
	return nonNegative(authStats.LikesCount), nil
}

// realtimeLikesCount 返回 Redis 权威点赞数；权威 Hash miss 时用 DB 冷备做冷启动。
func realtimeLikesCount(ctx context.Context, redisCli *redis.Client, video model.Video) int64 {
	authStats, err := readVideoStatsAuthWithBase(ctx, redisCli, video.ID, videoBaseStatsFromDB(video))
	if err != nil {
		return nonNegative(video.LikesCount)
	}
	return nonNegative(authStats.LikesCount)
}

// applyRedisCommentCreatedState 写评论发布后的 Redis 实时状态，同时用 Lua 原子叠加权威 Hash。
// 返回操作后 Redis 权威评论数（用户可见值）。
func applyRedisCommentCreatedState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64, commentID uint64, requestID string, video model.Video) (int64, error) {
	authStats, err := bumpVideoStatsAuth(ctx, redisCli, videoID, videoBaseStatsFromDB(video), videoStatDelta{
		CommentDelta:    1,
		PopularityDelta: commentPopularityWeight,
	})
	if err != nil {
		return 0, err
	}

	field := redisHashField(videoID)
	pipe := redisCli.TxPipeline()
	queueBumpCommentListVersion(ctx, pipe, videoID)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), float64(commentPopularityWeight), field)
	if requestID != "" {
		pipe.Set(ctx, rediskey.CommentIdempotencyKey(userID, requestID), strconv.FormatUint(commentID, 10), commentIdempotencyTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nonNegative(authStats.CommentsCount), err
	}
	return nonNegative(authStats.CommentsCount), nil
}

// applyRedisCommentDeletedState 写评论删除后的 Redis 实时状态，同时用 Lua 原子扣减权威 Hash。
func applyRedisCommentDeletedState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64, requestID string, video model.Video) (int64, error) {
	authStats, err := bumpVideoStatsAuth(ctx, redisCli, videoID, videoBaseStatsFromDB(video), videoStatDelta{
		CommentDelta:    -1,
		PopularityDelta: -commentPopularityWeight,
	})
	if err != nil {
		return 0, err
	}

	field := redisHashField(videoID)
	pipe := redisCli.TxPipeline()
	queueBumpCommentListVersion(ctx, pipe, videoID)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), -float64(commentPopularityWeight), field)
	if requestID != "" {
		pipe.Del(ctx, rediskey.CommentIdempotencyKey(userID, requestID))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nonNegative(authStats.CommentsCount), err
	}
	return nonNegative(authStats.CommentsCount), nil
}

// realtimeCommentsCount 返回 Redis 权威评论数；权威 Hash miss 时用 DB 冷备做冷启动。
func realtimeCommentsCount(ctx context.Context, redisCli *redis.Client, video model.Video) int64 {
	authStats, err := readVideoStatsAuthWithBase(ctx, redisCli, video.ID, videoBaseStatsFromDB(video))
	if err != nil {
		return nonNegative(video.CommentsCount)
	}
	return nonNegative(authStats.CommentsCount)
}
