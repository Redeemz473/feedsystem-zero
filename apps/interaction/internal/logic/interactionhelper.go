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

// fillUnlikedState 把“未点赞”状态回填到 Redis，避免短时间内反复查 MySQL。
func fillUnlikedState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64) error {
	pipe := redisCli.TxPipeline()
	pipe.SRem(ctx, rediskey.LikeVideoUsersKey(videoID), strconv.FormatUint(userID, 10))
	pipe.SRem(ctx, rediskey.LikeUserVideosKey(userID), strconv.FormatUint(videoID, 10))
	pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), "0", likeStateTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// applyRedisLikeState 写点赞后的 Redis 实时状态和增量计数。
func applyRedisLikeState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64) error {
	field := redisHashField(videoID)

	pipe := redisCli.TxPipeline()
	pipe.SAdd(ctx, rediskey.LikeVideoUsersKey(videoID), strconv.FormatUint(userID, 10))
	pipe.SAdd(ctx, rediskey.LikeUserVideosKey(userID), strconv.FormatUint(videoID, 10))
	pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), "1", likeStateTTL)
	pipe.HIncrBy(ctx, rediskey.VideoLikeDeltaKey(), field, 1)
	pipe.HIncrBy(ctx, rediskey.VideoPopularityDeltaKey(), field, likePopularityWeight)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), float64(likePopularityWeight), field)
	pipe.Incr(ctx, rediskey.LikeUserVideosListVersionKey(userID))
	pipe.Del(ctx, rediskey.VideoStatsCacheKey(videoID))
	_, err := pipe.Exec(ctx)
	return err
}

// applyRedisUnlikeState 写取消点赞后的 Redis 实时状态和增量计数。
func applyRedisUnlikeState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64) error {
	field := redisHashField(videoID)

	pipe := redisCli.TxPipeline()
	pipe.SRem(ctx, rediskey.LikeVideoUsersKey(videoID), strconv.FormatUint(userID, 10))
	pipe.SRem(ctx, rediskey.LikeUserVideosKey(userID), strconv.FormatUint(videoID, 10))
	pipe.Set(ctx, rediskey.LikeStateKey(videoID, userID), "0", likeStateTTL)
	pipe.HIncrBy(ctx, rediskey.VideoLikeDeltaKey(), field, -1)
	pipe.HIncrBy(ctx, rediskey.VideoPopularityDeltaKey(), field, -likePopularityWeight)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), -float64(likePopularityWeight), field)
	pipe.Incr(ctx, rediskey.LikeUserVideosListVersionKey(userID))
	pipe.Del(ctx, rediskey.VideoStatsCacheKey(videoID))
	_, err := pipe.Exec(ctx)
	return err
}

// realtimeLikesCount 返回 MySQL 基准点赞数 + Redis 尚未刷库的增量。
func realtimeLikesCount(ctx context.Context, redisCli *redis.Client, video model.Video) int64 {
	delta, err := redisCli.HGet(ctx, rediskey.VideoLikeDeltaKey(), redisHashField(video.ID)).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nonNegative(video.LikesCount)
	}
	return nonNegative(video.LikesCount + delta)
}

// applyRedisCommentCreatedState 写评论发布后的 Redis 实时状态和增量计数，并返回最新评论数 delta。
func applyRedisCommentCreatedState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64, commentID uint64, requestID string) (int64, error) {
	field := redisHashField(videoID)

	pipe := redisCli.TxPipeline()
	queueBumpCommentListVersion(ctx, pipe, videoID)
	commentDeltaCmd := pipe.HIncrBy(ctx, rediskey.VideoCommentDeltaKey(), field, 1)
	pipe.HIncrBy(ctx, rediskey.VideoPopularityDeltaKey(), field, commentPopularityWeight)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), float64(commentPopularityWeight), field)
	pipe.Del(ctx, rediskey.VideoStatsCacheKey(videoID))
	if requestID != "" {
		pipe.Set(ctx, rediskey.CommentIdempotencyKey(userID, requestID), strconv.FormatUint(commentID, 10), commentIdempotencyTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return commentDeltaCmd.Val(), nil
}

// applyRedisCommentDeletedState 写评论删除后的 Redis 实时状态和增量计数，并返回最新评论数 delta。
func applyRedisCommentDeletedState(ctx context.Context, redisCli *redis.Client, videoID uint64, userID uint64, commentID uint64, requestID string) (int64, error) {
	field := redisHashField(videoID)

	pipe := redisCli.TxPipeline()
	queueBumpCommentListVersion(ctx, pipe, videoID)
	commentDeltaCmd := pipe.HIncrBy(ctx, rediskey.VideoCommentDeltaKey(), field, -1)
	pipe.HIncrBy(ctx, rediskey.VideoPopularityDeltaKey(), field, -commentPopularityWeight)
	pipe.ZIncrBy(ctx, rediskey.HotVideoRealtimeKey(), -float64(commentPopularityWeight), field)
	pipe.Del(ctx, rediskey.VideoStatsCacheKey(videoID))
	if requestID != "" {
		pipe.Del(ctx, rediskey.CommentIdempotencyKey(userID, requestID))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return commentDeltaCmd.Val(), nil
}

// realtimeCommentsCount 返回 MySQL 基准评论数 + Redis 尚未刷库的增量。
func realtimeCommentsCount(ctx context.Context, redisCli *redis.Client, video model.Video) int64 {
	delta, err := redisCli.HGet(ctx, rediskey.VideoCommentDeltaKey(), redisHashField(video.ID)).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nonNegative(video.CommentsCount)
	}
	return nonNegative(video.CommentsCount + delta)
}
