package logic

import (
	"context"
	"errors"
	"strconv"
	"time"

	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	maxFlushInteractionEvents      = 500
	maxRebuildInteractionStatItems = 100
	interactionJobLockTTL          = 30 * time.Second

	interactionFlushLikeEventsJob    = "interaction:flush_like_events"
	interactionFlushCommentEventsJob = "interaction:flush_comment_events"
	interactionRebuildStatsJob       = "interaction:rebuild_video_stats"
)

type videoStatDelta struct {
	LikeDelta       int64
	CommentDelta    int64
	PopularityDelta int64
}

type videoStatCount struct {
	VideoID uint64
	Count   int64
}

// 一次处理事件的个数
func validateInternalEventBatchSize(size int) error {
	if size > maxFlushInteractionEvents {
		return status.Errorf(codes.InvalidArgument, "一次最多处理%d条事件", maxFlushInteractionEvents)
	}
	return nil
}

func processedEventExpireAt(now time.Time) *time.Time {
	expireAt := now.Add(time.Duration(eventx.DefaultProcessedEventTTLDays) * 24 * time.Hour)
	return &expireAt
}

// 消费核心幂等实现
func insertProcessedEvent(ctx context.Context, tx *gorm.DB, eventID string, consumerName string, topic string, now time.Time) (bool, error) {
	result := tx.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "event_id"}, {Name: "consumer_name"}}, //联合唯一索引：同一个事件 + 同一个消费任务 只能存在一条记录
		DoNothing: true,                                                         //如果索引冲突，不报错、不更新任何字段，直接忽略这条插入
	}).Create(&model.ProcessedEvent{
		EventID:      eventID,
		ConsumerName: consumerName,
		Topic:        topic,
		ProcessedAt:  now,
		ExpireAt:     processedEventExpireAt(now),
	})
	if result.Error != nil {
		return false, result.Error
	}
	//这条事件 + 消费任务从未处理过（无冲突）result.RowsAffected==1
	// 已经处理过（联合索引冲突） result.RowsAffected==0
	return result.RowsAffected > 0, nil
}

// Redis 锁做前置削峰，同一时刻只允许一个实例执行同步任务，避免多实例同时更新同一条视频统计
func tryAcquireInteractionJobLock(ctx context.Context, redisCli *redis.Client, name string) (string, string, bool, error) {
	lockToken, err := randomHex(16)
	if err != nil {
		return "", "", false, err
	}

	lockKey := rediskey.JobLockKey(name)
	locked, err := redisCli.SetNX(ctx, lockKey, lockToken, interactionJobLockTTL).Result()
	if err != nil {
		return "", "", false, err
	}
	return lockKey, lockToken, locked, nil
}

func isInteractionJobLocked(ctx context.Context, redisCli *redis.Client, name string) (bool, error) {
	n, err := redisCli.Exists(ctx, rediskey.JobLockKey(name)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func isStatsRebuildRunning(ctx context.Context, redisCli *redis.Client) (bool, error) {
	return isInteractionJobLocked(ctx, redisCli, interactionRebuildStatsJob)
}

func rejectIfStatsRebuildRunning(ctx context.Context, redisCli *redis.Client) error {
	redisCtx, cancel := context.WithTimeout(ctx, commentRedisOpTimeout)
	defer cancel()

	running, err := isStatsRebuildRunning(redisCtx, redisCli)
	if err != nil {
		return err
	}
	if running {
		return status.Error(codes.Aborted, "互动统计重建中，请稍后重试")
	}
	return nil
}

// 实现更新DB
func applyVideoStatDelta(ctx context.Context, tx *gorm.DB, videoID uint64, delta videoStatDelta) error {
	updates := map[string]any{
		"popularity": gorm.Expr("GREATEST(popularity + ?, 0)", delta.PopularityDelta),
	}
	if delta.LikeDelta != 0 {
		updates["likes_count"] = gorm.Expr("GREATEST(likes_count + ?, 0)", delta.LikeDelta)
	}
	if delta.CommentDelta != 0 {
		updates["comments_count"] = gorm.Expr("GREATEST(comments_count + ?, 0)", delta.CommentDelta)
	}

	//更新DB内的video表
	result := tx.WithContext(ctx).Model(&model.Video{}).
		Where("id = ? AND status = ? AND deleted_at IS NULL", videoID, model.VideoStatusNormal).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func mergeVideoStatDelta(deltas map[uint64]videoStatDelta, videoID uint64, delta videoStatDelta) {
	current := deltas[videoID]
	current.LikeDelta += delta.LikeDelta
	current.CommentDelta += delta.CommentDelta
	current.PopularityDelta += delta.PopularityDelta
	deltas[videoID] = current
}

func subtractFlushedVideoDeltas(ctx context.Context, redisCli *redis.Client, deltas map[uint64]videoStatDelta) error {
	var joined error
	for videoID, delta := range deltas {
		// deltas 保存的是“本批已成功落库事件”的净增量。
		// 同一个视频在同一批里 create/delete 抵消时，只扣减 Redis 中相同净增量，避免重复扣减。
		field := redisHashField(videoID)
		if delta.LikeDelta != 0 {
			joined = errors.Join(joined, subtractRedisHashDelta(ctx, redisCli, rediskey.VideoLikeDeltaKey(), field, delta.LikeDelta))
		}
		if delta.CommentDelta != 0 {
			joined = errors.Join(joined, subtractRedisHashDelta(ctx, redisCli, rediskey.VideoCommentDeltaKey(), field, delta.CommentDelta))
		}
		if delta.PopularityDelta != 0 {
			joined = errors.Join(joined, subtractRedisHashDelta(ctx, redisCli, rediskey.VideoPopularityDeltaKey(), field, delta.PopularityDelta))
		}
	}
	return joined
}

func subtractRedisHashDelta(ctx context.Context, redisCli *redis.Client, key string, field string, delta int64) error {
	if delta == 0 {
		return nil
	}

	const script = `
local current = redis.call("HGET", KEYS[1], ARGV[1])
if not current then
	return 0
end
current = tonumber(current)
if not current then
	redis.call("HDEL", KEYS[1], ARGV[1])
	return 0
end
local nextValue = current - tonumber(ARGV[2])
if nextValue == 0 then
	redis.call("HDEL", KEYS[1], ARGV[1])
else
	redis.call("HSET", KEYS[1], ARGV[1], nextValue)
end
return nextValue
`
	return redisCli.Eval(ctx, script, []string{key}, field, strconv.FormatInt(delta, 10)).Err()
}

func deleteVideoStatsCaches(ctx context.Context, redisCli *redis.Client, videoIDs []uint64) error {
	if len(videoIDs) == 0 {
		return nil
	}

	pipe := redisCli.Pipeline()
	for _, videoID := range videoIDs {
		pipe.Del(ctx, rediskey.VideoStatsCacheKey(videoID))
	}
	_, err := pipe.Exec(ctx)
	return err
}

func uniqueVideoIDsFromDeltas(deltas map[uint64]videoStatDelta) []uint64 {
	videoIDs := make([]uint64, 0, len(deltas))
	for videoID := range deltas {
		videoIDs = append(videoIDs, videoID)
	}
	return videoIDs
}
