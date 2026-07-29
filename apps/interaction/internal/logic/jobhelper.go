package logic

import (
	"context"
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
	interactionDeltaAckTimeout     = 3 * time.Second

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

type interactionDeltaAck struct {
	EventID         string
	VideoID         uint64
	Delta           videoStatDelta
	InvalidationKey string
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

const acknowledgeInteractionDeltaScript = `
if redis.call("EXISTS", KEYS[2]) == 1 then
	return 0
end

local function subtract(key, field, delta)
	if delta == 0 then
		return
	end
	local current = redis.call("HGET", key, field)
	if not current then
		return
	end
	current = tonumber(current)
	if not current then
		redis.call("HDEL", key, field)
		return
	end
	local nextValue = current - delta
	if nextValue == 0 then
		redis.call("HDEL", key, field)
	else
		redis.call("HSET", key, field, nextValue)
	end
end

if redis.call("EXISTS", KEYS[1]) == 1 then
	subtract(KEYS[3], ARGV[1], tonumber(ARGV[2]))
	subtract(KEYS[4], ARGV[1], tonumber(ARGV[3]))
	subtract(KEYS[5], ARGV[1], tonumber(ARGV[4]))
	redis.call("DEL", KEYS[1])
end

redis.call("SET", KEYS[2], "1", "EX", ARGV[5])
redis.call("DEL", KEYS[6])
redis.call("INCR", KEYS[7])
return 1
`

// acknowledgeInteractionDeltas 在 MySQL 事务提交后按事件确认 Redis 实时增量。
// Pipeline 只减少网络往返；每条 Eval 仍是独立原子操作，因此可以精确返回需要重试的事件。
func acknowledgeInteractionDeltas(ctx context.Context, redisCli *redis.Client, acks []interactionDeltaAck) map[string]error {
	failed := make(map[string]error)
	if len(acks) == 0 {
		return failed
	}

	pipe := redisCli.Pipeline()
	commands := make(map[string]*redis.Cmd, len(acks))
	ttlSeconds := strconv.FormatInt(int64(interactionDeltaTTL/time.Second), 10)
	for _, ack := range acks {
		field := redisHashField(ack.VideoID)
		commands[ack.EventID] = pipe.Eval(
			ctx,
			acknowledgeInteractionDeltaScript,
			[]string{
				rediskey.InteractionDeltaPendingKey(ack.EventID),
				rediskey.InteractionDeltaAckKey(ack.EventID),
				rediskey.VideoLikeDeltaKey(),
				rediskey.VideoCommentDeltaKey(),
				rediskey.VideoPopularityDeltaKey(),
				rediskey.VideoStatsCacheKey(ack.VideoID),
				ack.InvalidationKey,
			},
			field,
			strconv.FormatInt(ack.Delta.LikeDelta, 10),
			strconv.FormatInt(ack.Delta.CommentDelta, 10),
			strconv.FormatInt(ack.Delta.PopularityDelta, 10),
			ttlSeconds,
		)
	}
	_, _ = pipe.Exec(ctx)

	for eventID, command := range commands {
		if err := command.Err(); err != nil {
			failed[eventID] = err
		}
	}
	return failed
}
