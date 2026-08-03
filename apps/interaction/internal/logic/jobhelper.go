package logic

import (
	"context"
	"errors"
	"sort"
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
	interactionMutationDrainWait   = 10 * time.Second
	interactionMutationDrainPoll   = 20 * time.Millisecond

	interactionRebuildStatsJob       = "interaction:rebuild_video_stats"
	interactionStatsMutationLeaseJob = "interaction:stats_mutations"
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

type interactionFlushDBEvent struct {
	EventID string
	VideoID uint64
	Delta   videoStatDelta
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

const acquireInteractionStatsMutationLeaseScript = `
local now = redis.call("TIME")
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now_ms)

if redis.call("EXISTS", KEYS[2]) == 1 then
	return 0
end

local expires_at = now_ms + tonumber(ARGV[2])
redis.call("ZADD", KEYS[1], expires_at, ARGV[1])
redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]) * 2)
return 1
`

const releaseInteractionStatsMutationLeaseScript = `
redis.call("ZREM", KEYS[1], ARGV[1])
if redis.call("ZCARD", KEYS[1]) == 0 then
	redis.call("DEL", KEYS[1])
end
return 1
`

const acquireInteractionRebuildLockScript = `
local acquired = redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")
if acquired then
	return 1
end
return 0
`

const countInteractionStatsMutationLeasesScript = `
local now = redis.call("TIME")
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now_ms)
return redis.call("ZCARD", KEYS[1])
`

// acquireInteractionStatsMutationLease 允许在线互动写入和事件刷库并发执行，但与统计重建互斥。
// Lua 脚本在同一个原子操作里检查重建锁并登记租约，消除“先检查、后登记”的竞态窗口。
func acquireInteractionStatsMutationLease(ctx context.Context, redisCli *redis.Client) (string, string, bool, error) {
	token, err := randomHex(16)
	if err != nil {
		return "", "", false, err
	}

	leaseKey := rediskey.JobLeaseSetKey(interactionStatsMutationLeaseJob)
	rebuildLockKey := rediskey.JobLockKey(interactionRebuildStatsJob)
	acquired, err := redisCli.Eval(
		ctx,
		acquireInteractionStatsMutationLeaseScript,
		[]string{leaseKey, rebuildLockKey},
		token,
		strconv.FormatInt(interactionJobLockTTL.Milliseconds(), 10),
	).Int64()
	if err != nil {
		return "", "", false, err
	}
	return leaseKey, token, acquired == 1, nil
}

func releaseInteractionStatsMutationLease(ctx context.Context, redisCli *redis.Client, leaseKey string, token string) error {
	return redisCli.Eval(ctx, releaseInteractionStatsMutationLeaseScript, []string{leaseKey}, token).Err()
}

// tryAcquireInteractionRebuildLock 先关闭新的统计写入入口。
// 已经开始的写入由 waitForInteractionStatsMutationsDrained 等待完成，避免高流量下重建任务饥饿。
func tryAcquireInteractionRebuildLock(ctx context.Context, redisCli *redis.Client) (string, string, bool, error) {
	token, err := randomHex(16)
	if err != nil {
		return "", "", false, err
	}

	lockKey := rediskey.JobLockKey(interactionRebuildStatsJob)
	acquired, err := redisCli.Eval(
		ctx,
		acquireInteractionRebuildLockScript,
		[]string{lockKey},
		token,
		strconv.FormatInt(interactionJobLockTTL.Milliseconds(), 10),
	).Int64()
	if err != nil {
		return "", "", false, err
	}
	return lockKey, token, acquired == 1, nil
}

func waitForInteractionStatsMutationsDrained(ctx context.Context, redisCli *redis.Client) error {
	waitCtx, cancel := context.WithTimeout(ctx, interactionMutationDrainWait)
	defer cancel()

	leaseKey := rediskey.JobLeaseSetKey(interactionStatsMutationLeaseJob)
	for {
		active, err := redisCli.Eval(waitCtx, countInteractionStatsMutationLeasesScript, []string{leaseKey}).Int64()
		if err != nil {
			return err
		}
		if active == 0 {
			return nil
		}

		timer := time.NewTimer(interactionMutationDrainPoll)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return waitCtx.Err()
		case <-timer.C:
		}
	}
}

// 实现更新DB
func applyVideoStatDelta(ctx context.Context, tx *gorm.DB, videoID uint64, delta videoStatDelta) error {
	updates := videoStatDeltaUpdates(delta)

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

// applyInteractionFlushBatch 将一个 RPC 批次放进同一个事务：先写消费幂等记录，
// 再把首次消费事件按 video_id 聚合并按主键升序更新，减少事务提交和行锁死锁。
func applyInteractionFlushBatch(
	ctx context.Context,
	db *gorm.DB,
	events []interactionFlushDBEvent,
	consumerName string,
	topic string,
) error {
	if len(events) == 0 {
		return nil
	}

	orderedEvents := append([]interactionFlushDBEvent(nil), events...)
	sort.Slice(orderedEvents, func(i, j int) bool {
		return orderedEvents[i].EventID < orderedEvents[j].EventID
	})

	now := time.Now()
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		deltasByVideo := make(map[uint64]videoStatDelta)
		for _, event := range orderedEvents {
			inserted, err := insertProcessedEvent(ctx, tx, event.EventID, consumerName, topic, now)
			if err != nil {
				return err
			}
			if !inserted {
				continue
			}
			deltasByVideo[event.VideoID] = mergeVideoStatDelta(deltasByVideo[event.VideoID], event.Delta)
		}

		for _, videoID := range sortedVideoStatDeltaIDs(deltasByVideo) {
			delta := deltasByVideo[videoID]
			if delta == (videoStatDelta{}) {
				continue
			}
			if err := applyVideoStatDelta(ctx, tx, videoID, delta); err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// 视频已删除时仍提交 processed_events，避免历史消息永久重试。
					continue
				}
				return err
			}
		}
		return nil
	})
}

func mergeVideoStatDelta(current videoStatDelta, delta videoStatDelta) videoStatDelta {
	current.LikeDelta += delta.LikeDelta
	current.CommentDelta += delta.CommentDelta
	current.PopularityDelta += delta.PopularityDelta
	return current
}

func sortedVideoStatDeltaIDs(deltas map[uint64]videoStatDelta) []uint64 {
	videoIDs := make([]uint64, 0, len(deltas))
	for videoID := range deltas {
		videoIDs = append(videoIDs, videoID)
	}
	sort.Slice(videoIDs, func(i, j int) bool { return videoIDs[i] < videoIDs[j] })
	return videoIDs
}

func videoStatDeltaUpdates(delta videoStatDelta) map[string]any {
	// 聚合基准必须保留有符号增量。Kafka 是 at-least-once，历史消息也可能因
	// dispatcher 并发、重试或人工重放暂时反序；若每一步都 GREATEST(..., 0)，
	// “先 -1、后 +1”会错误收敛到 1。普通加法具备交换性，最终能按事件净和收敛。
	// 用户可见值仍由 BatchGetVideoStats/realtime* 在读侧统一截断为非负数。
	updates := map[string]any{
		"popularity": gorm.Expr("popularity + ?", delta.PopularityDelta),
	}
	if delta.LikeDelta != 0 {
		updates["likes_count"] = gorm.Expr("likes_count + ?", delta.LikeDelta)
	}
	if delta.CommentDelta != 0 {
		updates["comments_count"] = gorm.Expr("comments_count + ?", delta.CommentDelta)
	}
	return updates
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

	-- pending_count 是独立于增量算术的收敛不变量。最后一个事件确认后，
	-- 三个 Hash 中该视频不应再有任何实时增量；强制 HDEL 可以修复
	-- 高并发交错、进程切换或历史版本留下的孤立字段。
	local remaining = redis.call("DECR", KEYS[6])
	if remaining <= 0 then
		redis.call("DEL", KEYS[6])
		redis.call("HDEL", KEYS[3], ARGV[1])
		redis.call("HDEL", KEYS[4], ARGV[1])
		redis.call("HDEL", KEYS[5], ARGV[1])
	end
end

redis.call("SET", KEYS[2], "1", "EX", ARGV[5])
redis.call("DEL", KEYS[7])
redis.call("INCR", KEYS[8])
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
				rediskey.InteractionDeltaPendingCountKey(ack.VideoID),
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
