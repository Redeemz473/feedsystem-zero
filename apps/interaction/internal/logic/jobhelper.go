package logic

import (
	"context"
	"errors"
	"sort"
	"time"

	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/common/eventx"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	maxFlushInteractionEvents = 500
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

// applyVideoStatDelta 更新 MySQL videos 表的冷备统计字段。
// 方案 B 架构下，videos.likes_count/comments_count/popularity 只作为 Redis 权威计数的冷备份，
// interaction_sync job 消费 Kafka 后异步维护；读侧永远读 Redis 权威 Hash，只在 Redis miss 时把
// 冷备值作为初始基准 HSetNX 到 Redis。
func applyVideoStatDelta(ctx context.Context, tx *gorm.DB, videoID uint64, delta videoStatDelta) error {
	updates := videoStatDeltaUpdates(delta)

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
	// "先 -1、后 +1"会错误收敛到 1。普通加法具备交换性，最终能按事件净和收敛。
	// 用户可见值直接来自 Redis 权威 Hash；DB 冷备值只用于 Redis miss 时的冷启动兜底。
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
