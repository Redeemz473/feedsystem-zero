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

// applyVideoStatDelta 更新 MySQL videos 表的持久统计快照。
// 同一个视频在一个 Flush 事务内只更新一次，同时递增 stats_version；Redis Consumer 投影会用
// 这个版本做 CAS，防止并发批次把新快照覆盖成旧快照。
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
// 再把首次消费事件按 video_id 聚合并按主键升序更新。事务提交后会读取批次涉及视频的
// 最新持久快照；即使所有事件都是重放，调用方仍可重新投影 Redis，修复上一次失写。
func applyInteractionFlushBatch(
	ctx context.Context,
	db *gorm.DB,
	events []interactionFlushDBEvent,
	consumerName string,
	topic string,
) ([]videoStatsProjection, error) {
	if len(events) == 0 {
		return nil, nil
	}

	orderedEvents := append([]interactionFlushDBEvent(nil), events...)
	sort.Slice(orderedEvents, func(i, j int) bool {
		return orderedEvents[i].EventID < orderedEvents[j].EventID
	})

	videoIDSet := make(map[uint64]struct{}, len(events))
	for _, event := range events {
		videoIDSet[event.VideoID] = struct{}{}
	}

	now := time.Now()
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
	if err != nil {
		return nil, err
	}

	videoIDs := make([]uint64, 0, len(videoIDSet))
	for videoID := range videoIDSet {
		videoIDs = append(videoIDs, videoID)
	}
	sort.Slice(videoIDs, func(i, j int) bool { return videoIDs[i] < videoIDs[j] })
	return loadVideoStatsProjections(ctx, db, videoIDs)
}

func loadVideoStatsProjections(ctx context.Context, db *gorm.DB, videoIDs []uint64) ([]videoStatsProjection, error) {
	if len(videoIDs) == 0 {
		return nil, nil
	}

	videos := make([]model.Video, 0, len(videoIDs))
	if err := db.WithContext(ctx).
		Select("id", "likes_count", "comments_count", "popularity", "stats_version").
		Where("id IN ? AND status = ? AND deleted_at IS NULL", videoIDs, model.VideoStatusNormal).
		Order("id ASC").
		Find(&videos).Error; err != nil {
		return nil, err
	}

	projections := make([]videoStatsProjection, 0, len(videos))
	for _, video := range videos {
		projections = append(projections, videoStatsProjection{
			VideoID: video.ID,
			Stats:   videoBaseStatsFromDB(video),
		})
	}
	return projections, nil
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
	// 用户可见值优先来自 Redis 投影；MySQL 持久快照通过 stats_version CAS 同步到 Redis。
	updates := map[string]any{
		"popularity":    gorm.Expr("popularity + ?", delta.PopularityDelta),
		"stats_version": gorm.Expr("stats_version + 1"),
	}
	if delta.LikeDelta != 0 {
		updates["likes_count"] = gorm.Expr("likes_count + ?", delta.LikeDelta)
	}
	if delta.CommentDelta != 0 {
		updates["comments_count"] = gorm.Expr("comments_count + ?", delta.CommentDelta)
	}
	return updates
}
