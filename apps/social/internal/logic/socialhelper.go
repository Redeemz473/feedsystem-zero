package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"feedsystem-zero/apps/social/internal/model"
	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultSocialPageSize = 20
	maxSocialPageSize     = 50
	defaultSocialIDPrefix = "social"
)

// normalizeSocialPage 统一 Social 列表页大小。
// pageSize=0 使用默认值；pageSize<0 返回 InvalidArgument；pageSize>50 自动截断到 50。
func normalizeSocialPage(pageSize int64) (int, error) {
	if pageSize < 0 {
		return 0, status.Error(codes.InvalidArgument, "page_size不能为负数")
	}
	if pageSize == 0 {
		return defaultSocialPageSize, nil
	}
	if pageSize > maxSocialPageSize {
		return maxSocialPageSize, nil
	}
	return int(pageSize), nil
}

// validateFollowCursor 校验关注/粉丝列表游标。
// 两个游标必须同时为空或同时非空；cursorUpdatedAt 使用 Unix milliseconds。
func validateFollowCursor(cursorUpdatedAt int64, cursorFollowID uint64) (time.Time, bool, error) {
	if cursorUpdatedAt == 0 && cursorFollowID == 0 {
		return time.Time{}, false, nil
	}
	if cursorUpdatedAt <= 0 || cursorFollowID == 0 {
		return time.Time{}, false, status.Error(codes.InvalidArgument, "cursor_updated_at和cursor_follow_id必须同时为空或同时非空")
	}
	return time.UnixMilli(cursorUpdatedAt), true, nil
}

// normalizeUserIDs 过滤 0、去重并保留输入顺序，同时限制最大数量。
func normalizeUserIDs(ids []uint64, max int) ([]uint64, error) {
	if max <= 0 {
		return nil, status.Error(codes.InvalidArgument, "max必须大于0")
	}

	result := make([]uint64, 0, len(ids))
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if len(result) >= max {
			return nil, status.Errorf(codes.InvalidArgument, "user_ids数量不能超过%d", max)
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}

	return result, nil
}

// batchLoadFollowingStates 批量查询 viewerID 是否关注 targetUserIDs。
// 查询顺序：Redis 覆盖缓存 -> MySQL IN 查询兜底 -> Redis SetNX 回填命中/未命中状态。
func batchLoadFollowingStates(ctx context.Context, svcCtx *svc.ServiceContext, viewerID uint64, targetUserIDs []uint64) (map[uint64]bool, error) {
	result := make(map[uint64]bool, len(targetUserIDs))
	for _, targetUserID := range targetUserIDs {
		result[targetUserID] = false
	}

	if len(targetUserIDs) == 0 || viewerID == 0 {
		return result, nil
	}

	pipe := svcCtx.RedisCli.Pipeline()
	cmdMap := make(map[uint64]*redis.StringCmd, len(targetUserIDs))
	for _, targetUserID := range targetUserIDs {
		cmdMap[targetUserID] = pipe.Get(ctx, rediskey.SocialFollowingStateKey(viewerID, targetUserID))
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return batchLoadFollowingStatesFromDB(ctx, svcCtx, viewerID, targetUserIDs, result, false)
	}

	missIDs := make([]uint64, 0)
	for targetUserID, cmd := range cmdMap {
		value, err := cmd.Result()
		if err == nil {
			switch value {
			case "1":
				result[targetUserID] = true
			case "0":
				result[targetUserID] = false
			default:
				missIDs = append(missIDs, targetUserID)
			}
			continue
		}

		if errors.Is(err, redis.Nil) {
			missIDs = append(missIDs, targetUserID)
			continue
		}

		missIDs = append(missIDs, targetUserID)
	}

	if len(missIDs) == 0 {
		return result, nil
	}

	return batchLoadFollowingStatesFromDB(ctx, svcCtx, viewerID, missIDs, result, true)
}

func batchLoadFollowingStatesFromDB(ctx context.Context, svcCtx *svc.ServiceContext, viewerID uint64, targetUserIDs []uint64, result map[uint64]bool, backfillCache bool) (map[uint64]bool, error) {
	var followingIDs []uint64
	if err := svcCtx.GormDB.WithContext(ctx).
		Model(&model.Follow{}).
		Where("follower_id = ? AND following_id IN ? AND status = ? AND deleted_at IS NULL", viewerID, targetUserIDs, model.FollowStatusActive).
		Pluck("following_id", &followingIDs).Error; err != nil {
		return nil, status.Error(codes.Internal, "查询关注状态失败")
	}

	followingSet := make(map[uint64]struct{}, len(followingIDs))
	for _, followingID := range followingIDs {
		followingSet[followingID] = struct{}{}
		result[followingID] = true
	}

	if backfillCache {
		pipe := svcCtx.RedisCli.Pipeline()
		for _, targetUserID := range targetUserIDs {
			value := "0"
			if _, ok := followingSet[targetUserID]; ok {
				value = "1"
			}
			pipe.SetNX(ctx, rediskey.SocialFollowingStateKey(viewerID, targetUserID), value, rediskey.SocialFollowingStateTTL)
		}
		_, _ = pipe.Exec(ctx)
	}

	return result, nil
}

// newSocialEventID 生成全局唯一事件 ID。
func newSocialEventID(prefix string) (string, error) {
	if prefix == "" {
		prefix = defaultSocialIDPrefix
	}

	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixNano(), hex.EncodeToString(b)), nil
}

// buildFollowOutboxEvent 构造关注/取关 outbox 事件，payload 使用 eventx.Envelope 包裹业务 FollowEvent。
func buildFollowOutboxEvent(eventID string, followerID uint64, followingID uint64, action string, occurredAt time.Time) (*model.OutboxEvent, error) {
	occurredAtMs := occurredAt.UnixMilli()
	payloadBytes, err := model.BuildFollowPayload(eventID, followerID, followingID, action, occurredAtMs)
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化关注事件失败")
	}

	eventType := eventx.EventTypeFollowCreated
	switch action {
	case eventx.FollowActionFollow:
		eventType = eventx.EventTypeFollowCreated
	case eventx.FollowActionUnfollow:
		eventType = eventx.EventTypeFollowDeleted
	default:
		return nil, status.Error(codes.InvalidArgument, "不支持的关注事件动作")
	}

	aggregateID := fmt.Sprintf("%d:%d", followerID, followingID)
	envelopeBytes, err := json.Marshal(eventx.Envelope{
		EventID:       eventID,
		EventType:     eventType,
		AggregateType: eventx.AggregateFollow,
		AggregateID:   aggregateID,
		Producer:      "social-rpc",
		OccurredAt:    occurredAtMs,
		Payload:       payloadBytes,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化 outbox 事件失败")
	}

	return &model.OutboxEvent{
		EventID:       eventID,
		Topic:         eventx.TopicFollowEvents,
		EventType:     eventType,
		AggregateType: eventx.AggregateFollow,
		AggregateID:   aggregateID,
		Payload:       string(envelopeBytes),
		Status:        model.OutboxStatusPending,
		CreatedAt:     occurredAt,
		UpdatedAt:     occurredAt,
	}, nil
}

// applyFollowCacheAfterCommit 只能在 MySQL 事务提交成功后调用。
// 它更新单条关注状态缓存，并删除双方统计缓存；调用方应只记录 Redis 错误，不回滚已提交业务结果。
func applyFollowCacheAfterCommit(ctx context.Context, svcCtx *svc.ServiceContext, followerID uint64, followingID uint64, followed bool) error {
	value := "0"
	if followed {
		value = "1"
	}

	pipe := svcCtx.RedisCli.TxPipeline()
	pipe.Set(ctx, rediskey.SocialFollowingStateKey(followerID, followingID), value, rediskey.SocialFollowingStateTTL)
	pipe.Del(ctx, rediskey.SocialFollowStatsKey(followerID), rediskey.SocialFollowStatsKey(followingID))
	_, err := pipe.Exec(ctx)
	return err
}
