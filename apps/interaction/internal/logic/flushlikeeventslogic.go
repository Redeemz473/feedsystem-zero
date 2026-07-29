package logic

import (
	"context"
	"errors"
	"strconv"
	"time"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type FlushLikeEventsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFlushLikeEventsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FlushLikeEventsLogic {
	return &FlushLikeEventsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *FlushLikeEventsLogic) FlushLikeEvents(in *interaction.FlushLikeEventsReq) (*interaction.FlushLikeEventsResp, error) {
	events := in.GetEvents()
	if err := validateInternalEventBatchSize(len(events)); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return &interaction.FlushLikeEventsResp{}, nil
	}

	if running, err := l.statsRebuildRunning(); err != nil {
		l.Errorf("check rebuild stats lock failed before flushing like events, error:%v", err)
	} else if running {
		return nil, status.Error(codes.Aborted, "互动统计重建中，请稍后重试")
	}

	if lockKey, lockToken, locked, err := l.acquireFlushLikeEventsLock(); err != nil {
		// Redis 锁只是削峰保护；真正的跨实例幂等由 processed_events 唯一键保证。
		l.Errorf("acquire flush like events lock failed, fallback to db idempotency, error:%v", err)
	} else if !locked {
		return nil, status.Error(codes.Aborted, "点赞事件任务正在处理中")
	} else {
		defer l.releaseFlushLikeEventsLock(lockKey, lockToken)
	}

	resp := &interaction.FlushLikeEventsResp{
		FailedEventIds: make([]string, 0),
	}
	acks := make([]interactionDeltaAck, 0, len(events))

	for index, event := range events {
		eventID, delta, err := validateLikeFlushEvent(event, index)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("invalid like event, event_id:%s error:%v", eventID, err)
			continue
		}

		_, err = l.applyLikeFlushEvent(event, delta)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("flush like event failed, event_id:%s video_id:%d user_id:%d error:%v", eventID, event.GetVideoId(), event.GetUserId(), err)
			continue
		}
		// 即使 processed_events 已存在，也必须重试 Redis ack；上次可能在 DB
		// 提交后、确认增量前中断。
		acks = append(acks, interactionDeltaAck{
			EventID:         eventID,
			VideoID:         event.GetVideoId(),
			Delta:           delta,
			InvalidationKey: rediskey.LikeUserVideosListVersionKey(event.GetUserId()),
		})
	}

	l.ackLikeEventDeltas(resp, acks)
	return resp, nil
}

func validateLikeFlushEvent(event *interaction.LikeEvent, index int) (string, videoStatDelta, error) {
	if event == nil {
		return failedEventID(index, ""), videoStatDelta{}, status.Error(codes.InvalidArgument, "点赞事件不能为空")
	}

	eventID := event.GetEventId()
	if eventID == "" {
		return failedEventID(index, eventID), videoStatDelta{}, status.Error(codes.InvalidArgument, "event_id不能为空")
	}
	if event.GetVideoId() == 0 {
		return eventID, videoStatDelta{}, status.Error(codes.InvalidArgument, "video_id不能为空")
	}
	if event.GetUserId() == 0 {
		return eventID, videoStatDelta{}, status.Error(codes.InvalidArgument, "user_id不能为空")
	}

	switch event.GetAction() {
	case interaction.LikeAction_LIKE_ACTION_LIKE:
		return eventID, videoStatDelta{LikeDelta: 1, PopularityDelta: likePopularityWeight}, nil
	case interaction.LikeAction_LIKE_ACTION_UNLIKE:
		return eventID, videoStatDelta{LikeDelta: -1, PopularityDelta: -likePopularityWeight}, nil
	default:
		return eventID, videoStatDelta{}, status.Error(codes.InvalidArgument, "未知点赞事件类型")
	}
}

func (l *FlushLikeEventsLogic) applyLikeFlushEvent(event *interaction.LikeEvent, delta videoStatDelta) (bool, error) {
	now := time.Now()
	applied := false

	err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		inserted, err := insertProcessedEvent(
			l.ctx,
			tx,
			event.GetEventId(),
			eventx.ConsumerLikeSync,
			eventx.TopicInteractionLikeEvents,
			now,
		)
		if err != nil {
			return err
		}
		if !inserted {
			// 重复消费时直接跳过，不再更新聚合计数。
			return nil
		}

		if err := applyVideoStatDelta(l.ctx, tx, event.GetVideoId(), delta); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 视频已被删除时，这条历史互动统计已经没有用户可见价值。
				// processed_events 仍然提交，避免 Kafka 对同一条无意义事件无限重试。
				return nil
			}
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func (l *FlushLikeEventsLogic) ackLikeEventDeltas(resp *interaction.FlushLikeEventsResp, acks []interactionDeltaAck) {
	if len(acks) == 0 {
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, interactionDeltaAckTimeout)
	defer cancel()

	failed := acknowledgeInteractionDeltas(redisCtx, l.svcCtx.RedisCli, acks)
	for _, ack := range acks {
		if err, ok := failed[ack.EventID]; ok {
			resp.FailedEventIds = append(resp.FailedEventIds, ack.EventID)
			l.Errorf("ack redis like delta failed, event_id:%s video_id:%d error:%v", ack.EventID, ack.VideoID, err)
			continue
		}
		resp.SuccessCount++
	}
}

func (l *FlushLikeEventsLogic) acquireFlushLikeEventsLock() (string, string, bool, error) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	return tryAcquireInteractionJobLock(redisCtx, l.svcCtx.RedisCli, interactionFlushLikeEventsJob)
}

func (l *FlushLikeEventsLogic) releaseFlushLikeEventsLock(lockKey string, lockToken string) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release flush like events lock failed, key:%s error:%v", lockKey, err)
	}
}

func (l *FlushLikeEventsLogic) statsRebuildRunning() (bool, error) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	return isStatsRebuildRunning(redisCtx, l.svcCtx.RedisCli)
}

func failedEventID(index int, eventID string) string {
	if eventID != "" {
		return eventID
	}
	return "index:" + strconv.Itoa(index)
}
