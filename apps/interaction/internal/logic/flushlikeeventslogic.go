package logic

import (
	"context"
	"strconv"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	if leaseKey, leaseToken, acquired, err := l.acquireFlushLikeEventsLease(); err != nil {
		// Redis 不可用时仍可依赖 processed_events 唯一键和 MySQL 原子增量继续收敛。
		l.Errorf("acquire flush like events lease failed, fallback to db idempotency, error:%v", err)
	} else if !acquired {
		return nil, status.Error(codes.Aborted, "互动统计重建中，请稍后重试")
	} else {
		defer l.releaseFlushLikeEventsLease(leaseKey, leaseToken)
	}

	resp := &interaction.FlushLikeEventsResp{
		FailedEventIds: make([]string, 0),
	}
	acks := make([]interactionDeltaAck, 0, len(events))
	dbEvents := make([]interactionFlushDBEvent, 0, len(events))

	for index, event := range events {
		eventID, delta, err := validateLikeFlushEvent(event, index)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("invalid like event, event_id:%s error:%v", eventID, err)
			continue
		}

		dbEvents = append(dbEvents, interactionFlushDBEvent{EventID: eventID, VideoID: event.GetVideoId(), Delta: delta})
		// 即使 processed_events 已存在，也必须重试 Redis ack；上次可能在 DB
		// 提交后、确认增量前中断。
		acks = append(acks, interactionDeltaAck{
			EventID:         eventID,
			VideoID:         event.GetVideoId(),
			Delta:           delta,
			InvalidationKey: rediskey.LikeUserVideosListVersionKey(event.GetUserId()),
		})
	}

	if err := applyInteractionFlushBatch(
		l.ctx,
		l.svcCtx.GormDB,
		dbEvents,
		eventx.ConsumerLikeSync,
		eventx.TopicInteractionLikeEvents,
	); err != nil {
		l.Errorf("flush like event batch failed, size:%d error:%v", len(dbEvents), err)
		return nil, status.Error(codes.Internal, "批量刷新点赞统计失败")
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

func (l *FlushLikeEventsLogic) acquireFlushLikeEventsLease() (string, string, bool, error) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	return acquireInteractionStatsMutationLease(redisCtx, l.svcCtx.RedisCli)
}

func (l *FlushLikeEventsLogic) releaseFlushLikeEventsLease(leaseKey string, leaseToken string) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := releaseInteractionStatsMutationLease(redisCtx, l.svcCtx.RedisCli, leaseKey, leaseToken); err != nil {
		l.Errorf("release flush like events lease failed, key:%s error:%v", leaseKey, err)
	}
}

func failedEventID(index int, eventID string) string {
	if eventID != "" {
		return eventID
	}
	return "index:" + strconv.Itoa(index)
}
