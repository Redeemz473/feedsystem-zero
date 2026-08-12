package logic

import (
	"context"
	"strconv"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"

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

// FlushLikeEvents 消费 Kafka 后更新 MySQL 持久统计，并把版本化快照投影到 Redis。
// processed_events 保证 DB 增量幂等；Redis 投影失败时 RPC 返回失败，Kafka 重放会跳过 DB 增量，
// 但仍重新投影最新快照，实现失写自动恢复。
func (l *FlushLikeEventsLogic) FlushLikeEvents(in *interaction.FlushLikeEventsReq) (*interaction.FlushLikeEventsResp, error) {
	events := in.GetEvents()
	if err := validateInternalEventBatchSize(len(events)); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return &interaction.FlushLikeEventsResp{}, nil
	}

	resp := &interaction.FlushLikeEventsResp{
		FailedEventIds: make([]string, 0),
	}
	dbEvents := make([]interactionFlushDBEvent, 0, len(events))

	for index, event := range events {
		eventID, delta, err := validateLikeFlushEvent(event, index)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("invalid like event, event_id:%s error:%v", eventID, err)
			continue
		}

		dbEvents = append(dbEvents, interactionFlushDBEvent{EventID: eventID, VideoID: event.GetVideoId(), Delta: delta})
	}

	projections, err := applyInteractionFlushBatch(
		l.ctx,
		l.svcCtx.GormDB,
		dbEvents,
		eventx.ConsumerLikeSync,
		eventx.TopicInteractionLikeEvents,
	)
	if err != nil {
		l.Errorf("flush like event batch failed, size:%d error:%v", len(dbEvents), err)
		return nil, status.Error(codes.Internal, "批量刷新点赞统计失败")
	}
	if err := projectVideoStatsBatch(l.ctx, l.svcCtx.RedisCli, projections); err != nil {
		l.Errorf("project like stats to redis failed, videos:%d error:%v", len(projections), err)
		return nil, status.Error(codes.Internal, "投影点赞统计失败")
	}

	resp.SuccessCount = int64(len(dbEvents))
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

func failedEventID(index int, eventID string) string {
	if eventID != "" {
		return eventID
	}
	return "index:" + strconv.Itoa(index)
}
