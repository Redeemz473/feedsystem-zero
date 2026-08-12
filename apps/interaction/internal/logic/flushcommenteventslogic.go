package logic

import (
	"context"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlushCommentEventsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFlushCommentEventsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FlushCommentEventsLogic {
	return &FlushCommentEventsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// FlushCommentEvents 消费 Kafka 后把评论事件累加到 MySQL videos 冷备字段。
//
// 方案 B 架构下 Redis VideoStatsAuthKey 是用户可见值的权威，此 RPC 仅负责冷备维护，
// 不再申请 mutation lease、不再 ack Redis 增量。processed_events 唯一键保证幂等。
func (l *FlushCommentEventsLogic) FlushCommentEvents(in *interaction.FlushCommentEventsReq) (*interaction.FlushCommentEventsResp, error) {
	events := in.GetEvents()
	if err := validateInternalEventBatchSize(len(events)); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return &interaction.FlushCommentEventsResp{}, nil
	}

	resp := &interaction.FlushCommentEventsResp{
		FailedEventIds: make([]string, 0),
	}
	dbEvents := make([]interactionFlushDBEvent, 0, len(events))

	for index, event := range events {
		eventID, delta, err := validateCommentFlushEvent(event, index)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("invalid comment event, event_id:%s error:%v", eventID, err)
			continue
		}

		dbEvents = append(dbEvents, interactionFlushDBEvent{EventID: eventID, VideoID: event.GetVideoId(), Delta: delta})
	}

	if err := applyInteractionFlushBatch(
		l.ctx,
		l.svcCtx.GormDB,
		dbEvents,
		eventx.ConsumerCommentSync,
		eventx.TopicInteractionCommentEvents,
	); err != nil {
		l.Errorf("flush comment event batch failed, size:%d error:%v", len(dbEvents), err)
		return nil, status.Error(codes.Internal, "批量刷新评论冷备失败")
	}

	resp.SuccessCount = int64(len(dbEvents))
	return resp, nil
}

// 校验事件字段、区分创建 / 删除动作，输出对应增减 delta
func validateCommentFlushEvent(event *interaction.CommentEvent, index int) (string, videoStatDelta, error) {
	if event == nil {
		return failedEventID(index, ""), videoStatDelta{}, status.Error(codes.InvalidArgument, "评论事件不能为空")
	}

	eventID := event.GetEventId()
	if eventID == "" {
		return failedEventID(index, eventID), videoStatDelta{}, status.Error(codes.InvalidArgument, "event_id不能为空")
	}
	if event.GetVideoId() == 0 {
		return eventID, videoStatDelta{}, status.Error(codes.InvalidArgument, "video_id不能为空")
	}
	if event.GetCommentId() == 0 {
		return eventID, videoStatDelta{}, status.Error(codes.InvalidArgument, "comment_id不能为空")
	}

	switch event.GetAction() {
	case interaction.CommentAction_COMMENT_ACTION_CREATE:
		return eventID, videoStatDelta{CommentDelta: 1, PopularityDelta: commentPopularityWeight}, nil
	case interaction.CommentAction_COMMENT_ACTION_DELETE:
		return eventID, videoStatDelta{CommentDelta: -1, PopularityDelta: -commentPopularityWeight}, nil
	default:
		return eventID, videoStatDelta{}, status.Error(codes.InvalidArgument, "未知评论事件类型")
	}
}
