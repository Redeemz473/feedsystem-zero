package logic

import (
	"context"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

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

// 给Kafka consumer调用的
// 在线路径不会更新videos表，事件投递到Kafka后用这个rpc来消费
func (l *FlushCommentEventsLogic) FlushCommentEvents(in *interaction.FlushCommentEventsReq) (*interaction.FlushCommentEventsResp, error) {
	events := in.GetEvents()
	if err := validateInternalEventBatchSize(len(events)); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return &interaction.FlushCommentEventsResp{}, nil
	}

	if leaseKey, leaseToken, acquired, err := l.acquireFlushCommentEventsLease(); err != nil {
		// Redis 不可用时仍可依赖 processed_events 唯一键和 MySQL 原子增量继续收敛。
		l.Errorf("acquire flush comment events lease failed, fallback to db idempotency, error:%v", err)
	} else if !acquired {
		return nil, status.Error(codes.Aborted, "互动统计重建中，请稍后重试")
	} else {
		defer l.releaseFlushCommentEventsLease(leaseKey, leaseToken)
	}

	resp := &interaction.FlushCommentEventsResp{
		FailedEventIds: make([]string, 0),
	}
	acks := make([]interactionDeltaAck, 0, len(events))
	dbEvents := make([]interactionFlushDBEvent, 0, len(events))

	for index, event := range events {
		eventID, delta, err := validateCommentFlushEvent(event, index)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("invalid comment event, event_id:%s error:%v", eventID, err)
			continue
		}

		dbEvents = append(dbEvents, interactionFlushDBEvent{EventID: eventID, VideoID: event.GetVideoId(), Delta: delta})
		// 重复事件仍要执行 Redis ack，修复“DB 已提交但 Redis 扣减失败”的中断窗口。
		acks = append(acks, interactionDeltaAck{
			EventID:         eventID,
			VideoID:         event.GetVideoId(),
			Delta:           delta,
			InvalidationKey: rediskey.CommentListVersionKey(event.GetVideoId()),
		})
	}

	if err := applyInteractionFlushBatch(
		l.ctx,
		l.svcCtx.GormDB,
		dbEvents,
		eventx.ConsumerCommentSync,
		eventx.TopicInteractionCommentEvents,
	); err != nil {
		l.Errorf("flush comment event batch failed, size:%d error:%v", len(dbEvents), err)
		return nil, status.Error(codes.Internal, "批量刷新评论统计失败")
	}

	l.ackCommentEventDeltas(resp, acks)
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

func (l *FlushCommentEventsLogic) ackCommentEventDeltas(resp *interaction.FlushCommentEventsResp, acks []interactionDeltaAck) {
	if len(acks) == 0 {
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, interactionDeltaAckTimeout)
	defer cancel()

	failed := acknowledgeInteractionDeltas(redisCtx, l.svcCtx.RedisCli, acks)
	for _, ack := range acks {
		if err, ok := failed[ack.EventID]; ok {
			resp.FailedEventIds = append(resp.FailedEventIds, ack.EventID)
			l.Errorf("ack redis comment delta failed, event_id:%s video_id:%d error:%v", ack.EventID, ack.VideoID, err)
			continue
		}
		resp.SuccessCount++
	}
}

func (l *FlushCommentEventsLogic) acquireFlushCommentEventsLease() (string, string, bool, error) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	return acquireInteractionStatsMutationLease(redisCtx, l.svcCtx.RedisCli)
}

func (l *FlushCommentEventsLogic) releaseFlushCommentEventsLease(leaseKey string, leaseToken string) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := releaseInteractionStatsMutationLease(redisCtx, l.svcCtx.RedisCli, leaseKey, leaseToken); err != nil {
		l.Errorf("release flush comment events lease failed, key:%s error:%v", leaseKey, err)
	}
}
