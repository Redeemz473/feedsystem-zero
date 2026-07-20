package logic

import (
	"context"
	"errors"
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

	if running, err := l.statsRebuildRunning(); err != nil {
		l.Errorf("check rebuild stats lock failed before flushing comment events, error:%v", err)
	} else if running {
		return nil, status.Error(codes.Aborted, "互动统计重建中，请稍后重试")
	}

	//拿锁
	//Redis 分布式锁限制同一时间仅一个实例执行 DB 更新
	if lockKey, lockToken, locked, err := l.acquireFlushCommentEventsLock(); err != nil {
		// Redis 锁只是削峰保护；真正的跨实例幂等由 processed_events 唯一键保证。所以redis挂了可以继续跑
		l.Errorf("acquire flush comment events lock failed, fallback to db idempotency, error:%v", err)
	} else if !locked {
		return nil, status.Error(codes.Aborted, "评论事件任务正在处理中")
	} else {
		defer l.releaseFlushCommentEventsLock(lockKey, lockToken)
	}

	resp := &interaction.FlushCommentEventsResp{
		FailedEventIds: make([]string, 0),
	}
	flushedDeltas := make(map[uint64]videoStatDelta)

	for index, event := range events {
		eventID, delta, err := validateCommentFlushEvent(event, index)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("invalid comment event, event_id:%s error:%v", eventID, err)
			continue
		}

		applied, err := l.applyCommentFlushEvent(event, delta)
		if err != nil {
			resp.FailedEventIds = append(resp.FailedEventIds, eventID)
			l.Errorf("flush comment event failed, event_id:%s video_id:%d comment_id:%d error:%v", eventID, event.GetVideoId(), event.GetCommentId(), err)
			continue
		}
		if !applied {
			continue
		}

		resp.SuccessCount++
		mergeVideoStatDelta(flushedDeltas, event.GetVideoId(), delta)
	}

	l.refreshRedisAfterCommentFlush(flushedDeltas)
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

// DB 事务执行幂等写入 + 视频统计更新
func (l *FlushCommentEventsLogic) applyCommentFlushEvent(event *interaction.CommentEvent, delta videoStatDelta) (bool, error) {
	now := time.Now()
	applied := false

	err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		inserted, err := insertProcessedEvent(
			l.ctx,
			tx,
			event.GetEventId(),
			eventx.ConsumerCommentSync,
			eventx.TopicInteractionCommentEvents,
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
				// 视频已被删除时，这条历史评论统计已经没有用户可见价值。
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

func (l *FlushCommentEventsLogic) refreshRedisAfterCommentFlush(flushedDeltas map[uint64]videoStatDelta) {
	if len(flushedDeltas) == 0 {
		return
	}

	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()

	if err := subtractFlushedVideoDeltas(redisCtx, l.svcCtx.RedisCli, flushedDeltas); err != nil {
		l.Errorf("subtract flushed comment deltas failed, error:%v", err)
	}

	pipe := l.svcCtx.RedisCli.Pipeline()
	for _, videoID := range uniqueVideoIDsFromDeltas(flushedDeltas) {
		pipe.Del(redisCtx, rediskey.VideoStatsCacheKey(videoID))
		// 如果前台评论写 Redis 失败，job 至少能推进评论列表版本。
		queueBumpCommentListVersion(redisCtx, pipe, videoID)
	}
	if _, err := pipe.Exec(redisCtx); err != nil {
		l.Errorf("refresh redis after comment flush failed, error:%v", err)
	}
}

func (l *FlushCommentEventsLogic) acquireFlushCommentEventsLock() (string, string, bool, error) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	return tryAcquireInteractionJobLock(redisCtx, l.svcCtx.RedisCli, interactionFlushCommentEventsJob)
}

func (l *FlushCommentEventsLogic) releaseFlushCommentEventsLock(lockKey string, lockToken string) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
		l.Errorf("release flush comment events lock failed, key:%s error:%v", lockKey, err)
	}
}

// 检查rebuild重建是否在运行，redis 存在重建标记 Key 时，直接终止本次同步，等待重建完成再执行
func (l *FlushCommentEventsLogic) statsRebuildRunning() (bool, error) {
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	defer cancel()
	return isStatsRebuildRunning(redisCtx, l.svcCtx.RedisCli)
}
