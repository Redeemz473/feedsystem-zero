package logic

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type UnlikeVideoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnlikeVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnlikeVideoLogic {
	return &UnlikeVideoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

var errNoActiveRecord = errors.New("no active record")

func (l *UnlikeVideoLogic) UnlikeVideo(in *interaction.UnlikeVideoReq) (*interaction.UnlikeVideoResp, error) {
	// 校验 user_id、video_id 都不能为 0。
	userID := in.GetUserId()
	if userID == 0 {
		return nil, status.Error(codes.Unauthenticated, "用户未登录")
	}
	videoID := in.GetVideoId()
	if videoID == 0 {
		return nil, status.Error(codes.InvalidArgument, "视频ID不能为空")
	}
	var video model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("id = ? AND status = ? AND deleted_at IS NULL", videoID, model.VideoStatusNormal).
		First(&video).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "视频不存在或已删除")
		}
		l.Errorf("get video failed, video_id: %d, error: %v", videoID, err)
		return nil, status.Error(codes.Internal, "查询视频失败")
	}

	// 使用 rediskey.LikeActionLockKey(video_id,user_id) 加短 TTL 锁，和 LikeVideo 共用同一把锁。
	lockKey := rediskey.LikeActionLockKey(videoID, userID)
	lockToken, err := randomHex(16)
	if err != nil {
		return nil, status.Error(codes.Internal, "生成点击锁失败")
	}
	locked, err := l.svcCtx.RedisCli.SetNX(l.ctx, lockKey, lockToken, likeActionLockTTL).Result()
	if err != nil {
		return nil, status.Error(codes.Internal, "获取点击锁失败")
	}
	if !locked {
		return nil, status.Error(codes.Aborted, "操作过于频繁，请稍后重试")
	}
	defer func() {
		if err := releaseRedisLock(l.ctx, l.svcCtx.RedisCli, lockKey, lockToken); err != nil {
			l.Errorf("release like action lock failed, key: %s, error: %v", lockKey, err)
		}
	}()
	// 先查 Redis LikeStateKey：
	liked, hit, err := loadLikeStateFromRedis(l.ctx, l.svcCtx.RedisCli, videoID, userID)
	if err != nil {
		l.Errorf("get like state from redis failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		return nil, status.Error(codes.Internal, "查询点赞状态失败")
	}
	//    - 如果已经是 0，说明已取消点赞，直接幂等返回 liked=false。
	if hit && !liked {
		return &interaction.UnlikeVideoResp{
			Msg:        "已取消点赞",
			Liked:      false,
			LikesCount: realtimeLikesCount(l.ctx, l.svcCtx.RedisCli, video),
		}, nil
	}
	//    - 如果未命中，再查 MySQL likes 表中 status=1 的记录兜底。
	if !hit {
		dbLiked, err := loadLikeStateFromDB(l.ctx, l.svcCtx.GormDB, videoID, userID)
		if err != nil {
			l.Errorf("get like state from db failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
			return nil, status.Error(codes.Internal, "查询点赞状态失败")
		}
		// MySQL 也没有有效点赞记录，把 Redis 状态补齐后幂等返回。
		if !dbLiked {
			if err := fillUnlikedState(l.ctx, l.svcCtx.RedisCli, videoID, userID); err != nil {
				l.Errorf("fill redis unliked state failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
			}
			return &interaction.UnlikeVideoResp{
				Msg:        "已取消点赞",
				Liked:      false,
				LikesCount: realtimeLikesCount(l.ctx, l.svcCtx.RedisCli, video),
			}, nil
		}
	}

	// Redis 命中 liked=true 或 MySQL 确认已点赞时，继续执行取消点赞。
	fallbackLikesCount := nonNegative(realtimeLikesCount(l.ctx, l.svcCtx.RedisCli, video) - 1)

	//创建并封装事件
	eventID, err := newEventID("unlike")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成事件ID失败")
	}
	now := time.Now()
	occurredAt := now.UnixMilli()
	aggregateID := likeAggregateID(videoID, userID)

	unlikeEvent := eventx.LikeEvent{
		EventID:    eventID,
		VideoID:    videoID,
		UserID:     userID,
		Action:     eventx.LikeActionUnlike,
		Delta:      -1,
		OccurredAt: occurredAt,
	}

	payloadBytes, err := json.Marshal(unlikeEvent)
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化取消点赞事件失败")
	}
	envelopeBytes, err := json.Marshal(eventx.Envelope{
		EventID:       eventID,
		EventType:     eventx.EventTypeLikeDeleted,
		AggregateType: eventx.AggregateLike,
		AggregateID:   aggregateID,
		Producer:      "interaction-rpc",
		OccurredAt:    occurredAt,
		Payload:       payloadBytes,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化 outbox 事件失败")
	}

	var notificationOutbox *model.OutboxEvent
	if video.AuthorID != userID {
		notificationEventID, err := newEventID("notifyUnlike")
		if err != nil {
			return nil, status.Error(codes.Internal, "生成通知事件ID失败")
		}
		notificationOutbox, err = buildInteractionNotificationOutbox(
			notificationEventID,
			eventID,
			video.AuthorID,
			userID,
			videoID,
			0,
			eventx.NotificationTypeVideoLike,
			eventx.NotificationActionDelete,
			now,
		)
		if err != nil {
			return nil, status.Error(codes.Internal, "构造取消点赞通知事件失败")
		}
	}

	// MySQL 事务：软删除点赞关系，并写 interaction、领域 outbox 和通知 outbox。
	if err := runInteractionWriteTransaction(l.ctx, l.svcCtx.GormDB, func(tx *gorm.DB) error {
		result := tx.Model(&model.Like{}).
			Where(
				"video_id = ? AND user_id = ? AND status = ? AND deleted_at IS NULL",
				videoID,
				userID,
				model.LikeStatusActive,
			).
			Updates(map[string]any{
				"status":     model.LikeStatusDeleted,
				"deleted_at": now,
				"updated_at": now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return errNoActiveRecord
		}

		if err := tx.Create(&model.InteractionEvent{
			EventID:   eventID,
			EventType: eventx.EventTypeLikeDeleted,
			VideoID:   videoID,
			UserID:    userID,
			Action:    eventx.LikeActionUnlike,
			Delta:     -1,
			Payload:   string(payloadBytes),
			CreatedAt: now,
		}).Error; err != nil {
			return err
		}

		if err := tx.Create(&model.OutboxEvent{
			EventID:       eventID,
			Topic:         eventx.TopicInteractionLikeEvents,
			EventType:     eventx.EventTypeLikeDeleted,
			AggregateType: eventx.AggregateLike,
			AggregateID:   aggregateID,
			Payload:       string(envelopeBytes),
			Status:        model.OutboxStatusPending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}).Error; err != nil {
			return err
		}
		if notificationOutbox != nil {
			attemptNotification := *notificationOutbox
			attemptNotification.ID = 0
			return tx.Create(&attemptNotification).Error
		}
		return nil
	}); err != nil {
		if errors.Is(err, errNoActiveRecord) {
			if err := fillUnlikedState(l.ctx, l.svcCtx.RedisCli, videoID, userID); err != nil {
				l.Errorf("fill redis unliked state failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
			}
			return &interaction.UnlikeVideoResp{
				Msg:        "已取消点赞",
				Liked:      false,
				LikesCount: realtimeLikesCount(l.ctx, l.svcCtx.RedisCli, video),
			}, nil
		}
		l.Errorf("unlike video transaction failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		return nil, status.Error(codes.Internal, "取消点赞失败")
	}

	// MySQL 已经成功后，再更新 Redis 权威计数和实时状态。若写入失败，用 fallback 预估值。
	likesCount, err := applyRedisUnlikeState(l.ctx, l.svcCtx.RedisCli, videoID, userID, video)
	if err != nil {
		l.Errorf("apply redis unlike state failed after mysql committed, video_id: %d, user_id: %d, fallback_likes_count: %d, error: %v", videoID, userID, fallbackLikesCount, err)
		likesCount = fallbackLikesCount
	}

	return &interaction.UnlikeVideoResp{
		Msg:        "取消点赞成功",
		Liked:      false,
		LikesCount: likesCount,
	}, nil
}
