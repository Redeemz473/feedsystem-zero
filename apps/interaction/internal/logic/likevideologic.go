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
	"gorm.io/gorm/clause"
)

type LikeVideoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLikeVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LikeVideoLogic {
	return &LikeVideoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LikeVideoLogic) LikeVideo(in *interaction.LikeVideoReq) (*interaction.LikeVideoResp, error) {
	// 校验 user_id、video_id 都不能为 0；确认视频存在且未删除。
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

	// 使用 rediskey.LikeActionLockKey(video_id,user_id) 加短 TTL 锁，避免用户连续点击导致并发写状态。
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
	//    - 如果已经是 1，说明已点赞，直接幂等返回 liked=true。
	if hit && liked {
		return &interaction.LikeVideoResp{
			Msg:        "已点赞",
			Liked:      true,
			LikesCount: realtimeLikesCount(l.ctx, l.svcCtx.RedisCli, video),
		}, nil
	}

	//    - 如果未命中，再查 MySQL likes 表中 status=1 的记录兜底。
	if !hit || !liked {
		dbLiked, err := loadLikeStateFromDB(l.ctx, l.svcCtx.GormDB, videoID, userID)
		if err != nil {
			l.Errorf("get like state from db failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
			return nil, status.Error(codes.Internal, "查询点赞状态失败")
		}
		if dbLiked {
			if err := fillLikedState(l.ctx, l.svcCtx.RedisCli, videoID, userID); err != nil {
				l.Errorf("fill redis liked state failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
			}
			return &interaction.LikeVideoResp{
				Msg:        "已点赞",
				Liked:      true,
				LikesCount: realtimeLikesCount(l.ctx, l.svcCtx.RedisCli, video),
			}, nil
		}
	}

	// Redis 命中 liked=false 或 Redis/MySQL 都未点赞时，继续执行点赞。
	// fallbackLikesCount 仅在 Redis 权威写入失败时作兼容作为展示值：基于当前权威值预估一个"+1"后的合理数。
	fallbackLikesCount := nonNegative(realtimeLikesCount(l.ctx, l.svcCtx.RedisCli, video) + 1) //作为一个保护，防止redis挂了但是mysql写入了的一个保护

	//创建点赞事件并封装
	eventID, err := newEventID("like")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成事件ID失败")
	}
	now := time.Now()
	occurredAt := now.UnixMilli()
	aggregateID := likeAggregateID(videoID, userID)
	//构造业务层点赞事件
	likeEvent := eventx.LikeEvent{
		EventID:    eventID,
		VideoID:    videoID,
		UserID:     userID,
		Action:     eventx.LikeActionLike,
		Delta:      1,
		OccurredAt: occurredAt,
	}

	payloadBytes, err := json.Marshal(likeEvent)
	if err != nil {
		return nil, status.Error(codes.Internal, "序列化点赞事件失败")
	}
	//封装
	envelopeBytes, err := json.Marshal(eventx.Envelope{
		EventID:       eventID,
		EventType:     eventx.EventTypeLikeCreated,
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
		notificationEventID, err := newEventID("notifyLike")
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
			eventx.NotificationActionCreate,
			now,
		)
		if err != nil {
			return nil, status.Error(codes.Internal, "构造点赞通知事件失败")
		}
	}

	//MySQL 事务：点赞关系、互动事件、领域 outbox 与通知 outbox 必须一起提交。
	//开启事务
	if err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		like := model.Like{
			VideoID: videoID,
			UserID:  userID,
			Status:  model.LikeStatusActive,
		}
		//Columns 指定冲突唯一键：video_id + user_id
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "video_id"},
				{Name: "user_id"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"status":     model.LikeStatusActive,
				"deleted_at": nil,
				"updated_at": now,
			}),
		}).Create(&like).Error; err != nil {
			return err
		}

		//插入交互事件表，方便事件溯源
		if err := tx.Create(&model.InteractionEvent{
			EventID:   eventID,
			EventType: eventx.EventTypeLikeCreated,
			VideoID:   videoID,
			UserID:    userID,
			Action:    eventx.LikeActionLike,
			Delta:     1,
			Payload:   string(payloadBytes),
			CreatedAt: now,
		}).Error; err != nil {
			return err
		}

		// 插入领域 Outbox，供 interaction-sync/hotrank 消费。
		if err := tx.Create(&model.OutboxEvent{
			EventID:       eventID,
			Topic:         eventx.TopicInteractionLikeEvents,
			EventType:     eventx.EventTypeLikeCreated,
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
			return tx.Create(notificationOutbox).Error
		}
		return nil
	}); err != nil {
		l.Errorf("like video transaction failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		return nil, status.Error(codes.Internal, "点赞失败")
	}

	// 更新 Redis 权威计数和实时状态。若写入成功，直接返回权威值；
	// 若权威 Hash 写入失败，MySQL 已提交 + Kafka 事件将保证终一致，直接返回预估值。
	likesCount, err := applyRedisLikeState(l.ctx, l.svcCtx.RedisCli, videoID, userID, video)
	if err != nil {
		l.Errorf("apply redis like state failed after mysql committed, video_id: %d, user_id: %d, fallback_likes_count: %d, error: %v", videoID, userID, fallbackLikesCount, err)
		likesCount = fallbackLikesCount
	}

	return &interaction.LikeVideoResp{
		Msg:        "点赞成功",
		Liked:      true,
		LikesCount: likesCount,
	}, nil
}
