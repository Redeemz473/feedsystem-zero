package logic

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

const (
	commentRateLimitTTL       = 3 * time.Second
	commentRedisOpTimeout     = 500 * time.Millisecond
	maxCommentUsernameRunes   = 64
	maxCommentContentRunes    = 500
	maxCommentRequestIDLength = 128
)

type PublishCommentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewPublishCommentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PublishCommentLogic {
	return &PublishCommentLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *PublishCommentLogic) PublishComment(in *interaction.PublishCommentReq) (*interaction.PublishCommentResp, error) {
	// 1. 校验 user_id、video_id、username、content。
	userID := in.GetUserId()
	if userID == 0 {
		return nil, status.Error(codes.Unauthenticated, "用户未登录")
	}
	videoID := in.GetVideoId()
	if videoID == 0 {
		return nil, status.Error(codes.InvalidArgument, "视频ID不能为空")
	}
	username := strings.TrimSpace(in.GetUsername())
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "用户名不能为空")
	}
	if len([]rune(username)) > maxCommentUsernameRunes {
		return nil, status.Error(codes.InvalidArgument, "用户名过长")
	}
	content := strings.TrimSpace(in.GetContent())
	if content == "" {
		return nil, status.Error(codes.InvalidArgument, "评论内容不能为空")
	}
	if len([]rune(content)) > maxCommentContentRunes {
		return nil, status.Errorf(codes.InvalidArgument, "评论内容不能超过%d个字符", maxCommentContentRunes)
	}

	requestID := strings.TrimSpace(in.GetRequestId())
	if requestID == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id不能为空")
	}
	if len(requestID) > maxCommentRequestIDLength {
		return nil, status.Error(codes.InvalidArgument, "request_id过长")
	}

	// 2. 如果前端/网关传了 request_id，先走幂等查询。
	if resp, ok, err := l.loadIdempotentCommentResp(userID, requestID); err != nil {
		l.Errorf("load idempotent comment failed, user_id: %d, request_id: %s, error: %v", userID, requestID, err)
		return nil, status.Error(codes.Internal, "幂等校验失败")
	} else if ok {
		return resp, nil
	}

	// 3. 校验视频是否存在且未删除。
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

	// 4. 对真正的新评论做短 TTL 限流。
	//    锁 token 化：SetNX 用随机 token 而非固定 "1"，释放时通过 Lua CAS 只删自己写入的那把锁。
	//    这样即便本次请求处理耗时超过 TTL、TTL 自动过期后被下一个请求 A 拿到，本次 defer 也不会误删 A 的锁。
	rateLimitKey := rediskey.CommentRateLimitKey(userID, videoID)
	rateLimitToken, err := randomHex(16)
	if err != nil {
		return nil, status.Error(codes.Internal, "生成评论限流锁失败")
	}
	locked, err := l.svcCtx.RedisCli.SetNX(l.ctx, rateLimitKey, rateLimitToken, commentRateLimitTTL).Result()
	if err != nil {
		l.Errorf("set comment rate limit failed, user_id: %d, video_id: %d, error: %v", userID, videoID, err)
		return nil, status.Error(codes.Internal, "评论限流校验失败")
	}
	if !locked {
		return nil, status.Error(codes.ResourceExhausted, "评论发布过于频繁，请稍后重试")
	}
	keepRateLimit := false
	defer func() {
		if keepRateLimit {
			return
		}
		redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
		defer cancel()
		// 使用与 like/unlike 相同的 CAS 释放逻辑，避免误删他人锁。
		if err := releaseRedisLock(redisCtx, l.svcCtx.RedisCli, rateLimitKey, rateLimitToken); err != nil {
			l.Errorf("release comment rate limit failed, key: %s, error: %v", rateLimitKey, err)
		}
	}()

	eventID, err := newEventID("publishComment")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成事件ID失败")
	}
	notificationEventID := ""
	if video.AuthorID != userID {
		notificationEventID, err = newEventID("notifyComment")
		if err != nil {
			return nil, status.Error(codes.Internal, "生成通知事件ID失败")
		}
	}

	// 5. MySQL 事务：评论、互动事件、领域 outbox 与通知 outbox 必须一起提交。
	now := time.Now()
	comment := model.Comment{
		VideoID:   videoID,
		UserID:    userID,
		Username:  username,
		Content:   content,
		RequestID: requestID,
		Status:    model.CommentStatusNormal,
	}
	if err := runInteractionWriteTransaction(l.ctx, l.svcCtx.GormDB, func(tx *gorm.DB) error {
		// 前一次失败尝试可能已经被 GORM 回填自增 ID；重试必须重新申请主键。
		comment.ID = 0
		comment.CreatedAt = time.Time{}
		comment.UpdatedAt = time.Time{}
		if err := tx.Create(&comment).Error; err != nil {
			return err
		}

		occurredAt := now.UnixMilli()
		commentEvent := eventx.CommentEvent{
			EventID:    eventID,
			RequestID:  requestID,
			CommentID:  comment.ID,
			VideoID:    videoID,
			UserID:     userID,
			Username:   username,
			Action:     eventx.CommentActionCreate,
			Delta:      1,
			OccurredAt: occurredAt,
		}
		payloadBytes, err := json.Marshal(commentEvent)
		if err != nil {
			return err
		}
		envelopeBytes, err := json.Marshal(eventx.Envelope{
			EventID:       eventID,
			EventType:     eventx.EventTypeCommentCreated,
			AggregateType: eventx.AggregateComment,
			AggregateID:   strconv.FormatUint(comment.ID, 10),
			Producer:      "interaction-rpc",
			OccurredAt:    occurredAt,
			Payload:       payloadBytes,
		})
		if err != nil {
			return err
		}

		if err := tx.Create(&model.InteractionEvent{
			EventID:   eventID,
			EventType: eventx.EventTypeCommentCreated,
			VideoID:   videoID,
			UserID:    userID,
			CommentID: comment.ID,
			Action:    eventx.CommentActionCreate,
			Delta:     1,
			RequestID: requestID,
			Payload:   string(payloadBytes),
			CreatedAt: now,
		}).Error; err != nil {
			return err
		}

		if err := tx.Create(&model.OutboxEvent{
			EventID:       eventID,
			Topic:         eventx.TopicInteractionCommentEvents,
			EventType:     eventx.EventTypeCommentCreated,
			AggregateType: eventx.AggregateComment,
			AggregateID:   strconv.FormatUint(comment.ID, 10),
			Payload:       string(envelopeBytes),
			Status:        model.OutboxStatusPending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}).Error; err != nil {
			return err
		}

		if notificationEventID != "" {
			notificationOutbox, err := buildInteractionNotificationOutbox(
				notificationEventID,
				eventID,
				video.AuthorID,
				userID,
				videoID,
				comment.ID,
				eventx.NotificationTypeVideoComment,
				eventx.NotificationActionCreate,
				now,
			)
			if err != nil {
				return err
			}
			return tx.Create(notificationOutbox).Error
		}
		return nil
	}); err != nil {
		if isDuplicateCommentRequest(err) {
			if resp, ok, loadErr := l.loadIdempotentCommentResp(userID, requestID); loadErr == nil && ok {
				return resp, nil
			}
		}
		l.Errorf("publish comment transaction failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		return nil, status.Error(codes.Internal, "发布评论失败")
	}
	keepRateLimit = true

	// 6. Redis 更新：评论列表版本、评论权威计数、热度增量、request_id 幂等缓存。
	// 评论发布直接原子叠加 Redis 权威 Hash，返回值就是用户可见的权威评论数。
	commentsCount := nonNegative(video.CommentsCount + 1)
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	authCommentsCount, err := applyRedisCommentCreatedState(redisCtx, l.svcCtx.RedisCli, videoID, userID, comment.ID, requestID, video)
	cancel()
	if err != nil {
		l.Errorf("apply redis comment created state failed, video_id: %d, user_id: %d, comment_id: %d, error: %v", videoID, userID, comment.ID, err)
	} else {
		commentsCount = authCommentsCount
	}
	commentInfo := &interaction.CommentInfo{
		CommentId: comment.ID,
		VideoId:   videoID,
		UserId:    userID,
		Username:  username,
		Content:   comment.Content,
		CreatedAt: comment.CreatedAt.UnixMilli(),
		UpdatedAt: comment.UpdatedAt.UnixMilli(),
		CanDelete: true,
	}
	return &interaction.PublishCommentResp{
		Msg:           "评论成功",
		Comment:       commentInfo,
		CommentsCount: commentsCount,
	}, nil
}

func (l *PublishCommentLogic) loadIdempotentCommentResp(userID uint64, requestID string) (*interaction.PublishCommentResp, bool, error) {
	commentID, ok, err := l.loadIdempotentCommentIDFromRedis(userID, requestID)
	if err != nil {
		l.Errorf("load idempotent comment id from redis failed, user_id: %d, request_id: %s, error: %v", userID, requestID, err)
		ok = false
	}

	var comment model.Comment
	query := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("user_id = ? AND request_id = ? AND status = ? AND deleted_at IS NULL", userID, requestID, model.CommentStatusNormal)
	if ok {
		query = query.Where("id = ?", commentID)
	}
	if err := query.First(&comment).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	var video model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("id = ? AND status = ? AND deleted_at IS NULL", comment.VideoID, model.VideoStatusNormal).
		First(&video).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return &interaction.PublishCommentResp{
		Msg: "评论成功",
		Comment: &interaction.CommentInfo{
			CommentId: comment.ID,
			VideoId:   comment.VideoID,
			UserId:    comment.UserID,
			Username:  comment.Username,
			Content:   comment.Content,
			CreatedAt: comment.CreatedAt.UnixMilli(),
			UpdatedAt: comment.UpdatedAt.UnixMilli(),
			CanDelete: true,
		},
		CommentsCount: realtimeCommentsCount(l.ctx, l.svcCtx.RedisCli, video),
	}, true, nil
}

func (l *PublishCommentLogic) loadIdempotentCommentIDFromRedis(userID uint64, requestID string) (uint64, bool, error) {
	value, err := l.svcCtx.RedisCli.Get(l.ctx, rediskey.CommentIdempotencyKey(userID, requestID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}

	commentID, err := strconv.ParseUint(value, 10, 64)
	if err != nil || commentID == 0 {
		return 0, false, nil
	}
	return commentID, true, nil
}

func isDuplicateCommentRequest(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
