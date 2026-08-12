package logic

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/eventx"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type DeleteCommentLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteCommentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteCommentLogic {
	return &DeleteCommentLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteCommentLogic) DeleteComment(in *interaction.DeleteCommentReq) (*interaction.DeleteCommentResp, error) {
	userID := in.GetUserId()
	if userID == 0 {
		return nil, status.Error(codes.Unauthenticated, "用户未登录")
	}

	commentID := in.GetCommentId()
	if commentID == 0 {
		return nil, status.Error(codes.InvalidArgument, "评论ID不能为空")
	}

	comment, video, err := l.loadCommentForDelete(commentID)
	if err != nil {
		return nil, err
	}

	// 评论作者或视频作者可以删除。
	if comment.UserID != userID && video.AuthorID != userID {
		return nil, status.Error(codes.PermissionDenied, "无权限删除该评论")
	}
	if comment.Status != model.CommentStatusNormal || comment.DeletedAt != nil {
		return &interaction.DeleteCommentResp{
			Msg:           "评论已删除",
			Deleted:       false,
			CommentsCount: realtimeCommentsCount(l.ctx, l.svcCtx.RedisCli, video),
		}, nil
	}

	eventID, err := newEventID("deleteComment")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成事件ID失败")
	}

	now := time.Now()
	var notificationOutbox *model.OutboxEvent
	// 删除动作的 actor 必须使用原评论作者，而不是当前执行删除的人。
	// 视频作者代删他人评论时，才能准确撤回原评论通知的 business_key。
	if video.AuthorID != comment.UserID {
		notificationEventID, err := newEventID("notifyDeleteComment")
		if err != nil {
			return nil, status.Error(codes.Internal, "生成通知事件ID失败")
		}
		notificationOutbox, err = buildInteractionNotificationOutbox(
			notificationEventID,
			eventID,
			video.AuthorID,
			comment.UserID,
			comment.VideoID,
			comment.ID,
			eventx.NotificationTypeVideoComment,
			eventx.NotificationActionDelete,
			now,
		)
		if err != nil {
			return nil, status.Error(codes.Internal, "构造删除评论通知事件失败")
		}
	}

	if err := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&model.Comment{}).
			Where("id = ? AND status = ? AND deleted_at IS NULL", comment.ID, model.CommentStatusNormal).
			Updates(map[string]any{
				"status":     model.CommentStatusDeleted,
				"deleted_at": now,
				"updated_at": now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			// 并发重复删除时回滚当前事务，外层按幂等语义返回“已删除”。
			return errNoActiveRecord
		}

		occurredAt := now.UnixMilli()
		commentEvent := eventx.CommentEvent{
			EventID:    eventID,
			RequestID:  comment.RequestID,
			CommentID:  comment.ID,
			VideoID:    comment.VideoID,
			UserID:     userID,
			Username:   comment.Username,
			Action:     eventx.CommentActionDelete,
			Delta:      -1,
			OccurredAt: occurredAt,
		}

		payloadBytes, err := json.Marshal(commentEvent)
		if err != nil {
			return err
		}

		envelopeBytes, err := json.Marshal(eventx.Envelope{
			EventID:       eventID,
			EventType:     eventx.EventTypeCommentDeleted,
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
			EventType: eventx.EventTypeCommentDeleted,
			VideoID:   comment.VideoID,
			UserID:    userID,
			CommentID: comment.ID,
			Action:    eventx.CommentActionDelete,
			Delta:     -1,
			RequestID: comment.RequestID,
			Payload:   string(payloadBytes),
			CreatedAt: now,
		}).Error; err != nil {
			return err
		}

		if err := tx.Create(&model.OutboxEvent{
			EventID:       eventID,
			Topic:         eventx.TopicInteractionCommentEvents,
			EventType:     eventx.EventTypeCommentDeleted,
			AggregateType: eventx.AggregateComment,
			AggregateID:   strconv.FormatUint(comment.ID, 10),
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
		if errors.Is(err, errNoActiveRecord) {
			return &interaction.DeleteCommentResp{
				Msg:           "评论已删除",
				Deleted:       false,
				CommentsCount: realtimeCommentsCount(l.ctx, l.svcCtx.RedisCli, video),
			}, nil
		}

		l.Errorf("delete comment transaction failed, comment_id: %d, user_id: %d, error: %v", commentID, userID, err)
		return nil, status.Error(codes.Internal, "删除评论失败")
	}

	commentsCount := nonNegative(video.CommentsCount - 1)
	redisCtx, cancel := context.WithTimeout(l.ctx, commentRedisOpTimeout)
	authCommentsCount, err := applyRedisCommentDeletedState(redisCtx, l.svcCtx.RedisCli, video.ID, userID, comment.RequestID, video)
	cancel()
	if err != nil {
		l.Errorf("apply redis comment deleted state failed, video_id: %d, user_id: %d, comment_id: %d, error: %v", video.ID, userID, comment.ID, err)
	} else {
		commentsCount = authCommentsCount
	}

	return &interaction.DeleteCommentResp{
		Msg:           "删除评论成功",
		Deleted:       true,
		CommentsCount: commentsCount,
	}, nil
}

func (l *DeleteCommentLogic) loadCommentForDelete(commentID uint64) (model.Comment, model.Video, error) {
	var comment model.Comment
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("id = ?", commentID).
		First(&comment).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Comment{}, model.Video{}, status.Error(codes.NotFound, "评论不存在")
		}
		l.Errorf("get comment failed, comment_id: %d, error: %v", commentID, err)
		return model.Comment{}, model.Video{}, status.Error(codes.Internal, "查询评论失败")
	}

	var video model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("id = ? AND status = ? AND deleted_at IS NULL", comment.VideoID, model.VideoStatusNormal).
		First(&video).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Comment{}, model.Video{}, status.Error(codes.NotFound, "视频不存在或已删除")
		}
		l.Errorf("get video failed, video_id: %d, error: %v", comment.VideoID, err)
		return model.Comment{}, model.Video{}, status.Error(codes.Internal, "查询视频失败")
	}

	return comment, video, nil
}
