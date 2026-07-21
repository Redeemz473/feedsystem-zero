package logic

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"feedsystem-zero/apps/video/internal/model"
	"feedsystem-zero/apps/video/internal/svc"
	videopb "feedsystem-zero/apps/video/video"
	"feedsystem-zero/common/eventx"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DeleteVideoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteVideoLogic {
	return &DeleteVideoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 删除视频
//
// 事务边界（P0 + P4 一致性重构）：
//  1. 事务内 SELECT ... FOR UPDATE 拿到视频，校验作者身份；
//  2. 事务内 UPDATE videos 软删；
//  3. 事务内对 play_url / cover_url 各调一次 decreaseFileAssetRefInTx，
//     引用降到 0 时只置 PendingDelete，物理删除交给独立 cleanup job；
//  4. 事务内 INSERT outbox_events(video.deleted)，让下游 feed / 推荐 / 通知等异步系统能感知；
//  5. 事务任一步失败 → 全部回滚，不会出现"视频软删了但 ref_count 没减"或"事件已发但视频没删"的漂移。
func (l *DeleteVideoLogic) DeleteVideo(in *videopb.DeleteVideoReq) (*videopb.DeleteVideoResp, error) {
	videoID := in.GetVideoId()
	operatorID := in.GetOperatorId()

	if videoID == 0 {
		return nil, status.Error(codes.InvalidArgument, "视频ID不能为空")
	}
	if operatorID == 0 {
		return nil, status.Error(codes.Unauthenticated, "未登录")
	}

	// 预生成事件 ID 和 envelope，事务里只做插入。
	eventID, err := newEventID("video_deleted")
	if err != nil {
		return nil, status.Error(codes.Internal, "生成事件ID失败")
	}

	// 事务外先声明业务错误，事务里根据业务规则填充，事务返回错误时优先透出业务错误码。
	var bizErr error

	// 事务内使用的中间变量，用于事务提交后组装事件 payload。
	var (
		authorID uint64
		playURL  string
		coverURL string
	)

	if txErr := l.svcCtx.GormDB.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		// 1. 事务内加行锁读，避免并发删除同一视频时重复扣减 ref_count。
		var item model.Video
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "author_id", "play_url", "cover_url", "status").
			Where("id = ?", videoID).
			First(&item).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				bizErr = status.Error(codes.NotFound, "视频不存在")
				return err
			}
			return err
		}
		if item.Status != model.VideoStatusNormal {
			bizErr = status.Error(codes.NotFound, "视频不存在")
			return gorm.ErrRecordNotFound
		}
		if item.AuthorID != operatorID {
			bizErr = status.Error(codes.PermissionDenied, "无权限操作")
			return errors.New("permission denied")
		}

		authorID = item.AuthorID
		playURL = item.PlayURL
		coverURL = item.CoverURL

		// 2. 软删 videos
		now := time.Now()
		result := tx.Model(&model.Video{}).
			Where("id = ? AND status = ? AND deleted_at IS NULL", videoID, model.VideoStatusNormal).
			Updates(map[string]any{
				"status":     model.VideoStatusDeleted,
				"deleted_at": now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			// 并发场景下可能被别的事务先一步删除。
			bizErr = status.Error(codes.NotFound, "视频不存在")
			return gorm.ErrRecordNotFound
		}

		// 3. 事务内减引用计数（不做物理删除）
		if err := decreaseFileAssetRefInTx(l.ctx, tx, playURL); err != nil {
			return err
		}
		if err := decreaseFileAssetRefInTx(l.ctx, tx, coverURL); err != nil {
			return err
		}

		// 4. 事务内写 outbox（P4：video.deleted 事件）
		occurredAt := now.UnixMilli()
		payloadBytes, err := json.Marshal(eventx.FeedVideoEvent{
			EventID:    eventID,
			VideoID:    videoID,
			AuthorID:   authorID,
			Action:     "delete",
			OccurredAt: occurredAt,
		})
		if err != nil {
			return err
		}
		envelopeBytes, err := json.Marshal(eventx.Envelope{
			EventID:       eventID,
			EventType:     eventx.EventTypeVideoDeleted,
			AggregateType: eventx.AggregateVideo,
			AggregateID:   strconv.FormatUint(videoID, 10),
			Producer:      "video-rpc",
			OccurredAt:    occurredAt,
			Payload:       payloadBytes,
		})
		if err != nil {
			return err
		}

		if err := tx.Create(&model.OutboxEvent{
			EventID:       eventID,
			Topic:         eventx.TopicFeedVideoEvents,
			EventType:     eventx.EventTypeVideoDeleted,
			AggregateType: eventx.AggregateVideo,
			AggregateID:   strconv.FormatUint(videoID, 10),
			Payload:       string(envelopeBytes),
			Status:        model.OutboxStatusPending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}).Error; err != nil {
			return err
		}

		return nil
	}); txErr != nil {
		if bizErr != nil {
			return nil, bizErr
		}
		l.Errorf("delete video transaction failed, video_id: %d, operator_id: %d, error: %v", videoID, operatorID, txErr)
		return nil, status.Error(codes.Internal, "删除失败")
	}

	return &videopb.DeleteVideoResp{
		Msg: "删除成功",
	}, nil
}
