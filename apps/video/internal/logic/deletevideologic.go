package logic

import (
	"context"
	"errors"
	"time"

	"feedsystem-zero/apps/video/internal/model"
	"feedsystem-zero/apps/video/internal/svc"
	videopb "feedsystem-zero/apps/video/video"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
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
func (l *DeleteVideoLogic) DeleteVideo(in *videopb.DeleteVideoReq) (*videopb.DeleteVideoResp, error) {
	videoID := in.GetVideoId()
	operatorID := in.GetOperatorId()

	if videoID == 0 {
		return nil, status.Error(codes.InvalidArgument, "视频ID不能为空")
	}
	if operatorID == 0 {
		return nil, status.Error(codes.Unauthenticated, "未登录")
	}

	var item model.Video
	if err := l.svcCtx.GormDB.WithContext(l.ctx).
		Select("id", "author_id").
		Where("id = ? AND status = ? AND deleted_at IS NULL", videoID, model.VideoStatusNormal).
		First(&item).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "视频不存在")
		}
		l.Errorf("get video failed, video_id: %d, error: %v", videoID, err)
		return nil, status.Error(codes.Internal, "视频查询失败")
	}
	if item.AuthorID != operatorID {
		return nil, status.Error(codes.PermissionDenied, "无权限操作")
	}

	now := time.Now()
	result := l.svcCtx.GormDB.WithContext(l.ctx).
		Where("id = ? AND status = ? AND deleted_at IS NULL", videoID, model.VideoStatusNormal).
		Updates(map[string]any{
			"status":     model.VideoStatusDeleted,
			"deleted_at": now,
		})
	if result.Error != nil {
		l.Errorf("delete video failed, video_id: %d, operator_id: %d, error: %v", videoID, operatorID, result.Error)
		return nil, status.Error(codes.Internal, "删除失败")
	}
	if result.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "视频不存在")
	}

	return &videopb.DeleteVideoResp{
		Msg: "删除成功",
	}, nil
}
