package logic

import (
	"context"
	"errors"

	"feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type IsLikedLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIsLikedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IsLikedLogic {
	return &IsLikedLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IsLikedLogic) IsLiked(in *interaction.IsLikedReq) (*interaction.IsLikedResp, error) {
	// 未登录用户没有用户维度点赞状态，直接返回未点赞。
	userID := in.GetUserId()
	if userID == 0 {
		return &interaction.IsLikedResp{
			Liked: false,
		}, nil
	}

	// 登录用户需要校验 video_id，并确认视频存在且未删除。
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

	// 优先查询 Redis LikeStateKey(video_id,user_id)。
	liked, hit, err := loadLikeStateFromRedis(l.ctx, l.svcCtx.RedisCli, videoID, userID)
	if err != nil {
		l.Errorf("get like state from redis failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		return nil, status.Error(codes.Internal, "查询点赞状态失败")
	}
	if hit {
		return &interaction.IsLikedResp{
			Liked: liked,
		}, nil
	}

	// Redis 未命中时查询 MySQL likes 表：video_id + user_id + status=1 + deleted_at IS NULL。
	dbLiked, err := loadLikeStateFromDB(l.ctx, l.svcCtx.GormDB, videoID, userID)
	if err != nil {
		l.Errorf("get like state from db failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		return nil, status.Error(codes.Internal, "查询点赞状态失败")
	}

	// 将 MySQL 查询结果回写 Redis，减少详情页、列表页反复查库。
	if dbLiked {
		if err := fillLikedState(l.ctx, l.svcCtx.RedisCli, videoID, userID); err != nil {
			l.Errorf("fill redis liked state failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		}
	} else {
		if err := fillUnlikedState(l.ctx, l.svcCtx.RedisCli, videoID, userID); err != nil {
			l.Errorf("fill redis unliked state failed, video_id: %d, user_id: %d, error: %v", videoID, userID, err)
		}
	}

	return &interaction.IsLikedResp{
		Liked: dbLiked,
	}, nil
}
