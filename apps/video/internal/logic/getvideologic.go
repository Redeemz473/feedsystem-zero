package logic

import (
	"context"

	"feedsystem-zero/apps/video/internal/model"
	"feedsystem-zero/apps/video/internal/svc"
	videopb "feedsystem-zero/apps/video/video"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type GetVideoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetVideoLogic {
	return &GetVideoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 获取视频详情
func (l *GetVideoLogic) GetVideo(in *videopb.GetVideoReq) (*videopb.GetVideoResp, error) {
	videoID := in.GetVideoId()
	viewerID := in.GetViewerId()
	if videoID == 0 {
		return nil, status.Error(codes.InvalidArgument, "视频ID不能为空")
	}

	batchResp, err := NewBatchGetVideosLogic(l.ctx, l.svcCtx).BatchGetVideos(&videopb.BatchGetVideosReq{
		VideoIds: []uint64{videoID},
	})
	if err != nil {
		return nil, err
	}
	if len(batchResp.GetVideos()) == 0 {
		return nil, status.Error(codes.NotFound, "视频不存在")
	}
	videoInfo := batchResp.GetVideos()[0]

	isLiked := false
	if viewerID != 0 {
		var count int64
		err := l.svcCtx.GormDB.WithContext(l.ctx).
			Model(&model.Like{}).
			Where("video_id = ? AND user_id = ? AND status = ? AND deleted_at IS NULL", videoID, viewerID, model.LikeStatusActive).
			Count(&count).Error
		if err != nil {
			l.Errorf("check video liked failed, video_id: %d, viewer_id: %d, error: %v", videoID, viewerID, err)
			return nil, status.Error(codes.Internal, "查询点赞状态失败")
		}
		isLiked = count > 0
	}

	result, ok := proto.Clone(videoInfo).(*videopb.VideoInfo)
	if !ok {
		return nil, status.Error(codes.Internal, "视频数据异常")
	}
	result.IsLiked = isLiked
	return &videopb.GetVideoResp{Video: result}, nil
}
