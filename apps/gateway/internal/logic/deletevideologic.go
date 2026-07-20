// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/video/videoclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteVideoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewDeleteVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteVideoLogic {
	return &DeleteVideoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *DeleteVideoLogic) DeleteVideo(req *types.DeleteVideoReq) (resp *types.DeleteVideoResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	videoResp, err := l.svcCtx.VideoRpc.GetVideo(l.ctx, &videoclient.GetVideoReq{
		VideoId:  req.Videoid,
		ViewerId: userID,
	})
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.VideoRpc.DeleteVideo(l.ctx, &videoclient.DeleteVideoReq{
		VideoId:    req.Videoid,
		OperatorId: userID,
	})
	if err != nil {
		return nil, err
	}
	if videoResp.GetVideo() != nil {
		if err := decreaseFileAssetRefAndCleanup(l.ctx, l.svcCtx.GormDB, l.svcCtx.Config.Upload, videoResp.GetVideo().GetPlayUrl()); err != nil {
			l.Errorf("cleanup video file asset failed, video_id: %d, play_url: %s, error: %v", req.Videoid, videoResp.GetVideo().GetPlayUrl(), err)
		}
		if err := decreaseFileAssetRefAndCleanup(l.ctx, l.svcCtx.GormDB, l.svcCtx.Config.Upload, videoResp.GetVideo().GetCoverUrl()); err != nil {
			l.Errorf("cleanup cover file asset failed, video_id: %d, cover_url: %s, error: %v", req.Videoid, videoResp.GetVideo().GetCoverUrl(), err)
		}
	}

	return &types.DeleteVideoResp{
		Msg: rpcResp.GetMsg(),
	}, nil
}
