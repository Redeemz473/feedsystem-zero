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

type GetVideoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetVideoLogic {
	return &GetVideoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetVideoLogic) GetVideo(req *types.GetVideoReq) (resp *types.GetVideoResp, err error) {
	viewerID := optionalUserIDFromCtx(l.ctx)
	rpcResp, err := l.svcCtx.VideoRpc.GetVideo(l.ctx, &videoclient.GetVideoReq{
		VideoId:  req.Videoid,
		ViewerId: viewerID,
	})
	if err != nil {
		return nil, err
	}

	videos := []types.VideoInfo{
		toHTTPVideoInfo(rpcResp.GetVideo()),
	}
	if enrichedVideos, enrichErr := enrichHTTPVideoInteractions(l.ctx, l.svcCtx.InteractionRpc, viewerID, videos); enrichErr != nil {
		l.Errorf("enrich video interaction stats failed, video_id: %d, viewer_id: %d, error: %v", req.Videoid, viewerID, enrichErr)
	} else {
		videos = enrichedVideos
	}
	if enrichedVideos, enrichErr := enrichHTTPVideoAuthors(l.ctx, l.svcCtx.AccountRpc, videos); enrichErr != nil {
		l.Errorf("enrich video author failed, video_id: %d, error: %v", req.Videoid, enrichErr)
	} else {
		videos = enrichedVideos
	}

	return &types.GetVideoResp{
		Video: videos[0],
	}, nil
}
