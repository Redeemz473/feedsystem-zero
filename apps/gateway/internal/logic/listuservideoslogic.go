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

type ListUserVideosLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListUserVideosLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListUserVideosLogic {
	return &ListUserVideosLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListUserVideosLogic) ListUserVideos(req *types.ListUserVideosReq) (resp *types.ListUserVideosResp, err error) {
	viewerID := optionalUserIDFromCtx(l.ctx)
	rpcResp, err := l.svcCtx.VideoRpc.ListUserVideos(l.ctx, &videoclient.ListUserVideosReq{
		AuthorId:        req.Authorid,
		ViewerId:        viewerID,
		CursorCreatedAt: req.Cursorcreatedat,
		CursorVideoId:   req.Cursorvideoid,
		PageSize:        req.Pagesize,
	})
	if err != nil {
		return nil, err
	}

	videos := make([]types.VideoInfo, 0, len(rpcResp.GetVideos()))
	for _, item := range rpcResp.GetVideos() {
		videos = append(videos, toHTTPVideoInfo(item))
	}
	if enrichedVideos, enrichErr := enrichHTTPVideoInteractions(l.ctx, l.svcCtx.InteractionRpc, viewerID, videos); enrichErr != nil {
		l.Errorf("enrich user videos interaction stats failed, author_id: %d, viewer_id: %d, error: %v", req.Authorid, viewerID, enrichErr)
	} else {
		videos = enrichedVideos
	}
	if enrichedVideos, enrichErr := enrichHTTPVideoAuthors(l.ctx, l.svcCtx.AccountRpc, videos); enrichErr != nil {
		l.Errorf("enrich user videos author failed, author_id: %d, error: %v", req.Authorid, enrichErr)
	} else {
		videos = enrichedVideos
	}

	return &types.ListUserVideosResp{
		Videos:              videos,
		Nextcursorcreatedat: rpcResp.GetNextCursorCreatedAt(),
		Nextcursorvideoid:   rpcResp.GetNextCursorVideoId(),
		Hasmore:             rpcResp.GetHasMore(),
	}, nil
}
