// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/feed/feedclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetRecommendFeedLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetRecommendFeedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRecommendFeedLogic {
	return &GetRecommendFeedLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetRecommendFeedLogic) GetRecommendFeed(req *types.GetRecommendFeedReq) (resp *types.GetRecommendFeedResp, err error) {
	viewerID := optionalUserIDFromCtx(l.ctx)
	rpcResp, err := l.svcCtx.FeedRpc.GetRecommendFeed(l.ctx, &feedclient.GetRecommendFeedReq{
		ViewerId:          viewerID,
		CursorPublishedAt: req.Cursorpublishedat,
		CursorVideoId:     req.Cursorvideoid,
		PageSize:          req.Pagesize,
	})
	if err != nil {
		return nil, err
	}
	videos, err := hydrateFeedVideos(l.ctx, l.svcCtx, viewerID, rpcResp.GetItems())
	if err != nil {
		return nil, err
	}

	return &types.GetRecommendFeedResp{
		Videos:                videos,
		Nextcursorpublishedat: rpcResp.GetNextCursorPublishedAt(),
		Nextcursorvideoid:     rpcResp.GetNextCursorVideoId(),
		Hasmore:               rpcResp.GetHasMore(),
	}, nil
}
