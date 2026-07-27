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

type GetFollowingFeedLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetFollowingFeedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFollowingFeedLogic {
	return &GetFollowingFeedLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetFollowingFeedLogic) GetFollowingFeed(req *types.GetFollowingFeedReq) (resp *types.GetFollowingFeedResp, err error) {
	viewerID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	rpcResp, err := l.svcCtx.FeedRpc.GetFollowingFeed(l.ctx, &feedclient.GetFollowingFeedReq{
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

	return &types.GetFollowingFeedResp{
		Videos:                videos,
		Nextcursorpublishedat: rpcResp.GetNextCursorPublishedAt(),
		Nextcursorvideoid:     rpcResp.GetNextCursorVideoId(),
		Hasmore:               rpcResp.GetHasMore(),
	}, nil
}
