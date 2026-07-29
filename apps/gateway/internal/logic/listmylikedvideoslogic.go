// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/interaction/interactionclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListMyLikedVideosLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListMyLikedVideosLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListMyLikedVideosLogic {
	return &ListMyLikedVideosLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListMyLikedVideosLogic) ListMyLikedVideos(req *types.ListMyLikedVideosReq) (resp *types.ListMyLikedVideosResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.InteractionRpc.ListMyLikedVideos(l.ctx, &interactionclient.ListMyLikedVideosReq{
		UserId:          userID,
		CursorCreatedAt: req.Cursorcreatedat,
		CursorLikeId:    req.Cursorlikeid,
		PageSize:        req.Pagesize,
	})
	if err != nil {
		return nil, err
	}

	videoIDs := make([]uint64, 0, len(rpcResp.GetLikedVideos()))
	for _, item := range rpcResp.GetLikedVideos() {
		videoIDs = append(videoIDs, item.GetVideoId())
	}
	videoMap, err := loadHTTPVideosByIDs(l.ctx, l.svcCtx.AccountRpc, l.svcCtx.VideoRpc, l.svcCtx.InteractionRpc, userID, videoIDs)
	if err != nil {
		return nil, err
	}

	items := make([]types.LikedVideoInfo, 0, len(rpcResp.GetLikedVideos()))
	for _, item := range rpcResp.GetLikedVideos() {
		video, ok := videoMap[item.GetVideoId()]
		if !ok {
			continue
		}
		items = append(items, types.LikedVideoInfo{
			Likeid:  item.GetLikeId(),
			Likedat: item.GetLikedAt(),
			Video:   video,
		})
	}

	return &types.ListMyLikedVideosResp{
		Likedvideos:         items,
		Nextcursorcreatedat: rpcResp.GetNextCursorCreatedAt(),
		Nextcursorlikeid:    rpcResp.GetNextCursorLikeId(),
		Hasmore:             rpcResp.GetHasMore(),
	}, nil
}
