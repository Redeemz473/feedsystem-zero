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

type UnlikeVideoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUnlikeVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnlikeVideoLogic {
	return &UnlikeVideoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UnlikeVideoLogic) UnlikeVideo(req *types.UnlikeVideoReq) (resp *types.UnlikeVideoResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.InteractionRpc.UnlikeVideo(l.ctx, &interactionclient.UnlikeVideoReq{
		UserId:  userID,
		VideoId: req.Videoid,
	})
	if err != nil {
		return nil, err
	}

	return &types.UnlikeVideoResp{
		Msg:        rpcResp.GetMsg(),
		Liked:      rpcResp.GetLiked(),
		Likescount: rpcResp.GetLikesCount(),
	}, nil
}
