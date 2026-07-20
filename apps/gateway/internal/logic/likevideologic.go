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

type LikeVideoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLikeVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LikeVideoLogic {
	return &LikeVideoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LikeVideoLogic) LikeVideo(req *types.LikeVideoReq) (resp *types.LikeVideoResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.InteractionRpc.LikeVideo(l.ctx, &interactionclient.LikeVideoReq{
		UserId:  userID,
		VideoId: req.Videoid,
	})
	if err != nil {
		return nil, err
	}

	return &types.LikeVideoResp{
		Msg:        rpcResp.GetMsg(),
		Liked:      rpcResp.GetLiked(),
		Likescount: rpcResp.GetLikesCount(),
	}, nil
}
