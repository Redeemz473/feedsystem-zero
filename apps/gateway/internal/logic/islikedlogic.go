// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/interaction/interactionclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type IsLikedLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIsLikedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IsLikedLogic {
	return &IsLikedLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *IsLikedLogic) IsLiked(req *types.IsLikedReq) (resp *types.IsLikedResp, err error) {
	if req.Videoid == 0 {
		return nil, status.Error(codes.InvalidArgument, "视频ID不能为空")
	}

	rpcResp, err := l.svcCtx.InteractionRpc.IsLiked(l.ctx, &interactionclient.IsLikedReq{
		UserId:  optionalUserIDFromCtx(l.ctx),
		VideoId: req.Videoid,
	})
	if err != nil {
		return nil, err
	}

	return &types.IsLikedResp{
		Liked: rpcResp.GetLiked(),
	}, nil
}
