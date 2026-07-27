// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/social/socialclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UnfollowLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUnfollowLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnfollowLogic {
	return &UnfollowLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UnfollowLogic) Unfollow(req *types.UnfollowReq) (resp *types.UnfollowResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	if req == nil || req.Targetuserid == 0 {
		return nil, status.Error(codes.InvalidArgument, "目标用户不能为空")
	}
	if userID == req.Targetuserid {
		return nil, status.Error(codes.InvalidArgument, "用户不能取关自己")
	}

	rpcResp, err := l.svcCtx.SocialRpc.Unfollow(l.ctx, &socialclient.UnfollowReq{
		FollowerId:  userID,
		FollowingId: req.Targetuserid,
	})
	if err != nil {
		return nil, err
	}

	return &types.UnfollowResp{
		Msg:        rpcResp.GetMsg(),
		Unfollowed: rpcResp.GetUnfollowed(),
	}, nil
}
