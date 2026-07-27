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

type FollowLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewFollowLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FollowLogic {
	return &FollowLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *FollowLogic) Follow(req *types.FollowReq) (resp *types.FollowResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}
	if req == nil || req.Targetuserid == 0 {
		return nil, status.Error(codes.InvalidArgument, "目标用户不能为空")
	}
	if userID == req.Targetuserid {
		return nil, status.Error(codes.InvalidArgument, "用户不能关注自己")
	}

	rpcResp, err := l.svcCtx.SocialRpc.Follow(l.ctx, &socialclient.FollowReq{
		FollowerId:  userID,
		FollowingId: req.Targetuserid,
	})
	if err != nil {
		return nil, err
	}

	return &types.FollowResp{
		Msg:      rpcResp.GetMsg(),
		Followed: rpcResp.GetFollowed(),
	}, nil
}
