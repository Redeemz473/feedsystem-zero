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

type GetFollowStatsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetFollowStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFollowStatsLogic {
	return &GetFollowStatsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetFollowStatsLogic) GetFollowStats(req *types.GetFollowStatsReq) (resp *types.GetFollowStatsResp, err error) {
	if req == nil || req.Userid == 0 {
		return nil, status.Error(codes.InvalidArgument, "用户ID不能为空")
	}

	rpcResp, err := l.svcCtx.SocialRpc.GetFollowStats(l.ctx, &socialclient.GetFollowStatsReq{
		UserId: req.Userid,
	})
	if err != nil {
		return nil, err
	}
	return &types.GetFollowStatsResp{
		Followerscount:  rpcResp.GetFollowersCount(),
		Followingscount: rpcResp.GetFollowingsCount(),
	}, nil
}
