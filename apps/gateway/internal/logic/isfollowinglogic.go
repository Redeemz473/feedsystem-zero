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

type IsFollowingLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIsFollowingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IsFollowingLogic {
	return &IsFollowingLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *IsFollowingLogic) IsFollowing(req *types.IsFollowingReq) (resp *types.IsFollowingResp, err error) {
	if req == nil || req.Targetuserid == 0 {
		return nil, status.Error(codes.InvalidArgument, "目标用户不能为空")
	}

	viewerID := optionalUserIDFromCtx(l.ctx)
	// 游客以及查看自己时一定是未关注，直接返回可减少一次内部 RPC。
	if viewerID == 0 || viewerID == req.Targetuserid {
		return &types.IsFollowingResp{Following: false}, nil
	}

	rpcResp, err := l.svcCtx.SocialRpc.IsFollowing(l.ctx, &socialclient.IsFollowingReq{
		FollowerId:  viewerID,
		FollowingId: req.Targetuserid,
	})
	if err != nil {
		return nil, err
	}
	return &types.IsFollowingResp{Following: rpcResp.GetFollowing()}, nil
}
