// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
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
	// TODO:
	// 1. 校验 req.Userid 非 0。
	// 2. 调 SocialRpc.GetFollowStats(user_id=req.Userid)。
	// 3. 映射 followers_count/followings_count 返回。
	// 这个接口是公开读接口，不要求登录，也不需要把 viewerID 传给 social-rpc。

	return
}
