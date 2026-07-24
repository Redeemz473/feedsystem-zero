// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
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
	// TODO:
	// 1. 使用 optionalUserIDFromCtx(l.ctx) 获取查看者；未登录时得到 0。
	// 2. 校验 req.Targetuserid 非 0。
	// 3. 调 SocialRpc.IsFollowing(follower_id=viewerID, following_id=targetUserID)。
	//    social-rpc 会把 viewerID=0 直接处理成 false。
	// 4. 返回 types.IsFollowingResp{Following: rpcResp.GetFollowing()}。

	return
}
