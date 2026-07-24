// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
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
	// TODO:
	// 1. 从 JWT context 获取当前用户 ID，不接受前端传 follower_id。
	// 2. 校验 target_user_id。
	// 3. 调 SocialRpc.Unfollow(follower_id=currentUserID, following_id=targetUserID)。
	// 4. 映射 msg/unfollowed 返回；RPC 错误直接交给统一错误处理。

	return
}
