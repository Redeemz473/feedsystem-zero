// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
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
	// TODO: Gateway 这里只做身份可信转换和 RPC 转发，不直接操作 follows 表。
	//
	// 1. 调 userIDFromCtx(l.ctx) 取得 JWT 中的当前用户 ID。
	// 2. 校验 req.Targetuserid 非 0；自己关注自己也可以在这里提前返回参数错误，
	//    但 social-rpc 仍必须保留同样校验，防止内部调用绕过 gateway。
	// 3. 调 SocialRpc.Follow：
	//    follower_id=当前登录用户，following_id=req.Targetuserid。
	// 4. 将 RPC 的 msg/followed 映射成 types.FollowResp 返回。

	return
}
