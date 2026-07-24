// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListFollowingsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListFollowingsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowingsLogic {
	return &ListFollowingsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListFollowingsLogic) ListFollowings(req *types.ListFollowingsReq) (resp *types.ListFollowingsResp, err error) {
	// TODO: 实现方式与 ListFollowers 对称。
	//
	// 1. 获取可选 viewerID，调用 SocialRpc.ListFollowings。
	// 2. 本接口应收集 relation.following_id，而不是 follower_id。
	// 3. 使用 AccountRpc.BatchGetProfiles 一次批量补齐公开用户资料。
	// 4. 按 Social RPC 原始顺序组装 FollowRelationInfo，并保留
	//    relation.viewer_is_following。
	// 5. 原样透传 Social RPC 返回的双游标和 has_more。

	return
}
