// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListFollowersLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListFollowersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowersLogic {
	return &ListFollowersLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListFollowersLogic) ListFollowers(req *types.ListFollowersReq) (resp *types.ListFollowersResp, err error) {
	// TODO: Gateway 在这里负责“关系数据 + 用户公开资料”的批量聚合。
	//
	// 1. viewerID := optionalUserIDFromCtx(l.ctx)。
	// 2. 调 SocialRpc.ListFollowers，透传 user_id、viewer_id、双游标和 page_size。
	// 3. 按 RPC 返回顺序收集每条关系的 follower_id，调用
	//    AccountRpc.BatchGetProfiles 一次性补齐用户名、头像和简介。
	// 4. 把 profiles 转成 map[userID]PublicProfile，再按关系原顺序组装
	//    []types.FollowRelationInfo：
	//    - Relationid = relation.RelationId
	//    - User = follower_id 对应的公开资料
	//    - Followedat = relation.FollowedAt
	//    - Viewerisfollowing = relation.ViewerIsFollowing
	//    不要在循环中逐个 GetProfile，避免 N+1 RPC。
	// 5. 某个账号不存在时跳过该展示项，但下一页游标仍使用 Social RPC 返回值，
	//    不能根据跳过后的切片重新计算，否则会造成分页重复。
	// 6. 原样返回 next_cursor_updated_at、next_cursor_follow_id 和 has_more。

	return
}
