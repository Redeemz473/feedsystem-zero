package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type UnfollowLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnfollowLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnfollowLogic {
	return &UnfollowLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Unfollow 取关逻辑骨架（业务逻辑由你实现）：
//  1. 幂等：若 follows 已无 active 记录，直接返回 unfollowed=true。
//  2. 事务（同一 DB 事务内）：
//     a. 将 follows 行 status 置为 FollowStatusDeleted（软删，保留审计）。
//     b. 插入 OutboxEvent（event_type=EventTypeFollowDeleted, action=FollowActionUnfollow）。
//  3. 返回 UnfollowResp{unfollowed:true}。
//
// 同样：follower_id 由 gateway 从 JWT 解析传入。
func (l *UnfollowLogic) Unfollow(in *social.UnfollowReq) (*social.UnfollowResp, error) {
	// TODO: 实现取关业务逻辑（参考上述注释步骤）。
	return &social.UnfollowResp{
		Msg:        "not implemented",
		Unfollowed: false,
	}, nil
}
