package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListFollowingsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListFollowingsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowingsLogic {
	return &ListFollowingsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ListFollowings 查询“user_id 关注了谁”，分页规则与 ListFollowers 对称：
//
//  1. 校验 user_id、目标用户存在性、page_size 和双游标组合。
//  2. 查询 follows：
//     WHERE follower_id=? AND status=Active AND deleted_at IS NULL
//     后续页使用 (updated_at, id) 复合游标，
//     ORDER BY updated_at DESC, id DESC LIMIT pageSize+1。
//  3. 收集本页 following_id，批量计算 viewer_id 对这些用户的关注状态。
//     viewer_id=0 时全部为 false；不要循环访问 Redis/MySQL。
//  4. 组装 FollowRelation，followed_at 使用 UpdatedAt.UnixMilli()。
//  5. 截断多取的一条并计算 has_more、next_cursor_updated_at、
//     next_cursor_follow_id。
func (l *ListFollowingsLogic) ListFollowings(in *social.ListFollowingsReq) (*social.ListFollowingsResp, error) {
	// TODO: 根据上面的步骤实现核心业务逻辑。
	return &social.ListFollowingsResp{
		Followings:          nil,
		NextCursorUpdatedAt: 0,
		NextCursorFollowId:  0,
		HasMore:             false,
	}, nil
}
