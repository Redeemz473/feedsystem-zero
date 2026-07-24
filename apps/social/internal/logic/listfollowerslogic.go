package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListFollowersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListFollowersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListFollowersLogic {
	return &ListFollowersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ListFollowers 查询“谁关注了 user_id”，建议按下面步骤实现：
//
//  1. 校验 user_id 非 0，并通过 AccountRpc.GetProfile 确认用户存在。
//  2. 统一 page_size：默认 20，最大 50；cursor_updated_at 和 cursor_follow_id
//     必须同时为 0（首页）或同时非 0（后续页）。
//  3. 查询 follows：
//     WHERE following_id=? AND status=Active AND deleted_at IS NULL
//     后续页追加：
//     updated_at < cursorTime OR (updated_at = cursorTime AND id < cursorFollowID)
//     ORDER BY updated_at DESC, id DESC LIMIT pageSize+1。
//     cursor_updated_at 是 Unix 毫秒，使用 time.UnixMilli 转换。
//  4. 多取的一条只用于判断 has_more，返回结果截断为 pageSize。
//  5. 收集本页 follower_id；如果 viewer_id 非 0，批量查询
//     viewer_id 是否关注这些 follower_id。建议抽到 socialhelper.go 复用，
//     不要在循环中逐条调用 IsFollowing。
//  6. 组装 FollowRelation：
//     relation_id=row.ID、follower_id、following_id、
//     followed_at=row.UpdatedAt.UnixMilli()、viewer_is_following。
//  7. 下一页游标取本页最后一条的 updated_at 和 id；空列表返回 0。
//     查询条件与 011_social_final_indexes.sql 的索引顺序保持一致。
func (l *ListFollowersLogic) ListFollowers(in *social.ListFollowersReq) (*social.ListFollowersResp, error) {
	// TODO: 根据上面的步骤实现核心业务逻辑。
	return &social.ListFollowersResp{
		Followers:           nil,
		NextCursorUpdatedAt: 0,
		NextCursorFollowId:  0,
		HasMore:             false,
	}, nil
}
