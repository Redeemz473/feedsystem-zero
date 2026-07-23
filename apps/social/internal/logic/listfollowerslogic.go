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

// ListFollowers 查某用户的粉丝列表（谁关注了他）。
//
//	索引：idx_following(following_id)，按 id 倒序（最新关注排前）。
//	游标分页：cursor_id=0 首页；之后用上一页最小 id 作为下一页 cursor_id，
//	取 page_size+1 条判断是否 has_more，并回填 next_cursor_id。
//	仅返回 status=FollowStatusActive 的关系。
func (l *ListFollowersLogic) ListFollowers(in *social.ListFollowersReq) (*social.ListFollowersResp, error) {
	// TODO: 实现粉丝列表游标分页查询。
	return &social.ListFollowersResp{
		Followers:    nil,
		NextCursorId: 0,
		HasMore:      false,
	}, nil
}
