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

// ListFollowings 查某用户关注的人列表（他关注了谁）。
//
//	索引：idx_follower(follower_id)，按 id 倒序。
//	游标分页与 ListFollowers 同策略，仅返回 status=FollowStatusActive。
func (l *ListFollowingsLogic) ListFollowings(in *social.ListFollowingsReq) (*social.ListFollowingsResp, error) {
	// TODO: 实现关注列表游标分页查询。
	return &social.ListFollowingsResp{
		Followings:   nil,
		NextCursorId: 0,
		HasMore:      false,
	}, nil
}
