package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type IsFollowingLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIsFollowingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IsFollowingLogic {
	return &IsFollowingLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// IsFollowing 是否关注查询（读多写少，可走 MySQL；若需极致低延迟可加 Redis 缓存）。
//
//	查 follows 是否存在 (follower_id, following_id, status=FollowStatusActive) 记录。
func (l *IsFollowingLogic) IsFollowing(in *social.IsFollowingReq) (*social.IsFollowingResp, error) {
	// TODO: 实现关注判定查询。
	return &social.IsFollowingResp{
		Following: false,
	}, nil
}
