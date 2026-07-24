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

// IsFollowing 是缓存旁路查询，建议按下面步骤实现：
//
//  1. following_id 为 0 时返回 codes.InvalidArgument。
//  2. follower_id=0 表示未登录访问，直接返回 following=false；
//     follower_id==following_id 也直接返回 false。
//  3. 查询 Redis SocialFollowingStateKey：
//     - "1" 返回 true；
//     - "0" 返回 false；
//     - redis.Nil 表示未命中，继续查 MySQL；
//     - Redis 故障记录日志后降级查 MySQL，不要让公开查询直接失败。
//  4. MySQL 查询是否存在
//     (follower_id, following_id, status=Active, deleted_at IS NULL)。
//  5. 将结果按 "1"/"0" 回填 Redis，并设置 SocialFollowingStateTTL；
//     必须缓存 false，防止不存在关系持续穿透数据库。
func (l *IsFollowingLogic) IsFollowing(in *social.IsFollowingReq) (*social.IsFollowingResp, error) {
	// TODO: 根据上面的步骤实现核心业务逻辑。
	return &social.IsFollowingResp{
		Following: false,
	}, nil
}
