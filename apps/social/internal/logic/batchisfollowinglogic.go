package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type BatchIsFollowingLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchIsFollowingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchIsFollowingLogic {
	return &BatchIsFollowingLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BatchIsFollowingLogic) BatchIsFollowing(in *social.BatchIsFollowingReq) (*social.BatchIsFollowingResp, error) {
	// TODO: 按下面步骤实现，供 Feed/Gateway 一次查询多个作者的关注状态。
	//
	// 1. target_user_ids 最多允许 100 个；过滤 0、去重，并保留请求顺序。
	// 2. viewer_id=0 时不访问 Redis/MySQL，直接按请求顺序返回全部 false。
	// 3. 用 Pipeline/MGET 批量读取所有 SocialFollowingStateKey：
	//    命中 "1"/"0" 的直接记录，redis.Nil 的 ID 放入 misses。
	// 4. 对 misses 只执行一次 MySQL 查询：
	//    SELECT following_id FROM follows
	//    WHERE follower_id=? AND following_id IN ? AND status=Active AND deleted_at IS NULL。
	// 5. 用一次 Redis Pipeline 把 misses 的 true/false 全部回填并设置 TTL。
	//    Redis 故障允许降级到 MySQL；MySQL 故障返回 codes.Internal。
	// 6. 最终必须按去重后的请求顺序返回 FollowingState，不能依赖 SQL IN 的返回顺序。

	return &social.BatchIsFollowingResp{}, nil
}
