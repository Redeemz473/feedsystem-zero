package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetFollowStatsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetFollowStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFollowStatsLogic {
	return &GetFollowStatsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetFollowStatsLogic) GetFollowStats(in *social.GetFollowStatsReq) (*social.GetFollowStatsResp, error) {
	// TODO: 按下面步骤实现粉丝数和关注数查询。
	//
	// 1. 校验 user_id 非 0，并通过 AccountRpc.GetProfile 确认用户存在。
	// 2. 读取 Redis SocialFollowStatsKey 的 followers_count/followings_count；
	//    两个字段都存在且能解析为非负整数时直接返回。
	// 3. 缓存未命中或 Redis 故障时分别执行两次 COUNT：
	//    - followers_count: following_id=userID AND status=Active AND deleted_at IS NULL
	//    - followings_count: follower_id=userID AND status=Active AND deleted_at IS NULL
	//    两个查询分别命中不同联合索引，不建议用带 OR 的单条 SQL。
	// 4. 使用 Redis Pipeline/HSet 回填两个字段并设置 SocialFollowStatsTTL。
	//    Redis 写失败只记录日志，MySQL 结果仍正常返回。

	return &social.GetFollowStatsResp{}, nil
}
