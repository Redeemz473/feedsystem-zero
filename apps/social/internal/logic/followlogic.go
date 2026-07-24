package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type FollowLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFollowLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FollowLogic {
	return &FollowLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Follow 建议按下面顺序实现：
//
//  1. 参数校验：
//     - follower_id/following_id 不能为 0；
//     - 两者相等时返回 codes.InvalidArgument，用户不能关注自己；
//     - follower_id 必须由 gateway 从 JWT 解析，不能使用前端传来的身份。
//  2. 调 AccountRpc.GetProfile(following_id) 校验目标用户存在。
//     NotFound 原样转换为“目标用户不存在”，其它 RPC 错误记录日志后返回。
//  3. 预生成 event_id、occurred_at 和 aggregate_id（格式 followerID:followingID）。
//  4. 开启 MySQL 事务，并对唯一关系行 SELECT ... FOR UPDATE：
//     - 已存在且 status=Active：这是重复关注，直接幂等成功，不写第二条 Outbox；
//     - 已存在且 status=Deleted：更新为 Active、deleted_at=NULL、updated_at=now；
//     - 不存在：插入一条 Active 关系。
//     两个并发首次关注都查不到时，依赖 uk_follower_following 兜底；
//     后到事务若遇到 1062，应在事务外回读，确认已经 Active 后按幂等成功返回。
//  5. 只有关系真正从“未关注”变为“已关注”时，才在同一事务插入 OutboxEvent：
//     - topic=eventx.TopicFollowEvents
//     - event_type=eventx.EventTypeFollowCreated
//     - aggregate_type=eventx.AggregateFollow
//     - payload 不能只写 BuildFollowPayload 的结果，还要包一层 eventx.Envelope；
//     - producer 使用 "social-rpc"。
//  6. 事务提交后再更新 Redis：
//     - SocialFollowingStateKey(followerID, followingID) 写 "1" 并设置 TTL；
//     - 删除 followerID 和 followingID 的 SocialFollowStatsKey；
//     - Redis 失败只记录日志，不能把已经提交成功的关注事务返回成失败。
//  7. 返回 FollowResp{Msg:"关注成功", Followed:true}。
func (l *FollowLogic) Follow(in *social.FollowReq) (*social.FollowResp, error) {
	// TODO: 根据上面的步骤实现核心业务逻辑。
	return &social.FollowResp{
		Msg:      "not implemented",
		Followed: false,
	}, nil
}
