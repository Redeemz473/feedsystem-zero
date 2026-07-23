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

// Follow 关注逻辑骨架（业务逻辑由你实现）：
//  1. 参数校验：follower_id == following_id 应直接拒绝（不能关注自己）。
//  2. 幂等：先查 follows 是否已存在 active 记录，存在则直接返回 followed=true。
//  3. 事务（同一 DB 事务内）：
//     a. upsert follows 行（不存在则插入 status=FollowStatusActive；已软删则更新回 active）。
//     b. 插入 OutboxEvent（topic=TopicFollowEvents, event_type=EventTypeFollowCreated,
//     aggregate_type=AggregateFollow, aggregate_id="{follower_id}:{following_id}",
//     payload=BuildFollowPayload(...)）。
//     —— 关注是核心状态，必须在事务内落库 + 写 outbox，由 outbox job 异步投递 Kafka，
//     下游 feed-timeline-job / notification-job 再消费，保证最终一致。
//  4. 返回 FollowResp{followed:true}。
//
// 注意：follower_id 必须由 gateway 从 JWT 解析后传入，本 rpc 不信任前端伪造身份。
func (l *FollowLogic) Follow(in *social.FollowReq) (*social.FollowResp, error) {
	// TODO: 实现关注业务逻辑（参考上述注释步骤）。
	return &social.FollowResp{
		Msg:      "not implemented",
		Followed: false,
	}, nil
}
