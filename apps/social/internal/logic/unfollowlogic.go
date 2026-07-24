package logic

import (
	"context"

	"feedsystem-zero/apps/social/internal/svc"
	"feedsystem-zero/apps/social/social"

	"github.com/zeromicro/go-zero/core/logx"
)

type UnfollowLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnfollowLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnfollowLogic {
	return &UnfollowLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Unfollow 建议按下面顺序实现：
//
//  1. 校验 follower_id/following_id 非 0；两者相等可以直接返回参数错误。
//  2. 开启 MySQL 事务，按 (follower_id, following_id) SELECT ... FOR UPDATE：
//     - 记录不存在或已经 Deleted：属于重复取关，直接幂等成功，不重复写 Outbox；
//     - Active：更新 status=Deleted、deleted_at=now、updated_at=now。
//  3. 只有状态真正由 Active 变为 Deleted 时，才在同一事务插入 OutboxEvent：
//     - event_type=eventx.EventTypeFollowDeleted；
//     - action=eventx.FollowActionUnfollow；
//     - payload 使用 eventx.Envelope 包裹 FollowEvent。
//  4. 事务提交后将 SocialFollowingStateKey 写 "0" 并设置 TTL，
//     同时删除双方的 SocialFollowStatsKey。Redis 失败只记录日志。
//  5. 返回 UnfollowResp{Msg:"取关成功", Unfollowed:true}。
//
// follower_id 同样只能来自 gateway 解析后的 JWT。
func (l *UnfollowLogic) Unfollow(in *social.UnfollowReq) (*social.UnfollowResp, error) {
	// TODO: 根据上面的步骤实现核心业务逻辑。
	return &social.UnfollowResp{
		Msg:        "not implemented",
		Unfollowed: false,
	}, nil
}
