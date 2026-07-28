package logic

import (
	"context"

	"feedsystem-zero/apps/notification/internal/svc"
	"feedsystem-zero/apps/notification/notification"

	"github.com/zeromicro/go-zero/core/logx"
)

type MarkAllNotificationsReadLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewMarkAllNotificationsReadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MarkAllNotificationsReadLogic {
	return &MarkAllNotificationsReadLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *MarkAllNotificationsReadLogic) MarkAllNotificationsRead(in *notification.MarkAllNotificationsReadReq) (*notification.MarkAllNotificationsReadResp, error) {
	// TODO 按以下顺序实现：
	//  1. 校验 receiver_id 非 0，该值由 gateway 从 JWT 注入。
	//  2. 一条 UPDATE 将该用户所有 status=未读且 deleted_at IS NULL 的通知改为已读，
	//     并统一写入 read_at/updated_at；不需要先 SELECT，也不需要逐条循环更新。
	//  3. RowsAffected 即 changed_count，重复调用应自然返回 0，保证幂等。
	//  4. MySQL 成功后再删除或重建该用户的未读数缓存；Redis 失败记录日志并依靠短 TTL 收敛。

	return &notification.MarkAllNotificationsReadResp{}, nil
}
