package logic

import (
	"context"

	"feedsystem-zero/apps/notification/internal/svc"
	"feedsystem-zero/apps/notification/notification"

	"github.com/zeromicro/go-zero/core/logx"
)

type MarkNotificationReadLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewMarkNotificationReadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MarkNotificationReadLogic {
	return &MarkNotificationReadLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *MarkNotificationReadLogic) MarkNotificationRead(in *notification.MarkNotificationReadReq) (*notification.MarkNotificationReadResp, error) {
	// TODO 按以下顺序实现：
	//  1. 校验 receiver_id、notification_id 非 0；receiver_id 必须来自 gateway 的 JWT。
	//  2. 只更新 id=? AND receiver_id=? AND status=未读 AND deleted_at IS NULL 的记录，
	//     将 status 改为已读并写 read_at/updated_at。receiver_id 条件用于阻止越权操作。
	//  3. RowsAffected=1 返回 changed=true；记录不存在、已读或已撤回均幂等返回 changed=false，
	//     不向调用方泄露其他用户的通知是否存在。
	//  4. MySQL 成功后再处理未读数缓存失效；Redis 失败只记录日志，不能回滚已经提交的已读状态。

	return &notification.MarkNotificationReadResp{}, nil
}
