package logic

import (
	"context"
	"time"

	"feedsystem-zero/apps/notification/internal/model"
	"feedsystem-zero/apps/notification/internal/svc"
	"feedsystem-zero/apps/notification/notification"
	"feedsystem-zero/common/notificationcache"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// MarkAllNotificationsRead 将当前用户所有未读通知一次性置为已读。
//
// 关键语义：
//  1. receiver_id 必须由 gateway 从 JWT 注入，SQL WHERE 里再兜底校验，
//     避免任意用户通过传参把别人的未读通知全部清空。
//  2. 单条 UPDATE 完成"扫描 -> 置读"两步；不需要先 SELECT 再逐条更新。
//     status=未读 AND deleted_at IS NULL 保证幂等：
//     - 首次点击：命中若干未读行，changed_count = RowsAffected
//     - 二次点击：无匹配行，changed_count=0，无副作用
//  3. changed_count>0 才 bump version（对齐 A 表：未读数发生真实变化才失效缓存）；
//     changed_count=0 时严格不触碰 Redis，避免无谓的下一次读回源 MySQL COUNT。
func (l *MarkAllNotificationsReadLogic) MarkAllNotificationsRead(in *notification.MarkAllNotificationsReadReq) (*notification.MarkAllNotificationsReadResp, error) {
	receiverID := in.GetReceiverId()
	if err := validateReceiverID(receiverID); err != nil {
		return nil, err
	}

	// 用同一个 now 值同时写 read_at 和 updated_at，避免两次 time.Now() 之间的毫秒漂移。
	now := time.Now()
	res := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Notification{}).
		Where("receiver_id = ? AND status = ? AND deleted_at IS NULL",
			receiverID, model.NotificationStatusUnread).
		Updates(map[string]any{
			"status":     model.NotificationStatusRead,
			"read_at":    now,
			"updated_at": now,
		})
	if res.Error != nil {
		l.Errorf(
			"mark all notifications read failed, receiver_id:%d error:%v",
			receiverID, res.Error,
		)
		return nil, status.Error(codes.Internal, "全部已读失败，请稍后重试")
	}

	changedCount := res.RowsAffected

	// 真的把 N 条未读一次性变成了 read，未读数从 N 变 0，需要失效缓存。
	// Redis 失败仅打日志（BumpUnreadVersion 内部处理），绝不回滚已提交的 MySQL 状态。
	if changedCount > 0 {
		notificationcache.BumpUnreadVersion(l.ctx, l.svcCtx.RedisCli, receiverID)
	}

	return &notification.MarkAllNotificationsReadResp{
		ChangedCount: changedCount,
	}, nil
}
