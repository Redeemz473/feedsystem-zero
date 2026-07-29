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

// MarkNotificationRead 将单条通知从"未读"改为"已读"。
//
// 关键语义：
//  1. receiver_id 必须由 gateway 从 JWT 注入，SQL WHERE 里再校验一次，
//     防止 A 用户通过篡改 notification_id 越权修改 B 的通知。
//  2. RowsAffected=0 一律幂等返回 changed=false（不返回错误）：
//     覆盖"已读过 / 已撤回 / 记录不存在 / 不属于自己"四种情况，
//     响应对调用方一致，避免通过错误码枚举出他人的通知 ID。
//  3. 只有真正的 DB/网络错误才向上抛 codes.Internal，方便调用方区分
//     "点了但没变化"（业务无异常）和"存储异常"（需重试/告警）。
func (l *MarkNotificationReadLogic) MarkNotificationRead(in *notification.MarkNotificationReadReq) (*notification.MarkNotificationReadResp, error) {
	receiverID := in.GetReceiverId()
	if err := validateReceiverID(receiverID); err != nil {
		return nil, err
	}
	notificationID := in.GetNotificationId()
	if err := validateNotificationID(notificationID); err != nil {
		return nil, err
	}

	// 一次 UPDATE 完成"读 -> 判断 -> 写"三步，靠 status=未读 保证幂等：
	//   - 首次点击：命中 unread 行，RowsAffected=1
	//   - 重复点击 / 已撤回 / 越权 / 不存在：WHERE 无命中，RowsAffected=0
	// 用同一个 now 值同时写 read_at 和 updated_at，避免两次 time.Now() 之间的毫秒漂移。
	now := time.Now()
	res := l.svcCtx.GormDB.WithContext(l.ctx).
		Model(&model.Notification{}).
		Where("id = ? AND receiver_id = ? AND status = ? AND deleted_at IS NULL",
			notificationID, receiverID, model.NotificationStatusUnread).
		Updates(map[string]any{
			"status":     model.NotificationStatusRead,
			"read_at":    now,
			"updated_at": now,
		})
	if res.Error != nil {
		l.Errorf(
			"mark notification read failed, receiver_id:%d notification_id:%d error:%v",
			receiverID, notificationID, res.Error,
		)
		return nil, status.Error(codes.Internal, "标记已读失败，请稍后重试")
	}

	changed := res.RowsAffected == 1

	// changed=true 说明本次真的把一条 unread 变成了 read，未读数发生变化。
	// 按方案 B 只需 INCR version key 让旧缓存失效，下一次 GetUnreadCount 会重新 COUNT。
	// Redis 失败仅打日志（BumpUnreadVersion 内部处理），绝不回滚已经提交的 MySQL 状态。
	if changed {
		notificationcache.BumpUnreadVersion(l.ctx, l.svcCtx.RedisCli, receiverID)
	}

	return &notification.MarkNotificationReadResp{
		Changed: changed,
	}, nil
}
