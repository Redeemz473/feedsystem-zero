package logic

import (
	"time"

	"feedsystem-zero/apps/notification/internal/model"
	"feedsystem-zero/apps/notification/notification"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validateReceiverID 校验通知接收者ID。
// receiver_id 由 gateway 从 JWT 中解析注入，正常情况下不会为 0；
// 这里做防御性校验，避免 gateway 或调用方漏传导致 SQL WHERE 命中他人数据。
func validateReceiverID(receiverID uint64) error {
	if receiverID == 0 {
		return status.Error(codes.InvalidArgument, "用户id不能为空")
	}
	return nil
}

// validateNotificationID 校验单条通知ID非零。
func validateNotificationID(notificationID uint64) error {
	if notificationID == 0 {
		return status.Error(codes.InvalidArgument, "通知id不能为空")
	}
	return nil
}

// mapModelStatusToProto 把存储层的 int32 状态码映射为 proto enum。
// canceled 已在 SQL WHERE 阶段过滤，这里仅返回 UNREAD/READ；未知值统一 UNSPECIFIED。
func mapModelStatusToProto(status int32) notification.NotificationStatus {
	switch status {
	case model.NotificationStatusUnread:
		return notification.NotificationStatus_NOTIFICATION_STATUS_UNREAD
	case model.NotificationStatusRead:
		return notification.NotificationStatus_NOTIFICATION_STATUS_READ
	default:
		return notification.NotificationStatus_NOTIFICATION_STATUS_UNSPECIFIED
	}
}

// notificationTypeStringToProto 把 notifications.notification_type 字符串映射为 proto enum。
// 未知类型返回 UNSPECIFIED，让前端做兜底展示而不是直接错误退化。
// 字符串常量与 outbox 生产端保持一致，若日后新增类型需同步更新枚举与字符串对照表。
func notificationTypeStringToProto(t string) notification.NotificationType {
	switch t {
	case "video_like":
		return notification.NotificationType_NOTIFICATION_TYPE_VIDEO_LIKE
	case "video_comment":
		return notification.NotificationType_NOTIFICATION_TYPE_VIDEO_COMMENT
	case "follow":
		return notification.NotificationType_NOTIFICATION_TYPE_FOLLOW
	default:
		return notification.NotificationType_NOTIFICATION_TYPE_UNSPECIFIED
	}
}

// millisecondsToDBTime 把客户端游标里的 Unix 毫秒转成入库使用的 time.Local。
// notification-job 写入时使用 UTC->time.Local 的转换（详见 consumer.go 的 OccurredAt 处理），
// 这里必须保持一致，否则 WHERE occurred_at < ? 会因为时区偏移错位一大段数据。
func millisecondsToDBTime(ms int64) time.Time {
	return time.UnixMilli(ms).UTC().In(time.Local)
}
