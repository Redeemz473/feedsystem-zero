package eventx

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const maxNotificationEventIDLength = 128

// BuildNotificationEnvelope 校验通知事件并生成统一 Envelope。
// aggregate_id 使用稳定业务键，使同一通知的创建和撤回进入同一个 Kafka partition。
func BuildNotificationEnvelope(event NotificationEvent, producer string) ([]byte, string, error) {
	if err := ValidateNotificationEvent(event); err != nil {
		return nil, "", err
	}
	producer = strings.TrimSpace(producer)
	if producer == "" {
		return nil, "", errors.New("notification producer不能为空")
	}

	businessKey, err := NotificationBusinessKey(event)
	if err != nil {
		return nil, "", err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, "", fmt.Errorf("序列化notification payload失败: %w", err)
	}

	eventType := EventTypeNotificationCreate
	if event.Action == NotificationActionDelete {
		eventType = EventTypeNotificationDelete
	}
	envelope, err := json.Marshal(Envelope{
		EventID:       event.EventID,
		EventType:     eventType,
		AggregateType: AggregateNotification,
		AggregateID:   businessKey,
		Producer:      producer,
		OccurredAt:    event.OccurredAt,
		Payload:       payload,
	})
	if err != nil {
		return nil, "", fmt.Errorf("序列化notification envelope失败: %w", err)
	}
	return envelope, businessKey, nil
}

func ValidateNotificationEvent(event NotificationEvent) error {
	event.EventID = strings.TrimSpace(event.EventID)
	event.SourceEventID = strings.TrimSpace(event.SourceEventID)
	if event.EventID == "" {
		return errors.New("notification event_id不能为空")
	}
	if len(event.EventID) > maxNotificationEventIDLength {
		return fmt.Errorf("notification event_id不能超过%d字节", maxNotificationEventIDLength)
	}
	if event.SourceEventID == "" {
		return errors.New("notification source_event_id不能为空")
	}
	if len(event.SourceEventID) > maxNotificationEventIDLength {
		return fmt.Errorf("notification source_event_id不能超过%d字节", maxNotificationEventIDLength)
	}
	if event.ReceiverID == 0 {
		return errors.New("notification receiver_id不能为空")
	}
	if event.ActorID == 0 {
		return errors.New("notification actor_id不能为空")
	}
	if event.ReceiverID == event.ActorID {
		return errors.New("不能给自己创建notification事件")
	}
	if event.OccurredAt <= 0 {
		return errors.New("notification occurred_at不能为空")
	}
	switch event.Action {
	case NotificationActionCreate, NotificationActionDelete:
	default:
		return fmt.Errorf("不支持的notification action: %s", event.Action)
	}

	switch event.NotificationType {
	case NotificationTypeVideoLike:
		if event.VideoID == 0 {
			return errors.New("点赞通知video_id不能为空")
		}
		if event.CommentID != 0 {
			return errors.New("点赞通知不能携带comment_id")
		}
	case NotificationTypeVideoComment:
		if event.VideoID == 0 || event.CommentID == 0 {
			return errors.New("评论通知video_id和comment_id不能为空")
		}
	case NotificationTypeFollow:
		if event.VideoID != 0 || event.CommentID != 0 {
			return errors.New("关注通知不能携带video_id或comment_id")
		}
	default:
		return fmt.Errorf("不支持的notification_type: %s", event.NotificationType)
	}
	return nil
}

// NotificationBusinessKey 标识一条业务通知，而不是某次 Kafka 投递。
// 用户取消后再次点赞/关注时会复用同一行，并重新变为未读。
func NotificationBusinessKey(event NotificationEvent) (string, error) {
	if err := ValidateNotificationEvent(event); err != nil {
		return "", err
	}

	switch event.NotificationType {
	case NotificationTypeVideoLike:
		return strings.Join([]string{
			"like",
			strconv.FormatUint(event.ReceiverID, 10),
			strconv.FormatUint(event.ActorID, 10),
			strconv.FormatUint(event.VideoID, 10),
		}, ":"), nil
	case NotificationTypeVideoComment:
		return strings.Join([]string{
			"comment",
			strconv.FormatUint(event.ReceiverID, 10),
			strconv.FormatUint(event.ActorID, 10),
			strconv.FormatUint(event.CommentID, 10),
		}, ":"), nil
	case NotificationTypeFollow:
		return strings.Join([]string{
			"follow",
			strconv.FormatUint(event.ReceiverID, 10),
			strconv.FormatUint(event.ActorID, 10),
		}, ":"), nil
	default:
		return "", fmt.Errorf("不支持的notification_type: %s", event.NotificationType)
	}
}
