package logic

import (
	"encoding/json"
	"testing"
	"time"

	"feedsystem-zero/apps/job/notification/internal/config"
	"feedsystem-zero/apps/job/notification/internal/model"
	"feedsystem-zero/apps/job/notification/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
)

func TestDecodeNotificationEvents(t *testing.T) {
	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.Local)
	consumer := newTestNotificationConsumer(now)

	tests := []struct {
		name      string
		eventType string
		action    string
	}{
		{name: "创建通知", eventType: eventx.EventTypeNotificationCreate, action: eventx.NotificationActionCreate},
		{name: "撤回通知", eventType: eventx.EventTypeNotificationDelete, action: eventx.NotificationActionDelete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := eventx.NotificationEvent{
				EventID:          "notify_1",
				SourceEventID:    "like_1",
				ReceiverID:       10,
				ActorID:          20,
				VideoID:          30,
				NotificationType: eventx.NotificationTypeVideoLike,
				Action:           tt.action,
				OccurredAt:       now.UnixMilli(),
			}
			message := makeNotificationMessage(t, event, tt.eventType)
			decoded, _, err := consumer.decodeMessage(message)
			if err != nil {
				t.Fatalf("decodeMessage() error = %v", err)
			}
			if decoded.BusinessKey != "like:10:20:30" {
				t.Fatalf("BusinessKey = %q", decoded.BusinessKey)
			}
			if decoded.Event.Action != tt.action {
				t.Fatalf("Action = %q, want %q", decoded.Event.Action, tt.action)
			}
		})
	}
}

func TestDecodeNotificationRejectsActionMismatch(t *testing.T) {
	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.Local)
	consumer := newTestNotificationConsumer(now)
	event := eventx.NotificationEvent{
		EventID:          "notify_bad",
		SourceEventID:    "follow_1",
		ReceiverID:       10,
		ActorID:          20,
		NotificationType: eventx.NotificationTypeFollow,
		Action:           eventx.NotificationActionDelete,
		OccurredAt:       now.UnixMilli(),
	}
	message := makeNotificationMessage(t, event, eventx.EventTypeNotificationCreate)
	if _, _, err := consumer.decodeMessage(message); err == nil {
		t.Fatal("decodeMessage() expected action mismatch error")
	}
}

func TestNotificationRecordFromDeleteEventCreatesTombstone(t *testing.T) {
	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.Local)
	decoded := decodedNotificationEvent{
		BusinessKey: "follow:10:20",
		OccurredAt:  now.Add(-time.Minute),
		Event: eventx.NotificationEvent{
			EventID:          "notify_delete",
			SourceEventID:    "unfollow_1",
			ReceiverID:       10,
			ActorID:          20,
			NotificationType: eventx.NotificationTypeFollow,
			Action:           eventx.NotificationActionDelete,
			OccurredAt:       now.Add(-time.Minute).UnixMilli(),
		},
	}
	record := notificationRecordFromEvent(decoded, now)
	if record.Status != model.NotificationStatusCanceled {
		t.Fatalf("Status = %d, want canceled", record.Status)
	}
	if record.DeletedAt == nil || !record.DeletedAt.Equal(decoded.OccurredAt) {
		t.Fatalf("DeletedAt = %v, want %v", record.DeletedAt, decoded.OccurredAt)
	}
}

func TestGroupNotificationMessagesKeepsPartitionOrder(t *testing.T) {
	messages := []kafkax.Message{
		{Topic: eventx.TopicNotificationEvents, Partition: 1, Offset: 10},
		{Topic: eventx.TopicNotificationEvents, Partition: 2, Offset: 20},
		{Topic: eventx.TopicNotificationEvents, Partition: 1, Offset: 11},
	}
	groups := groupNotificationMessages(messages)
	if len(groups) != 2 {
		t.Fatalf("len(groups) = %d, want 2", len(groups))
	}
	if groups[0].Messages[0].Offset != 10 || groups[0].Messages[1].Offset != 11 {
		t.Fatalf("partition order changed: %+v", groups[0].Messages)
	}
}

func newTestNotificationConsumer(now time.Time) *NotificationConsumer {
	return &NotificationConsumer{
		svcCtx: &svc.ServiceContext{Config: config.Config{}},
		now: func() time.Time {
			return now
		},
	}
}

func makeNotificationMessage(
	t *testing.T,
	event eventx.NotificationEvent,
	eventType string,
) kafkax.Message {
	t.Helper()

	data, businessKey, err := eventx.BuildNotificationEnvelope(event, "test")
	if err != nil {
		t.Fatalf("BuildNotificationEnvelope() error = %v", err)
	}
	// BuildNotificationEnvelope 根据 action 设置 event_type。测试 mismatch 时单独覆盖 envelope。
	if eventType != "" {
		var envelope eventx.Envelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		envelope.EventType = eventType
		data, err = json.Marshal(envelope)
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
	}
	return kafkax.Message{
		Topic:     eventx.TopicNotificationEvents,
		Key:       []byte(businessKey),
		Value:     data,
		Partition: 0,
		Offset:    1,
	}
}
