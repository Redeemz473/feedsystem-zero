package eventx

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildNotificationEnvelope(t *testing.T) {
	event := NotificationEvent{
		EventID:          "notify_1",
		SourceEventID:    "like_1",
		ReceiverID:       10,
		ActorID:          20,
		VideoID:          30,
		NotificationType: NotificationTypeVideoLike,
		Action:           NotificationActionCreate,
		OccurredAt:       time.Now().UnixMilli(),
	}
	data, businessKey, err := BuildNotificationEnvelope(event, "interaction-rpc")
	if err != nil {
		t.Fatalf("BuildNotificationEnvelope() error = %v", err)
	}
	if businessKey != "like:10:20:30" {
		t.Fatalf("businessKey = %q, want like:10:20:30", businessKey)
	}

	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if envelope.EventType != EventTypeNotificationCreate {
		t.Fatalf("EventType = %q, want %q", envelope.EventType, EventTypeNotificationCreate)
	}
	if envelope.AggregateID != businessKey {
		t.Fatalf("AggregateID = %q, want %q", envelope.AggregateID, businessKey)
	}
}

func TestValidateNotificationEventRejectsSelfNotification(t *testing.T) {
	event := NotificationEvent{
		EventID:          "notify_self",
		SourceEventID:    "follow_self",
		ReceiverID:       10,
		ActorID:          10,
		NotificationType: NotificationTypeFollow,
		Action:           NotificationActionCreate,
		OccurredAt:       time.Now().UnixMilli(),
	}
	if err := ValidateNotificationEvent(event); err == nil {
		t.Fatal("ValidateNotificationEvent() expected self notification error")
	}
}
