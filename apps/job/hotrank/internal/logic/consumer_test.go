package logic

import (
	"encoding/json"
	"testing"
	"time"

	"feedsystem-zero/apps/job/hotrank/internal/config"
	"feedsystem-zero/apps/job/hotrank/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
)

func TestDecodeLikeEvents(t *testing.T) {
	now := time.Date(2026, time.July, 28, 12, 34, 56, 0, time.UTC)
	consumer := newTestHotRankConsumer(now)

	tests := []struct {
		name      string
		eventType string
		action    string
		delta     int64
		wantScore int64
	}{
		{
			name:      "点赞增加热度",
			eventType: eventx.EventTypeLikeCreated,
			action:    eventx.LikeActionLike,
			delta:     1,
			wantScore: eventx.LikePopularityWeight,
		},
		{
			name:      "取消点赞减少热度",
			eventType: eventx.EventTypeLikeDeleted,
			action:    eventx.LikeActionUnlike,
			delta:     -1,
			wantScore: -eventx.LikePopularityWeight,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := eventx.LikeEvent{
				EventID:    "like_event_1",
				VideoID:    101,
				UserID:     201,
				Action:     tt.action,
				Delta:      tt.delta,
				OccurredAt: now.UnixMilli(),
			}
			msg := makeHotRankMessage(
				t,
				eventx.TopicInteractionLikeEvents,
				eventx.Envelope{
					EventID:       payload.EventID,
					EventType:     tt.eventType,
					AggregateType: eventx.AggregateLike,
					OccurredAt:    payload.OccurredAt,
				},
				payload,
			)

			got, _, err := consumer.decodeMessage(msg)
			if err != nil {
				t.Fatalf("decodeMessage() error = %v", err)
			}
			if got.VideoID != payload.VideoID {
				t.Fatalf("VideoID = %d, want %d", got.VideoID, payload.VideoID)
			}
			if got.ScoreDelta != tt.wantScore {
				t.Fatalf("ScoreDelta = %d, want %d", got.ScoreDelta, tt.wantScore)
			}
			if !got.OccurredAt.Equal(now) {
				t.Fatalf("OccurredAt = %v, want %v", got.OccurredAt, now)
			}
		})
	}
}

func TestDecodeCommentEvents(t *testing.T) {
	now := time.Date(2026, time.July, 28, 12, 34, 56, 0, time.UTC)
	consumer := newTestHotRankConsumer(now)

	tests := []struct {
		name      string
		eventType string
		action    string
		delta     int64
		wantScore int64
	}{
		{
			name:      "发布评论增加热度",
			eventType: eventx.EventTypeCommentCreated,
			action:    eventx.CommentActionCreate,
			delta:     1,
			wantScore: eventx.CommentPopularityWeight,
		},
		{
			name:      "删除评论减少热度",
			eventType: eventx.EventTypeCommentDeleted,
			action:    eventx.CommentActionDelete,
			delta:     -1,
			wantScore: -eventx.CommentPopularityWeight,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := eventx.CommentEvent{
				EventID:    "comment_event_1",
				CommentID:  301,
				VideoID:    101,
				UserID:     201,
				Action:     tt.action,
				Delta:      tt.delta,
				OccurredAt: now.UnixMilli(),
			}
			msg := makeHotRankMessage(
				t,
				eventx.TopicInteractionCommentEvents,
				eventx.Envelope{
					EventID:       payload.EventID,
					EventType:     tt.eventType,
					AggregateType: eventx.AggregateComment,
					OccurredAt:    payload.OccurredAt,
				},
				payload,
			)

			got, _, err := consumer.decodeMessage(msg)
			if err != nil {
				t.Fatalf("decodeMessage() error = %v", err)
			}
			if got.ScoreDelta != tt.wantScore {
				t.Fatalf("ScoreDelta = %d, want %d", got.ScoreDelta, tt.wantScore)
			}
		})
	}
}

func TestDecodeRejectsInconsistentLikeDelta(t *testing.T) {
	now := time.Date(2026, time.July, 28, 12, 34, 56, 0, time.UTC)
	consumer := newTestHotRankConsumer(now)
	payload := eventx.LikeEvent{
		EventID:    "like_event_bad_delta",
		VideoID:    101,
		UserID:     201,
		Action:     eventx.LikeActionLike,
		Delta:      -1,
		OccurredAt: now.UnixMilli(),
	}
	msg := makeHotRankMessage(
		t,
		eventx.TopicInteractionLikeEvents,
		eventx.Envelope{
			EventID:       payload.EventID,
			EventType:     eventx.EventTypeLikeCreated,
			AggregateType: eventx.AggregateLike,
			OccurredAt:    payload.OccurredAt,
		},
		payload,
	)

	if _, _, err := consumer.decodeMessage(msg); err == nil {
		t.Fatal("decodeMessage() expected inconsistent delta error")
	}
}

func TestDecodeRejectsFutureEvent(t *testing.T) {
	now := time.Date(2026, time.July, 28, 12, 34, 56, 0, time.UTC)
	consumer := newTestHotRankConsumer(now)
	future := now.Add(defaultHotRankFutureTolerance + time.Second)
	payload := eventx.LikeEvent{
		EventID:    "like_event_future",
		VideoID:    101,
		UserID:     201,
		Action:     eventx.LikeActionLike,
		Delta:      1,
		OccurredAt: future.UnixMilli(),
	}
	msg := makeHotRankMessage(
		t,
		eventx.TopicInteractionLikeEvents,
		eventx.Envelope{
			EventID:       payload.EventID,
			EventType:     eventx.EventTypeLikeCreated,
			AggregateType: eventx.AggregateLike,
			OccurredAt:    payload.OccurredAt,
		},
		payload,
	)

	if _, _, err := consumer.decodeMessage(msg); err == nil {
		t.Fatal("decodeMessage() expected future timestamp error")
	}
}

func TestHotRankWindowUsesUTCMinute(t *testing.T) {
	location := time.FixedZone("UTC+8", 8*60*60)
	occurredAt := time.Date(2026, time.July, 28, 20, 34, 56, 0, location)

	if got, want := hotRankWindowBucket(occurredAt), "202607281234"; got != want {
		t.Fatalf("hotRankWindowBucket() = %s, want %s", got, want)
	}

	gotExpire := hotRankWindowExpireAt(occurredAt, 2*time.Hour)
	wantExpire := time.Date(2026, time.July, 28, 14, 34, 0, 0, time.UTC)
	if !gotExpire.Equal(wantExpire) {
		t.Fatalf("hotRankWindowExpireAt() = %v, want %v", gotExpire, wantExpire)
	}
}

func TestGroupHotRankMessagesKeepsPartitionOrder(t *testing.T) {
	messages := []kafkax.Message{
		{Topic: eventx.TopicInteractionLikeEvents, Partition: 1, Offset: 10},
		{Topic: eventx.TopicInteractionCommentEvents, Partition: 0, Offset: 20},
		{Topic: eventx.TopicInteractionLikeEvents, Partition: 1, Offset: 11},
	}

	groups := groupHotRankMessages(messages)
	if len(groups) != 2 {
		t.Fatalf("len(groups) = %d, want 2", len(groups))
	}
	if len(groups[0].Messages) != 2 {
		t.Fatalf("len(groups[0].Messages) = %d, want 2", len(groups[0].Messages))
	}
	if groups[0].Messages[0].Offset != 10 || groups[0].Messages[1].Offset != 11 {
		t.Fatalf("partition order changed: %+v", groups[0].Messages)
	}
}

func TestHotRankDeadLetterPayloadHandlesInvalidUTF8(t *testing.T) {
	got := hotRankDeadLetterPayload([]byte{0xff, 0xfe})
	if got != "base64://4=" {
		t.Fatalf("hotRankDeadLetterPayload() = %q, want base64 payload", got)
	}
}

func newTestHotRankConsumer(now time.Time) *HotRankConsumer {
	return &HotRankConsumer{
		svcCtx: &svc.ServiceContext{
			Config: config.Config{},
		},
		now: func() time.Time {
			return now
		},
	}
}

func makeHotRankMessage(
	t *testing.T,
	topic string,
	envelope eventx.Envelope,
	payload any,
) kafkax.Message {
	t.Helper()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	envelope.Payload = payloadBytes

	value, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal(envelope) error = %v", err)
	}
	return kafkax.Message{
		Topic:     topic,
		Partition: 0,
		Offset:    1,
		Value:     value,
	}
}
