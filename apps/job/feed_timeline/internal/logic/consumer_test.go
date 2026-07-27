package logic

import (
	"encoding/json"
	"testing"

	"feedsystem-zero/common/eventx"
)

func TestDecodeVideoTimelineEvent(t *testing.T) {
	payload, err := json.Marshal(eventx.FeedVideoEvent{
		EventID: "video-event-1", VideoID: 9, AuthorID: 3, Action: videoActionPublish, OccurredAt: 1700000000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeVideoTimelineEvent(eventx.Envelope{
		EventID: "video-event-1", EventType: eventx.EventTypeVideoPublished,
		AggregateType: eventx.AggregateVideo, AggregateID: "9", OccurredAt: 1700000000000, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.VideoID != 9 || event.AuthorID != 3 || event.Action != videoActionPublish {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestDecodeVideoTimelineEventRejectsActionMismatch(t *testing.T) {
	payload, err := json.Marshal(eventx.FeedVideoEvent{
		EventID: "video-event-2", VideoID: 9, AuthorID: 3, Action: videoActionDelete, OccurredAt: 1700000000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = decodeVideoTimelineEvent(eventx.Envelope{
		EventID: "video-event-2", EventType: eventx.EventTypeVideoPublished,
		AggregateType: eventx.AggregateVideo, AggregateID: "9", Payload: payload,
	})
	if err == nil {
		t.Fatal("expected action mismatch error")
	}
}

func TestDecodeFollowTimelineEvent(t *testing.T) {
	payload, err := json.Marshal(eventx.FollowEvent{
		EventID: "follow-event-1", FollowerID: 4, FollowingID: 8,
		Action: eventx.FollowActionFollow, OccurredAt: 1700000000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeFollowTimelineEvent(eventx.Envelope{
		EventID: "follow-event-1", EventType: eventx.EventTypeFollowCreated,
		AggregateType: eventx.AggregateFollow, AggregateID: "4:8", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.FollowerID != 4 || event.FollowingID != 8 {
		t.Fatalf("unexpected event: %+v", event)
	}
}
