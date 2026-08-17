package logic

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"feedsystem-zero/apps/job/feed_timeline/internal/config"
	"feedsystem-zero/apps/job/feed_timeline/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestInvalidateUserTimelineRemovesSnapshotAndAdvancesVersion(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	const userID uint64 = 42
	ctx := context.Background()
	if err := rdb.ZAdd(ctx, rediskey.FeedTimelineKey(userID), redis.Z{
		Score:  0,
		Member: "00000000000000000001:00000000000000000001",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, rediskey.FeedTimelineReadyKey(userID), "1", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, rediskey.FeedTimelineVersionKey(userID), "7", time.Hour).Err(); err != nil {
		t.Fatal(err)
	}

	consumer := NewTimelineConsumer(&svc.ServiceContext{
		Config: config.Config{Timeline: config.TimelineConf{
			UserTimelineTTLSeconds: 3600,
			RedisOpTimeoutMs:       1000,
		}},
		RedisCli: rdb,
	})
	if err := consumer.invalidateUserTimeline(ctx, userID); err != nil {
		t.Fatalf("invalidate user timeline: %v", err)
	}

	if count := mr.Exists(rediskey.FeedTimelineKey(userID)); count {
		t.Fatal("timeline key should be deleted")
	}
	if count := mr.Exists(rediskey.FeedTimelineReadyKey(userID)); count {
		t.Fatal("ready key should be deleted")
	}
	if version, err := rdb.Get(ctx, rediskey.FeedTimelineVersionKey(userID)).Int64(); err != nil || version != 8 {
		t.Fatalf("version=%d error=%v want=8", version, err)
	}
}

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
