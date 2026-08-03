package logic

import (
	"reflect"
	"strings"
	"testing"

	"feedsystem-zero/common/rediskey"

	"gorm.io/gorm/clause"
)

func TestVideoStatDeltaUpdatesRemainCommutative(t *testing.T) {
	t.Parallel()

	updates := videoStatDeltaUpdates(videoStatDelta{
		LikeDelta:       -1,
		CommentDelta:    -1,
		PopularityDelta: -8,
	})
	tests := map[string]struct {
		sql  string
		want int64
	}{
		"likes_count":    {sql: "likes_count + ?", want: -1},
		"comments_count": {sql: "comments_count + ?", want: -1},
		"popularity":     {sql: "popularity + ?", want: -8},
	}
	for field, tt := range tests {
		expr, ok := updates[field].(clause.Expr)
		if !ok {
			t.Fatalf("%s update type = %T, want clause.Expr", field, updates[field])
		}
		if expr.SQL != tt.sql {
			t.Fatalf("%s SQL = %q, want %q", field, expr.SQL, tt.sql)
		}
		if len(expr.Vars) != 1 || expr.Vars[0] != tt.want {
			t.Fatalf("%s vars = %#v, want [%d]", field, expr.Vars, tt.want)
		}
		if strings.Contains(strings.ToLower(expr.SQL), "greatest") {
			t.Fatalf("%s must not clamp each delta: %s", field, expr.SQL)
		}
	}
}

func TestMergeAndSortVideoStatDeltas(t *testing.T) {
	t.Parallel()

	deltas := map[uint64]videoStatDelta{}
	deltas[20] = mergeVideoStatDelta(deltas[20], videoStatDelta{LikeDelta: 1, PopularityDelta: 3})
	deltas[10] = mergeVideoStatDelta(deltas[10], videoStatDelta{CommentDelta: 1, PopularityDelta: 5})
	deltas[20] = mergeVideoStatDelta(deltas[20], videoStatDelta{LikeDelta: -1, PopularityDelta: -3})

	if got, want := sortedVideoStatDeltaIDs(deltas), []uint64{10, 20}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted ids = %v, want %v", got, want)
	}
	if got := deltas[20]; got != (videoStatDelta{}) {
		t.Fatalf("video 20 delta = %+v, want zero net delta", got)
	}
	if got := deltas[10]; got.CommentDelta != 1 || got.PopularityDelta != 5 {
		t.Fatalf("video 10 delta = %+v", got)
	}
}

func TestInteractionFlushLeaseScriptsAreMutuallyExclusiveWithRebuild(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		`redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now_ms)`,
		`redis.call("EXISTS", KEYS[2]) == 1`,
		`redis.call("ZADD", KEYS[1], expires_at, ARGV[1])`,
	} {
		if !strings.Contains(acquireInteractionStatsMutationLeaseScript, fragment) {
			t.Fatalf("flush lease script missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		`redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")`,
	} {
		if !strings.Contains(acquireInteractionRebuildLockScript, fragment) {
			t.Fatalf("rebuild lock script missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		`redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now_ms)`,
		`return redis.call("ZCARD", KEYS[1])`,
	} {
		if !strings.Contains(countInteractionStatsMutationLeasesScript, fragment) {
			t.Fatalf("mutation drain script missing %q", fragment)
		}
	}
}

func TestInteractionDeltaScriptsEnforcePendingCountInvariant(t *testing.T) {
	t.Parallel()

	if got, want := rediskey.InteractionDeltaPendingCountKey(42), "fsz:interaction:delta:pending_count:42"; got != want {
		t.Fatalf("pending count key = %q, want %q", got, want)
	}
	for _, fragment := range []string{
		`redis.call("INCR", KEYS[6])`,
		`redis.call("EXPIRE", KEYS[6], ARGV[5])`,
	} {
		if !strings.Contains(applyInteractionDeltaScript, fragment) {
			t.Fatalf("apply delta script missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		`local remaining = redis.call("DECR", KEYS[6])`,
		`redis.call("HDEL", KEYS[3], ARGV[1])`,
		`redis.call("HDEL", KEYS[4], ARGV[1])`,
		`redis.call("HDEL", KEYS[5], ARGV[1])`,
	} {
		if !strings.Contains(acknowledgeInteractionDeltaScript, fragment) {
			t.Fatalf("ack delta script missing %q", fragment)
		}
	}
}
