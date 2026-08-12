package logic

import (
	"reflect"
	"strings"
	"testing"

	"feedsystem-zero/common/rediskey"

	"gorm.io/gorm/clause"
)

// TestVideoStatDeltaUpdatesRemainCommutative 断言 videos 冷备的增量更新表达式保持"普通加法"，
// 保证 at-least-once 消费与消息乱序场景下的最终收敛。
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

// TestMergeAndSortVideoStatDeltas 断言 flush 聚合按 video_id 升序返回，
// 保证 MySQL 更新顺序稳定，防止行锁死锁。
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

// TestVideoStatsAuthKeyLayout 断言权威 Hash 的 key 命名保持稳定；一旦改动，
// 部署期间可能出现"新旧版本读写不同 key"，需要显式在 CI 上报警。
func TestVideoStatsAuthKeyLayout(t *testing.T) {
	t.Parallel()

	if got, want := rediskey.VideoStatsAuthKey(42), "fsz:video:stats:auth:42"; got != want {
		t.Fatalf("auth key = %q, want %q", got, want)
	}
}

// TestBumpVideoStatsAuthScriptShape 断言权威写入 Lua 脚本保留冷启动 + HINCRBY + EXPIRE 三段结构。
// 这三段决定了"多副本并发首次点赞不会互相覆盖基准值"的正确性，任何调整都需要显式过审。
func TestBumpVideoStatsAuthScriptShape(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		`redis.call("EXISTS", KEYS[1]) == 0`,
		`redis.call("HSET", KEYS[1],`,
		`redis.call("HINCRBY", KEYS[1], "likes_count", ARGV[4])`,
		`redis.call("HINCRBY", KEYS[1], "comments_count", ARGV[5])`,
		`redis.call("HINCRBY", KEYS[1], "popularity", ARGV[6])`,
		`redis.call("EXPIRE", KEYS[1], ARGV[7])`,
	} {
		if !strings.Contains(bumpVideoStatsAuthScript, fragment) {
			t.Fatalf("bumpVideoStatsAuthScript missing %q", fragment)
		}
	}
}

// TestReadVideoStatsAuthScriptShape 断言读侧 Lua 脚本保留"EXISTS 检查 + 冷启动 + HMGET"的结构，
// 保证读侧 miss 时能用 DB 冷备值原子建立基准。
func TestReadVideoStatsAuthScriptShape(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		`redis.call("EXISTS", KEYS[1]) == 0`,
		`redis.call("HSET", KEYS[1],`,
		`redis.call("EXPIRE", KEYS[1], ARGV[4])`,
		`return redis.call("HMGET", KEYS[1], "likes_count", "comments_count", "popularity")`,
	} {
		if !strings.Contains(readVideoStatsAuthScript, fragment) {
			t.Fatalf("readVideoStatsAuthScript missing %q", fragment)
		}
	}
}

// TestParseVideoStatsAuthResultSupportsMixedTypes 断言 Lua 返回值解析兼容 int64/int/string 三种形式。
// go-redis 对 Lua 数组返回值的类型不稳定，任一形式解析失败都会让权威值 fallback 到 0，属于严重故障。
func TestParseVideoStatsAuthResultSupportsMixedTypes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		values []any
		want   videoStatsAuthResult
	}{
		"int64":  {values: []any{int64(3), int64(5), int64(11)}, want: videoStatsAuthResult{LikesCount: 3, CommentsCount: 5, Popularity: 11}},
		"int":    {values: []any{7, 2, 9}, want: videoStatsAuthResult{LikesCount: 7, CommentsCount: 2, Popularity: 9}},
		"string": {values: []any{"12", "34", "56"}, want: videoStatsAuthResult{LikesCount: 12, CommentsCount: 34, Popularity: 56}},
		"short":  {values: []any{int64(1)}, want: videoStatsAuthResult{LikesCount: 1}},
	}
	for name, tt := range tests {
		got := parseVideoStatsAuthResult(tt.values)
		if got != tt.want {
			t.Fatalf("%s: got=%+v want=%+v", name, got, tt.want)
		}
	}
}
