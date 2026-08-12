package logic

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"feedsystem-zero/common/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
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
	versionExpr, ok := updates["stats_version"].(clause.Expr)
	if !ok || versionExpr.SQL != "stats_version + 1" {
		t.Fatalf("stats_version update = %#v, want stats_version + 1", updates["stats_version"])
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

// TestBumpVideoStatsAuthScriptShape 断言在线乐观写入保留冷启动、版本兼容、增量和续期结构。
// 这三段决定了"多副本并发首次点赞不会互相覆盖基准值"的正确性，任何调整都需要显式过审。
func TestBumpVideoStatsAuthScriptShape(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		`redis.call("EXISTS", KEYS[1]) == 0`,
		`redis.call("HSET", KEYS[1],`,
		`not tonumber(redis.call("HGET", KEYS[1], "stats_version"))`,
		`redis.call("HINCRBY", KEYS[1], "likes_count", ARGV[5])`,
		`redis.call("HINCRBY", KEYS[1], "comments_count", ARGV[6])`,
		`redis.call("HINCRBY", KEYS[1], "popularity", ARGV[7])`,
		`redis.call("EXPIRE", KEYS[1], ARGV[8])`,
	} {
		if !strings.Contains(bumpVideoStatsAuthScript, fragment) {
			t.Fatalf("bumpVideoStatsAuthScript missing %q", fragment)
		}
	}
}

// TestReadVideoStatsAuthScriptShape 断言读侧 Lua 保留版本比较、冷启动、续期和完整字段读取。
func TestReadVideoStatsAuthScriptShape(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		`current_version_number < tonumber(ARGV[4])`,
		`redis.call("HSET", KEYS[1],`,
		`redis.call("EXPIRE", KEYS[1], ARGV[5])`,
		`return redis.call("HMGET", KEYS[1], "likes_count", "comments_count", "popularity", "stats_version")`,
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
		"int64":  {values: []any{int64(3), int64(5), int64(11), int64(2)}, want: videoStatsAuthResult{LikesCount: 3, CommentsCount: 5, Popularity: 11, StatsVersion: 2}},
		"int":    {values: []any{7, 2, 9, 4}, want: videoStatsAuthResult{LikesCount: 7, CommentsCount: 2, Popularity: 9, StatsVersion: 4}},
		"string": {values: []any{"12", "34", "56", "8"}, want: videoStatsAuthResult{LikesCount: 12, CommentsCount: 34, Popularity: 56, StatsVersion: 8}},
		"short":  {values: []any{int64(1)}, want: videoStatsAuthResult{LikesCount: 1}},
	}
	for name, tt := range tests {
		got := parseVideoStatsAuthResult(tt.values)
		if got != tt.want {
			t.Fatalf("%s: got=%+v want=%+v", name, got, tt.want)
		}
	}
}

func TestParseVideoStatsAuthHashRejectsPartialOrCorruptValues(t *testing.T) {
	t.Parallel()

	valid := map[string]string{
		"likes_count": "3", "comments_count": "4", "popularity": "29", "stats_version": "7",
	}
	if got, ok := parseVideoStatsAuthHash(valid); !ok || got.StatsVersion != 7 || got.Popularity != 29 {
		t.Fatalf("valid hash parsed as got=%+v ok=%v", got, ok)
	}
	for name, values := range map[string]map[string]string{
		"legacy_without_version": {"likes_count": "3", "comments_count": "4", "popularity": "29"},
		"corrupt_count":          {"likes_count": "x", "comments_count": "4", "popularity": "29", "stats_version": "7"},
		"negative_version":       {"likes_count": "3", "comments_count": "4", "popularity": "29", "stats_version": "-1"},
	} {
		if got, ok := parseVideoStatsAuthHash(values); ok {
			t.Fatalf("%s parsed unexpectedly: %+v", name, got)
		}
	}
	legacy := map[string]string{"likes_count": "3", "comments_count": "4", "popularity": "29"}
	if got, ok := parseLegacyVideoStatsAuthHash(legacy); !ok || got.LikesCount != 3 {
		t.Fatalf("legacy hash parsed as got=%+v ok=%v", got, ok)
	}
	if _, ok := parseLegacyVideoStatsAuthHash(valid); ok {
		t.Fatal("versioned hash must not be treated as legacy")
	}
}

// TestProjectVideoStatsBatchVersionCAS 验证多个 Consumer 并发投影时旧版本不能覆盖新版本，
// 而相同版本重放可以修复一次损坏/失写的 Redis 快照。
func TestProjectVideoStatsBatchVersionCAS(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	videoID := uint64(42)

	var wg sync.WaitGroup
	for version := uint64(1); version <= 32; version++ {
		version := version
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = projectVideoStatsBatch(ctx, client, []videoStatsProjection{{
				VideoID: videoID,
				Stats: videoStatsAuthResult{
					LikesCount: int64(version), Popularity: int64(version * 3), StatsVersion: version,
				},
			}})
		}()
	}
	wg.Wait()

	key := rediskey.VideoStatsAuthKey(videoID)
	hashValues, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	got, ok := parseVideoStatsAuthHash(hashValues)
	if !ok || got.StatsVersion != 32 || got.LikesCount != 32 || got.Popularity != 96 {
		t.Fatalf("concurrent projection got=%+v ok=%v", got, ok)
	}

	if err := client.HSet(ctx, key, "likes_count", 999).Err(); err != nil {
		t.Fatal(err)
	}
	if err := projectVideoStatsBatch(ctx, client, []videoStatsProjection{{
		VideoID: videoID,
		Stats:   videoStatsAuthResult{LikesCount: 32, Popularity: 96, StatsVersion: 32},
	}}); err != nil {
		t.Fatal(err)
	}
	if got := client.HGet(ctx, key, "likes_count").Val(); got != "32" {
		t.Fatalf("same-version replay did not repair hash, likes_count=%s", got)
	}

	if err := client.HSet(ctx, key, "stats_version", "broken", "likes_count", "broken").Err(); err != nil {
		t.Fatal(err)
	}
	if err := projectVideoStatsBatch(ctx, client, []videoStatsProjection{{
		VideoID: videoID,
		Stats:   videoStatsAuthResult{LikesCount: 33, Popularity: 99, StatsVersion: 33},
	}}); err != nil {
		t.Fatal(err)
	}
	hashValues, err = client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	got, ok = parseVideoStatsAuthHash(hashValues)
	if !ok || got.StatsVersion != 33 || got.LikesCount != 33 {
		t.Fatalf("corrupt projection was not repaired, got=%+v ok=%v", got, ok)
	}
}

func TestProjectVideoStatsBatchReturnsRedisFailure(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(), DialTimeout: 50 * time.Millisecond, ReadTimeout: 50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond, MaxRetries: 0,
	})
	mr.Close()
	t.Cleanup(func() { _ = client.Close() })

	err := projectVideoStatsBatch(context.Background(), client, []videoStatsProjection{{
		VideoID: 1,
		Stats:   videoStatsAuthResult{StatsVersion: 1},
	}})
	if err == nil {
		t.Fatal("projectVideoStatsBatch error=nil, want redis connection failure")
	}
}

func TestReadVideoStatsAuthPreservesLegacyPendingCounts(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	key := rediskey.VideoStatsAuthKey(9)
	if err := client.HSet(ctx, key,
		"likes_count", 12,
		"comments_count", 4,
		"popularity", 56,
	).Err(); err != nil {
		t.Fatal(err)
	}

	got, err := readVideoStatsAuthWithBase(ctx, client, 9, videoStatsAuthResult{
		LikesCount: 10, CommentsCount: 4, Popularity: 50, StatsVersion: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.LikesCount != 12 || got.CommentsCount != 4 || got.Popularity != 56 || got.StatsVersion != 7 {
		t.Fatalf("legacy hash pending counts were overwritten: %+v", got)
	}
}
