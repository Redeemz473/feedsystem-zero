package logic

import (
	"math"
	"testing"
	"time"

	"feedsystem-zero/apps/feed/internal/config"
	"feedsystem-zero/apps/feed/internal/svc"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeFeedPageSize(t *testing.T) {
	svcCtx := &svc.ServiceContext{Config: config.Config{Timeline: config.TimelineConf{
		DefaultPageSize: 20,
		MaxPageSize:     50,
	}}}
	tests := []struct {
		input int64
		want  int64
	}{
		{input: 0, want: 20},
		{input: -1, want: 20},
		{input: 10, want: 10},
		{input: 50, want: 50},
		{input: 100, want: 50},
	}
	for _, tt := range tests {
		if got := normalizeFeedPageSize(svcCtx, tt.input); got != tt.want {
			t.Fatalf("input=%d got=%d want=%d", tt.input, got, tt.want)
		}
	}
}

func TestValidateFeedCursor(t *testing.T) {
	if err := validateFeedCursor(0, 0); err != nil {
		t.Fatalf("first page cursor should be valid: %v", err)
	}
	if err := validateFeedCursor(time.Now().UnixMilli(), 1); err != nil {
		t.Fatalf("normal cursor should be valid: %v", err)
	}
	if code := status.Code(validateFeedCursor(time.Now().UnixMilli(), 0)); code != codes.InvalidArgument {
		t.Fatalf("unexpected incomplete cursor code: %v", code)
	}
	if code := status.Code(validateFeedCursor(time.Now().Add(time.Hour).UnixMilli(), 1)); code != codes.InvalidArgument {
		t.Fatalf("unexpected future cursor code: %v", code)
	}
}

func TestNormalizeHotFeedQuery(t *testing.T) {
	now := time.Date(2026, time.July, 29, 10, 25, 42, 0, time.FixedZone("UTC+8", 8*60*60))
	svcCtx := &svc.ServiceContext{Config: config.Config{
		Timeline: config.TimelineConf{
			DefaultPageSize: 20,
			MaxPageSize:     50,
		},
		HotRank: config.HotRankConf{
			MaxRankSize:            1000,
			MaxSnapshotAgeSeconds:  1800,
			FutureToleranceSeconds: 300,
		},
	}}

	firstPage, err := normalizeHotFeedQueryAt(svcCtx, 0, 0, 100, now)
	if err != nil {
		t.Fatalf("normalize first page error: %v", err)
	}
	wantSnapshot := now.UTC().Truncate(time.Minute).Unix()
	if firstPage.SnapshotAt != wantSnapshot {
		t.Fatalf("SnapshotAt=%d want=%d", firstPage.SnapshotAt, wantSnapshot)
	}
	if firstPage.PageSize != 50 {
		t.Fatalf("PageSize=%d want=50", firstPage.PageSize)
	}

	if code := status.Code(mustHotFeedQueryError(svcCtx, 0, 20, 20, now)); code != codes.InvalidArgument {
		t.Fatalf("offset without snapshot code=%v want=%v", code, codes.InvalidArgument)
	}
	if code := status.Code(mustHotFeedQueryError(
		svcCtx,
		now.Add(-time.Hour).Unix(),
		0,
		20,
		now,
	)); code != codes.InvalidArgument {
		t.Fatalf("expired snapshot code=%v want=%v", code, codes.InvalidArgument)
	}

	// 五分钟容差内的未来时间固定到服务端当前分钟，不能创建未来榜单。
	futurePage, err := normalizeHotFeedQueryAt(svcCtx, now.Add(2*time.Minute).Unix(), 0, 20, now)
	if err != nil {
		t.Fatalf("normalize tolerated future snapshot error: %v", err)
	}
	if futurePage.SnapshotAt != wantSnapshot {
		t.Fatalf("future SnapshotAt=%d want current minute=%d", futurePage.SnapshotAt, wantSnapshot)
	}
}

func TestHotRankMergeSources(t *testing.T) {
	snapshot := time.Date(2026, time.July, 29, 2, 25, 42, 0, time.UTC)
	keys, weights := hotRankMergeSources(snapshot, 3, 2)
	if len(keys) != 3 || len(weights) != 3 {
		t.Fatalf("sources lengths=(%d,%d) want=(3,3)", len(keys), len(weights))
	}
	wantKeys := []string{
		"fsz:hot:window:202607290225",
		"fsz:hot:window:202607290224",
		"fsz:hot:window:202607290223",
	}
	for index := range wantKeys {
		if keys[index] != wantKeys[index] {
			t.Fatalf("keys[%d]=%q want=%q", index, keys[index], wantKeys[index])
		}
	}
	wantWeights := []float64{1, math.Sqrt(0.5), 0.5}
	for index := range wantWeights {
		if math.Abs(weights[index]-wantWeights[index]) > 1e-12 {
			t.Fatalf("weights[%d]=%f want=%f", index, weights[index], wantWeights[index])
		}
	}
}

func TestDecodeHotRankVideoID(t *testing.T) {
	if got, err := decodeHotRankVideoID("123"); err != nil || got != 123 {
		t.Fatalf("decode string got=%d error=%v", got, err)
	}
	if got, err := decodeHotRankVideoID([]byte("456")); err != nil || got != 456 {
		t.Fatalf("decode bytes got=%d error=%v", got, err)
	}
	if _, err := decodeHotRankVideoID("0"); err == nil {
		t.Fatal("zero video ID should be rejected")
	}
}

func mustHotFeedQueryError(
	svcCtx *svc.ServiceContext,
	snapshotAt int64,
	offset int64,
	pageSize int64,
	now time.Time,
) error {
	_, err := normalizeHotFeedQueryAt(svcCtx, snapshotAt, offset, pageSize, now)
	return err
}
