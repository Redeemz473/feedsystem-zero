package logic

import (
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
