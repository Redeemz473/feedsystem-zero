package logic

import (
	"testing"
	"time"

	"feedsystem-zero/apps/interaction/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeListCommentsPageSize(t *testing.T) {
	tests := []struct {
		name     string
		pageSize int64
		want     int64
		wantCode codes.Code
	}{
		{name: "default", pageSize: 0, want: defaultListCommentsPageSize},
		{name: "requested", pageSize: 12, want: 12},
		{name: "capped", pageSize: 101, want: maxListCommentsPageSize},
		{name: "negative", pageSize: -1, wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeListCommentsPageSize(tt.pageSize)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("error code = %v, want %v", status.Code(err), tt.wantCode)
			}
			if got != tt.want {
				t.Fatalf("page size = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCommentPageBounds(t *testing.T) {
	tests := []struct {
		name               string
		itemCount          int
		pageSize           int64
		hasMoreAfterWindow bool
		wantCount          int
		wantHasMore        bool
	}{
		{
			name:      "empty window",
			pageSize:  20,
			wantCount: 0,
		},
		{
			name:        "small request slices cached window",
			itemCount:   20,
			pageSize:    5,
			wantCount:   5,
			wantHasMore: true,
		},
		{
			name:      "full window without more data",
			itemCount: 20,
			pageSize:  20,
			wantCount: 20,
		},
		{
			name:               "full window with twenty first row",
			itemCount:          20,
			pageSize:           20,
			hasMoreAfterWindow: true,
			wantCount:          20,
			wantHasMore:        true,
		},
		{
			name:      "database contains fewer comments than request",
			itemCount: 12,
			pageSize:  20,
			wantCount: 12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCount, gotHasMore := commentPageBounds(tt.itemCount, tt.pageSize, tt.hasMoreAfterWindow)
			if gotCount != tt.wantCount {
				t.Fatalf("count = %d, want %d", gotCount, tt.wantCount)
			}
			if gotHasMore != tt.wantHasMore {
				t.Fatalf("hasMore = %v, want %v", gotHasMore, tt.wantHasMore)
			}
		})
	}
}

func TestIsCommentFirstPageCacheable(t *testing.T) {
	tests := []struct {
		name            string
		cursorCreatedAt int64
		cursorCommentID uint64
		pageSize        int64
		wantCacheable   bool
	}{
		{
			name:          "small first page",
			pageSize:      5,
			wantCacheable: true,
		},
		{
			name:          "full cached window",
			pageSize:      20,
			wantCacheable: true,
		},
		{
			name:     "large first page",
			pageSize: 21,
		},
		{
			name:            "history page",
			cursorCreatedAt: 1,
			cursorCommentID: 1,
			pageSize:        20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCommentFirstPageCacheable(tt.cursorCreatedAt, tt.cursorCommentID, tt.pageSize)
			if got != tt.wantCacheable {
				t.Fatalf("cacheable = %v, want %v", got, tt.wantCacheable)
			}
		})
	}
}

func TestCommentWindowBuildsCursorFromReturnedLastItem(t *testing.T) {
	baseTime := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.Local)
	comments := make([]model.Comment, 0, 20)
	for i := 0; i < 20; i++ {
		comments = append(comments, model.Comment{
			ID:        uint64(i + 1),
			VideoID:   100,
			UserID:    uint64(i + 10),
			Username:  "user",
			Content:   "comment",
			CreatedAt: baseTime.Add(-time.Duration(i) * time.Second),
			UpdatedAt: baseTime.Add(-time.Duration(i) * time.Second),
		})
	}

	selected, hasMore := selectCommentPage(comments, 5, false)
	resp := buildListCommentsResp(selected, 0, 99, hasMore)

	if len(resp.GetComments()) != 5 {
		t.Fatalf("comments length = %d, want 5", len(resp.GetComments()))
	}
	if !resp.GetHasMore() {
		t.Fatal("hasMore = false, want true")
	}
	if resp.GetNextCursorCommentId() != 5 {
		t.Fatalf("next cursor comment id = %d, want 5", resp.GetNextCursorCommentId())
	}
	if resp.GetNextCursorCreatedAt() != comments[4].CreatedAt.UnixMilli() {
		t.Fatalf(
			"next cursor created at = %d, want %d",
			resp.GetNextCursorCreatedAt(),
			comments[4].CreatedAt.UnixMilli(),
		)
	}
	for _, item := range resp.GetComments() {
		if item.GetCanDelete() {
			t.Fatal("guest response contains deletable comment")
		}
	}
}
