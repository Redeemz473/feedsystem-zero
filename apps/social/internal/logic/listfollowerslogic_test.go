package logic

import (
	"context"
	"testing"
	"time"

	"feedsystem-zero/apps/social/internal/model"
)

func TestFollowListPageBounds(t *testing.T) {
	tests := []struct {
		name               string
		itemCount          int
		pageSize           int
		hasMoreAfterWindow bool
		wantCount          int
		wantHasMore        bool
	}{
		{
			name:        "empty",
			itemCount:   0,
			pageSize:    20,
			wantCount:   0,
			wantHasMore: false,
		},
		{
			name:        "slice smaller page",
			itemCount:   50,
			pageSize:    20,
			wantCount:   20,
			wantHasMore: true,
		},
		{
			name:               "full window with more",
			itemCount:          50,
			pageSize:           50,
			hasMoreAfterWindow: true,
			wantCount:          50,
			wantHasMore:        true,
		},
		{
			name:        "all data returned",
			itemCount:   12,
			pageSize:    20,
			wantCount:   12,
			wantHasMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCount, gotHasMore := followListPageBounds(tt.itemCount, tt.pageSize, tt.hasMoreAfterWindow)
			if gotCount != tt.wantCount {
				t.Fatalf("return count = %d, want %d", gotCount, tt.wantCount)
			}
			if gotHasMore != tt.wantHasMore {
				t.Fatalf("has more = %v, want %v", gotHasMore, tt.wantHasMore)
			}
		})
	}
}

func TestFollowRowsToCacheItemsPreservesOrder(t *testing.T) {
	base := time.Now()
	follows := []model.Follow{
		{ID: 3, FollowerID: 30, FollowingID: 100, UpdatedAt: base},
		{ID: 2, FollowerID: 20, FollowingID: 100, UpdatedAt: base.Add(-time.Second)},
		{ID: 1, FollowerID: 10, FollowingID: 100, UpdatedAt: base.Add(-2 * time.Second)},
	}

	items := followRowsToCacheItems(follows)
	for i, wantID := range []uint64{30, 20, 10} {
		if items[i].FollowerID != wantID {
			t.Fatalf("item %d follower_id = %d, want %d", i, items[i].FollowerID, wantID)
		}
	}
}

func TestBuildListFollowersRespPreservesOrderAndCursor(t *testing.T) {
	logic := &ListFollowersLogic{ctx: context.Background()}
	relations := []followListCacheItem{
		{RelationID: 3, FollowerID: 30, FollowingID: 100, FollowedAt: 3000},
		{RelationID: 2, FollowerID: 20, FollowingID: 100, FollowedAt: 2000},
		{RelationID: 1, FollowerID: 10, FollowingID: 100, FollowedAt: 1000},
	}

	resp, err := logic.buildListFollowersResp(relations, 0, 2, false)
	if err != nil {
		t.Fatalf("build response: %v", err)
	}
	if len(resp.Followers) != 2 {
		t.Fatalf("followers length = %d, want 2", len(resp.Followers))
	}
	if resp.Followers[0].FollowerId != 30 || resp.Followers[1].FollowerId != 20 {
		t.Fatalf("unexpected follower order: %v", []uint64{
			resp.Followers[0].FollowerId,
			resp.Followers[1].FollowerId,
		})
	}
	if !resp.HasMore {
		t.Fatal("has_more = false, want true")
	}
	if resp.NextCursorUpdatedAt != 2000 || resp.NextCursorFollowId != 2 {
		t.Fatalf(
			"next cursor = (%d, %d), want (2000, 2)",
			resp.NextCursorUpdatedAt,
			resp.NextCursorFollowId,
		)
	}
}
