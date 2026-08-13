package logic

import (
	"context"
	"testing"
	"time"

	"feedsystem-zero/apps/interaction/internal/model"
	"feedsystem-zero/apps/interaction/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestIsLikedVideosFirstPageCacheable(t *testing.T) {
	tests := []struct {
		name            string
		cursorCreatedAt int64
		cursorLikeID    uint64
		pageSize        int64
		want            bool
	}{
		{name: "small first page", pageSize: 5, want: true},
		{name: "full first page window", pageSize: 20, want: true},
		{name: "large first page", pageSize: 21},
		{name: "history page", cursorCreatedAt: 1, cursorLikeID: 1, pageSize: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isLikedVideosFirstPageCacheable(tt.cursorCreatedAt, tt.cursorLikeID, tt.pageSize)
			if got != tt.want {
				t.Fatalf("cacheable = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLikedVideosPageBounds(t *testing.T) {
	tests := []struct {
		name               string
		itemCount          int
		pageSize           int64
		hasMoreAfterWindow bool
		wantCount          int
		wantHasMore        bool
	}{
		{name: "empty", pageSize: 20},
		{name: "slice small page", itemCount: 20, pageSize: 5, wantCount: 5, wantHasMore: true},
		{name: "complete window", itemCount: 20, pageSize: 20, wantCount: 20},
		{name: "data after window", itemCount: 20, pageSize: 20, hasMoreAfterWindow: true, wantCount: 20, wantHasMore: true},
		{name: "short result", itemCount: 8, pageSize: 20, wantCount: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, hasMore := likedVideosPageBounds(tt.itemCount, tt.pageSize, tt.hasMoreAfterWindow)
			if count != tt.wantCount || hasMore != tt.wantHasMore {
				t.Fatalf("bounds = (%d, %v), want (%d, %v)", count, hasMore, tt.wantCount, tt.wantHasMore)
			}
		})
	}
}

func TestSaveLikedVideosFirstPageCacheInitializesMissingVersion(t *testing.T) {
	l, client, mr := newLikedVideosCacheTestLogic(t)
	const userID uint64 = 42
	const version int64 = 0
	cacheKey := rediskey.LikeUserVideosFirstPageCacheKey(userID, version)
	lockKey := rediskey.LikeUserVideosFirstPageCacheBuildLockKey(cacheKey)
	const lockToken = "owner"
	if err := client.Set(context.Background(), lockKey, lockToken, likedVideosListCacheLockTTL).Err(); err != nil {
		t.Fatal(err)
	}

	likes := likedVideosTestRows(20)
	l.saveLikedVideosFirstPageCache(userID, cacheKey, version, lockKey, lockToken, likes, true)

	if got, err := client.Get(context.Background(), rediskey.LikeUserVideosListVersionKey(userID)).Int64(); err != nil || got != version {
		t.Fatalf("version = %d, err = %v, want %d", got, err, version)
	}
	resp, hit := l.loadLikedVideosFirstPageCache(cacheKey, version, 5)
	if !hit {
		t.Fatal("cache hit = false, want true")
	}
	if len(resp.GetLikedVideos()) != 5 || !resp.GetHasMore() {
		t.Fatalf("response size = %d, has_more = %v, want 5, true", len(resp.GetLikedVideos()), resp.GetHasMore())
	}
	if resp.GetNextCursorLikeId() != likes[4].ID || resp.GetNextCursorCreatedAt() != likes[4].UpdatedAt.UnixMilli() {
		t.Fatal("cursor was not built from the last returned item")
	}
	if ttl := mr.TTL(cacheKey); ttl <= 0 || ttl > likedVideosListCacheTTL {
		t.Fatalf("cache TTL = %v, want (0, %v]", ttl, likedVideosListCacheTTL)
	}
}

func TestSaveLikedVideosFirstPageCacheRejectsStaleVersionAndWrongLock(t *testing.T) {
	tests := []struct {
		name           string
		currentVersion string
		storedToken    string
		writerToken    string
	}{
		{name: "stale version", currentVersion: "8", storedToken: "owner", writerToken: "owner"},
		{name: "wrong lock owner", currentVersion: "7", storedToken: "new-owner", writerToken: "expired-owner"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, client, mr := newLikedVideosCacheTestLogic(t)
			const userID uint64 = 7
			const queriedVersion int64 = 7
			cacheKey := rediskey.LikeUserVideosFirstPageCacheKey(userID, queriedVersion)
			lockKey := rediskey.LikeUserVideosFirstPageCacheBuildLockKey(cacheKey)
			ctx := context.Background()
			if err := client.Set(ctx, rediskey.LikeUserVideosListVersionKey(userID), tt.currentVersion, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.Set(ctx, lockKey, tt.storedToken, likedVideosListCacheLockTTL).Err(); err != nil {
				t.Fatal(err)
			}

			l.saveLikedVideosFirstPageCache(userID, cacheKey, queriedVersion, lockKey, tt.writerToken, likedVideosTestRows(20), true)
			if mr.Exists(cacheKey) {
				t.Fatal("stale or unlocked request wrote the cache")
			}
		})
	}
}

func newLikedVideosCacheTestLogic(t *testing.T) (*ListMyLikedVideosLogic, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	l := NewListMyLikedVideosLogic(context.Background(), &svc.ServiceContext{RedisCli: client})
	return l, client, mr
}

func likedVideosTestRows(size int) []model.Like {
	base := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.Local)
	likes := make([]model.Like, 0, size)
	for i := 0; i < size; i++ {
		likes = append(likes, model.Like{
			ID:        uint64(i + 1),
			VideoID:   uint64(1000 + i),
			UpdatedAt: base.Add(-time.Duration(i) * time.Second),
		})
	}
	return likes
}
