package logic

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/feed/feedclient"
	"feedsystem-zero/apps/gateway/internal/config"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/interaction/interactionclient"
	"feedsystem-zero/apps/video/videoclient"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

type blockingHotFeedRPC struct {
	feedclient.Feed
	calls   atomic.Int64
	started chan<- struct{}
	release <-chan struct{}
}

func (rpc *blockingHotFeedRPC) GetHotFeed(
	ctx context.Context,
	_ *feedclient.GetHotFeedReq,
	_ ...grpc.CallOption,
) (*feedclient.GetHotFeedResp, error) {
	rpc.calls.Add(1)
	select {
	case rpc.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-rpc.release:
	}
	return &feedclient.GetHotFeedResp{
		Items:      []*feedclient.HotFeedVideoItem{{VideoId: 11, HotScore: 9.5, Rank: 1}},
		SnapshotAt: 12345,
	}, nil
}

type countingVideoRPC struct {
	videoclient.Video
	calls atomic.Int64
}

func (rpc *countingVideoRPC) BatchGetVideos(
	context.Context,
	*videoclient.BatchGetVideosReq,
	...grpc.CallOption,
) (*videoclient.BatchGetVideosResp, error) {
	rpc.calls.Add(1)
	return &videoclient.BatchGetVideosResp{Videos: []*videoclient.VideoInfo{{
		VideoId: 11, AuthorId: 22, Title: "video",
	}}}, nil
}

type countingAccountRPC struct {
	accountclient.Account
	calls atomic.Int64
}

func (rpc *countingAccountRPC) BatchGetProfiles(
	context.Context,
	*accountclient.BatchGetProfilesReq,
	...grpc.CallOption,
) (*accountclient.BatchGetProfilesResp, error) {
	rpc.calls.Add(1)
	return &accountclient.BatchGetProfilesResp{Profiles: []*accountclient.PublicProfile{{
		UserId: 22, Username: "author",
	}}}, nil
}

type countingInteractionRPC struct {
	interactionclient.Interaction
	calls atomic.Int64
}

func (rpc *countingInteractionRPC) BatchGetVideoStats(
	context.Context,
	*interactionclient.BatchGetVideoStatsReq,
	...grpc.CallOption,
) (*interactionclient.BatchGetVideoStatsResp, error) {
	rpc.calls.Add(1)
	return &interactionclient.BatchGetVideoStatsResp{Stats: []*interactionclient.VideoInteractionStats{{
		VideoId: 11, LikesCount: 7, Popularity: 21,
	}}}, nil
}

func TestAnonymousHotFeedPageCacheKeyUsesCurrentUTCMinute(t *testing.T) {
	now := time.Date(2026, 8, 13, 3, 9, 42, 0, time.FixedZone("CST", 8*60*60))
	req := &types.GetHotFeedReq{Pagesize: 20}

	first, ok := anonymousHotFeedPageCacheKey(req, now)
	if !ok {
		t.Fatal("current hot feed request should be cacheable")
	}
	sameMinute, _ := anonymousHotFeedPageCacheKey(req, now.Add(10*time.Second))
	if first != sameMinute {
		t.Fatalf("same minute must share key: first=%q second=%q", first, sameMinute)
	}
	nextMinute, _ := anonymousHotFeedPageCacheKey(req, now.Add(30*time.Second))
	if first == nextMinute {
		t.Fatalf("different minutes must not share key: %q", first)
	}
	explicitDefault, _ := anonymousHotFeedPageCacheKey(&types.GetHotFeedReq{Pagesize: 20}, now)
	if first != explicitDefault {
		t.Fatalf("default and explicit page size must share key: default=%q explicit=%q", first, explicitDefault)
	}
	capped, _ := anonymousHotFeedPageCacheKey(&types.GetHotFeedReq{Pagesize: 100}, now)
	explicitMax, _ := anonymousHotFeedPageCacheKey(&types.GetHotFeedReq{Pagesize: 50}, now)
	if capped != explicitMax {
		t.Fatalf("capped and max page size must share key: capped=%q max=%q", capped, explicitMax)
	}

	historyReq := &types.GetHotFeedReq{Snapshotat: 12355, Offset: 20, Pagesize: 20}
	historyFirst, _ := anonymousHotFeedPageCacheKey(historyReq, now)
	historyLater, _ := anonymousHotFeedPageCacheKey(historyReq, now.Add(time.Hour))
	if historyFirst != historyLater {
		t.Fatalf("fixed snapshot pagination key changed: first=%q later=%q", historyFirst, historyLater)
	}
	normalizedHistory, _ := anonymousHotFeedPageCacheKey(
		&types.GetHotFeedReq{Snapshotat: 12345, Offset: 20, Pagesize: 20},
		now,
	)
	if historyFirst != normalizedHistory {
		t.Fatalf("timestamps in same minute must share key: first=%q normalized=%q", historyFirst, normalizedHistory)
	}

	if _, ok := anonymousHotFeedPageCacheKey(&types.GetHotFeedReq{Offset: -1}, now); ok {
		t.Fatal("invalid request must bypass cache and be validated by feed-rpc")
	}
}

func TestAnonymousHotFeedPageCacheRoundTripAndTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	redisCli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisCli.Close() })

	svcCtx := &svc.ServiceContext{
		Config: config.Config{HotFeedCache: config.HotFeedCacheConf{
			AnonymousTTLMilliseconds: 1000,
			RedisOpTimeoutMs:         100,
		}},
		RedisCli: redisCli,
	}
	logic := NewGetHotFeedLogic(context.Background(), svcCtx)
	cacheKey := "test:hot-feed-page"
	want := &types.GetHotFeedResp{
		Items: []types.HotFeedVideo{{
			Video: types.VideoInfo{
				Videoid:        11,
				Authorid:       22,
				Authorusername: "author",
				Likescount:     7,
				Tags:           []string{"go", "kafka"},
			},
			Hotscore: 9.5,
			Rank:     1,
		}},
		Snapshotat: 12345,
		Nextoffset: 20,
		Hasmore:    true,
	}

	logic.saveAnonymousHotFeedPage(cacheKey, want)
	got, hit := logic.loadAnonymousHotFeedPage(cacheKey)
	if !hit {
		t.Fatal("saved page cache was not found")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cache round trip mismatch:\n got=%+v\nwant=%+v", got, want)
	}

	mr.FastForward(1100 * time.Millisecond)
	if _, hit := logic.loadAnonymousHotFeedPage(cacheKey); hit {
		t.Fatal("expired page cache must miss")
	}
}

func TestAnonymousHotFeedCacheCollapsesConcurrentBuilds(t *testing.T) {
	mr := miniredis.RunT(t)
	redisCli := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisCli.Close() })

	feedStarted := make(chan struct{}, 1)
	releaseFeed := make(chan struct{})
	feedRPC := &blockingHotFeedRPC{started: feedStarted, release: releaseFeed}
	videoRPC := &countingVideoRPC{}
	accountRPC := &countingAccountRPC{}
	interactionRPC := &countingInteractionRPC{}
	svcCtx := &svc.ServiceContext{
		Config: config.Config{HotFeedCache: config.HotFeedCacheConf{
			Enabled:                  true,
			AnonymousTTLMilliseconds: 2000,
			RedisOpTimeoutMs:         100,
			BuildLockTTLMilliseconds: 1000,
			BuildWaitMs:              200,
		}},
		RedisCli:       redisCli,
		FeedRpc:        feedRPC,
		VideoRpc:       videoRPC,
		AccountRpc:     accountRPC,
		InteractionRpc: interactionRPC,
	}

	const concurrency = 20
	start := make(chan struct{})
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-start
			logic := NewGetHotFeedLogic(context.Background(), svcCtx)
			resp, err := logic.GetHotFeed(&types.GetHotFeedReq{Pagesize: 20})
			if err == nil && (resp == nil || len(resp.Items) != 1 || resp.Items[0].Video.Videoid != 11) {
				err = context.Canceled
			}
			errCh <- err
		}()
	}
	close(start)

	select {
	case <-feedStarted:
	case <-time.After(time.Second):
		t.Fatal("cache builder did not reach feed-rpc")
	}
	// 首个构建仍阻塞时，其余请求应等待同一个 SingleFlight，而不是再次回源。
	time.Sleep(50 * time.Millisecond)
	if got := feedRPC.calls.Load(); got != 1 {
		t.Fatalf("feed-rpc calls while build blocked = %d, want 1", got)
	}
	close(releaseFeed)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent GetHotFeed() error = %v", err)
		}
	}

	if got := feedRPC.calls.Load(); got != 1 {
		t.Fatalf("feed-rpc calls = %d, want 1", got)
	}
	if got := videoRPC.calls.Load(); got != 1 {
		t.Fatalf("video-rpc calls = %d, want 1", got)
	}
	if got := accountRPC.calls.Load(); got != 1 {
		t.Fatalf("account-rpc calls = %d, want 1", got)
	}
	if got := interactionRPC.calls.Load(); got != 1 {
		t.Fatalf("interaction-rpc calls = %d, want 1", got)
	}

	// 构建完成后的请求应直接读取 Redis 成品缓存。
	logic := NewGetHotFeedLogic(context.Background(), svcCtx)
	if _, err := logic.GetHotFeed(&types.GetHotFeedReq{Pagesize: 20}); err != nil {
		t.Fatalf("cached GetHotFeed() error = %v", err)
	}
	if got := feedRPC.calls.Load(); got != 1 {
		t.Fatalf("feed-rpc calls after cache hit = %d, want 1", got)
	}
}
