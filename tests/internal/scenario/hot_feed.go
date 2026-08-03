package scenario

import (
	"context"
	"fmt"

	"feedsystem-zero/tests/internal/httpclient"
	"feedsystem-zero/tests/internal/loadgen"
	"feedsystem-zero/tests/internal/testconfig"
)

// HotFeedScenario exercises GET /feed/hot. This endpoint accepts anonymous
// traffic, so no login is required — perfect for testing raw QPS on the
// hot-rank snapshot cache.
//
// Each iteration alternates between "first page" (fresh snapshot) and
// "second page" (same snapshot, offset=pageSize) to mimic real infinite scroll.
type HotFeedScenario struct {
	client        *httpclient.Client
	pageSize      int
	workerCursors []hotFeedCursor
}

// hotFeedCursor 只由对应 workerID 的 goroutine 读写，因此不需要额外加锁。
// 压测的预热阶段和计时阶段串行执行，也不会同时访问同一个 worker 槽位。
type hotFeedCursor struct {
	snapshotAt int64
	nextOffset int64
	hasMore    bool
}

func (s *HotFeedScenario) Name() string { return "hot_feed" }

func (s *HotFeedScenario) Setup(ctx context.Context, f testconfig.LoadTestFlags) error {
	s.client = httpclient.New(f.BaseURL, f.Timeout)
	s.pageSize = 20
	workerCount := f.Concurrency
	if workerCount < 1 {
		workerCount = 1
	}
	s.workerCursors = make([]hotFeedCursor, workerCount)

	// One preflight call to ensure the endpoint is reachable and warm the
	// snapshot on the server side.
	resp, err := s.client.GetHotFeed(ctx, 0, 0, s.pageSize)
	if err != nil {
		return fmt.Errorf("preflight hot feed: %w", err)
	}
	if resp.SnapshotAt <= 0 {
		return fmt.Errorf("preflight hot feed: response is missing snapshot_at")
	}

	// 同一次热榜快照对所有匿名访问者一致，先用预检响应初始化各 worker；
	// 后续每个 worker 请求首页时会独立刷新自己的分页游标。
	initialCursor := hotFeedCursor{
		snapshotAt: resp.SnapshotAt,
		nextOffset: resp.NextOffset,
		hasMore:    resp.HasMore,
	}
	for i := range s.workerCursors {
		s.workerCursors[i] = initialCursor
	}
	return nil
}

func (s *HotFeedScenario) Op() loadgen.Op {
	return func(ctx context.Context, workerID int) error {
		if workerID < 0 || workerID >= len(s.workerCursors) {
			return fmt.Errorf("hot feed: invalid worker id %d", workerID)
		}

		rng := perWorkerRand(workerID)
		cursor := s.workerCursors[workerID]

		// 30% 请求使用该 worker 最近一次首页响应的快照翻到第二页。
		// 没有有效快照或榜单不足一页时仍请求首页，不能发送
		// offset > 0、snapshot_at = 0 这种服务端明确拒绝的无效请求。
		useNextPage := cursor.snapshotAt > 0 && cursor.hasMore && cursor.nextOffset > 0 && rng.Intn(10) < 3
		if useNextPage {
			if _, err := s.client.GetHotFeed(ctx, cursor.snapshotAt, cursor.nextOffset, s.pageSize); err != nil {
				// 快照可能恰好过期或被淘汰，清空后让下一次请求从首页重新建会话。
				s.workerCursors[workerID] = hotFeedCursor{}
				return fmt.Errorf("hot feed next page: %w", err)
			}
			return nil
		}

		resp, err := s.client.GetHotFeed(ctx, 0, 0, s.pageSize)
		if err != nil {
			return fmt.Errorf("hot feed first page: %w", err)
		}
		if resp.SnapshotAt <= 0 {
			return fmt.Errorf("hot feed first page: response is missing snapshot_at")
		}
		s.workerCursors[workerID] = hotFeedCursor{
			snapshotAt: resp.SnapshotAt,
			nextOffset: resp.NextOffset,
			hasMore:    resp.HasMore,
		}
		return nil
	}
}
