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
	client   *httpclient.Client
	pageSize int
}

func (s *HotFeedScenario) Name() string { return "hot_feed" }

func (s *HotFeedScenario) Setup(ctx context.Context, f testconfig.LoadTestFlags) error {
	s.client = httpclient.New(f.BaseURL, f.Timeout)
	s.pageSize = 20
	// One preflight call to ensure the endpoint is reachable and warm the
	// snapshot on the server side.
	if _, err := s.client.GetHotFeed(ctx, 0, 0, s.pageSize); err != nil {
		return fmt.Errorf("preflight hot feed: %w", err)
	}
	return nil
}

func (s *HotFeedScenario) Op() loadgen.Op {
	return func(ctx context.Context, workerID int) error {
		rng := perWorkerRand(workerID)
		// 30% requests ask for offset > 0 to simulate scroll depth.
		var offset int64
		if rng.Intn(10) < 3 {
			offset = int64(s.pageSize) // second page
		}
		if _, err := s.client.GetHotFeed(ctx, 0, offset, s.pageSize); err != nil {
			return fmt.Errorf("hot feed: %w", err)
		}
		return nil
	}
}
