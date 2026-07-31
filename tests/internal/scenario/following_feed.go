package scenario

import (
	"context"
	"fmt"
	"log"
	"time"

	"feedsystem-zero/tests/internal/httpclient"
	"feedsystem-zero/tests/internal/loadgen"
	"feedsystem-zero/tests/internal/testconfig"
)

// FollowingFeedScenario exercises GET /feed/following. Read-heavy; each
// iteration picks a token and requests the first page of its follow feed.
// A prep step makes each token follow a handful of seed users so the feed
// isn't empty (otherwise we'd measure the "empty feed" fast-path only).
type FollowingFeedScenario struct {
	tokens *tokenPool
	client *httpclient.Client
	// pageSize passed to GetFollowingFeed; 0 means server default (usually 20).
	pageSize int
}

func (s *FollowingFeedScenario) Name() string { return "following_feed" }

func (s *FollowingFeedScenario) Setup(ctx context.Context, f testconfig.LoadTestFlags) error {
	db, err := openMySQL(f.MysqlDSN)
	if err != nil {
		return err
	}
	usernames, err := sampleUsernames(ctx, db, f.LoginPoolSize)
	if err != nil {
		return err
	}
	log.Printf("[following_feed] sampled %d users, logging in...", len(usernames))
	tokens, err := prepareTokens(ctx, f.BaseURL, f.Timeout, usernames, f.LoginPoolSize)
	if err != nil {
		return err
	}
	s.tokens = tokens
	s.client = httpclient.New(f.BaseURL, f.Timeout)
	s.pageSize = 20

	// Warm-follow: each token follows 10 random seed users so its feed is
	// populated. We do this here (not inside the hot loop) because feed
	// warm-up shouldn't count as read-throughput.
	targets := make([]uint64, 0, f.TargetPoolSize)
	if err := db.WithContext(ctx).
		Raw("SELECT id FROM accounts WHERE username LIKE ? ORDER BY RAND() LIMIT ?",
			testconfig.SeedUserPrefix+"%", f.TargetPoolSize).
		Scan(&targets).Error; err != nil {
		return err
	}
	if len(targets) == 0 {
		return fmt.Errorf("no target users to warm follow")
	}
	log.Printf("[following_feed] warming follows: %d tokens x 10 targets", len(s.tokens.tokens))
	for i, tok := range s.tokens.tokens {
		actorID := s.tokens.userIDs[i]
		fctx := httpclient.WithToken(ctx, tok)
		followed := 0
		for j := 0; j < 30 && followed < 10; j++ {
			t := targets[(i*7+j*13)%len(targets)]
			if t == actorID {
				continue
			}
			if _, err := s.client.Follow(fctx, t); err != nil {
				// Duplicate follows return 400; that's fine.
				continue
			}
			followed++
		}
	}
	log.Printf("[following_feed] warmup complete")
	return nil
}

func (s *FollowingFeedScenario) Op() loadgen.Op {
	return func(ctx context.Context, workerID int) error {
		rng := perWorkerRand(workerID)
		token, _ := s.tokens.pick(rng.Int())
		ctx = httpclient.WithToken(ctx, token)
		if _, err := s.client.GetFollowingFeed(ctx, s.pageSize); err != nil {
			return fmt.Errorf("get following feed: %w", err)
		}
		return nil
	}
}

// unused import guard: keep time reference explicit
var _ = time.Second
