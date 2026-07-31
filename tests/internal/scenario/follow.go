package scenario

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"feedsystem-zero/tests/internal/httpclient"
	"feedsystem-zero/tests/internal/loadgen"
	"feedsystem-zero/tests/internal/testconfig"
)

// FollowScenario exercises POST /social/users/:id/follow (and reverses it).
// Each iteration:
//  1. Pick actor from the token pool
//  2. Pick a target user id different from actor
//  3. Follow, then Unfollow to keep counters bounded
type FollowScenario struct {
	baseURL   string
	timeout   time.Duration
	tokens    *tokenPool
	targetIDs []uint64
	client    *httpclient.Client
}

func (s *FollowScenario) Name() string { return "follow" }

func (s *FollowScenario) Setup(ctx context.Context, f testconfig.LoadTestFlags) error {
	s.baseURL = f.BaseURL
	s.timeout = f.Timeout

	db, err := openMySQL(f.MysqlDSN)
	if err != nil {
		return err
	}
	usernames, err := sampleUsernames(ctx, db, f.LoginPoolSize)
	if err != nil {
		return err
	}
	log.Printf("[follow] sampled %d users, logging in...", len(usernames))
	tokens, err := prepareTokens(ctx, f.BaseURL, f.Timeout, usernames, f.LoginPoolSize)
	if err != nil {
		return err
	}
	s.tokens = tokens

	// Sample target user IDs; overlap with token pool is fine — we filter
	// self-follow at runtime.
	if err := db.WithContext(ctx).
		Raw("SELECT id FROM accounts WHERE username LIKE ? ORDER BY RAND() LIMIT ?",
			testconfig.SeedUserPrefix+"%", f.TargetPoolSize).
		Scan(&s.targetIDs).Error; err != nil {
		return err
	}
	if len(s.targetIDs) == 0 {
		return fmt.Errorf("no target users sampled")
	}
	log.Printf("[follow] sampled %d target user ids", len(s.targetIDs))

	s.client = httpclient.New(f.BaseURL, f.Timeout)
	return nil
}

func (s *FollowScenario) Op() loadgen.Op {
	return func(ctx context.Context, workerID int) error {
		rng := perWorkerRand(workerID)
		token, actorID := s.tokens.pick(rng.Int())
		// Pick a target != actor with a small retry budget.
		var target uint64
		for i := 0; i < 5; i++ {
			t := s.targetIDs[rng.Intn(len(s.targetIDs))]
			if t != actorID {
				target = t
				break
			}
		}
		if target == 0 {
			return nil // extremely unlikely; skip this iteration
		}

		ctx = httpclient.WithToken(ctx, token)
		if _, err := s.client.Follow(ctx, target); err != nil {
			// Idempotent follow already exists -> 400 is fine.
			var api *httpclient.APIError
			if errors.As(err, &api) && api.Status == 400 {
				// still exercise the unfollow path
			} else {
				return fmt.Errorf("follow: %w", err)
			}
		}
		if _, err := s.client.Unfollow(ctx, target); err != nil {
			var api *httpclient.APIError
			if errors.As(err, &api) && api.Status == 400 {
				return nil
			}
			return fmt.Errorf("unfollow: %w", err)
		}
		return nil
	}
}
