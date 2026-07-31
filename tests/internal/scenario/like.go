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

// LikeScenario exercises POST /interaction/video/:id/like. Each iteration:
//  1. picks a random (worker, video) pair
//  2. issues Like
//  3. immediately issues Unlike so state doesn't drift (keeps like counts
//     bounded and lets us run for arbitrary durations without hitting the
//     "already liked" idempotent short-circuit every time)
type LikeScenario struct {
	baseURL string
	timeout time.Duration
	tokens  *tokenPool
	videos  *videoPool
	client  *httpclient.Client
}

func (s *LikeScenario) Name() string { return "like" }

func (s *LikeScenario) Setup(ctx context.Context, f testconfig.LoadTestFlags) error {
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
	log.Printf("[like] sampled %d users, logging in...", len(usernames))
	tokens, err := prepareTokens(ctx, f.BaseURL, f.Timeout, usernames, f.LoginPoolSize)
	if err != nil {
		return err
	}
	s.tokens = tokens

	videos, err := sampleVideos(ctx, db, f.TargetPoolSize)
	if err != nil {
		return err
	}
	s.videos = videos
	log.Printf("[like] sampled %d target videos", len(videos.ids))

	// Workers do not share a single Client's token field, so we build one
	// per request via short-lived clients. Reusing the transport pool is
	// still important, so we keep one long-lived transport via New() — but
	// each Op invocation creates a lightweight Client copy with its own
	// bearer token to avoid data races.
	s.client = httpclient.New(f.BaseURL, f.Timeout)
	return nil
}

func (s *LikeScenario) Op() loadgen.Op {
	return func(ctx context.Context, workerID int) error {
		rng := perWorkerRand(workerID)
		token, _ := s.tokens.pick(rng.Int())
		videoID, _ := s.videos.pickRand(rng)

		// Per-request bearer token via context; the shared Client stays goroutine-safe.
		ctx = httpclient.WithToken(ctx, token)

		if _, err := s.client.LikeVideo(ctx, videoID); err != nil {
			return fmt.Errorf("like: %w", err)
		}
		// Immediately reverse so the state doesn't monotonically grow.
		if _, err := s.client.UnlikeVideo(ctx, videoID); err != nil {
			// Some races (duplicate cancel) may return 400; ignore benign errors.
			var api *httpclient.APIError
			if errors.As(err, &api) && api.Status == 400 {
				return nil
			}
			return fmt.Errorf("unlike: %w", err)
		}
		return nil
	}
}
