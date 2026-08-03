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
	if len(tokens.tokens) < f.Concurrency {
		return fmt.Errorf(
			"like scenario requires at least one login user per worker: users=%d concurrency=%d; increase -login-pool or seed users",
			len(tokens.tokens),
			f.Concurrency,
		)
	}
	s.tokens = tokens

	videos, err := sampleVideos(ctx, db, f.TargetPoolSize)
	if err != nil {
		return err
	}
	s.videos = videos
	log.Printf("[like] sampled %d target videos", len(videos.ids))

	// Bearer token 通过请求 context 传递，共享 Client 不保存可变 token 状态，
	// 因而可以安全复用底层 HTTP Transport 和连接池。
	s.client = httpclient.New(f.BaseURL, f.Timeout)
	return nil
}

func (s *LikeScenario) Op() loadgen.Op {
	return func(ctx context.Context, workerID int) error {
		rng := perWorkerRand(workerID)
		// 每个 worker 固定使用独立用户，避免普通吞吐测试随机制造
		// “同一用户同时操作同一视频”的 409 业务冲突。
		// 视频仍随机选择，因此仍能覆盖热点视频上的多用户并发点赞。
		token, _ := s.tokens.pick(workerID)
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
