// Package scenario contains the concrete load-test scenarios wired to the
// gateway API. Every scenario implements the same Scenario interface so the
// cmd/loadtest driver can select one by name.
//
// A scenario is responsible for:
//  1. Preparing shared state (sampling seed users / videos, logging in,
//     building token pools).
//  2. Returning a loadgen.Op that the runner invokes repeatedly.
//
// Shared state is created ONCE in Setup; the returned Op must be safe for
// concurrent use across all workers.
package scenario

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"feedsystem-zero/tests/internal/httpclient"
	"feedsystem-zero/tests/internal/loadgen"
	"feedsystem-zero/tests/internal/testconfig"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Scenario describes one runnable load pattern.
type Scenario interface {
	Name() string
	Setup(ctx context.Context, f testconfig.LoadTestFlags) error
	Op() loadgen.Op
}

// Registry maps scenario name -> constructor. cmd/loadtest looks up names here.
var Registry = map[string]func() Scenario{
	"like":           func() Scenario { return &LikeScenario{} },
	"follow":         func() Scenario { return &FollowScenario{} },
	"following_feed": func() Scenario { return &FollowingFeedScenario{} },
	"hot_feed":       func() Scenario { return &HotFeedScenario{} },
	"publish_video":  func() Scenario { return &PublishVideoScenario{} },
}

// -----------------------------------------------------------------------------
// Shared helpers (kept unexported; each scenario embeds/uses these directly).
// -----------------------------------------------------------------------------

// tokenPool holds pre-obtained access tokens and hands them out round-robin.
type tokenPool struct {
	tokens  []string
	userIDs []uint64 // parallel array: userIDs[i] is the account behind tokens[i]
}

func (p *tokenPool) pick(seed int) (string, uint64) {
	if len(p.tokens) == 0 {
		return "", 0
	}
	idx := seed % len(p.tokens)
	if idx < 0 {
		idx += len(p.tokens)
	}
	return p.tokens[idx], p.userIDs[idx]
}

// videoPool holds a snapshot of video IDs sampled from MySQL for random access.
type videoPool struct {
	ids     []uint64
	authors []uint64 // parallel: authors[i] is the author_id of ids[i]
}

func (p *videoPool) pickRand(rng *rand.Rand) (uint64, uint64) {
	if len(p.ids) == 0 {
		return 0, 0
	}
	i := rng.Intn(len(p.ids))
	return p.ids[i], p.authors[i]
}

// prepareTokens logs in up to `size` random seed users and returns a pool.
// The gateway happens to enforce single-token-per-user (login overwrites),
// so we don't have to worry about lingering old tokens.
func prepareTokens(ctx context.Context, base string, timeout time.Duration, usernames []string, size int) (*tokenPool, error) {
	if size > len(usernames) {
		size = len(usernames)
	}
	pool := &tokenPool{
		tokens:  make([]string, 0, size),
		userIDs: make([]uint64, 0, size),
	}
	// A short-lived client per login is fine; the runtime pool is huge.
	cli := httpclient.New(base, timeout)
	for i := 0; i < size; i++ {
		name := usernames[i]
		lr, err := cli.Login(ctx, name, testconfig.SeedPassword)
		if err != nil {
			return nil, fmt.Errorf("login %s: %w", name, err)
		}
		// Fetch the user id via /account/profile so scenarios can avoid
		// self-actions (e.g. don't follow yourself).
		cli.SetToken(lr.AccessToken)
		profile, err := cli.GetProfile(ctx)
		if err != nil {
			return nil, fmt.Errorf("profile %s: %w", name, err)
		}
		pool.tokens = append(pool.tokens, lr.AccessToken)
		pool.userIDs = append(pool.userIDs, profile.UserID)
	}
	return pool, nil
}

// sampleUsernames picks `n` random seed usernames from accounts.
func sampleUsernames(ctx context.Context, db *gorm.DB, n int) ([]string, error) {
	var names []string
	// ORDER BY RAND() is expensive for very large tables but our seed pool
	// is bounded (<= 20k rows) so it's acceptable and gives us true randomness.
	if err := db.WithContext(ctx).
		Raw("SELECT username FROM accounts WHERE username LIKE ? ORDER BY RAND() LIMIT ?",
			testconfig.SeedUserPrefix+"%", n).
		Scan(&names).Error; err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no seed users found; did you run `cmd/seed` first?")
	}
	return names, nil
}

// sampleVideos returns a random slice of (id, author_id) seed videos.
func sampleVideos(ctx context.Context, db *gorm.DB, n int) (*videoPool, error) {
	type row struct {
		ID       uint64
		AuthorID uint64
	}
	var rows []row
	if err := db.WithContext(ctx).
		Raw("SELECT id, author_id FROM videos WHERE request_id LIKE ? AND status = 1 ORDER BY RAND() LIMIT ?",
			testconfig.SeedRequestIDPrefix+"%", n).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no seed videos found; did you run `cmd/seed` first?")
	}
	p := &videoPool{
		ids:     make([]uint64, len(rows)),
		authors: make([]uint64, len(rows)),
	}
	for i, r := range rows {
		p.ids[i] = r.ID
		p.authors[i] = r.AuthorID
	}
	return p, nil
}

// openMySQL is a convenience wrapper for scenarios that need direct DB access.
func openMySQL(dsn string) (*gorm.DB, error) {
	return gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error),
	})
}

// perWorkerRand builds one rand.Rand per worker to avoid the global rand
// mutex contention. Not thread-safe; each worker must call this once.
func perWorkerRand(workerID int) *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(workerID)*2654435761))
}

// once ensures Setup is called exactly once per scenario.
type once struct {
	sync.Once
	err error
}
