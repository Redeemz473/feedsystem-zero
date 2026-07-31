// Package seed bulk-inserts test users, file assets and videos directly into
// MySQL using GORM. It bypasses the RPC/HTTP layer on purpose: the goal is to
// build a large steady state in seconds so load-tests focus on read/write
// hotspots rather than the upload pipeline.
//
// Behaviour:
//   - Every row it produces is namespaced so `-reset` can wipe them cleanly:
//     accounts.username LIKE 'seed_user_%', videos.request_id LIKE 'seed-video-%',
//     file_assets.storage_path LIKE '/seed/%'.
//   - All users share one bcrypt hash of testconfig.SeedPassword.
//   - Videos reference a small pool of placeholder file_assets rows (with
//     realistic ref_count >= 1) to exercise the "instant-upload" dedup path.
//   - created_at is spread over the past N days so cursor pagination and
//     hot-rank recency scoring see real distribution.
//
// The seed tool intentionally does NOT touch Redis or Kafka: derived caches
// and Feed timelines should be rebuilt by the job consumers, which is the
// most realistic starting point for load tests.
package seed

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"feedsystem-zero/tests/internal/testconfig"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Account matches deploy/sql/001_schema.sql `accounts` table.
type Account struct {
	ID             uint64    `gorm:"primaryKey;column:id"`
	Username       string    `gorm:"column:username"`
	PasswordHash   string    `gorm:"column:password_hash"`
	Email          string    `gorm:"column:email"`
	AvatarURL      string    `gorm:"column:avatar_url"`
	Bio            string    `gorm:"column:bio"`
	FollowerCount  int64     `gorm:"column:follower_count"`
	FollowingCount int64     `gorm:"column:following_count"`
	CreatedAt      time.Time `gorm:"column:created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at"`
}

func (Account) TableName() string { return "accounts" }

// FileAsset matches deploy/sql/002_file_assets.sql.
type FileAsset struct {
	ID          uint64    `gorm:"primaryKey;column:id"`
	FileHash    string    `gorm:"column:file_hash"`
	FileType    string    `gorm:"column:file_type"`
	URL         string    `gorm:"column:url"`
	StoragePath string    `gorm:"column:storage_path"`
	Size        int64     `gorm:"column:size"`
	RefCount    int64     `gorm:"column:ref_count"`
	Status      int8      `gorm:"column:status"`
	CreatedAt   time.Time `gorm:"column:created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
}

func (FileAsset) TableName() string { return "file_assets" }

// Video matches deploy/sql/001_schema.sql `videos` table.
type Video struct {
	ID             uint64    `gorm:"primaryKey;column:id"`
	AuthorID       uint64    `gorm:"column:author_id"`
	AuthorUsername string    `gorm:"column:author_username"`
	Title          string    `gorm:"column:title"`
	Description    string    `gorm:"column:description"`
	PlayURL        string    `gorm:"column:play_url"`
	CoverURL       string    `gorm:"column:cover_url"`
	RequestID      string    `gorm:"column:request_id"`
	LikesCount     int64     `gorm:"column:likes_count"`
	CommentsCount  int64     `gorm:"column:comments_count"`
	Popularity     int64     `gorm:"column:popularity"`
	Status         int8      `gorm:"column:status"`
	CreatedAt      time.Time `gorm:"column:created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at"`
}

func (Video) TableName() string { return "videos" }

// Runner encapsulates a seed run.
type Runner struct {
	DB    *gorm.DB
	Flags testconfig.SeedFlags
}

// Open connects to MySQL using the DSN from flags. Callers must Close via
// r.DB's underlying sql.DB when finished.
func Open(f testconfig.SeedFlags) (*Runner, error) {
	db, err := gorm.Open(mysql.Open(f.MysqlDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(20)
	sqlDB.SetMaxIdleConns(10)
	return &Runner{DB: db, Flags: f}, nil
}

// Run executes the full seed pipeline: reset (optional), users, file_assets, videos.
func (r *Runner) Run(ctx context.Context) error {
	if r.Flags.Reset {
		if err := r.reset(ctx); err != nil {
			return fmt.Errorf("reset: %w", err)
		}
	}
	if err := r.seedUsers(ctx); err != nil {
		return fmt.Errorf("seed users: %w", err)
	}
	assetIDs, err := r.seedFileAssets(ctx)
	if err != nil {
		return fmt.Errorf("seed file_assets: %w", err)
	}
	if err := r.seedVideos(ctx, assetIDs); err != nil {
		return fmt.Errorf("seed videos: %w", err)
	}
	return nil
}

// reset removes only rows that were produced by previous seed runs; real
// accounts, videos or file uploads are untouched.
func (r *Runner) reset(ctx context.Context) error {
	log.Println("[seed] reset: deleting previous seed_* rows...")
	// Order matters: videos reference file_assets by URL uniqueness, but
	// there is no FK, so we can just delete top-down.
	tx := r.DB.WithContext(ctx).Exec("DELETE FROM videos WHERE request_id LIKE ?", testconfig.SeedRequestIDPrefix+"%")
	if tx.Error != nil {
		return tx.Error
	}
	log.Printf("[seed]   videos deleted: %d", tx.RowsAffected)

	tx = r.DB.WithContext(ctx).Exec("DELETE FROM file_assets WHERE storage_path LIKE '/seed/%'")
	if tx.Error != nil {
		return tx.Error
	}
	log.Printf("[seed]   file_assets deleted: %d", tx.RowsAffected)

	tx = r.DB.WithContext(ctx).Exec("DELETE FROM accounts WHERE username LIKE ?", testconfig.SeedUserPrefix+"%")
	if tx.Error != nil {
		return tx.Error
	}
	log.Printf("[seed]   accounts deleted: %d", tx.RowsAffected)
	return nil
}

// seedUsers inserts N accounts in batches. Existing seed users are skipped
// via ON DUPLICATE KEY behaviour (unique on username).
func (r *Runner) seedUsers(ctx context.Context) error {
	// bcrypt is expensive; do it exactly once for the shared password.
	hash, err := bcrypt.GenerateFromPassword([]byte(testconfig.SeedPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	const batch = 500
	start := time.Now()
	rows := make([]Account, 0, batch)
	now := time.Now()
	for i := 1; i <= r.Flags.Users; i++ {
		rows = append(rows, Account{
			Username:     fmt.Sprintf("%s%d", testconfig.SeedUserPrefix, i),
			PasswordHash: string(hash),
			Email:        fmt.Sprintf("%s%d%s", testconfig.SeedUserPrefix, i, testconfig.SeedEmailSuffix),
			AvatarURL:    "",
			Bio:          "seed user",
			CreatedAt:    now,
			UpdatedAt:    now,
		})
		if len(rows) == batch || i == r.Flags.Users {
			// CreateInBatches with OnConflict-DoNothing avoids blowing up when
			// a previous run already inserted some of these usernames.
			if err := r.DB.WithContext(ctx).Session(&gorm.Session{}).
				Clauses(onConflictDoNothing("username")).
				CreateInBatches(rows, batch).Error; err != nil {
				return err
			}
			rows = rows[:0]
		}
	}
	log.Printf("[seed] users done: %d rows in %s", r.Flags.Users, time.Since(start).Round(time.Millisecond))
	return nil
}

// seedFileAssets creates a small pool of placeholder assets that many videos
// will share (ref_count > 1) to mimic instant-upload behaviour.
func (r *Runner) seedFileAssets(ctx context.Context) ([]uint64, error) {
	start := time.Now()
	now := time.Now()
	assets := make([]FileAsset, 0, r.Flags.FileAssetBuckets)
	for i := 1; i <= r.Flags.FileAssetBuckets; i++ {
		assets = append(assets, FileAsset{
			FileHash:    fmt.Sprintf("seedhash-%08d", i),
			FileType:    "video",
			URL:         fmt.Sprintf(testconfig.SeedPlayURLTemplate, i),
			StoragePath: fmt.Sprintf("/seed/video_%d.mp4", i),
			Size:        1024 * 1024,
			RefCount:    0, // updated after videos are inserted
			Status:      1,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	}
	if err := r.DB.WithContext(ctx).
		Clauses(onConflictDoNothing("file_hash")).
		CreateInBatches(assets, 100).Error; err != nil {
		return nil, err
	}

	// Read back their IDs (rows we just inserted OR pre-existing rows with the same hash).
	var reread []FileAsset
	if err := r.DB.WithContext(ctx).
		Where("storage_path LIKE '/seed/%'").
		Order("id ASC").
		Find(&reread).Error; err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(reread))
	for _, a := range reread {
		ids = append(ids, a.ID)
	}
	log.Printf("[seed] file_assets done: %d rows in %s", len(ids), time.Since(start).Round(time.Millisecond))
	return ids, nil
}

// seedVideos inserts N videos with random authors and staggered created_at.
// ref_count on the underlying file_assets is bumped afterwards to reflect
// the number of videos pointing at each bucket.
func (r *Runner) seedVideos(ctx context.Context, assetIDs []uint64) error {
	if len(assetIDs) == 0 {
		return fmt.Errorf("no file_assets available")
	}

	// Grab the seed user id range so we can pick random authors.
	var minID, maxID uint64
	row := r.DB.WithContext(ctx).Raw(
		"SELECT COALESCE(MIN(id),0), COALESCE(MAX(id),0) FROM accounts WHERE username LIKE ?",
		testconfig.SeedUserPrefix+"%",
	).Row()
	if err := row.Scan(&minID, &maxID); err != nil {
		return fmt.Errorf("scan seed user id range: %w", err)
	}
	if minID == 0 || maxID == 0 {
		return fmt.Errorf("no seed users found; did you skip seedUsers?")
	}

	const batch = 500
	start := time.Now()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	spanSeconds := int64(r.Flags.VideoTimeSpanDays) * 24 * 3600
	rows := make([]Video, 0, batch)
	usedURLs := make(map[string]struct{}, r.Flags.Videos)
	assetRefs := make(map[uint64]int64, len(assetIDs))

	for i := 1; i <= r.Flags.Videos; i++ {
		// Author uniform in [minID, maxID]; note maxID-minID+1 may exceed r.Flags.Users if
		// a partial rerun happened, but rand handles that fine.
		author := minID + uint64(rng.Int63n(int64(maxID-minID+1)))
		bucket := assetIDs[rng.Intn(len(assetIDs))]
		assetRefs[bucket]++

		// Video play_url must be unique per uk_url on file_assets, but the videos
		// table has no such uniqueness — however we still want distinct URLs so
		// that "GET /video/:id" pages don't accidentally collapse.
		playURL := fmt.Sprintf("%s#v=%d", fmt.Sprintf(testconfig.SeedPlayURLTemplate, bucket), i)
		coverURL := fmt.Sprintf(testconfig.SeedCoverURLTemplate, bucket)
		usedURLs[playURL] = struct{}{}

		offset := time.Duration(rng.Int63n(spanSeconds)) * time.Second
		createdAt := time.Now().Add(-offset)

		rows = append(rows, Video{
			AuthorID:       author,
			AuthorUsername: fmt.Sprintf("%s%d", testconfig.SeedUserPrefix, author-minID+1),
			Title:          fmt.Sprintf("seed video %d", i),
			Description:    "auto-generated by tests/seed",
			PlayURL:        playURL,
			CoverURL:       coverURL,
			RequestID:      fmt.Sprintf("%s%d", testconfig.SeedRequestIDPrefix, i),
			Status:         1,
			CreatedAt:      createdAt,
			UpdatedAt:      createdAt,
		})
		if len(rows) == batch || i == r.Flags.Videos {
			if err := r.DB.WithContext(ctx).
				Clauses(onConflictDoNothing("author_id", "request_id")).
				CreateInBatches(rows, batch).Error; err != nil {
				return err
			}
			rows = rows[:0]
		}
	}

	// Bump ref_count on each bucket to match how many seed videos reference it.
	for bucket, delta := range assetRefs {
		if err := r.DB.WithContext(ctx).Exec(
			"UPDATE file_assets SET ref_count = ref_count + ? WHERE id = ?",
			delta, bucket,
		).Error; err != nil {
			return err
		}
	}
	log.Printf("[seed] videos done: %d rows in %s", r.Flags.Videos, time.Since(start).Round(time.Millisecond))
	return nil
}
