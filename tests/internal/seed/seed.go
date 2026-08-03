// Package seed bulk-inserts test users, file assets and videos directly into
// MySQL using GORM. It bypasses the RPC/HTTP layer on purpose: the goal is to
// build a large steady state in seconds so load-tests focus on read/write
// hotspots rather than the upload pipeline.
//
// Behaviour:
//   - Every row it produces is namespaced so `-reset` can wipe it and related
//     load-test business rows cleanly without touching real accounts/uploads.
//   - All users share one bcrypt hash of testconfig.SeedPassword.
//   - Videos reference a small pool of placeholder file_assets rows (with
//     realistic ref_count >= 1) to exercise the "instant-upload" dedup path.
//   - created_at is spread over the past N days so cursor pagination and
//     hot-rank recency scoring see real distribution.
//
// Redis cleanup is opt-in through `-reset-redis`; Kafka offsets/topics are not
// reset because existing consumer groups can safely continue from their
// committed offsets when new test events are appended.
package seed

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"feedsystem-zero/tests/internal/testconfig"

	"github.com/redis/go-redis/v9"
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

type seedAssetPair struct {
	video FileAsset
	cover FileAsset
}

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
		if r.Flags.ResetRedis {
			if err := r.resetRedis(ctx); err != nil {
				return fmt.Errorf("reset redis: %w", err)
			}
		}
		if err := r.reset(ctx); err != nil {
			return fmt.Errorf("reset: %w", err)
		}
	}
	if err := r.seedUsers(ctx); err != nil {
		return fmt.Errorf("seed users: %w", err)
	}
	assets, err := r.seedFileAssets(ctx)
	if err != nil {
		return fmt.Errorf("seed file_assets: %w", err)
	}
	if err := r.seedVideos(ctx, assets); err != nil {
		return fmt.Errorf("seed videos: %w", err)
	}
	if err := r.verifySeedData(ctx); err != nil {
		return fmt.Errorf("verify seed data: %w", err)
	}
	return nil
}

// reset removes seed users and all business rows created by/for those users.
// It also removes publish-loadtest videos because they reuse seed users and
// seed file assets. Real accounts, videos and uploaded files are preserved.
func (r *Runner) reset(ctx context.Context) error {
	log.Println("[seed] reset: deleting previous seed/loadtest business data...")
	seedUser := testconfig.SeedUserPrefix + "%"
	seedVideo := testconfig.SeedRequestIDPrefix + "%"
	loadVideo := "loadtest-%"

	return r.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		steps := []struct {
			name  string
			query string
			args  []any
		}{
			{
				name: "notifications",
				query: `DELETE n FROM notifications n
					LEFT JOIN accounts receiver ON receiver.id = n.receiver_id
					LEFT JOIN accounts actor ON actor.id = n.actor_id
					LEFT JOIN videos v ON v.id = n.video_id
					LEFT JOIN accounts author ON author.id = v.author_id
					WHERE receiver.username LIKE ? OR actor.username LIKE ?
					   OR v.request_id LIKE ? OR v.request_id LIKE ? OR author.username LIKE ?`,
				args: []any{seedUser, seedUser, seedVideo, loadVideo, seedUser},
			},
			{
				name: "interaction_events",
				query: `DELETE ie FROM interaction_events ie
					LEFT JOIN accounts actor ON actor.id = ie.user_id
					LEFT JOIN videos v ON v.id = ie.video_id
					LEFT JOIN accounts author ON author.id = v.author_id
					WHERE actor.username LIKE ? OR v.request_id LIKE ?
					   OR v.request_id LIKE ? OR author.username LIKE ?`,
				args: []any{seedUser, seedVideo, loadVideo, seedUser},
			},
			{
				name: "video_tags",
				query: `DELETE vt FROM video_tags vt
					JOIN videos v ON v.id = vt.video_id
					LEFT JOIN accounts author ON author.id = v.author_id
					WHERE v.request_id LIKE ? OR v.request_id LIKE ? OR author.username LIKE ?`,
				args: []any{seedVideo, loadVideo, seedUser},
			},
			{
				name: "likes",
				query: `DELETE l FROM likes l
					LEFT JOIN accounts actor ON actor.id = l.user_id
					LEFT JOIN videos v ON v.id = l.video_id
					LEFT JOIN accounts author ON author.id = v.author_id
					WHERE actor.username LIKE ? OR v.request_id LIKE ?
					   OR v.request_id LIKE ? OR author.username LIKE ?`,
				args: []any{seedUser, seedVideo, loadVideo, seedUser},
			},
			{
				name: "comments",
				query: `DELETE c FROM comments c
					LEFT JOIN accounts actor ON actor.id = c.user_id
					LEFT JOIN videos v ON v.id = c.video_id
					LEFT JOIN accounts author ON author.id = v.author_id
					WHERE actor.username LIKE ? OR v.request_id LIKE ?
					   OR v.request_id LIKE ? OR author.username LIKE ?`,
				args: []any{seedUser, seedVideo, loadVideo, seedUser},
			},
			{
				name: "follows",
				query: `DELETE f FROM follows f
					LEFT JOIN accounts follower ON follower.id = f.follower_id
					LEFT JOIN accounts following ON following.id = f.following_id
					WHERE follower.username LIKE ? OR following.username LIKE ?`,
				args: []any{seedUser, seedUser},
			},
			{
				name: "videos",
				query: `DELETE v FROM videos v
					LEFT JOIN accounts author ON author.id = v.author_id
					WHERE v.request_id LIKE ? OR v.request_id LIKE ? OR author.username LIKE ?`,
				args: []any{seedVideo, loadVideo, seedUser},
			},
			{
				name:  "file_assets",
				query: "DELETE FROM file_assets WHERE storage_path LIKE '/seed/%'",
			},
			{
				name:  "accounts",
				query: "DELETE FROM accounts WHERE username LIKE ?",
				args:  []any{seedUser},
			},
			{
				name:  "orphan_tags",
				query: "DELETE t FROM tags t LEFT JOIN video_tags vt ON vt.tag_id = t.id WHERE vt.id IS NULL",
			},
		}

		for _, step := range steps {
			result := tx.Exec(step.query, step.args...)
			if result.Error != nil {
				return fmt.Errorf("delete %s: %w", step.name, result.Error)
			}
			log.Printf("[seed]   %s deleted: %d", step.name, result.RowsAffected)
		}

		// Test users may have followed real users (or vice versa). Rebuild the
		// surviving accounts' counters after those relationships are removed.
		if err := tx.Exec(`UPDATE accounts a
			LEFT JOIN (
				SELECT following_id AS user_id, COUNT(*) AS cnt
				FROM follows WHERE status = 1 AND deleted_at IS NULL
				GROUP BY following_id
			) followers ON followers.user_id = a.id
			LEFT JOIN (
				SELECT follower_id AS user_id, COUNT(*) AS cnt
				FROM follows WHERE status = 1 AND deleted_at IS NULL
				GROUP BY follower_id
			) followings ON followings.user_id = a.id
			SET a.follower_count = COALESCE(followers.cnt, 0),
				a.following_count = COALESCE(followings.cnt, 0),
				a.is_big_v = CASE WHEN COALESCE(followers.cnt, 0) >= 5000 THEN 1 ELSE 0 END`).Error; err != nil {
			return fmt.Errorf("rebuild surviving account counters: %w", err)
		}
		return nil
	})
}

func (r *Runner) resetRedis(ctx context.Context) error {
	client := redis.NewClient(&redis.Options{
		Addr:     r.Flags.RedisAddr,
		Password: r.Flags.RedisPass,
		DB:       r.Flags.RedisDB,
	})
	defer client.Close()

	redisCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := client.Ping(redisCtx).Err(); err != nil {
		return err
	}

	var cursor uint64
	var deleted int64
	for {
		keys, next, err := client.Scan(redisCtx, cursor, "fsz:*", 1000).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			count, err := client.Unlink(redisCtx, keys...).Result()
			if err != nil {
				return err
			}
			deleted += count
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	log.Printf("[seed] reset redis: fsz:* keys deleted: %d", deleted)
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

// seedFileAssets creates video/cover pairs shared by many videos. Both URLs
// exist in file_assets, so seeded rows obey the same publish invariant as
// videos created through video-rpc.
func (r *Runner) seedFileAssets(ctx context.Context) ([]seedAssetPair, error) {
	start := time.Now()
	now := time.Now()
	assets := buildSeedAssets(r.Flags.FileAssetBuckets, now)
	if len(assets) == 0 {
		return nil, fmt.Errorf("file asset buckets must be positive")
	}
	if err := r.DB.WithContext(ctx).
		Clauses(onConflictDoNothing("file_hash")).
		CreateInBatches(assets, 100).Error; err != nil {
		return nil, err
	}

	var reread []FileAsset
	if err := r.DB.WithContext(ctx).
		Where("storage_path LIKE '/seed/%'").
		Order("id ASC").
		Find(&reread).Error; err != nil {
		return nil, err
	}
	byURL := make(map[string]FileAsset, len(reread))
	for _, asset := range reread {
		byURL[asset.URL] = asset
	}
	pairs := make([]seedAssetPair, 0, r.Flags.FileAssetBuckets)
	for i := 1; i <= r.Flags.FileAssetBuckets; i++ {
		videoURL := fmt.Sprintf(testconfig.SeedPlayURLTemplate, i)
		coverURL := fmt.Sprintf(testconfig.SeedCoverURLTemplate, i)
		videoAsset, videoOK := byURL[videoURL]
		coverAsset, coverOK := byURL[coverURL]
		if !videoOK || !coverOK {
			return nil, fmt.Errorf("seed asset pair %d incomplete: video=%t cover=%t", i, videoOK, coverOK)
		}
		pairs = append(pairs, seedAssetPair{video: videoAsset, cover: coverAsset})
	}
	log.Printf("[seed] file_assets done: %d rows (%d pairs) in %s", len(reread), len(pairs), time.Since(start).Round(time.Millisecond))
	return pairs, nil
}

func buildSeedAssets(bucketCount int, now time.Time) []FileAsset {
	assets := make([]FileAsset, 0, bucketCount*2)
	for i := 1; i <= bucketCount; i++ {
		assets = append(assets,
			FileAsset{
				FileHash:    fmt.Sprintf("seed-video-hash-%08d", i),
				FileType:    "video",
				URL:         fmt.Sprintf(testconfig.SeedPlayURLTemplate, i),
				StoragePath: fmt.Sprintf("/seed/video_%d.mp4", i),
				Size:        1024 * 1024,
				Status:      1,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
			FileAsset{
				FileHash:    fmt.Sprintf("seed-cover-hash-%08d", i),
				FileType:    "cover",
				URL:         fmt.Sprintf(testconfig.SeedCoverURLTemplate, i),
				StoragePath: fmt.Sprintf("/seed/cover_%d.jpg", i),
				Size:        256 * 1024,
				Status:      1,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
		)
	}
	return assets
}

// seedVideos inserts N videos with random authors and staggered created_at.
// ref_count is recalculated from active videos afterwards, making reruns
// idempotent even when some video inserts hit an existing request_id.
func (r *Runner) seedVideos(ctx context.Context, assets []seedAssetPair) error {
	if len(assets) == 0 {
		return fmt.Errorf("no file_assets available")
	}

	var authors []Account
	if err := r.DB.WithContext(ctx).
		Select("id", "username").
		Where("username LIKE ?", testconfig.SeedUserPrefix+"%").
		Order("id ASC").
		Find(&authors).Error; err != nil {
		return fmt.Errorf("load seed users: %w", err)
	}
	if len(authors) == 0 {
		return fmt.Errorf("no seed users found; did you skip seedUsers?")
	}

	const batch = 500
	start := time.Now()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	spanSeconds := int64(r.Flags.VideoTimeSpanDays) * 24 * 3600
	rows := make([]Video, 0, batch)

	for i := 1; i <= r.Flags.Videos; i++ {
		author := authors[(i-1)%len(authors)]
		asset := assets[(i-1)%len(assets)]

		offset := time.Duration(rng.Int63n(spanSeconds)) * time.Second
		createdAt := time.Now().Add(-offset)

		rows = append(rows, Video{
			AuthorID:       author.ID,
			AuthorUsername: author.Username,
			Title:          fmt.Sprintf("seed video %d", i),
			Description:    "auto-generated by tests/seed",
			PlayURL:        asset.video.URL,
			CoverURL:       asset.cover.URL,
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

	if err := r.rebuildSeedAssetRefs(ctx); err != nil {
		return err
	}
	log.Printf("[seed] videos done: %d rows in %s", r.Flags.Videos, time.Since(start).Round(time.Millisecond))
	return nil
}

func (r *Runner) rebuildSeedAssetRefs(ctx context.Context) error {
	return r.DB.WithContext(ctx).Exec(`UPDATE file_assets fa
		LEFT JOIN (
			SELECT url, COUNT(*) AS refs
			FROM (
				SELECT play_url AS url FROM videos WHERE status = 1 AND deleted_at IS NULL
				UNION ALL
				SELECT cover_url AS url FROM videos WHERE status = 1 AND deleted_at IS NULL
			) active_refs
			GROUP BY url
		) actual ON actual.url = fa.url
		SET fa.ref_count = COALESCE(actual.refs, 0),
			fa.status = 1,
			fa.deleted_at = NULL
		WHERE fa.storage_path LIKE '/seed/%'`).Error
}

func (r *Runner) verifySeedData(ctx context.Context) error {
	type verificationResult struct {
		SeedUsers         int64 `gorm:"column:seed_users"`
		SeedVideos        int64 `gorm:"column:seed_videos"`
		SeedAssets        int64 `gorm:"column:seed_assets"`
		AssetRefMismatch  int64 `gorm:"column:asset_ref_mismatch"`
		OrphanAuthors     int64 `gorm:"column:orphan_authors"`
		MissingAssetLinks int64 `gorm:"column:missing_asset_links"`
	}

	var result verificationResult
	if err := r.DB.WithContext(ctx).Raw(`SELECT
		(SELECT COUNT(*) FROM accounts WHERE username LIKE ?) AS seed_users,
		(SELECT COUNT(*) FROM videos WHERE request_id LIKE ?) AS seed_videos,
		(SELECT COUNT(*) FROM file_assets WHERE storage_path LIKE '/seed/%') AS seed_assets,
		(SELECT COUNT(*)
		 FROM file_assets fa
		 LEFT JOIN (
			 SELECT url, COUNT(*) AS refs
			 FROM (
				 SELECT play_url AS url FROM videos WHERE status = 1 AND deleted_at IS NULL
				 UNION ALL
				 SELECT cover_url AS url FROM videos WHERE status = 1 AND deleted_at IS NULL
			 ) active_refs
			 GROUP BY url
		 ) actual ON actual.url = fa.url
		 WHERE fa.storage_path LIKE '/seed/%'
		   AND fa.ref_count <> COALESCE(actual.refs, 0)) AS asset_ref_mismatch,
		(SELECT COUNT(*)
		 FROM videos v
		 LEFT JOIN accounts a ON a.id = v.author_id
		 WHERE v.request_id LIKE ? AND a.id IS NULL) AS orphan_authors,
		(SELECT COUNT(*)
		 FROM videos v
		 LEFT JOIN file_assets play_asset ON play_asset.url = v.play_url
		 LEFT JOIN file_assets cover_asset ON cover_asset.url = v.cover_url
		 WHERE v.request_id LIKE ?
		   AND (play_asset.id IS NULL OR cover_asset.id IS NULL)) AS missing_asset_links`,
		testconfig.SeedUserPrefix+"%",
		testconfig.SeedRequestIDPrefix+"%",
		testconfig.SeedRequestIDPrefix+"%",
		testconfig.SeedRequestIDPrefix+"%",
	).Scan(&result).Error; err != nil {
		return err
	}

	expectedAssets := int64(r.Flags.FileAssetBuckets * 2)
	if result.SeedUsers != int64(r.Flags.Users) ||
		result.SeedVideos != int64(r.Flags.Videos) ||
		result.SeedAssets != expectedAssets ||
		result.AssetRefMismatch != 0 ||
		result.OrphanAuthors != 0 ||
		result.MissingAssetLinks != 0 {
		return fmt.Errorf(
			"unexpected counts: users=%d/%d videos=%d/%d assets=%d/%d ref_mismatch=%d orphan_authors=%d missing_assets=%d",
			result.SeedUsers,
			r.Flags.Users,
			result.SeedVideos,
			r.Flags.Videos,
			result.SeedAssets,
			expectedAssets,
			result.AssetRefMismatch,
			result.OrphanAuthors,
			result.MissingAssetLinks,
		)
	}
	log.Printf(
		"[seed] verify: users=%d videos=%d assets=%d ref_mismatch=0 orphan_authors=0 missing_assets=0",
		result.SeedUsers,
		result.SeedVideos,
		result.SeedAssets,
	)
	return nil
}
