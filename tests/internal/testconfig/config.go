// Package testconfig aggregates command-line flags and defaults shared by
// the seed tool, the load-test runner and the e2e smoke test.
//
// All values are simple types so callers can override them with flag parsing
// or plain struct literals; nothing depends on go-zero's config loader to
// keep the test tooling completely standalone.
package testconfig

import (
	"flag"
	"time"
)

// Default connection endpoints match deploy/docker-compose.yml.
const (
	DefaultGatewayBase = "http://127.0.0.1:8888"
	DefaultMysqlDSN    = "root:123456@tcp(127.0.0.1:3308)/feedsystem_zero?charset=utf8mb4&parseTime=true&loc=Local"
	DefaultRedisAddr   = "127.0.0.1:6380"
	DefaultRedisPass   = "123456"
	DefaultRedisDB     = 0
	DefaultUploadDir   = "uploads"

	// SeedUserPrefix is the mandatory prefix for every seeded username / email,
	// so a `-reset` pass can wipe test data without touching real accounts.
	SeedUserPrefix = "seed_user_"
	// SeedEmailSuffix identifies fake mailboxes used by seed accounts.
	SeedEmailSuffix = "@loadtest.local"
	// SeedPassword is the shared plaintext for every seed account; it is
	// hashed exactly once during seeding.
	SeedPassword = "LoadTest@123"

	// SeedPlayURL / SeedCoverURL are the placeholder URLs used by every
	// seeded video. They must be unique across file_assets rows so we suffix
	// with a hash bucket; see seed/seed.go.
	SeedPlayURLTemplate  = "/uploads/seed/video_%d.mp4"
	SeedCoverURLTemplate = "/uploads/seed/cover_%d.jpg"

	// SeedRequestIDPrefix marks the request_id column of every seed video so
	// resets can target them precisely.
	SeedRequestIDPrefix = "seed-video-"
)

// SeedFlags collects every knob the seed tool exposes.
type SeedFlags struct {
	MysqlDSN  string
	RedisAddr string
	RedisPass string
	RedisDB   int
	// UploadDir 必须与 gateway 的 Upload.Dir 指向同一目录。seed 会在其
	// seed 子目录创建真实占位文件，使 PublishVideo 的磁盘校验保持生效。
	UploadDir string
	Users     int
	Videos    int
	Reset     bool
	// ResetRedis removes only feedsystem-zero keys (fsz:*). It is intentionally
	// opt-in because clearing token/cache state logs local test users out.
	ResetRedis bool
	// VideoTimeSpanDays spreads created_at over the past N days so cursor
	// pagination and hot-rank recency weighting behave realistically.
	VideoTimeSpanDays int
	// FileAssetBuckets controls how many distinct placeholder file_assets
	// rows we create. Multiple videos share the same asset (ref_count > 1)
	// which mirrors the real "instant-upload" behaviour.
	FileAssetBuckets int
}

// RegisterSeedFlags binds SeedFlags fields to a flag.FlagSet.
func RegisterSeedFlags(fs *flag.FlagSet, f *SeedFlags) {
	fs.StringVar(&f.MysqlDSN, "mysql", DefaultMysqlDSN, "MySQL DSN")
	fs.StringVar(&f.RedisAddr, "redis", DefaultRedisAddr, "Redis address")
	fs.StringVar(&f.RedisPass, "redis-pass", DefaultRedisPass, "Redis password")
	fs.IntVar(&f.RedisDB, "redis-db", DefaultRedisDB, "Redis database")
	fs.StringVar(&f.UploadDir, "upload-dir", DefaultUploadDir, "gateway upload directory used for seed asset files")
	fs.IntVar(&f.Users, "users", 10000, "number of seed users to create")
	fs.IntVar(&f.Videos, "videos", 5000, "number of seed videos to create")
	fs.BoolVar(&f.Reset, "reset", false, "delete existing seed/loadtest business data before inserting new rows")
	fs.BoolVar(&f.ResetRedis, "reset-redis", false, "with -reset, delete feedsystem-zero Redis keys (fsz:*)")
	fs.IntVar(&f.VideoTimeSpanDays, "video-span-days", 30, "distribute video created_at over the past N days")
	fs.IntVar(&f.FileAssetBuckets, "file-buckets", 20, "how many distinct file_assets rows placeholder videos share")
}

// LoadTestFlags collects flags shared by every scenario in cmd/loadtest.
type LoadTestFlags struct {
	Scenario    string
	BaseURL     string
	MysqlDSN    string
	Concurrency int
	Duration    time.Duration
	Warmup      time.Duration
	Timeout     time.Duration
	// LoginPoolSize caps how many users we pre-login before the run starts;
	// each worker picks a token round-robin, avoiding thundering-herd on
	// the login endpoint itself.
	LoginPoolSize int
	// TargetPoolSize caps how many videos / users we sample for the hot path.
	TargetPoolSize int
	Verbose        bool
}

// RegisterLoadTestFlags binds LoadTestFlags to a flag.FlagSet.
func RegisterLoadTestFlags(fs *flag.FlagSet, f *LoadTestFlags) {
	fs.StringVar(&f.Scenario, "scenario", "like", "scenario name: like | follow | following_feed | hot_feed | publish_video")
	fs.StringVar(&f.BaseURL, "base", DefaultGatewayBase, "gateway base URL")
	fs.StringVar(&f.MysqlDSN, "mysql", DefaultMysqlDSN, "MySQL DSN, used to sample seed users / videos")
	fs.IntVar(&f.Concurrency, "c", 50, "concurrent workers")
	fs.DurationVar(&f.Duration, "d", 30*time.Second, "test duration (excluding warmup)")
	fs.DurationVar(&f.Warmup, "warmup", 3*time.Second, "warmup duration before metrics collection starts")
	fs.DurationVar(&f.Timeout, "timeout", 5*time.Second, "per-request HTTP timeout")
	fs.IntVar(&f.LoginPoolSize, "login-pool", 200, "number of users to pre-login and rotate as callers")
	fs.IntVar(&f.TargetPoolSize, "target-pool", 2000, "number of target videos/users sampled from seed data")
	fs.BoolVar(&f.Verbose, "v", false, "verbose logging (per-request errors)")
}
