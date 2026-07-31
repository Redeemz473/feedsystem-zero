// Command seed populates MySQL with a large steady-state of test users and
// videos so the load tester has realistic data to hit.
//
// Usage:
//
//	go run ./tests/cmd/seed              # 10000 users + 5000 videos (default)
//	go run ./tests/cmd/seed -users 20000 -videos 8000
//	go run ./tests/cmd/seed -reset       # drop previous seed data first
//
// Seeded data is namespaced (seed_user_*, seed-video-*, /seed/*) so it never
// touches real accounts or uploads.
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"feedsystem-zero/tests/internal/seed"
	"feedsystem-zero/tests/internal/testconfig"
)

func main() {
	var flags testconfig.SeedFlags
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	testconfig.RegisterSeedFlags(fs, &flags)
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatalf("parse flags: %v", err)
	}
	if flags.Users <= 0 || flags.Videos <= 0 {
		log.Fatalf("-users and -videos must be positive; got users=%d videos=%d", flags.Users, flags.Videos)
	}

	log.Printf("[seed] target: %d users, %d videos, span=%d days, buckets=%d",
		flags.Users, flags.Videos, flags.VideoTimeSpanDays, flags.FileAssetBuckets)

	runner, err := seed.Open(flags)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer func() {
		if sqlDB, err := runner.DB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}()

	if err := runner.Run(context.Background()); err != nil {
		log.Fatalf("seed run: %v", err)
	}
	log.Println("[seed] done.")
}
