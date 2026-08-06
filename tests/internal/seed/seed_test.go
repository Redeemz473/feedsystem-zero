package seed

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"feedsystem-zero/tests/internal/testconfig"
)

func TestBuildSeedAssetsCreatesVideoCoverPairs(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	assets := buildSeedAssets(3, now, t.TempDir())
	if len(assets) != 6 {
		t.Fatalf("len(assets) = %d, want 6", len(assets))
	}

	seenHashes := make(map[string]struct{}, len(assets))
	seenURLs := make(map[string]struct{}, len(assets))
	for _, asset := range assets {
		if asset.FileType != "video" && asset.FileType != "cover" {
			t.Fatalf("unexpected file type %q", asset.FileType)
		}
		if asset.URL == "" || asset.StoragePath == "" || asset.FileHash == "" {
			t.Fatalf("incomplete asset: %+v", asset)
		}
		if _, ok := seenHashes[asset.FileHash]; ok {
			t.Fatalf("duplicate file hash %q", asset.FileHash)
		}
		if _, ok := seenURLs[asset.URL]; ok {
			t.Fatalf("duplicate URL %q", asset.URL)
		}
		seenHashes[asset.FileHash] = struct{}{}
		seenURLs[asset.URL] = struct{}{}
		if asset.RefCount != 0 || asset.Status != 1 {
			t.Fatalf("unexpected initial state: %+v", asset)
		}
		if !asset.CreatedAt.Equal(now) || !asset.UpdatedAt.Equal(now) {
			t.Fatalf("unexpected timestamps: %+v", asset)
		}
	}
}

func TestBuildSeedAssetsRejectsNonPositiveBucketCount(t *testing.T) {
	if assets := buildSeedAssets(0, time.Now(), t.TempDir()); len(assets) != 0 {
		t.Fatalf("len(assets) = %d, want 0", len(assets))
	}
}

func TestPrepareSeedAssetFiles(t *testing.T) {
	runner := Runner{Flags: testconfig.SeedFlags{UploadDir: t.TempDir()}}
	seedDir, err := runner.prepareSeedAssetFiles(2)
	if err != nil {
		t.Fatalf("prepareSeedAssetFiles() error = %v", err)
	}

	tests := []struct {
		name string
		size int64
	}{
		{name: "video_1.mp4", size: seedVideoFileSize},
		{name: "cover_2.jpg", size: seedCoverFileSize},
	}
	for _, tt := range tests {
		info, err := os.Stat(filepath.Join(seedDir, tt.name))
		if err != nil {
			t.Fatalf("stat %s: %v", tt.name, err)
		}
		if !info.Mode().IsRegular() || info.Size() != tt.size {
			t.Fatalf("%s mode=%v size=%d, want regular size=%d", tt.name, info.Mode(), info.Size(), tt.size)
		}
	}
}
