package logic

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveAssetFile(t *testing.T) {
	baseDir := t.TempDir()
	videoDir := filepath.Join(baseDir, "videos")
	if err := os.MkdirAll(videoDir, 0755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(videoDir, "video.mp4")
	if err := os.WriteFile(target, []byte("video"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := removeAssetFile(context.Background(), baseDir, target); err != nil {
		t.Fatalf("removeAssetFile() error = %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target should be removed, stat error = %v", err)
	}
}

func TestRemoveAssetFileSupportsLegacyRelativePath(t *testing.T) {
	baseDir := t.TempDir()
	target := filepath.Join(baseDir, "videos", "legacy.mp4")
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("video"), 0644); err != nil {
		t.Fatal(err)
	}

	legacyPath := filepath.Join(filepath.Base(baseDir), "videos", "legacy.mp4")
	if err := removeAssetFile(context.Background(), baseDir, legacyPath); err != nil {
		t.Fatalf("removeAssetFile() error = %v", err)
	}
}

func TestRemoveAssetFileRejectsOutsideBaseDir(t *testing.T) {
	baseDir := t.TempDir()
	outsideDir := t.TempDir()
	target := filepath.Join(outsideDir, "outside.mp4")
	if err := os.WriteFile(target, []byte("video"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := removeAssetFile(context.Background(), baseDir, target); err == nil {
		t.Fatal("removeAssetFile() should reject paths outside the upload directory")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("outside file should remain, stat error = %v", err)
	}
}

func TestAssetFileExists(t *testing.T) {
	baseDir := t.TempDir()
	target := filepath.Join(baseDir, "videos", "video.mp4")
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("video"), 0644); err != nil {
		t.Fatal(err)
	}

	exists, err := assetFileExists(baseDir, target)
	if err != nil || !exists {
		t.Fatalf("assetFileExists() = (%v, %v), want (true, nil)", exists, err)
	}

	exists, err = assetFileExists(baseDir, filepath.Join(baseDir, "videos", "missing.mp4"))
	if err != nil || exists {
		t.Fatalf("missing assetFileExists() = (%v, %v), want (false, nil)", exists, err)
	}

	if _, err := assetFileExists(baseDir, baseDir); err == nil {
		t.Fatal("assetFileExists() should reject the upload root itself")
	}
}

func TestAssetFileExistsSupportsLegacyRelativePath(t *testing.T) {
	baseDir := t.TempDir()
	target := filepath.Join(baseDir, "videos", "legacy.mp4")
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("video"), 0644); err != nil {
		t.Fatal(err)
	}

	legacyPath := filepath.Join(filepath.Base(baseDir), "videos", "legacy.mp4")
	exists, err := assetFileExists(baseDir, legacyPath)
	if err != nil || !exists {
		t.Fatalf("legacy assetFileExists() = (%v, %v), want (true, nil)", exists, err)
	}
}
