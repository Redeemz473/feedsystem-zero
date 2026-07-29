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
