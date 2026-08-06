package logic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"feedsystem-zero/apps/video/internal/model"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestAggregateFileAssetRefs(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []fileAssetRef
	}{
		{
			name: "sorts lock order",
			in:   []string{"/uploads/video_b.mp4", "/uploads/video_a.mp4"},
			want: []fileAssetRef{
				{URL: "/uploads/video_a.mp4", Delta: 1},
				{URL: "/uploads/video_b.mp4", Delta: 1},
			},
		},
		{
			name: "aggregates duplicate logical references",
			in:   []string{"/uploads/shared.mp4", "/uploads/shared.mp4"},
			want: []fileAssetRef{{URL: "/uploads/shared.mp4", Delta: 2}},
		},
		{
			name: "ignores blank urls",
			in:   []string{"", "  ", " /uploads/video.mp4 "},
			want: []fileAssetRef{{URL: "/uploads/video.mp4", Delta: 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := aggregateFileAssetRefs(tt.in...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("aggregateFileAssetRefs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnavailablePublishFileAssetURL(t *testing.T) {
	missingErr := &publishFileAssetError{
		URL: "/uploads/missing.mp4",
		Err: gorm.ErrRecordNotFound,
	}
	url, ok := unavailablePublishFileAssetURL(missingErr)
	if !ok || url != missingErr.URL {
		t.Fatalf("unavailablePublishFileAssetURL() = (%q, %v), want (%q, true)", url, ok, missingErr.URL)
	}

	storageErr := &publishFileAssetError{
		URL: "/uploads/deleted.mp4",
		Err: fmt.Errorf("wrapped: %w", errFileAssetStorageUnavailable),
	}
	url, ok = unavailablePublishFileAssetURL(storageErr)
	if !ok || url != storageErr.URL {
		t.Fatalf("unavailablePublishFileAssetURL() = (%q, %v), want (%q, true)", url, ok, storageErr.URL)
	}

	if url, ok = unavailablePublishFileAssetURL(errors.New("database timeout")); ok || url != "" {
		t.Fatalf("unexpected unavailable result for infrastructure error: (%q, %v)", url, ok)
	}
}

func TestBuildPreparedPublishFileAssets(t *testing.T) {
	dir := t.TempDir()
	sharedPath := filepath.Join(dir, "shared.bin")
	if err := os.WriteFile(sharedPath, []byte("asset"), 0644); err != nil {
		t.Fatal(err)
	}

	refs := aggregateFileAssetRefs("/uploads/shared.bin", "/uploads/shared.bin")
	prepared, err := buildPreparedPublishFileAssets(refs, []model.FileAsset{{
		ID:          7,
		URL:         "/uploads/shared.bin",
		StoragePath: sharedPath,
	}})
	if err != nil {
		t.Fatalf("build prepared assets: %v", err)
	}
	want := []preparedPublishFileAsset{{
		ID:          7,
		URL:         "/uploads/shared.bin",
		StoragePath: sharedPath,
		RefDelta:    2,
	}}
	if !reflect.DeepEqual(prepared, want) {
		t.Fatalf("prepared assets = %#v, want %#v", prepared, want)
	}

	_, err = buildPreparedPublishFileAssets(
		[]fileAssetRef{{URL: "/uploads/missing.bin", Delta: 1}},
		nil,
	)
	if url, ok := unavailablePublishFileAssetURL(err); !ok || url != "/uploads/missing.bin" {
		t.Fatalf("missing asset error = %v, url = %q, unavailable = %v", err, url, ok)
	}
}

func TestUpdatePreparedPublishFileAssetRefUsesConditionalAtomicUpdate(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       "root:password@tcp(127.0.0.1:3306)/test",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run gorm: %v", err)
	}

	asset := preparedPublishFileAsset{
		ID:          9,
		URL:         "/uploads/shared.bin",
		StoragePath: "/srv/uploads/shared.bin",
		RefDelta:    2,
	}
	query := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return updatePreparedPublishFileAssetRef(context.Background(), tx, asset)
	})
	for _, fragment := range []string{
		"UPDATE `file_assets`",
		"`ref_count`=ref_count + 2",
		"id = 9 AND url = '/uploads/shared.bin' AND storage_path = '/srv/uploads/shared.bin' AND status = 1",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("conditional update missing %q: %s", fragment, query)
		}
	}
}

func TestValidatePublishAssetFile(t *testing.T) {
	dir := t.TempDir()
	regularFile := filepath.Join(dir, "video.mp4")
	if err := os.WriteFile(regularFile, []byte("video"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := validatePublishAssetFile(regularFile); err != nil {
		t.Fatalf("regular file should be available, error = %v", err)
	}

	tests := []struct {
		name string
		path string
	}{
		{name: "empty path", path: ""},
		{name: "missing file", path: filepath.Join(dir, "missing.mp4")},
		{name: "directory", path: dir},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePublishAssetFile(tt.path)
			if !errors.Is(err, errFileAssetStorageUnavailable) {
				t.Fatalf("error = %v, want errFileAssetStorageUnavailable", err)
			}
		})
	}
}
