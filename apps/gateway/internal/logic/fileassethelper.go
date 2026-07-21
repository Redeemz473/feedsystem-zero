package logic

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"feedsystem-zero/apps/gateway/internal/config"
	"feedsystem-zero/apps/gateway/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func upsertFileAsset(ctx context.Context, db *gorm.DB, fileType string, fileHash string, url string, storagePath string, size int64) error {
	if strings.TrimSpace(fileHash) == "" || strings.TrimSpace(url) == "" || strings.TrimSpace(storagePath) == "" {
		return nil
	}

	asset := model.FileAsset{
		FileHash:    fileHash,
		FileType:    fileType,
		URL:         url,
		StoragePath: storagePath,
		Size:        size,
		RefCount:    0,
		Status:      model.FileAssetStatusActive,
	}

	return db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "file_hash"}},
		DoUpdates: clause.Assignments(map[string]any{
			"file_type":    fileType,
			"url":          url,
			"storage_path": storagePath,
			"size":         size,
			"status":       model.FileAssetStatusActive,
			"deleted_at":   nil,
		}),
	}).Create(&asset).Error
}

// NOTE: file_assets 的引用计数变更 (+1 / -1) 已经全部下沉到 video-rpc 端，
//       与 videos 表的写入放在同一个本地事务里保证强一致性；
//       gateway 侧的 reserveFileAssetRefByURL / releaseFileAssetRefByURL 已删除。
//
//       decreaseFileAssetRefAndCleanup 和 removeLocalAssetFile 当前不再被调用，
//       保留作为未来 asset_cleanup job 的参考实现——
//       该 job 会扫 file_assets 中 status=PendingDelete 的记录，
//       调用类似逻辑做磁盘物理清理并最终置 Deleted。
//       等 job 独立成型后，这两个函数可以整体迁移到 apps/job/asset_cleanup 里删除。

func decreaseFileAssetRefAndCleanup(ctx context.Context, db *gorm.DB, upload config.UploadConf, url string) error {
	if strings.TrimSpace(url) == "" {
		return nil
	}

	var cleanupPath string
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var asset model.FileAsset
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("url = ? AND status <> ?", url, model.FileAssetStatusDeleted).
			First(&asset).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}

		var activeRefs int64
		if err := tx.Table("videos").
			Where("status = ? AND deleted_at IS NULL AND (play_url = ? OR cover_url = ?)", 1, url, url).
			Count(&activeRefs).Error; err != nil {
			return err
		}

		nextRef := asset.RefCount - 1
		if nextRef < 0 {
			nextRef = 0
		}
		if activeRefs > nextRef {
			nextRef = activeRefs
		}
		if nextRef > 0 {
			return tx.Model(&model.FileAsset{}).
				Where("id = ?", asset.ID).
				Updates(map[string]any{
					"ref_count":  nextRef,
					"status":     model.FileAssetStatusActive,
					"deleted_at": nil,
				}).Error
		}

		now := time.Now()
		if err := tx.Model(&model.FileAsset{}).
			Where("id = ?", asset.ID).
			Updates(map[string]any{
				"ref_count":  0,
				"status":     model.FileAssetStatusPendingDelete,
				"deleted_at": now,
			}).Error; err != nil {
			return err
		}
		cleanupPath = asset.StoragePath
		return nil
	}); err != nil {
		return err
	}
	if cleanupPath == "" {
		return nil
	}

	if err := removeLocalAssetFile(upload, cleanupPath); err != nil {
		return err
	}

	return db.WithContext(ctx).
		Model(&model.FileAsset{}).
		Where("url = ? AND status = ? AND ref_count = 0", url, model.FileAssetStatusPendingDelete).
		Update("status", model.FileAssetStatusDeleted).Error
}

func removeLocalAssetFile(upload config.UploadConf, storagePath string) error {
	if strings.TrimSpace(storagePath) == "" {
		return nil
	}

	baseAbs, err := filepath.Abs(uploadBaseDir(upload))
	if err != nil {
		return err
	}
	pathAbs, err := filepath.Abs(storagePath)
	if err != nil {
		return err
	}
	if pathAbs != baseAbs && !strings.HasPrefix(pathAbs, baseAbs+string(os.PathSeparator)) {
		return nil
	}

	if err := os.Remove(pathAbs); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
