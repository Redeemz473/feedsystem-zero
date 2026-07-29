package logic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"feedsystem-zero/apps/gateway/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// upsertFileAsset 登记文件并返回数据库中的规范资产地址。
// file_hash 唯一，因此同一内容即使使用不同扩展名再次上传，也复用已有 URL，
// 不允许把已被视频引用的记录悄悄改指向另一个路径。
func upsertFileAsset(ctx context.Context, db *gorm.DB, fileType string, fileHash string, url string, storagePath string, size int64) (model.FileAsset, error) {
	if strings.TrimSpace(fileHash) == "" || strings.TrimSpace(url) == "" || strings.TrimSpace(storagePath) == "" {
		return model.FileAsset{}, errors.New("文件资产参数不完整")
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

	result := db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "file_hash"}},
		DoNothing: true,
	}).Create(&asset)
	if result.Error != nil {
		return model.FileAsset{}, result.Error
	}
	if result.RowsAffected == 1 {
		return asset, nil
	}

	var existing model.FileAsset
	cleaningWaitDeadline := time.Now().Add(5 * time.Second)
	for {
		if err := db.WithContext(ctx).
			Where("file_hash = ?", fileHash).
			First(&existing).Error; err != nil {
			return model.FileAsset{}, err
		}
		if existing.FileType != fileType {
			return model.FileAsset{}, fmt.Errorf("文件 hash 已被另一种资产类型占用: %s", existing.FileType)
		}
		if existing.Status != model.FileAssetStatusCleaning {
			break
		}
		if time.Now().After(cleaningWaitDeadline) {
			return model.FileAsset{}, errors.New("文件资产正在清理，请稍后重试")
		}

		// cleanup job 已认领该资产时等待它完成物理删除，再决定是否重新激活。
		// 不能直接把 Cleaning 改回 Active，否则 job 可能在激活后删除新上传文件。
		select {
		case <-ctx.Done():
			return model.FileAsset{}, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}

	if existing.Status != model.FileAssetStatusActive {
		if _, err := os.Stat(storagePath); err != nil {
			return model.FileAsset{}, fmt.Errorf("待登记文件不可用: %w", err)
		}
		update := db.WithContext(ctx).
			Model(&model.FileAsset{}).
			Where("id = ? AND status = ?", existing.ID, existing.Status).
			Updates(map[string]any{
				"url":          url,
				"storage_path": storagePath,
				"size":         size,
				"status":       model.FileAssetStatusActive,
				"deleted_at":   nil,
			})
		if update.Error != nil {
			return model.FileAsset{}, update.Error
		}
		if update.RowsAffected == 0 {
			// 状态在查询后被 cleanup job 抢占，重新读取并按最新状态处理。
			return upsertFileAsset(ctx, db, fileType, fileHash, url, storagePath, size)
		}
		existing.URL = url
		existing.StoragePath = storagePath
		existing.Size = size
		existing.Status = model.FileAssetStatusActive
		existing.DeletedAt = nil
		return existing, nil
	}

	// 已有活跃资产是规范副本。新上传路径不同时删除重复文件，调用方统一返回已有 URL。
	existingAbs, existingAbsErr := filepath.Abs(existing.StoragePath)
	uploadedAbs, uploadedAbsErr := filepath.Abs(storagePath)
	samePath := existingAbsErr == nil && uploadedAbsErr == nil && existingAbs == uploadedAbs
	if !samePath {
		if _, err := os.Stat(existing.StoragePath); err == nil {
			if err := os.Remove(storagePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return model.FileAsset{}, err
			}
		} else if errors.Is(err, os.ErrNotExist) {
			// 记录存在但文件意外丢失时，用本次已通过 hash 校验的副本修复规范路径，
			// 保持已有 videos.play_url / cover_url 不变。
			if err := os.MkdirAll(filepath.Dir(existing.StoragePath), 0755); err != nil {
				return model.FileAsset{}, err
			}
			if err := os.Rename(storagePath, existing.StoragePath); err != nil {
				return model.FileAsset{}, err
			}
		} else {
			return model.FileAsset{}, err
		}
	}
	return existing, nil
}
