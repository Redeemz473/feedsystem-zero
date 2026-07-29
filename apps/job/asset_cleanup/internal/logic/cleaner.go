package logic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"feedsystem-zero/apps/job/asset_cleanup/internal/model"
	"feedsystem-zero/apps/job/asset_cleanup/internal/svc"
	"feedsystem-zero/common/rediskey"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Cleaner struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCleaner(svcCtx *svc.ServiceContext) *Cleaner {
	return &Cleaner{
		svcCtx: svcCtx,
		Logger: logx.WithContext(context.Background()),
	}
}

func (c *Cleaner) Run(ctx context.Context) error {
	interval := time.Duration(c.svcCtx.Config.AssetCleanup.PollIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}

	if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.Errorf("initial asset cleanup failed: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.Errorf("asset cleanup cycle failed: %v", err)
			}
		}
	}
}

func (c *Cleaner) runOnce(ctx context.Context) error {
	ids, err := c.loadCandidateIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.cleanOne(ctx, id); err != nil {
			c.Errorf("clean file asset failed, asset_id:%d error:%v", id, err)
		}
	}
	return nil
}

func (c *Cleaner) loadCandidateIDs(ctx context.Context) ([]uint64, error) {
	conf := c.svcCtx.Config.AssetCleanup
	batchSize := conf.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	if batchSize > 500 {
		batchSize = 500
	}
	grace := time.Duration(conf.GraceSeconds) * time.Second
	if grace < 0 {
		grace = 0
	}
	claimTimeout := time.Duration(conf.ClaimTimeoutSeconds) * time.Second
	if claimTimeout <= 0 {
		claimTimeout = 5 * time.Minute
	}
	now := time.Now()

	var ids []uint64
	err := c.svcCtx.GormDB.WithContext(ctx).
		Model(&model.FileAsset{}).
		Where(
			"(status = ? AND ref_count = 0 AND deleted_at IS NOT NULL AND deleted_at <= ?) OR "+
				"(status = ? AND ref_count = 0 AND updated_at <= ?)",
			model.FileAssetStatusPendingDelete,
			now.Add(-grace),
			model.FileAssetStatusCleaning,
			now.Add(-claimTimeout),
		).
		Order("id ASC").
		Limit(batchSize).
		Pluck("id", &ids).Error
	return ids, err
}

func (c *Cleaner) cleanOne(ctx context.Context, assetID uint64) error {
	asset, claimed, err := c.claimAsset(ctx, assetID)
	if err != nil || !claimed {
		return err
	}

	timeout := time.Duration(c.svcCtx.Config.AssetCleanup.DeleteTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deleteCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := removeAssetFile(deleteCtx, c.svcCtx.Config.Upload.Dir, asset.StoragePath); err != nil {
		_ = c.svcCtx.GormDB.WithContext(ctx).
			Model(&model.FileAsset{}).
			Where("id = ? AND status = ? AND ref_count = 0", asset.ID, model.FileAssetStatusCleaning).
			Update("status", model.FileAssetStatusPendingDelete).Error
		return err
	}

	result := c.svcCtx.GormDB.WithContext(ctx).
		Model(&model.FileAsset{}).
		Where("id = ? AND status = ? AND ref_count = 0", asset.ID, model.FileAssetStatusCleaning).
		Updates(map[string]any{
			"status":     model.FileAssetStatusDeleted,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return nil
	}

	// 缓存删除失败不会回滚已完成的物理清理；Gateway 秒传还会以 DB 状态二次校验。
	if err := c.svcCtx.RedisCli.Del(ctx, rediskey.ChunkUploadGlobalHashKey(asset.FileHash)).Err(); err != nil {
		c.Errorf("invalidate instant upload cache failed, asset_id:%d hash:%s error:%v", asset.ID, asset.FileHash, err)
	}
	return nil
}

func (c *Cleaner) claimAsset(ctx context.Context, assetID uint64) (model.FileAsset, bool, error) {
	var asset model.FileAsset
	claimed := false
	err := c.svcCtx.GormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", assetID).
			First(&asset).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if asset.RefCount != 0 ||
			(asset.Status != model.FileAssetStatusPendingDelete && asset.Status != model.FileAssetStatusCleaning) {
			return nil
		}

		var activeRefs int64
		if err := tx.Table("videos").
			Where("status = ? AND deleted_at IS NULL AND (play_url = ? OR cover_url = ?)", 1, asset.URL, asset.URL).
			Count(&activeRefs).Error; err != nil {
			return err
		}
		if activeRefs > 0 {
			return tx.Model(&model.FileAsset{}).
				Where("id = ?", asset.ID).
				Updates(map[string]any{
					"ref_count":  activeRefs,
					"status":     model.FileAssetStatusActive,
					"deleted_at": nil,
				}).Error
		}

		result := tx.Model(&model.FileAsset{}).
			Where("id = ? AND ref_count = 0", asset.ID).
			Updates(map[string]any{
				"status":     model.FileAssetStatusCleaning,
				"updated_at": time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}
		claimed = result.RowsAffected == 1
		return nil
	})
	return asset, claimed, err
}

func removeAssetFile(ctx context.Context, baseDir string, storagePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	baseAbs, err := filepath.Abs(strings.TrimSpace(baseDir))
	if err != nil {
		return err
	}
	if strings.TrimSpace(storagePath) == "" {
		return errors.New("storage_path 为空")
	}

	pathAbs := storagePath
	if !filepath.IsAbs(pathAbs) {
		relative := filepath.Clean(pathAbs)
		baseNamePrefix := filepath.Base(baseAbs) + string(os.PathSeparator)
		relative = strings.TrimPrefix(relative, baseNamePrefix)
		pathAbs = filepath.Join(baseAbs, relative)
	}
	pathAbs, err = filepath.Abs(pathAbs)
	if err != nil {
		return err
	}
	if pathAbs == baseAbs || !strings.HasPrefix(pathAbs, baseAbs+string(os.PathSeparator)) {
		return fmt.Errorf("拒绝删除上传目录之外的文件: %s", pathAbs)
	}

	info, err := os.Stat(pathAbs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("拒绝删除非普通文件: %s", pathAbs)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Remove(pathAbs)
}
