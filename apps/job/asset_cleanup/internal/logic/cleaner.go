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
	svcCtx          *svc.ServiceContext
	reconcileCursor uint64
	logx.Logger
}

func NewCleaner(svcCtx *svc.ServiceContext) *Cleaner {
	return &Cleaner{
		svcCtx: svcCtx,
		Logger: logx.WithContext(context.Background()),
	}
}

func (c *Cleaner) Run(ctx context.Context) error {
	cleanupInterval := time.Duration(c.svcCtx.Config.AssetCleanup.PollIntervalSeconds) * time.Second
	if cleanupInterval <= 0 {
		cleanupInterval = 30 * time.Second
	}
	reconcileInterval := time.Duration(c.svcCtx.Config.AssetCleanup.ReconcileIntervalSeconds) * time.Second
	if reconcileInterval <= 0 {
		reconcileInterval = time.Minute
	}

	if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.Errorf("initial asset cleanup failed: %v", err)
	}
	if err := c.runReconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.Errorf("initial asset reconcile failed: %v", err)
	}

	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()
	reconcileTicker := time.NewTicker(reconcileInterval)
	defer reconcileTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cleanupTicker.C:
			if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.Errorf("asset cleanup cycle failed: %v", err)
			}
		case <-reconcileTicker.C:
			if err := c.runReconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.Errorf("asset reconcile cycle failed: %v", err)
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

// runReconcileOnce 按主键游标检查一批 Active 资产，避免每轮都从表头扫描热点行。
func (c *Cleaner) runReconcileOnce(ctx context.Context) error {
	assets, err := c.loadActiveAssetBatch(ctx, c.reconcileCursor)
	if err != nil {
		return err
	}
	if len(assets) == 0 {
		c.reconcileCursor = 0
		return nil
	}

	for i := range assets {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.reconcileCursor = assets[i].ID
		if err := c.reconcileActiveAsset(ctx, assets[i].ID); err != nil {
			c.Errorf("reconcile active file asset failed, asset_id:%d error:%v", assets[i].ID, err)
		}
	}

	if len(assets) < c.reconcileBatchSize() {
		c.reconcileCursor = 0
	}
	return nil
}

func (c *Cleaner) loadActiveAssetBatch(ctx context.Context, afterID uint64) ([]model.FileAsset, error) {
	assets := make([]model.FileAsset, 0, c.reconcileBatchSize())
	err := c.svcCtx.GormDB.WithContext(ctx).
		Select("id").
		Where("status = ? AND id > ?", model.FileAssetStatusActive, afterID).
		Order("id ASC").
		Limit(c.reconcileBatchSize()).
		Find(&assets).Error
	return assets, err
}

func (c *Cleaner) reconcileBatchSize() int {
	batchSize := c.svcCtx.Config.AssetCleanup.ReconcileBatchSize
	if batchSize <= 0 {
		batchSize = 200
	}
	if batchSize > 500 {
		batchSize = 500
	}
	return batchSize
}

type assetReconcileResult struct {
	Asset             model.FileAsset
	Found             bool
	ActualRefs        int64
	FileMissing       bool
	RefCountCorrected bool
	MarkedDeleted     bool
}

// reconcileActiveAsset 在资产行锁内读取真实引用数，防止与发布和删除事务交错后覆盖新状态。
func (c *Cleaner) reconcileActiveAsset(ctx context.Context, assetID uint64) error {
	var result assetReconcileResult
	err := c.svcCtx.GormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var asset model.FileAsset
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND status = ?", assetID, model.FileAssetStatusActive).
			First(&asset).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}

		actualRefs, err := countActiveVideoReferences(tx, asset.URL)
		if err != nil {
			return err
		}
		fileExists, err := assetFileExists(c.svcCtx.Config.Upload.Dir, asset.StoragePath)
		if err != nil {
			return err
		}

		result.Asset = asset
		result.Found = true
		result.ActualRefs = actualRefs
		result.FileMissing = !fileExists
		result.RefCountCorrected = asset.RefCount != actualRefs

		updates := make(map[string]any)
		if result.RefCountCorrected {
			updates["ref_count"] = actualRefs
		}
		if !fileExists && actualRefs == 0 {
			now := time.Now()
			updates["status"] = model.FileAssetStatusDeleted
			updates["deleted_at"] = now
			updates["updated_at"] = now
			result.MarkedDeleted = true
		} else if asset.DeletedAt != nil {
			updates["deleted_at"] = nil
		}
		if len(updates) == 0 {
			return nil
		}
		return tx.Model(&model.FileAsset{}).
			Where("id = ? AND status = ?", asset.ID, model.FileAssetStatusActive).
			Updates(updates).Error
	})
	if err != nil || !result.Found {
		return err
	}

	if result.FileMissing {
		// DB 状态仍会在每次秒传时复核；这里只清理全局加速 key，不扫描用户级 key。
		if err := c.svcCtx.RedisCli.Del(ctx, rediskey.ChunkUploadGlobalHashKey(result.Asset.FileHash)).Err(); err != nil {
			c.Errorf("invalidate missing asset instant upload cache failed, asset_id:%d hash:%s error:%v", result.Asset.ID, result.Asset.FileHash, err)
		}
	}
	if result.MarkedDeleted {
		c.Infof(
			"missing unreferenced file asset marked deleted, asset_id:%d url:%s",
			result.Asset.ID,
			result.Asset.URL,
		)
	} else if result.FileMissing && result.ActualRefs > 0 {
		c.Errorf(
			"active file asset is missing on disk, asset_id:%d url:%s actual_refs:%d; existing videos require repair",
			result.Asset.ID,
			result.Asset.URL,
			result.ActualRefs,
		)
	} else if result.RefCountCorrected {
		c.Infof(
			"file asset ref_count reconciled, asset_id:%d old:%d new:%d",
			result.Asset.ID,
			result.Asset.RefCount,
			result.ActualRefs,
		)
	}
	return nil
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

		activeRefs, err := countActiveVideoReferences(tx, asset.URL)
		if err != nil {
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

// countActiveVideoReferences 分别统计 play_url 和 cover_url，确保同一视频两个字段都引用
// 相同资产时按两个逻辑引用计数，与发布阶段的两次 reserve 保持一致。
func countActiveVideoReferences(tx *gorm.DB, url string) (int64, error) {
	var row struct {
		Count int64 `gorm:"column:ref_count"`
	}
	err := tx.Raw(
		`SELECT
			(SELECT COUNT(*) FROM videos WHERE status = ? AND deleted_at IS NULL AND play_url = ?)
			+
			(SELECT COUNT(*) FROM videos WHERE status = ? AND deleted_at IS NULL AND cover_url = ?)
			AS ref_count`,
		model.VideoStatusNormal,
		url,
		model.VideoStatusNormal,
		url,
	).Scan(&row).Error
	return row.Count, err
}

// assetFileExists 仅检查上传目录内的普通文件。路径异常和非普通文件都作为错误处理，
// 避免对账任务把配置错误误判为“文件已丢失”并修改数据库状态。
func assetFileExists(baseDir string, storagePath string) (bool, error) {
	pathAbs, err := resolveAssetStoragePath(baseDir, storagePath)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(pathAbs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("资产路径不是普通文件: %s", pathAbs)
	}
	return true, nil
}

func resolveAssetStoragePath(baseDir string, storagePath string) (string, error) {
	baseAbs, err := filepath.Abs(strings.TrimSpace(baseDir))
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(storagePath) == "" {
		return "", errors.New("storage_path 为空")
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
		return "", err
	}
	if pathAbs == baseAbs || !strings.HasPrefix(pathAbs, baseAbs+string(os.PathSeparator)) {
		return "", fmt.Errorf("拒绝访问上传目录之外的文件: %s", pathAbs)
	}
	return pathAbs, nil
}

func removeAssetFile(ctx context.Context, baseDir string, storagePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pathAbs, err := resolveAssetStoragePath(baseDir, storagePath)
	if err != nil {
		return err
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
