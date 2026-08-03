package logic

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"feedsystem-zero/apps/video/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// orderedFileAssetURLs 为同一事务涉及的资产 URL 提供稳定的加锁顺序。
// 保留重复 URL，因为视频地址和封面地址即使相同，也代表两个逻辑引用。
func orderedFileAssetURLs(urls ...string) []string {
	ordered := append([]string(nil), urls...)
	sort.Strings(ordered)
	return ordered
}

// reserveFileAssetRefByURL 在给定的 db(可以是 *gorm.DB 事务句柄) 内，
// 将 file_assets 表中 url 对应资产的 ref_count +1。
// 如果资产不存在（或已被删除），返回 gorm.ErrRecordNotFound，
// 由调用方决定如何转成业务错误码。
func reserveFileAssetRefByURL(ctx context.Context, db *gorm.DB, url string) error {
	if strings.TrimSpace(url) == "" {
		return nil
	}

	result := db.WithContext(ctx).
		Model(&model.FileAsset{}).
		Where("url = ? AND status = ?", url, model.FileAssetStatusActive).
		Updates(map[string]any{
			"ref_count":  gorm.Expr("ref_count + 1"),
			"status":     model.FileAssetStatusActive,
			"deleted_at": nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// releaseFileAssetRefByURL 在给定的 db 内，将 file_assets 表中 url 对应资产的 ref_count -1（不低于 0）。
// 目前保留导出给未来的补偿/回滚路径使用；本地事务模式下，事务失败会自动回滚 reserve，不需要显式 release。
func releaseFileAssetRefByURL(ctx context.Context, db *gorm.DB, url string) error {
	if strings.TrimSpace(url) == "" {
		return nil
	}

	return db.WithContext(ctx).
		Model(&model.FileAsset{}).
		Where("url = ? AND status <> ?", url, model.FileAssetStatusDeleted).
		Update("ref_count", gorm.Expr("GREATEST(ref_count - 1, 0)")).Error
}

// decreaseFileAssetRefInTx 在给定事务句柄内，对 url 对应的资产做「引用计数 -1」。
// 与旧版 gateway.decreaseFileAssetRefAndCleanup 的区别：
//  1. 完全在调用方传入的事务 tx 内执行，可以和 videos 软删同事务提交/回滚，保证一致性；
//  2. 不做物理文件删除，减到 0 时只把 status 置为 PendingDelete + deleted_at=now，
//     真正的磁盘清理交给独立的 asset_cleanup job 扫 PendingDelete 后再处理；
//  3. 依旧会 SELECT COUNT(*) FROM videos 做实时校准，防止历史遗留脏数据把 ref_count 打到 0 后误清理。
func decreaseFileAssetRefInTx(ctx context.Context, tx *gorm.DB, url string) error {
	if strings.TrimSpace(url) == "" {
		return nil
	}

	var asset model.FileAsset
	if err := tx.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("url = ? AND status <> ?", url, model.FileAssetStatusDeleted).
		First(&asset).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 资产已被彻底清理或从未登记，视为幂等成功。
			return nil
		}
		return err
	}

	// 用当前视频表的真实活跃引用数校准，避免历史漂移导致误清理。
	// 注意：此刻调用方事务里的 videos 软删已经生效（同事务可见），
	// 所以这里 count 的结果就是"扣掉本次删除之后"的真实引用数。
	var activeRefs int64
	if err := tx.WithContext(ctx).Table("videos").
		Where("status = ? AND deleted_at IS NULL AND (play_url = ? OR cover_url = ?)",
			model.VideoStatusNormal, url, url).
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
		return tx.WithContext(ctx).Model(&model.FileAsset{}).
			Where("id = ?", asset.ID).
			Updates(map[string]any{
				"ref_count":  nextRef,
				"status":     model.FileAssetStatusActive,
				"deleted_at": nil,
			}).Error
	}

	// 引用降到 0：置 PendingDelete，磁盘物理清理由独立 job 完成。
	now := time.Now()
	return tx.WithContext(ctx).Model(&model.FileAsset{}).
		Where("id = ?", asset.ID).
		Updates(map[string]any{
			"ref_count":  0,
			"status":     model.FileAssetStatusPendingDelete,
			"deleted_at": now,
		}).Error
}
