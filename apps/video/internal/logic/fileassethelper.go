package logic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"feedsystem-zero/apps/video/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errFileAssetStorageUnavailable = errors.New("file asset storage unavailable")

type fileAssetRef struct {
	URL   string
	Delta int64
}

type preparedPublishFileAsset struct {
	ID          uint64
	URL         string
	StoragePath string
	RefDelta    int64
}

type publishFileAssetError struct {
	URL string
	Err error
}

func (e *publishFileAssetError) Error() string {
	return fmt.Sprintf("publish file asset unavailable, url:%s: %v", e.URL, e.Err)
}

func (e *publishFileAssetError) Unwrap() error {
	return e.Err
}

// aggregateFileAssetRefs 将相同 URL 聚合为一个引用增量，同时按 URL 排序固定行锁顺序。
// 视频地址和封面地址相同时 Delta=2，仍然保留两个逻辑引用。
func aggregateFileAssetRefs(urls ...string) []fileAssetRef {
	counts := make(map[string]int64, len(urls))
	for _, rawURL := range urls {
		url := strings.TrimSpace(rawURL)
		if url == "" {
			continue
		}
		counts[url]++
	}

	orderedURLs := make([]string, 0, len(counts))
	for url := range counts {
		orderedURLs = append(orderedURLs, url)
	}
	sort.Strings(orderedURLs)

	refs := make([]fileAssetRef, 0, len(orderedURLs))
	for _, url := range orderedURLs {
		refs = append(refs, fileAssetRef{URL: url, Delta: counts[url]})
	}
	return refs
}

// preparePublishFileAssets 在事务外一次性读取并检查发布所需的唯一资产。
// 磁盘 I/O 不占用数据库行锁；事务内仍会再次用 status/storage_path 条件防止状态并发变化。
func preparePublishFileAssets(ctx context.Context, db *gorm.DB, urls ...string) ([]preparedPublishFileAsset, error) {
	refs := aggregateFileAssetRefs(urls...)
	if len(refs) == 0 {
		return nil, nil
	}

	orderedURLs := make([]string, 0, len(refs))
	for _, ref := range refs {
		orderedURLs = append(orderedURLs, ref.URL)
	}

	var assets []model.FileAsset
	if err := db.WithContext(ctx).
		Select("id", "url", "storage_path").
		Where("url IN ? AND status = ?", orderedURLs, model.FileAssetStatusActive).
		Find(&assets).Error; err != nil {
		return nil, err
	}
	return buildPreparedPublishFileAssets(refs, assets)
}

func buildPreparedPublishFileAssets(
	refs []fileAssetRef,
	assets []model.FileAsset,
) ([]preparedPublishFileAsset, error) {
	assetsByURL := make(map[string]model.FileAsset, len(assets))
	for _, asset := range assets {
		assetsByURL[asset.URL] = asset
	}

	prepared := make([]preparedPublishFileAsset, 0, len(refs))
	for _, ref := range refs {
		asset, ok := assetsByURL[ref.URL]
		if !ok {
			return nil, &publishFileAssetError{URL: ref.URL, Err: gorm.ErrRecordNotFound}
		}
		if err := validatePublishAssetFile(asset.StoragePath); err != nil {
			return nil, &publishFileAssetError{
				URL: ref.URL,
				Err: fmt.Errorf("validate asset_id:%d: %w", asset.ID, err),
			}
		}
		prepared = append(prepared, preparedPublishFileAsset{
			ID:          asset.ID,
			URL:         asset.URL,
			StoragePath: asset.StoragePath,
			RefDelta:    ref.Delta,
		})
	}
	return prepared, nil
}

// reservePreparedPublishFileAsset 在发布事务中使用条件原子更新增加引用数。
// UPDATE 本身会持有资产行锁，并与删除、cleanup 状态迁移串行化；调用方必须传入事务句柄。
func reservePreparedPublishFileAsset(ctx context.Context, db *gorm.DB, asset preparedPublishFileAsset) error {
	if asset.ID == 0 || asset.RefDelta <= 0 {
		return &publishFileAssetError{URL: asset.URL, Err: gorm.ErrRecordNotFound}
	}

	result := updatePreparedPublishFileAssetRef(ctx, db, asset)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return &publishFileAssetError{URL: asset.URL, Err: gorm.ErrRecordNotFound}
	}
	return nil
}

func updatePreparedPublishFileAssetRef(
	ctx context.Context,
	db *gorm.DB,
	asset preparedPublishFileAsset,
) *gorm.DB {
	return db.WithContext(ctx).
		Model(&model.FileAsset{}).
		Where(
			"id = ? AND url = ? AND storage_path = ? AND status = ?",
			asset.ID,
			asset.URL,
			asset.StoragePath,
			model.FileAssetStatusActive,
		).
		Updates(map[string]any{
			"ref_count":  gorm.Expr("ref_count + ?", asset.RefDelta),
			"deleted_at": nil,
		})
}

func unavailablePublishFileAssetURL(err error) (string, bool) {
	var assetErr *publishFileAssetError
	if !errors.As(err, &assetErr) {
		return "", false
	}
	if !errors.Is(assetErr, gorm.ErrRecordNotFound) &&
		!errors.Is(assetErr, errFileAssetStorageUnavailable) {
		return "", false
	}
	return assetErr.URL, true
}

func validatePublishAssetFile(storagePath string) error {
	path := strings.TrimSpace(storagePath)
	if path == "" {
		return fmt.Errorf("%w: storage_path is empty", errFileAssetStorageUnavailable)
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: file does not exist", errFileAssetStorageUnavailable)
		}
		return fmt.Errorf("stat asset file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: path is not a regular file", errFileAssetStorageUnavailable)
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
