package logic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"feedsystem-zero/apps/job/event_cleanup/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultCleanupBatchSize          = 100
	maxCleanupBatchSize              = 5000
	defaultMaxBatchesPerRun          = 20
	maxCleanupBatchesPerRun          = 200
	defaultCleanupInterval           = 5 * time.Minute
	defaultDeleteTimeout             = 5 * time.Second
	defaultBatchInterval             = 200 * time.Millisecond
	defaultMaxRunDuration            = 30 * time.Second
	defaultOutboxSentRetention       = 7 * 24 * time.Hour
	outboxStatusSent           int32 = 2
)

type Cleaner struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

type cleanupTarget struct {
	Name         string
	TableName    string
	SelectIDsSQL string
	Args         []any
	BatchSize    int
	MaxBatches   int
}

type cleanupRow struct {
	ID uint64 `gorm:"column:id;primaryKey"`
}

func NewCleaner(svcCtx *svc.ServiceContext) *Cleaner {
	return &Cleaner{
		svcCtx: svcCtx,
		Logger: logx.WithContext(context.Background()),
	}
}

func (c *Cleaner) Run(ctx context.Context) error {
	if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.Errorf("initial event cleanup failed: %v", err)
	}

	interval := cleanupPollInterval(c.svcCtx.Config.EventCleanup.PollIntervalSeconds)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.Errorf("event cleanup cycle failed: %v", err)
			}
		}
	}
}

func (c *Cleaner) runOnce(ctx context.Context) error {
	conf := c.svcCtx.Config.EventCleanup
	runCtx, cancel := context.WithTimeout(ctx, cleanupMaxRunDuration(conf.MaxRunSeconds))
	defer cancel()

	batchSize := normalizeCleanupBatchSize(conf.BatchSize)
	maxBatches := normalizeMaxCleanupBatches(conf.MaxBatchesPerRun)
	now := time.Now()

	outboxRetention := durationHoursOrDefault(conf.OutboxSentRetentionHours, defaultOutboxSentRetention)
	targets := []cleanupTarget{
		{
			Name:      "outbox_events",
			TableName: "outbox_events",
			SelectIDsSQL: `SELECT id FROM outbox_events FORCE INDEX (idx_status_sent_id)
WHERE status = ? AND sent_at IS NOT NULL AND sent_at < ?
ORDER BY sent_at ASC, id ASC LIMIT ?`,
			Args:       []any{outboxStatusSent, now.Add(-outboxRetention)},
			BatchSize:  batchSize,
			MaxBatches: maxBatches,
		},
		{
			Name:      "processed_events",
			TableName: "processed_events",
			SelectIDsSQL: `SELECT id FROM processed_events FORCE INDEX (idx_expire_id)
WHERE expire_at IS NOT NULL AND expire_at < ?
ORDER BY expire_at ASC, id ASC LIMIT ?`,
			Args:       []any{now},
			BatchSize:  batchSize,
			MaxBatches: maxBatches,
		},
	}

	// 死信默认不自动删除，避免永久性坏消息还未审计就丢失。
	if conf.DeadLetterRetentionHours > 0 {
		targets = append(targets, cleanupTarget{
			Name:      "dead_letter_events",
			TableName: "dead_letter_events",
			SelectIDsSQL: `SELECT id FROM dead_letter_events FORCE INDEX (idx_created_id)
WHERE created_at < ?
ORDER BY created_at ASC, id ASC LIMIT ?`,
			Args:       []any{now.Add(-time.Duration(conf.DeadLetterRetentionHours) * time.Hour)},
			BatchSize:  batchSize,
			MaxBatches: maxBatches,
		})
	}

	var joined error
	for _, target := range targets {
		if err := runCtx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				logx.WithContext(ctx).Info("event cleanup reached per-run time budget; remaining rows will continue next cycle")
				return joined
			}
			return err
		}
		deleted, err := c.cleanupTarget(runCtx, target)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				logx.WithContext(ctx).Infof(
					"event cleanup reached per-run time budget, table:%s deleted:%d; remaining rows will continue next cycle",
					target.Name,
					deleted,
				)
				return joined
			}
			joined = errors.Join(joined, fmt.Errorf("cleanup %s failed: %w", target.Name, err))
			continue
		}
		if deleted > 0 {
			logx.WithContext(ctx).Infof("event cleanup completed, table:%s deleted:%d", target.Name, deleted)
		}
	}
	return joined
}

func (c *Cleaner) cleanupTarget(ctx context.Context, target cleanupTarget) (int64, error) {
	var deleted int64
	for batch := 0; batch < target.MaxBatches; batch++ {
		batchCtx, cancel := context.WithTimeout(ctx, cleanupDeleteTimeout(c.svcCtx.Config.EventCleanup.DeleteTimeoutMs))
		args := append(append([]any(nil), target.Args...), target.BatchSize)

		// 先用覆盖索引只读取主键，再按主键删除。这样不会让 DELETE 自己承担范围扫描和排序，
		// 锁范围也被限制在明确的一小批历史行。多实例选到相同 ID 时，少删或删不到都属于幂等成功。
		ids := make([]uint64, 0, target.BatchSize)
		if err := c.svcCtx.GormDB.WithContext(batchCtx).
			Raw(target.SelectIDsSQL, args...).
			Scan(&ids).Error; err != nil {
			cancel()
			return deleted, err
		}
		if len(ids) == 0 {
			cancel()
			break
		}

		result := c.svcCtx.GormDB.WithContext(batchCtx).
			Table(target.TableName).
			Where("id IN ?", ids).
			Delete(&cleanupRow{})
		cancel()
		if result.Error != nil {
			return deleted, result.Error
		}
		deleted += result.RowsAffected
		if len(ids) < target.BatchSize {
			break
		}
		if err := sleepWithContext(ctx, cleanupBatchInterval(c.svcCtx.Config.EventCleanup.BatchIntervalMs)); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func normalizeCleanupBatchSize(value int) int {
	if value <= 0 {
		return defaultCleanupBatchSize
	}
	if value > maxCleanupBatchSize {
		return maxCleanupBatchSize
	}
	return value
}

func normalizeMaxCleanupBatches(value int) int {
	if value <= 0 {
		return defaultMaxBatchesPerRun
	}
	if value > maxCleanupBatchesPerRun {
		return maxCleanupBatchesPerRun
	}
	return value
}

func cleanupPollInterval(seconds int) time.Duration {
	if seconds <= 0 {
		return defaultCleanupInterval
	}
	return time.Duration(seconds) * time.Second
}

func cleanupDeleteTimeout(milliseconds int) time.Duration {
	if milliseconds <= 0 {
		return defaultDeleteTimeout
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func cleanupBatchInterval(milliseconds int) time.Duration {
	if milliseconds <= 0 {
		return defaultBatchInterval
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func cleanupMaxRunDuration(seconds int) time.Duration {
	if seconds <= 0 {
		return defaultMaxRunDuration
	}
	return time.Duration(seconds) * time.Second
}

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func durationHoursOrDefault(hours int, fallback time.Duration) time.Duration {
	if hours <= 0 {
		return fallback
	}
	return time.Duration(hours) * time.Hour
}
