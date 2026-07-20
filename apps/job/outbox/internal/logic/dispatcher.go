package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"feedsystem-zero/apps/job/outbox/internal/config"
	"feedsystem-zero/apps/job/outbox/internal/svc"
	"feedsystem-zero/common/kafkax"
	"feedsystem-zero/model"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultOutboxBatchSize       = 100
	maxOutboxBatchSize           = 500
	defaultOutboxWorkerCount     = 4
	maxOutboxWorkerCount         = 32
	defaultOutboxPollInterval    = time.Second
	defaultOutboxPublishTimeout  = 5 * time.Second
	defaultOutboxEventTimeout    = 10 * time.Second
	defaultOutboxClaimTimeout    = time.Minute
	maxOutboxLastErrorRunes      = 1024
	outboxLockTokenRandomBytes   = 16
	outboxInstanceIDRandomBytes  = 6
	outboxClaimLostLogErrorLevel = "claim_lost"
)

type Dispatcher struct {
	svcCtx     *svc.ServiceContext
	instanceID string
	logx.Logger
}

func NewDispatcher(svcCtx *svc.ServiceContext) *Dispatcher {
	return &Dispatcher{
		svcCtx:     svcCtx,
		instanceID: newDispatcherInstanceID(),
		Logger:     logx.WithContext(context.Background()),
	}
}

// Run 是 outbox job 的主循环：
// 业务服务只负责在本地事务里写 outbox_events，真正投递 Kafka 由这里异步完成。
func (d *Dispatcher) Run(ctx context.Context) error {
	interval := time.Duration(d.svcCtx.Config.Outbox.PollIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = defaultOutboxPollInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 调度循环不直接阻塞在 DispatchOnce 上；同时用 1 个令牌限制批次并发，避免堆积时无限创建 goroutine。
	inFlight := make(chan struct{}, 1)
	var wg sync.WaitGroup
	startDispatch := func() {
		select {
		case inFlight <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					<-inFlight
				}()

				if err := d.DispatchOnce(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
					// outbox 是后台常驻任务，单轮失败只记录日志，下一轮继续重试。
					d.Errorf("dispatch outbox once failed: %v", err)
				}
			}()
		default:
			d.Errorf("skip outbox dispatch tick: previous dispatch is still running")
		}
	}

	startDispatch()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			startDispatch()
		}
	}
}

func (d *Dispatcher) DispatchOnce(ctx context.Context) error {
	// 一轮调度分两段：
	// 1. 先从 DB claim 一批待投递事件，拿到本实例的 lock_token；
	// 2. 再把已 claim 的事件并发投递 Kafka。
	events, err := d.claimDueOutboxEvents(ctx)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	return d.dispatchClaimedEvents(ctx, events)
}

func (d *Dispatcher) claimDueOutboxEvents(ctx context.Context) ([]model.OutboxEvent, error) {
	limit := normalizeOutboxBatchSize(d.svcCtx.Config.Outbox.BatchSize)
	now := time.Now()
	staleBefore := now.Add(-outboxClaimTimeout(d.svcCtx.Config.Outbox))

	// 先只查 id，控制单轮扫描量；真正抢占在 claimOneOutboxEvent 中完成。
	// 多个 outbox job 实例同时运行时，靠行锁和 SKIP LOCKED 分摊事件。
	ids := make([]uint64, 0, limit)
	err := d.svcCtx.GormDB.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Select("id").
		Scopes(dueOutboxScope(now, staleBefore)).
		Order("id ASC").
		Limit(limit).
		Pluck("id", &ids).Error
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	events := make([]model.OutboxEvent, 0, len(ids))
	var firstErr error
	for _, id := range ids {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		event, ok, err := d.claimOneOutboxEvent(ctx, id, staleBefore)
		if err != nil {
			d.Errorf("claim outbox event failed, id: %d, error: %v", id, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if ok {
			events = append(events, event)
		}
	}

	if len(events) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return events, nil
}

func (d *Dispatcher) claimOneOutboxEvent(ctx context.Context, id uint64, staleBefore time.Time) (model.OutboxEvent, bool, error) {
	token, err := randomHex(outboxLockTokenRandomBytes)
	if err != nil {
		return model.OutboxEvent{}, false, err
	}

	now := time.Now()
	var event model.OutboxEvent
	claimed := false

	err = d.svcCtx.GormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 只在短事务里做 claim，不在事务里发 Kafka。
		// 发 Kafka 可能慢或超时，如果放在 DB 事务里会长期占用行锁。
		err := tx.WithContext(ctx).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Scopes(dueOutboxScope(now, staleBefore)).
			Where("id = ?", id).
			First(&event).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}

		result := tx.WithContext(ctx).
			Model(&model.OutboxEvent{}).
			Where("id = ?", event.ID).
			Updates(map[string]any{
				// processing + lock_token 表示这条事件已被当前实例接管。
				// 如果进程崩溃，dueOutboxScope 会在 locked_at 超时后把它重新捞出来。
				"status":        model.OutboxStatusProcessing,
				"lock_token":    token,
				"locked_by":     d.instanceID,
				"locked_at":     now,
				"updated_at":    now,
				"last_error":    "",
				"sent_at":       nil,
				"next_retry_at": nil,
			})
		if result.Error != nil {
			return result.Error
		}
		claimed = result.RowsAffected > 0
		return nil
	})
	if err != nil {
		return model.OutboxEvent{}, false, err
	}
	if !claimed {
		return model.OutboxEvent{}, false, nil
	}

	event.Status = model.OutboxStatusProcessing
	event.LockToken = token
	event.LockedBy = d.instanceID
	event.LockedAt = &now
	event.LastError = ""
	event.SentAt = nil
	event.NextRetryAt = nil
	return event, true, nil
}

func (d *Dispatcher) dispatchClaimedEvents(ctx context.Context, events []model.OutboxEvent) error {
	workerCount := normalizeOutboxWorkerCount(d.svcCtx.Config.Outbox.WorkerCount, len(events))
	if workerCount == 0 {
		return nil
	}

	jobs := make(chan model.OutboxEvent)
	var wg sync.WaitGroup

	// 已经 claim 到本实例的事件可以并发投递；并发数受配置限制，避免瞬时打满 Kafka。
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for event := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := d.dispatchClaimedEvent(ctx, event); err != nil {
					// 单条失败不阻断整批，失败信息会写回 outbox_events，后续按 next_retry_at 重试。
					d.Errorf("dispatch outbox event failed, id: %d, event_id: %s, error: %v", event.ID, event.EventID, err)
				}
			}
		}()
	}

	for _, event := range events {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- event:
		}
	}
	close(jobs)
	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (d *Dispatcher) dispatchClaimedEvent(ctx context.Context, event model.OutboxEvent) error {
	// Kafka 投递在 DB 事务外执行；投递完成后再用 lock_token 回写状态。
	// 因此这个 job 是 at-least-once 语义，consumer 侧必须用 processed_events 做幂等。
	eventCtx, cancel := context.WithTimeout(ctx, outboxEventTimeout(d.svcCtx.Config.Outbox))
	publishErr := d.publishEvent(eventCtx, event)
	cancel()

	markCtx, markCancel := context.WithTimeout(ctx, outboxEventTimeout(d.svcCtx.Config.Outbox))
	defer markCancel()

	now := time.Now()
	if publishErr != nil {
		if err := d.markFailed(markCtx, event, publishErr, now); err != nil {
			return fmt.Errorf("publish outbox event failed: %v; mark failed: %w", publishErr, err)
		}
		return nil
	}

	return d.markSent(markCtx, event, now)
}

func (d *Dispatcher) publishEvent(ctx context.Context, event model.OutboxEvent) error {
	publishCtx, cancel := context.WithTimeout(ctx, outboxPublishTimeout(d.svcCtx.Config.Outbox))
	defer cancel()

	return d.svcCtx.Producer.Publish(publishCtx,
		event.Topic,
		[]byte(event.AggregateID),
		[]byte(event.Payload),
		kafkax.Header{Key: "event_id", Value: []byte(event.EventID)},
		kafkax.Header{Key: "event_type", Value: []byte(event.EventType)},
		kafkax.Header{Key: "aggregate_type", Value: []byte(event.AggregateType)},
		kafkax.Header{Key: "aggregate_id", Value: []byte(event.AggregateID)},
	)
}

func (d *Dispatcher) markSent(ctx context.Context, event model.OutboxEvent, now time.Time) error {
	// 必须带上 lock_token 更新，避免旧 worker 超时后又回写，覆盖新 worker 的处理结果。
	result := d.svcCtx.GormDB.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ? AND status = ? AND lock_token = ?",
			event.ID,
			model.OutboxStatusProcessing,
			event.LockToken,
		).
		Updates(map[string]any{
			"status":        model.OutboxStatusSent,
			"sent_at":       now,
			"last_error":    "",
			"next_retry_at": nil,
			"lock_token":    "",
			"locked_by":     "",
			"locked_at":     nil,
			"updated_at":    now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		d.Errorf("mark sent skipped, reason: %s, id: %d, event_id: %s", outboxClaimLostLogErrorLevel, event.ID, event.EventID)
	}
	return nil
}

func (d *Dispatcher) markFailed(ctx context.Context, event model.OutboxEvent, cause error, now time.Time) error {
	nextRetryCount := event.RetryCount + 1
	nextStatus := model.OutboxStatusFailed
	var nextRetryAt any = now.Add(nextRetryDelay(d.svcCtx.Config.Outbox, nextRetryCount))
	if d.svcCtx.Config.Outbox.MaxRetry > 0 && int(nextRetryCount) >= d.svcCtx.Config.Outbox.MaxRetry {
		// 超过最大重试后进入 dead 状态，避免坏事件无限占用调度资源。
		nextStatus = model.OutboxStatusDead
		nextRetryAt = nil
	}

	result := d.svcCtx.GormDB.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id = ? AND status = ? AND lock_token = ?",
			event.ID,
			model.OutboxStatusProcessing,
			event.LockToken,
		).
		Updates(map[string]any{
			"status":        nextStatus,
			"retry_count":   gorm.Expr("retry_count + 1"),
			"next_retry_at": nextRetryAt,
			"last_error":    truncateLastError(cause.Error()),
			"lock_token":    "",
			"locked_by":     "",
			"locked_at":     nil,
			"updated_at":    now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		d.Errorf("mark failed skipped, reason: %s, id: %d, event_id: %s", outboxClaimLostLogErrorLevel, event.ID, event.EventID)
	}
	return nil
}

func dueOutboxScope(now, staleBefore time.Time) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		// 可投递事件包括：
		// 1. pending/failed 且到达 next_retry_at；
		// 2. processing 但 locked_at 超时，说明上一个 worker 可能崩溃或卡死。
		return db.Where(
			"(status IN ? AND (next_retry_at IS NULL OR next_retry_at <= ?)) OR "+
				"(status = ? AND locked_at IS NOT NULL AND locked_at <= ?)",
			[]int32{model.OutboxStatusPending, model.OutboxStatusFailed},
			now,
			model.OutboxStatusProcessing,
			staleBefore,
		)
	}
}

func nextRetryDelay(conf config.OutboxConf, retryCount int32) time.Duration {
	base := time.Duration(conf.RetryBaseMs) * time.Millisecond
	if base <= 0 {
		base = time.Second
	}

	maxDelay := time.Duration(conf.RetryMaxMs) * time.Millisecond
	if maxDelay <= 0 {
		maxDelay = time.Minute
	}

	if conf.MaxRetry > 0 && int(retryCount) >= conf.MaxRetry {
		return maxDelay
	}

	delay := base
	for i := int32(1); i < retryCount; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

// 单次拉取条数，限制 0~500
func normalizeOutboxBatchSize(size int) int {
	if size <= 0 {
		return defaultOutboxBatchSize
	}
	if size > maxOutboxBatchSize {
		return maxOutboxBatchSize
	}
	return size
}

// 读取配置并发 worker；上限 32，下限 4；
// worker 数量不能超过当前批次消息总数，避免空协程
func normalizeOutboxWorkerCount(workerCount, eventCount int) int {
	if eventCount <= 0 {
		return 0
	}
	if workerCount <= 0 {
		workerCount = defaultOutboxWorkerCount
	}
	if workerCount > maxOutboxWorkerCount {
		workerCount = maxOutboxWorkerCount
	}
	if workerCount > eventCount {
		return eventCount
	}
	return workerCount
}

func outboxEventTimeout(conf config.OutboxConf) time.Duration {
	timeout := time.Duration(conf.EventTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		return defaultOutboxEventTimeout
	}
	return timeout
}

func outboxPublishTimeout(conf config.OutboxConf) time.Duration {
	timeout := time.Duration(conf.PublishTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		return defaultOutboxPublishTimeout
	}
	return timeout
}

func outboxClaimTimeout(conf config.OutboxConf) time.Duration {
	timeout := time.Duration(conf.ClaimTimeoutSeconds) * time.Second
	if timeout <= 0 {
		return defaultOutboxClaimTimeout
	}
	return timeout
}

func truncateLastError(message string) string {
	runes := []rune(message)
	if len(runes) <= maxOutboxLastErrorRunes {
		return message
	}
	return string(runes[:maxOutboxLastErrorRunes])
}

func newDispatcherInstanceID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "outbox"
	}

	suffix, err := randomHex(outboxInstanceIDRandomBytes)
	if err != nil {
		suffix = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s", hostname, suffix)
}

func randomHex(byteLen int) (string, error) {
	buf := make([]byte, byteLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
