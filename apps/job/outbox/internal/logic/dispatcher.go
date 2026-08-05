package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	//读取配置里设置的轮询间隔，目前是1秒扫一次outbox_events表
	interval := time.Duration(d.svcCtx.Config.Outbox.PollIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = defaultOutboxPollInterval
	}

	//心跳发生器：每 1 秒钟通道里就多一个可读消息
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 调度循环不直接阻塞在 DispatchOnce 上；同时用 1 个令牌限制批次并发，避免堆积时无限创建 goroutine
	// 解决上一轮还没跑完，下一次心跳又到了的问题
	inFlight := make(chan struct{}, 1) //作为信号量，看看有没有dispatch在跑，有的话这一轮心跳就先跳过
	var wg sync.WaitGroup              //等待那一个运行的DispatchOnce goroutine完成
	startDispatch := func() {
		select {
		case inFlight <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done() //通知Run主循环完成了
				defer func() {
					<-inFlight //释放令牌
				}()

				if err := d.DispatchOnce(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
					// outbox 是后台常驻任务，单轮失败只记录日志，下一轮继续重试
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

// 进行 claim（认领）或 lease（租约），即从共享待办池里领一批任务，并把任务标记为'我来处理'
func (d *Dispatcher) claimDueOutboxEvents(ctx context.Context) ([]model.OutboxEvent, error) {
	limit := normalizeOutboxBatchSize(d.svcCtx.Config.Outbox.BatchSize) //限制每次处理的事件数量
	now := time.Now()                                                   //统一时间基准
	staleBefore := now.Add(-outboxClaimTimeout(d.svcCtx.Config.Outbox)) //如果一条 processing 事件的 locked_at 早于这个时刻，认为它已经过期了，可以重新认领
	token, err := randomHex(outboxLockTokenRandomBytes)                 //用于在后续回写时检验 事件的锁是哪个实例的 防止冲突
	if err != nil {
		return nil, err
	}

	// 一批事件共用本次 claim token；每次回写仍同时校验 id + token，
	// 所以不会把其它实例或超时重领后的处理结果覆盖掉。
	events := make([]model.OutboxEvent, 0, limit)
	err = d.svcCtx.GormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 在一个短事务中批量锁定并认领，避免旧实现为每条事件分别开启事务。
		// dueOutboxScope 同时保证同一 aggregate 只能认领最早的未完成事件：
		// 即使启用多个 dispatcher 实例或多个发布 worker，后续事件也必须等前序状态变为 sent 后才能进入下一轮，避免 create/delete、like/unlike 反序。
		// Kafka 发布仍在事务提交后执行，不会长期占用这些行锁。
		if err := tx.WithContext(ctx).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}). //给命中的行加X锁，防止其他实例同时修改，并且通过SKIP LOCKED修饰，表示遇到被别的事务锁住的行直接跳过不阻塞
			Scopes(dueOutboxScope(now, staleBefore)).                            //筛选可以被认领的事件，并且要注意同一个aggregate一次只能推进一条
			Order("id ASC").
			Limit(limit).
			Find(&events).Error; err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}

		ids := make([]uint64, 0, len(events))
		for _, event := range events {
			ids = append(ids, event.ID)
		}

		//对筛选出来的行进行处理
		result := tx.WithContext(ctx).
			Model(&model.OutboxEvent{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				// processing + lock_token 表示这批事件已被当前实例接管。
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
		if result.RowsAffected != int64(len(events)) {
			return fmt.Errorf("claim outbox batch affected %d rows, want %d", result.RowsAffected, len(events))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i := range events {
		events[i].Status = model.OutboxStatusProcessing
		events[i].LockToken = token
		events[i].LockedBy = d.instanceID
		events[i].LockedAt = &now
		events[i].LastError = ""
		events[i].SentAt = nil
		events[i].NextRetryAt = nil
	}
	return events, nil
}

// 把已经从 DB 认领到本实例的一批事件，用多个 goroutine 并发地投递到 Kafka
func (d *Dispatcher) dispatchClaimedEvents(ctx context.Context, events []model.OutboxEvent) error {
	workerCount := normalizeOutboxWorkerCount(d.svcCtx.Config.Outbox.WorkerCount, len(events))
	if workerCount == 0 {
		return nil
	}

	// 每次的整个 claim 批次中，同一 aggregate 最多只有一个事件，因此可以将不同aggregate 分成若干批并发发送。每个批次只调用一次 WriteMessages，避免每条事件都单独等待 Kafka BatchTimeout。
	batches := splitOutboxBatches(events, workerCount) //把events大致均分成workerCount个段，每个段里包含多个events
	jobs := make(chan []model.OutboxEvent)             //任务分发管道
	var wg sync.WaitGroup

	// 已经 claim 到本实例的事件可以并发投递；并发数受配置限制，避免瞬时打满 Kafka。
	for i := 0; i < len(batches); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := d.dispatchClaimedBatch(ctx, batch); err != nil {
					d.Errorf("dispatch outbox batch failed, size: %d, first_id: %d, error: %v", len(batch), batch[0].ID, err)
				}
			}
		}()
	}

	for _, batch := range batches {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- batch:
		}
	}
	close(jobs)
	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// 将单个batch向Kafka投递
func (d *Dispatcher) dispatchClaimedBatch(ctx context.Context, events []model.OutboxEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Kafka 投递在 DB 事务外执行；投递完成后再用 lock_token 批量回写状态。
	// 若 Kafka 已收到消息但 DB 回写失败，事件会再次投递，因此 consumer 仍必须使用 processed_events 保证 at-least-once 下的业务幂等。
	eventCtx, cancel := context.WithTimeout(ctx, outboxEventTimeout(d.svcCtx.Config.Outbox)) //为 Kafka 阶段单独开一个带超时的 ctx
	publishErr := d.publishEvents(eventCtx, events)
	cancel()

	markCtx, markCancel := context.WithTimeout(ctx, outboxEventTimeout(d.svcCtx.Config.Outbox)) //为回写DB阶段准备ctx
	defer markCancel()

	now := time.Now()
	if publishErr != nil {
		// kafka-go 的同步批量写在超时或部分失败时可能已经写入部分消息。
		// 这一个分片的整批进入重试会产生重复投递，但不会丢消息，consumer 幂等负责去重。
		var firstMarkErr error
		for _, event := range events {
			if err := d.markFailed(markCtx, event, publishErr, now); err != nil && firstMarkErr == nil {
				firstMarkErr = err
			}
		}
		if firstMarkErr != nil {
			return fmt.Errorf("publish outbox batch failed: %v; mark failed: %w", publishErr, firstMarkErr)
		}
		d.Errorf("publish outbox batch failed and scheduled retry, size: %d, first_id: %d, error: %v", len(events), events[0].ID, publishErr)
		return nil
	}

	return d.markSentBatch(markCtx, events, now)
}

func (d *Dispatcher) publishEvents(ctx context.Context, events []model.OutboxEvent) error {
	publishCtx, cancel := context.WithTimeout(ctx, outboxPublishTimeout(d.svcCtx.Config.Outbox))
	defer cancel()

	messages := make([]kafkax.Message, 0, len(events))
	for _, event := range events {
		messages = append(messages, kafkax.Message{
			Topic: event.Topic,
			Key:   []byte(event.AggregateID),
			Value: []byte(event.Payload),
			Headers: []kafkax.Header{
				{Key: "event_id", Value: []byte(event.EventID)},
				{Key: "event_type", Value: []byte(event.EventType)},
				{Key: "aggregate_type", Value: []byte(event.AggregateType)},
				{Key: "aggregate_id", Value: []byte(event.AggregateID)},
			},
		})
	}
	return d.svcCtx.Producer.PublishBatch(publishCtx, messages)
}

func (d *Dispatcher) markSentBatch(ctx context.Context, events []model.OutboxEvent, now time.Time) error {
	if len(events) == 0 {
		return nil
	}

	ids := make([]uint64, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ID)
	}

	// 同一 claim 批次共用 lock_token。批量回写减少每条消息一次 UPDATE，
	// 同时仍避免旧 worker 覆盖超时后被新实例重新认领的结果。
	result := d.svcCtx.GormDB.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Where("id IN ? AND status = ? AND lock_token = ?",
			ids,
			model.OutboxStatusProcessing,
			events[0].LockToken,
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
	if result.RowsAffected != int64(len(events)) {
		return fmt.Errorf(
			"mark sent affected %d rows, want %d, reason: %s, first_id: %d",
			result.RowsAffected,
			len(events),
			outboxClaimLostLogErrorLevel,
			events[0].ID,
		)
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
		).Where(
			// 同一聚合只允许最早的未完成事件被 claim。
			// idx_aggregate_status_id 让子查询只扫描四种未完成状态，避免遍历同一聚合已经发送的全部历史事件。
			// dead 事件也会阻塞后续事件，要求先人工补偿，不能静默越过并破坏顺序。
			`NOT EXISTS (
				SELECT 1
				FROM outbox_events AS predecessor
				WHERE predecessor.aggregate_type = outbox_events.aggregate_type
				  AND predecessor.aggregate_id = outbox_events.aggregate_id
				  AND predecessor.id < outbox_events.id
				  AND predecessor.status IN ?
			)`,
			[]int32{
				model.OutboxStatusPending,
				model.OutboxStatusFailed,
				model.OutboxStatusDead,
				model.OutboxStatusProcessing,
			},
		)
	}
}

func splitOutboxBatches(events []model.OutboxEvent, batchCount int) [][]model.OutboxEvent {
	if len(events) == 0 || batchCount <= 0 {
		return nil
	}
	if batchCount > len(events) {
		batchCount = len(events)
	}

	batchSize := (len(events) + batchCount - 1) / batchCount
	batches := make([][]model.OutboxEvent, 0, batchCount)
	for start := 0; start < len(events); start += batchSize {
		end := start + batchSize
		if end > len(events) {
			end = len(events)
		}
		batches = append(batches, events[start:end])
	}
	return batches
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
