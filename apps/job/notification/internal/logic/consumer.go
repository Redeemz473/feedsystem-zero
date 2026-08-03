package logic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"feedsystem-zero/apps/job/notification/internal/model"
	"feedsystem-zero/apps/job/notification/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
	"feedsystem-zero/common/notificationcache"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultNotificationBatchSize         = 100
	maxNotificationBatchSize             = 500
	defaultNotificationWorkerCount       = 4
	maxNotificationWorkerCount           = 16
	defaultNotificationFlushInterval     = time.Second
	defaultNotificationDBWriteTimeout    = 5 * time.Second
	defaultNotificationDBMaxRetries      = 3
	maxNotificationDBMaxRetries          = 8
	defaultNotificationDBRetryBase       = 20 * time.Millisecond
	defaultNotificationDBRetryMax        = 200 * time.Millisecond
	defaultNotificationProcessedEventTTL = 14 * 24 * time.Hour
	defaultNotificationFutureTolerance   = 5 * time.Minute
	notificationProcessChunkSize         = 100
	deadLetterInsertBatchSize            = 50
	maxDeadLetterReasonRunes             = 1024
	maxDeadLetterIdentityRunes           = 128
	maxDeadLetterEventTypeRunes          = 64
	maxDeadLetterPayloadBytes            = 1 << 20
)

type NotificationConsumer struct {
	svcCtx *svc.ServiceContext
	now    func() time.Time
}

type notificationMessageGroup struct {
	Key      string
	Messages []kafkax.Message
}

type decodedNotificationEvent struct {
	Message     kafkax.Message
	Envelope    eventx.Envelope
	Event       eventx.NotificationEvent
	BusinessKey string
	OccurredAt  time.Time
}

type kafkaHeaderRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewNotificationConsumer(svcCtx *svc.ServiceContext) *NotificationConsumer {
	return &NotificationConsumer{
		svcCtx: svcCtx,
		now:    time.Now,
	}
}

func (c *NotificationConsumer) Run(ctx context.Context) error {
	batchSize := c.svcCtx.Config.Notification.BatchSize
	if batchSize <= 0 {
		batchSize = c.svcCtx.Config.Kafka.BatchSize
	}
	batchSize = normalizeNotificationBatchSize(batchSize)

	flushInterval := time.Duration(c.svcCtx.Config.Notification.FlushMs) * time.Millisecond
	if flushInterval <= 0 {
		flushInterval = defaultNotificationFlushInterval
	}
	return c.svcCtx.Consumer.RunBatch(ctx, batchSize, flushInterval, c.HandleBatch)
}

// HandleBatch 按 topic+partition 分组。Kafka 只保证单分区有序，因此同一通知
// business_key 的创建和撤回在组内顺序执行，不同分区则并发提高吞吐。
func (c *NotificationConsumer) HandleBatch(ctx context.Context, messages []kafkax.Message) error {
	if len(messages) == 0 {
		return nil
	}

	groups := groupNotificationMessages(messages)
	workerCount := normalizeNotificationWorkerCount(c.svcCtx.Config.Notification.WorkerCount, len(groups))
	if workerCount == 0 {
		return nil
	}

	jobs := make(chan notificationMessageGroup)
	errCh := make(chan error, len(groups))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := c.handleMessageGroup(ctx, group); err != nil {
					errCh <- fmt.Errorf("处理通知消息组失败, group:%s size:%d: %w", group.Key, len(group.Messages), err)
				}
			}
		}()
	}

	for _, group := range groups {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- group:
		}
	}
	close(jobs)
	wg.Wait()
	close(errCh)

	var joined error
	for err := range errCh {
		joined = errors.Join(joined, err)
	}
	if joined != nil {
		return joined
	}
	logx.WithContext(ctx).Infof(
		"notification batch handled, messages:%d groups:%d workers:%d",
		len(messages),
		len(groups),
		workerCount,
	)
	return nil
}

func (c *NotificationConsumer) handleMessageGroup(ctx context.Context, group notificationMessageGroup) error {
	events := make([]decodedNotificationEvent, 0, len(group.Messages))
	deadLetters := make([]model.DeadLetterEvent, 0)
	for _, message := range group.Messages {
		decoded, envelope, err := c.decodeMessage(message)
		if err != nil {
			deadLetters = append(deadLetters, c.deadLetterFromMessage(message, envelope, err))
			continue
		}
		events = append(events, decoded)
	}

	if len(deadLetters) > 0 {
		// 永久性格式错误先进入死信再放行，避免一条坏消息一直阻塞整个 partition。
		if err := c.recordDeadLetters(ctx, deadLetters); err != nil {
			return fmt.Errorf("写入通知死信失败: %w", err)
		}
		logx.WithContext(ctx).Errorf(
			"notification decode dead letters recorded, group:%s count:%d",
			group.Key,
			len(deadLetters),
		)
	}
	return c.processEvents(ctx, events)
}

func (c *NotificationConsumer) decodeMessage(
	message kafkax.Message,
) (decodedNotificationEvent, eventx.Envelope, error) {
	envelope, err := decodeNotificationEnvelope(message.Value)
	if err != nil {
		return decodedNotificationEvent{}, envelope, err
	}
	if message.Topic != eventx.TopicNotificationEvents {
		return decodedNotificationEvent{}, envelope, fmt.Errorf("非预期的notification topic: %s", message.Topic)
	}
	if envelope.AggregateType != eventx.AggregateNotification {
		return decodedNotificationEvent{}, envelope, fmt.Errorf(
			"notification aggregate_type不匹配: %s",
			envelope.AggregateType,
		)
	}

	expectedAction, err := notificationActionFromEventType(envelope.EventType)
	if err != nil {
		return decodedNotificationEvent{}, envelope, err
	}

	var notificationEvent eventx.NotificationEvent
	if err := json.Unmarshal(envelope.Payload, &notificationEvent); err != nil {
		return decodedNotificationEvent{}, envelope, fmt.Errorf("解析notification payload失败: %w", err)
	}
	if notificationEvent.EventID != envelope.EventID {
		return decodedNotificationEvent{}, envelope, fmt.Errorf(
			"notification envelope.event_id与payload.event_id不一致, envelope:%s payload:%s",
			envelope.EventID,
			notificationEvent.EventID,
		)
	}
	if notificationEvent.Action != expectedAction {
		return decodedNotificationEvent{}, envelope, fmt.Errorf(
			"notification event_type与action不一致, event_type:%s action:%s",
			envelope.EventType,
			notificationEvent.Action,
		)
	}
	if err := eventx.ValidateNotificationEvent(notificationEvent); err != nil {
		return decodedNotificationEvent{}, envelope, err
	}
	if envelope.OccurredAt != notificationEvent.OccurredAt {
		return decodedNotificationEvent{}, envelope, fmt.Errorf(
			"notification envelope.occurred_at与payload.occurred_at不一致",
		)
	}

	businessKey, err := eventx.NotificationBusinessKey(notificationEvent)
	if err != nil {
		return decodedNotificationEvent{}, envelope, err
	}
	if envelope.AggregateID != businessKey {
		return decodedNotificationEvent{}, envelope, fmt.Errorf(
			"notification aggregate_id与business_key不一致, aggregate:%s business:%s",
			envelope.AggregateID,
			businessKey,
		)
	}

	occurredAtUTC := time.UnixMilli(notificationEvent.OccurredAt).UTC()
	if occurredAtUTC.After(c.currentTime().UTC().Add(c.futureTolerance())) {
		return decodedNotificationEvent{}, envelope, errors.New("notification occurred_at超出未来时间容差")
	}
	return decodedNotificationEvent{
		Message:     message,
		Envelope:    envelope,
		Event:       notificationEvent,
		BusinessKey: businessKey,
		// MySQL DATETIME 不保存时区，连接使用 loc=Local，因此落库前转为本地时区，
		// 后续读取再转 Unix 毫秒时才能保持原始时间点不变。
		OccurredAt: occurredAtUTC.In(time.Local),
	}, envelope, nil
}

func (c *NotificationConsumer) processEvents(ctx context.Context, events []decodedNotificationEvent) error {
	for start := 0; start < len(events); start += notificationProcessChunkSize {
		end := start + notificationProcessChunkSize
		if end > len(events) {
			end = len(events)
		}
		if err := c.processChunk(ctx, events[start:end]); err != nil {
			return fmt.Errorf("处理通知事件分片失败, range:%d-%d: %w", start, end, err)
		}
	}
	return nil
}

// processChunk 将 processed_events 幂等标记与通知状态写入同一个事务。
// 事务失败时 Kafka offset 不会提交；重试后也不会出现“已标记但通知未落库”的空洞。
//
// 事务提交成功后，对本批次内所有“真正影响未读数”的 receiver 触发 Redis version bump。
// 判定规则见 applyNotificationEvent 返回的 bumpReceiverID：
//   - 新建 unread / 已撤回 -> unread 复活 / unread -> canceled 才需要 bump
//   - 旧事件被跳过、canceled 事件针对已读记录 等情况都不 bump，避免无谓的缓存失效
//
// Redis 失败仅打日志、绝不回滚 MySQL：TTL 会兜底收敛不一致，未读数属于弱一致展示字段。
func (c *NotificationConsumer) processChunk(ctx context.Context, events []decodedNotificationEvent) error {
	if len(events) == 0 {
		return nil
	}

	dbCtx, cancel := context.WithTimeout(ctx, c.dbWriteTimeout())
	defer cancel()

	now := c.currentTime()
	expireAt := now.Add(c.processedEventTTL())

	// 多个 partition 会并发写 processed_events。InnoDB 在高并发插入同一索引末页时
	// 仍可能选择其中一个事务作为 1213 死锁牺牲者。事务已完整回滚，因此只对
	// 1213/1205 做有限重试；其他错误立即返回，避免掩盖数据或配置问题。
	var bumpReceivers map[uint64]struct{}
	maxRetries := c.dbMaxRetries()
	for attempt := 0; ; attempt++ {
		// 每次尝试使用独立集合。失败事务中收集的 receiver 不能在后续提交后 bump。
		attemptBumpReceivers := make(map[uint64]struct{})
		txErr := c.svcCtx.GormDB.WithContext(dbCtx).Transaction(func(tx *gorm.DB) error {
			for _, decoded := range events {
				inserted, err := insertNotificationProcessedEvent(tx, decoded, now, expireAt)
				if err != nil {
					return err
				}
				if !inserted {
					continue
				}
				bumpReceiverID, err := applyNotificationEvent(tx, decoded, now)
				if err != nil {
					return err
				}
				if bumpReceiverID != 0 {
					attemptBumpReceivers[bumpReceiverID] = struct{}{}
				}
			}
			return nil
		})
		if txErr == nil {
			bumpReceivers = attemptBumpReceivers
			break
		}
		if !isRetryableNotificationDBError(txErr) {
			return txErr
		}
		if attempt >= maxRetries {
			return fmt.Errorf("notification事务重试耗尽, retries:%d: %w", maxRetries, txErr)
		}

		retryNumber := attempt + 1
		delay := c.dbRetryDelay(retryNumber)
		logx.WithContext(ctx).Infof(
			"notification事务发生可重试锁冲突, retry:%d/%d delay:%s error:%v",
			retryNumber,
			maxRetries,
			delay,
			txErr,
		)
		if err := waitNotificationDBRetry(dbCtx, delay); err != nil {
			return err
		}
	}

	// 事务已经提交，MySQL 状态是最终值；即便 Redis 全部失败，下一次读走 MySQL COUNT 也仍然正确。
	for receiverID := range bumpReceivers {
		notificationcache.BumpUnreadVersion(ctx, c.svcCtx.RedisCli, receiverID)
	}
	return nil
}

func insertNotificationProcessedEvent(
	tx *gorm.DB,
	decoded decodedNotificationEvent,
	now time.Time,
	expireAt time.Time,
) (bool, error) {
	record := &model.ProcessedEvent{
		EventID:      decoded.Event.EventID,
		ConsumerName: eventx.ConsumerNotification,
		Topic:        decoded.Message.Topic,
		PartitionNo:  int32(decoded.Message.Partition),
		OffsetNo:     decoded.Message.Offset,
		ProcessedAt:  now,
		ExpireAt:     &expireAt,
	}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(record)
	if result.Error != nil {
		return false, fmt.Errorf("插入notification processed_event失败: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

// applyNotificationEvent 把一条已解码的事件落到 notifications 表，并返回是否需要
// bump 该 receiver 的未读数 version（用于事务提交后失效 Redis 缓存）。
// bumpReceiverID == 0 表示本次不需要 bump；具体判定见函数内注释。
func applyNotificationEvent(
	tx *gorm.DB,
	decoded decodedNotificationEvent,
	now time.Time,
) (bumpReceiverID uint64, err error) {
	var current model.Notification
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("business_key = ?", decoded.BusinessKey).
		Take(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		record := notificationRecordFromEvent(decoded, now)
		if err := tx.Create(&record).Error; err != nil {
			return 0, err
		}
		// 全新记录：create 事件直接落 unread（未读数 +1，需 bump）；
		// delete 事件用于补偿旧撤回，落 canceled，从未存在于未读集合，不 bump。
		if decoded.Event.Action == eventx.NotificationActionCreate {
			return decoded.Event.ReceiverID, nil
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// 手工重放或跨集群补偿可能让旧事件晚到。旧事件只记 processed_events，
	// 不能覆盖业务键上已经应用的更新状态，也不改变未读集合，因此不 bump。
	if decoded.OccurredAt.Before(current.OccurredAt) {
		return 0, nil
	}

	// 记录变更前是否处于“未读”集合，以便与变更后对比——只有翻转才是真正影响未读数的事件。
	wasUnread := current.Status == model.NotificationStatusUnread && current.DeletedAt == nil

	updates := map[string]any{
		"source_event_id":   decoded.Event.SourceEventID,
		"receiver_id":       decoded.Event.ReceiverID,
		"actor_id":          decoded.Event.ActorID,
		"notification_type": decoded.Event.NotificationType,
		"video_id":          nullableUint64(decoded.Event.VideoID),
		"comment_id":        nullableUint64(decoded.Event.CommentID),
		"occurred_at":       decoded.OccurredAt,
		"updated_at":        now,
	}
	var willBeUnread bool
	if decoded.Event.Action == eventx.NotificationActionCreate {
		updates["status"] = model.NotificationStatusUnread
		updates["read_at"] = nil
		updates["deleted_at"] = nil
		willBeUnread = true
	} else {
		updates["status"] = model.NotificationStatusCanceled
		updates["read_at"] = nil
		updates["deleted_at"] = decoded.OccurredAt
		willBeUnread = false
	}
	if err := tx.Model(&model.Notification{}).
		Where("id = ?", current.ID).
		Updates(updates).Error; err != nil {
		return 0, err
	}

	// unread 集合是否发生翻转：
	//   - 复活已撤回通知（false -> true）：未读数 +1，bump
	//   - 撤回一条未读通知（true -> false）：未读数 -1，bump
	//   - 撤回已读通知（false -> false）：未读集合不变，不 bump
	//   - 幂等重放同一 create（true -> true）：未读集合不变，不 bump
	//
	// receiver_id 使用变更后的值：正常场景两者一致；理论上事件重定向到不同 receiver 时，
	// 只对最终生效的接收者失效缓存即可（旧 receiver 的未读集合本来就没算这条）。
	if wasUnread != willBeUnread {
		return decoded.Event.ReceiverID, nil
	}
	return 0, nil
}

func notificationRecordFromEvent(decoded decodedNotificationEvent, now time.Time) model.Notification {
	record := model.Notification{
		BusinessKey:      decoded.BusinessKey,
		SourceEventID:    decoded.Event.SourceEventID,
		ReceiverID:       decoded.Event.ReceiverID,
		ActorID:          decoded.Event.ActorID,
		NotificationType: decoded.Event.NotificationType,
		VideoID:          nullableUint64(decoded.Event.VideoID),
		CommentID:        nullableUint64(decoded.Event.CommentID),
		Status:           model.NotificationStatusUnread,
		OccurredAt:       decoded.OccurredAt,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if decoded.Event.Action == eventx.NotificationActionDelete {
		record.Status = model.NotificationStatusCanceled
		record.DeletedAt = &decoded.OccurredAt
	}
	return record
}

func nullableUint64(value uint64) *uint64 {
	if value == 0 {
		return nil
	}
	return &value
}

func (c *NotificationConsumer) recordDeadLetters(ctx context.Context, letters []model.DeadLetterEvent) error {
	if len(letters) == 0 {
		return nil
	}
	dbCtx, cancel := context.WithTimeout(ctx, c.dbWriteTimeout())
	defer cancel()

	return c.svcCtx.GormDB.WithContext(dbCtx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "consumer_name"},
			{Name: "topic"},
			{Name: "partition_no"},
			{Name: "offset_no"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"event_id":       gormValuesColumn("event_id"),
			"event_type":     gormValuesColumn("event_type"),
			"aggregate_type": gormValuesColumn("aggregate_type"),
			"aggregate_id":   gormValuesColumn("aggregate_id"),
			"reason":         gormValuesColumn("reason"),
			"payload":        gormValuesColumn("payload"),
			"headers":        gormValuesColumn("headers"),
			"updated_at":     gormValuesColumn("updated_at"),
		}),
	}).CreateInBatches(&letters, deadLetterInsertBatchSize).Error
}

func (c *NotificationConsumer) deadLetterFromMessage(
	message kafkax.Message,
	envelope eventx.Envelope,
	cause error,
) model.DeadLetterEvent {
	now := c.currentTime()
	return model.DeadLetterEvent{
		ConsumerName:  eventx.ConsumerNotification,
		Topic:         truncateRunes(message.Topic, maxDeadLetterIdentityRunes),
		PartitionNo:   int32(message.Partition),
		OffsetNo:      message.Offset,
		EventID:       truncateRunes(envelope.EventID, maxDeadLetterIdentityRunes),
		EventType:     truncateRunes(envelope.EventType, maxDeadLetterEventTypeRunes),
		AggregateType: truncateRunes(envelope.AggregateType, maxDeadLetterEventTypeRunes),
		AggregateID:   truncateRunes(envelope.AggregateID, maxDeadLetterIdentityRunes),
		Reason:        truncateRunes(cause.Error(), maxDeadLetterReasonRunes),
		Payload:       deadLetterPayload(message.Value),
		Headers:       marshalKafkaHeaders(message.Headers),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func decodeNotificationEnvelope(value []byte) (eventx.Envelope, error) {
	if len(value) == 0 {
		return eventx.Envelope{}, errors.New("notification Kafka消息体不能为空")
	}
	var envelope eventx.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return eventx.Envelope{}, fmt.Errorf("解析notification envelope失败: %w", err)
	}
	envelope.EventID = strings.TrimSpace(envelope.EventID)
	envelope.EventType = strings.TrimSpace(envelope.EventType)
	envelope.AggregateType = strings.TrimSpace(envelope.AggregateType)
	envelope.AggregateID = strings.TrimSpace(envelope.AggregateID)
	envelope.Producer = strings.TrimSpace(envelope.Producer)
	if envelope.EventID == "" {
		return envelope, errors.New("notification envelope.event_id不能为空")
	}
	if envelope.EventType == "" {
		return envelope, errors.New("notification envelope.event_type不能为空")
	}
	if envelope.AggregateType == "" {
		return envelope, errors.New("notification envelope.aggregate_type不能为空")
	}
	if envelope.AggregateID == "" {
		return envelope, errors.New("notification envelope.aggregate_id不能为空")
	}
	if envelope.Producer == "" {
		return envelope, errors.New("notification envelope.producer不能为空")
	}
	if envelope.OccurredAt <= 0 {
		return envelope, errors.New("notification envelope.occurred_at不能为空")
	}
	if len(envelope.Payload) == 0 || string(envelope.Payload) == "null" {
		return envelope, errors.New("notification envelope.payload不能为空")
	}
	return envelope, nil
}

func notificationActionFromEventType(eventType string) (string, error) {
	switch eventType {
	case eventx.EventTypeNotificationCreate:
		return eventx.NotificationActionCreate, nil
	case eventx.EventTypeNotificationDelete:
		return eventx.NotificationActionDelete, nil
	default:
		return "", fmt.Errorf("不支持的notification event_type: %s", eventType)
	}
}

func groupNotificationMessages(messages []kafkax.Message) []notificationMessageGroup {
	indexes := make(map[string]int)
	groups := make([]notificationMessageGroup, 0)
	for _, message := range messages {
		key := fmt.Sprintf("%s:%d", message.Topic, message.Partition)
		index, ok := indexes[key]
		if !ok {
			indexes[key] = len(groups)
			groups = append(groups, notificationMessageGroup{Key: key})
			index = len(groups) - 1
		}
		groups[index].Messages = append(groups[index].Messages, message)
	}
	return groups
}

func normalizeNotificationBatchSize(size int) int {
	if size <= 0 {
		return defaultNotificationBatchSize
	}
	if size > maxNotificationBatchSize {
		return maxNotificationBatchSize
	}
	return size
}

func normalizeNotificationWorkerCount(workerCount int, groupCount int) int {
	if groupCount <= 0 {
		return 0
	}
	if workerCount <= 0 {
		workerCount = defaultNotificationWorkerCount
	}
	if workerCount > maxNotificationWorkerCount {
		workerCount = maxNotificationWorkerCount
	}
	if workerCount > groupCount {
		return groupCount
	}
	return workerCount
}

func (c *NotificationConsumer) currentTime() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

func (c *NotificationConsumer) dbWriteTimeout() time.Duration {
	ms := c.svcCtx.Config.Notification.DBWriteTimeoutMs
	if ms <= 0 {
		return defaultNotificationDBWriteTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

func (c *NotificationConsumer) dbMaxRetries() int {
	retries := c.svcCtx.Config.Notification.DBMaxRetries
	if retries <= 0 {
		return defaultNotificationDBMaxRetries
	}
	if retries > maxNotificationDBMaxRetries {
		return maxNotificationDBMaxRetries
	}
	return retries
}

func (c *NotificationConsumer) dbRetryDelay(retryNumber int) time.Duration {
	base := time.Duration(c.svcCtx.Config.Notification.DBRetryBaseMs) * time.Millisecond
	if base <= 0 {
		base = defaultNotificationDBRetryBase
	}
	maximum := time.Duration(c.svcCtx.Config.Notification.DBRetryMaxMs) * time.Millisecond
	if maximum <= 0 {
		maximum = defaultNotificationDBRetryMax
	}
	if maximum < base {
		maximum = base
	}

	delay := base
	for i := 1; i < retryNumber && delay < maximum; i++ {
		if delay >= maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}

	// 添加最多 50% 抖动，让同时被回滚的 partition 不会再次同步争锁。
	jitterWindow := delay / 2
	if jitterWindow <= 0 || delay >= maximum {
		return delay
	}
	jitter := time.Duration(c.currentTime().UnixNano() % int64(jitterWindow+1))
	if delay+jitter > maximum {
		return maximum
	}
	return delay + jitter
}

func isRetryableNotificationDBError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1213 || mysqlErr.Number == 1205
}

func waitNotificationDBRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *NotificationConsumer) processedEventTTL() time.Duration {
	days := c.svcCtx.Config.Notification.ProcessedEventTTLDays
	if days <= 0 {
		return defaultNotificationProcessedEventTTL
	}
	return time.Duration(days) * 24 * time.Hour
}

func (c *NotificationConsumer) futureTolerance() time.Duration {
	seconds := c.svcCtx.Config.Notification.FutureToleranceSeconds
	if seconds <= 0 {
		return defaultNotificationFutureTolerance
	}
	return time.Duration(seconds) * time.Second
}

func marshalKafkaHeaders(headers []kafkax.Header) string {
	if len(headers) == 0 {
		return "[]"
	}
	records := make([]kafkaHeaderRecord, 0, len(headers))
	for _, header := range headers {
		records = append(records, kafkaHeaderRecord{Key: header.Key, Value: string(header.Value)})
	}
	data, err := json.Marshal(records)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func deadLetterPayload(value []byte) string {
	truncated := len(value) > maxDeadLetterPayloadBytes
	if truncated {
		value = value[:maxDeadLetterPayloadBytes]
	}
	var payload string
	if utf8.Valid(value) {
		payload = string(value)
	} else {
		payload = "base64:" + base64.StdEncoding.EncodeToString(value)
	}
	if truncated {
		return "[truncated]\n" + payload
	}
	return payload
}

func truncateRunes(value string, maxRunes int) string {
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes])
}

func gormValuesColumn(name string) clause.Expr {
	return clause.Expr{SQL: "VALUES(" + name + ")"}
}
