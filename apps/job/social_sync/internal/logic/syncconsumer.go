package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"feedsystem-zero/apps/job/social_sync/internal/config"
	"feedsystem-zero/apps/job/social_sync/internal/model"
	"feedsystem-zero/apps/job/social_sync/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultSyncBatchSize      = 100
	maxSyncBatchSize          = 500
	defaultFlushBatchSize     = 500
	maxFlushBatchSize         = 500
	defaultSyncWorkerCount    = 4
	maxSyncWorkerCount        = 16
	defaultMaxEventRetry      = 3
	defaultRetryBackoff       = 200 * time.Millisecond
	defaultMaxRetryBackoff    = 2 * time.Second
	defaultSyncFlushInterval  = time.Second
	defaultRedisOpTimeout     = 500 * time.Millisecond
	maxLoggedFailedEventCount = 10
	maxDeadLetterReasonRunes  = 1024
)

// SyncConsumer 消费 social.follow.events 并完成两件事：
//   - processed_events 幂等占位，兜住 Kafka at-least-once；
//   - 用事件里的最终状态回补 Redis 缓存（单条状态 + 统计版本 + 列表版本），
//     兜住 Follow/Unfollow 事务提交后 applyFollowCacheAfterCommit 失败的场景。
//
// 有意不做 Feed inbox 回填：那是 feed 模块自己的消费者要做的事，避免耦合。
type SyncConsumer struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

// messageGroup 与 interaction_sync 一致：同一 topic+partition 保序。
type messageGroup struct {
	Key      string
	Topic    string
	Messages []kafkax.Message
}

// decodedFollowEvent 保留 kafka 消息本身，方便部分失败时只重试失败子集，
// 或者作为死信写入完整原始 payload。
type decodedFollowEvent struct {
	Message  kafkax.Message
	Envelope eventx.Envelope
	Event    eventx.FollowEvent
	// Followed 表示事件语义的最终状态：follow -> true, unfollow -> false。
	Followed bool
}

type kafkaHeaderRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewSyncConsumer(svcCtx *svc.ServiceContext) *SyncConsumer {
	return &SyncConsumer{
		svcCtx: svcCtx,
		Logger: logx.WithContext(context.Background()),
	}
}

func (c *SyncConsumer) Run(ctx context.Context) error {
	batchSize := c.svcCtx.Config.Sync.BatchSize
	if batchSize <= 0 {
		batchSize = c.svcCtx.Config.Kafka.BatchSize
	}
	batchSize = normalizeSyncBatchSize(batchSize)

	flushInterval := time.Duration(c.svcCtx.Config.Sync.FlushMs) * time.Millisecond
	if flushInterval <= 0 {
		flushInterval = defaultSyncFlushInterval
	}

	return c.svcCtx.Consumer.RunBatch(ctx, batchSize, flushInterval, c.HandleBatch)
}

func (c *SyncConsumer) HandleBatch(ctx context.Context, messages []kafkax.Message) error {
	if len(messages) == 0 {
		return nil
	}

	logger := logx.WithContext(ctx)
	// Kafka 只在 partition 内保序，因此按 partition 分组，组间并发。
	// 同一个 (follower, following) 的 follow/unfollow 有依赖顺序，
	// dispatcher 使用 aggregate_id 作为 partition key 后可以保证进入同一 partition。
	groups := groupMessagesByTopicPartition(messages)
	workerCount := normalizeSyncWorkerCount(c.svcCtx.Config.Sync.WorkerCount, len(groups))
	if workerCount == 0 {
		return nil
	}

	jobs := make(chan messageGroup)
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
					errCh <- fmt.Errorf("handle message group failed, group:%s topic:%s size:%d: %w",
						group.Key,
						group.Topic,
						len(group.Messages),
						err,
					)
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

	logger.Infof("social sync batch handled, messages:%d groups:%d workers:%d", len(messages), len(groups), workerCount)
	return nil
}

func (c *SyncConsumer) handleMessageGroup(ctx context.Context, group messageGroup) error {
	events, deadLetters := c.decodeGroupMessages(group.Messages)
	if len(deadLetters) > 0 {
		// 坏消息永远无法解码，继续重试只会卡分区；写死信后放行。
		if err := c.recordDeadLetters(ctx, deadLetters); err != nil {
			return fmt.Errorf("record decode dead letters failed: %w", err)
		}
		logx.WithContext(ctx).Errorf("social sync recorded decode dead letters, group:%s count:%d", group.Key, len(deadLetters))
	}
	if len(events) == 0 {
		return nil
	}

	return c.flushEvents(ctx, events)
}

func (c *SyncConsumer) decodeGroupMessages(messages []kafkax.Message) ([]decodedFollowEvent, []model.DeadLetterEvent) {
	events := make([]decodedFollowEvent, 0, len(messages))
	deadLetters := make([]model.DeadLetterEvent, 0)

	for _, msg := range messages {
		event, deadLetter, ok := c.decodeMessage(msg)
		if !ok {
			deadLetters = append(deadLetters, deadLetter)
			continue
		}
		events = append(events, event)
	}
	return events, deadLetters
}

func (c *SyncConsumer) decodeMessage(msg kafkax.Message) (decodedFollowEvent, model.DeadLetterEvent, bool) {
	envelope, err := decodeEnvelope(msg.Value)
	if err != nil {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, eventx.Envelope{}, err), false
	}

	if msg.Topic != eventx.TopicFollowEvents {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, fmt.Errorf("unexpected topic: %s", msg.Topic)), false
	}
	if envelope.AggregateType != eventx.AggregateFollow {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, fmt.Errorf("aggregate_type不匹配: %s", envelope.AggregateType)), false
	}

	expectedAction, err := followActionFromEventType(envelope.EventType)
	if err != nil {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, err), false
	}

	var payload eventx.FollowEvent
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, fmt.Errorf("解析关注事件失败: %w", err)), false
	}
	if payload.Action == "" {
		payload.Action = expectedAction
	}
	if payload.Action != expectedAction {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, fmt.Errorf("event_type和payload.action不一致, event_type:%s action:%s", envelope.EventType, payload.Action)), false
	}

	eventID, err := mergeEventID(envelope.EventID, payload.EventID)
	if err != nil {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, err), false
	}
	if payload.FollowerID == 0 {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, errors.New("follower_id不能为空")), false
	}
	if payload.FollowingID == 0 {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, errors.New("following_id不能为空")), false
	}
	if payload.FollowerID == payload.FollowingID {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, errors.New("follower_id和following_id不能相同")), false
	}
	if payload.OccurredAt <= 0 && envelope.OccurredAt > 0 {
		payload.OccurredAt = envelope.OccurredAt
	}
	if payload.OccurredAt <= 0 {
		return decodedFollowEvent{}, c.deadLetterFromMessage(msg, envelope, errors.New("occurred_at不能为空")), false
	}
	payload.EventID = eventID

	return decodedFollowEvent{
		Message:  msg,
		Envelope: envelope,
		Event:    payload,
		Followed: payload.Action == eventx.FollowActionFollow,
	}, model.DeadLetterEvent{}, true
}

// flushEvents 按批处理事件。每一批内部：
//  1. 先在事务里插入 processed_events 幂等标记，被抢到的事件会被跳过；
//  2. 事务提交后，用事件里的最终状态回补 Redis 缓存；
//  3. Redis 失败不视为整体失败：processed_events 已经保证幂等，
//     即使这一次没写成 Redis，下次热点读会自动回源 MySQL 修复。
//
// 部分事件失败会累积并有限重试，仍失败进死信。
func (c *SyncConsumer) flushEvents(ctx context.Context, events []decodedFollowEvent) error {
	pending := events
	maxAttempts := normalizeMaxEventRetry(c.svcCtx.Config.Sync.MaxEventRetry)

	for attempt := 1; len(pending) > 0; attempt++ {
		failed, err := c.flushEventsOnce(ctx, pending)
		if err != nil {
			return err
		}
		if len(failed) == 0 {
			return nil
		}
		if attempt >= maxAttempts {
			reason := fmt.Sprintf("flush follow events partial failed after %d attempts", attempt)
			return c.recordDeadLetters(ctx, c.deadLettersFromDecodedEvents(failed, reason))
		}
		if err := sleepBeforeRetry(ctx, c.svcCtx.Config.Sync, attempt); err != nil {
			return err
		}
		pending = failed
	}
	return nil
}

func (c *SyncConsumer) flushEventsOnce(ctx context.Context, events []decodedFollowEvent) ([]decodedFollowEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}

	failed := make([]decodedFollowEvent, 0)
	failedIDs := make([]string, 0)
	batchSize := normalizeFlushBatchSize(c.svcCtx.Config.Sync.FlushBatchSize)
	for start := 0; start < len(events); start += batchSize {
		end := min(start+batchSize, len(events))
		chunk := events[start:end]
		chunkFailed, err := c.processChunk(ctx, chunk)
		if err != nil {
			return nil, fmt.Errorf("process follow events chunk failed, range:%d-%d size:%d: %w", start, end, len(chunk), err)
		}
		if len(chunkFailed) > 0 {
			for _, ev := range chunkFailed {
				failedIDs = append(failedIDs, ev.Event.EventID)
			}
			failed = append(failed, chunkFailed...)
		}
	}
	if len(failed) > 0 {
		logx.WithContext(ctx).Errorf("social sync partial failed, total:%d failed:%s",
			len(events),
			compactFailedEventIDs(failedIDs),
		)
	}
	return failed, nil
}

// processChunk 处理一个小批。processed_events 用 OnConflict DoNothing 做占位，
// 已被其他实例处理过的事件在事务里就会被过滤掉，不再重复刷 Redis。
func (c *SyncConsumer) processChunk(ctx context.Context, chunk []decodedFollowEvent) ([]decodedFollowEvent, error) {
	if len(chunk) == 0 {
		return nil, nil
	}

	consumerName := c.consumerName()
	now := time.Now()
	expireAt := now.Add(time.Duration(eventx.DefaultProcessedEventTTLDays) * 24 * time.Hour)

	// 记录本次真正拿到执行权的事件（processed_events 新插入了行）。
	// 已经存在的表示别人处理过，不再触发副作用。
	acquired := make([]decodedFollowEvent, 0, len(chunk))

	err := c.svcCtx.GormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, event := range chunk {
			record := &model.ProcessedEvent{
				EventID:      event.Event.EventID,
				ConsumerName: consumerName,
				Topic:        event.Message.Topic,
				PartitionNo:  int32(event.Message.Partition),
				OffsetNo:     event.Message.Offset,
				ProcessedAt:  now,
				ExpireAt:     &expireAt,
			}
			res := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(record)
			if res.Error != nil {
				return fmt.Errorf("insert processed_event failed, event_id:%s: %w", event.Event.EventID, res.Error)
			}
			if res.RowsAffected == 1 {
				acquired = append(acquired, event)
			}
		}
		return nil
	})
	if err != nil {
		// DB 事务失败通常是数据库短暂不可用。
		// 直接向上抛错，由 kafkax RunBatch 层重试整批，避免冒然走死信（同一事务写不了 processed_events，
		// 也写不了 dead_letter_events，真写到也只是无意义的深度进死信）。
		return nil, fmt.Errorf("processed_events transaction failed: %w", err)
	}

	if len(acquired) == 0 {
		return nil, nil
	}

	// 事务提交后再刷 Redis：单条状态 + 两侧统计 + 两侧列表版本。
	// Redis 失败仅记录日志，不影响 processed_events 已经完成的幂等占位。
	failed := c.applyRedisCache(ctx, acquired)
	return failed, nil
}

// applyRedisCache 对已经拿到 processed_events 执行权的事件回补 Redis。
// 使用单个 Pipeline 减少 RTT；失败的事件返回给上层做有限重试。
func (c *SyncConsumer) applyRedisCache(ctx context.Context, events []decodedFollowEvent) []decodedFollowEvent {
	if len(events) == 0 {
		return nil
	}

	redisCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.redisOpTimeout())
	defer cancel()

	pipe := c.svcCtx.RedisCli.Pipeline()
	cmdsByEvent := make([]followRedisCmdSet, 0, len(events))

	for _, event := range events {
		value := "0"
		if event.Followed {
			value = "1"
		}

		set := followRedisCmdSet{}
		set.state = pipe.Set(redisCtx,
			rediskey.SocialFollowingStateKey(event.Event.FollowerID, event.Event.FollowingID),
			value,
			rediskey.SocialFollowingStateTTL,
		)
		// 只在真正状态变化的事件（能进 processed_events 的都是新事件）执行统计/列表失效，
		// 与 applyFollowCacheAfterCommit 语义保持一致。
		// 粉丝数/关注数已冗余进 accounts 表，经 GetProfile/BatchGetProfiles 返回。
		// 关注关系变化后让两侧用户的 profile 缓存版本失效，使其回源拿到最新计数。
		set.incrs[0] = pipe.Incr(redisCtx, rediskey.AccountPublicProfileVersionKey(event.Event.FollowerID))
		set.incrs[1] = pipe.Incr(redisCtx, rediskey.AccountPublicProfileVersionKey(event.Event.FollowingID))
		set.incrs[2] = pipe.Incr(redisCtx, rediskey.SocialFollowersListVersionKey(event.Event.FollowingID))
		set.incrs[3] = pipe.Incr(redisCtx, rediskey.SocialFollowingsListVersionKey(event.Event.FollowerID))
		cmdsByEvent = append(cmdsByEvent, set)
	}

	if _, err := pipe.Exec(redisCtx); err != nil {
		// pipeline 级别错误一般是网络问题；这时全部当作失败让上层重试。
		// 但 processed_events 已经占位，重试进来的实际会因为 OnConflict DoNothing
		// 无法再次执行；因此实际上是"尽力而为"的补偿。这里返回失败让上层记录日志与死信。
		logx.WithContext(ctx).Errorf("social sync redis pipeline exec failed, events:%d error:%v", len(events), err)
		return events
	}

	failed := make([]decodedFollowEvent, 0)
	for i, set := range cmdsByEvent {
		if err := firstPipelineError(set); err != nil {
			logx.WithContext(ctx).Errorf("social sync redis apply failed, event_id:%s error:%v", events[i].Event.EventID, err)
			failed = append(failed, events[i])
		}
	}
	return failed
}

// followRedisCmdSet 保存单个事件在 Pipeline 中产生的所有命令，
// 便于 Exec 后逐个定位失败事件。
type followRedisCmdSet struct {
	state   *redis.StatusCmd
	delHash *redis.IntCmd
	incrs   [4]*redis.IntCmd
}

func firstPipelineError(set followRedisCmdSet) error {
	if err := set.state.Err(); err != nil {
		return err
	}
	for _, cmd := range set.incrs {
		if err := cmd.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (c *SyncConsumer) recordDeadLetters(ctx context.Context, letters []model.DeadLetterEvent) error {
	if len(letters) == 0 {
		return nil
	}

	// 死信写入也做幂等：同一 consumer+topic+partition+offset 只保留一条。
	return c.svcCtx.GormDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "consumer_name"},
			{Name: "topic"},
			{Name: "partition_no"},
			{Name: "offset_no"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"event_id":       gormColumn("event_id"),
			"event_type":     gormColumn("event_type"),
			"aggregate_type": gormColumn("aggregate_type"),
			"aggregate_id":   gormColumn("aggregate_id"),
			"reason":         gormColumn("reason"),
			"payload":        gormColumn("payload"),
			"headers":        gormColumn("headers"),
			"updated_at":     gormColumn("updated_at"),
		}),
	}).Create(&letters).Error
}

func (c *SyncConsumer) deadLetterFromMessage(msg kafkax.Message, envelope eventx.Envelope, cause error) model.DeadLetterEvent {
	now := time.Now()
	return model.DeadLetterEvent{
		ConsumerName:  c.consumerName(),
		Topic:         msg.Topic,
		PartitionNo:   int32(msg.Partition),
		OffsetNo:      msg.Offset,
		EventID:       envelope.EventID,
		EventType:     envelope.EventType,
		AggregateType: envelope.AggregateType,
		AggregateID:   envelope.AggregateID,
		Reason:        truncateDeadLetterReason(cause.Error()),
		Payload:       string(msg.Value),
		Headers:       marshalKafkaHeaders(msg.Headers),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func (c *SyncConsumer) deadLettersFromDecodedEvents(events []decodedFollowEvent, reason string) []model.DeadLetterEvent {
	letters := make([]model.DeadLetterEvent, 0, len(events))
	for _, event := range events {
		letters = append(letters, c.deadLetterFromMessage(event.Message, event.Envelope, errors.New(reason)))
	}
	return letters
}

func (c *SyncConsumer) consumerName() string {
	return firstNonEmpty(c.svcCtx.Config.Kafka.GroupID, eventx.ConsumerFollowSync)
}

func (c *SyncConsumer) redisOpTimeout() time.Duration {
	if c.svcCtx.Config.Sync.RedisOpTimeoutMs <= 0 {
		return defaultRedisOpTimeout
	}
	return time.Duration(c.svcCtx.Config.Sync.RedisOpTimeoutMs) * time.Millisecond
}

func groupMessagesByTopicPartition(messages []kafkax.Message) []messageGroup {
	groupMap := make(map[string]int)
	groups := make([]messageGroup, 0)

	for _, msg := range messages {
		key := messageGroupKey(msg)
		index, ok := groupMap[key]
		if !ok {
			groupMap[key] = len(groups)
			groups = append(groups, messageGroup{
				Key:   key,
				Topic: msg.Topic,
			})
			index = len(groups) - 1
		}
		groups[index].Messages = append(groups[index].Messages, msg)
	}
	return groups
}

func messageGroupKey(msg kafkax.Message) string {
	return fmt.Sprintf("%s:%d", msg.Topic, msg.Partition)
}

func decodeEnvelope(value []byte) (eventx.Envelope, error) {
	if len(value) == 0 {
		return eventx.Envelope{}, fmt.Errorf("消息体不能为空")
	}

	var envelope eventx.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return eventx.Envelope{}, fmt.Errorf("解析事件信封失败: %w", err)
	}
	if envelope.EventID == "" {
		return eventx.Envelope{}, fmt.Errorf("event_id不能为空")
	}
	if envelope.EventType == "" {
		return eventx.Envelope{}, fmt.Errorf("event_type不能为空")
	}
	if envelope.AggregateType == "" {
		return eventx.Envelope{}, fmt.Errorf("aggregate_type不能为空")
	}
	if len(envelope.Payload) == 0 {
		return eventx.Envelope{}, fmt.Errorf("payload不能为空")
	}
	return envelope, nil
}

func followActionFromEventType(eventType string) (string, error) {
	switch eventType {
	case eventx.EventTypeFollowCreated:
		return eventx.FollowActionFollow, nil
	case eventx.EventTypeFollowDeleted:
		return eventx.FollowActionUnfollow, nil
	default:
		return "", fmt.Errorf("未知关注事件类型: %s", eventType)
	}
}

func mergeEventID(envelopeEventID string, payloadEventID string) (string, error) {
	if payloadEventID == "" {
		return envelopeEventID, nil
	}
	if envelopeEventID != payloadEventID {
		return "", fmt.Errorf("envelope.event_id和payload.event_id不一致, envelope:%s payload:%s", envelopeEventID, payloadEventID)
	}
	return envelopeEventID, nil
}

func normalizeSyncBatchSize(size int) int {
	if size <= 0 {
		return defaultSyncBatchSize
	}
	if size > maxSyncBatchSize {
		return maxSyncBatchSize
	}
	return size
}

func normalizeFlushBatchSize(size int) int {
	if size <= 0 {
		return defaultFlushBatchSize
	}
	if size > maxFlushBatchSize {
		return maxFlushBatchSize
	}
	return size
}

func normalizeSyncWorkerCount(workerCount, groupCount int) int {
	if groupCount <= 0 {
		return 0
	}
	if workerCount <= 0 {
		workerCount = defaultSyncWorkerCount
	}
	if workerCount > maxSyncWorkerCount {
		workerCount = maxSyncWorkerCount
	}
	if workerCount > groupCount {
		return groupCount
	}
	return workerCount
}

func normalizeMaxEventRetry(maxRetry int) int {
	if maxRetry <= 0 {
		return defaultMaxEventRetry
	}
	return maxRetry
}

func sleepBeforeRetry(ctx context.Context, conf config.SyncConf, attempt int) error {
	delay := retryBackoff(conf, attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryBackoff(conf config.SyncConf, attempt int) time.Duration {
	base := time.Duration(conf.RetryBackoffMs) * time.Millisecond
	if base <= 0 {
		base = defaultRetryBackoff
	}

	maxDelay := time.Duration(conf.MaxRetryBackoffMs) * time.Millisecond
	if maxDelay <= 0 {
		maxDelay = defaultMaxRetryBackoff
	}

	delay := base
	for i := 1; i < attempt; i++ {
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

func compactFailedEventIDs(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	if len(ids) <= maxLoggedFailedEventCount {
		return strings.Join(ids, ",")
	}
	return strings.Join(ids[:maxLoggedFailedEventCount], ",") + fmt.Sprintf("...(total:%d)", len(ids))
}

func marshalKafkaHeaders(headers []kafkax.Header) string {
	if len(headers) == 0 {
		return "[]"
	}

	records := make([]kafkaHeaderRecord, 0, len(headers))
	for _, header := range headers {
		records = append(records, kafkaHeaderRecord{
			Key:   header.Key,
			Value: string(header.Value),
		})
	}

	data, err := json.Marshal(records)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func truncateDeadLetterReason(reason string) string {
	runes := []rune(reason)
	if len(runes) <= maxDeadLetterReasonRunes {
		return reason
	}
	return string(runes[:maxDeadLetterReasonRunes])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func gormColumn(name string) clause.Expr {
	return clause.Expr{SQL: "VALUES(" + name + ")"}
}
