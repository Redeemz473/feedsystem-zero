package logic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"feedsystem-zero/apps/job/hotrank/internal/model"
	"feedsystem-zero/apps/job/hotrank/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
	"feedsystem-zero/common/rediskey"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm/clause"
)

const (
	defaultHotRankBatchSize               = 100
	maxHotRankBatchSize                   = 500
	defaultHotRankWorkerCount             = 4
	maxHotRankWorkerCount                 = 16
	defaultHotRankFlushInterval           = time.Second
	defaultHotRankWindowRetention         = 2 * time.Hour
	defaultHotRankProcessedEventTTL       = 14 * 24 * time.Hour
	defaultHotRankRedisOpTimeout          = time.Second
	defaultHotRankDBWriteTimeout          = 3 * time.Second
	defaultHotRankFutureTolerance         = 5 * time.Minute
	maxDeadLetterReasonRunes              = 1024
	maxDeadLetterIdentityRunes            = 128
	maxDeadLetterEventTypeRunes           = 64
	maxDeadLetterPayloadBytes             = 1 << 20
	deadLetterInsertBatchSize             = 50
	hotRankWindowMinuteLayout             = "200601021504"
	hotRankApplyResultDuplicate     int64 = 0
	hotRankApplyResultApplied       int64 = 1
	hotRankApplyResultStale         int64 = 2
)

// applyHotRankEventScript 将消费幂等标记和分钟窗口增量放在同一个 Lua 脚本中。
// 如果进程在 Redis 写成功、Kafka offset 提交前退出，消息重投时会命中幂等标记，
// 因而不会重复增加热度。过期事件只写幂等标记，不重新创建历史窗口。
const applyHotRankEventScript = `
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end

local processed_ttl = tonumber(ARGV[3])
local window_expire_at = tonumber(ARGV[4])
if not processed_ttl or processed_ttl <= 0 then
	return redis.error_reply("invalid processed event ttl")
end

if not window_expire_at or window_expire_at <= 0 then
	redis.call("SET", KEYS[1], "1", "NX", "EX", processed_ttl)
	return 2
end

local window_type = redis.call("TYPE", KEYS[2])
if type(window_type) == "table" then
	window_type = window_type["ok"]
end
if window_type ~= "none" and window_type ~= "zset" then
	return redis.error_reply("hot rank window key has wrong type")
end

local inserted = redis.call("SET", KEYS[1], "1", "NX", "EX", processed_ttl)
if not inserted then
	return 0
end

redis.call("ZINCRBY", KEYS[2], ARGV[1], ARGV[2])
redis.call("EXPIREAT", KEYS[2], window_expire_at)
return 1
`

// HotRankConsumer 消费点赞和评论事件，将热度增量写入 UTC 单分钟 ZSet。
//
// 这里只维护时间窗口，不修改 HotVideoRealtimeKey。后者已经由 interaction
// 在线写路径维护，再写一次会造成同一互动被重复计分。
type HotRankConsumer struct {
	svcCtx *svc.ServiceContext
	now    func() time.Time
}

type hotRankMessageGroup struct {
	Key      string
	Messages []kafkax.Message
}

type decodedHotRankEvent struct {
	Message    kafkax.Message
	Envelope   eventx.Envelope
	EventID    string
	VideoID    uint64
	ScoreDelta int64
	OccurredAt time.Time
}

type hotRankApplyCommand struct {
	Event decodedHotRankEvent
	Cmd   *redis.Cmd
}

type kafkaHeaderRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewHotRankConsumer(svcCtx *svc.ServiceContext) *HotRankConsumer {
	return &HotRankConsumer{
		svcCtx: svcCtx,
		now:    time.Now,
	}
}

func (c *HotRankConsumer) Run(ctx context.Context) error {
	batchSize := c.svcCtx.Config.HotRank.BatchSize
	if batchSize <= 0 {
		batchSize = c.svcCtx.Config.Kafka.BatchSize
	}
	batchSize = normalizeHotRankBatchSize(batchSize)

	flushInterval := time.Duration(c.svcCtx.Config.HotRank.FlushMs) * time.Millisecond
	if flushInterval <= 0 {
		flushInterval = defaultHotRankFlushInterval
	}

	return c.svcCtx.Consumer.RunBatch(ctx, batchSize, flushInterval, c.HandleBatch)
}

// HandleBatch 按 topic+partition 分组并发，组内仍按 Kafka offset 顺序处理。
// Kafka 只保证同一 partition 内有序；分组后可以提高多分区堆积时的消费吞吐，
// 同时避免同一分区的先后事件被并发打乱。
func (c *HotRankConsumer) HandleBatch(ctx context.Context, messages []kafkax.Message) error {
	if len(messages) == 0 {
		return nil
	}

	groups := groupHotRankMessages(messages)
	workerCount := normalizeHotRankWorkerCount(c.svcCtx.Config.HotRank.WorkerCount, len(groups))
	if workerCount == 0 {
		return nil
	}

	jobs := make(chan hotRankMessageGroup)
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
					errCh <- fmt.Errorf("处理热榜消息组失败, group:%s size:%d: %w", group.Key, len(group.Messages), err)
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
		"hotrank batch handled, messages:%d groups:%d workers:%d",
		len(messages),
		len(groups),
		workerCount,
	)
	return nil
}

func (c *HotRankConsumer) handleMessageGroup(ctx context.Context, group hotRankMessageGroup) error {
	events := make([]decodedHotRankEvent, 0, len(group.Messages))
	deadLetters := make([]model.DeadLetterEvent, 0)

	for _, msg := range group.Messages {
		decoded, envelope, err := c.decodeMessage(msg)
		if err != nil {
			deadLetters = append(deadLetters, c.deadLetterFromMessage(msg, envelope, err))
			continue
		}
		events = append(events, decoded)
	}

	if len(deadLetters) > 0 {
		// 解码错误属于不可重试错误。先落死信再放过这些消息，避免单条脏数据
		// 永久阻塞所在 Kafka partition。
		if err := c.recordDeadLetters(ctx, deadLetters); err != nil {
			return fmt.Errorf("写入热榜死信失败: %w", err)
		}
		logx.WithContext(ctx).Errorf(
			"hotrank decode dead letters recorded, group:%s count:%d",
			group.Key,
			len(deadLetters),
		)
	}

	if len(events) == 0 {
		return nil
	}
	return c.applyEvents(ctx, events)
}

func (c *HotRankConsumer) decodeMessage(msg kafkax.Message) (decodedHotRankEvent, eventx.Envelope, error) {
	envelope, err := decodeHotRankEnvelope(msg.Value)
	if err != nil {
		return decodedHotRankEvent{}, envelope, err
	}

	switch msg.Topic {
	case eventx.TopicInteractionLikeEvents:
		decoded, err := c.decodeLikeEvent(msg, envelope)
		return decoded, envelope, err
	case eventx.TopicInteractionCommentEvents:
		decoded, err := c.decodeCommentEvent(msg, envelope)
		return decoded, envelope, err
	default:
		return decodedHotRankEvent{}, envelope, fmt.Errorf("非预期的 Kafka topic: %s", msg.Topic)
	}
}

func (c *HotRankConsumer) decodeLikeEvent(msg kafkax.Message, envelope eventx.Envelope) (decodedHotRankEvent, error) {
	if envelope.AggregateType != eventx.AggregateLike {
		return decodedHotRankEvent{}, fmt.Errorf("点赞事件 aggregate_type 不匹配: %s", envelope.AggregateType)
	}

	expectedAction, expectedDelta, err := likeSemantics(envelope.EventType)
	if err != nil {
		return decodedHotRankEvent{}, err
	}

	var payload eventx.LikeEvent
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return decodedHotRankEvent{}, fmt.Errorf("解析点赞事件 payload 失败: %w", err)
	}
	eventID, err := mergeHotRankEventID(envelope.EventID, payload.EventID)
	if err != nil {
		return decodedHotRankEvent{}, err
	}
	if payload.VideoID == 0 {
		return decodedHotRankEvent{}, errors.New("点赞事件 video_id 不能为空")
	}
	if payload.UserID == 0 {
		return decodedHotRankEvent{}, errors.New("点赞事件 user_id 不能为空")
	}
	if payload.Action != expectedAction {
		return decodedHotRankEvent{}, fmt.Errorf(
			"点赞事件 event_type 与 action 不一致, event_type:%s action:%s",
			envelope.EventType,
			payload.Action,
		)
	}
	if payload.Delta != expectedDelta {
		return decodedHotRankEvent{}, fmt.Errorf(
			"点赞事件 action 与 delta 不一致, action:%s delta:%d",
			payload.Action,
			payload.Delta,
		)
	}

	occurredAt, err := c.resolveOccurredAt(envelope.OccurredAt, payload.OccurredAt)
	if err != nil {
		return decodedHotRankEvent{}, err
	}

	return decodedHotRankEvent{
		Message:    msg,
		Envelope:   envelope,
		EventID:    eventID,
		VideoID:    payload.VideoID,
		ScoreDelta: payload.Delta * eventx.LikePopularityWeight,
		OccurredAt: occurredAt,
	}, nil
}

func (c *HotRankConsumer) decodeCommentEvent(msg kafkax.Message, envelope eventx.Envelope) (decodedHotRankEvent, error) {
	if envelope.AggregateType != eventx.AggregateComment {
		return decodedHotRankEvent{}, fmt.Errorf("评论事件 aggregate_type 不匹配: %s", envelope.AggregateType)
	}

	expectedAction, expectedDelta, err := commentSemantics(envelope.EventType)
	if err != nil {
		return decodedHotRankEvent{}, err
	}

	var payload eventx.CommentEvent
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return decodedHotRankEvent{}, fmt.Errorf("解析评论事件 payload 失败: %w", err)
	}
	eventID, err := mergeHotRankEventID(envelope.EventID, payload.EventID)
	if err != nil {
		return decodedHotRankEvent{}, err
	}
	if payload.CommentID == 0 {
		return decodedHotRankEvent{}, errors.New("评论事件 comment_id 不能为空")
	}
	if payload.VideoID == 0 {
		return decodedHotRankEvent{}, errors.New("评论事件 video_id 不能为空")
	}
	if payload.UserID == 0 {
		return decodedHotRankEvent{}, errors.New("评论事件 user_id 不能为空")
	}
	if payload.Action != expectedAction {
		return decodedHotRankEvent{}, fmt.Errorf(
			"评论事件 event_type 与 action 不一致, event_type:%s action:%s",
			envelope.EventType,
			payload.Action,
		)
	}
	if payload.Delta != expectedDelta {
		return decodedHotRankEvent{}, fmt.Errorf(
			"评论事件 action 与 delta 不一致, action:%s delta:%d",
			payload.Action,
			payload.Delta,
		)
	}

	occurredAt, err := c.resolveOccurredAt(envelope.OccurredAt, payload.OccurredAt)
	if err != nil {
		return decodedHotRankEvent{}, err
	}

	return decodedHotRankEvent{
		Message:    msg,
		Envelope:   envelope,
		EventID:    eventID,
		VideoID:    payload.VideoID,
		ScoreDelta: payload.Delta * eventx.CommentPopularityWeight,
		OccurredAt: occurredAt,
	}, nil
}

func (c *HotRankConsumer) resolveOccurredAt(envelopeAt int64, payloadAt int64) (time.Time, error) {
	if envelopeAt > 0 && payloadAt > 0 && envelopeAt != payloadAt {
		return time.Time{}, fmt.Errorf(
			"envelope.occurred_at 与 payload.occurred_at 不一致, envelope:%d payload:%d",
			envelopeAt,
			payloadAt,
		)
	}

	occurredAtMillis := payloadAt
	if occurredAtMillis <= 0 {
		occurredAtMillis = envelopeAt
	}
	if occurredAtMillis <= 0 {
		return time.Time{}, errors.New("occurred_at 不能为空")
	}

	occurredAt := time.UnixMilli(occurredAtMillis).UTC()
	if occurredAt.After(c.currentTime().UTC().Add(c.futureTolerance())) {
		return time.Time{}, fmt.Errorf("occurred_at 超出允许的未来时间范围: %d", occurredAtMillis)
	}
	return occurredAt, nil
}

// applyEvents 用 Redis Pipeline 降低网络往返次数，每条消息仍由 Lua 单独保证原子性。
// Pipeline 部分成功后即使客户端超时，Kafka 重试也会被每条事件的幂等 Key 拦住。
func (c *HotRankConsumer) applyEvents(ctx context.Context, events []decodedHotRankEvent) error {
	if len(events) == 0 {
		return nil
	}

	redisCtx, cancel := context.WithTimeout(ctx, c.redisOpTimeout())
	defer cancel()

	now := c.currentTime().UTC()
	retention := c.windowRetention()
	processedTTLSeconds := int64(c.processedEventTTL() / time.Second)

	pipe := c.svcCtx.RedisCli.Pipeline()
	commands := make([]hotRankApplyCommand, 0, len(events))
	for _, event := range events {
		windowExpireAt := hotRankWindowExpireAt(event.OccurredAt, retention)
		windowExpireUnix := windowExpireAt.Unix()
		if !windowExpireAt.After(now) {
			// 过期消息仍写幂等标记，但不重建已经淘汰的历史分钟窗口。
			windowExpireUnix = 0
		}

		cmd := pipe.Eval(
			redisCtx,
			applyHotRankEventScript,
			[]string{
				rediskey.ProcessedEventKey(event.EventID, eventx.ConsumerHotRank),
				rediskey.HotVideoWindowKey(hotRankWindowBucket(event.OccurredAt)),
			},
			strconv.FormatInt(event.ScoreDelta, 10),
			strconv.FormatUint(event.VideoID, 10),
			processedTTLSeconds,
			windowExpireUnix,
		)
		commands = append(commands, hotRankApplyCommand{Event: event, Cmd: cmd})
	}

	if _, err := pipe.Exec(redisCtx); err != nil {
		return fmt.Errorf("执行热榜 Redis Pipeline 失败: %w", err)
	}

	var applied, duplicate, stale int
	for _, command := range commands {
		result, err := command.Cmd.Int64()
		if err != nil {
			return fmt.Errorf("读取热榜 Lua 结果失败, event_id:%s: %w", command.Event.EventID, err)
		}
		switch result {
		case hotRankApplyResultDuplicate:
			duplicate++
		case hotRankApplyResultApplied:
			applied++
		case hotRankApplyResultStale:
			stale++
		default:
			return fmt.Errorf("热榜 Lua 返回未知状态, event_id:%s result:%d", command.Event.EventID, result)
		}
	}

	logx.WithContext(ctx).Infof(
		"hotrank events applied, total:%d applied:%d duplicate:%d stale:%d",
		len(events),
		applied,
		duplicate,
		stale,
	)
	return nil
}

func (c *HotRankConsumer) recordDeadLetters(ctx context.Context, letters []model.DeadLetterEvent) error {
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

func (c *HotRankConsumer) deadLetterFromMessage(
	msg kafkax.Message,
	envelope eventx.Envelope,
	cause error,
) model.DeadLetterEvent {
	now := c.currentTime()
	return model.DeadLetterEvent{
		ConsumerName:  eventx.ConsumerHotRank,
		Topic:         truncateRunes(msg.Topic, maxDeadLetterIdentityRunes),
		PartitionNo:   int32(msg.Partition),
		OffsetNo:      msg.Offset,
		EventID:       truncateRunes(envelope.EventID, maxDeadLetterIdentityRunes),
		EventType:     truncateRunes(envelope.EventType, maxDeadLetterEventTypeRunes),
		AggregateType: truncateRunes(envelope.AggregateType, maxDeadLetterEventTypeRunes),
		AggregateID:   truncateRunes(envelope.AggregateID, maxDeadLetterIdentityRunes),
		Reason:        truncateRunes(cause.Error(), maxDeadLetterReasonRunes),
		Payload:       hotRankDeadLetterPayload(msg.Value),
		Headers:       marshalHotRankKafkaHeaders(msg.Headers),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func (c *HotRankConsumer) currentTime() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

func (c *HotRankConsumer) windowRetention() time.Duration {
	seconds := c.svcCtx.Config.HotRank.WindowRetentionSeconds
	if seconds <= 0 {
		return defaultHotRankWindowRetention
	}
	return time.Duration(seconds) * time.Second
}

func (c *HotRankConsumer) processedEventTTL() time.Duration {
	days := c.svcCtx.Config.HotRank.ProcessedEventTTLDays
	if days <= 0 {
		return defaultHotRankProcessedEventTTL
	}
	return time.Duration(days) * 24 * time.Hour
}

func (c *HotRankConsumer) redisOpTimeout() time.Duration {
	ms := c.svcCtx.Config.HotRank.RedisOpTimeoutMs
	if ms <= 0 {
		return defaultHotRankRedisOpTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

func (c *HotRankConsumer) dbWriteTimeout() time.Duration {
	ms := c.svcCtx.Config.HotRank.DBWriteTimeoutMs
	if ms <= 0 {
		return defaultHotRankDBWriteTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

func (c *HotRankConsumer) futureTolerance() time.Duration {
	seconds := c.svcCtx.Config.HotRank.FutureToleranceSeconds
	if seconds <= 0 {
		return defaultHotRankFutureTolerance
	}
	return time.Duration(seconds) * time.Second
}

func decodeHotRankEnvelope(value []byte) (eventx.Envelope, error) {
	if len(value) == 0 {
		return eventx.Envelope{}, errors.New("Kafka 消息体不能为空")
	}

	var envelope eventx.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return eventx.Envelope{}, fmt.Errorf("解析事件信封失败: %w", err)
	}
	envelope.EventID = strings.TrimSpace(envelope.EventID)
	envelope.EventType = strings.TrimSpace(envelope.EventType)
	envelope.AggregateType = strings.TrimSpace(envelope.AggregateType)
	if envelope.EventID == "" {
		return envelope, errors.New("envelope.event_id 不能为空")
	}
	if len([]rune(envelope.EventID)) > maxDeadLetterIdentityRunes {
		return envelope, fmt.Errorf("envelope.event_id 长度不能超过 %d", maxDeadLetterIdentityRunes)
	}
	if envelope.EventType == "" {
		return envelope, errors.New("envelope.event_type 不能为空")
	}
	if envelope.AggregateType == "" {
		return envelope, errors.New("envelope.aggregate_type 不能为空")
	}
	if len(envelope.Payload) == 0 || string(envelope.Payload) == "null" {
		return envelope, errors.New("envelope.payload 不能为空")
	}
	return envelope, nil
}

func mergeHotRankEventID(envelopeEventID string, payloadEventID string) (string, error) {
	payloadEventID = strings.TrimSpace(payloadEventID)
	if payloadEventID == "" {
		return envelopeEventID, nil
	}
	if envelopeEventID != payloadEventID {
		return "", fmt.Errorf(
			"envelope.event_id 与 payload.event_id 不一致, envelope:%s payload:%s",
			envelopeEventID,
			payloadEventID,
		)
	}
	return envelopeEventID, nil
}

func likeSemantics(eventType string) (string, int64, error) {
	switch eventType {
	case eventx.EventTypeLikeCreated:
		return eventx.LikeActionLike, 1, nil
	case eventx.EventTypeLikeDeleted:
		return eventx.LikeActionUnlike, -1, nil
	default:
		return "", 0, fmt.Errorf("未知点赞事件类型: %s", eventType)
	}
}

func commentSemantics(eventType string) (string, int64, error) {
	switch eventType {
	case eventx.EventTypeCommentCreated:
		return eventx.CommentActionCreate, 1, nil
	case eventx.EventTypeCommentDeleted:
		return eventx.CommentActionDelete, -1, nil
	default:
		return "", 0, fmt.Errorf("未知评论事件类型: %s", eventType)
	}
}

func hotRankWindowBucket(occurredAt time.Time) string {
	return occurredAt.UTC().Truncate(time.Minute).Format(hotRankWindowMinuteLayout)
}

func hotRankWindowExpireAt(occurredAt time.Time, retention time.Duration) time.Time {
	return occurredAt.UTC().Truncate(time.Minute).Add(retention)
}

func groupHotRankMessages(messages []kafkax.Message) []hotRankMessageGroup {
	groupIndexes := make(map[string]int)
	groups := make([]hotRankMessageGroup, 0)
	for _, msg := range messages {
		key := fmt.Sprintf("%s:%d", msg.Topic, msg.Partition)
		index, ok := groupIndexes[key]
		if !ok {
			groupIndexes[key] = len(groups)
			groups = append(groups, hotRankMessageGroup{Key: key})
			index = len(groups) - 1
		}
		groups[index].Messages = append(groups[index].Messages, msg)
	}
	return groups
}

func normalizeHotRankBatchSize(size int) int {
	if size <= 0 {
		return defaultHotRankBatchSize
	}
	if size > maxHotRankBatchSize {
		return maxHotRankBatchSize
	}
	return size
}

func normalizeHotRankWorkerCount(workerCount int, groupCount int) int {
	if groupCount <= 0 {
		return 0
	}
	if workerCount <= 0 {
		workerCount = defaultHotRankWorkerCount
	}
	if workerCount > maxHotRankWorkerCount {
		workerCount = maxHotRankWorkerCount
	}
	if workerCount > groupCount {
		return groupCount
	}
	return workerCount
}

func marshalHotRankKafkaHeaders(headers []kafkax.Header) string {
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

func hotRankDeadLetterPayload(value []byte) string {
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
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes])
}

func gormValuesColumn(name string) clause.Expr {
	return clause.Expr{SQL: "VALUES(" + name + ")"}
}
