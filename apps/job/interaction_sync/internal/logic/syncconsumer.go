package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	interactionpb "feedsystem-zero/apps/interaction/interaction"
	"feedsystem-zero/apps/interaction/interactionclient"
	"feedsystem-zero/apps/job/interaction_sync/internal/config"
	"feedsystem-zero/apps/job/interaction_sync/internal/model"
	"feedsystem-zero/apps/job/interaction_sync/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"

	"github.com/zeromicro/go-zero/core/logx"
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
	defaultSyncRPCTimeout     = 10 * time.Second
	maxLoggedFailedEventCount = 10
	maxDeadLetterReasonRunes  = 1024
)

type interactionEventKind int

const (
	interactionEventKindLike interactionEventKind = iota + 1
	interactionEventKindComment
)

type SyncConsumer struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

// messageGroup 表示同一个 topic + partition 的消息组。
// Kafka 只保证单个 partition 内有序，所以组内必须顺序处理，组之间才可以并发。
type messageGroup struct {
	Key      string
	Topic    string
	Messages []kafkax.Message
}

// decodedInteractionEvent 保留原始 Kafka 消息和业务事件。
// 这样 Flush 部分失败时，可以只重试失败事件；最终失败时也能写入完整死信信息。
type decodedInteractionEvent struct {
	Kind     interactionEventKind
	Like     *interactionclient.LikeEvent
	Comment  *interactionclient.CommentEvent
	Message  kafkax.Message
	Envelope eventx.Envelope
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
	// 按 topic + partition 分组：
	// 不同 partition 可以并发提高吞吐；同一 partition 内保持原始顺序，避免点赞/取消点赞顺序被打乱。
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

	logger.Infof("interaction sync batch handled, messages:%d groups:%d workers:%d", len(messages), len(groups), workerCount)
	return nil
}

func (c *SyncConsumer) handleMessageGroup(ctx context.Context, group messageGroup) error {
	events, deadLetters := c.decodeGroupMessages(group.Messages)
	if len(deadLetters) > 0 {
		// 解码失败通常是永久性坏消息，继续重试只会卡住同一个 Kafka 分区。
		// 写入死信表后放行，后续可以人工排查或做补偿重放。
		if err := c.recordDeadLetters(ctx, deadLetters); err != nil {
			return fmt.Errorf("record decode dead letters failed: %w", err)
		}
		logx.WithContext(ctx).Errorf("interaction sync recorded decode dead letters, group:%s count:%d", group.Key, len(deadLetters))
	}
	if len(events) == 0 {
		return nil
	}

	if err := c.flushDecodedEvents(ctx, events); err != nil {
		return err
	}
	return nil
}

func (c *SyncConsumer) decodeGroupMessages(messages []kafkax.Message) ([]decodedInteractionEvent, []model.DeadLetterEvent) {
	events := make([]decodedInteractionEvent, 0, len(messages))
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

func (c *SyncConsumer) decodeMessage(msg kafkax.Message) (decodedInteractionEvent, model.DeadLetterEvent, bool) {
	// 这里严格校验 envelope 与 payload 的一致性。
	// 因为这些消息理论上都来自本系统 outbox，一旦结构不对，应该进入死信而不是静默跳过。
	envelope, err := decodeEnvelope(msg.Value)
	if err != nil {
		return decodedInteractionEvent{}, c.deadLetterFromMessage(msg, eventx.Envelope{}, err), false
	}

	switch msg.Topic {
	case eventx.TopicInteractionLikeEvents:
		event, err := decodeLikeEvent(envelope)
		if err != nil {
			return decodedInteractionEvent{}, c.deadLetterFromMessage(msg, envelope, err), false
		}
		return decodedInteractionEvent{
			Kind:     interactionEventKindLike,
			Like:     event,
			Message:  msg,
			Envelope: envelope,
		}, model.DeadLetterEvent{}, true
	case eventx.TopicInteractionCommentEvents:
		event, err := decodeCommentEvent(envelope)
		if err != nil {
			return decodedInteractionEvent{}, c.deadLetterFromMessage(msg, envelope, err), false
		}
		return decodedInteractionEvent{
			Kind:     interactionEventKindComment,
			Comment:  event,
			Message:  msg,
			Envelope: envelope,
		}, model.DeadLetterEvent{}, true
	default:
		return decodedInteractionEvent{}, c.deadLetterFromMessage(msg, envelope, fmt.Errorf("未知互动事件 topic: %s", msg.Topic)), false
	}
}

func (c *SyncConsumer) flushDecodedEvents(ctx context.Context, events []decodedInteractionEvent) error {
	// interaction-rpc 将点赞和评论拆成两个 Flush 接口；
	// consumer 先按事件类型拆分，再分别调用对应 RPC。
	likeEvents := make([]decodedInteractionEvent, 0)
	commentEvents := make([]decodedInteractionEvent, 0)

	for _, event := range events {
		switch event.Kind {
		case interactionEventKindLike:
			likeEvents = append(likeEvents, event)
		case interactionEventKindComment:
			commentEvents = append(commentEvents, event)
		default:
			return fmt.Errorf("未知解码事件类型: %d", event.Kind)
		}
	}

	if err := c.flushLikeEvents(ctx, likeEvents); err != nil {
		return err
	}
	if err := c.flushCommentEvents(ctx, commentEvents); err != nil {
		return err
	}
	return nil
}

func (c *SyncConsumer) flushLikeEvents(ctx context.Context, events []decodedInteractionEvent) error {
	pending := events
	maxAttempts := normalizeMaxEventRetry(c.svcCtx.Config.Sync.MaxEventRetry)

	for attempt := 1; len(pending) > 0; attempt++ {
		// Flush RPC 返回 FailedEventIds 时，只重试失败子集。
		// 已成功事件已经由 processed_events 幂等表兜住，不需要反复进入 DB。
		failed, err := c.flushLikeEventsOnce(ctx, pending)
		if err != nil {
			return err
		}
		if len(failed) == 0 {
			return nil
		}
		if attempt >= maxAttempts {
			// 多次仍失败说明更可能是数据问题，写死信后提交 offset，避免整个分区永久阻塞。
			reason := fmt.Sprintf("flush like events partial failed after %d attempts", attempt)
			return c.recordDeadLetters(ctx, c.deadLettersFromDecodedEvents(failed, reason))
		}
		if err := sleepBeforeRetry(ctx, c.svcCtx.Config.Sync, attempt); err != nil {
			return err
		}
		pending = failed
	}
	return nil
}

func (c *SyncConsumer) flushLikeEventsOnce(ctx context.Context, events []decodedInteractionEvent) ([]decodedInteractionEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}

	failedEvents := make([]decodedInteractionEvent, 0)
	batchSize := normalizeFlushBatchSize(c.svcCtx.Config.Sync.FlushBatchSize)
	for start := 0; start < len(events); start += batchSize {
		end := min(start+batchSize, len(events))
		chunk := events[start:end]

		rpcCtx, cancel := context.WithTimeout(ctx, syncRPCTimeout(c.svcCtx.Config.Sync))
		resp, err := c.svcCtx.InteractionRpc.FlushLikeEvents(rpcCtx, &interactionclient.FlushLikeEventsReq{
			Events: protoLikeEvents(chunk),
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("flush like events failed, range:%d-%d size:%d: %w", start, end, len(chunk), err)
		}
		if resp == nil {
			return nil, fmt.Errorf("flush like events failed, range:%d-%d size:%d: empty rpc response", start, end, len(chunk))
		}
		failed, err := failedDecodedEvents(chunk, resp.GetFailedEventIds())
		if err != nil {
			return nil, fmt.Errorf("flush like events failed, range:%d-%d: %w", start, end, err)
		}
		if len(failed) > 0 {
			logx.WithContext(ctx).Errorf("flush like events partial failed, range:%d-%d success:%d failed:%s",
				start,
				end,
				resp.GetSuccessCount(),
				compactFailedEventIDs(resp.GetFailedEventIds()),
			)
			failedEvents = append(failedEvents, failed...)
		}
	}
	return failedEvents, nil
}

func (c *SyncConsumer) flushCommentEvents(ctx context.Context, events []decodedInteractionEvent) error {
	pending := events
	maxAttempts := normalizeMaxEventRetry(c.svcCtx.Config.Sync.MaxEventRetry)

	for attempt := 1; len(pending) > 0; attempt++ {
		// 评论事件和点赞事件采用同样策略：成功的不再重复刷库，失败子集有限重试。
		failed, err := c.flushCommentEventsOnce(ctx, pending)
		if err != nil {
			return err
		}
		if len(failed) == 0 {
			return nil
		}
		if attempt >= maxAttempts {
			// 达到最大重试次数后落死信，后续靠 dead_letter_events 做人工处理或补偿任务。
			reason := fmt.Sprintf("flush comment events partial failed after %d attempts", attempt)
			return c.recordDeadLetters(ctx, c.deadLettersFromDecodedEvents(failed, reason))
		}
		if err := sleepBeforeRetry(ctx, c.svcCtx.Config.Sync, attempt); err != nil {
			return err
		}
		pending = failed
	}
	return nil
}

func (c *SyncConsumer) flushCommentEventsOnce(ctx context.Context, events []decodedInteractionEvent) ([]decodedInteractionEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}

	failedEvents := make([]decodedInteractionEvent, 0)
	batchSize := normalizeFlushBatchSize(c.svcCtx.Config.Sync.FlushBatchSize)
	for start := 0; start < len(events); start += batchSize {
		end := min(start+batchSize, len(events))
		chunk := events[start:end]

		rpcCtx, cancel := context.WithTimeout(ctx, syncRPCTimeout(c.svcCtx.Config.Sync))
		resp, err := c.svcCtx.InteractionRpc.FlushCommentEvents(rpcCtx, &interactionclient.FlushCommentEventsReq{
			Events: protoCommentEvents(chunk),
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("flush comment events failed, range:%d-%d size:%d: %w", start, end, len(chunk), err)
		}
		if resp == nil {
			return nil, fmt.Errorf("flush comment events failed, range:%d-%d size:%d: empty rpc response", start, end, len(chunk))
		}
		failed, err := failedDecodedEvents(chunk, resp.GetFailedEventIds())
		if err != nil {
			return nil, fmt.Errorf("flush comment events failed, range:%d-%d: %w", start, end, err)
		}
		if len(failed) > 0 {
			logx.WithContext(ctx).Errorf("flush comment events partial failed, range:%d-%d success:%d failed:%s",
				start,
				end,
				resp.GetSuccessCount(),
				compactFailedEventIDs(resp.GetFailedEventIds()),
			)
			failedEvents = append(failedEvents, failed...)
		}
	}
	return failedEvents, nil
}

func (c *SyncConsumer) recordDeadLetters(ctx context.Context, letters []model.DeadLetterEvent) error {
	if len(letters) == 0 {
		return nil
	}

	// 死信写入也做幂等：同一个 consumer + topic + partition + offset 只保留一条记录。
	// 如果 consumer 在写死信后、提交 offset 前重启，重放时不会重复插入。
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
		ConsumerName:  firstNonEmpty(c.svcCtx.Config.Kafka.GroupID, "interaction-sync-job"),
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

func (c *SyncConsumer) deadLettersFromDecodedEvents(events []decodedInteractionEvent, reason string) []model.DeadLetterEvent {
	letters := make([]model.DeadLetterEvent, 0, len(events))
	for _, event := range events {
		letters = append(letters, c.deadLetterFromMessage(event.Message, event.Envelope, errors.New(reason)))
	}
	return letters
}

func groupMessagesByTopicPartition(messages []kafkax.Message) []messageGroup {
	// Kafka 的顺序保证粒度是 partition，不是整个 topic。
	// 因此这里把一批消息拆成多个保序小队列，再交给 worker pool 并发执行。
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

func protoLikeEvents(events []decodedInteractionEvent) []*interactionclient.LikeEvent {
	result := make([]*interactionclient.LikeEvent, 0, len(events))
	for _, event := range events {
		if event.Like != nil {
			result = append(result, event.Like)
		}
	}
	return result
}

func protoCommentEvents(events []decodedInteractionEvent) []*interactionclient.CommentEvent {
	result := make([]*interactionclient.CommentEvent, 0, len(events))
	for _, event := range events {
		if event.Comment != nil {
			result = append(result, event.Comment)
		}
	}
	return result
}

func failedDecodedEvents(events []decodedInteractionEvent, failedIDs []string) ([]decodedInteractionEvent, error) {
	if len(failedIDs) == 0 {
		return nil, nil
	}

	// RPC 只返回失败 event_id，这里映射回原始事件，方便下一轮只重试失败子集。
	eventsByID := make(map[string]decodedInteractionEvent, len(events))
	for _, event := range events {
		eventID := decodedEventID(event)
		if eventID != "" {
			eventsByID[eventID] = event
		}
	}

	failed := make([]decodedInteractionEvent, 0, len(failedIDs))
	seen := make(map[string]struct{}, len(failedIDs))
	for _, eventID := range failedIDs {
		if _, ok := seen[eventID]; ok {
			continue
		}
		event, ok := eventsByID[eventID]
		if !ok {
			return nil, fmt.Errorf("rpc returned unknown failed_event_id:%s", eventID)
		}
		seen[eventID] = struct{}{}
		failed = append(failed, event)
	}
	return failed, nil
}

func decodedEventID(event decodedInteractionEvent) string {
	switch event.Kind {
	case interactionEventKindLike:
		if event.Like != nil {
			return event.Like.GetEventId()
		}
	case interactionEventKindComment:
		if event.Comment != nil {
			return event.Comment.GetEventId()
		}
	}
	return ""
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

func decodeLikeEvent(envelope eventx.Envelope) (*interactionclient.LikeEvent, error) {
	if envelope.AggregateType != eventx.AggregateLike {
		return nil, fmt.Errorf("aggregate_type不匹配: %s", envelope.AggregateType)
	}

	expectedAction, err := likeActionFromEventType(envelope.EventType)
	if err != nil {
		return nil, err
	}

	var payload eventx.LikeEvent
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return nil, fmt.Errorf("解析点赞事件失败: %w", err)
	}
	if payload.Action == "" {
		payload.Action = expectedAction
	}
	if payload.Action != expectedAction {
		return nil, fmt.Errorf("event_type和payload.action不一致, event_type:%s action:%s", envelope.EventType, payload.Action)
	}

	eventID, err := mergeEventID(envelope.EventID, payload.EventID)
	if err != nil {
		return nil, err
	}
	if payload.VideoID == 0 {
		return nil, fmt.Errorf("video_id不能为空")
	}
	if payload.UserID == 0 {
		return nil, fmt.Errorf("user_id不能为空")
	}

	action, err := toProtoLikeAction(payload.Action)
	if err != nil {
		return nil, err
	}

	eventTime := firstPositiveInt64(payload.OccurredAt, envelope.OccurredAt)
	if eventTime <= 0 {
		return nil, fmt.Errorf("event_time不能为空")
	}

	return &interactionclient.LikeEvent{
		EventId:   eventID,
		VideoId:   payload.VideoID,
		UserId:    payload.UserID,
		Action:    action,
		EventTime: eventTime,
	}, nil
}

func decodeCommentEvent(envelope eventx.Envelope) (*interactionclient.CommentEvent, error) {
	if envelope.AggregateType != eventx.AggregateComment {
		return nil, fmt.Errorf("aggregate_type不匹配: %s", envelope.AggregateType)
	}

	expectedAction, err := commentActionFromEventType(envelope.EventType)
	if err != nil {
		return nil, err
	}

	var payload eventx.CommentEvent
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return nil, fmt.Errorf("解析评论事件失败: %w", err)
	}
	if payload.Action == "" {
		payload.Action = expectedAction
	}
	if payload.Action != expectedAction {
		return nil, fmt.Errorf("event_type和payload.action不一致, event_type:%s action:%s", envelope.EventType, payload.Action)
	}

	eventID, err := mergeEventID(envelope.EventID, payload.EventID)
	if err != nil {
		return nil, err
	}
	if payload.VideoID == 0 {
		return nil, fmt.Errorf("video_id不能为空")
	}
	if payload.CommentID == 0 {
		return nil, fmt.Errorf("comment_id不能为空")
	}
	if payload.UserID == 0 {
		return nil, fmt.Errorf("user_id不能为空")
	}

	action, err := toProtoCommentAction(payload.Action)
	if err != nil {
		return nil, err
	}

	eventTime := firstPositiveInt64(payload.OccurredAt, envelope.OccurredAt)
	if eventTime <= 0 {
		return nil, fmt.Errorf("event_time不能为空")
	}

	return &interactionclient.CommentEvent{
		EventId:   eventID,
		VideoId:   payload.VideoID,
		CommentId: payload.CommentID,
		UserId:    payload.UserID,
		Action:    action,
		EventTime: eventTime,
	}, nil
}

func likeActionFromEventType(eventType string) (string, error) {
	switch eventType {
	case eventx.EventTypeLikeCreated:
		return eventx.LikeActionLike, nil
	case eventx.EventTypeLikeDeleted:
		return eventx.LikeActionUnlike, nil
	default:
		return "", fmt.Errorf("未知点赞事件类型: %s", eventType)
	}
}

func commentActionFromEventType(eventType string) (string, error) {
	switch eventType {
	case eventx.EventTypeCommentCreated:
		return eventx.CommentActionCreate, nil
	case eventx.EventTypeCommentDeleted:
		return eventx.CommentActionDelete, nil
	default:
		return "", fmt.Errorf("未知评论事件类型: %s", eventType)
	}
}

func toProtoLikeAction(action string) (interactionpb.LikeAction, error) {
	switch action {
	case eventx.LikeActionLike:
		return interactionpb.LikeAction_LIKE_ACTION_LIKE, nil
	case eventx.LikeActionUnlike:
		return interactionpb.LikeAction_LIKE_ACTION_UNLIKE, nil
	default:
		return interactionpb.LikeAction_LIKE_ACTION_UNKNOWN, fmt.Errorf("未知点赞动作: %s", action)
	}
}

func toProtoCommentAction(action string) (interactionpb.CommentAction, error) {
	switch action {
	case eventx.CommentActionCreate:
		return interactionpb.CommentAction_COMMENT_ACTION_CREATE, nil
	case eventx.CommentActionDelete:
		return interactionpb.CommentAction_COMMENT_ACTION_DELETE, nil
	default:
		return interactionpb.CommentAction_COMMENT_ACTION_UNKNOWN, fmt.Errorf("未知评论动作: %s", action)
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

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
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

func syncRPCTimeout(conf config.SyncConf) time.Duration {
	timeout := time.Duration(conf.RpcTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		return defaultSyncRPCTimeout
	}
	return timeout
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
	// MySQL upsert 中引用即将插入的新值，用于死信重复写入时刷新原因和 payload。
	return clause.Expr{SQL: "VALUES(" + name + ")"}
}
