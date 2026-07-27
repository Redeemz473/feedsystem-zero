package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"feedsystem-zero/apps/job/feed_timeline/internal/model"
	"feedsystem-zero/apps/job/feed_timeline/internal/svc"
	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm/clause"
)

const (
	timelineEventKindVideo timelineEventKind = iota + 1
	timelineEventKindFollow

	videoActionPublish = "publish"
	videoActionDelete  = "delete"

	maxDeadLetterReasonRunes = 1024
)

type timelineEventKind int

// TimelineConsumer 同时消费视频与关注事件：
//   - feed.video.events 维护全局最新流，并向已构建的活跃用户 Timeline fanout；
//   - social.follow.events 对已构建 Timeline 做关注回填或取关清理；
//   - 坏消息写 dead_letter_events，成功副作用写 processed_events。
type TimelineConsumer struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

type timelineMessageGroup struct {
	Key      string
	Messages []kafkax.Message
}

type decodedTimelineEvent struct {
	Kind     timelineEventKind
	Message  kafkax.Message
	Envelope eventx.Envelope
	Video    *eventx.FeedVideoEvent
	Follow   *eventx.FollowEvent
}

type kafkaHeaderRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewTimelineConsumer(svcCtx *svc.ServiceContext) *TimelineConsumer {
	return &TimelineConsumer{
		svcCtx: svcCtx,
		Logger: logx.WithContext(context.Background()),
	}
}

func (c *TimelineConsumer) Run(ctx context.Context) error {
	return c.svcCtx.Consumer.RunBatch(ctx, c.timelineBatchSize(), c.timelineFlushInterval(), c.HandleBatch)
}

func (c *TimelineConsumer) HandleBatch(ctx context.Context, messages []kafkax.Message) error {
	if len(messages) == 0 {
		return nil
	}
	groups := groupTimelineMessages(messages)
	workerCount := c.workerCount(len(groups))
	jobs := make(chan timelineMessageGroup)
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
					errCh <- fmt.Errorf("handle feed timeline group failed, group:%s size:%d: %w", group.Key, len(group.Messages), err)
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
	if joined == nil {
		logx.WithContext(ctx).Infof("feed timeline batch handled, messages:%d groups:%d workers:%d", len(messages), len(groups), workerCount)
	}
	return joined
}

func (c *TimelineConsumer) handleMessageGroup(ctx context.Context, group timelineMessageGroup) error {
	events := make([]decodedTimelineEvent, 0, len(group.Messages))
	deadLetters := make([]model.DeadLetterEvent, 0)
	for _, message := range group.Messages {
		event, deadLetter, ok := c.decodeMessage(message)
		if !ok {
			deadLetters = append(deadLetters, deadLetter)
			continue
		}
		events = append(events, event)
	}
	if err := c.recordDeadLetters(ctx, deadLetters); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	processed, err := c.loadProcessedEventIDs(ctx, events)
	if err != nil {
		return err
	}
	records := make([]model.ProcessedEvent, 0, len(events))
	for _, event := range events {
		if _, ok := processed[event.Envelope.EventID]; ok {
			continue
		}

		switch event.Kind {
		case timelineEventKindVideo:
			err = c.applyVideoEvent(ctx, *event.Video)
		case timelineEventKindFollow:
			err = c.applyFollowEvent(ctx, *event.Follow)
		default:
			err = fmt.Errorf("unknown timeline event kind: %d", event.Kind)
		}
		if err != nil {
			return fmt.Errorf("apply timeline event failed, event_id:%s: %w", event.Envelope.EventID, err)
		}

		now := time.Now()
		expireAt := now.Add(time.Duration(c.processedEventTTLDays()) * 24 * time.Hour)
		records = append(records, model.ProcessedEvent{
			EventID:      event.Envelope.EventID,
			ConsumerName: c.consumerName(),
			Topic:        event.Message.Topic,
			PartitionNo:  int32(event.Message.Partition),
			OffsetNo:     event.Message.Offset,
			ProcessedAt:  now,
			ExpireAt:     &expireAt,
		})
		// 同一批中若出现重复 event_id，第一次副作用完成后立即在内存中去重。
		processed[event.Envelope.EventID] = struct{}{}
	}
	return c.recordProcessedEvents(ctx, records)
}

func (c *TimelineConsumer) decodeMessage(message kafkax.Message) (decodedTimelineEvent, model.DeadLetterEvent, bool) {
	envelope, err := decodeTimelineEnvelope(message.Value)
	if err != nil {
		return decodedTimelineEvent{}, c.deadLetterFromMessage(message, eventx.Envelope{}, err), false
	}

	switch message.Topic {
	case eventx.TopicFeedVideoEvents:
		event, err := decodeVideoTimelineEvent(envelope)
		if err != nil {
			return decodedTimelineEvent{}, c.deadLetterFromMessage(message, envelope, err), false
		}
		return decodedTimelineEvent{
			Kind: timelineEventKindVideo, Message: message, Envelope: envelope, Video: &event,
		}, model.DeadLetterEvent{}, true
	case eventx.TopicFollowEvents:
		event, err := decodeFollowTimelineEvent(envelope)
		if err != nil {
			return decodedTimelineEvent{}, c.deadLetterFromMessage(message, envelope, err), false
		}
		return decodedTimelineEvent{
			Kind: timelineEventKindFollow, Message: message, Envelope: envelope, Follow: &event,
		}, model.DeadLetterEvent{}, true
	default:
		return decodedTimelineEvent{}, c.deadLetterFromMessage(message, envelope, fmt.Errorf("unexpected topic: %s", message.Topic)), false
	}
}

func decodeTimelineEnvelope(value []byte) (eventx.Envelope, error) {
	if len(value) == 0 {
		return eventx.Envelope{}, errors.New("消息体不能为空")
	}
	var envelope eventx.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return eventx.Envelope{}, fmt.Errorf("解析事件信封失败: %w", err)
	}
	if envelope.EventID == "" || envelope.EventType == "" || envelope.AggregateType == "" || len(envelope.Payload) == 0 {
		return eventx.Envelope{}, errors.New("事件信封缺少必要字段")
	}
	return envelope, nil
}

func decodeVideoTimelineEvent(envelope eventx.Envelope) (eventx.FeedVideoEvent, error) {
	if envelope.AggregateType != eventx.AggregateVideo {
		return eventx.FeedVideoEvent{}, fmt.Errorf("视频事件aggregate_type不正确: %s", envelope.AggregateType)
	}
	expectedAction := ""
	switch envelope.EventType {
	case eventx.EventTypeVideoPublished:
		expectedAction = videoActionPublish
	case eventx.EventTypeVideoDeleted:
		expectedAction = videoActionDelete
	default:
		return eventx.FeedVideoEvent{}, fmt.Errorf("未知视频Feed事件类型: %s", envelope.EventType)
	}

	var event eventx.FeedVideoEvent
	if err := json.Unmarshal(envelope.Payload, &event); err != nil {
		return eventx.FeedVideoEvent{}, fmt.Errorf("解析视频Feed事件失败: %w", err)
	}
	if event.EventID == "" {
		event.EventID = envelope.EventID
	}
	if event.EventID != envelope.EventID {
		return eventx.FeedVideoEvent{}, errors.New("视频事件envelope与payload的event_id不一致")
	}
	if event.Action == "" {
		event.Action = expectedAction
	}
	if event.Action != expectedAction {
		return eventx.FeedVideoEvent{}, errors.New("视频事件event_type与action不一致")
	}
	if event.VideoID == 0 || event.AuthorID == 0 {
		return eventx.FeedVideoEvent{}, errors.New("视频事件缺少video_id或author_id")
	}
	if envelope.AggregateID != "" && envelope.AggregateID != strconv.FormatUint(event.VideoID, 10) {
		return eventx.FeedVideoEvent{}, errors.New("视频事件aggregate_id与video_id不一致")
	}
	if event.OccurredAt <= 0 {
		event.OccurredAt = envelope.OccurredAt
	}
	if event.OccurredAt <= 0 {
		return eventx.FeedVideoEvent{}, errors.New("视频事件缺少occurred_at")
	}
	return event, nil
}

func decodeFollowTimelineEvent(envelope eventx.Envelope) (eventx.FollowEvent, error) {
	if envelope.AggregateType != eventx.AggregateFollow {
		return eventx.FollowEvent{}, fmt.Errorf("关注事件aggregate_type不正确: %s", envelope.AggregateType)
	}
	expectedAction := ""
	switch envelope.EventType {
	case eventx.EventTypeFollowCreated:
		expectedAction = eventx.FollowActionFollow
	case eventx.EventTypeFollowDeleted:
		expectedAction = eventx.FollowActionUnfollow
	default:
		return eventx.FollowEvent{}, fmt.Errorf("未知关注事件类型: %s", envelope.EventType)
	}

	var event eventx.FollowEvent
	if err := json.Unmarshal(envelope.Payload, &event); err != nil {
		return eventx.FollowEvent{}, fmt.Errorf("解析关注事件失败: %w", err)
	}
	if event.EventID == "" {
		event.EventID = envelope.EventID
	}
	if event.EventID != envelope.EventID {
		return eventx.FollowEvent{}, errors.New("关注事件envelope与payload的event_id不一致")
	}
	if event.Action == "" {
		event.Action = expectedAction
	}
	if event.Action != expectedAction {
		return eventx.FollowEvent{}, errors.New("关注事件event_type与action不一致")
	}
	if event.FollowerID == 0 || event.FollowingID == 0 || event.FollowerID == event.FollowingID {
		return eventx.FollowEvent{}, errors.New("关注事件用户ID不合法")
	}
	expectedAggregateID := fmt.Sprintf("%d:%d", event.FollowerID, event.FollowingID)
	if envelope.AggregateID != "" && envelope.AggregateID != expectedAggregateID {
		return eventx.FollowEvent{}, errors.New("关注事件aggregate_id与用户ID不一致")
	}
	if event.OccurredAt <= 0 {
		event.OccurredAt = envelope.OccurredAt
	}
	if event.OccurredAt <= 0 {
		return eventx.FollowEvent{}, errors.New("关注事件缺少occurred_at")
	}
	return event, nil
}

func (c *TimelineConsumer) loadProcessedEventIDs(ctx context.Context, events []decodedTimelineEvent) (map[string]struct{}, error) {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.Envelope.EventID)
	}
	var existing []string
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	if err := c.svcCtx.GormDB.WithContext(dbCtx).
		Model(&model.ProcessedEvent{}).
		Where("consumer_name = ? AND event_id IN ?", c.consumerName(), ids).
		Pluck("event_id", &existing).Error; err != nil {
		return nil, err
	}
	result := make(map[string]struct{}, len(existing))
	for _, eventID := range existing {
		result[eventID] = struct{}{}
	}
	return result, nil
}

func (c *TimelineConsumer) recordProcessedEvents(ctx context.Context, records []model.ProcessedEvent) error {
	if len(records) == 0 {
		return nil
	}
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	return c.svcCtx.GormDB.WithContext(dbCtx).
		Clauses(clause.OnConflict{DoNothing: true}).
		CreateInBatches(&records, defaultTimelineBatchSize).Error
}

func (c *TimelineConsumer) recordDeadLetters(ctx context.Context, records []model.DeadLetterEvent) error {
	if len(records) == 0 {
		return nil
	}
	dbCtx, cancel := context.WithTimeout(ctx, c.dbTimeout())
	defer cancel()
	return c.svcCtx.GormDB.WithContext(dbCtx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "consumer_name"}, {Name: "topic"}, {Name: "partition_no"}, {Name: "offset_no"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"event_id", "event_type", "aggregate_type", "aggregate_id", "reason", "payload", "headers", "updated_at",
		}),
	}).Create(&records).Error
}

func (c *TimelineConsumer) deadLetterFromMessage(message kafkax.Message, envelope eventx.Envelope, cause error) model.DeadLetterEvent {
	now := time.Now()
	return model.DeadLetterEvent{
		ConsumerName:  c.consumerName(),
		Topic:         message.Topic,
		PartitionNo:   int32(message.Partition),
		OffsetNo:      message.Offset,
		EventID:       envelope.EventID,
		EventType:     envelope.EventType,
		AggregateType: envelope.AggregateType,
		AggregateID:   envelope.AggregateID,
		Reason:        truncateTimelineDeadLetterReason(cause.Error()),
		Payload:       string(message.Value),
		Headers:       marshalTimelineKafkaHeaders(message.Headers),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func (c *TimelineConsumer) consumerName() string {
	if c.svcCtx.Config.Kafka.GroupID != "" {
		return c.svcCtx.Config.Kafka.GroupID
	}
	return eventx.ConsumerFeedTimeline
}

func groupTimelineMessages(messages []kafkax.Message) []timelineMessageGroup {
	indexByKey := make(map[string]int)
	groups := make([]timelineMessageGroup, 0)
	for _, message := range messages {
		key := fmt.Sprintf("%s:%d", message.Topic, message.Partition)
		index, ok := indexByKey[key]
		if !ok {
			index = len(groups)
			indexByKey[key] = index
			groups = append(groups, timelineMessageGroup{Key: key})
		}
		groups[index].Messages = append(groups[index].Messages, message)
	}
	return groups
}

func marshalTimelineKafkaHeaders(headers []kafkax.Header) string {
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

func truncateTimelineDeadLetterReason(reason string) string {
	runes := []rune(strings.TrimSpace(reason))
	if len(runes) <= maxDeadLetterReasonRunes {
		return string(runes)
	}
	return string(runes[:maxDeadLetterReasonRunes])
}
