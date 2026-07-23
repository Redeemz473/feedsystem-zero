package model

import (
	"encoding/json"
	"time"

	"feedsystem-zero/common/eventx"
)

// OutboxStatus* 与 interaction 保持一致，统一 outbox 状态机。
const (
	OutboxStatusPending    int32 = 1
	OutboxStatusSent       int32 = 2
	OutboxStatusFailed     int32 = 3
	OutboxStatusDead       int32 = 4
	OutboxStatusProcessing int32 = 5
)

// OutboxEvent 与 common 约定一致，关注事件(key=TopicFollowEvents)通过它异步分发。
// 业务逻辑里写关注状态时，应在同一 DB 事务内插入 OutboxEvent，
// 由 outbox job 扫描后投递到 Kafka，下游(如 feed-timeline-job / notification-job)消费。
type OutboxEvent struct {
	ID            uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	EventID       string     `gorm:"column:event_id"`
	Topic         string     `gorm:"column:topic"`
	EventType     string     `gorm:"column:event_type"`
	AggregateType string     `gorm:"column:aggregate_type"`
	AggregateID   string     `gorm:"column:aggregate_id"`
	Payload       string     `gorm:"column:payload"`
	Status        int32      `gorm:"column:status"`
	LockToken     string     `gorm:"column:lock_token"`
	LockedBy      string     `gorm:"column:locked_by"`
	LockedAt      *time.Time `gorm:"column:locked_at"`
	RetryCount    int32      `gorm:"column:retry_count"`
	NextRetryAt   *time.Time `gorm:"column:next_retry_at"`
	SentAt        *time.Time `gorm:"column:sent_at"`
	LastError     string     `gorm:"column:last_error"`
	CreatedAt     time.Time  `gorm:"column:created_at"`
	UpdatedAt     time.Time  `gorm:"column:updated_at"`
}

func (OutboxEvent) TableName() string {
	return "outbox_events"
}

// ProcessedEvent 消费者幂等表。关注事件的下游消费者(feed/notification)
// 用 (consumer_name, event_id) 唯一键去重，避免重复处理。
type ProcessedEvent struct {
	ID           uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	EventID      string     `gorm:"column:event_id"`
	ConsumerName string     `gorm:"column:consumer_name"`
	Topic        string     `gorm:"column:topic"`
	PartitionNo  int32      `gorm:"column:partition_no"`
	OffsetNo     int64      `gorm:"column:offset_no"`
	ProcessedAt  time.Time  `gorm:"column:processed_at"`
	ExpireAt     *time.Time `gorm:"column:expire_at"`
}

func (ProcessedEvent) TableName() string {
	return "processed_events"
}

// BuildFollowPayload 构造关注事件 payload（与 eventx 约定对齐）。
// 业务逻辑在写 outbox 时调用，避免各 logic 自行拼 JSON。
func BuildFollowPayload(eventID string, followerID, followingID uint64, action string, occurredAt int64) ([]byte, error) {
	p := eventx.FollowEvent{
		EventID:     eventID,
		FollowerID:  followerID,
		FollowingID: followingID,
		Action:      action,
		OccurredAt:  occurredAt,
	}
	return json.Marshal(p)
}
