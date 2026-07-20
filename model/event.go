package model

import "time"

const (
	OutboxStatusPending    int32 = 1
	OutboxStatusSent       int32 = 2
	OutboxStatusFailed     int32 = 3
	OutboxStatusDead       int32 = 4
	OutboxStatusProcessing int32 = 5
)

type InteractionEvent struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	EventID   string    `gorm:"column:event_id"`
	EventType string    `gorm:"column:event_type"`
	VideoID   uint64    `gorm:"column:video_id"`
	UserID    uint64    `gorm:"column:user_id"`
	CommentID uint64    `gorm:"column:comment_id"`
	Action    string    `gorm:"column:action"`
	Delta     int64     `gorm:"column:delta"`
	RequestID string    `gorm:"column:request_id"`
	Payload   string    `gorm:"column:payload"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (InteractionEvent) TableName() string {
	return "interaction_events"
}

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
