package model

import "time"

// DeadLetterEvent 与 interaction_sync 中的结构对齐，共用 dead_letter_events 表。
// 唯一键 (consumer_name, topic, partition_no, offset_no) 保证同一条卡死消息
// 只保留一条死信记录。
type DeadLetterEvent struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	ConsumerName  string    `gorm:"column:consumer_name"`
	Topic         string    `gorm:"column:topic"`
	PartitionNo   int32     `gorm:"column:partition_no"`
	OffsetNo      int64     `gorm:"column:offset_no"`
	EventID       string    `gorm:"column:event_id"`
	EventType     string    `gorm:"column:event_type"`
	AggregateType string    `gorm:"column:aggregate_type"`
	AggregateID   string    `gorm:"column:aggregate_id"`
	Reason        string    `gorm:"column:reason"`
	Payload       string    `gorm:"column:payload"`
	Headers       string    `gorm:"column:headers"`
	CreatedAt     time.Time `gorm:"column:created_at"`
	UpdatedAt     time.Time `gorm:"column:updated_at"`
}

func (DeadLetterEvent) TableName() string {
	return "dead_letter_events"
}

// ProcessedEvent 幂等表。social-sync-job 用 (event_id, consumer_name)
// 唯一键在批处理事务中占位，Kafka 重投也不会重复执行副作用。
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
