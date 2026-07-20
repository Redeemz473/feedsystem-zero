package model

import "time"

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
