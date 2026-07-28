package model

import "time"

const (
	NotificationStatusUnread   int32 = 1
	NotificationStatusRead     int32 = 2
	NotificationStatusCanceled int32 = 3
)

type Notification struct {
	ID               uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	BusinessKey      string     `gorm:"column:business_key"`
	SourceEventID    string     `gorm:"column:source_event_id"`
	ReceiverID       uint64     `gorm:"column:receiver_id"`
	ActorID          uint64     `gorm:"column:actor_id"`
	NotificationType string     `gorm:"column:notification_type"`
	VideoID          *uint64    `gorm:"column:video_id"`
	CommentID        *uint64    `gorm:"column:comment_id"`
	Status           int32      `gorm:"column:status"`
	OccurredAt       time.Time  `gorm:"column:occurred_at"`
	ReadAt           *time.Time `gorm:"column:read_at"`
	DeletedAt        *time.Time `gorm:"column:deleted_at"`
	CreatedAt        time.Time  `gorm:"column:created_at"`
	UpdatedAt        time.Time  `gorm:"column:updated_at"`
}

func (Notification) TableName() string {
	return "notifications"
}
