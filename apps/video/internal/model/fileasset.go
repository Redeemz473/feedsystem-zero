package model

import "time"

const (
	FileAssetStatusActive        int32 = 1
	FileAssetStatusPendingDelete int32 = 2
	FileAssetStatusDeleted       int32 = 3
)

const (
	FileAssetTypeVideo = "video"
	FileAssetTypeCover = "cover"
)

const (
	OutboxStatusPending    int32 = 1
	OutboxStatusSent       int32 = 2
	OutboxStatusFailed     int32 = 3
	OutboxStatusDead       int32 = 4
	OutboxStatusProcessing int32 = 5
)

type FileAsset struct {
	ID          uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	FileHash    string     `gorm:"column:file_hash"`
	FileType    string     `gorm:"column:file_type"`
	URL         string     `gorm:"column:url"`
	StoragePath string     `gorm:"column:storage_path"`
	Size        int64      `gorm:"column:size"`
	RefCount    int64      `gorm:"column:ref_count"`
	Status      int32      `gorm:"column:status"`
	DeletedAt   *time.Time `gorm:"column:deleted_at"`
	CreatedAt   time.Time  `gorm:"column:created_at"`
	UpdatedAt   time.Time  `gorm:"column:updated_at"`
}

func (FileAsset) TableName() string {
	return "file_assets"
}

// OutboxEvent 与 outbox_events 表结构一致。
// video-rpc 在自己的本地事务里写入 outbox_events，由独立的 job/outbox 异步投递到 Kafka。
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
