package model

import "time"

const (
	VideoStatusNormal  int32 = 1
	FollowStatusActive int32 = 1
)

type Video struct {
	ID        uint64     `gorm:"column:id;primaryKey"`
	AuthorID  uint64     `gorm:"column:author_id"`
	Status    int32      `gorm:"column:status"`
	DeletedAt *time.Time `gorm:"column:deleted_at"`
	CreatedAt time.Time  `gorm:"column:created_at"`
}

func (Video) TableName() string {
	return "videos"
}

type Follow struct {
	ID          uint64     `gorm:"column:id;primaryKey"`
	FollowerID  uint64     `gorm:"column:follower_id"`
	FollowingID uint64     `gorm:"column:following_id"`
	Status      int32      `gorm:"column:status"`
	UpdatedAt   time.Time  `gorm:"column:updated_at"`
	DeletedAt   *time.Time `gorm:"column:deleted_at"`
}

func (Follow) TableName() string {
	return "follows"
}

// Account 是 Feed 判定大 V 时使用的最小只读投影。
// 依赖 accounts.is_big_v 只升不降标记位，一次主键查询即可完成判定；
// follower_count 仅在需要排序或诊断时使用，不再参与写侧 fanout 判定。
type Account struct {
	ID            uint64 `gorm:"column:id;primaryKey"`
	FollowerCount int64  `gorm:"column:follower_count"`
	IsBigV        bool   `gorm:"column:is_big_v"`
}

func (Account) TableName() string {
	return "accounts"
}

// ProcessedEvent 在 Redis 副作用完成后写入。
// Timeline 的 ZADD/ZREM 天然幂等，因此进程在“写 Redis 后、写幂等表前”崩溃时，
// Kafka 重投只会重复执行同一最终状态，不会产生重复数据。
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
