package model

import "time"

const (
	VideoStatusNormal  int32 = 1
	FollowStatusActive int32 = 1
)

// Video 是 Feed 冷启动构建所需的最小只读投影。
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

// Follow 是用户关注流冷启动和 Timeline fanout 使用的最小只读投影。
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
// 保留 follower_count 便于排序（fan-in 优先保留粉丝多的大 V）。
type Account struct {
	ID            uint64 `gorm:"column:id;primaryKey"`
	FollowerCount int64  `gorm:"column:follower_count"`
	IsBigV        bool   `gorm:"column:is_big_v"`
}

func (Account) TableName() string {
	return "accounts"
}
