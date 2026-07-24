package model

import "time"

// 单向关注关系状态。
// 采用软删除语义：取关不物理删除行，而是把 Status 置为 FollowStatusDeleted，
// 这样 Follow 记录本身可作为"曾经关注过"的审计，也方便后续统计。
const (
	FollowStatusActive  int32 = 1
	FollowStatusDeleted int32 = 2
)

// Follow 表示一条单向关注关系：
// follower_id 主动关注了 following_id（类似微博/抖音，不要求互关）。
type Follow struct {
	ID          uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	FollowerID  uint64     `gorm:"column:follower_id"`
	FollowingID uint64     `gorm:"column:following_id"`
	Status      int32      `gorm:"column:status"`
	CreatedAt   time.Time  `gorm:"column:created_at"`
	UpdatedAt   time.Time  `gorm:"column:updated_at"`
	DeletedAt   *time.Time `gorm:"column:deleted_at"`
}

// TableName 指定关注关系表名。
func (Follow) TableName() string {
	return "follows"
}

// 供业务逻辑参考的索引（在 deploy/sql 的迁移中创建，不在此处自动建索引）：
//   UNIQUE KEY uk_follower_following (follower_id, following_id)
//   KEY idx_following_status_updated (following_id, status, updated_at, id)
//   KEY idx_follower_status_updated (follower_id, status, updated_at, id)
