package model

// Account 是 social 模块私有的最小账户模型，仅用于关注/取关事务内维护
// accounts 表的 follower_count / following_count 冗余计数，避免依赖 account 模块的完整 model。
type Account struct {
	ID             uint64 `gorm:"column:id;primaryKey"`
	FollowerCount  int64  `gorm:"column:follower_count"`
	FollowingCount int64  `gorm:"column:following_count"`
}

func (Account) TableName() string {
	return "accounts"
}
