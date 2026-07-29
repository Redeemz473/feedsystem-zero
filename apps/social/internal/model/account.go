package model

// Account 是 social 模块私有的最小账户模型，仅用于关注/取关事务内维护
// accounts 表的 follower_count / following_count 冗余计数，避免依赖 account 模块的完整 model。
//
// IsBigV 表示作者是否已升级为大 V，只升不降：粉丝数首次达到
// feedx.BigCreatorFollowerThreshold 时由本模块在同一事务内置为 true。
// 该字段用于 Feed 推拉分离决策，避免直接比较 follower_count 引发的阈值反向穿越。
type Account struct {
	ID             uint64 `gorm:"column:id;primaryKey"`
	FollowerCount  int64  `gorm:"column:follower_count"`
	FollowingCount int64  `gorm:"column:following_count"`
	IsBigV         bool   `gorm:"column:is_big_v"`
}

func (Account) TableName() string {
	return "accounts"
}
