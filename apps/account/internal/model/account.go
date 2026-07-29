package model

import "time"

type Account struct {
	ID           uint64  `gorm:"column:id;primaryKey;autoIncrement"`
	Username     string  `gorm:"column:username"`
	PasswordHash string  `gorm:"column:password_hash"`
	Email        string  `gorm:"column:email"`
	RefreshToken *string `gorm:"column:refresh_token"`
	AvatarURL    string  `gorm:"column:avatar_url"`
	Bio          string  `gorm:"column:bio"`
	// FollowerCount / FollowingCount 是冗余计数，由 social 模块在关注/取关事务中维护，
	// 与 follows 表保持最终一致；读取走 accounts 表，避免 COUNT 查询。
	FollowerCount  int64 `gorm:"column:follower_count"`
	FollowingCount int64 `gorm:"column:following_count"`
	// IsBigV 表示作者是否已升级为大 V，只升不降，由 social 模块在关注事务内维护。
	// 用于 Feed 推拉分离决策，避免直接依赖 follower_count 的阈值反向穿越问题。
	IsBigV    bool      `gorm:"column:is_big_v"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

func (Account) TableName() string {
	return "accounts"
}
