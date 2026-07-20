package model

import "time"

type Account struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Username     string    `gorm:"column:username"`
	PasswordHash string    `gorm:"column:password_hash"`
	Email        string    `gorm:"column:email"`
	RefreshToken *string   `gorm:"column:refresh_token"`
	AvatarURL    string    `gorm:"column:avatar_url"`
	Bio          string    `gorm:"column:bio"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

func (Account) TableName() string {
	return "accounts"
}
