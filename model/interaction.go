package model

import "time"

const (
	VideoStatusNormal  int32 = 1
	VideoStatusDeleted int32 = 2
	VideoStatusBlocked int32 = 3
)

const (
	LikeStatusActive  int32 = 1
	LikeStatusDeleted int32 = 2
)

const (
	CommentStatusNormal  int32 = 1
	CommentStatusDeleted int32 = 2
)

type Like struct {
	ID        uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	VideoID   uint64     `gorm:"column:video_id"`
	UserID    uint64     `gorm:"column:user_id"`
	Status    int32      `gorm:"column:status"`
	DeletedAt *time.Time `gorm:"column:deleted_at"`
	CreatedAt time.Time  `gorm:"column:created_at"`
	UpdatedAt time.Time  `gorm:"column:updated_at"`
}

func (Like) TableName() string {
	return "likes"
}

type Comment struct {
	ID        uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	VideoID   uint64     `gorm:"column:video_id"`
	UserID    uint64     `gorm:"column:user_id"`
	Username  string     `gorm:"column:username"`
	Content   string     `gorm:"column:content"`
	RequestID string     `gorm:"column:request_id"`
	Status    int32      `gorm:"column:status"`
	DeletedAt *time.Time `gorm:"column:deleted_at"`
	CreatedAt time.Time  `gorm:"column:created_at"`
	UpdatedAt time.Time  `gorm:"column:updated_at"`
}

func (Comment) TableName() string {
	return "comments"
}

type VideoStat struct {
	ID            uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	LikesCount    int64  `gorm:"column:likes_count"`
	CommentsCount int64  `gorm:"column:comments_count"`
	Popularity    int64  `gorm:"column:popularity"`
}

func (VideoStat) TableName() string {
	return "videos"
}
