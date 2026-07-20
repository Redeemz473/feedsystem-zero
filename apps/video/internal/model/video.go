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

type Video struct {
	ID             uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	AuthorID       uint64     `gorm:"column:author_id"`
	AuthorUsername string     `gorm:"column:author_username"`
	Title          string     `gorm:"column:title"`
	Description    string     `gorm:"column:description"`
	PlayURL        string     `gorm:"column:play_url"`
	CoverURL       string     `gorm:"column:cover_url"`
	LikesCount     int64      `gorm:"column:likes_count"`
	CommentsCount  int64      `gorm:"column:comments_count"`
	Popularity     int64      `gorm:"column:popularity"`
	Status         int32      `gorm:"column:status"`
	DeletedAt      *time.Time `gorm:"column:deleted_at"`
	CreatedAt      time.Time  `gorm:"column:created_at"`
	UpdatedAt      time.Time  `gorm:"column:updated_at"`
}

func (Video) TableName() string {
	return "videos"
}

type Tag struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (Tag) TableName() string {
	return "tags"
}

type VideoTag struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	VideoID   uint64    `gorm:"column:video_id"`
	TagID     uint64    `gorm:"column:tag_id"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func (VideoTag) TableName() string {
	return "video_tags"
}

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
