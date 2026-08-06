package model

import "time"

const (
	FileAssetStatusActive        int32 = 1
	FileAssetStatusPendingDelete int32 = 2
	FileAssetStatusDeleted       int32 = 3
	FileAssetStatusCleaning      int32 = 4
)

const VideoStatusNormal int32 = 1

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
