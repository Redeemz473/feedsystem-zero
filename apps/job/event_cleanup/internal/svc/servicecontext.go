package svc

import (
	"feedsystem-zero/apps/job/event_cleanup/internal/config"
	"feedsystem-zero/common/gormx"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config config.Config
	GormDB *gorm.DB
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)
	return &ServiceContext{Config: c, GormDB: db}
}

func (s *ServiceContext) Close() {
	if s == nil || s.GormDB == nil {
		return
	}
	sqlDB, err := s.GormDB.DB()
	if err != nil {
		logx.Errorf("get event cleanup sql db failed: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		logx.Errorf("close event cleanup sql db failed: %v", err)
	}
}
