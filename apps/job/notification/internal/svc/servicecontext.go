package svc

import (
	"feedsystem-zero/apps/job/notification/internal/config"
	"feedsystem-zero/common/gormx"
	"feedsystem-zero/common/kafkax"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config   config.Config
	GormDB   *gorm.DB
	Consumer *kafkax.Consumer
}

func NewServiceContext(c config.Config) *ServiceContext {
	logx.Must(c.Validate())

	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)
	consumer, err := kafkax.NewConsumer(c.Kafka)
	logx.Must(err)

	return &ServiceContext{
		Config:   c,
		GormDB:   db,
		Consumer: consumer,
	}
}

func (s *ServiceContext) Close() {
	if s == nil {
		return
	}
	if s.Consumer != nil {
		if err := s.Consumer.Close(); err != nil {
			logx.Errorf("close notification kafka consumer failed: %v", err)
		}
	}
	if s.GormDB == nil {
		return
	}
	sqlDB, err := s.GormDB.DB()
	if err != nil {
		logx.Errorf("get notification sql db failed: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		logx.Errorf("close notification sql db failed: %v", err)
	}
}
