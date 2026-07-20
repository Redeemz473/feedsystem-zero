package svc

import (
	"feedsystem-zero/apps/job/outbox/internal/config"
	"feedsystem-zero/common/gormx"
	"feedsystem-zero/common/kafkax"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config   config.Config
	GormDB   *gorm.DB
	Producer *kafkax.Producer
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)

	producer, err := kafkax.NewProducer(c.Kafka)
	logx.Must(err)

	return &ServiceContext{
		Config:   c,
		GormDB:   db,
		Producer: producer,
	}
}

func (s *ServiceContext) Close() {
	if s == nil || s.Producer == nil {
		return
	}
	if err := s.Producer.Close(); err != nil {
		logx.Errorf("close kafka producer failed: %v", err)
	}
}
