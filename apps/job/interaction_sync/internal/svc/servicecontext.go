package svc

import (
	"feedsystem-zero/apps/interaction/interactionclient"
	"feedsystem-zero/apps/job/interaction_sync/internal/config"
	"feedsystem-zero/common/gormx"
	"feedsystem-zero/common/kafkax"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config         config.Config
	GormDB         *gorm.DB
	Consumer       *kafkax.Consumer
	InteractionRpc interactionclient.Interaction
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)

	consumer, err := kafkax.NewConsumer(c.Kafka)
	logx.Must(err)

	interactionCli := zrpc.MustNewClient(c.InteractionRpc)

	return &ServiceContext{
		Config:         c,
		GormDB:         db,
		Consumer:       consumer,
		InteractionRpc: interactionclient.NewInteraction(interactionCli),
	}
}

func (s *ServiceContext) Close() {
	if s == nil {
		return
	}
	if s.Consumer != nil {
		if err := s.Consumer.Close(); err != nil {
			logx.Errorf("close kafka consumer failed: %v", err)
		}
	}
	if s.GormDB == nil {
		return
	}
	sqlDB, err := s.GormDB.DB()
	if err != nil {
		logx.Errorf("get interaction sync sql db failed: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		logx.Errorf("close interaction sync sql db failed: %v", err)
	}
}
