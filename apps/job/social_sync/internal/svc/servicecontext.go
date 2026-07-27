package svc

import (
	"context"
	"time"

	"feedsystem-zero/apps/job/social_sync/internal/config"
	"feedsystem-zero/common/gormx"
	"feedsystem-zero/common/kafkax"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config   config.Config
	GormDB   *gorm.DB
	RedisCli *redis.Client
	Consumer *kafkax.Consumer
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)

	consumer, err := kafkax.NewConsumer(c.Kafka)
	logx.Must(err)

	rdb := redis.NewClient(&redis.Options{
		Addr:     c.BizRedis.Addr,
		Password: c.BizRedis.Password,
		DB:       c.BizRedis.DB,
	})

	// 与 social-rpc 保持一致：启动时先 Ping 探活，避免用错的 Redis 地址跑起来后
	// 事件缓存补偿全部静默失败。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logx.Must(rdb.Ping(ctx).Err())

	return &ServiceContext{
		Config:   c,
		GormDB:   db,
		RedisCli: rdb,
		Consumer: consumer,
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
	if s.RedisCli != nil {
		if err := s.RedisCli.Close(); err != nil {
			logx.Errorf("close redis client failed: %v", err)
		}
	}
	if s.GormDB == nil {
		return
	}
	sqlDB, err := s.GormDB.DB()
	if err != nil {
		logx.Errorf("get social sync sql db failed: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		logx.Errorf("close social sync sql db failed: %v", err)
	}
}
