package svc

import (
	"context"
	"time"

	"feedsystem-zero/apps/job/notification/internal/config"
	"feedsystem-zero/common/gormx"
	"feedsystem-zero/common/kafkax"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config   config.Config
	GormDB   *gorm.DB
	Consumer *kafkax.Consumer
	// RedisCli 用于事件成功入库后 INCR 用户未读数缓存版本号。
	// Redis 不可用时不阻塞消费；bump 失败只打日志，缓存靠 TTL 自然收敛。
	RedisCli *redis.Client
}

func NewServiceContext(c config.Config) *ServiceContext {
	logx.Must(c.Validate())

	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)
	consumer, err := kafkax.NewConsumer(c.Kafka)
	logx.Must(err)

	rdb := redis.NewClient(&redis.Options{
		Addr:     c.BizRedis.Addr,
		Password: c.BizRedis.Password,
		DB:       c.BizRedis.DB,
	})
	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logx.Must(rdb.Ping(pingCtx).Err())

	return &ServiceContext{
		Config:   c,
		GormDB:   db,
		Consumer: consumer,
		RedisCli: rdb,
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
	if s.RedisCli != nil {
		if err := s.RedisCli.Close(); err != nil {
			logx.Errorf("close notification redis failed: %v", err)
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
