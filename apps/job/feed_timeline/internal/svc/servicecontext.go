package svc

import (
	"context"
	"time"

	"feedsystem-zero/apps/job/feed_timeline/internal/config"
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logx.Must(rdb.Ping(ctx).Err())

	return &ServiceContext{Config: c, GormDB: db, RedisCli: rdb, Consumer: consumer}
}

func (s *ServiceContext) Close() {
	if s == nil {
		return
	}
	if s.Consumer != nil {
		if err := s.Consumer.Close(); err != nil {
			logx.Errorf("close feed timeline kafka consumer failed: %v", err)
		}
	}
	if s.RedisCli != nil {
		if err := s.RedisCli.Close(); err != nil {
			logx.Errorf("close feed timeline redis failed: %v", err)
		}
	}
	if s.GormDB == nil {
		return
	}
	sqlDB, err := s.GormDB.DB()
	if err != nil {
		logx.Errorf("get feed timeline sql db failed: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		logx.Errorf("close feed timeline sql db failed: %v", err)
	}
}
