package svc

import (
	"feedsystem-zero/apps/job/asset_cleanup/internal/config"
	"feedsystem-zero/common/gormx"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config   config.Config
	GormDB   *gorm.DB
	RedisCli *redis.Client
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)

	return &ServiceContext{
		Config: c,
		GormDB: db,
		RedisCli: redis.NewClient(&redis.Options{
			Addr:     c.BizRedis.Addr,
			Password: c.BizRedis.Password,
			DB:       c.BizRedis.DB,
		}),
	}
}

func (s *ServiceContext) Close() {
	if s == nil {
		return
	}
	if s.RedisCli != nil {
		if err := s.RedisCli.Close(); err != nil {
			logx.Errorf("close asset cleanup redis client failed: %v", err)
		}
	}
	if s.GormDB == nil {
		return
	}
	sqlDB, err := s.GormDB.DB()
	if err != nil {
		logx.Errorf("get asset cleanup sql db failed: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil {
		logx.Errorf("close asset cleanup sql db failed: %v", err)
	}
}
