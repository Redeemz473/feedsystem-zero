package svc

import (
	"context"
	"time"

	"feedsystem-zero/apps/account/internal/config"
	"feedsystem-zero/common/gormx"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ServiceContext 服务上下文，把 config、DB、Redis、Model 等依赖注入到一起
// account-rpc 的所有 logic 都通过 l.svcCtx 拿到这些资源
type ServiceContext struct {
	Config   config.Config
	GormDB   *gorm.DB
	RedisCli *redis.Client
}

func NewServiceContext(c config.Config) *ServiceContext {
	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)

	rdb := redis.NewClient(&redis.Options{
		Addr:     c.BizRedis.Addr,
		Password: c.BizRedis.Password,
		DB:       c.BizRedis.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logx.Must(rdb.Ping(ctx).Err())

	return &ServiceContext{
		Config:   c,
		GormDB:   db,
		RedisCli: rdb,
	}
}
