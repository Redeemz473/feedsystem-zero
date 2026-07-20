// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"context"
	"time"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/config"
	"feedsystem-zero/apps/gateway/internal/middleware"
	"feedsystem-zero/apps/interaction/interactionclient"
	"feedsystem-zero/apps/video/videoclient"
	"feedsystem-zero/common/gormx"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"gorm.io/gorm"
)

type ServiceContext struct {
	Config            config.Config
	AccountRpc        accountclient.Account
	VideoRpc          videoclient.Video
	InteractionRpc    interactionclient.Interaction
	RedisCli          *redis.Client
	GormDB            *gorm.DB
	TokenAuth         rest.Middleware
	OptionalTokenAuth rest.Middleware
}

func NewServiceContext(c config.Config) *ServiceContext {
	accountCli := zrpc.MustNewClient(c.AccountRpc)
	videoCli := zrpc.MustNewClient(c.VideoRpc)
	interactionCli := zrpc.MustNewClient(c.InteractionRpc)
	redisCli := redis.NewClient(&redis.Options{
		Addr:     c.BizRedis.Addr,
		Password: c.BizRedis.Password,
		DB:       c.BizRedis.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logx.Must(redisCli.Ping(ctx).Err())
	db, err := gormx.NewDB(c.Mysql.DataSource)
	logx.Must(err)

	return &ServiceContext{
		Config:            c,
		AccountRpc:        accountclient.NewAccount(accountCli),
		VideoRpc:          videoclient.NewVideo(videoCli),
		InteractionRpc:    interactionclient.NewInteraction(interactionCli),
		RedisCli:          redisCli,
		GormDB:            db,
		TokenAuth:         middleware.NewTokenAuthMiddleware(redisCli).Handle,
		OptionalTokenAuth: middleware.NewOptionalTokenAuthMiddleware(redisCli, c.Auth.AccessSecret).Handle,
	}
}
