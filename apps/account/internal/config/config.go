package config

import (
	"feedsystem-zero/common/emailx"

	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Mysql    MysqlConf
	BizRedis RedisConf
	Jwt      JwtConf
	Email    emailx.SMTPConf
}

type MysqlConf struct {
	DataSource string
}

type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}

type JwtConf struct {
	AccessSecret string
	AccessExpire int64
}
