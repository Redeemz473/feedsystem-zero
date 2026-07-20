package config

import (
	"feedsystem-zero/common/kafkax"

	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Mysql    MysqlConf
	BizRedis RedisConf
	Kafka    kafkax.ProducerConf
}

type MysqlConf struct {
	DataSource string
}

type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}
