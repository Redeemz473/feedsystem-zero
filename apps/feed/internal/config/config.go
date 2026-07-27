package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	Mysql    MysqlConf
	BizRedis RedisConf
	Timeline TimelineConf
}

type MysqlConf struct {
	DataSource string
}

type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}

// TimelineConf 同时约束分页和冷启动构建，避免 Logic 内散落魔法数字。
type TimelineConf struct {
	DefaultPageSize        int64 `json:",default=20"`
	MaxPageSize            int64 `json:",default=50"`
	GlobalTimelineMaxLen   int64 `json:",default=10000"`
	UserTimelineMaxLen     int64 `json:",default=2000"`
	UserTimelineTTLSeconds int64 `json:",default=2592000"`
	BuildLockTTLSeconds    int64 `json:",default=15"`
	BuildWaitMs            int64 `json:",default=1500"`
	RedisOpTimeoutMs       int64 `json:",default=1000"`
	DBQueryTimeoutMs       int64 `json:",default=3000"`
}
