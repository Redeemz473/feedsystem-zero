package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	Mysql    MysqlConf
	BizRedis RedisConf
	Timeline TimelineConf
	HotRank  HotRankConf
}

type MysqlConf struct {
	DataSource string
}

type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}

// TimelineConf 同时约束分页和冷启动构建。
type TimelineConf struct {
	DefaultPageSize        int64 `json:",default=20"`
	MaxPageSize            int64 `json:",default=50"`
	GlobalTimelineMaxLen   int64 `json:",default=10000"`
	UserTimelineMaxLen     int64 `json:",default=2000"`
	UserTimelineTTLSeconds int64 `json:",default=2592000"`
	AuthorOutboxMaxLen     int64 `json:",default=500"`
	AuthorOutboxTTLSeconds int64 `json:",default=2592000"`
	MaxBigCreatorFanIn     int   `json:",default=100"`
	BuildLockTTLSeconds    int64 `json:",default=15"`
	BuildWaitMs            int64 `json:",default=1500"`
	RedisOpTimeoutMs       int64 `json:",default=1000"`
	DBQueryTimeoutMs       int64 `json:",default=3000"`
}

// HotRankConf 约束热榜窗口合并、快照生命周期和分页上限。
// WindowMinutes 对应的时长加上 MaxSnapshotAgeSeconds，必须小于 hotrank-job
// 的窗口保留时间，否则较旧快照无法收集到完整的分钟窗口。
type HotRankConf struct {
	WindowMinutes          int64 `json:",default=60"`
	MaxRankSize            int64 `json:",default=1000"`
	DecayHalfLifeMinutes   int64 `json:",default=30"`
	SnapshotTTLSeconds     int64 `json:",default=1800"`
	MaxSnapshotAgeSeconds  int64 `json:",default=1800"`
	BuildLockTTLSeconds    int64 `json:",default=10"`
	BuildWaitMs            int64 `json:",default=1200"`
	RedisOpTimeoutMs       int64 `json:",default=1000"`
	FutureToleranceSeconds int64 `json:",default=300"`
}
