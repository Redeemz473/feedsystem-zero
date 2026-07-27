package config

import "feedsystem-zero/common/kafkax"

// Config 是 feed-timeline-job 的运行配置。
type Config struct {
	Name     string
	Mysql    MysqlConf
	BizRedis RedisConf
	Kafka    kafkax.ConsumerConf
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

type TimelineConf struct {
	BatchSize                int   `json:",default=100"`
	FlushMs                  int   `json:",default=1000"`
	WorkerCount              int   `json:",default=4"`
	FollowerQueryBatchSize   int   `json:",default=500"`
	GlobalTimelineMaxLen     int64 `json:",default=10000"`
	UserTimelineMaxLen       int64 `json:",default=2000"`
	UserTimelineTTLSeconds   int64 `json:",default=2592000"`
	FollowBackfillVideoLimit int   `json:",default=100"`
	RedisOpTimeoutMs         int   `json:",default=3000"`
	DBQueryTimeoutMs         int   `json:",default=5000"`
	ProcessedEventTTLDays    int   `json:",default=14"`
}
