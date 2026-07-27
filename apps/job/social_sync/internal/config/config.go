package config

import (
	"feedsystem-zero/common/kafkax"
)

// Config 是 social-sync-job 的配置。
// 该 job 消费 social.follow.events，负责：
//   - 幂等标记 processed_events；
//   - 用事件里的最终状态回补 Redis 缓存，兜住 Follow/Unfollow 事务提交后
//     applyFollowCacheAfterCommit 失败的场景；
//   - 解码/处理失败写入 dead_letter_events，避免同一分区卡死。
type Config struct {
	Name     string
	Mysql    MysqlConf
	BizRedis RedisConf
	Kafka    kafkax.ConsumerConf
	Sync     SyncConf
}

type MysqlConf struct {
	DataSource string
}

// RedisConf 与 apps/social 侧保持一致，直接连接同一实例。
type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}

// SyncConf 与 interaction-sync 对齐，方便运维统一调优。
type SyncConf struct {
	BatchSize         int `json:",default=100"`
	FlushMs           int `json:",default=1000"`
	FlushBatchSize    int `json:",default=500"`
	WorkerCount       int `json:",default=4"`
	MaxEventRetry     int `json:",default=3"`
	RetryBackoffMs    int `json:",default=200"`
	MaxRetryBackoffMs int `json:",default=2000"`
	RedisOpTimeoutMs  int `json:",default=500"`
}
