package config

import (
	"feedsystem-zero/common/kafkax"

	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	Name           string
	Mysql          MysqlConf
	Kafka          kafkax.ConsumerConf
	InteractionRpc zrpc.RpcClientConf
	Sync           SyncConf
}

type MysqlConf struct {
	DataSource string
}

type SyncConf struct {
	BatchSize         int `json:",default=500"`
	FlushMs           int `json:",default=1000"`
	FlushBatchSize    int `json:",default=500"`
	RpcTimeoutMs      int `json:",default=10000"`
	WorkerCount       int `json:",default=4"`
	MaxEventRetry     int `json:",default=3"`
	RetryBackoffMs    int `json:",default=200"`
	MaxRetryBackoffMs int `json:",default=2000"`
}
