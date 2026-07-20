package config

import (
	"feedsystem-zero/common/kafkax"
)

type Config struct {
	Name   string
	Mysql  MysqlConf
	Kafka  kafkax.ProducerConf
	Outbox OutboxConf
}

type MysqlConf struct {
	DataSource string
}

type OutboxConf struct {
	BatchSize           int `json:",default=100"`
	WorkerCount         int `json:",default=4"`
	PollIntervalMs      int `json:",default=1000"`
	MaxRetry            int `json:",default=10"`
	RetryBaseMs         int `json:",default=1000"`
	RetryMaxMs          int `json:",default=60000"`
	ClaimTimeoutSeconds int `json:",default=60"`
	EventTimeoutMs      int `json:",default=10000"`
	PublishTimeoutMs    int `json:",default=5000"`
}
