package config

import (
	"fmt"

	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
)

type Config struct {
	Name         string
	Mysql        MysqlConf
	Kafka        kafkax.ConsumerConf
	Notification NotificationConf
}

type MysqlConf struct {
	DataSource string
}

type NotificationConf struct {
	BatchSize              int   `json:",default=100"`
	FlushMs                int   `json:",default=1000"`
	WorkerCount            int   `json:",default=4"`
	DBWriteTimeoutMs       int   `json:",default=5000"`
	ProcessedEventTTLDays  int   `json:",default=14"`
	FutureToleranceSeconds int64 `json:",default=300"`
}

func (c Config) Validate() error {
	if len(c.Kafka.Topics) != 1 || c.Kafka.Topics[0] != eventx.TopicNotificationEvents {
		return fmt.Errorf("notification-job必须且只能订阅%s", eventx.TopicNotificationEvents)
	}
	return nil
}
