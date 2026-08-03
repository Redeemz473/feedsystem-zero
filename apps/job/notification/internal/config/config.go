package config

import (
	"fmt"

	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
)

type Config struct {
	Name         string
	Mysql        MysqlConf
	BizRedis     RedisConf
	Kafka        kafkax.ConsumerConf
	Notification NotificationConf
}

type MysqlConf struct {
	DataSource string
}

// RedisConf 是通知未读数缓存使用的 Redis 连接配置。
// notification-job 事件处理成功后需要 INCR 用户的未读数 version key，
// 让 rpc 侧的缓存自然失效。
type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}

type NotificationConf struct {
	BatchSize              int   `json:",default=100"`
	FlushMs                int   `json:",default=1000"`
	WorkerCount            int   `json:",default=4"`
	DBWriteTimeoutMs       int   `json:",default=5000"`
	DBMaxRetries           int   `json:",default=3"`
	DBRetryBaseMs          int   `json:",default=20"`
	DBRetryMaxMs           int   `json:",default=200"`
	ProcessedEventTTLDays  int   `json:",default=14"`
	FutureToleranceSeconds int64 `json:",default=300"`
}

func (c Config) Validate() error {
	if len(c.Kafka.Topics) != 1 || c.Kafka.Topics[0] != eventx.TopicNotificationEvents {
		return fmt.Errorf("notification-job必须且只能订阅%s", eventx.TopicNotificationEvents)
	}
	return nil
}
