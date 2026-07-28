package config

import (
	"fmt"

	"feedsystem-zero/common/eventx"
	"feedsystem-zero/common/kafkax"
)

// Config 是 hotrank-job 的运行配置。
// 该任务只消费互动事件并写入 Redis 分钟窗口；热榜快照的合并和分页由 feed-rpc 完成。
type Config struct {
	Name     string
	Mysql    MysqlConf
	BizRedis RedisConf
	Kafka    kafkax.ConsumerConf
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

type HotRankConf struct {
	BatchSize              int   `json:",default=100"`
	FlushMs                int   `json:",default=1000"`
	WorkerCount            int   `json:",default=4"`
	WindowRetentionSeconds int64 `json:",default=7200"`
	ProcessedEventTTLDays  int   `json:",default=14"`
	RedisOpTimeoutMs       int   `json:",default=1000"`
	DBWriteTimeoutMs       int   `json:",default=3000"`
	FutureToleranceSeconds int64 `json:",default=300"`
}

// Validate 防止 topic 漏配或误配。HotRank 必须完整订阅点赞、评论两个事件流，
// 也不能把无关 topic 当作毒消息消费。
func (c Config) Validate() error {
	requiredTopics := map[string]bool{
		eventx.TopicInteractionLikeEvents:    false,
		eventx.TopicInteractionCommentEvents: false,
	}
	for _, topic := range c.Kafka.Topics {
		seen, ok := requiredTopics[topic]
		if !ok {
			return fmt.Errorf("hotrank 不支持 Kafka topic: %s", topic)
		}
		if seen {
			return fmt.Errorf("hotrank Kafka topic 重复配置: %s", topic)
		}
		requiredTopics[topic] = true
	}
	for topic, configured := range requiredTopics {
		if !configured {
			return fmt.Errorf("hotrank 缺少 Kafka topic: %s", topic)
		}
	}
	return nil
}
