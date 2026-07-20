package kafkax

import "time"

type Header struct {
	Key   string
	Value []byte
}

// Message 是项目内部使用的 Kafka 消息结构，避免业务代码直接依赖 kafka-go 的类型。
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []Header
	Time      time.Time
	Partition int
	Offset    int64
}
