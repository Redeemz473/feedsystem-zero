package kafkax

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
)

const defaultConsumerHandlerRetryInterval = time.Second

type Handler func(ctx context.Context, msg Message) error

type BatchHandler func(ctx context.Context, messages []Message) error

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(conf ConsumerConf) (*Consumer, error) {
	conf = conf.Normalize()
	if len(conf.Brokers) == 0 {
		return nil, errors.New("kafka brokers is empty")
	}
	if conf.GroupID == "" {
		return nil, errors.New("kafka group id is empty")
	}
	if len(conf.Topics) == 0 {
		return nil, errors.New("kafka topics is empty")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        conf.Brokers,
		GroupID:        conf.GroupID,
		GroupTopics:    conf.Topics,
		MinBytes:       conf.MinBytes,
		MaxBytes:       conf.MaxBytes,
		MaxWait:        time.Duration(conf.MaxWaitMs) * time.Millisecond,
		CommitInterval: time.Duration(conf.CommitInterval) * time.Millisecond,
	})

	return &Consumer{reader: reader}, nil
}

func MustNewConsumer(conf ConsumerConf) *Consumer {
	consumer, err := NewConsumer(conf)
	if err != nil {
		panic(err)
	}
	return consumer
}

// Run 按单条消息消费。handler 成功后才提交 offset，失败则不提交，等待 Kafka 重投。
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	for {
		rawMsg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		msg := fromKafkaMessage(rawMsg)
		if err := handler(ctx, msg); err != nil {
			return err
		}
		if err := c.reader.CommitMessages(ctx, rawMsg); err != nil {
			return err
		}
	}
}

// RunBatch 按批消费。整批 handler 成功后才统一提交 offset。
// 适合 interaction-sync 这类“攒一批事件再调 Flush RPC”的任务。
func (c *Consumer) RunBatch(ctx context.Context, batchSize int, flushInterval time.Duration, handler BatchHandler) error {
	if batchSize <= 0 {
		batchSize = 1
	}
	if flushInterval <= 0 {
		flushInterval = time.Second
	}

	for {
		rawMessages, messages, err := c.fetchBatch(ctx, batchSize, flushInterval)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			continue
		}

		for {
			if err := handler(ctx, messages); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// handler 失败时不能继续拉下一批，否则后续 offset 一旦提交就可能越过失败消息。
				// 这里原地重试同一批，保证 at-least-once 语义。
				logx.Errorf("handle kafka batch failed, retry same batch later, size:%d error:%v", len(messages), err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(defaultConsumerHandlerRetryInterval):
				}
				continue
			}
			break
		}
		if err := c.reader.CommitMessages(ctx, rawMessages...); err != nil {
			return err
		}
	}
}

func (c *Consumer) fetchBatch(ctx context.Context, batchSize int, flushInterval time.Duration) ([]kafka.Message, []Message, error) {
	rawMessages := make([]kafka.Message, 0, batchSize)
	messages := make([]Message, 0, batchSize)

	first, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, err
	}
	rawMessages = append(rawMessages, first)
	messages = append(messages, fromKafkaMessage(first))

	for len(messages) < batchSize {
		fetchCtx, cancel := context.WithTimeout(ctx, flushInterval)
		rawMsg, err := c.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			return nil, nil, err
		}
		rawMessages = append(rawMessages, rawMsg)
		messages = append(messages, fromKafkaMessage(rawMsg))
	}

	return rawMessages, messages, nil
}

func (c *Consumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}

func fromKafkaMessage(msg kafka.Message) Message {
	headers := make([]Header, 0, len(msg.Headers))
	for _, header := range msg.Headers {
		headers = append(headers, Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	return Message{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
		Time:      msg.Time,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}
