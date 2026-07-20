package kafkax

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(conf ProducerConf) (*Producer, error) {
	conf = conf.Normalize()
	if len(conf.Brokers) == 0 {
		return nil, errors.New("kafka brokers is empty")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(conf.Brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequiredAcks(conf.RequiredAcks),
		BatchSize:    conf.BatchSize,
		BatchBytes:   int64(conf.BatchBytes),
		BatchTimeout: time.Duration(conf.FlushMs) * time.Millisecond,
		MaxAttempts:  conf.RetryMax,
		Async:        false,
	}

	return &Producer{writer: writer}, nil
}

func MustNewProducer(conf ProducerConf) *Producer {
	producer, err := NewProducer(conf)
	if err != nil {
		panic(err)
	}
	return producer
}

func (p *Producer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers ...Header) error {
	return p.PublishMessage(ctx, Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}

func (p *Producer) PublishMessage(ctx context.Context, msg Message) error {
	return p.PublishBatch(ctx, []Message{msg})
}

func (p *Producer) PublishBatch(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for _, msg := range messages {
		kafkaMessages = append(kafkaMessages, toKafkaMessage(msg))
	}
	return p.writer.WriteMessages(ctx, kafkaMessages...)
}

func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func toKafkaMessage(msg Message) kafka.Message {
	headers := make([]kafka.Header, 0, len(msg.Headers))
	for _, header := range msg.Headers {
		headers = append(headers, kafka.Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	return kafka.Message{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
		Time:    msg.Time,
	}
}
