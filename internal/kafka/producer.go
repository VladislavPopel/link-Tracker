package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Producer отправляет JSON-сообщения в указанный топик Kafka
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		RequiredAcks: kafka.RequireAll, // запись подтверждается всеми ISR-репликами
		Async:        false,            // синхронная запись, ошибки не теряются
	}
	return &Producer{writer: w}
}

// Send сериализует значение в JSON и отправляет сообщение с указанным ключом
func (p *Producer) Send(ctx context.Context, key string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("kafka producer marshal: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: body,
	}
	if err = p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka producer write: %w", err)
	}
	return nil
}

// Close завершает работу продюсера
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("kafka producer close: %w", err)
	}
	return nil
}
