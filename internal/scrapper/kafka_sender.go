package scrapper

import (
	"context"
	"fmt"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/kafka"
)

// KafkaMessageSender реализует MessageSender через Apache Kafka
type KafkaMessageSender struct {
	producer *kafka.Producer
}

// NewKafkaMessageSender создаёт продюсера, подключённого к указанным брокерам
func NewKafkaMessageSender(brokers []string, topic string) *KafkaMessageSender {
	return &KafkaMessageSender{
		producer: kafka.NewProducer(brokers, topic),
	}
}

// SendUpdate сериализует LinkUpdate в JSON и отправляет в Kafka
func (s *KafkaMessageSender) SendUpdate(ctx context.Context, update client.LinkUpdate) error {
	if err := s.producer.Send(ctx, update.URL, update); err != nil {
		return fmt.Errorf("kafka message sender: %w", err)
	}
	return nil
}

// Close освобождает ресурсы продюсера. Вызывать при завершении scrapper
func (s *KafkaMessageSender) Close() error {
	if err := s.producer.Close(); err != nil {
		return fmt.Errorf("kafka message sender close: %w", err)
	}
	return nil
}
