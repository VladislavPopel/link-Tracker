package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	minBytes       = 1
	maxBytes       = 10 << 20 // 10 MB
	retryBaseDelay = 100 * time.Millisecond
)

// HandlerFunc - функция обработки одного сообщения
type HandlerFunc func(ctx context.Context, msg []byte) error

var (
	// ErrDeserialize - ошибка десериализации: сообщение сразу в DLQ без retry
	ErrDeserialize = errors.New("deserialization error")
	// ErrValidation - ошибка валидации: сообщение сразу в DLQ без retry
	ErrValidation = errors.New("validation error")
)

// Consumer читает сообщения из Kafka-топика с retry-логикой и DLQ
type Consumer struct {
	reader     *kafka.Reader
	dlqWriter  *kafka.Writer
	maxRetries int
}

func NewConsumer(brokers []string, topic, groupID, dlqTopic string, maxRetries int) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,          // 10 MB (батчинг для снижения нагрузки на сеть)
		CommitInterval: 0,                 // ручной коммит
		StartOffset:    kafka.FirstOffset, // при первом старте читать с начала топика
	})

	dlq := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}

	return &Consumer{
		reader:     r,
		dlqWriter:  dlq,
		maxRetries: maxRetries,
	}
}

// Run запускает цикл чтения сообщений. Блокирует до отмены ctx
func (c *Consumer) Run(ctx context.Context, handler HandlerFunc) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("kafka consumer fetch: %w", err)
		}

		c.processWithRetry(ctx, msg, handler)

		// Коммитим offset только после обработки (успешной или через DLQ)
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			slog.Error("kafka consumer: commit failed", "err", commitErr)
		}
	}
}

// Close завершает работу консьюмера
func (c *Consumer) Close() error {
	var errs []error
	if err := c.reader.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close reader: %w", err))
	}
	if err := c.dlqWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close dlq writer: %w", err))
	}
	return errors.Join(errs...)
}

// processWithRetry обрабатывает сообщение с повторными попытками
// При ошибках десериализации/валидации - в DLQ
func (c *Consumer) processWithRetry(ctx context.Context, msg kafka.Message, handler HandlerFunc) {
	var lastErr error

	for attempt := range c.maxRetries {
		lastErr = handler(ctx, msg.Value)
		if lastErr == nil {
			return
		}

		// Ошибки десериализации и валидации - сразу DLQ, без retry
		if errors.Is(lastErr, ErrDeserialize) || errors.Is(lastErr, ErrValidation) {
			slog.Warn("kafka consumer: fatal message error, sending to DLQ",
				"topic", msg.Topic,
				"offset", msg.Offset,
				"err", lastErr,
			)
			c.sendToDLQ(ctx, msg, lastErr)
			return
		}

		slog.Warn("kafka consumer: handler failed, retrying",
			"attempt", attempt+1,
			"max", c.maxRetries,
			"err", lastErr,
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(retryDelay(attempt)):
		}
	}

	slog.Error("kafka consumer: max retries exceeded, sending to DLQ",
		"topic", msg.Topic,
		"offset", msg.Offset,
		"err", lastErr,
	)
	c.sendToDLQ(ctx, msg, lastErr)
}

// sendToDLQ отправляет необработанное сообщение в Dead Letter Queue
func (c *Consumer) sendToDLQ(ctx context.Context, original kafka.Message, reason error) {
	type dlqEnvelope struct {
		OriginalTopic string    `json:"original_topic"`
		OriginalKey   string    `json:"original_key"`
		Payload       string    `json:"payload"`
		Error         string    `json:"error"`
		FailedAt      time.Time `json:"failed_at"`
	}

	envelope, err := json.Marshal(dlqEnvelope{
		OriginalTopic: original.Topic,
		OriginalKey:   string(original.Key),
		Payload:       string(original.Value),
		Error:         reason.Error(),
		FailedAt:      time.Now(),
	})
	if err != nil {
		slog.Error("kafka consumer: DLQ marshal failed", "err", err)
		return
	}

	dlqMsg := kafka.Message{
		Key:   original.Key,
		Value: envelope,
	}
	if writeErr := c.dlqWriter.WriteMessages(ctx, dlqMsg); writeErr != nil {
		slog.Error("kafka consumer: DLQ write failed", "err", writeErr)
	}
}

// retryDelay возвращает задержку перед следующей попыткой
func retryDelay(attempt int) time.Duration {
	base := retryBaseDelay
	for range attempt {
		base *= 2
	}
	return base
}
