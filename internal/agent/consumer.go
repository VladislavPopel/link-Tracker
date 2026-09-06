package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

const (
	maxFetchBytes = 10e6
)

// Consumer читает сообщения из input-топика и передаёт их в Processor
type Consumer struct {
	reader    *kafka.Reader
	processor *Processor
}

// NewConsumer создаёт Consumer на основе конфигурации
func NewConsumer(cfg KafkaAgentConfig, processor *Processor) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.InputTopic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: maxFetchBytes,
	})

	return &Consumer{
		reader:    reader,
		processor: processor,
	}
}

// Run запускает цикл чтения сообщений. Блокирует до отмены ctx
func (c *Consumer) Run(ctx context.Context) error {
	slog.Info("agent consumer started", "topic", c.reader.Config().Topic)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				slog.Info("agent consumer stopped")
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		if err = c.handleMessage(ctx, msg); err != nil {
			slog.Error("handle message failed", "offset", msg.Offset, "err", err)
		}

		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			slog.Error("commit message failed", "offset", msg.Offset, "err", commitErr)
		}
	}
}

// Close освобождает ресурсы reader'а
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("close kafka reader: %w", err)
	}
	return nil
}

// handleMessage десериализует и обрабатывает одно сообщение
func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	var raw RawUpdate
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		return fmt.Errorf("unmarshal raw update (offset=%d): %w", msg.Offset, err)
	}

	slog.Debug("received raw update", "id", raw.ID, "author", raw.Author)

	if err := c.processor.Process(ctx, raw); err != nil {
		return fmt.Errorf("process update id=%d: %w", raw.ID, err)
	}

	return nil
}
