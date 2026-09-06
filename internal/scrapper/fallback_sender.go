package scrapper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/sony/gobreaker/v2"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/resilience"
)

// FallbackMessageSender реализует:
// пробует основной  способ отправки (HTTP), при сбое переключается на Kafka
type FallbackMessageSender struct {
	primary  MessageSender
	fallback MessageSender
}

// NewFallbackMessageSender создает sender с HTTP-primary и Kafka-fallback
func NewFallbackMessageSender(cfg *config.ScrapperConfig, httpClient *http.Client) *FallbackMessageSender {
	var primary MessageSender
	if httpClient != nil {
		primary = &HTTPMessageSender{
			botClient: client.NewBotHTTPClientWithHTTP(cfg.BotURL, httpClient),
		}
	} else {
		primary = NewHTTPMessageSender(cfg.BotURL)
	}
	return &FallbackMessageSender{
		primary:  primary,
		fallback: NewKafkaMessageSender(cfg.Kafka.Brokers, cfg.Kafka.Topic),
	}
}

// SendUpdate отправляет через primary; при ошибке - через fallback
func (s *FallbackMessageSender) SendUpdate(ctx context.Context, update client.LinkUpdate) error {
	err := s.primary.SendUpdate(ctx, update)
	if err == nil {
		return nil
	}

	if !isFallbackWorthy(err) {
		return fmt.Errorf("primary sender permanent error: %w", err)
	}

	slog.Warn("primary transport failed, falling back to kafka",
		"url", update.URL,
		"err", err,
	)

	if fallbackErr := s.fallback.SendUpdate(ctx, update); fallbackErr != nil {
		return fmt.Errorf("both transports failed: primary=%w, fallback=%w", err, fallbackErr)
	}
	return nil
}

// isFallbackWorthy возвращает true, если ошибка означает недоступность транспорта
func isFallbackWorthy(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return true
	}
	var cbErr *resilience.CircuitBreakerError
	if errors.As(err, &cbErr) {
		return true
	}
	return true
}
