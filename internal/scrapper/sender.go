package scrapper

import (
	"context"
	"fmt"
	"net/http"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
)

// MessageSender - интерфейс для отправки уведомлений scrapper - bot
type MessageSender interface {
	SendUpdate(ctx context.Context, update client.LinkUpdate) error
}

// HTTPMessageSender отправляет уведомления через HTTP
type HTTPMessageSender struct {
	botClient *client.BotHTTPClient
}

func NewHTTPMessageSender(botURL string) *HTTPMessageSender {
	return &HTTPMessageSender{botClient: client.NewBotHTTPClient(botURL)}
}

func NewHTTPMessageSenderWithHTTP(botURL string, httpClient *http.Client) *HTTPMessageSender {
	return &HTTPMessageSender{
		botClient: client.NewBotHTTPClientWithHTTP(botURL, httpClient),
	}
}

func (s *HTTPMessageSender) SendUpdate(ctx context.Context, update client.LinkUpdate) error {
	if err := s.botClient.SendUpdate(ctx, update); err != nil {
		return fmt.Errorf("http message sender: %w", err)
	}
	return nil
}

// NewMessageSender - фабрика: выбирает реализацию по конфигурации
func NewMessageSender(cfg *config.ScrapperConfig) MessageSender {
	switch cfg.NotificationTransport {
	case config.TransportHTTP:
		return NewHTTPMessageSender(cfg.BotURL)
	case config.TransportKafka:
		return NewKafkaMessageSender(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	default:
		return NewKafkaMessageSender(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	}
}
