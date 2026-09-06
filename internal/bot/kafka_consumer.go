package bot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	kafkaclient "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/kafka"
)

// LinkUpdateConsumer читает уведомления из Kafka и отправляет их в Telegram
type LinkUpdateConsumer struct {
	consumer *kafkaclient.Consumer
	sender   MessageSender
}

// NewLinkUpdateConsumer создаёт консьюмера уведомлений для бота
func NewLinkUpdateConsumer(
	brokers []string,
	topic, groupID, dlqTopic string,
	maxRetries int,
	sender MessageSender,
) *LinkUpdateConsumer {
	return &LinkUpdateConsumer{
		consumer: kafkaclient.NewConsumer(brokers, topic, groupID, dlqTopic, maxRetries),
		sender:   sender,
	}
}

// Run запускает цикл обработки сообщений. Блокирует до отмены ctx
func (c *LinkUpdateConsumer) Run(ctx context.Context) error {
	slog.Info("bot kafka consumer started")
	if err := c.consumer.Run(ctx, c.handle); err != nil {
		return fmt.Errorf("link update consumer: %w", err)
	}
	return nil
}

// Close освобождает ресурсы консьюмера
func (c *LinkUpdateConsumer) Close() error {
	if err := c.consumer.Close(); err != nil {
		return fmt.Errorf("link update consumer close: %w", err)
	}
	return nil
}

// handle десериализует LinkUpdate из JSON и рассылает уведомления в Telegram
// Соответствует HandlerFunc из kafka/consumer.go
func (c *LinkUpdateConsumer) handle(_ context.Context, msg []byte) error {
	var update LinkUpdate
	if err := json.Unmarshal(msg, &update); err != nil {
		// Ошибка десериализации - сразу в DLQ (нет смысла ретраить)
		return fmt.Errorf("%w: %w", kafkaclient.ErrDeserialize, err)
	}

	if update.URL == "" || len(update.TgChatIDs) == 0 {
		// Невалидное сообщение - сразу в DLQ
		return fmt.Errorf("%w: url and tgChatIds are required", kafkaclient.ErrValidation)
	}

	text := fmt.Sprintf("Обновление!\nСсылка: %s\n%s", update.URL, update.Description)

	var sendErrors []error
	for _, chatID := range update.TgChatIDs {
		msg := tgbotapi.NewMessage(chatID, text)
		if _, err := c.sender.Send(msg); err != nil {
			slog.Error("bot kafka consumer: failed to send telegram message",
				"chat_id", chatID,
				"url", update.URL,
				"err", err,
			)
			sendErrors = append(sendErrors, err)
		}
	}

	if len(sendErrors) > 0 {
		// Частичная ошибка - retry может помочь (Telegram временно недоступен)
		return fmt.Errorf("telegram send errors (%d/%d): %w",
			len(sendErrors), len(update.TgChatIDs), errors.Join(sendErrors...))
	}
	return nil
}
