package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/segmentio/kafka-go"
)

const defaultPriority = "HIGH"

// Processor обрабатывает одно входящее обновление:
// фильтрует - суммаризирует - публикует в выходной топик
type Processor struct {
	filter     *Filter
	summarizer Summarizer
	writer     *kafka.Writer
	threshold  int
}

func NewProcessor(filter *Filter, summarizer Summarizer, writer *kafka.Writer, threshold int) *Processor {
	return &Processor{
		filter:     filter,
		summarizer: summarizer,
		writer:     writer,
		threshold:  threshold,
	}
}

// Process обрабатывает одно RawUpdate
func (p *Processor) Process(ctx context.Context, raw RawUpdate) error {
	// фильтрация
	result := p.filter.Allow(raw)
	if !result.Allowed {
		slog.Info("update filtered out",
			"id", raw.ID,
			"author", raw.Author,
			"reason", result.Reason,
		)
		return nil
	}

	// суммаризация (если текст превышает порог)
	description := raw.Description
	if len([]rune(description)) > p.threshold {
		summarized, err := p.summarizer.Summarize(ctx, description)
		if err != nil {
			slog.Warn("summarization failed, using original text",
				"id", raw.ID,
				"err", err,
			)
		} else {
			description = summarized
			slog.Debug("update summarized", "id", raw.ID,
				"original_len", len([]rune(raw.Description)),
				"summarized_len", len([]rune(description)),
			)
		}
	}

	// публикация - URL пробрасывается из RawUpdate
	out := ProcessedUpdate{
		ID:          raw.ID,
		URL:         raw.URL, // бот требует это поле
		Description: description,
		TgChatIDs:   raw.TgChatIDs,
		Priority:    defaultPriority,
	}

	if err := p.publish(ctx, out); err != nil {
		return fmt.Errorf("publish processed update: %w", err)
	}

	slog.Info("update processed and published",
		"id", raw.ID,
		"url", raw.URL,
		"chats", len(raw.TgChatIDs),
	)
	return nil
}

func (p *Processor) publish(ctx context.Context, out ProcessedUpdate) error {
	body, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("marshal processed update: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(strconv.FormatInt(out.ID, 10)),
		Value: body,
	}

	if err = p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}
	return nil
}
