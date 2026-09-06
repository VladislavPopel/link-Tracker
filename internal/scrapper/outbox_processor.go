package scrapper

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository"
)

type OutboxProcessor struct {
	repo     *repository.OutboxRepository
	sender   MessageSender
	interval time.Duration
	batch    int
}

func NewOutboxProcessor(
	repo *repository.OutboxRepository,
	sender MessageSender,
	interval time.Duration,
	batch int,
) *OutboxProcessor {
	return &OutboxProcessor{repo: repo, sender: sender, interval: interval, batch: batch}
}

func (p *OutboxProcessor) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.process(ctx)
		}
	}
}

func (p *OutboxProcessor) process(ctx context.Context) {
	// Открываем транзакцию - строки заблокированы FOR UPDATE SKIP LOCKED
	tx, err := p.repo.BeginTx(ctx)
	if err != nil {
		slog.Error("outbox processor: begin tx", "err", err)
		return
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			slog.Error("outbox processor: rollback", "err", rollbackErr)
		}
	}()

	items, err := p.repo.GetPendingForUpdate(ctx, tx, p.batch)
	if err != nil {
		slog.Error("outbox processor: get pending", "err", err)
		return
	}

	for _, item := range items {
		var update client.LinkUpdate
		if unmarshalErr := json.Unmarshal(item.Payload, &update); unmarshalErr != nil {
			slog.Error("outbox processor: unmarshal", "id", item.ID, "err", unmarshalErr)
			_ = p.repo.UpdateStatusTx(ctx, tx, item.ID, repository.StatusFailed)
			continue
		}

		if sendErr := p.sender.SendUpdate(ctx, update); sendErr != nil {
			slog.Warn("outbox processor: send failed, will retry next tick",
				"id", item.ID, "err", sendErr)
			// статус не меняем
			continue
		}

		if updateErr := p.repo.UpdateStatusTx(ctx, tx, item.ID, repository.StatusProcessed); updateErr != nil {
			slog.Error("outbox processor: failed to update status", "id", item.ID, "err", updateErr)
		}
	}

	if commitErr := tx.Commit(); commitErr != nil {
		slog.Error("outbox processor: commit", "err", commitErr)
	}
}
