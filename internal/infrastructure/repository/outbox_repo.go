package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
)

type OutboxStatus string

const (
	StatusPending   OutboxStatus = "pending"
	StatusProcessed OutboxStatus = "processed"
	StatusFailed    OutboxStatus = "failed"
)

type OutboxItem struct {
	ID      int64
	Payload []byte
}

type OutboxRepository struct {
	db *sql.DB
}

func NewOutboxRepository(db *sql.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// Insert добавляет запись в outbox в рамках переданной транзакции
func (r *OutboxRepository) Insert(ctx context.Context, tx *sql.Tx, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox (payload, status) VALUES ($1, $2)`, data, StatusPending)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// GetPendingForUpdate возвращает необработанные записи и блокирует их строки
// (FOR UPDATE SKIP LOCKED), чтобы параллельные воркеры не брали одни и те же
// Должна вызываться внутри транзакции
func (r *OutboxRepository) GetPendingForUpdate(ctx context.Context, tx *sql.Tx, limit int) ([]OutboxItem, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id, payload
		   FROM outbox
		  WHERE status = $1
		  ORDER BY created_at
		  LIMIT $2
		  FOR UPDATE SKIP LOCKED`,
		StatusPending, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query outbox: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Error("outbox: close rows", "err", closeErr)
		}
	}()

	var items []OutboxItem
	for rows.Next() {
		var item OutboxItem
		if scanErr := rows.Scan(&item.ID, &item.Payload); scanErr != nil {
			return nil, fmt.Errorf("scan outbox row: %w", scanErr)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox rows: %w", err)
	}
	return items, nil
}

// UpdateStatusTx меняет статус записи внутри транзакции
func (r *OutboxRepository) UpdateStatusTx(ctx context.Context, tx *sql.Tx, id int64, status OutboxStatus) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE outbox SET status = $1, updated_at = now() WHERE id = $2`,
		status, id,
	)
	if err != nil {
		return fmt.Errorf("update outbox status: %w", err)
	}
	return nil
}

// BeginTx открывает транзакцию с уровнем ReadCommitted
func (r *OutboxRepository) BeginTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return tx, nil
}
