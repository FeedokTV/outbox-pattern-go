package database

import (
	"context"
	"encoding/json"
	"fmt"
	"outbox-pattern-go/internal/transaction"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
)

type EmitMessage struct {
	ID          uuid.UUID
	Type        string
	AggregateID string
	Payload     []byte
}

func CreateTransaction(ctx context.Context, pool *pgxpool.Pool, transaction transaction.Transaction) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(
		ctx,
		`INSERT INTO transactions 
			(id, sender, recipient, amount) 
		VALUES ($1, $2, $3, $4)`,
		transaction.ID,
		transaction.Sender,
		transaction.Recipient,
		transaction.Amount,
	)

	if err != nil {
		return err
	}

	event := map[string]any{
		"id":        transaction.ID,
		"type":      "TransactionCreated",
		"sender":    transaction.Sender,
		"recipient": transaction.Recipient,
		"amount":    transaction.Amount,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal CDC event: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`SELECT pg_logical_emit_message(
            true,
            'cdc_transaction_event',
            $1::text
        )`,
		string(payload),
	)

	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
