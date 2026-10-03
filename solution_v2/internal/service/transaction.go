package service

import (
	"context"
	"fmt"
	"outbox-pattern-go/internal/transaction"
	"outbox-pattern-go/solution_v2/internal/outbox"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func CreateTransaction(ctx context.Context, pool *pgxpool.Pool, t transaction.Transaction) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := transaction.Insert(ctx, tx, t); err != nil {
			return fmt.Errorf("insert transaction: %w", err)
		}

		msg, err := outbox.NewMessage(
			transaction.TransactionCreatedType,
			t.ID.String(),
			transaction.TransactionCreatedEvent{
				TransactionID: t.ID,
				Sender:        t.Sender,
				Recipient:     t.Recipient,
				Amount:        t.Amount,
			},
		)
		if err != nil {
			return err
		}

		return outbox.Emit(ctx, tx, msg)
	})
}
