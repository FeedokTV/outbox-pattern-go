package transaction

import (
	"context"
	"fmt"
	"outbox-pattern-go/internal/models"
	"outbox-pattern-go/internal/outbox"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Create(ctx context.Context, pool *pgxpool.Pool, t Transaction) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := insertTransaction(ctx, tx, t); err != nil {
			return fmt.Errorf("insert transaction: %w", err)
		}

		msg, err := outbox.NewMessage(
			models.TransactionCreatedType,
			t.ID.String(),
			models.TransactionCreatedEvent{
				TransactionID: t.ID,
				Sender:        t.Sender,
				Recipient:     t.Recipient,
				Amount:        int64(t.Amount),
			},
		)
		if err != nil {
			return err
		}

		return outbox.Emit(ctx, tx, msg)
	})
}
