package transaction

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func Insert(ctx context.Context, tx pgx.Tx, t Transaction) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO transactions (id, sender, recipient, amount) VALUES ($1, $2, $3, $4)`,
		t.ID, t.Sender, t.Recipient, t.Amount,
	)
	return err
}
