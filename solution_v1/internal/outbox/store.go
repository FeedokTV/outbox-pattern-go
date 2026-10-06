package outbox

import (
	"context"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Record is an outbox row claimed by the relay
type Record struct {
	ID          uuid.UUID
	AggregateID string
	Payload     []byte
}

// Store is the relay side of the outbox table
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Claim(ctx context.Context, limit int, lease time.Duration) ([]Record, error) {
	rows, err := s.pool.Query(ctx,
		`WITH claimed AS (
			UPDATE outbox
			SET locked_until = now() + make_interval(secs => $2)
			WHERE id IN (
				SELECT id FROM outbox
				WHERE sended_at IS NULL
				  AND locked_until < now()
				ORDER BY created_at, id
				LIMIT $1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id, aggregate_id, payload
		)
		SELECT id, aggregate_id, payload FROM claimed ORDER BY id`,
		limit, lease.Seconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("claim outbox rows: %w", err)
	}

	records, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Record, error) {
		var r Record
		err := row.Scan(&r.ID, &r.AggregateID, &r.Payload)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan outbox rows: %w", err)
	}

	return records, nil
}

func (s *PostgresStore) MarkSent(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox SET sended_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark outbox row %s as sent: %w", id, err)
	}
	return nil
}

func (s *PostgresStore) Release(ctx context.Context, ids []uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox SET locked_until = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("release outbox rows: %w", err)
	}
	return nil
}
