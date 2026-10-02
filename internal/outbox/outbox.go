package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
)

const PREFIX = "outbox"

type Message struct {
	ID          uuid.UUID       `json:"id"` // unique per event for consumer dedup
	Type        string          `json:"type"`
	AggregateID string          `json:"aggregate_id"` // Kafka key
	OccurredAt  time.Time       `json:"occurred_at"`
	Payload     json.RawMessage `json:"payload"` // keep like that, because json.Marshal will make it Base64
}

func NewMessage(eventType, aggregateID string, event any) (Message, error) {
	payload, err := json.Marshal(event)

	if err != nil {
		return Message{}, fmt.Errorf("cannot parse payload: %w", err)
	}

	return Message{
		ID:          uuid.NewV7(),
		Type:        eventType,
		AggregateID: aggregateID,
		OccurredAt:  time.Now().UTC(),
		Payload:     payload,
	}, nil
}

func Emit(ctx context.Context, tx pgx.Tx, m Message) error {
	envelope, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("cannot parse outbox message: %w", err)
	}
	_, err = tx.Exec(ctx,
		`SELECT pg_logical_emit_message(true, $1, $2::bytea)`,
		PREFIX, envelope,
	)
	if err != nil {
		return fmt.Errorf("cannot emit outbox message %s: %w", m.Type, err)
	}
	return nil
}
