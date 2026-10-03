package relay

import (
	"context"
	"fmt"
	"log"
	"outbox-pattern-go/solution_v1/internal/outbox"
	"time"
	"uuid"
)

const (
	pollInterval = time.Second      // how often we look into the outbox table
	batchSize    = 100              // max rows claimed per poll
	leaseTime    = 30 * time.Second // how long a claimed row stays ours
)

// ==== Abstractions =====

type OutboxStore interface {
	Claim(ctx context.Context, limit int, lease time.Duration) ([]outbox.Record, error)
	MarkSent(ctx context.Context, id uuid.UUID) error
	Release(ctx context.Context, ids []uuid.UUID) error
}

type Message struct {
	Key   string
	Value []byte
}

type MessagePublisher interface {
	Publish(ctx context.Context, msg Message) error
}

// ===== Relay =====

type Relay struct {
	store     OutboxStore
	publisher MessagePublisher
}

func NewRelay(store OutboxStore, publisher MessagePublisher) *Relay {
	return &Relay{
		store:     store,
		publisher: publisher,
	}
}

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if err := r.processBatch(ctx); err != nil && ctx.Err() == nil {
			log.Printf("relay batch failed, will retry: %v", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Relay) processBatch(ctx context.Context) error {
	records, err := r.store.Claim(ctx, batchSize, leaseTime)
	if err != nil {
		return err
	}

	for i, record := range records {
		fmt.Println("Got event from outbox with ID", record.ID)

		msg := Message{
			Key:   record.AggregateID,
			Value: record.Payload,
		}

		if err := r.publisher.Publish(ctx, msg); err != nil {
			// Stop at the first failure, so later events can't overtake this one
			r.release(ctx, records[i:])
			return fmt.Errorf("publish event %s: %w", record.ID, err)
		}

		// If we crash right here, the event is already in Kafka but not marked as sent
		if err := r.store.MarkSent(ctx, record.ID); err != nil {
			return err
		}

		fmt.Println("Succesfully sent message to Kafka")
	}

	return nil
}

func (r *Relay) release(ctx context.Context, records []outbox.Record) {
	ids := make([]uuid.UUID, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}

	if err := r.store.Release(ctx, ids); err != nil {
		log.Printf("release outbox rows: %v", err)
	}
}
