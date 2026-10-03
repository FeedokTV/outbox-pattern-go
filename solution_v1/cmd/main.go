package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"outbox-pattern-go/internal/database"
	"outbox-pattern-go/internal/kafka"
	"outbox-pattern-go/internal/transaction"
	"outbox-pattern-go/solution_v1/internal/outbox"
	"outbox-pattern-go/solution_v1/internal/publisher"
	"outbox-pattern-go/solution_v1/internal/relay"
	"outbox-pattern-go/solution_v1/internal/service"
	"syscall"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := runApp(); err != nil {
		log.Printf("application stopped with error: %v", err)
		os.Exit(1)
	}
}

func runApp() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize database pool connection
	pool, err := database.NewPool(ctx)
	if err != nil {
		return fmt.Errorf("create pool to database: %w", err)
	}
	defer pool.Close()

	// Kafka
	producer, err := kafka.GetKafkaSyncProducer(ctx)
	if err != nil {
		return fmt.Errorf("initialize kafka producer: %w", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("close kafka producer: %v", err)
		}
	}()

	kafkaPublisher := publisher.NewKafkaPublisher(producer, "transactions")

	// Store
	outboxStore := outbox.NewPostgresStore(pool)

	// Relay
	outboxRelay := relay.NewRelay(outboxStore, kafkaPublisher)

	errGroup, groupCtx := errgroup.WithContext(ctx)

	errGroup.Go(func() error {
		return outboxRelay.Run(groupCtx)
	})

	errGroup.Go(func() error {
		return bankingServiceExample(groupCtx, pool)
	})

	return errGroup.Wait()
}

func bankingServiceExample(ctx context.Context, pool *pgxpool.Pool) error {
	transactions := []transaction.Transaction{
		{ID: uuid.New(), Sender: "John Doe", Recipient: "Bob Smith", Amount: 1000},
		{ID: uuid.New(), Sender: "Bob Smith", Recipient: "Aileen Wick", Amount: 1000},
	}

	for _, t := range transactions {
		if err := service.CreateTransaction(ctx, pool, t); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return fmt.Errorf("create transaction: %w", err)
		}
	}

	return nil
}
