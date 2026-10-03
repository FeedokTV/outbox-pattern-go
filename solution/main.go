package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"outbox-pattern-go/internal/cdc"
	"outbox-pattern-go/internal/database"
	"outbox-pattern-go/internal/kafka"
	"outbox-pattern-go/internal/relay"
	"outbox-pattern-go/internal/transaction"
	"syscall"
	"time"
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
	pool, err := database.GetDatabasePool(ctx)
	if err != nil {
		log.Fatalf("failed to create pool to database: %v", err)
	}
	defer pool.Close()

	// Initialize connection for replication
	replConn, err := database.GetDatabaseConnectionReplication(ctx)
	if err != nil {
		log.Fatalf("failed to create connection for replication: %v", err)
	}

	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := replConn.Close(closeCtx); err != nil {
			log.Printf("close replication connection: %v", err)
		}
	}()

	// Replication service
	replService := database.NewPgReplicationService(ctx, replConn)

	err = replService.Initialize(ctx)
	if err != nil {
		log.Fatalf("cannot initialize replication service: %v", err)
	}

	// CDC Service
	pgCDC := cdc.NewPostgresCDC(replConn)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := pgCDC.Stop(closeCtx); err != nil {
			log.Printf("close Postgres CDC: %v", err)
		}
	}()

	// Kafka
	producer, err := kafka.GetKafkaSyncProducer(ctx)
	if err != nil {
		log.Fatalf("cannot initialize kaffka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("close kafka producer: %v", err)
		}
	}()

	kafkaPublisher := kafka.NewPublisher(producer, "transactions")

	// Relay
	pgRelay := relay.NewRelay(pgCDC, kafkaPublisher)

	// Two gorutines in error groups. First is relay, the second is independent process
	// for example in second one we just create two transactions
	// in real practice it can be HTTP server or etc.
	errGroup, groupCtx := errgroup.WithContext(ctx)

	errGroup.Go(func() error {
		err := pgRelay.Run(groupCtx)

		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}

		return fmt.Errorf("relay: %w", err)
	})

	errGroup.Go(func() error {
		if err := bankingServiceExample(groupCtx, pool); err != nil {
			return err
		}

		return nil
	})

	if err := errGroup.Wait(); err != nil {
		return err
	}

	return nil
}

func bankingServiceExample(ctx context.Context, pool *pgxpool.Pool) error {
	var err error

	transaction1 := transaction.Transaction{
		ID:        uuid.New(),
		Sender:    "John Doe",
		Recipient: "Bob Smith",
		Amount:    1000,
	}

	if err = transaction.Create(
		ctx,
		pool,
		transaction1,
	); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}

		return fmt.Errorf("create transaction: %w", err)

	}

	transaction2 := transaction.Transaction{
		ID:        uuid.New(),
		Sender:    "Bob Smith",
		Recipient: "Aileen Wick",
		Amount:    1000,
	}

	if err = transaction.Create(
		ctx,
		pool,
		transaction2,
	); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}

		return fmt.Errorf("create transaction: %w", err)

	}

	return nil
}
