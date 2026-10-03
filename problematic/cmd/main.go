package main

import (
	"context"
	"encoding/json/v2"
	"log"
	"os"
	"os/signal"
	"outbox-pattern-go/internal/database"
	"outbox-pattern-go/internal/kafka"
	"outbox-pattern-go/internal/transaction"
	"uuid"

	"syscall"

	"github.com/IBM/sarama"
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
		log.Fatalf("failed to create pool to database: %v", err)
	}
	defer pool.Close()

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

	// ----- MAIN PROBLEM -----

	// Create transaction with Postgres transaction

	testTransaction := transaction.Transaction{
		ID:        uuid.New(),
		Sender:    "John Doe",
		Recipient: "Bob Smith",
		Amount:    1000,
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`INSERT INTO transactions (id, sender, recipient, amount) VALUES ($1, $2, $3, $4)`,
		testTransaction.ID, testTransaction.Sender, testTransaction.Recipient, testTransaction.Amount,
	)

	tx.Commit(ctx)

	// ----- What if error occurs here? -----

	// Time to publish to Kafka

	payload, err := json.Marshal(testTransaction)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: "transactions",
		Key:   sarama.StringEncoder(testTransaction.ID.String()),
		Value: sarama.ByteEncoder(payload),
	}

	producer.SendMessage(msg)

	return nil
}
