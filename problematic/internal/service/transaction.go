package service

import (
	"context"
	"encoding/json"
	"outbox-pattern-go/internal/transaction"

	"github.com/IBM/sarama"
)

var (
	PRODUCER_TOPIC = "transactions-events"
	PARTITION      = 0
)

func CreateTransactionCreatedEvent(ctx context.Context, producer sarama.SyncProducer, t transaction.Transaction) error {
	event := &transaction.TransactionCreatedEvent{
		TransactionID: t.ID,
		Sender:        t.Sender,
		Recipient:     t.Recipient,
		Amount:        t.Amount,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     PRODUCER_TOPIC,
		Partition: int32(PARTITION),
		Value:     sarama.ByteEncoder(payload),
		Key:       sarama.StringEncoder(t.ID.String()),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}
