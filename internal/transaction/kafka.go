package transaction

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
)

var (
	PRODUCER_TOPIC = "transactions-events"
	PARTITION      = 0
)

func CreateTransactionCreatedEvent(ctx context.Context, producer sarama.SyncProducer, transaction Transaction) error {
	event := &TransactionCreatedEvent{
		TransactionID: transaction.ID,
		Sender:        transaction.Sender,
		Recipient:     transaction.Recipient,
		Amount:        transaction.Amount,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     PRODUCER_TOPIC,
		Partition: int32(PARTITION),
		Value:     sarama.ByteEncoder(payload),
		Key:       sarama.StringEncoder(transaction.ID.String()),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}
