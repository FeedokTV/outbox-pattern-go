package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"outbox-pattern-go/internal/cdc"

	"github.com/IBM/sarama"
)

// ==== Abstractions =====

type CDCSource interface {
	Next(ctx context.Context) (*cdc.CDCEvent, error)
}

// ===== Relay =====

type Relay struct {
	source   CDCSource
	producer sarama.SyncProducer
	topic    string
}

func NewRelay(source CDCSource, producer sarama.SyncProducer, topic string) *Relay {
	return &Relay{
		source:   source,
		producer: producer,
		topic:    topic,
	}
}

func (r *Relay) Run(ctx context.Context) error {
	for {
		event, err := r.source.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			if errors.Is(err, io.EOF) {
				continue
			}

			fmt.Println("Error occured from CDCSource", err)
			return err
		}

		if event == nil {
			continue
		}

		fmt.Println("Got event from CDCSource with ID", event.ID)

		msg := &sarama.ProducerMessage{
			Topic: r.topic,
			Key:   sarama.StringEncoder(event.ID),
			Value: sarama.StringEncoder(event.Content),
		}

		if _, _, err = r.producer.SendMessage(msg); err != nil {
			fmt.Println("Error occured while sending message to Kafka", err)
			return err
		}

		if event.Ack != nil {
			if err := event.Ack(ctx); err != nil {
				fmt.Println("Ack function returned error", err)
				return err
			}
		}

		fmt.Println("Succesfully sent message to Kafka")
	}
}
