package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"outbox-pattern-go/internal/cdc"
)

// ==== Abstractions =====

type CDCSource interface {
	Next(ctx context.Context) (*cdc.CDCEvent, error)
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
	source    CDCSource
	publisher MessagePublisher
	topic     string
}

func NewRelay(source CDCSource, publisher MessagePublisher) *Relay {
	return &Relay{
		source:    source,
		publisher: publisher,
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

		// msg := &sarama.ProducerMessage{
		// 	Topic: r.topic,
		// 	Key:   sarama.StringEncoder(event.ID),
		// 	Value: sarama.StringEncoder(event.Content),
		// }

		msg := Message{
			Key:   event.ID,
			Value: event.Content,
		}

		if err = r.publisher.Publish(ctx, msg); err != nil {
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
