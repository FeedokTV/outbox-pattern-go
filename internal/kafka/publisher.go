package kafka

import (
	"context"
	"outbox-pattern-go/internal/relay"

	"github.com/IBM/sarama"
)

type Publisher struct {
	producer sarama.SyncProducer
	topic    string
}

func NewPublisher(producer sarama.SyncProducer, topic string) *Publisher {
	return &Publisher{producer: producer, topic: topic}
}

func (p *Publisher) Publish(ctx context.Context, msg relay.Message) error {

	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	})
	return err
}
