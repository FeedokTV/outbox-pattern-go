package publisher

import (
	"context"
	"outbox-pattern-go/solution_v2/internal/relay"

	"github.com/IBM/sarama"
)

type KafkaPublisher struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaPublisher(producer sarama.SyncProducer, topic string) *KafkaPublisher {
	return &KafkaPublisher{producer: producer, topic: topic}
}

func (p *KafkaPublisher) Publish(ctx context.Context, msg relay.Message) error {

	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	})
	return err
}
