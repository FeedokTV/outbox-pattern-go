package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

func GetKafkaSyncProducer(ctx context.Context) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
