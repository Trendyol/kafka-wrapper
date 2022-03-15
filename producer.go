package kafka_wrapper

import (
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"

	"strings"

	"github.com/Shopify/sarama"
)

func NewProducer(connectionParams ConnectionParameters) (sarama.SyncProducer, error) {
	saramaConfig := connectionParams.Conf
	producer, err := sarama.NewSyncProducer(strings.Split(connectionParams.Brokers, ","), saramaConfig)
	if err != nil {
		return nil, err
	}

	producer = otelsarama.WrapSyncProducer(saramaConfig, producer)
	return producer, nil
}
