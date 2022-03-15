package kafka_wrapper

import (
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"

	"github.com/Shopify/sarama"
	"strings"
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
