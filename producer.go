package kafka_wrapper

import (
	"github.com/Shopify/sarama"
	"strings"
)

func NewProducer(connectionParams ConnectionParameters) (sarama.SyncProducer, error) {
	syncProducer, err := sarama.NewSyncProducer(strings.Split(connectionParams.Brokers, ","), connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	return syncProducer, err
}
