package kafka_wrapper

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
	"strings"
)

func NewProducer(connectionParams params.ConnectionParameters) (sarama.SyncProducer, error) {
	syncProducer, err := sarama.NewSyncProducer(strings.Split(connectionParams.Brokers, ","), connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	return syncProducer, err
}
