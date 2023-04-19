package kafka_wrapper

import (
	"github.com/Shopify/sarama"
	"strings"
)

func NewProducer(configManipulator ConfigurationManipulation, connectionParams ConnectionParameters) (sarama.SyncProducer, error) {
	connectionParams.Conf = configManipulator.ManipulateMetadataRetrieval(connectionParams.Conf)

	syncProducer, err := sarama.NewSyncProducer(strings.Split(connectionParams.Brokers, ","), connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	return syncProducer, err
}
