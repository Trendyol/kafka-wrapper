package kafka_wrapper

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
	"log"
	"strings"
)

func NewProducer(connectionParams params.ConnectionParameters) (sarama.SyncProducer, error) {
	syncProducer, err := sarama.NewSyncProducer(strings.Split(connectionParams.Brokers, ","), connectionParams.Conf)
	if err != nil {
		log.Printf("could not create producer: %s\n", err)
		return nil, err
	}

	return syncProducer, err
}
