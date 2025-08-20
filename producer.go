package kafka_wrapper

import (
	"context"
	"strings"

	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
)

func NewProducer(connectionParams params.ConnectionParameters) (sarama.SyncProducer, error) {
	syncProducer, err := sarama.NewSyncProducer(strings.Split(connectionParams.Brokers, ","), connectionParams.Conf)
	if err != nil {
		loggerHelper := params.NewLoggerHelper(connectionParams.Logger)
		loggerHelper.Error(context.Background(), "could not create producer: %s", err)
		return nil, err
	}

	return syncProducer, err
}
