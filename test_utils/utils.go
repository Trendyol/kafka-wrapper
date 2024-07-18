package test_utils

import (
	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
)

func Consume(kafkaConfig params.ConnectionParameters, topic string) (string, string, []*sarama.RecordHeader) {
	master, err := sarama.NewConsumer([]string{kafkaConfig.Brokers}, kafkaConfig.Conf)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	partitions, _ := master.Partitions(topic)
	partitionConsumer, err := master.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	msg := <-partitionConsumer.Messages()
	return string(msg.Key), string(msg.Value), msg.Headers
}

func CreateBasicConf() *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	v, _ := sarama.ParseKafkaVersion("2.2.0")
	kafkaConfig.Version = v
	kafkaConfig.ClientID = "kafka-client-id"
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Consumer.Return.Errors = true

	return kafkaConfig
}
