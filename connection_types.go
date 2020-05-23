package kafka_wrapper

import "github.com/Shopify/sarama"

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         string
	Topics          []string
	ErrorTopic      string
	RetryTopic      string
	ConsumerGroupID string
}
