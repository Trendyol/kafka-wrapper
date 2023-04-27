package params

import "github.com/Shopify/sarama"

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         string
	ConsumerGroupID string
}
