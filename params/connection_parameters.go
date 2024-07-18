package params

import "github.com/IBM/sarama"

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         string
	ConsumerGroupID string
}
