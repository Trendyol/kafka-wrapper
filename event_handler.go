package kafka_wrapper

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour"
)

type EventHandler interface {
	BehavioralSelector() execution_behaviour.BehavioralSelector
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
}
