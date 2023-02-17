package test_utils

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
)

func NewEventHandler(message chan string) kafka_wrapper.EventHandler {
	return &testEventHandler{
		message:              message,
		subscriptionStatusCh: make(chan bool),
	}
}

type testEventHandler struct {
	message              chan string
	subscriptionStatusCh chan bool
}

func (ge *testEventHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (ge *testEventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (ge *testEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		ge.message <- string(message.Value)
	}

	return nil
}
