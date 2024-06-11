package test_utils

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour"
)

func NewEventHandler(message chan string) kafka_wrapper.EventHandler {
	return &testEventHandler{
		message:              message,
		subscriptionStatusCh: make(chan bool),
	}
}

func NewEventHandlerWithError(message chan string) kafka_wrapper.EventHandler {
	return &testEventHandler{
		message:              message,
		subscriptionStatusCh: make(chan bool),
	}
}

type testEventHandler struct {
	message              chan string
	subscriptionStatusCh chan bool
}

func (ge *testEventHandler) BehavioralSelector() execution_behaviour.BehavioralSelector {
	//TODO implement me
	panic("implement me")
}

func (ge *testEventHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (ge *testEventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (ge *testEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("message: %+v", string(message.Value))
		ge.message <- string(message.Value)
	}

	return nil
}
