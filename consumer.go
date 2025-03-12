package kafka_wrapper

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
	"strings"
)

type Consumer interface {
	Subscribe(topicParams []params.TopicsParameters, handler EventHandler)
	SubscribeToTopic(topicParams params.TopicsParameters, handler EventHandler)
	Unsubscribe()
}

type kafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	ctx           context.Context
	cancelFunc    context.CancelFunc
}

func NewConsumer(connectionParams params.ConnectionParameters) (Consumer, error) {
	cg, err := sarama.NewConsumerGroup(strings.Split(connectionParams.Brokers, ","), connectionParams.ConsumerGroupID, connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &kafkaConsumer{
		consumerGroup: cg,
		ctx:           ctx,
		cancelFunc:    cancel,
	}, nil
}

func (c *kafkaConsumer) SubscribeToTopic(topicParams params.TopicsParameters, handler EventHandler) {
	c.Subscribe([]params.TopicsParameters{topicParams}, handler)
}

func (c *kafkaConsumer) Subscribe(topicParams []params.TopicsParameters, handler EventHandler) {
	go func() {
		for {
			if c.ctx.Err() != nil {
				fmt.Printf("Consumer context canceled, exiting\n")
				return
			}

			err := c.consumerGroup.Consume(c.ctx, params.JoinAllTopics(topicParams), handler)
			if err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				fmt.Printf("Error from consumer: %v\n", err)
			}
		}
	}()

	go func() {
		for err := range c.consumerGroup.Errors() {
			fmt.Printf("Error from consumer group: %v\n", err)
		}
	}()

	fmt.Printf("Kafka consumer listens topic : %+v \n", topicParams)
}

func (c *kafkaConsumer) Unsubscribe() {
	c.cancelFunc()
	if err := c.consumerGroup.Close(); err != nil {
		fmt.Printf("Client wasn't closed: %v\n", err)
	}
	fmt.Println("Kafka consumer closed")
}
