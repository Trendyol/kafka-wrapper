package kafka_wrapper

import (
	"context"
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
}

func NewConsumer(connectionParams params.ConnectionParameters) (Consumer, error) {
	cg, err := sarama.NewConsumerGroup(strings.Split(connectionParams.Brokers, ","), connectionParams.ConsumerGroupID, connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	return &kafkaConsumer{
		consumerGroup: cg,
	}, nil
}

func (c *kafkaConsumer) SubscribeToTopic(topicParams params.TopicsParameters, handler EventHandler) {
	c.Subscribe([]params.TopicsParameters{topicParams}, handler)
}

func (c *kafkaConsumer) Subscribe(topicParams []params.TopicsParameters, handler EventHandler) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			if err := c.consumerGroup.Consume(ctx, params.JoinAllTopics(topicParams), handler); err != nil {
				fmt.Printf("Error from consumer: %+v \n", err.Error())
			}

			if ctx.Err() != nil {
				fmt.Printf("Error from consumer: %+v \n", ctx.Err().Error())
			}
		}
	}()

	go func() {
		for err := range c.consumerGroup.Errors() {
			fmt.Printf("Error from consumer group: %+v \n", err.Error())
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("terminating: context cancelled")
				cancel()
			}
		}
	}()
	fmt.Printf("Kafka consumer listens topic : %+v \n", topicParams)
}

func (c *kafkaConsumer) Unsubscribe() {
	if err := c.consumerGroup.Close(); err != nil {
		fmt.Printf("Client wasn't closed: %+v \n", err)
	}
	fmt.Println("Kafka consumer closed")
}
