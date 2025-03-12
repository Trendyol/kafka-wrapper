package kafka_wrapper

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
	"strings"
)

type remoteKafkaConsumer struct {
	remoteConsumer sarama.ConsumerGroup
	localConsumer  sarama.ConsumerGroup
	ctx            context.Context
	cancelFunc     context.CancelFunc
}

func NewRemoteConsumer(remoteParams params.ConnectionParameters, localParams params.ConnectionParameters) (Consumer, error) {
	remote, err := sarama.NewConsumerGroup(strings.Split(remoteParams.Brokers, ","), remoteParams.ConsumerGroupID, remoteParams.Conf)
	if err != nil {
		return nil, err
	}
	local, err := sarama.NewConsumerGroup(strings.Split(localParams.Brokers, ","), localParams.ConsumerGroupID, localParams.Conf)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &remoteKafkaConsumer{
		remoteConsumer: remote,
		localConsumer:  local,
		ctx:            ctx,
		cancelFunc:     cancel,
	}, nil
}
func (c *remoteKafkaConsumer) SubscribeToTopic(topicParams params.TopicsParameters, handler EventHandler) {
	c.Subscribe([]params.TopicsParameters{topicParams}, handler)
}

func (c *remoteKafkaConsumer) Subscribe(topicParams []params.TopicsParameters, handler EventHandler) {
	go func() {
		for {
			if c.ctx.Err() != nil {
				return
			}
			err := c.remoteConsumer.Consume(c.ctx, params.JoinMainTopics(topicParams), handler)
			if err != nil {
				fmt.Printf("Error from remote consumer: %v\n", err)
			}
		}
	}()

	go func() {
		for {
			if c.ctx.Err() != nil {
				return
			}
			err := c.localConsumer.Consume(c.ctx, params.JoinSecondaryTopics(topicParams), handler)
			if err != nil {
				fmt.Printf("Error from local consumer: %v\n", err)
			}
		}
	}()

	go func() {
		for err := range c.remoteConsumer.Errors() {
			fmt.Printf("Error from remote consumer group: %v\n", err)
		}
	}()

	go func() {
		for err := range c.localConsumer.Errors() {
			fmt.Printf("Error from local consumer group: %v\n", err)
		}
	}()

	fmt.Printf("Kafka consumer listens topic: %+v\n", topicParams)
}

func (c *remoteKafkaConsumer) Unsubscribe() {
	c.cancelFunc()

	if err := c.remoteConsumer.Close(); err != nil {
		fmt.Printf("Remote consumer wasn't closed: %v\n", err)
	}
	if err := c.localConsumer.Close(); err != nil {
		fmt.Printf("Local consumer wasn't closed: %v\n", err)
	}
	fmt.Println("Kafka consumer closed")
}
