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
}

func NewRemoteConsumer(remoteParams params.ConnectionParameters, localParams params.ConnectionParameters) (Consumer, error) {
	remote, err := sarama.NewConsumerGroup(strings.Split(remoteParams.Brokers, ","), remoteParams.ConsumerGroupID, remoteParams.Conf)
	local, err := sarama.NewConsumerGroup(strings.Split(localParams.Brokers, ","), localParams.ConsumerGroupID, localParams.Conf)
	if err != nil {
		return nil, err
	}

	return &remoteKafkaConsumer{
		remoteConsumer: remote,
		localConsumer:  local,
	}, nil
}

func (c *remoteKafkaConsumer) SubscribeToTopic(topicParams params.TopicsParameters, handler EventHandler) {
	c.Subscribe([]params.TopicsParameters{topicParams}, handler)
}

func (c *remoteKafkaConsumer) Subscribe(topicParams []params.TopicsParameters, handler EventHandler) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {

			if err := c.remoteConsumer.Consume(ctx, params.JoinMainTopics(topicParams), handler); err != nil {
				fmt.Printf("Error from consumer: %+v \n", err.Error())
			}

			fmt.Printf("Subscribed topics: %+v to remote kafka server %+v \n", params.JoinMainTopics(topicParams), c.remoteConsumer)

			if ctx.Err() != nil {
				fmt.Printf("Error from consumer: %+v \n", ctx.Err().Error())
			}
		}
	}()

	go func() {
		for {

			if err := c.localConsumer.Consume(ctx, params.JoinSecondaryTopics(topicParams), handler); err != nil {
				fmt.Printf("Error from consumer: %+v \n", err.Error())
			}

			fmt.Printf("Subscribed topics: %+v to local kafka server \n", params.JoinSecondaryTopics(topicParams))

			if ctx.Err() != nil {
				fmt.Printf("Error from consumer: %+v \n", ctx.Err().Error())
			}
		}
	}()

	go func() {
		for err := range c.remoteConsumer.Errors() {
			fmt.Printf("Error from consumer group: %+v \n", err.Error())
		}

		for err := range c.localConsumer.Errors() {
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

func (c *remoteKafkaConsumer) Unsubscribe() {

	if err := c.remoteConsumer.Close(); err != nil {
		fmt.Printf("Client wasn't closed: %+v \n", err)
	}
	if err := c.localConsumer.Close(); err != nil {
		fmt.Printf("Client wasn't closed: %+v \n", err)
	}
	fmt.Println("Kafka consumer closed")
}
