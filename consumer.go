package kafka_wrapper

import (
	"context"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

type Consumer interface {
	Subscribe(handler EventHandler)
	Unsubscribe()
}

type EventHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
}

type kafkaConsumer struct {
	topic         []string
	retryTopic    string
	errorTopic    string
	consumerGroup sarama.ConsumerGroup
}

func NewConsumer(connectionParams ConnectionParameters) (Consumer, error) {
	v, err := sarama.ParseKafkaVersion(connectionParams.Version)
	if err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Return.Errors = true
	config.ClientID = connectionParams.ClientID
	if connectionParams.FromBeginning {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 5 * time.Second

	cg, err := sarama.NewConsumerGroup(strings.Split(connectionParams.Brokers, ","), connectionParams.ConsumerGroupID, config)
	if err != nil {
		return nil, err
	}

	return &kafkaConsumer{
		topic:         connectionParams.Topic,
		retryTopic:    connectionParams.RetryTopic,
		errorTopic:    connectionParams.ErrorTopic,
		consumerGroup: cg,
	}, nil
}

func (c *kafkaConsumer) Subscribe(handler EventHandler) {
	ctx := context.Background()
	topics := func() []string {
		result := make([]string, 0)
		if c.errorTopic != "" {
			result = append(result, c.errorTopic)
		}
		if c.retryTopic != "" {
			result = append(result, c.retryTopic)
		}
		result = append(result, c.topic...)
		return result
	}

	go func() {
		for {
			if err := c.consumerGroup.Consume(ctx, topics(), handler);
				err != nil {
					Logger.Println("Error from consumer : ", err.Error())
				return
			}

			if ctx.Err() != nil {
				Logger.Println("Error from consumer : ", ctx.Err().Error())
				return
			}
		}
	}()
	go func() {
		for err := range c.consumerGroup.Errors() {
			Logger.Println("Error from consumer group : ", err.Error())
		}
	}()
	Logger.Printf("Kafka  consumer listens topic : %v \n", c.topic)
}

func (c *kafkaConsumer) Unsubscribe() {
	if err := c.consumerGroup.Close(); err != nil {
		Logger.Printf("Client wasn't closed :%+v", err)
	}
	Logger.Println("Kafka consumer closed")
}
