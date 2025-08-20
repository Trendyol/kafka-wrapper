package kafka_wrapper

import (
	"context"
	"errors"
	"strings"

	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
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
	loggerHelper  *params.LoggerHelper
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
		loggerHelper:  params.NewLoggerHelper(connectionParams.Logger),
	}, nil
}

func (c *kafkaConsumer) SubscribeToTopic(topicParams params.TopicsParameters, handler EventHandler) {
	c.Subscribe([]params.TopicsParameters{topicParams}, handler)
}

func (c *kafkaConsumer) Subscribe(topicParams []params.TopicsParameters, handler EventHandler) {
	ctx := context.Background()

	go func() {
		for {
			if c.ctx.Err() != nil {
				c.loggerHelper.Error(ctx, "Consumer context canceled, exiting: %v", c.ctx.Err())
				return
			}

			err := c.consumerGroup.Consume(c.ctx, params.JoinAllTopics(topicParams), handler)
			if err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				c.loggerHelper.Error(ctx, "Error from consumer: %v", err)
			}
		}
	}()

	go func() {
		for err := range c.consumerGroup.Errors() {
			c.loggerHelper.Error(ctx, "Error from consumer group: %v", err)
		}
	}()

	c.loggerHelper.Info(ctx, "Kafka consumer listens topic : %+v ", topicParams)
}

func (c *kafkaConsumer) Unsubscribe() {
	c.cancelFunc()

	ctx := context.Background()

	if err := c.consumerGroup.Close(); err != nil {
		c.loggerHelper.Error(ctx, "Client wasn't closed: %v", err)
	}

	c.loggerHelper.Info(ctx, "Kafka consumer closed", nil)
}
