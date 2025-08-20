package kafka_wrapper

import (
	"context"
	"strings"

	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
)

type remoteKafkaConsumer struct {
	remoteConsumer sarama.ConsumerGroup
	localConsumer  sarama.ConsumerGroup
	ctx            context.Context
	cancelFunc     context.CancelFunc
	loggerHelper   *params.LoggerHelper
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
		loggerHelper:   params.NewLoggerHelper(remoteParams.Logger),
	}, nil
}
func (c *remoteKafkaConsumer) SubscribeToTopic(topicParams params.TopicsParameters, handler EventHandler) {
	c.Subscribe([]params.TopicsParameters{topicParams}, handler)
}

func (c *remoteKafkaConsumer) Subscribe(topicParams []params.TopicsParameters, handler EventHandler) {
	ctx := context.Background()

	go func() {
		for {
			if c.ctx.Err() != nil {
				return
			}
			err := c.remoteConsumer.Consume(c.ctx, params.JoinMainTopics(topicParams, c.loggerHelper), handler)
			if err != nil {
				c.loggerHelper.Error(ctx, "Error from remote consumer: %v", err)
			}
		}
	}()

	go func() {
		for {
			if c.ctx.Err() != nil {
				return
			}
			err := c.localConsumer.Consume(c.ctx, params.JoinSecondaryTopics(topicParams, c.loggerHelper), handler)
			if err != nil {
				c.loggerHelper.Error(ctx, "Error from local consumer: %v", err)
			}
		}
	}()

	go func() {
		for err := range c.remoteConsumer.Errors() {
			c.loggerHelper.Error(ctx, "Error from remote consumer group: %v", err)
		}
	}()

	go func() {
		for err := range c.localConsumer.Errors() {
			c.loggerHelper.Error(ctx, "Error from local consumer group: %v", err)
		}
	}()

	c.loggerHelper.Info(ctx, "Kafka consumer listens topic: %+v", topicParams)
}

func (c *remoteKafkaConsumer) Unsubscribe() {
	c.cancelFunc()

	ctx := context.Background()

	if err := c.remoteConsumer.Close(); err != nil {
		c.loggerHelper.Error(ctx, "Remote client wasn't closed: %v", err)
	}
	if err := c.localConsumer.Close(); err != nil {
		c.loggerHelper.Error(ctx, "Local client wasn't closed: %v", err)
	}

	c.loggerHelper.Info(ctx, "Kafka consumer closed", nil)
}
