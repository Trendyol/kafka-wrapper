package execution_behaviour

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
	"github.com/Trendyol/kafka-wrapper/slack_api_client"
)

type BehavioralSelector interface {
	GetBehavioral(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor
}

type retryBehaviourSelector struct {
	executor       behavioral.BehaviourExecutor
	logicOperator  behavioral.LogicOperator
	producer       sarama.SyncProducer
	slackApiClient slack_api_client.Client
	retryTopic     string
	errorTopic     string
	retryCount     int
	title          string
}

func NewRetryBehaviourSelector(logicOperator behavioral.LogicOperator, producer sarama.SyncProducer, slackApiClient slack_api_client.Client, retryCount int, retryTopic, errorTopic,
	title string) *retryBehaviourSelector {
	return &retryBehaviourSelector{
		logicOperator:  logicOperator,
		producer:       producer,
		slackApiClient: slackApiClient,
		title:          title,
		retryTopic:     retryTopic,
		errorTopic:     errorTopic,
		retryCount:     retryCount,
	}
}

func (r *retryBehaviourSelector) GetBehavioral(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor {
	if claim.Topic() == r.retryTopic {
		return behavioral.RetryBehavioral(r.producer, r.errorTopic, r.logicOperator, r.retryCount)
	} else if claim.Topic() == r.errorTopic {
		return behavioral.ErrorBehavioral(r.slackApiClient, r.title)
	} else {
		return behavioral.NormalBehavioral(r.producer, r.retryTopic, r.logicOperator)
	}
}
