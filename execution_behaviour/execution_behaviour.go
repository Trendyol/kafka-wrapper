package execution_behaviour

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
)

type BehavioralSelector interface {
	GetBehavioral(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor
}

type retryBehaviourSelector struct {
	normalOperator behavioral.LogicOperator
	errorOperator  behavioral.LogicOperator
	producer       sarama.SyncProducer
	retryTopic     string
	errorTopic     string
	retryCount     int
}

func NewRetryBehaviourSelector(normalOperator behavioral.LogicOperator, errorOperator behavioral.LogicOperator, producer sarama.SyncProducer,
	retryCount int, retryTopic, errorTopic string) *retryBehaviourSelector {
	return &retryBehaviourSelector{
		normalOperator: normalOperator,
		producer:       producer,
		retryTopic:     retryTopic,
		errorTopic:     errorTopic,
		retryCount:     retryCount,
		errorOperator:  errorOperator,
	}
}

func (r *retryBehaviourSelector) GetBehavioral(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor {
	if claim.Topic() == r.retryTopic {
		return behavioral.RetryBehavioral(r.producer, r.errorTopic, r.normalOperator, r.retryCount)
	} else if claim.Topic() == r.errorTopic {
		return behavioral.ErrorBehavioral(r.errorOperator)
	} else {
		return behavioral.NormalBehavioral(r.producer, r.retryTopic, r.normalOperator)
	}
}
