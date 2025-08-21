package execution_behaviour

import (
	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
	"github.com/Trendyol/kafka-wrapper/params"
	"github.com/Trendyol/kafka-wrapper/utils"
)

type BehavioralSelector interface {
	GetBehavioral(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor
}

type behaviourSelector struct {
	normalOperator behavioral.LogicOperator
	errorOperator  behavioral.LogicOperator
	producer       sarama.SyncProducer
	retryTopic     string
	errorTopic     string
	retryCount     int
	headerOperator utils.HeaderOperation
	logger         params.Logger
}

func NewBehaviourSelector(normalOperator behavioral.LogicOperator, errorOperator behavioral.LogicOperator, producer sarama.SyncProducer,
	retryCount int, retryTopic, errorTopic string) *behaviourSelector {
	return &behaviourSelector{
		normalOperator: normalOperator,
		producer:       producer,
		retryTopic:     retryTopic,
		errorTopic:     errorTopic,
		retryCount:     retryCount,
		errorOperator:  errorOperator,
		headerOperator: utils.NewHeaderOperator(),
		logger:         nil,
	}
}

func NewBehaviourSelectorWithLogger(normalOperator behavioral.LogicOperator, errorOperator behavioral.LogicOperator, producer sarama.SyncProducer,
	retryCount int, retryTopic, errorTopic string, logger params.Logger) *behaviourSelector {
	return &behaviourSelector{
		normalOperator: normalOperator,
		producer:       producer,
		retryTopic:     retryTopic,
		errorTopic:     errorTopic,
		retryCount:     retryCount,
		errorOperator:  errorOperator,
		headerOperator: utils.NewHeaderOperator(),
		logger:         logger,
	}
}

func NewRetryOnlyBehavioralSelector(normalOperator behavioral.LogicOperator, producer sarama.SyncProducer,
	retryCount int, retryTopic, errorTopic string) *behaviourSelector {
	return &behaviourSelector{
		normalOperator: normalOperator,
		errorOperator:  nil,
		producer:       producer,
		retryTopic:     retryTopic,
		errorTopic:     errorTopic,
		retryCount:     retryCount,
		headerOperator: utils.NewHeaderOperator(),
		logger:         nil,
	}
}

func NewRetryOnlyBehavioralSelectorWithLogger(normalOperator behavioral.LogicOperator, producer sarama.SyncProducer,
	retryCount int, retryTopic, errorTopic string, logger params.Logger) *behaviourSelector {
	return &behaviourSelector{
		normalOperator: normalOperator,
		errorOperator:  nil,
		producer:       producer,
		retryTopic:     retryTopic,
		errorTopic:     errorTopic,
		retryCount:     retryCount,
		headerOperator: utils.NewHeaderOperator(),
		logger:         logger,
	}
}

func (r *behaviourSelector) GetBehavioral(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor {
	if claim.Topic() == r.retryTopic {
		return behavioral.RetryBehavioral(r.producer, r.errorTopic, r.normalOperator, r.retryCount, r.headerOperator, r.logger)
	} else if r.errorOperator != nil && claim.Topic() == r.errorTopic {
		return behavioral.ErrorBehavioral(r.errorOperator)
	} else {
		return behavioral.NormalBehavioral(r.producer, r.retryTopic, r.normalOperator, r.logger)
	}
}
