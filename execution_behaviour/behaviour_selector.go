package execution_behaviour

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
	"github.com/Trendyol/kafka-wrapper/utils"
)

const (
	RetrySuffix = "_retry"
	ErrorSuffix = "_error"
)

type BehaviorSelection interface {
	GetBehavior(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor
}

type behaviorSelector struct {
	normalBehaviour behavioral.BehaviourExecutor
	retryBehaviour  behavioral.BehaviourExecutor
	errorBehaviour  behavioral.BehaviourExecutor
}

func NewBehaviourSelector(normalBehaviour behavioral.BehaviourExecutor, retryBehaviour behavioral.BehaviourExecutor, errorBehaviour behavioral.BehaviourExecutor) *behaviorSelector {
	return &behaviorSelector{
		normalBehaviour: normalBehaviour,
		retryBehaviour:  retryBehaviour,
		errorBehaviour:  errorBehaviour,
	}
}

func (r *behaviorSelector) GetBehavior(claim sarama.ConsumerGroupClaim) behavioral.BehaviourExecutor {
	if utils.HasSuffixCaseInsensitive(claim.Topic(), RetrySuffix) {
		return r.retryBehaviour
	} else if utils.HasSuffixCaseInsensitive(claim.Topic(), ErrorSuffix) {
		return r.errorBehaviour
	} else {
		return r.normalBehaviour
	}
}
