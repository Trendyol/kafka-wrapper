package behavioral

import (
	"context"
	"github.com/Shopify/sarama"
)

type errorBehaviour struct {
	executor LogicOperator
}

func ErrorBehavioral(executor LogicOperator) BehaviourExecutor {
	return &errorBehaviour{
		executor: executor,
	}
}

func (k *errorBehaviour) Process(ctx context.Context, message *sarama.ConsumerMessage) error {
	return k.executor.Operate(ctx, message)
}
