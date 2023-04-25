package behavioral

import (
	"context"
	"github.com/Shopify/sarama"
)

//go:generate mockgen -destination=../../mocks/behavior_executor_mock.go -package=mocks -source=type.go

type BehaviourExecutor interface {
	Process(ctx context.Context, message *sarama.ConsumerMessage) error
}
