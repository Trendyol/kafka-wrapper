package behavioral

import (
	"context"

	"github.com/IBM/sarama"
)

//go:generate mockgen -destination=../../mocks/logic_operator_mock.go -package=mocks -source=logic_executor.go

type LogicOperator interface {
	Operate(ctx context.Context, message *sarama.ConsumerMessage) error
}

// LogicOperatorFunc functional implementation of the LogicOperator interface
type LogicOperatorFunc func(ctx context.Context, message *sarama.ConsumerMessage) error

func (fn LogicOperatorFunc) Operate(ctx context.Context, message *sarama.ConsumerMessage) error {
	return fn(ctx, message)
}

// type-check
var _ LogicOperator = (LogicOperatorFunc)(nil)
