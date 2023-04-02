package behavioral

import (
	"context"
	"github.com/Shopify/sarama"
)

//go:generate mockgen -destination=../../mocks/logic_operator_mock.go -package=mocks -source=logic_operator.go

type LogicOperator interface {
	Operate(ctx context.Context, message *sarama.ConsumerMessage) error
}
