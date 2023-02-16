package behavioral

import (
	"context"
	"github.com/Shopify/sarama"
)

type LogicOperator interface {
	Operate(ctx context.Context, message *sarama.ConsumerMessage) error
}
