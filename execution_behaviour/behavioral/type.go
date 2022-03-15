package behavioral

import (
	"context"

	"github.com/Shopify/sarama"
)

type BehaviourExecutor interface {
	Process(ctx context.Context, message *sarama.ConsumerMessage) error
}
