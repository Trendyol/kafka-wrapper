package params

import (
	"context"

	"github.com/IBM/sarama"
)

type Logger interface {
	Error(ctx context.Context, message string, args ...interface{})
	Warn(ctx context.Context, message string, args ...interface{})
	Info(ctx context.Context, message string, args ...interface{})
	Debug(ctx context.Context, message string, args ...interface{})
}

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         string
	ConsumerGroupID string
	Logger          Logger
}
