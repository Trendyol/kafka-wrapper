package params

import (
	"context"
	"fmt"
)

type LoggerHelper struct {
	logger    Logger
	hasLogger bool
}

func NewLoggerHelper(logger Logger) *LoggerHelper {
	return &LoggerHelper{
		logger:    logger,
		hasLogger: logger != nil,
	}
}

func (h *LoggerHelper) Error(ctx context.Context, message string, args ...interface{}) {
	if h.hasLogger {
		h.logger.Error(ctx, message, args...)
	} else {
		fmt.Printf(message+"\n", args...)
	}
}

func (h *LoggerHelper) Info(ctx context.Context, message string, args ...interface{}) {
	if h.hasLogger {
		h.logger.Info(ctx, message, args...)
	} else {
		fmt.Printf(message+"\n", args...)
	}
}

func (h *LoggerHelper) Warn(ctx context.Context, message string, args ...interface{}) {
	if h.hasLogger {
		h.logger.Warn(ctx, message, args...)
	} else {
		fmt.Printf(message+"\n", args...)
	}
}

func (h *LoggerHelper) Debug(ctx context.Context, message string, args ...interface{}) {
	if h.hasLogger {
		h.logger.Debug(ctx, message, args...)
	} else {
		fmt.Printf(message+"\n", args...)
	}
}

func (h *LoggerHelper) HasLogger() bool {
	return h.hasLogger
}
