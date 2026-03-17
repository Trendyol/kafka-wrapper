package kafka_wrapper

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
	"github.com/Trendyol/kafka-wrapper/params"
)

const (
	defaultBatchRoutineCount = 1
	defaultBatchBufferSize   = 100
)

// ContextEnricher is used for project-specific context enrichment (tracing, otel, etc.).
// Enrich reads headers from the message and writes them to context.
type ContextEnricher interface {
	Enrich(ctx context.Context, message *sarama.ConsumerMessage) (context.Context)
}

// BatchEventHandlerConfig is the configuration for the batch event handler.
type BatchEventHandlerConfig struct {
	// RoutineCount is the number of workers that process messages (defaults to 1 if 0).
	RoutineCount int
	// BufferSize is the channel buffer size (default is used if 0).
	BufferSize int
	// ContextEnricher is optional; if nil, context.Background() is used.
	ContextEnricher ContextEnricher
	// Logger is optional; used for error logging.
	Logger params.Logger
}

type batchMessage struct {
	session   sarama.ConsumerGroupSession
	message   *sarama.ConsumerMessage
	processor behavioral.BehaviourExecutor
}

type batchEventHandler struct {
	cfg        *BatchEventHandlerConfig
	selector   execution_behaviour.BehavioralSelector
	msgCh      chan *batchMessage
}

// NewBatchEventHandler returns the shared batch event handler with the given BehavioralSelector and config.
// Projects can use this handler by providing only a ContextEnricher (tracing/newrelic, etc.) and selector.
func NewBatchEventHandler(selector execution_behaviour.BehavioralSelector, cfg *BatchEventHandlerConfig) EventHandler {
	if cfg == nil {
		cfg = &BatchEventHandlerConfig{}
	}
	routineCount := cfg.RoutineCount
	if routineCount == 0 {
		routineCount = defaultBatchRoutineCount
	}
	bufferSize := cfg.BufferSize
	if bufferSize == 0 {
		bufferSize = defaultBatchBufferSize
	}

	msgCh := make(chan *batchMessage, bufferSize)
	h := &batchEventHandler{
		cfg:      cfg,
		selector: selector,
		msgCh:    msgCh,
	}

	for i := 0; i < routineCount; i++ {
		go h.worker(msgCh)
	}

	return h
}

func (h *batchEventHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *batchEventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *batchEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	processor := h.selector.GetBehavioral(claim)
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			h.msgCh <- &batchMessage{
				session:   session,
				message:   message,
				processor: processor,
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *batchEventHandler) worker(msgCh chan *batchMessage) {
	for work := range msgCh {
		ctx := context.Background()
		if h.cfg.ContextEnricher != nil {
			ctx = h.cfg.ContextEnricher.Enrich(ctx, work.message)
		}
		if err := work.processor.Process(ctx, work.message); err != nil {
			if h.cfg.Logger != nil {
				h.cfg.Logger.Error(ctx, "batch event handler process error: %v", err)
			} else {
				fmt.Printf("batch event handler process error: %v\n", err)
			}
		}
		work.session.MarkMessage(work.message, "")
	}
}
