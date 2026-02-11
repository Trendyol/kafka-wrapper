package kafka_tracing

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/Trendyol/kafka-wrapper/kafka_tracing"
)

// otelEventHandler wraps an EventHandler with OpenTelemetry tracing.
type otelEventHandler struct {
	delegate   kafka_wrapper.EventHandler
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// WrapEventHandler wraps an EventHandler with OpenTelemetry instrumentation.
// The returned handler creates a span for each consumed message and propagates
// trace context extracted from the message headers.
func WrapEventHandler(handler kafka_wrapper.EventHandler) kafka_wrapper.EventHandler {
	return &otelEventHandler{
		delegate:   handler,
		tracer:     otel.GetTracerProvider().Tracer(instrumentationName),
		propagator: otel.GetTextMapPropagator(),
	}
}

func (h *otelEventHandler) Setup(session sarama.ConsumerGroupSession) error {
	return h.delegate.Setup(session)
}

func (h *otelEventHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return h.delegate.Cleanup(session)
}

func (h *otelEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgs := make(chan *sarama.ConsumerMessage)
	proxyClaim := &otelConsumerGroupClaim{
		ConsumerGroupClaim: claim,
		messages:           msgs,
	}

	// Dispatcher goroutine: reads from the original claim, creates spans,
	// and forwards messages to the proxy channel.
	go func() {
		defer close(msgs)
		var prevSpan trace.Span

		for {
			select {
			case msg, ok := <-claim.Messages():
				if !ok {
					if prevSpan != nil {
						prevSpan.End()
					}
					return
				}

				// End the previous message's span (processing is complete
				// once we receive the next message from the delegate).
				if prevSpan != nil {
					prevSpan.End()
				}

				// Extract parent trace context from message headers.
				parentCtx := h.propagator.Extract(context.Background(), NewConsumerMessageCarrier(msg))

				attrs := []attribute.KeyValue{
					semconv.MessagingSystem("kafka"),
					semconv.MessagingOperationReceive,
					semconv.MessagingDestinationName(msg.Topic),
					semconv.MessagingKafkaMessageOffset(int(msg.Offset)),
					semconv.MessagingKafkaDestinationPartition(int(msg.Partition)),
				}

				spanName := fmt.Sprintf("%s receive", msg.Topic)
				_, span := h.tracer.Start(parentCtx, spanName,
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(attrs...),
				)
				prevSpan = span

				// Forward the message to the proxy channel.
				select {
				case msgs <- msg:
				case <-session.Context().Done():
					span.End()
					return
				}

			case <-session.Context().Done():
				if prevSpan != nil {
					prevSpan.End()
				}
				return
			}
		}
	}()

	return h.delegate.ConsumeClaim(session, proxyClaim)
}

// otelConsumerGroupClaim is a proxy around sarama.ConsumerGroupClaim that
// replaces the Messages() channel with a traced one.
type otelConsumerGroupClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

// Messages returns the proxy messages channel with OTel tracing applied.
func (c *otelConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}
