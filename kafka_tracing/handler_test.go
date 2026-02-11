package kafka_tracing

import (
	"testing"

	"github.com/IBM/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
	"github.com/stretchr/testify/assert"
)

func TestWrapEventHandler_ReturnsWrappedHandler(t *testing.T) {
	mockHandler := &mockEventHandler{}
	wrapped := WrapEventHandler(mockHandler)

	assert.NotNil(t, wrapped)
	assert.IsType(t, &otelEventHandler{}, wrapped)

	otelHandler := wrapped.(*otelEventHandler)
	assert.Equal(t, mockHandler, otelHandler.delegate)
	assert.NotNil(t, otelHandler.tracer)
	assert.NotNil(t, otelHandler.propagator)
}

type mockEventHandler struct{}

func (m *mockEventHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (m *mockEventHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (m *mockEventHandler) ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error {
	return nil
}

// Compile-time check that mockEventHandler implements EventHandler
var _ kafka_wrapper.EventHandler = &mockEventHandler{}
