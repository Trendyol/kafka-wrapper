package kafka_tracing

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestConsumerMessageCarrier_Get(t *testing.T) {
	msg := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{
			{Key: []byte("traceparent"), Value: []byte("00-abc123-def456-01")},
			{Key: []byte("tracestate"), Value: []byte("vendor=value")},
			{Key: []byte("correlation-id"), Value: []byte("corr-123")},
		},
	}

	carrier := NewConsumerMessageCarrier(msg)

	assert.Equal(t, "00-abc123-def456-01", carrier.Get("traceparent"))
	assert.Equal(t, "vendor=value", carrier.Get("tracestate"))
	assert.Equal(t, "corr-123", carrier.Get("correlation-id"))
	assert.Equal(t, "", carrier.Get("non-existent"))
}

func TestConsumerMessageCarrier_Get_NilHeader(t *testing.T) {
	msg := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{
			nil,
			{Key: []byte("traceparent"), Value: []byte("00-abc123-def456-01")},
		},
	}

	carrier := NewConsumerMessageCarrier(msg)

	assert.Equal(t, "00-abc123-def456-01", carrier.Get("traceparent"))
	assert.Equal(t, "", carrier.Get("non-existent"))
}

func TestConsumerMessageCarrier_Set(t *testing.T) {
	msg := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{},
	}

	carrier := NewConsumerMessageCarrier(msg)
	carrier.Set("traceparent", "00-abc123-def456-01")

	assert.Len(t, msg.Headers, 1)
	assert.Equal(t, "traceparent", string(msg.Headers[0].Key))
	assert.Equal(t, "00-abc123-def456-01", string(msg.Headers[0].Value))
}

func TestConsumerMessageCarrier_Keys(t *testing.T) {
	msg := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{
			{Key: []byte("traceparent"), Value: []byte("val1")},
			{Key: []byte("tracestate"), Value: []byte("val2")},
		},
	}

	carrier := NewConsumerMessageCarrier(msg)
	keys := carrier.Keys()

	assert.Equal(t, []string{"traceparent", "tracestate"}, keys)
}

func TestConsumerMessageCarrier_Keys_WithNil(t *testing.T) {
	msg := &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{
			nil,
			{Key: []byte("traceparent"), Value: []byte("val1")},
		},
	}

	carrier := NewConsumerMessageCarrier(msg)
	keys := carrier.Keys()

	assert.Equal(t, []string{"traceparent"}, keys)
}
