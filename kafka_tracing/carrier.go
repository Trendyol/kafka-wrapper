package kafka_tracing

import (
	"github.com/IBM/sarama"
)

// ConsumerMessageCarrier implements propagation.TextMapCarrier for extracting
// and injecting trace context from/to Kafka consumer message headers.
type ConsumerMessageCarrier struct {
	msg *sarama.ConsumerMessage
}

// NewConsumerMessageCarrier creates a new ConsumerMessageCarrier.
func NewConsumerMessageCarrier(msg *sarama.ConsumerMessage) ConsumerMessageCarrier {
	return ConsumerMessageCarrier{msg: msg}
}

// Get retrieves a single value for a given key from the message headers.
func (c ConsumerMessageCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set sets a header on the message.
func (c ConsumerMessageCarrier) Set(key, val string) {
	c.msg.Headers = append(c.msg.Headers, &sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(val),
	})
}

// Keys returns a slice of all key identifiers in the carrier.
func (c ConsumerMessageCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Headers))
	for _, h := range c.msg.Headers {
		if h != nil {
			keys = append(keys, string(h.Key))
		}
	}
	return keys
}
