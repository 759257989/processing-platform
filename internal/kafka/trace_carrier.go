package kafka

import (
	"github.com/segmentio/kafka-go"
)

// kafkaHeaderCarrier adapts []kafka.Header to OTel's propagation.TextMapCarrier
// interface. OTel propagators write traceparent / tracestate via Set on the
// producer side, and read them via Get on the consumer side — passing through
// Kafka's existing header mechanism.
//
// Without this, every Kafka message creates a new trace on the consumer; with
// it, the consumer span becomes a child of the producer span.
type kafkaHeaderCarrier []kafka.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, val string) {
	// Replace if exists (some propagators set the same key twice)
	for i, h := range *c {
		if h.Key == key {
			(*c)[i].Value = []byte(val)
			return
		}
	}
	*c = append(*c, kafka.Header{Key: key, Value: []byte(val)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for _, h := range c {
		out = append(out, h.Key)
	}
	return out
}
