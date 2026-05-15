package kafka

import (
	"context"

	"github.com/759257989/processing-platform/internal/observability"
)

// InstrumentedProducer wraps Producer with metrics. Drop-in replacement.
type InstrumentedProducer struct {
	*Producer
	m *observability.Metrics
}

func NewInstrumented(p *Producer, m *observability.Metrics) *InstrumentedProducer {
	return &InstrumentedProducer{Producer: p, m: m}
}

func (ip *InstrumentedProducer) Publish(ctx context.Context, topic string, key, value []byte) error {
	err := ip.Producer.Publish(ctx, topic, key, value)
	result := "success"
	if err != nil {
		result = "error"
	}
	ip.m.KafkaProduceTotal.WithLabelValues(topic, result).Inc()
	return err
}
