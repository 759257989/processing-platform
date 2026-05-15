// Package kafka wraps segmentio/kafka-go with our project conventions.
package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.opentelemetry.io/otel"
)

// Producer wraps a kafka.Writer with our defaults.
type Producer struct {
	w *kafka.Writer
}

// NewProducer builds a Producer that connects to the given brokers.
// Brokers is a comma-separated list, e.g. "pp-kafka:9092".
func NewProducer(brokers []string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.Hash{}, // partition by key
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
			Compression:  compress.Lz4,
			RequiredAcks: kafka.RequireAll, // acks=all → no data loss on broker failure
			Async:        false,            // synchronous = caller knows whether publish succeeded
		},
	}
}

// Publish sends one message to the named topic. The key partitions by device_id
// so messages for the same device land on the same partition (preserving order).
//
// Injects the current span's trace context (W3C traceparent) into Kafka headers
// so the consumer side can continue the same trace. If ctx has no active span
// the propagator just writes nothing.
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	headers := kafkaHeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: []kafka.Header(headers),
	})
}

// Close flushes pending messages and shuts down the writer.
func (p *Producer) Close() error {
	return p.w.Close()
}

// PublishWithHeader sends a message with one custom header. Used by the
// worker to attach "retry-at: <unix-ts>" when scheduling a retry.
//
// Same trace-context injection as Publish, plus the caller's custom header.
func (p *Producer) PublishWithHeader(ctx context.Context, topic string, key, value []byte, headerKey string, headerValue []byte) error {
	headers := kafkaHeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, &headers)
	headers = append(headers, kafka.Header{Key: headerKey, Value: headerValue})
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: []kafka.Header(headers),
	})
}
