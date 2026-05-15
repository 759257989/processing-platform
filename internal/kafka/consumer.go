package kafka

import (
    "context"
    "time"

    "github.com/segmentio/kafka-go"
    "go.opentelemetry.io/otel"
)

// Consumer wraps a kafka.Reader configured for our consumer-group pattern.
type Consumer struct {
    r *kafka.Reader
}

// NewConsumer builds a Consumer for `topic` under `groupID`. All workers
// in the same group share the partitions of the topic; each partition is
// owned by exactly one worker at a time.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
    return &Consumer{
        r: kafka.NewReader(kafka.ReaderConfig{
            Brokers:        brokers,
            Topic:          topic,
            GroupID:        groupID,
            MinBytes:       1,
            MaxBytes:       10e6, // 10 MB
            CommitInterval: 0,    // 0 disables auto-commit; we commit manually
            MaxWait:        500 * time.Millisecond,
        }),
    }
}

// FetchMessage blocks until a message is available or ctx is done.
// Caller must call CommitMessage after successful processing.
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
    return c.r.FetchMessage(ctx)
}

// FetchMessageWithCtx is like FetchMessage but also returns a context that
// has the producer's trace context extracted from message headers. Use the
// returned ctx for downstream processing (handler, DB calls, follow-up
// Publish) so the entire processing path stays in the same trace.
//
// If the message has no traceparent header the returned ctx == input ctx;
// no harm done, just a fresh trace will start.
func (c *Consumer) FetchMessageWithCtx(ctx context.Context) (kafka.Message, context.Context, error) {
    msg, err := c.r.FetchMessage(ctx)
    if err != nil {
        return msg, ctx, err
    }
    carrier := kafkaHeaderCarrier(msg.Headers)
    // Extract needs propagation.TextMapCarrier interface, which requires Set
    // (pointer receiver). Pass &carrier even though Extract only reads.
    enriched := otel.GetTextMapPropagator().Extract(ctx, &carrier)
    return msg, enriched, nil
}

// CommitMessage marks the message as successfully processed.
// Only commit after the work is durably persisted (DB write succeeded).
func (c *Consumer) CommitMessage(ctx context.Context, msg kafka.Message) error {
    return c.r.CommitMessages(ctx, msg)
}

// Close shuts down the reader.
func (c *Consumer) Close() error {
    return c.r.Close()
}
