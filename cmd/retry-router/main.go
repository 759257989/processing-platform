// cmd/retry-router: drains jobs.<tier>.retry topics, waits until each
// message's retry-at timestamp, and republishes to jobs.<tier> for normal
// processing.
//
// We run THIS as a separate service rather than inside workers because:
//   1. A worker holding a message with time.Sleep blocks its Kafka partition
//      for everyone else on that partition.
//   2. Centralizing retry logic makes it tunable and observable (one log
//      stream for all retries).
package main

import (
    "context"
    "log/slog"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "syscall"
    "time"

    segkafka "github.com/segmentio/kafka-go"

    "github.com/759257989/processing-platform/internal/kafka"
)

func main() {
    log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
    slog.SetDefault(log)

    brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    producer := kafka.NewProducer(brokers)
    defer producer.Close()

    // One goroutine per retry topic. Each tier has its own retry topic and
    // its own goroutine drains it independently. They share the producer.
    tiers := []string{"realtime", "standard", "bulk"}
    for _, tier := range tiers {
        go drain(ctx, log, brokers, tier, producer)
    }

    <-ctx.Done()
    log.Info("shutting down")
}

func drain(ctx context.Context, log *slog.Logger, brokers []string, tier string, producer *kafka.Producer) {
    retryTopic := "jobs." + tier + ".retry"
    targetTopic := "jobs." + tier
    groupID := "retry-router-" + tier

    consumer := kafka.NewConsumer(brokers, retryTopic, groupID)
    defer consumer.Close()

    log.Info("retry-router draining", "topic", retryTopic)

    for {
        msg, err := consumer.FetchMessage(ctx)
        if err != nil {
            if ctx.Err() != nil {
                return
            }
            log.Error("fetch retry msg", "err", err)
            time.Sleep(time.Second)
            continue
        }

        // Read retry-at header.
        retryAt := time.Now()
        for _, h := range msg.Headers {
            if h.Key == "retry-at" {
                if n, err := strconv.ParseInt(string(h.Value), 10, 64); err == nil {
                    retryAt = time.Unix(n, 0)
                }
            }
        }

        // Sleep until retry-at, but be responsive to ctx cancellation.
        delay := time.Until(retryAt)
        if delay > 0 {
            timer := time.NewTimer(delay)
            select {
            case <-timer.C:
            case <-ctx.Done():
                timer.Stop()
                return
            }
        }

        // Republish to the target topic without the retry-at header
        // (it's no longer relevant).
        if err := producer.Publish(ctx, targetTopic,
            []byte(msg.Key), msg.Value); err != nil {
            log.Error("republish failed", "err", err, "topic", targetTopic)
            // Don't commit — let it redeliver.
            continue
        }

        if err := consumer.CommitMessage(ctx, msg); err != nil {
            log.Error("commit retry msg", "err", err)
        }
    }
}

func mustEnv(k string) string {
    v := os.Getenv(k)
    if v == "" {
        os.Stderr.WriteString("env " + k + " required\n")
        os.Exit(1)
    }
    return v
}

// segkafka import is unused in this file but keeps go vet happy if you
// later add direct kafka.Message handling. Safe to remove if unused.
var _ = segkafka.Message{}
