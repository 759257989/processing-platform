// cmd/worker-standard: standard-tier worker. Consumes jobs.standard and
// dispatches each message through the handler registry.
//
// Identical to worker-realtime and worker-bulk except for three constants:
// topic, group ID, and allowed tier.
package main

import (
    "context"
    "log/slog"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "syscall"

    "github.com/759257989/processing-platform/internal/handlers"
    "github.com/759257989/processing-platform/internal/jobs"
    "github.com/759257989/processing-platform/internal/kafka"
    "github.com/759257989/processing-platform/internal/mockclients"
    "github.com/759257989/processing-platform/internal/store"
    "github.com/759257989/processing-platform/internal/worker"
)

const (
    topic       = "jobs.standard"
    groupID     = "worker-standard"
    allowedTier = jobs.TierStandard
)

func main() {
    log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
    slog.SetDefault(log)

    pgDSN := mustEnv("POSTGRES_DSN")
    brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")
    deviceURL := envOr("DEVICE_URL", "http://mock-device:8080")
    webhookURL := envOr("WEBHOOK_URL", "http://mock-webhook:8080")

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    st, err := store.New(ctx, pgDSN)
    if err != nil {
        log.Error("postgres connect", "err", err)
        os.Exit(1)
    }
    defer st.Close()

    consumer := kafka.NewConsumer(brokers, topic, groupID)
    defer consumer.Close()

    // NEW (Phase 3): producer is used by the worker to publish failed
    // messages to jobs.<tier>.retry or jobs.dlq. Same broker list as the
    // consumer; one producer is shared for both retry and DLQ destinations.
    producer := kafka.NewProducer(brokers)
    defer producer.Close()

    // Wire handler dependencies. The DeviceClient and WebhookClient hit the
    // mock services we'll add in Phase 6. JobSubmitter is the producer
    // wrapper for cross-tier enqueue (also Phase 6).
    deps := handlers.Deps{
        Store:         st,
        DeviceClient:  mockclients.NewDeviceClient(http.DefaultClient, deviceURL),
        WebhookClient: mockclients.NewWebhookClient(http.DefaultClient, webhookURL),
        JobSubmitter:  newKafkaSubmitter(brokers),
    }

    if err := worker.Run(ctx,
        worker.Config{
            Topic:       topic,
            GroupID:     groupID,
            AllowedTier: allowedTier,
            Registry:    handlers.Build(deps),
        },
        worker.Deps{Consumer: consumer, Producer: producer, Store: st, Log: log},
    ); err != nil {
        log.Error("worker exited with error", "err", err)
        os.Exit(1)
    }
}

func mustEnv(k string) string {
    v := os.Getenv(k)
    if v == "" {
        os.Stderr.WriteString("env " + k + " is required\n")
        os.Exit(1)
    }
    return v
}

func envOr(k, fb string) string {
    if v := os.Getenv(k); v != "" {
        return v
    }
    return fb
}

// newKafkaSubmitter is a stub for cross-tier enqueue; we wire it properly
// in Phase 6. For Phases 2–5, returning a no-op is fine because
// HealthCheckHandler is the only handler that uses it, and we won't
// exercise that path until Phase 6.
func newKafkaSubmitter(brokers []string) handlers.JobSubmitter {
    return &noopSubmitter{}
}

type noopSubmitter struct{}

func (noopSubmitter) Submit(ctx context.Context, typ jobs.Type, deviceID, idempKey string, payload []byte) error {
    return nil
}
