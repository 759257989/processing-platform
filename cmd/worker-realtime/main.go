// cmd/worker-realtime: realtime-tier worker. Consumes jobs.realtime and
// dispatches each message through the handler registry.
//
// Identical to worker-standard and worker-bulk except for three constants:
// topic, group ID, and allowed tier. The realtime tier handles latency-
// sensitive jobs (e.g. REMOTE_COMMAND_EXECUTION) where an operator is
// waiting for a result — it has the smallest queue and the most aggressive
// scale-up behaviour (Stage 5).
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
	topic       = "jobs.realtime"
	groupID     = "worker-realtime"
	allowedTier = jobs.TierRealtime
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

	// NEW (Phase 3): producer for retry and DLQ publishing.
	producer := kafka.NewProducer(brokers)
	defer producer.Close()

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

// newKafkaSubmitter is a stub for cross-tier enqueue; wired properly in Phase 6.
func newKafkaSubmitter(brokers []string) handlers.JobSubmitter {
	return &noopSubmitter{}
}

type noopSubmitter struct{}

func (noopSubmitter) Submit(ctx context.Context, typ jobs.Type, deviceID, idempKey string, payload []byte) error {
	return nil
}
