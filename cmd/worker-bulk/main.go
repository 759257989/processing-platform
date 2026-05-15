// cmd/worker-bulk: bulk-tier worker. Consumes jobs.bulk and dispatches each
// message through the handler registry.
//
// Identical to worker-realtime and worker-standard except for three constants:
// topic, group ID, and allowed tier. The bulk tier handles long-running,
// large-payload jobs (e.g. FIRMWARE_UPDATE_DISPATCH) where latency doesn't
// matter — it can wait minutes.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/759257989/processing-platform/internal/handlers"
	"github.com/759257989/processing-platform/internal/jobs"
	"github.com/759257989/processing-platform/internal/jobsubmitter"
	"github.com/759257989/processing-platform/internal/kafka"
	"github.com/759257989/processing-platform/internal/mockclients"
	"github.com/759257989/processing-platform/internal/observability"
	"github.com/759257989/processing-platform/internal/store"
	"github.com/759257989/processing-platform/internal/worker"
)

const (
	topic       = "jobs.bulk"
	groupID     = "worker-bulk"
	allowedTier = jobs.TierBulk
)

func main() {
	pgDSN := mustEnv("POSTGRES_DSN")
	brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")
	deviceURL := envOr("DEVICE_URL", "http://mock-device:8080")
	webhookURL := envOr("WEBHOOK_URL", "http://mock-webhook:8080")
	metricsAddr := envOr("METRICS_ADDR", ":9090")
	otlpEndpoint := envOr("OTLP_ENDPOINT", "pp-tempo:4317")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	obs, shutdown, err := observability.Init(ctx, "worker-bulk", metricsAddr, otlpEndpoint)
	if err != nil {
		slog.Default().Error("observability init failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(shutdownCtx)
	}()
	log := obs.Log

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
		JobSubmitter:  jobsubmitter.New(producer, st),
		Log:           log,
	}

	if err := worker.Run(ctx,
		worker.Config{
			Topic:       topic,
			GroupID:     groupID,
			AllowedTier: allowedTier,
			Registry:    handlers.Build(deps),
		},
		worker.Deps{Consumer: consumer, Producer: producer, Store: st, Log: log, Tracer: obs.Tracer},
		obs.Metrics,
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

