// cmd/worker-standard: consumes the jobs.standard Kafka topic and
// processes each message. For Stage 2 we only handle TELEMETRY_PROCESSING,
// which we simulate as a 50–250ms operation that always succeeds.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/759257989/processing-platform/internal/jobs"
	"github.com/759257989/processing-platform/internal/kafka"
	"github.com/759257989/processing-platform/internal/store"
	"github.com/759257989/processing-platform/internal/store/db"
)

const (
	topic   = "jobs.standard"
	groupID = "worker-standard"
)

type kafkaJobMsg struct {
	JobID    string          `json:"job_id"`
	Type     string          `json:"type"`
	Tier     string          `json:"tier"`
	DeviceID string          `json:"device_id"`
	Payload  json.RawMessage `json:"payload"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	pgDSN := mustEnv("POSTGRES_DSN")
	brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")

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

	log.Info("worker-standard ready", "topic", topic, "group", groupID)

	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("shutting down")
				return
			}
			log.Error("fetch", "err", err)
			time.Sleep(time.Second)
			continue
		}

		if err := processMessage(ctx, log, st, msg.Value); err != nil {
			log.Error("process failed (no retry yet)", "err", err)
			// Stage 2 has no retry: log and commit so we move on.
			// Stage 3 adds retry topic + DLQ.
		}

		if err := consumer.CommitMessage(ctx, msg); err != nil {
			log.Error("commit", "err", err)
		}
	}
}

func processMessage(ctx context.Context, log *slog.Logger, st *store.Store, value []byte) error {
	var m kafkaJobMsg
	if err := json.Unmarshal(value, &m); err != nil {
		return err
	}

	jobID, err := uuid.Parse(m.JobID)
	if err != nil {
		return err
	}

	log.Info("processing job", "job_id", jobID, "type", m.Type)

	// Move to RUNNING.
	if _, err := st.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
		ID:    jobID,
		State: string(jobs.StateRunning),
	}); err != nil {
		return err
	}

	// "Do the work." For TELEMETRY_PROCESSING, simulate a 50–250ms operation.
	// Stage 3 will replace this with a real per-task-type handler.
	time.Sleep(time.Duration(50+rand.IntN(200)) * time.Millisecond)

	// Move to SUCCESS.
	if _, err := st.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
		ID:    jobID,
		State: string(jobs.StateSuccess),
	}); err != nil {
		return err
	}

	log.Info("job done", "job_id", jobID)
	return nil
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		os.Stderr.WriteString("env " + k + " is required\n")
		os.Exit(1)
	}
	return v
}
