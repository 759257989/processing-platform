package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/759257989/processing-platform/internal/handlers"
	"github.com/759257989/processing-platform/internal/jobs"
	"github.com/759257989/processing-platform/internal/kafka"
	"github.com/759257989/processing-platform/internal/observability"
	"github.com/759257989/processing-platform/internal/retry"
	"github.com/759257989/processing-platform/internal/store"
	"github.com/759257989/processing-platform/internal/store/db"
)

type Config struct {
	Topic       string
	GroupID     string
	AllowedTier jobs.Tier
	Registry    handlers.Registry
}

type Deps struct {
	Consumer *kafka.Consumer
	Producer *kafka.Producer // NEW: for publishing to retry / DLQ
	Store    *store.Store
	Log      *slog.Logger
	Tracer   trace.Tracer // NEW (Phase 4): used to start the per-message span
}

type kafkaJobMsg struct {
	JobID    string          `json:"job_id"`
	Type     string          `json:"type"`
	Tier     string          `json:"tier"`
	DeviceID string          `json:"device_id"`
	Payload  json.RawMessage `json:"payload"`
}

func Run(ctx context.Context, cfg Config, deps Deps, m *observability.Metrics) error {
	deps.Log.Info("worker ready", "topic", cfg.Topic, "group", cfg.GroupID)

	for {
		// FetchMessageWithCtx pulls traceparent out of the message headers and
		// returns a ctx whose remote parent span = the API's submit span.
		// Use msgCtx (not ctx) for everything downstream so spans stay linked.
		msg, msgCtx, err := deps.Consumer.FetchMessageWithCtx(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			deps.Log.Error("fetch failed", "err", err)
			continue
		}

		// Count every consumed message — this is the input rate. Divergence
		// between this and JobsProcessedTotal indicates messages stuck in
		// processing (handler blocked).
		m.KafkaMessagesConsumed.WithLabelValues(cfg.Topic, cfg.GroupID).Inc()

		// Parse first so the span carries job.id / type / tier as attributes.
		var parsed struct {
			JobID string `json:"job_id"`
			Type  string `json:"type"`
			Tier  string `json:"tier"`
		}
		_ = json.Unmarshal(msg.Value, &parsed)

		// Start a Consumer-kind span as a child of the producer span. Any
		// downstream Producer.Publish (retry / DLQ / cross-tier enqueue) made
		// with spanCtx will inject this span as its parent, so the whole job
		// processing chain appears as one trace in Tempo.
		spanCtx, span := deps.Tracer.Start(msgCtx, "process_job",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("job.id", parsed.JobID),
				attribute.String("job.type", parsed.Type),
				attribute.String("job.tier", parsed.Tier),
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", cfg.Topic),
				attribute.Int("messaging.kafka.partition", msg.Partition),
				attribute.Int64("messaging.kafka.offset", msg.Offset),
			),
		)

		start := time.Now()
		perr := processOne(spanCtx, cfg, deps, msg)

		state := "success"
		if perr != nil {
			state = "failed"
			span.RecordError(perr)
			deps.Log.Error("process failed", "err", perr)
		}
		span.End()

		m.ObserveJob(parsed.Type, parsed.Tier, state, time.Since(start).Seconds())

		if cerr := deps.Consumer.CommitMessage(ctx, msg); cerr != nil {
			deps.Log.Error("commit failed", "err", cerr)
		}
	}
}

func processOne(ctx context.Context, cfg Config, deps Deps, msg segkafka.Message) error {
	var m kafkaJobMsg
	if err := json.Unmarshal(msg.Value, &m); err != nil {
		return fmt.Errorf("decode kafka message: %w", err)
	}

	if jobs.Tier(m.Tier) != cfg.AllowedTier {
		return fmt.Errorf("misrouted: tier %q ≠ allowed %q", m.Tier, cfg.AllowedTier)
	}

	jobID, err := uuid.Parse(m.JobID)
	if err != nil {
		return err
	}

	// Read current state from DB. We need attempts + max_attempts to decide
	// retry vs DLQ on failure. (Stage 2 just looked up by job ID and updated.)
	job, err := deps.Store.Queries.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}

	handler, err := cfg.Registry.Get(jobs.Type(m.Type))
	if err != nil {
		// Unknown type: route directly to DLQ. No retry would help.
		return sendToDLQ(ctx, deps, msg.Value, jobID, err.Error())
	}

	// ACCEPTED/QUEUED/RETRY → RUNNING.
	if _, err := deps.Store.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
		ID:    jobID,
		State: string(jobs.StateRunning),
	}); err != nil {
		return fmt.Errorf("update RUNNING: %w", err)
	}

	// Right before calling handler.Handle:
	stopHB := startHeartbeat(ctx, deps, jobID, 10*time.Second)

	handlerErr := handler.Handle(ctx, handlers.Job{
		ID:       m.JobID,
		Type:     jobs.Type(m.Type),
		DeviceID: m.DeviceID,
		Payload:  m.Payload,
	})
	stopHB()

	if handlerErr == nil {
		_, err := deps.Store.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
			ID:    jobID,
			State: string(jobs.StateSuccess),
		})
		return err
	}

	// ----- failure path -----
	// Important: we always return handlerErr (not the retry/DLQ scheduling
	// outcome) so that pp_jobs_processed_total{state="failed"} actually counts
	// handler failures. Otherwise a successfully-scheduled retry would be
	// recorded as state="success" and SLO burn-rate alerts could never fire.
	nextAttempt := int(job.Attempts) + 1
	if nextAttempt >= int(job.MaxAttempts) {
		if dlqErr := sendToDLQ(ctx, deps, msg.Value, jobID, handlerErr.Error()); dlqErr != nil {
			deps.Log.Error("dlq publish", "err", dlqErr, "job_id", jobID)
		}
		return handlerErr
	}

	if retryErr := sendToRetry(ctx, cfg, deps, msg.Value, jobID, nextAttempt, handlerErr.Error()); retryErr != nil {
		deps.Log.Error("retry publish", "err", retryErr, "job_id", jobID)
	}
	return handlerErr
}

// sendToRetry publishes to "jobs.<tier>.retry" with a "retry-at" header.
func sendToRetry(ctx context.Context, cfg Config, deps Deps, value []byte, jobID uuid.UUID, attempt int, errMsg string) error {
	retryAt := retry.NextAt(time.Now(), attempt)

	deps.Log.Info("scheduling retry",
		"job_id", jobID, "attempt", attempt,
		"retry_at", retryAt.Format(time.RFC3339))

	if err := deps.Producer.PublishWithHeader(
		ctx,
		cfg.Topic+".retry",
		[]byte(jobID.String()),
		value,
		"retry-at",
		[]byte(strconv.FormatInt(retryAt.Unix(), 10)),
	); err != nil {
		return fmt.Errorf("publish retry: %w", err)
	}

	errStr := errMsg
	_, err := deps.Store.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
		ID:        jobID,
		State:     string(jobs.StateRetry),
		LastError: &errStr,
	})
	if err != nil {
		return err
	}
	_, err = deps.Store.Queries.IncrementJobAttempts(ctx, jobID)
	return err
}

func sendToDLQ(ctx context.Context, deps Deps, value []byte, jobID uuid.UUID, errMsg string) error {
	deps.Log.Warn("sending to DLQ", "job_id", jobID, "err", errMsg)

	if err := deps.Producer.Publish(ctx, "jobs.dlq", []byte(jobID.String()), value); err != nil {
		return fmt.Errorf("publish dlq: %w", err)
	}

	errStr := errMsg
	_, err := deps.Store.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
		ID:        jobID,
		State:     string(jobs.StateFailed),
		LastError: &errStr,
	})
	return err
}

func startHeartbeat(ctx context.Context, deps Deps, jobID uuid.UUID, interval time.Duration) context.CancelFunc {
	hbCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if _, err := deps.Store.Pool.Exec(hbCtx,
					"UPDATE jobs SET heartbeat_at = NOW() WHERE id = $1", jobID); err != nil {
					deps.Log.Warn("heartbeat write failed", "job_id", jobID, "err", err)
				}
			}
		}
	}()
	return cancel
}
