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

    "github.com/759257989/processing-platform/internal/handlers"
    "github.com/759257989/processing-platform/internal/jobs"
    "github.com/759257989/processing-platform/internal/kafka"
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
    Consumer    *kafka.Consumer
    Producer    *kafka.Producer  // NEW: for publishing to retry / DLQ
    Store       *store.Store
    Log         *slog.Logger
}

type kafkaJobMsg struct {
    JobID    string          `json:"job_id"`
    Type     string          `json:"type"`
    Tier     string          `json:"tier"`
    DeviceID string          `json:"device_id"`
    Payload  json.RawMessage `json:"payload"`
}

func Run(ctx context.Context, cfg Config, deps Deps) error {
    deps.Log.Info("worker ready", "topic", cfg.Topic, "group", cfg.GroupID)

    for {
        msg, err := deps.Consumer.FetchMessage(ctx)
        if err != nil {
            if errors.Is(err, context.Canceled) {
                return nil
            }
            deps.Log.Error("fetch failed", "err", err)
            continue
        }

        if perr := processOne(ctx, cfg, deps, msg); perr != nil {
            deps.Log.Error("process failed", "err", perr)
        }

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

    handlerErr := handler.Handle(ctx, handlers.Job{
        ID:       m.JobID,
        Type:     jobs.Type(m.Type),
        DeviceID: m.DeviceID,
        Payload:  m.Payload,
    })

    if handlerErr == nil {
        _, err := deps.Store.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
            ID:    jobID,
            State: string(jobs.StateSuccess),
        })
        return err
    }

    // ----- failure path -----
    nextAttempt := int(job.Attempts) + 1
    if nextAttempt >= int(job.MaxAttempts) {
        // No more retries: DLQ + FAILED.
        return sendToDLQ(ctx, deps, msg.Value, jobID, handlerErr.Error())
    }

    return sendToRetry(ctx, cfg, deps, msg.Value, jobID, nextAttempt, handlerErr.Error())
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
