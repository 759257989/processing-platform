// cmd/reaper: scans for jobs that are stuck in RUNNING (worker died after
// claiming them but before finishing), and re-queues them so a healthy
// worker can pick them up.
//
// Two design choices worth noting:
//   1. Runs as a long-lived pod with an internal ticker (vs. a CronJob).
//      Either works; the long-lived pod gives more responsive recovery
//      and simpler observability.
//   2. Uses Postgres "FOR UPDATE SKIP LOCKED" so multiple reaper replicas
//      can run safely in parallel without coordination.
package main

import (
    "context"
    "log/slog"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/jackc/pgx/v5/pgtype"

    "github.com/759257989/processing-platform/internal/jobs"
    "github.com/759257989/processing-platform/internal/kafka"
    "github.com/759257989/processing-platform/internal/store"
    "github.com/759257989/processing-platform/internal/store/db"
    "encoding/json"
    "strings"
)

const (
    scanInterval = 30 * time.Second
    staleness    = "60 seconds"  // PG interval string
    maxBatch     = 50
)

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

    producer := kafka.NewProducer(brokers)
    defer producer.Close()

    log.Info("reaper started", "interval", scanInterval, "staleness", staleness)

    ticker := time.NewTicker(scanInterval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            log.Info("shutting down")
            return
        case <-ticker.C:
            if err := scanOnce(ctx, log, st, producer); err != nil {
                log.Error("scan failed", "err", err)
            }
        }
    }
}

func scanOnce(ctx context.Context, log *slog.Logger, st *store.Store, producer *kafka.Producer) error {
    // pgtype.Interval expects a structure; easiest is to pass the literal SQL
    // interval string. The query uses $1::interval so PG will cast it.
    interval := pgtype.Interval{Microseconds: int64(60 * time.Second / time.Microsecond), Valid: true}

    stale, err := st.Queries.ReapStaleJobs(ctx, db.ReapStaleJobsParams{
        Column1: interval,
        Limit:   maxBatch,
    })
    if err != nil {
        return err
    }
    if len(stale) == 0 {
        return nil
    }

    log.Info("reaping stale jobs", "count", len(stale))
    for _, s := range stale {
        if err := requeue(ctx, log, st, producer, s); err != nil {
            log.Error("requeue failed", "job_id", s.ID, "err", err)
        }
    }
    return nil
}

func requeue(ctx context.Context, log *slog.Logger, st *store.Store, producer *kafka.Producer, s db.ReapStaleJobsRow) error {
    // Update DB first.
    job, err := st.Queries.RequeueJob(ctx, s.ID)
    if err != nil {
        return err
    }
    if int(job.Attempts) > int(job.MaxAttempts) {
        // Don't republish; let it sit as FAILED. (Could also DLQ here.)
        log.Warn("stale job exhausted attempts", "job_id", s.ID)
        return nil
    }

    // Republish to the original tier topic.
    payload, _ := json.Marshal(map[string]any{
        "job_id":    job.ID.String(),
        "type":      job.Type,
        "tier":      job.Tier,
        "device_id": job.DeviceID,
        "payload":   json.RawMessage(job.Payload),
    })
    topic := "jobs." + job.Tier
    return producer.Publish(ctx, topic, []byte(job.DeviceID), payload)
}

func mustEnv(k string) string {
    v := os.Getenv(k)
    if v == "" {
        os.Stderr.WriteString("env " + k + " required\n")
        os.Exit(1)
    }
    return v
}

// jobs is imported only to ensure compile-time linkage to the constant
// state names; remove if not referenced.
var _ = jobs.StateRunning
