// cmd/api: the REST gateway. Accepts job submissions, persists them to
// Postgres, publishes them to Kafka, and exposes job lookup + health.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/759257989/processing-platform/internal/idempotency"
	"github.com/759257989/processing-platform/internal/jobs"
	"github.com/759257989/processing-platform/internal/kafka"
	"github.com/759257989/processing-platform/internal/observability"
	"github.com/759257989/processing-platform/internal/store"
	"github.com/759257989/processing-platform/internal/store/db"
)

func main() {
	// Config from environment. Stage 4 will switch this to a typed config struct.
	pgDSN := mustEnv("POSTGRES_DSN")
	redisAddr := mustEnv("REDIS_ADDR")
	kafkaBrokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")
	listenAddr := envOr("LISTEN_ADDR", ":8080")
	metricsAddr := envOr("METRICS_ADDR", ":9090")
	otlpEndpoint := envOr("OTLP_ENDPOINT", "pp-tempo:4317")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Bootstrap observability before anything else. Init wires:
	//   - slog JSON logger to stdout (Promtail picks it up)
	//   - Prometheus registry + /metrics endpoint on metricsAddr
	//   - OTel TracerProvider with OTLP gRPC exporter pointing at Tempo
	// shutdown() flushes pending traces — must run before process exits.
	obs, shutdown, err := observability.Init(ctx, "api", metricsAddr, otlpEndpoint)
	if err != nil {
		slog.Default().Error("observability init failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		// 5s budget for trace exporter flush. Use fresh ctx because the main
		// ctx may already be cancelled by the time we get here.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(shutdownCtx)
	}()
	log := obs.Log

	// Connect to Postgres.
	st, err := store.New(ctx, pgDSN)
	if err != nil {
		log.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Connect to Redis.
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis connect failed", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	// Wire idempotency.
	idemp := idempotency.NewRedis(rdb)

	// Wire Kafka producer.
	// producer := kafka.NewProducer(kafkaBrokers)
	producer := kafka.NewInstrumented(kafka.NewProducer(kafkaBrokers), obs.Metrics)
	defer producer.Close()

	// Build the HTTP handler.
	r := gin.New()
	r.Use(gin.Recovery())
	// Middleware order matters:
	//   1) Recovery first — a panic in later middleware/handlers still gets
	//      converted to 500 so metrics + traces record the failure cleanly
	//   2) Our Prometheus middleware — records pp_http_* metrics
	//   3) otelgin — starts a span per request, extracts traceparent from
	//      incoming headers, injects it into outgoing ctx
	r.Use(observability.GinMetrics(obs.Metrics))
	r.Use(otelgin.Middleware("api"))

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	h := &handler{
		log:      log,
		store:    st,
		idemp:    idemp,
		producer: producer,
	}
	r.POST("/jobs", h.submitJob)
	r.GET("/jobs/:id", h.getJob)

	server := &http.Server{Addr: listenAddr, Handler: r}

	go func() {
		log.Info("api listening", "addr", listenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

// ----- handler -----

// publisher is the only kafka.Producer surface the API handler needs.
// Declared as an interface so both *kafka.Producer and *kafka.InstrumentedProducer
// (which adds metrics) can satisfy it without the handler caring.
type publisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
}

type handler struct {
	log      *slog.Logger
	store    *store.Store
	idemp    idempotency.Acquirer
	producer publisher
}

type submitJobRequest struct {
	Type           string          `json:"type" binding:"required"`
	DeviceID       string          `json:"device_id" binding:"required"`
	IdempotencyKey string          `json:"idempotency_key" binding:"required"`
	Payload        json.RawMessage `json:"payload"`
}

type submitJobResponse struct {
	JobID string `json:"job_id"`
	State string `json:"state"`
}

func (h *handler) submitJob(c *gin.Context) {
	var req submitJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jobType := jobs.Type(req.Type)
	if !jobType.Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown type"})
		return
	}

	if err := jobs.ValidateIdempotencyKey(req.IdempotencyKey); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tier, err := jobs.TierFor(jobType)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// Idempotency: if the key was already used, return the existing job.
	_, isNew, err := h.idemp.Acquire(ctx, req.IdempotencyKey)
	if err != nil {
		h.log.Error("idempotency acquire", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
		return
	}
	if !isNew {
		existing, err := h.store.Queries.GetJobByIdempotencyKey(ctx, db.GetJobByIdempotencyKeyParams{
			IdempotencyKey: req.IdempotencyKey,
			Type:           string(jobType),
		})
		if err != nil {
			h.log.Error("lookup existing job", "err", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
			return
		}
		c.JSON(http.StatusOK, submitJobResponse{JobID: existing.ID.String(), State: existing.State})
		return
	}

	// INSERT job in ACCEPTED state.
	payload := req.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	job, err := h.store.Queries.CreateJob(ctx, db.CreateJobParams{
		Type:           string(jobType),
		Tier:           string(tier),
		DeviceID:       req.DeviceID,
		Payload:        payload,
		IdempotencyKey: req.IdempotencyKey,
		MaxAttempts:    3,
	})
	if err != nil {
		h.log.Error("create job", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
		return
	}

	// Publish to Kafka.
	msgValue, _ := json.Marshal(map[string]any{
		"job_id":    job.ID.String(),
		"type":      job.Type,
		"tier":      job.Tier,
		"device_id": job.DeviceID,
		"payload":   json.RawMessage(job.Payload),
	})

	topic := "jobs." + string(tier)
	if err := h.producer.Publish(ctx, topic, []byte(req.DeviceID), msgValue); err != nil {
		h.log.Error("kafka publish", "err", err, "job_id", job.ID)
		// Job is in Postgres with state=ACCEPTED. A future reaper-like
		// job (Stage 3+) can republish. Return 202 so client doesn't retry.
		c.JSON(http.StatusAccepted, submitJobResponse{JobID: job.ID.String(), State: job.State})
		return
	}

	// Bump state to QUEUED.
	queued, err := h.store.Queries.UpdateJobState(ctx, db.UpdateJobStateParams{
		ID:    job.ID,
		State: string(jobs.StateQueued),
	})
	if err != nil {
		h.log.Error("update to QUEUED", "err", err, "job_id", job.ID)
		c.JSON(http.StatusAccepted, submitJobResponse{JobID: job.ID.String(), State: job.State})
		return
	}

	c.JSON(http.StatusAccepted, submitJobResponse{JobID: queued.ID.String(), State: queued.State})
}

func (h *handler) getJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid uuid"})
		return
	}
	job, err := h.store.Queries.GetJob(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		h.log.Error("get job", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
		return
	}
	c.JSON(http.StatusOK, job)
}

// ----- helpers -----

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "env %s is required\n", k)
		os.Exit(1)
	}
	return v
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
