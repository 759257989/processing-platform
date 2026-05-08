package handlers

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/759257989/processing-platform/internal/store/db"
)

// TelemetryHandler aggregates a single telemetry sample into device_metrics.
// In production this would batch many samples; for the project we accept
// one sample per job (the device simulator in Stage 5 will produce many).
type TelemetryHandler struct {
    Deps Deps
}

// telemetryPayload defines the shape we expect in Job.Payload.
// JSON tags must match the sender (the API, the ingestion service).
type telemetryPayload struct {
    Value     float64   `json:"value"`               // the sampled metric
    Timestamp time.Time `json:"timestamp,omitempty"` // when sampled; defaults to NOW()
}

func (h *TelemetryHandler) Handle(ctx context.Context, j Job) error {
    var p telemetryPayload
    if err := json.Unmarshal(j.Payload, &p); err != nil {
        return fmt.Errorf("decode telemetry payload: %w", err)
    }
    if p.Timestamp.IsZero() {
        p.Timestamp = time.Now()
    }

    // For Stage 3 we treat each job as one sample. The metrics row is
    // deliberately denormalized — sample_count=1 — because aggregation
    // logic belongs in the device simulator (Stage 5) or a future
    // upstream batcher, not here.
    _, err := h.Deps.Store.Queries.InsertDeviceMetric(ctx, db.InsertDeviceMetricParams{
        DeviceID:    j.DeviceID,
        MetricAt:    p.Timestamp,
        AvgValue:    p.Value,
        SampleCount: 1,
    })
    return err
}
