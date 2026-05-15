package handlers

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/759257989/processing-platform/internal/jobs"
    "github.com/759257989/processing-platform/internal/observability"
)

// HealthCheckHandler polls a device's health; if it's unhealthy, it spawns
// a DEVICE_ALERT_GENERATION job (cross-tier enqueue). This is the canonical
// example of "one job kicks off another."
type HealthCheckHandler struct {
    Deps Deps
}

func (h *HealthCheckHandler) Handle(ctx context.Context, j Job) error {
    log := observability.WithTrace(ctx, h.Deps.Log)
    log.Info("starting health check", "job_id", j.ID, "device_id", j.DeviceID)

    healthy, lastSeenAgo, err := h.Deps.DeviceClient.GetHealth(ctx, j.DeviceID)
    if err != nil {
        return fmt.Errorf("device health check: %w", err)
    }

    if dbErr := h.Deps.Store.Queries.TouchDevice(ctx, j.DeviceID); dbErr != nil {
        return fmt.Errorf("touch device: %w", dbErr)
    }

    if healthy {
        return nil
    }

    // Unhealthy: enqueue a DEVICE_ALERT_GENERATION job. We use the same
    // SubmitJob call the API uses, which gives us idempotency, audit, etc.
    // for free. We synthesize an idempotency key from device_id + the
    // health-check job ID, so retries of THIS job don't double-alert.
    payload, _ := json.Marshal(map[string]any{
        "severity":      "WARN",
        "message":       fmt.Sprintf("device %s unhealthy (last seen %s ago)", j.DeviceID, lastSeenAgo),
        "source_job_id": j.ID,
    })
    idemp := "alert-from-" + j.ID  // 12+ chars, safe alphabet
    return h.Deps.JobSubmitter.Submit(ctx, jobs.TypeAlertGeneration, j.DeviceID, idemp, payload)
}
