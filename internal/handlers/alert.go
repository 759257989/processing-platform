package handlers

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/759257989/processing-platform/internal/observability"
    "github.com/759257989/processing-platform/internal/store/db"
)

// AlertHandler writes the alert to the alerts table AND fans it out to
// the webhook receiver. Order matters: we DB-write first so the alert
// is durable even if the webhook call fails.
type AlertHandler struct {
    Deps Deps
}

type alertPayload struct {
    Severity string          `json:"severity"`
    Message  string          `json:"message"`
    Extra    json.RawMessage `json:"extra,omitempty"`
}

func (h *AlertHandler) Handle(ctx context.Context, j Job) error {
    log := observability.WithTrace(ctx, h.Deps.Log)
    log.Info("starting alert generation", "job_id", j.ID, "device_id", j.DeviceID)

    var p alertPayload
    if err := json.Unmarshal(j.Payload, &p); err != nil {
        return fmt.Errorf("decode alert payload: %w", err)
    }

    // Step 1: durably record the alert. If this fails, retry will re-create.
    extra := []byte(p.Extra)
    if len(extra) == 0 {
        extra = []byte("{}")
    }
    if _, err := h.Deps.Store.Queries.InsertAlert(ctx, db.InsertAlertParams{
        DeviceID: j.DeviceID,
        Severity: p.Severity,
        Message:  p.Message,
        Payload:  extra,
    }); err != nil {
        return fmt.Errorf("insert alert: %w", err)
    }

    // Step 2: fan out via webhook. Failure here means the alert is in DB
    // but the webhook receiver didn't see it — retry will re-fire the
    // webhook (which is why the receiver should be idempotent on its end).
    return h.Deps.WebhookClient.PostAlert(ctx, p.Severity, p.Message, p.Extra)
}
