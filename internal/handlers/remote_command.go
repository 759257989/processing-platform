package handlers

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "time"

    "github.com/759257989/processing-platform/internal/observability"
    "github.com/759257989/processing-platform/internal/store/db"
)

// RemoteCommandHandler invokes a command on a device via the DeviceClient
// (which in production is the mock-device service over HTTP) and records
// the result in command_audit.
type RemoteCommandHandler struct {
    Deps Deps
}

type remoteCommandPayload struct {
    Command   string          `json:"command"`
    Arguments json.RawMessage `json:"arguments"`
}

func (h *RemoteCommandHandler) Handle(ctx context.Context, j Job) error {
    log := observability.WithTrace(ctx, h.Deps.Log)
    log.Info("starting remote command", "job_id", j.ID, "device_id", j.DeviceID)

    var p remoteCommandPayload
    if err := json.Unmarshal(j.Payload, &p); err != nil {
        return fmt.Errorf("decode remote-command payload: %w", err)
    }

    start := time.Now()
    resp, err := h.Deps.DeviceClient.SendCommand(ctx, j.DeviceID, p.Command, p.Arguments)
    duration := time.Since(start).Milliseconds()

    result := "SUCCESS"
    if err != nil {
        result = "FAILURE"
        if errors.Is(err, context.DeadlineExceeded) {
            result = "TIMEOUT"
        }
    }

    // Record the audit row regardless of success — auditability requires
    // we capture failures too. The original `err` is propagated below
    // so the worker correctly transitions the job to FAILED/RETRY.
    if _, dbErr := h.Deps.Store.Queries.InsertCommandAudit(ctx, db.InsertCommandAuditParams{
        DeviceID:   j.DeviceID,
        Command:    p.Command,
        Arguments:  []byte(p.Arguments),
        Result:     result,
        Response:   resp,
        DurationMs: int32(duration),
    }); dbErr != nil {
        return fmt.Errorf("insert command audit: %w", dbErr)
    }

    return err
}
