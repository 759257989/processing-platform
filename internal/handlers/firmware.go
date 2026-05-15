package handlers

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/759257989/processing-platform/internal/observability"
    "github.com/759257989/processing-platform/internal/store/db"
)

// FirmwareHandler dispatches a firmware update to a device, then records
// the attempt in firmware_history. On success it also bumps the device's
// recorded firmware_version.
type FirmwareHandler struct {
    Deps Deps
}

type firmwarePayload struct {
    TargetVersion string `json:"target_version"`
}

func (h *FirmwareHandler) Handle(ctx context.Context, j Job) error {
    log := observability.WithTrace(ctx, h.Deps.Log)
    log.Info("starting firmware update", "job_id", j.ID, "device_id", j.DeviceID)

    var p firmwarePayload
    if err := json.Unmarshal(j.Payload, &p); err != nil {
        return fmt.Errorf("decode firmware payload: %w", err)
    }

    pushErr := h.Deps.DeviceClient.PushFirmware(ctx, j.DeviceID, p.TargetVersion)

    state := "APPLIED"
    var failureReason *string
    if pushErr != nil {
        state = "FAILED"
        msg := pushErr.Error()
        failureReason = &msg
    }

    if _, dbErr := h.Deps.Store.Queries.InsertFirmwareAttempt(ctx, db.InsertFirmwareAttemptParams{
        DeviceID:       j.DeviceID,
        TargetVersion:  p.TargetVersion,
        State:          state,
        FailureReason:  failureReason,
    }); dbErr != nil {
        return fmt.Errorf("insert firmware attempt: %w", dbErr)
    }

    if pushErr == nil {
        if dbErr := h.Deps.Store.Queries.UpdateDeviceFirmware(ctx, db.UpdateDeviceFirmwareParams{
            ID:               j.DeviceID,
            FirmwareVersion:  &p.TargetVersion,
        }); dbErr != nil {
            return fmt.Errorf("update device firmware: %w", dbErr)
        }
    }

    return pushErr
}
