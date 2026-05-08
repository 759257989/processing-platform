// Package jobs holds the pure logic of the job system: states, types, tiers,
// transitions, validation. No I/O. No external dependencies. Easy to test.
package jobs

// Type is one of the job task types. Each type maps to exactly one tier
// (see TierFor) and one handler (see internal/handlers).
type Type string

const (
    // TELEMETRY_PROCESSING — aggregate device telemetry into device_metrics.
    // High volume, low priority. Standard tier.
    TypeTelemetryProcessing Type = "TELEMETRY_PROCESSING"

    // REMOTE_COMMAND_EXECUTION — fire a command at a device via mock-device,
    // record the response. Latency-sensitive (operator is waiting). Realtime tier.
    TypeRemoteCommand Type = "REMOTE_COMMAND_EXECUTION"

    // FIRMWARE_UPDATE_DISPATCH — initiate a firmware push. Long-running,
    // doesn't need low latency, large payloads. Bulk tier.
    TypeFirmwareUpdate Type = "FIRMWARE_UPDATE_DISPATCH"

    // DEVICE_HEALTH_CHECK — poll a device's health, decide whether it needs
    // an alert. Routine work. Standard tier.
    TypeHealthCheck Type = "DEVICE_HEALTH_CHECK"

    // DEVICE_ALERT_GENERATION — fan out an alert via webhook + DB.
    // Triggered by health check, not by external clients. Standard tier.
    TypeAlertGeneration Type = "DEVICE_ALERT_GENERATION"
)

// Valid reports whether t is a recognized type. Used by the API to reject
// unknown types at the edge — invalid types should never reach a worker.
func (t Type) Valid() bool {
    switch t {
    case TypeTelemetryProcessing,
        TypeRemoteCommand,
        TypeFirmwareUpdate,
        TypeHealthCheck,
        TypeAlertGeneration:
        return true
    }
    return false
}

type Tier string

const (
    TierRealtime Tier = "realtime"
    TierStandard Tier = "standard"
    TierBulk     Tier = "bulk"
)

type State string

const (
    StateAccepted State = "ACCEPTED"
    StateQueued   State = "QUEUED"
    StateRunning  State = "RUNNING"
    StateSuccess  State = "SUCCESS"
    StateFailed   State = "FAILED"
    StateRetry    State = "RETRY"
)
