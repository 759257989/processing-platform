// Package jobs holds the pure logic of the job system: states, types, tiers,
// transitions, validation. No I/O. No external dependencies. Easy to test.
package jobs

// Type is one of the job task types defined in the spec.
type Type string

const (
    TypeTelemetryProcessing Type = "TELEMETRY_PROCESSING"
    // (more types added in Stage 3: REMOTE_COMMAND_EXECUTION,
    //  FIRMWARE_UPDATE_DISPATCH, DEVICE_HEALTH_CHECK,
    //  DEVICE_ALERT_GENERATION)
)

// Valid reports whether t is a known type.
func (t Type) Valid() bool {
    switch t {
    case TypeTelemetryProcessing:
        return true
    }
    return false
}

// Tier represents the worker pool a job runs in.
type Tier string

const (
    TierRealtime Tier = "realtime"
    TierStandard Tier = "standard"
    TierBulk     Tier = "bulk"
)

// State represents the lifecycle position of a job.
type State string

const (
    StateAccepted State = "ACCEPTED"
    StateQueued   State = "QUEUED"
    StateRunning  State = "RUNNING"
    StateSuccess  State = "SUCCESS"
    StateFailed   State = "FAILED"
    StateRetry    State = "RETRY"
)
