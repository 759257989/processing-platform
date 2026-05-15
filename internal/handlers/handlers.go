// Package handlers contains one Handler per job Type. Each handler does
// the actual work for that task type — DB writes, HTTP calls, etc.
//
// Workers don't know about handlers individually; they look one up via
// the registry and call it. This keeps the worker loop type-agnostic.
package handlers

import (
    "context"
    "fmt"
    "log/slog"

    "github.com/759257989/processing-platform/internal/jobs"
    "github.com/759257989/processing-platform/internal/store"
)

// Job is the trimmed-down struct workers pass to handlers. It's not the full
// DB row — only the fields the handler needs. Decouples handlers from sqlc's
// generated types (which would force them all to live in the same module
// boundary as the DB layer).
type Job struct {
    ID       string                 // UUID as string; handlers don't need the typed UUID
    Type     jobs.Type
    DeviceID string
    Payload  []byte                 // raw JSON; handlers parse what they care about
}

// Handler does the work for one job. Implementations must be:
//   - idempotent (workers may re-run on retry; same input → same final state)
//   - bounded in time (handler returning is what releases the Kafka partition)
//   - safe under concurrency (no shared mutable state across handler calls)
type Handler interface {
    Handle(ctx context.Context, j Job) error
}

// Registry is a Type → Handler lookup. Built once at startup.
type Registry map[jobs.Type]Handler

// Get returns the handler for `t` or an error. Workers should fail loudly
// (not silently skip) when a message arrives for a type they can't handle —
// that means a misrouted message, which is a bug worth surfacing.
func (r Registry) Get(t jobs.Type) (Handler, error) {
    h, ok := r[t]
    if !ok {
        return nil, fmt.Errorf("no handler for type %q", t)
    }
    return h, nil
}

// Deps groups dependencies that handlers need. Keeps signatures clean.
// One Deps is built at worker startup and shared across all handler instances.
type Deps struct {
    Store          *store.Store
    DeviceClient   DeviceClient    // talks to the mock-device service
    WebhookClient  WebhookClient   // talks to the mock-webhook service
    JobSubmitter   JobSubmitter    // for cross-tier enqueue (Phase 6)
    Log            *slog.Logger    // wrap with observability.WithTrace(ctx, ...) inside Handle
}

// DeviceClient and WebhookClient are interfaces so handlers don't import
// HTTP libraries directly — keeps testing easy (swap in a fake in tests).
type DeviceClient interface {
    SendCommand(ctx context.Context, deviceID, command string, args []byte) (response []byte, err error)
    GetHealth(ctx context.Context, deviceID string) (healthy bool, lastSeenAgo string, err error)
    PushFirmware(ctx context.Context, deviceID, version string) error
}

type WebhookClient interface {
    PostAlert(ctx context.Context, severity, message string, payload []byte) error
}

// JobSubmitter is what handlers call to enqueue another job (cross-tier).
// We use this in Phase 6 so DEVICE_HEALTH_CHECK can spawn DEVICE_ALERT_GENERATION.
type JobSubmitter interface {
    Submit(ctx context.Context, typ jobs.Type, deviceID, idempKey string, payload []byte) error
}
