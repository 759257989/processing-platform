package handlers

import "github.com/759257989/processing-platform/internal/jobs"

// Build returns a Registry wired with all handlers. Workers call this once
// at startup, with all dependencies injected via Deps.
//
// Tests can build a partial registry by constructing only the entries
// they need.
func Build(deps Deps) Registry {
    return Registry{
        jobs.TypeTelemetryProcessing: &TelemetryHandler{Deps: deps},
        jobs.TypeRemoteCommand:       &RemoteCommandHandler{Deps: deps},
        jobs.TypeFirmwareUpdate:      &FirmwareHandler{Deps: deps},
        jobs.TypeHealthCheck:         &HealthCheckHandler{Deps: deps},
        jobs.TypeAlertGeneration:     &AlertHandler{Deps: deps},
    }
}
