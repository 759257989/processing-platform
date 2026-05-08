package jobs

import "fmt"

// TierFor returns the worker tier responsible for a given job type.
// This is the SOURCE OF TRUTH for type→tier mapping. Both the API
// (when publishing) and the worker (when validating it owns the message)
// call this. If you change a type's tier, change it here and only here.
func TierFor(t Type) (Tier, error) {
    switch t {
    case TypeRemoteCommand:
        // Latency-sensitive: an operator is waiting for the result.
        // Realtime tier has the smallest queue and the most aggressive scale-up.
        return TierRealtime, nil

    case TypeTelemetryProcessing,
        TypeHealthCheck,
        TypeAlertGeneration:
        // Default volume, no special latency requirement. Standard tier.
        return TierStandard, nil

    case TypeFirmwareUpdate:
        // Long-running, big payloads, can wait. Bulk tier.
        return TierBulk, nil

    default:
        return "", fmt.Errorf("no tier mapping for type %q", t)
    }
}
