package jobs

import "fmt"

// TierFor returns the worker tier responsible for a given job type.
// Mapping per spec §10: every task type has exactly one tier.
func TierFor(t Type) (Tier, error) {
    switch t {
    case TypeTelemetryProcessing:
        return TierStandard, nil
    // (Stage 3 will fill in the other 4 types.)
    default:
        return "", fmt.Errorf("no tier mapping for type %q", t)
    }
}
