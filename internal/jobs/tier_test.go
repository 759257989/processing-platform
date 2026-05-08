package jobs

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestTierFor_Known(t *testing.T) {
    tier, err := TierFor(TypeTelemetryProcessing)
    require.NoError(t, err)
    assert.Equal(t, TierStandard, tier)
}

func TestTierFor_Unknown(t *testing.T) {
    _, err := TierFor("UNKNOWN_TYPE")
    assert.Error(t, err)
}
