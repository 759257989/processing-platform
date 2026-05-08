package jobs

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestTierFor_AllKnownTypes(t *testing.T) {
    cases := map[Type]Tier{
        TypeRemoteCommand:       TierRealtime,
        TypeTelemetryProcessing: TierStandard,
        TypeHealthCheck:         TierStandard,
        TypeAlertGeneration:     TierStandard,
        TypeFirmwareUpdate:      TierBulk,
    }
    for typ, expectedTier := range cases {
        t.Run(string(typ), func(t *testing.T) {
            tier, err := TierFor(typ)
            require.NoError(t, err)
            assert.Equal(t, expectedTier, tier)
        })
    }
}

func TestTierFor_Unknown(t *testing.T) {
    _, err := TierFor("UNKNOWN_TYPE")
    assert.Error(t, err)
}
