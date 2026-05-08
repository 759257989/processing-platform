//go:build integration
// +build integration

// test/integration: end-to-end tests against real Postgres/Redis/Kafka.
// Stage 2 leaves these as a placeholder. Stage 3 will refactor cmd/api and
// cmd/worker-standard so they expose an app.Run(ctx, cfg) function the test
// can drive in-process, then the full lifecycle test will be implemented here.
//
// Run with: go test -tags=integration ./test/integration/...
package integration

import "testing"

func TestJobLifecycle_Success(t *testing.T) {
    t.Skip("placeholder — see stage 3 for full integration test")
}

func TestJobLifecycle_Idempotent(t *testing.T) {
    t.Skip("placeholder — see stage 3 for full integration test")
}
