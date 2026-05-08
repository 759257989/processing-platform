-- Stage 3, Phase 1: tables that the four new task types write to.

-- Telemetry already had a stub in Stage 2; now we materialize it properly.
-- Each row is a per-minute aggregate of device telemetry samples.
CREATE TABLE device_metrics (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id   TEXT NOT NULL REFERENCES devices(id),
    metric_at   TIMESTAMPTZ NOT NULL,
    avg_value   DOUBLE PRECISION NOT NULL,
    sample_count INT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Index for "show me the metrics for device X over time."
CREATE INDEX device_metrics_device_time
    ON device_metrics (device_id, metric_at DESC);

-- FIRMWARE_UPDATE_DISPATCH writes here on success/failure.
-- One row per attempt; we keep history for auditing.
CREATE TABLE firmware_history (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id       TEXT NOT NULL REFERENCES devices(id),
    target_version  TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('PENDING','APPLIED','FAILED')),
    failure_reason  TEXT,
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX firmware_history_device ON firmware_history (device_id, attempted_at DESC);

-- REMOTE_COMMAND_EXECUTION writes one row per command invocation.
CREATE TABLE command_audit (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id    TEXT NOT NULL REFERENCES devices(id),
    command      TEXT NOT NULL,
    arguments    JSONB NOT NULL DEFAULT '{}'::jsonb,
    result       TEXT NOT NULL CHECK (result IN ('SUCCESS','FAILURE','TIMEOUT')),
    response     JSONB,
    duration_ms  INT NOT NULL,
    executed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- DEVICE_ALERT_GENERATION writes here. Driven by DEVICE_HEALTH_CHECK results.
CREATE TABLE alerts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id   TEXT NOT NULL REFERENCES devices(id),
    severity    TEXT NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
    message     TEXT NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Acknowledged is "an operator saw it." Out of scope for Stage 3 but the
    -- column lets the admin UI in Stage 7 mark alerts as read.
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- DEVICE_HEALTH_CHECK has no dedicated table — its "output" is to ENQUEUE a
-- DEVICE_ALERT_GENERATION job (cross-tier enqueue, Phase 6). We add a couple
-- of columns to `devices` so the health-check has something to read/write.
ALTER TABLE devices
    ADD COLUMN last_seen_at    TIMESTAMPTZ,
    ADD COLUMN firmware_version TEXT;
