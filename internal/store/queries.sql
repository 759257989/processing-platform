-- name: CreateDevice :one
-- Create a device row. ON CONFLICT DO NOTHING means submitting the same
-- device twice is harmless (we use this in `make seed`).
INSERT INTO devices (id) VALUES ($1)
ON CONFLICT (id) DO NOTHING
RETURNING *;

-- name: GetDevice :one
SELECT * FROM devices WHERE id = $1;

-- name: CreateJob :one
INSERT INTO jobs (type, tier, device_id, payload, idempotency_key, max_attempts)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetJob :one
SELECT * FROM jobs WHERE id = $1;

-- name: GetJobByIdempotencyKey :one
SELECT * FROM jobs WHERE idempotency_key = $1 AND type = $2;

-- name: UpdateJobState :one
UPDATE jobs
SET state = $2,
    updated_at = NOW(),
    started_at = COALESCE(started_at, CASE WHEN $2 = 'RUNNING' THEN NOW() ELSE NULL END),
    finished_at = CASE WHEN $2 IN ('SUCCESS','FAILED') THEN NOW() ELSE finished_at END,
    last_error = COALESCE(sqlc.narg('last_error'), last_error)
WHERE id = $1
RETURNING *;

-- name: IncrementJobAttempts :one
UPDATE jobs
SET attempts = attempts + 1,
    updated_at = NOW()
WHERE id = $1
RETURNING *;


-- ===== Phase 1 additions =====

-- Telemetry: insert one aggregate row.
-- name: InsertDeviceMetric :one
INSERT INTO device_metrics (device_id, metric_at, avg_value, sample_count)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- Firmware: record an attempt. Always inserts; we keep history.
-- name: InsertFirmwareAttempt :one
INSERT INTO firmware_history (device_id, target_version, state, failure_reason)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- Update the device's recorded firmware version after a successful update.
-- name: UpdateDeviceFirmware :exec
UPDATE devices SET firmware_version = $2 WHERE id = $1;

-- Remote command: audit log entry.
-- name: InsertCommandAudit :one
INSERT INTO command_audit (device_id, command, arguments, result, response, duration_ms)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- Health check: read the columns the handler decides on.
-- name: GetDeviceForHealthCheck :one
SELECT id, last_seen_at, firmware_version
FROM devices WHERE id = $1;

-- Health check: bump last_seen.
-- name: TouchDevice :exec
UPDATE devices SET last_seen_at = NOW() WHERE id = $1;

-- Alert: record an alert row.
-- name: InsertAlert :one
INSERT INTO alerts (device_id, severity, message, payload)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: ReapStaleJobs :many
-- Find RUNNING jobs whose heartbeat is older than `staleness`. The
-- FOR UPDATE SKIP LOCKED clause is critical: multiple reaper instances
-- (or the reaper running concurrently with workers) must not fight over
-- the same row. SKIP LOCKED makes them cooperate naturally.
SELECT id, type, tier, attempts, max_attempts
FROM jobs
WHERE state = 'RUNNING'
  AND heartbeat_at < NOW() - $1::interval
ORDER BY heartbeat_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;

-- name: RequeueJob :one
-- Reset a stale RUNNING job back to QUEUED so it'll be picked up again.
-- Increments attempts so we don't retry forever.
UPDATE jobs
SET state = 'QUEUED',
    attempts = attempts + 1,
    heartbeat_at = NULL,
    started_at = NULL,
    updated_at = NOW()
WHERE id = $1
RETURNING *;