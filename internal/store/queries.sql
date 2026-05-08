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
