-- The devices table is referenced by jobs (foreign key).
-- We keep it minimal: just an id and a created_at. Stage 3 will add more.
CREATE TABLE devices (
    id          TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Job state machine (string-based for legibility — `text` is cheap in PG).
-- Allowed values: ACCEPTED, QUEUED, RUNNING, SUCCESS, FAILED, RETRY.
-- We don't use a Postgres ENUM type because adding new states later requires
-- ALTER TYPE which is awkward. A CHECK constraint plus discipline in code is fine.
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT NOT NULL,
    tier            TEXT NOT NULL,
    device_id       TEXT NOT NULL REFERENCES devices(id),
    state           TEXT NOT NULL DEFAULT 'ACCEPTED'
                    CHECK (state IN ('ACCEPTED','QUEUED','RUNNING','SUCCESS','FAILED','RETRY')),
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    last_error      TEXT,
    heartbeat_at    TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotency: same (idempotency_key, type) → same job. Submit twice, get
-- the same job_id. Enforced at the database level, not just in code, so
-- two concurrent submitters can't race past application-level checks.
CREATE UNIQUE INDEX jobs_idempotency_key_unique
    ON jobs (idempotency_key, type);

-- Hot index for the worker: "give me the job by id."
-- (PRIMARY KEY already provides this — included for explicitness.)

-- Hot index for the reaper (Stage 3): scan RUNNING jobs with stale heartbeat.
CREATE INDEX jobs_running_heartbeat
    ON jobs (heartbeat_at)
    WHERE state = 'RUNNING';
