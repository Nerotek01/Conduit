CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'queued',
    priority SMALLINT NOT NULL DEFAULT 2,
    attempts INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    idempotency_key TEXT,
    scheduled_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_error TEXT,
    worker_id TEXT,
    result JSONB,
    run_id UUID,
    node_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT jobs_status_chk CHECK (status IN ('queued','scheduled','retrying','running','succeeded','dead','cancelled')),
    CONSTRAINT jobs_priority_chk CHECK (priority BETWEEN 1 AND 4),
    CONSTRAINT jobs_attempts_chk CHECK (attempts >= 0),
    CONSTRAINT jobs_max_retries_chk CHECK (max_retries BETWEEN 0 AND 25)
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key_uidx
    ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status);

CREATE INDEX IF NOT EXISTS jobs_due_idx ON jobs (scheduled_at)
    WHERE status IN ('scheduled','retrying');

CREATE INDEX IF NOT EXISTS jobs_run_idx ON jobs (run_id);

CREATE INDEX IF NOT EXISTS jobs_type_status_idx ON jobs (type, status);

CREATE INDEX IF NOT EXISTS jobs_worker_idx ON jobs (worker_id)
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    node_states JSONB NOT NULL DEFAULT '{}'::jsonb,
    version INT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT run_status_chk CHECK (status IN ('pending','running','succeeded','failed','cancelled'))
);

CREATE INDEX IF NOT EXISTS workflow_runs_wf_idx ON workflow_runs (workflow_id);

CREATE INDEX IF NOT EXISTS workflow_runs_status_idx ON workflow_runs (status)
    WHERE status IN ('pending','running');

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    concurrency INT NOT NULL DEFAULT 8,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT worker_status_chk CHECK (status IN ('active','dead'))
);

CREATE INDEX IF NOT EXISTS workers_heartbeat_idx ON workers (last_heartbeat)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    cron_expr TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    priority SMALLINT NOT NULL DEFAULT 2,
    max_retries INT NOT NULL DEFAULT 3,
    next_run TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT schedule_priority_chk CHECK (priority BETWEEN 1 AND 4),
    CONSTRAINT schedule_max_retries_chk CHECK (max_retries BETWEEN 0 AND 25)
);

CREATE INDEX IF NOT EXISTS schedules_due_idx ON schedules (next_run)
    WHERE active;
