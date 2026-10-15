-- 0003: index support for the coordinator sweeps and keyset pagination.
CREATE INDEX IF NOT EXISTS jobs_stale_queued_idx ON jobs (updated_at)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS jobs_created_id_idx ON jobs (created_at, id);
