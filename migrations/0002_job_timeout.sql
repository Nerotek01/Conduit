-- 0002: per-job execution timeout (milliseconds; NULL = server default).
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS timeout_ms BIGINT;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'jobs_timeout_chk'
    ) THEN
        ALTER TABLE jobs ADD CONSTRAINT jobs_timeout_chk
            CHECK (timeout_ms IS NULL OR timeout_ms BETWEEN 1000 AND 86400000);
    END IF;
END $$;
