package storage

import (
        "context"
        "errors"
        "fmt"
        "time"

        "github.com/google/uuid"
        "github.com/jackc/pgx/v5"
        "github.com/jackc/pgx/v5/pgconn"

        "github.com/Nerotek01/Conduit/internal/model"
)

var (
        ErrNotFound       = errors.New("not found")
        ErrDuplicateKey   = errors.New("duplicate idempotency key")
        ErrStatusConflict = errors.New("status conflict")
)

const jobColumns = `id, type, payload, status, priority, attempts, max_retries, idempotency_key,
COALESCE(timeout_ms, 0) AS timeout_ms,
scheduled_at, started_at, finished_at, last_error, worker_id, result, run_id, node_id,
created_at, updated_at`

func scanJob(row pgx.Row) (*model.Job, error) {
        var j model.Job
        err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Priority, &j.Attempts, &j.MaxRetries,
                &j.IdempotencyKey, &j.TimeoutMS, &j.ScheduledAt, &j.StartedAt, &j.FinishedAt, &j.LastError, &j.WorkerID,
                &j.Result, &j.RunID, &j.NodeID, &j.CreatedAt, &j.UpdatedAt)
        if err != nil {
                return nil, err
        }
        return &j, nil
}

func validUUID(id string) error {
        if _, err := uuid.Parse(id); err != nil {
                return fmt.Errorf("%w: invalid id %q", ErrNotFound, id)
        }
        return nil
}

func (s *Store) CreateJob(ctx context.Context, in model.SubmitInput) (*model.Job, bool, error) {
        var scheduledStatus string
        var runAt *time.Time
        if in.RunAt != nil && in.RunAt.After(time.Now().Add(50*time.Millisecond)) {
                scheduledStatus = string(model.StatusScheduled)
                runAt = in.RunAt
        } else {
                scheduledStatus = string(model.StatusQueued)
        }
        maxRetries := in.MaxRetries
        if maxRetries < 0 {
                maxRetries = 0
        }
        priority := in.Priority
        if priority == 0 {
                priority = model.PriorityNormal
        }
        var timeoutMS *int64
        if in.TimeoutMS > 0 {
                timeoutMS = &in.TimeoutMS
        }
        row := s.pool.QueryRow(ctx, `
INSERT INTO jobs (type, payload, status, priority, max_retries, idempotency_key, timeout_ms, scheduled_at, run_id, node_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING `+jobColumns,
                in.Type, []byte(payloadOrDefault(in.Payload)), scheduledStatus, priority, maxRetries,
                in.IdempotencyKey, timeoutMS, runAt, in.RunID, in.NodeID)
        job, err := scanJob(row)
        if err != nil {
                var pgErr *pgconn.PgError
                if errors.As(err, &pgErr) && pgErr.Code == "23505" {
                        // A unique violation can only come from the partial
                        // idempotency-key index, whose predicate requires a
                        // non-NULL key; guard anyway so a future unique index
                        // on another column cannot panic the process here.
                        if in.IdempotencyKey == nil {
                                return nil, false, fmt.Errorf("insert job: unique constraint %s: %w", pgErr.ConstraintName, ErrDuplicateKey)
                        }
                        existing, getErr := s.GetJobByIdempotencyKey(ctx, *in.IdempotencyKey)
                        if getErr != nil {
                                return nil, false, fmt.Errorf("duplicate key lookup: %w", getErr)
                        }
                        return existing, true, nil
                }
                return nil, false, fmt.Errorf("insert job: %w", err)
        }
        return job, false, nil
}

func payloadOrDefault(p []byte) []byte {
        if len(p) == 0 {
                return []byte(`{}`)
        }
        return p
}

func (s *Store) GetJob(ctx context.Context, id string) (*model.Job, error) {
        if err := validUUID(id); err != nil {
                return nil, err
        }
        row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: job %s", ErrNotFound, id)
                }
                return nil, fmt.Errorf("get job: %w", err)
        }
        return job, nil
}

func (s *Store) GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error) {
        row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, key)
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: idempotency key", ErrNotFound)
                }
                return nil, fmt.Errorf("get job by key: %w", err)
        }
        return job, nil
}

type ListFilter struct {
        Status   *model.JobStatus
        Type     string
        RunID    *string
        Cursor   *time.Time
        BeforeID string
        Limit    int
}

func (s *Store) ListJobs(ctx context.Context, f ListFilter) ([]model.Job, bool, error) {
        limit := f.Limit
        if limit < 1 || limit > 200 {
                limit = 50
        }
        args := []any{}
        q := `SELECT ` + jobColumns + ` FROM jobs WHERE 1=1`
        if f.Status != nil && *f.Status != "" {
                args = append(args, string(*f.Status))
                q += fmt.Sprintf(" AND status = $%d", len(args))
        }
        if f.Type != "" {
                args = append(args, f.Type)
                q += fmt.Sprintf(" AND type = $%d", len(args))
        }
        if f.RunID != nil {
                if err := validUUID(*f.RunID); err != nil {
                        return nil, false, err
                }
                args = append(args, *f.RunID)
                q += fmt.Sprintf(" AND run_id = $%d", len(args))
        }
        if f.Cursor != nil {
                args = append(args, *f.Cursor)
                // Row-value comparison against (created_at, id): the id part
                // MUST be the last row's id, not a hardcoded zero-uuid.
                // Otherwise every row sharing the boundary row's created_at is
                // excluded by the comparison and silently skipped - with
                // batch submissions, many jobs share the same timestamp.
                beforeID := f.BeforeID
                if beforeID == "" {
                        beforeID = "00000000-0000-0000-0000-000000000000"
                } else if err := validUUID(beforeID); err != nil {
                        return nil, false, err
                }
                args = append(args, beforeID)
                q += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
        }
        q += " ORDER BY created_at DESC, id DESC"
        args = append(args, limit+1)
        q += fmt.Sprintf(" LIMIT $%d", len(args))

        rows, err := s.pool.Query(ctx, q, args...)
        if err != nil {
                return nil, false, fmt.Errorf("list jobs: %w", err)
        }
        defer rows.Close()
        jobs := make([]model.Job, 0, limit)
        for rows.Next() {
                j := model.Job{}
                if err := rows.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Priority, &j.Attempts, &j.MaxRetries,
                        &j.IdempotencyKey, &j.TimeoutMS, &j.ScheduledAt, &j.StartedAt, &j.FinishedAt, &j.LastError, &j.WorkerID,
                        &j.Result, &j.RunID, &j.NodeID, &j.CreatedAt, &j.UpdatedAt); err != nil {
                        return nil, false, fmt.Errorf("scan job: %w", err)
                }
                jobs = append(jobs, j)
        }
        if err := rows.Err(); err != nil {
                return nil, false, fmt.Errorf("iterate jobs: %w", err)
        }
        more := false
        if len(jobs) > limit {
                jobs = jobs[:limit]
                more = true
        }
        return jobs, more, nil
}

func (s *Store) ClaimRunning(ctx context.Context, id, workerID string) (*model.Job, error) {
        if err := validUUID(id); err != nil {
                return nil, err
        }
        row := s.pool.QueryRow(ctx, `
UPDATE jobs SET status = 'running', started_at = now(), finished_at = NULL,
worker_id = $2, attempts = attempts + 1, updated_at = now()
WHERE id = $1 AND status IN ('queued','retrying')
RETURNING `+jobColumns, id, workerID)
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: job %s not claimable", ErrStatusConflict, id)
                }
                return nil, fmt.Errorf("claim job: %w", err)
        }
        return job, nil
}

func (s *Store) MarkSucceeded(ctx context.Context, id string, result []byte) (*model.Job, error) {
        row := s.pool.QueryRow(ctx, `
UPDATE jobs SET status = 'succeeded', finished_at = now(), result = $2,
worker_id = NULL, updated_at = now()
WHERE id = $1 AND status = 'running'
RETURNING `+jobColumns, id, result)
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: job %s not running", ErrStatusConflict, id)
                }
                return nil, fmt.Errorf("mark succeeded: %w", err)
        }
        return job, nil
}

func (s *Store) MarkRetry(ctx context.Context, id string, runAt time.Time, lastErr string) (*model.Job, error) {
        row := s.pool.QueryRow(ctx, `
UPDATE jobs SET status = 'retrying', scheduled_at = $2, last_error = $3,
worker_id = NULL, finished_at = NULL, updated_at = now()
WHERE id = $1 AND status = 'running'
RETURNING `+jobColumns, id, runAt, truncateErr(lastErr))
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: job %s not running", ErrStatusConflict, id)
                }
                return nil, fmt.Errorf("mark retry: %w", err)
        }
        return job, nil
}

func (s *Store) MarkDead(ctx context.Context, id string, lastErr string) (*model.Job, error) {
        row := s.pool.QueryRow(ctx, `
UPDATE jobs SET status = 'dead', finished_at = now(), last_error = $2,
worker_id = NULL, updated_at = now()
WHERE id = $1 AND status IN ('running','queued','retrying')
RETURNING `+jobColumns, id, truncateErr(lastErr))
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: job %s", ErrStatusConflict, id)
                }
                return nil, fmt.Errorf("mark dead: %w", err)
        }
        return job, nil
}

func (s *Store) CancelJob(ctx context.Context, id string) (*model.Job, error) {
        if err := validUUID(id); err != nil {
                return nil, err
        }
        row := s.pool.QueryRow(ctx, `
UPDATE jobs SET status = 'cancelled', finished_at = now(), worker_id = NULL, updated_at = now()
WHERE id = $1 AND status IN ('queued','scheduled','retrying')
RETURNING `+jobColumns, id)
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        current, getErr := s.GetJob(ctx, id)
                        if getErr != nil {
                                return nil, getErr
                        }
                        return current, fmt.Errorf("%w: job %s in status %s", ErrStatusConflict, id, current.Status)
                }
                return nil, fmt.Errorf("cancel job: %w", err)
        }
        return job, nil
}

// RequeueJob puts a terminal dead or cancelled job back on the queue with a
// fresh retry budget. The CAS guard (status IN dead/cancelled) makes it safe
// to call concurrently and rejects any non-requeueable job with
// ErrStatusConflict (surfaced as HTTP 409 by the API layer).
func (s *Store) RequeueJob(ctx context.Context, id string) (*model.Job, error) {
        if err := validUUID(id); err != nil {
                return nil, err
        }
        row := s.pool.QueryRow(ctx, `
UPDATE jobs SET status = 'queued', attempts = 0, last_error = NULL, worker_id = NULL,
        started_at = NULL, finished_at = NULL, scheduled_at = NULL, updated_at = now()
WHERE id = $1 AND status IN ('dead','cancelled')
RETURNING `+jobColumns, id)
        job, err := scanJob(row)
        if err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        current, getErr := s.GetJob(ctx, id)
                        if getErr != nil {
                                return nil, getErr
                        }
                        return current, fmt.Errorf("%w: job %s in status %s", ErrStatusConflict, id, current.Status)
                }
                return nil, fmt.Errorf("requeue job: %w", err)
        }
        return job, nil
}

func (s *Store) ClaimDueJobs(ctx context.Context, now time.Time, limit int) ([]model.Job, error) {
        if limit < 1 || limit > 1000 {
                limit = 500
        }
        rows, err := s.pool.Query(ctx, `
UPDATE jobs SET status = 'queued', scheduled_at = NULL, updated_at = now()
WHERE id IN (
    SELECT id FROM jobs
    WHERE status IN ('scheduled','retrying') AND scheduled_at <= $1
    ORDER BY priority, scheduled_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING `+jobColumns, now, limit)
        if err != nil {
                return nil, fmt.Errorf("claim due jobs: %w", err)
        }
        defer rows.Close()
        return collectJobs(rows)
}

func (s *Store) StaleQueuedJobs(ctx context.Context, olderThan time.Time, limit int) ([]model.Job, error) {
        if limit < 1 || limit > 1000 {
                limit = 500
        }
        // Touch updated_at on the rows we hand out so each re-enqueued job gets a
        // fresh lease: without this, a backlogged queued job would be re-enqueued
        // every sweep (duplicate stream messages proportional to backlog depth).
        rows, err := s.pool.Query(ctx, `
UPDATE jobs SET updated_at = now()
WHERE id IN (
    SELECT id FROM jobs
    WHERE status = 'queued' AND updated_at < $1
    ORDER BY updated_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING `+jobColumns, olderThan, limit)
        if err != nil {
                return nil, fmt.Errorf("stale queued jobs: %w", err)
        }
        defer rows.Close()
        return collectJobs(rows)
}

func (s *Store) OrphanedRunningJobs(ctx context.Context, heartbeatDeadline time.Time, limit int) ([]model.Job, error) {
        if limit < 1 || limit > 1000 {
                limit = 500
        }
        rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs
WHERE status = 'running' AND worker_id IS NOT NULL AND worker_id IN (
    SELECT id FROM workers WHERE status = 'dead' OR last_heartbeat < $1
) LIMIT $2`, heartbeatDeadline, limit)
        if err != nil {
                return nil, fmt.Errorf("orphaned running jobs: %w", err)
        }
        defer rows.Close()
        return collectJobs(rows)
}

func (s *Store) RequeueOrphaned(ctx context.Context, id string) (bool, error) {
        tag, err := s.pool.Exec(ctx, `
UPDATE jobs SET status = 'queued', worker_id = NULL, updated_at = now()
WHERE id = $1 AND status = 'running'`, id)
        if err != nil {
                return false, fmt.Errorf("requeue orphaned: %w", err)
        }
        return tag.RowsAffected() == 1, nil
}

func (s *Store) CountByStatus(ctx context.Context) (map[string]int64, error) {
        rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
        if err != nil {
                return nil, fmt.Errorf("count by status: %w", err)
        }
        defer rows.Close()
        out := make(map[string]int64, 8)
        for rows.Next() {
                var status string
                var count int64
                if err := rows.Scan(&status, &count); err != nil {
                        return nil, fmt.Errorf("scan count: %w", err)
                }
                out[status] = count
        }
        return out, rows.Err()
}

func collectJobs(rows pgx.Rows) ([]model.Job, error) {
        jobs := make([]model.Job, 0, 16)
        for rows.Next() {
                j := model.Job{}
                if err := rows.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Priority, &j.Attempts, &j.MaxRetries,
                        &j.IdempotencyKey, &j.TimeoutMS, &j.ScheduledAt, &j.StartedAt, &j.FinishedAt, &j.LastError, &j.WorkerID,
                        &j.Result, &j.RunID, &j.NodeID, &j.CreatedAt, &j.UpdatedAt); err != nil {
                        return nil, fmt.Errorf("scan job: %w", err)
                }
                jobs = append(jobs, j)
        }
        if err := rows.Err(); err != nil {
                return nil, fmt.Errorf("iterate jobs: %w", err)
        }
        return jobs, nil
}

func truncateErr(msg string) string {
        if len(msg) > 2048 {
                return msg[:2048]
        }
        return msg
}
