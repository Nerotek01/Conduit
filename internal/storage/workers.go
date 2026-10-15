package storage

import (
        "context"
        "errors"
        "fmt"
        "time"

        "github.com/jackc/pgx/v5"
        "github.com/robfig/cron/v3"

        "github.com/Nerotek01/Conduit/internal/model"
)

func (s *Store) UpsertWorker(ctx context.Context, w model.WorkerInfo) error {
        _, err := s.pool.Exec(ctx, `
INSERT INTO workers (id, hostname, status, concurrency, last_heartbeat)
VALUES ($1, $2, 'active', $3, now())
ON CONFLICT (id) DO UPDATE
SET hostname = EXCLUDED.hostname, concurrency = EXCLUDED.concurrency,
    last_heartbeat = now(), status = 'active'`, w.ID, w.Hostname, w.Concurrency)
        if err != nil {
                return fmt.Errorf("upsert worker: %w", err)
        }
        return nil
}

func (s *Store) MarkDeadWorkers(ctx context.Context, deadline time.Time) (int, error) {
        tag, err := s.pool.Exec(ctx, `
UPDATE workers SET status = 'dead'
WHERE status = 'active' AND last_heartbeat < $1`, deadline)
        if err != nil {
                return 0, fmt.Errorf("mark dead workers: %w", err)
        }
        return int(tag.RowsAffected()), nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]model.WorkerInfo, error) {
        rows, err := s.pool.Query(ctx, `SELECT id, hostname, status, last_heartbeat, concurrency, started_at FROM workers ORDER BY id`)
        if err != nil {
                return nil, fmt.Errorf("list workers: %w", err)
        }
        defer rows.Close()
        workers := make([]model.WorkerInfo, 0, 8)
        for rows.Next() {
                var w model.WorkerInfo
                if err := rows.Scan(&w.ID, &w.Hostname, &w.Status, &w.LastHeartbeat, &w.Concurrency, &w.StartedAt); err != nil {
                        return nil, fmt.Errorf("scan worker: %w", err)
                }
                workers = append(workers, w)
        }
        return workers, rows.Err()
}

func (s *Store) ActiveWorkerCount(ctx context.Context) (int, error) {
        var n int
        err := s.pool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE status = 'active'`).Scan(&n)
        if err != nil {
                return 0, fmt.Errorf("count active workers: %w", err)
        }
        return n, nil
}

func (s *Store) CreateSchedule(ctx context.Context, sch model.Schedule) (*model.Schedule, error) {
        if err := validCron(sch.Cron); err != nil {
                return nil, err
        }
        tz := sch.Timezone
        if tz == "" {
                tz = "UTC"
        }
        priority := sch.Priority
        if priority == 0 {
                priority = model.PriorityNormal
        }
        // Defense-in-depth: keep non-API callers (tests, future CLI tools) from
        // hitting the DB CHECK constraint with an opaque pg error.
        if priority < model.MinPriority || priority > model.MaxPriority {
                return nil, fmt.Errorf("priority must be between %d and %d", model.MinPriority, model.MaxPriority)
        }
        if sch.MaxRetries < 0 || sch.MaxRetries > model.MaxMaxRetries {
                return nil, fmt.Errorf("max_retries must be between 0 and %d", model.MaxMaxRetries)
        }
        if _, err := time.LoadLocation(tz); err != nil {
                return nil, fmt.Errorf("unknown timezone %q", tz)
        }
        row := s.pool.QueryRow(ctx, `
INSERT INTO schedules (job_type, payload, cron_expr, timezone, priority, max_retries, next_run)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, job_type, payload, cron_expr, timezone, priority, max_retries, next_run, active, created_at`,
                sch.JobType, []byte(payloadOrDefault(sch.Payload)), sch.Cron, tz, priority, sch.MaxRetries, sch.NextRun)
        return scanSchedule(row)
}

func validCron(expr string) error {
        if len(expr) == 0 || len(expr) > 128 {
                return fmt.Errorf("cron expression must be 1-128 characters")
        }
        // Parse for real: a length-only check would let garbage through now that
        // ClaimDueSchedules no longer hides bad rows behind a NULL next_run.
        if _, err := cron.ParseStandard(expr); err != nil {
                return fmt.Errorf("invalid cron expression: %w", err)
        }
        return nil
}

func scanSchedule(row pgx.Row) (*model.Schedule, error) {
        var sch model.Schedule
        if err := row.Scan(&sch.ID, &sch.JobType, &sch.Payload, &sch.Cron, &sch.Timezone, &sch.Priority,
                &sch.MaxRetries, &sch.NextRun, &sch.Active, &sch.CreatedAt); err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: schedule", ErrNotFound)
                }
                return nil, err
        }
        return &sch, nil
}

func (s *Store) ListSchedules(ctx context.Context) ([]model.Schedule, error) {
        rows, err := s.pool.Query(ctx, `SELECT id, job_type, payload, cron_expr, timezone, priority, max_retries, next_run, active, created_at FROM schedules ORDER BY created_at`)
        if err != nil {
                return nil, fmt.Errorf("list schedules: %w", err)
        }
        defer rows.Close()
        out := make([]model.Schedule, 0, 8)
        for rows.Next() {
                var sch model.Schedule
                if err := rows.Scan(&sch.ID, &sch.JobType, &sch.Payload, &sch.Cron, &sch.Timezone, &sch.Priority,
                        &sch.MaxRetries, &sch.NextRun, &sch.Active, &sch.CreatedAt); err != nil {
                        return nil, fmt.Errorf("scan schedule: %w", err)
                }
                out = append(out, sch)
        }
        return out, rows.Err()
}

func (s *Store) DeleteSchedule(ctx context.Context, id string) (bool, error) {
        if err := validUUID(id); err != nil {
                return false, err
        }
        tag, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, id)
        if err != nil {
                return false, fmt.Errorf("delete schedule: %w", err)
        }
        return tag.RowsAffected() == 1, nil
}

func (s *Store) ClaimDueSchedules(ctx context.Context, now time.Time, limit int) ([]model.Schedule, error) {
        if limit < 1 || limit > 500 {
                limit = 100
        }
        // The claim marks rows as "firing" by nulling next_run, but returns the
        // OLD next_run as DueAt via the CTE so the firing path can (a) build a
        // deterministic idempotency key per occurrence and (b) not lose the due
        // time it claimed. RETURNING alone cannot express the pre-update value.
        rows, err := s.pool.Query(ctx, `
WITH due AS (
    SELECT id, next_run AS due_at
    FROM schedules
    WHERE active AND next_run IS NOT NULL AND next_run <= $1
    ORDER BY next_run
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE schedules SET next_run = NULL
FROM due
WHERE schedules.id = due.id
RETURNING schedules.id, job_type, payload, cron_expr, timezone, priority, max_retries, next_run, active, created_at, due.due_at`, now, limit)
        if err != nil {
                return nil, fmt.Errorf("claim due schedules: %w", err)
        }
        defer rows.Close()
        out := make([]model.Schedule, 0, 4)
        for rows.Next() {
                var sch model.Schedule
                if err := rows.Scan(&sch.ID, &sch.JobType, &sch.Payload, &sch.Cron, &sch.Timezone, &sch.Priority,
                        &sch.MaxRetries, &sch.NextRun, &sch.Active, &sch.CreatedAt, &sch.DueAt); err != nil {
                        return nil, fmt.Errorf("scan schedule: %w", err)
                }
                out = append(out, sch)
        }
        return out, rows.Err()
}

// OrphanedSchedules lists active schedules whose next_run is NULL: rows a
// scheduler claimed for firing but never finished (crash between the claim
// and SetScheduleNextRun). Without healing, the claim query's
// "next_run IS NOT NULL" predicate makes them invisible forever and the
// schedule silently dies. Healing is idempotent: concurrent healers compute
// the same next_run from the same cron expression.
func (s *Store) OrphanedSchedules(ctx context.Context) ([]model.Schedule, error) {
        rows, err := s.pool.Query(ctx, `
SELECT id, job_type, payload, cron_expr, timezone, priority, max_retries, next_run, active, created_at
FROM schedules WHERE active AND next_run IS NULL`)
        if err != nil {
                return nil, fmt.Errorf("orphaned schedules: %w", err)
        }
        defer rows.Close()
        out := make([]model.Schedule, 0, 4)
        for rows.Next() {
                var sch model.Schedule
                if err := rows.Scan(&sch.ID, &sch.JobType, &sch.Payload, &sch.Cron, &sch.Timezone, &sch.Priority,
                        &sch.MaxRetries, &sch.NextRun, &sch.Active, &sch.CreatedAt); err != nil {
                        return nil, fmt.Errorf("scan schedule: %w", err)
                }
                out = append(out, sch)
        }
        return out, rows.Err()
}

func (s *Store) SetScheduleNextRun(ctx context.Context, id string, next time.Time) error {
        tag, err := s.pool.Exec(ctx, `UPDATE schedules SET next_run = $2 WHERE id = $1`, id, next)
        if err != nil {
                return fmt.Errorf("set next run: %w", err)
        }
        if tag.RowsAffected() == 0 {
                return fmt.Errorf("%w: schedule %s", ErrNotFound, id)
        }
        return nil
}
