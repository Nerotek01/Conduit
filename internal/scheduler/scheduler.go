package scheduler

import (
        "context"
        "fmt"
        "log/slog"
        "time"

        "github.com/robfig/cron/v3"

        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
)

type Scheduler struct {
        store    *storage.Store
        q        *queue.RedisQueue
        logger   *slog.Logger
}

func New(store *storage.Store, q *queue.RedisQueue, logger *slog.Logger) *Scheduler {
        return &Scheduler{store: store, q: q, logger: logger}
}

// Tick runs one scheduling pass: promote due delayed/retrying jobs and
// fire due cron schedules. Exported for deterministic testing and for
// embedding in custom runtimes.
func (s *Scheduler) Tick(ctx context.Context) error {
        if err := s.healOrphanedSchedules(ctx); err != nil {
                return err
        }
        if err := s.promoteDueJobs(ctx); err != nil {
                return err
        }
        return s.fireDueSchedules(ctx)
}

// healOrphanedSchedules repairs schedules stuck in the "claimed for firing"
// state (active with NULL next_run). This happens when a scheduler crashes
// between ClaimDueSchedules and SetScheduleNextRun; without healing those
// schedules would never fire again. The occurrence that was being fired when
// the crash happened is skipped (at-most-once per occurrence) - the priority
// is guaranteeing the schedule itself survives.
func (s *Scheduler) healOrphanedSchedules(ctx context.Context) error {
        orphans, err := s.store.OrphanedSchedules(ctx)
        if err != nil {
                return err
        }
        for i := range orphans {
                sch := orphans[i]
                next, err := NextRun(sch.Cron, sch.Timezone, time.Now())
                if err != nil {
                        s.logger.Error("heal schedule failed: unparseable cron", "schedule_id", sch.ID, "cron", sch.Cron, "error", err)
                        continue
                }
                if err := s.store.SetScheduleNextRun(ctx, sch.ID, next); err != nil {
                        s.logger.Error("heal schedule failed", "schedule_id", sch.ID, "error", err)
                        continue
                }
                s.logger.Warn("healed orphaned schedule after crash window", "schedule_id", sch.ID, "next_run", next)
        }
        return nil
}

func (s *Scheduler) promoteDueJobs(ctx context.Context) error {
        jobs, err := s.store.ClaimDueJobs(ctx, time.Now().UTC(), 500)
        if err != nil {
                return err
        }
        for i := range jobs {
                if err := s.q.Enqueue(ctx, jobs[i].ID, jobs[i].Priority); err != nil {
                        s.logger.Error("enqueue due job failed", "job_id", jobs[i].ID, "error", err)
                }
        }
        return nil
}

func (s *Scheduler) fireDueSchedules(ctx context.Context) error {
        due, err := s.store.ClaimDueSchedules(ctx, time.Now().UTC(), 100)
        if err != nil {
                return err
        }
        for i := range due {
                sch := due[i]
                // Deterministic idempotency key per (schedule, occurrence): a
                // retry or a double-claim of the same due time produces the
                // exact same key, so CreateJob deduplicates instead of firing
                // the occurrence twice.
                key := fmt.Sprintf("sched:%s:%d", sch.ID, sch.DueAt.Unix())
                job, duplicate, err := s.store.CreateJob(ctx, model.SubmitInput{
                        Type:           sch.JobType,
                        Payload:        sch.Payload,
                        Priority:       sch.Priority,
                        MaxRetries:     sch.MaxRetries,
                        IdempotencyKey: &key,
                })
                if err != nil {
                        s.logger.Error("schedule instance creation failed", "schedule_id", sch.ID, "error", err)
                } else if duplicate {
                        // The occurrence already has a job row. If it never
                        // made it onto the stream (enqueue failed or the
                        // process died in between), re-enqueue it; duplicate
                        // stream entries are safe: ClaimRunning conflicts on
                        // non-queued statuses are acked by the worker.
                        if job.Status == model.StatusQueued {
                                if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                                        s.logger.Error("re-enqueue duplicate schedule instance failed", "job_id", job.ID, "error", err)
                                }
                        }
                        s.logger.Info("schedule occurrence already fired (deduplicated)", "schedule_id", sch.ID, "job_id", job.ID)
                } else if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                        // The job row exists in 'queued'; the coordinator's stale-queued
                        // recovery re-enqueues it, but log loudly because execution is
                        // delayed until then.
                        s.logger.Error("enqueue fired schedule failed; relying on stale-queued recovery", "job_id", job.ID, "error", err)
                } else {
                        s.logger.Info("schedule fired", "schedule_id", sch.ID, "job_type", sch.JobType, "job_id", job.ID)
                }
                // Always advance next_run, even when instance creation failed:
                // leaving it NULL would make the row invisible to the claim
                // query and effectively kill the schedule. A failed occurrence
                // is skipped (logged above), the schedule itself survives.
                next, err := NextRun(sch.Cron, sch.Timezone, time.Now())
                if err != nil {
                        s.logger.Error("compute next run failed", "schedule_id", sch.ID, "error", err)
                        continue
                }
                if err := s.store.SetScheduleNextRun(ctx, sch.ID, next); err != nil {
                        s.logger.Error("set next run failed", "schedule_id", sch.ID, "error", err)
                }
        }
        return nil
}

func NextRun(expr, timezone string, from time.Time) (time.Time, error) {
        schedule, err := cron.ParseStandard(expr)
        if err != nil {
                return time.Time{}, err
        }
        loc := time.UTC
        if timezone != "" {
                loaded, err := time.LoadLocation(timezone)
                if err != nil {
                        return time.Time{}, err
                }
                loc = loaded
        }
        next := schedule.Next(from.In(loc))
        return next.UTC(), nil
}
