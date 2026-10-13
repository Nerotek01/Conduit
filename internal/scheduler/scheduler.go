package scheduler

import (
        "context"
        "log/slog"
        "time"

        "github.com/robfig/cron/v3"

        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
)

type Scheduler struct {
        store     *storage.Store
        q         *queue.RedisQueue
        interval  time.Duration
        logger    *slog.Logger
}

func New(store *storage.Store, q *queue.RedisQueue, interval time.Duration, logger *slog.Logger) *Scheduler {
        if interval < 250*time.Millisecond {
                interval = 250 * time.Millisecond
        }
        return &Scheduler{store: store, q: q, interval: interval, logger: logger}
}

func (s *Scheduler) Run(ctx context.Context) error {
        ticker := time.NewTicker(s.interval)
        defer ticker.Stop()
        for {
                select {
                case <-ctx.Done():
                        return ctx.Err()
                case <-ticker.C:
                        if err := s.Tick(ctx); err != nil {
                                s.logger.Error("scheduler tick failed", "error", err)
                        }
                }
        }
}

// Tick runs one scheduling pass: promote due delayed/retrying jobs and
// fire due cron schedules. Exported for deterministic testing and for
// embedding in custom runtimes.
func (s *Scheduler) Tick(ctx context.Context) error {
        if err := s.promoteDueJobs(ctx); err != nil {
                return err
        }
        return s.fireDueSchedules(ctx)
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
                job, duplicate, err := s.store.CreateJob(ctx, model.SubmitInput{
                        Type:       sch.JobType,
                        Payload:    sch.Payload,
                        Priority:   sch.Priority,
                        MaxRetries: sch.MaxRetries,
                })
                if err != nil {
                        s.logger.Error("schedule instance creation failed", "schedule_id", sch.ID, "error", err)
                } else if duplicate {
                        s.logger.Warn("schedule instance collided with existing idempotency key", "schedule_id", sch.ID)
                } else if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                        // The job row exists in 'queued'; the coordinator's stale-queued
                        // recovery re-enqueues it, but log loudly because execution is
                        // delayed until then.
                        s.logger.Error("enqueue fired schedule failed; relying on stale-queued recovery", "job_id", job.ID, "error", err)
                } else {
                        s.logger.Info("schedule fired", "schedule_id", sch.ID, "job_type", sch.JobType, "job_id", job.ID)
                }
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
