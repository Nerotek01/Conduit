package coordinator

import (
        "context"
        "log/slog"
        "sync/atomic"
        "time"

        "github.com/redis/go-redis/v9"

        "github.com/Nerotek01/Conduit/internal/lock"
        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/scheduler"
        "github.com/Nerotek01/Conduit/internal/storage"
        "github.com/Nerotek01/Conduit/internal/workflow"
)

const (
        leaderLockKey     = "coordinator:leader"
        leaderLockTTL     = 15 * time.Second
        leaderRefreshEvery = 5 * time.Second
        reaperInterval    = 5 * time.Second
        recoveryInterval  = 30 * time.Second
        staleQueuedAge    = 30 * time.Second
        reapBatch         = 200
        reapConsumer      = "conduit-reaper"
)

type Options struct {
        QueuePrefix       string
        VisibilityTimeout time.Duration
        HeartbeatInterval time.Duration
        SchedulerInterval time.Duration
}

type Coordinator struct {
        store         *storage.Store
        q             *queue.RedisQueue
        engine        *workflow.Engine
        sched         *scheduler.Scheduler
        lock          *lock.RedisLock
        logger        *slog.Logger
        visibility    time.Duration
        heartbeat     time.Duration
        schedInterval time.Duration
        leading       atomic.Bool
}

func New(store *storage.Store, q *queue.RedisQueue, redisClient *redis.Client, logger *slog.Logger, opts Options) *Coordinator {
        submitter := workflow.NewSubmitter(store, q)
        engine := workflow.NewEngine(store, submitter.Submit, submitter.Cancel)
        sched := scheduler.New(store, q, logger)
        heartbeat := opts.HeartbeatInterval
        if heartbeat <= 0 {
                heartbeat = 5 * time.Second
        }
        visibility := opts.VisibilityTimeout
        if visibility <= 0 {
                visibility = 60 * time.Second
        }
        schedInterval := opts.SchedulerInterval
        if schedInterval <= 0 {
                schedInterval = time.Second
        }
        return &Coordinator{
                store:         store,
                q:             q,
                engine:        engine,
                sched:         sched,
                lock:          lock.New(redisClient, opts.QueuePrefix+":"+leaderLockKey, leaderLockTTL),
                logger:        logger,
                visibility:    visibility,
                heartbeat:     heartbeat,
                schedInterval: schedInterval,
        }
}

func (c *Coordinator) Run(ctx context.Context) error {
        if err := c.q.EnsureGroups(ctx); err != nil {
                return err
        }
        go c.schedLoop(ctx)
        go c.reapLoop(ctx)
        go c.recoveryLoop(ctx)

        ticker := time.NewTicker(leaderRefreshEvery)
        defer ticker.Stop()
        for {
                select {
                case <-ctx.Done():
                        c.leading.Store(false)
                        _ = c.lock.Release(ctx)
                        return ctx.Err()
                case <-ticker.C:
                        var ok bool
                        var err error
                        if c.leading.Load() {
                                ok, err = c.lock.Refresh(ctx)
                        } else {
                                ok, err = c.lock.Acquire(ctx)
                        }
                        if err != nil {
                                c.logger.Warn("coordinator lock error, continuing in shared mode", "error", err)
                                c.leading.Store(false)
                                continue
                        }
                        if ok && !c.leading.Load() {
                                c.leading.Store(true)
                                c.logger.Info("coordinator acquired leadership")
                        } else if !ok && c.leading.Load() {
                                c.leading.Store(false)
                                c.logger.Warn("coordinator lost leadership")
                        }
                }
        }
}

func (c *Coordinator) schedLoop(ctx context.Context) {
        ticker := time.NewTicker(c.schedInterval)
        defer ticker.Stop()
        for {
                select {
                case <-ctx.Done():
                        return
                case <-ticker.C:
                        if !c.leading.Load() {
                                continue
                        }
                        if err := c.sched.Tick(ctx); err != nil {
                                c.logger.Error("scheduler tick failed", "error", err)
                        }
                }
        }
}

func (c *Coordinator) reapLoop(ctx context.Context) {
        ticker := time.NewTicker(reaperInterval)
        defer ticker.Stop()
        for {
                select {
                case <-ctx.Done():
                        return
                case <-ticker.C:
                        if !c.leading.Load() {
                                continue
                        }
                        if err := c.reap(ctx); err != nil {
                                c.logger.Error("reap failed", "error", err)
                        }
                        if err := c.recoverStaleQueued(ctx); err != nil {
                                c.logger.Error("stale queued recovery failed", "error", err)
                        }
                }
        }
}

func (c *Coordinator) recoveryLoop(ctx context.Context) {
        ticker := time.NewTicker(recoveryInterval)
        defer ticker.Stop()
        for {
                select {
                case <-ctx.Done():
                        return
                case <-ticker.C:
                        if !c.leading.Load() {
                                continue
                        }
                        if n, err := c.engine.RecoverRuns(ctx); err != nil {
                                c.logger.Error("workflow recovery failed", "error", err)
                        } else if n > 0 {
                                c.logger.Info("recovered workflow runs", "count", n)
                        }
                }
        }
}

func (c *Coordinator) reap(ctx context.Context) error {
        deadline := time.Now().UTC().Add(-2 * c.heartbeat)
        if _, err := c.store.MarkDeadWorkers(ctx, deadline); err != nil {
                return err
        }
        claimed, err := c.q.ReclaimPending(ctx, reapConsumer, c.visibility, reapBatch)
        if err != nil {
                return err
        }
        if len(claimed) == 0 {
                return nil
        }
        workers, err := c.store.ListWorkers(ctx)
        if err != nil {
                return err
        }
        for _, msg := range claimed {
                if err := c.reconcile(ctx, msg, workers); err != nil {
                        c.logger.Error("reconcile failed", "job_id", msg.JobID, "error", err)
                }
        }
        return nil
}

func (c *Coordinator) reconcile(ctx context.Context, msg queue.Message, workers []model.WorkerInfo) error {
        job, err := c.store.GetJob(ctx, msg.JobID)
        if err != nil {
                return c.q.Ack(ctx, msg.Stream, msg.ID)
        }
        switch job.Status {
        case model.StatusQueued:
                if err := c.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                        return err
                }
                return c.q.Ack(ctx, msg.Stream, msg.ID)
        case model.StatusRunning:
                if workerAlive(workers, job, 2*c.heartbeat) {
                        return nil
                }
                requeued, err := c.store.RequeueOrphaned(ctx, job.ID)
                if err != nil {
                        return err
                }
                if requeued {
                        if err := c.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                                return err
                        }
                }
                return c.q.Ack(ctx, msg.Stream, msg.ID)
        default:
                return c.q.Ack(ctx, msg.Stream, msg.ID)
        }
}

func workerAlive(workers []model.WorkerInfo, job *model.Job, staleAfter time.Duration) bool {
        if job.WorkerID == nil {
                return false
        }
        for _, w := range workers {
                if w.ID == *job.WorkerID {
                        return w.Status == "active" && time.Since(w.LastHeartbeat) < staleAfter
                }
        }
        return false
}

func (c *Coordinator) recoverStaleQueued(ctx context.Context) error {
        olderThan := time.Now().UTC().Add(-staleQueuedAge)
        jobs, err := c.store.StaleQueuedJobs(ctx, olderThan, reapBatch)
        if err != nil {
                return err
        }
        for i := range jobs {
                if err := c.q.Enqueue(ctx, jobs[i].ID, jobs[i].Priority); err != nil {
                        c.logger.Error("re-enqueue stale queued failed", "job_id", jobs[i].ID, "error", err)
                }
        }
        return nil
}
