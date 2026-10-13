package worker

import (
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "log/slog"
        "os"
        "sync"
        "time"

        "github.com/Nerotek01/Conduit/internal/events"
        "github.com/Nerotek01/Conduit/internal/metrics"
        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/retry"
        "github.com/Nerotek01/Conduit/internal/storage"
)

type Handler func(ctx context.Context, job *model.Job) (json.RawMessage, error)

type Registry struct {
        mu       sync.RWMutex
        handlers map[string]Handler
}

func NewRegistry() *Registry {
        return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) error {
        if !model.JobTypePattern.MatchString(jobType) {
                return fmt.Errorf("invalid job type %q", jobType)
        }
        r.mu.Lock()
        defer r.mu.Unlock()
        if _, exists := r.handlers[jobType]; exists {
                return fmt.Errorf("handler for %q already registered", jobType)
        }
        r.handlers[jobType] = h
        return nil
}

func (r *Registry) Lookup(jobType string) (Handler, bool) {
        r.mu.RLock()
        defer r.mu.RUnlock()
        h, ok := r.handlers[jobType]
        return h, ok
}

type TerminalHook func(ctx context.Context, job *model.Job) error

type Pool struct {
        store          *storage.Store
        q              *queue.RedisQueue
        registry       *Registry
        bus            events.Bus
        met            *metrics.Metrics
        logger         *slog.Logger
        id             string
        hostname       string
        concurrency    int
        heartbeat       time.Duration
        defaultTimeout time.Duration
        retryBase      time.Duration
        retryMax       time.Duration
        hook           TerminalHook
}

type Options struct {
        WorkerID       string
        Hostname       string
        Concurrency    int
        Heartbeat      time.Duration
        Hook           TerminalHook
        DefaultTimeout time.Duration
        RetryBaseDelay time.Duration
        RetryMaxDelay  time.Duration
}

func NewPool(store *storage.Store, q *queue.RedisQueue, registry *Registry, bus events.Bus, met *metrics.Metrics, logger *slog.Logger, opts Options) (*Pool, error) {
        id := opts.WorkerID
        if id == "" {
                host, _ := os.Hostname()
                id = fmt.Sprintf("w-%d-%s", os.Getpid(), host)
        }
        hostname := opts.Hostname
        if hostname == "" {
                hostname, _ = os.Hostname()
        }
        if opts.Concurrency < 1 || opts.Concurrency > 512 {
                return nil, fmt.Errorf("concurrency must be between 1 and 512")
        }
        if registry == nil {
                return nil, errors.New("handler registry is required")
        }
        if bus == nil {
                bus = events.NoopBus{}
        }
        heartbeat := opts.Heartbeat
        if heartbeat <= 0 {
                heartbeat = 5 * time.Second
        }
        defaultTimeout := opts.DefaultTimeout
        if defaultTimeout <= 0 {
                defaultTimeout = DefaultJobTimeout
        }
        retryBase := opts.RetryBaseDelay
        if retryBase <= 0 {
                retryBase = 500 * time.Millisecond
        }
        retryMax := opts.RetryMaxDelay
        if retryMax < retryBase {
                retryMax = 5 * time.Minute
        }
        return &Pool{
                store:          store,
                q:              q,
                registry:       registry,
                bus:            bus,
                met:            met,
                logger:         logger,
                id:             id,
                hostname:       hostname,
                concurrency:    opts.Concurrency,
                heartbeat:       heartbeat,
                defaultTimeout: defaultTimeout,
                retryBase:      retryBase,
                retryMax:       retryMax,
                hook:           opts.Hook,
        }, nil
}

func (p *Pool) ID() string {
        return p.id
}

func (p *Pool) Run(ctx context.Context) error {
        if err := p.q.EnsureGroups(ctx); err != nil {
                return fmt.Errorf("ensure consumer groups: %w", err)
        }
        if err := p.store.UpsertWorker(ctx, model.WorkerInfo{ID: p.id, Hostname: p.hostname, Concurrency: p.concurrency}); err != nil {
                return fmt.Errorf("register worker: %w", err)
        }
        p.met.ActiveWorkers.Inc()
        defer p.met.ActiveWorkers.Dec()

        runCtx, cancel := context.WithCancel(ctx)
        defer cancel()

        var wg sync.WaitGroup
        wg.Add(1)
        go func() {
                defer wg.Done()
                p.heartbeatLoop(runCtx)
        }()

        slots := make(chan struct{}, p.concurrency)
        execWg := &sync.WaitGroup{}
        err := p.readLoop(runCtx, slots, execWg)

        done := make(chan struct{})
        go func() {
                execWg.Wait()
                close(done)
        }()
        select {
        case <-done:
        case <-time.After(30 * time.Second):
                p.logger.Warn("timed out waiting for in-flight jobs")
        }
        cancel()
        wg.Wait()
        return err
}

func (p *Pool) heartbeatLoop(ctx context.Context) {
        ticker := time.NewTicker(p.heartbeat)
        defer ticker.Stop()
        for {
                select {
                case <-ctx.Done():
                        return
                case <-ticker.C:
                        hbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
                        if err := p.store.UpsertWorker(hbCtx, model.WorkerInfo{ID: p.id, Hostname: p.hostname, Concurrency: p.concurrency}); err != nil {
                                p.logger.Error("heartbeat failed", "error", err)
                        }
                        cancel()
                }
        }
}

func (p *Pool) readLoop(ctx context.Context, slots chan struct{}, execWg *sync.WaitGroup) error {
        for {
                select {
                case <-ctx.Done():
                        return ctx.Err()
                default:
                }
                msgs, err := p.q.Read(ctx, p.id, int64(p.concurrency), 2*time.Second)
                if err != nil {
                        if ctx.Err() != nil {
                                return ctx.Err()
                        }
                        p.logger.Error("queue read failed", "error", err)
                        select {
                        case <-ctx.Done():
                                return ctx.Err()
                        case <-time.After(time.Second):
                        }
                        continue
                }
                for _, msg := range msgs {
                        select {
                        case <-ctx.Done():
                                return ctx.Err()
                        case slots <- struct{}{}:
                        }
                        execWg.Add(1)
                        go func(msg queue.Message) {
                                defer execWg.Done()
                                defer func() { <-slots }()
                                p.execute(context.WithoutCancel(ctx), msg)
                        }(msg)
                }
        }
}

func (p *Pool) execute(ctx context.Context, msg queue.Message) {
        started := time.Now()
        job, err := p.store.ClaimRunning(ctx, msg.JobID, p.id)
        if err != nil {
                if errors.Is(err, storage.ErrStatusConflict) || errors.Is(err, storage.ErrNotFound) {
                        _ = p.q.Ack(ctx, msg.Stream, msg.ID)
                        return
                }
                p.logger.Error("claim failed", "job_id", msg.JobID, "error", err)
                return
        }
        p.bus.Publish(events.Event{Kind: events.JobStarted, Job: job})
        p.met.InFlightJobs.Inc()

        outcome := p.runHandler(ctx, job)
        p.met.InFlightJobs.Dec()
        p.met.JobDuration.Observe(time.Since(started).Seconds())

        switch {
        case outcome.panic != nil:
                p.logger.Error("job panicked", "job_id", job.ID, "panic", outcome.panic)
                p.finishDead(ctx, job, fmt.Sprintf("panic: %v", outcome.panic))
                _ = p.q.SendToDLQ(ctx, job.ID, "panic", job.Attempts)
                _ = p.q.Ack(ctx, msg.Stream, msg.ID)
        case outcome.err != nil:
                attempts := job.Attempts
                if attempts > job.MaxRetries || retry.IsPermanent(outcome.err) {
                        reason := "max_retries_exceeded"
                        if retry.IsPermanent(outcome.err) {
                                reason = "permanent_failure"
                        }
                        p.finishDead(ctx, job, outcome.err.Error())
                        _ = p.q.SendToDLQ(ctx, job.ID, reason, attempts)
                        _ = p.q.Ack(ctx, msg.Stream, msg.ID)
                        return
                }
                p.finishRetry(ctx, job, outcome.err)
                _ = p.q.Ack(ctx, msg.Stream, msg.ID)
        default:
                p.finishSucceeded(ctx, job, outcome.result)
                _ = p.q.Ack(ctx, msg.Stream, msg.ID)
        }
}

type handlerOutcome struct {
        result json.RawMessage
        err    error
        panic  error
}

func (p *Pool) runHandler(ctx context.Context, job *model.Job) (outcome handlerOutcome) {
        handler, ok := p.registry.Lookup(job.Type)
        if !ok {
                // A missing handler can never succeed on retry: handlers are
                // registered at pool startup. Dead-letter immediately.
                return handlerOutcome{err: retry.Permanent(fmt.Errorf("no handler registered for job type %q", job.Type))}
        }
        execCtx, cancel := context.WithTimeout(ctx, p.jobTimeout(job))
        defer cancel()
        defer func() {
                if r := recover(); r != nil {
                        if err, isErr := r.(error); isErr {
                                outcome = handlerOutcome{panic: err}
                        } else {
                                outcome = handlerOutcome{panic: fmt.Errorf("%v", r)}
                        }
                }
        }()
        result, err := handler(execCtx, job)
        return handlerOutcome{result: result, err: err}
}

const DefaultJobTimeout = 10 * time.Minute

// jobTimeout resolves the execution deadline for a job: the per-job
// timeout_ms submitted with the job, falling back to the pool default.
func (p *Pool) jobTimeout(job *model.Job) time.Duration {
        if job.TimeoutMS > 0 {
                return time.Duration(job.TimeoutMS) * time.Millisecond
        }
        return p.defaultTimeout
}

func (p *Pool) finishSucceeded(ctx context.Context, job *model.Job, result json.RawMessage) {
        finCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
        defer cancel()
        updated, err := p.store.MarkSucceeded(finCtx, job.ID, result)
        if err != nil {
                p.logger.Error("mark succeeded failed", "job_id", job.ID, "error", err)
                return
        }
        p.met.JobsSucceeded.Inc()
        p.bus.Publish(events.Event{Kind: events.JobSucceeded, Job: updated})
        if p.hook != nil {
                hookCtx, hookCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
                defer hookCancel()
                p.hook(hookCtx, updated)
        }
}

func (p *Pool) finishRetry(ctx context.Context, job *model.Job, cause error) {
        policy := retry.Policy{Base: p.retryBase, Max: p.retryMax, MaxAttempts: job.MaxRetries}
        runAt := time.Now().UTC().Add(policy.NextDelay(job.Attempts))
        finCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
        defer cancel()
        updated, err := p.store.MarkRetry(finCtx, job.ID, runAt, cause.Error())
        if err != nil {
                p.logger.Error("mark retry failed", "job_id", job.ID, "error", err)
                return
        }
        p.met.JobsRetried.Inc()
        p.bus.Publish(events.Event{Kind: events.JobRetrying, Job: updated})
}

func (p *Pool) finishDead(ctx context.Context, job *model.Job, cause string) {
        finCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
        defer cancel()
        updated, err := p.store.MarkDead(finCtx, job.ID, cause)
        if err != nil {
                p.logger.Error("mark dead failed", "job_id", job.ID, "error", err)
                return
        }
        p.met.JobsDead.Inc()
        p.bus.Publish(events.Event{Kind: events.JobDead, Job: updated})
        if p.hook != nil {
                hookCtx, hookCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
                defer hookCancel()
                p.hook(hookCtx, updated)
        }
}
