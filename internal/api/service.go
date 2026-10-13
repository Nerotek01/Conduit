package api

import (
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "log/slog"
        "time"

        "github.com/robfig/cron/v3"

        "github.com/Nerotek01/Conduit/internal/events"
        "github.com/Nerotek01/Conduit/internal/metrics"
        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
        "github.com/Nerotek01/Conduit/internal/workflow"
)

type InvalidInputError struct {
        Err error
}

func (e *InvalidInputError) Error() string { return e.Err.Error() }

func (e *InvalidInputError) Unwrap() error { return e.Err }

func invalid(err error) error {
        return &InvalidInputError{Err: err}
}

type Service struct {
        store            *storage.Store
        q                *queue.RedisQueue
        bus              events.Bus
        met              *metrics.Metrics
        logger           *slog.Logger
        engine           *workflow.Engine
        maxPayloadBytes  int64
        queuePrefix      string
}

func NewService(store *storage.Store, q *queue.RedisQueue, bus events.Bus, met *metrics.Metrics, logger *slog.Logger, maxPayloadBytes int64, queuePrefix string) *Service {
        svc := &Service{
                store:           store,
                q:               q,
                bus:             bus,
                met:             met,
                logger:          logger,
                maxPayloadBytes: maxPayloadBytes,
                queuePrefix:     queuePrefix,
        }
        svc.engine = workflow.NewEngine(store, svc.submitForWorkflow)
        return svc
}

func (s *Service) Engine() *workflow.Engine {
        return s.engine
}

func (s *Service) submitForWorkflow(ctx context.Context, in model.SubmitInput) (*model.Job, error) {
        job, duplicate, err := s.store.CreateJob(ctx, in)
        if err != nil {
                return nil, err
        }
        if duplicate {
                // Idempotency hit: if the node's job is still queued it may
                // never have reached the stream (previous submit died between
                // CreateJob and Enqueue); re-enqueue instead of stalling the
                // node until the coordinator's 30s stale-queued sweep.
                if job.Status == model.StatusQueued && in.RunAt == nil {
                        if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                                return nil, fmt.Errorf("enqueue: %w", err)
                        }
                }
                return job, nil
        }
        if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                return nil, fmt.Errorf("enqueue: %w", err)
        }
        return job, nil
}

type SubmitOptions struct {
        Type           string          `json:"type"`
        Payload        json.RawMessage `json:"payload"`
        Priority       int             `json:"priority"`
        MaxRetries     *int            `json:"max_retries"`
        IdempotencyKey *string         `json:"idempotency_key"`
        DelayMS        int64           `json:"delay_ms"`
        TimeoutMS      int64           `json:"timeout_ms"`
}

func (s *Service) SubmitJob(ctx context.Context, opts SubmitOptions) (*model.Job, bool, error) {
        maxRetries := model.DefaultMaxRetries
        if opts.MaxRetries != nil {
                maxRetries = *opts.MaxRetries
        }
        priority := opts.Priority
        if priority == 0 {
                priority = model.PriorityNormal
        }
        in := model.SubmitInput{
                Type:           opts.Type,
                Payload:        normalizePayload(opts.Payload),
                Priority:       priority,
                MaxRetries:     maxRetries,
                IdempotencyKey: opts.IdempotencyKey,
                TimeoutMS:      opts.TimeoutMS,
        }
        if opts.DelayMS > 0 {
                runAt := time.Now().UTC().Add(time.Duration(opts.DelayMS) * time.Millisecond)
                in.RunAt = &runAt
        }
        if err := in.Validate(s.maxPayloadBytes); err != nil {
                return nil, false, invalid(err)
        }
        job, duplicate, err := s.store.CreateJob(ctx, in)
        if err != nil {
                return nil, false, err
        }
        if duplicate {
                return job, true, nil
        }
        if in.RunAt == nil {
                if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                        return nil, false, fmt.Errorf("enqueue: %w", err)
                }
        }
        s.met.JobsSubmitted.Inc()
        s.bus.Publish(events.Event{Kind: events.JobSubmitted, Job: job})
        return job, false, nil
}

func normalizePayload(p json.RawMessage) json.RawMessage {
        if len(p) == 0 {
                return json.RawMessage(`{}`)
        }
        return p
}

func (s *Service) GetJob(ctx context.Context, id string) (*model.Job, error) {
        return s.store.GetJob(ctx, id)
}

type ListOptions struct {
        Status   string
        Type     string
        Limit    int
        Cursor   *time.Time
        BeforeID string
}

type ListResult struct {
        Jobs         []model.Job `json:"jobs"`
        NextCursor   *time.Time  `json:"next_cursor,omitempty"`
        NextBeforeID string      `json:"next_before_id,omitempty"`
}

func (s *Service) ListJobs(ctx context.Context, opts ListOptions) (*ListResult, error) {
        var status *model.JobStatus
        if opts.Status != "" {
                st := model.JobStatus(opts.Status)
                if !validStatus(st) {
                        return nil, invalid(fmt.Errorf("unknown status %q", opts.Status))
                }
                status = &st
        }
        jobs, more, err := s.store.ListJobs(ctx, storage.ListFilter{
                Status:   status,
                Type:     opts.Type,
                Cursor:   opts.Cursor,
                BeforeID: opts.BeforeID,
                Limit:    opts.Limit,
        })
        if err != nil {
                return nil, err
        }
        res := &ListResult{Jobs: jobs}
        if more && len(jobs) > 0 {
                last := jobs[len(jobs)-1]
                res.NextCursor = &last.CreatedAt
                res.NextBeforeID = last.ID
        }
        return res, nil
}

func validStatus(s model.JobStatus) bool {
        for _, known := range model.AllStatuses {
                if known == s {
                        return true
                }
        }
        return false
}

func (s *Service) CancelJob(ctx context.Context, id string) (*model.Job, error) {
        job, err := s.store.CancelJob(ctx, id)
        if err != nil {
                return nil, err
        }
        if job.Status == model.StatusCancelled {
                s.met.JobsCancelled.Inc()
                s.bus.Publish(events.Event{Kind: events.JobCancelled, Job: job})
        }
        return job, nil
}

func (s *Service) CreateWorkflow(ctx context.Context, def model.WorkflowDefinition) (*model.Workflow, error) {
        if err := def.Validate(); err != nil {
                return nil, invalid(err)
        }
        return s.store.CreateWorkflow(ctx, def)
}

func (s *Service) ListWorkflows(ctx context.Context, limit int) ([]model.Workflow, error) {
        return s.store.ListWorkflows(ctx, limit)
}

func (s *Service) GetWorkflow(ctx context.Context, id string) (*model.Workflow, error) {
        return s.store.GetWorkflow(ctx, id)
}

func (s *Service) RunWorkflow(ctx context.Context, id string) (*model.WorkflowRun, error) {
        return s.engine.StartRun(ctx, id)
}

func (s *Service) ListRuns(ctx context.Context, workflowID string, limit int) ([]model.WorkflowRun, error) {
        return s.store.ListRuns(ctx, workflowID, limit)
}

func (s *Service) QueueSnapshot(ctx context.Context) (map[string]any, error) {
        depths, err := s.q.Depth(ctx)
        if err != nil {
                return nil, err
        }
        counts, err := s.store.CountByStatus(ctx)
        if err != nil {
                return nil, err
        }
        pending, err := s.q.Pending(ctx)
        if err != nil {
                return nil, err
        }
        s.met.RecordQueueDepth(depths)
        out := map[string]any{
                "streams": depths,
                "pending": pending,
                "statuses": counts,
        }
        return out, nil
}

func (s *Service) ListWorkers(ctx context.Context) ([]model.WorkerInfo, error) {
        return s.store.ListWorkers(ctx)
}

func (s *Service) CreateSchedule(ctx context.Context, in model.Schedule, next time.Time) (*model.Schedule, error) {
        in.NextRun = &next
        if !model.JobTypePattern.MatchString(in.JobType) {
                return nil, invalid(fmt.Errorf("invalid job type %q", in.JobType))
        }
        return s.store.CreateSchedule(ctx, in)
}

func (s *Service) ListSchedules(ctx context.Context) ([]model.Schedule, error) {
        return s.store.ListSchedules(ctx)
}

func (s *Service) DeleteSchedule(ctx context.Context, id string) error {
        deleted, err := s.store.DeleteSchedule(ctx, id)
        if err != nil {
                return err
        }
        if !deleted {
                return fmt.Errorf("%w: schedule %s", storage.ErrNotFound, id)
        }
        return nil
}

func (s *Service) Healthy(ctx context.Context) error {
        ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
        defer cancel()
        if err := s.store.Pool().Ping(ctx); err != nil {
                return fmt.Errorf("postgres: %w", err)
        }
        if err := s.q.Ping(ctx); err != nil {
                return fmt.Errorf("redis: %w", err)
        }
        return nil
}

var ErrNotFound = storage.ErrNotFound
var ErrDuplicate = storage.ErrDuplicateKey
var ErrConflict = storage.ErrStatusConflict

func IsNotFound(err error) bool  { return errors.Is(err, storage.ErrNotFound) }
func IsConflict(err error) bool  { return errors.Is(err, storage.ErrStatusConflict) }

func nextScheduleRun(expr, timezone string) (time.Time, error) {
        if expr == "" {
                return time.Time{}, fmt.Errorf("cron expression is required")
        }
        loc := "UTC"
        if timezone != "" {
                loc = timezone
        }
        schedule, err := cron.ParseStandard(expr)
        if err != nil {
                return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
        }
        loaded, err := time.LoadLocation(loc)
        if err != nil {
                return time.Time{}, fmt.Errorf("unknown timezone %q", loc)
        }
        return schedule.Next(time.Now().In(loaded)).UTC(), nil
}
