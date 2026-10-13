package workflow

import (
        "context"
        "fmt"

        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
)

type SubmitterFunc func(ctx context.Context, in model.SubmitInput) (*model.Job, error)

type StoreSubmitter struct {
        store *storage.Store
        q     *queue.RedisQueue
}

func NewSubmitter(store *storage.Store, q *queue.RedisQueue) *StoreSubmitter {
        return &StoreSubmitter{store: store, q: q}
}

func (s *StoreSubmitter) Submit(ctx context.Context, in model.SubmitInput) (*model.Job, error) {
        job, duplicate, err := s.store.CreateJob(ctx, in)
        if err != nil {
                return nil, err
        }
        if duplicate {
                // The node's job already exists (idempotency hit). If it is
                // still queued it may never have reached the stream: the
                // previous submit could have died between CreateJob and
                // Enqueue, which would otherwise stall the node for the
                // coordinator's 30s stale-queued sweep. Re-enqueue now; a
                // duplicate stream entry is harmless because ClaimRunning
                // conflicts on non-queued statuses get acked by the worker.
                if job.Status == model.StatusQueued && in.RunAt == nil {
                        if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                                return nil, fmt.Errorf("enqueue workflow job: %w", err)
                        }
                }
                return job, nil
        }
        if in.RunAt == nil {
                if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
                        return nil, fmt.Errorf("enqueue workflow job: %w", err)
                }
        }
        return job, nil
}
