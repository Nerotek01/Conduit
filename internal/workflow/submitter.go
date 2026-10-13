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
		return job, nil
	}
	if in.RunAt == nil {
		if err := s.q.Enqueue(ctx, job.ID, job.Priority); err != nil {
			return nil, fmt.Errorf("enqueue workflow job: %w", err)
		}
	}
	return job, nil
}
