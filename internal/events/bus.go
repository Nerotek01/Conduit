package events

import (
	"encoding/json"

	"github.com/Nerotek01/Conduit/internal/model"
)

const (
	JobSubmitted = "job.submitted"
	JobStarted   = "job.started"
	JobSucceeded = "job.succeeded"
	JobRetrying  = "job.retrying"
	JobDead      = "job.dead"
	JobCancelled = "job.cancelled"
	QueueDepth   = "queue.depth"
)

type Event struct {
	Kind string     `json:"event"`
	Job  *model.Job `json:"job,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

type Bus interface {
	Publish(Event)
}

type NoopBus struct{}

func (NoopBus) Publish(Event) {}

type ChanBus struct {
	ch     chan Event
	dropped func()
}

func NewChanBus(size int, onDrop func()) *ChanBus {
	if size < 1 {
		size = 1
	}
	return &ChanBus{ch: make(chan Event, size), dropped: onDrop}
}

func (b *ChanBus) Publish(e Event) {
	select {
	case b.ch <- e:
	default:
		select {
		case <-b.ch:
		default:
		}
		if b.dropped != nil {
			b.dropped()
		}
		select {
		case b.ch <- e:
		default:
		}
	}
}

func (b *ChanBus) C() <-chan Event {
	return b.ch
}
