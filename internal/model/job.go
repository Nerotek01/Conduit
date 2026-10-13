package model

import (
        "encoding/json"
        "fmt"
        "regexp"
        "time"
)

type JobStatus string

const (
        StatusQueued    JobStatus = "queued"
        StatusScheduled JobStatus = "scheduled"
        StatusRetrying  JobStatus = "retrying"
        StatusRunning   JobStatus = "running"
        StatusSucceeded JobStatus = "succeeded"
        StatusDead      JobStatus = "dead"
        StatusCancelled JobStatus = "cancelled"
)

func (s JobStatus) Terminal() bool {
        return s == StatusSucceeded || s == StatusDead || s == StatusCancelled
}

func (s JobStatus) Resolved() bool {
        return s.Terminal() || s == StatusCancelled
}

var AllStatuses = []JobStatus{StatusQueued, StatusScheduled, StatusRetrying, StatusRunning, StatusSucceeded, StatusDead, StatusCancelled}

const (
        PriorityHigh   = 1
        PriorityNormal = 2
        PriorityLow    = 3
        PriorityBulk   = 4
)

const (
        MinPriority = PriorityHigh
        MaxPriority = PriorityBulk
)

const (
        DefaultMaxRetries = 3
        MaxMaxRetries     = 25
)

const (
        MinTimeoutMS = int64(1000)         // 1s
        MaxTimeoutMS = int64(86400000)     // 24h
)

var JobTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

var IdempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

type Job struct {
        ID             string          `json:"id"`
        Type           string          `json:"type"`
        Payload        json.RawMessage `json:"payload"`
        Status         JobStatus       `json:"status"`
        Priority       int             `json:"priority"`
        Attempts       int             `json:"attempts"`
        MaxRetries     int             `json:"max_retries"`
        IdempotencyKey *string         `json:"idempotency_key,omitempty"`
        TimeoutMS       int64           `json:"timeout_ms,omitempty"`
        ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
        StartedAt      *time.Time      `json:"started_at,omitempty"`
        FinishedAt     *time.Time      `json:"finished_at,omitempty"`
        LastError      *string         `json:"last_error,omitempty"`
        WorkerID       *string         `json:"worker_id,omitempty"`
        Result         json.RawMessage `json:"result,omitempty"`
        RunID          *string         `json:"run_id,omitempty"`
        NodeID         *string         `json:"node_id,omitempty"`
        CreatedAt      time.Time       `json:"created_at"`
        UpdatedAt      time.Time       `json:"updated_at"`
}

type SubmitInput struct {
        Type           string
        Payload        json.RawMessage
        Priority       int
        MaxRetries     int
        IdempotencyKey *string
        RunAt          *time.Time
        RunID          *string
        NodeID         *string
        TimeoutMS      int64
}

func (in SubmitInput) Validate(maxPayloadBytes int64) error {
        if !JobTypePattern.MatchString(in.Type) {
                return fmt.Errorf("invalid job type %q", in.Type)
        }
        if in.Priority < MinPriority || in.Priority > MaxPriority {
                return fmt.Errorf("priority must be between %d and %d", MinPriority, MaxPriority)
        }
        if in.MaxRetries < 0 || in.MaxRetries > MaxMaxRetries {
                return fmt.Errorf("max_retries must be between 0 and %d", MaxMaxRetries)
        }
        if int64(len(in.Payload)) > maxPayloadBytes {
                return fmt.Errorf("payload exceeds %d bytes", maxPayloadBytes)
        }
        if in.IdempotencyKey != nil && !IdempotencyKeyPattern.MatchString(*in.IdempotencyKey) {
                return fmt.Errorf("invalid idempotency key")
        }
        if in.TimeoutMS != 0 && (in.TimeoutMS < MinTimeoutMS || in.TimeoutMS > MaxTimeoutMS) {
                return fmt.Errorf("timeout_ms must be between %d and %d ms (or omitted)", MinTimeoutMS, MaxTimeoutMS)
        }
        return nil
}
