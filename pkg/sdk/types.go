package sdk

import (
	"github.com/Nerotek01/Conduit/internal/model"
	"github.com/Nerotek01/Conduit/internal/retry"
)

type Job = model.Job
type JobStatus = model.JobStatus
type WorkflowDefinition = model.WorkflowDefinition
type Workflow = model.Workflow
type WorkflowRun = model.WorkflowRun
type Schedule = model.Schedule
type WorkerInfo = model.WorkerInfo
type NodeSpec = model.NodeSpec
type Condition = model.Condition

const (
	PriorityHigh   = model.PriorityHigh
	PriorityNormal = model.PriorityNormal
	PriorityLow    = model.PriorityLow
	PriorityBulk   = model.PriorityBulk

	StatusQueued    = model.StatusQueued
	StatusScheduled = model.StatusScheduled
	StatusRetrying  = model.StatusRetrying
	StatusRunning   = model.StatusRunning
	StatusSucceeded = model.StatusSucceeded
	StatusDead      = model.StatusDead
	StatusCancelled = model.StatusCancelled

	DefaultMaxRetries = model.DefaultMaxRetries
)

func Permanent(err error) error { return retry.Permanent(err) }

func IsPermanent(err error) bool { return retry.IsPermanent(err) }
