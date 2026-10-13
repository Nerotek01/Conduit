package workflow

import (
        "bytes"
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "time"

        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/storage"
)

type Submitter func(ctx context.Context, in model.SubmitInput) (*model.Job, error)

// Canceler best-effort cancels a node's backing job. It mirrors storage
// semantics: when the job can no longer be cancelled (it is running or
// already terminal) it returns the job with its current status alongside a
// status-conflict error.
type Canceler func(ctx context.Context, jobID string) (*model.Job, error)

type Engine struct {
        store  *storage.Store
        submit Submitter
        cancel Canceler
}

func NewEngine(store *storage.Store, submit Submitter, cancel Canceler) *Engine {
        return &Engine{store: store, submit: submit, cancel: cancel}
}

func (e *Engine) StartRun(ctx context.Context, workflowID string) (*model.WorkflowRun, error) {
        wf, err := e.store.GetWorkflow(ctx, workflowID)
        if err != nil {
                return nil, err
        }
        states := initialStates(&wf.Definition)
        run, err := e.store.CreateRun(ctx, workflowID, states)
        if err != nil {
                return nil, err
        }
        if err := e.advance(ctx, wf, run); err != nil {
                return run, err
        }
        return run, nil
}

func initialStates(def *model.WorkflowDefinition) map[string]model.NodeState {
        states := make(map[string]model.NodeState, len(def.Nodes))
        for _, n := range def.Nodes {
                states[n.ID] = model.NodeState{Status: model.NodePending}
        }
        return states
}

func (e *Engine) OnJobTerminal(ctx context.Context, job *model.Job) error {
        if job.RunID == nil || job.NodeID == nil {
                return nil
        }
        var lastErr error
        for attempt := 0; attempt < 5; attempt++ {
                run, err := e.store.GetRun(ctx, *job.RunID)
                if err != nil {
                        if errors.Is(err, storage.ErrNotFound) {
                                return nil
                        }
                        return err
                }
                state, ok := run.NodeStates[*job.NodeID]
                if !ok || state.Status.Resolved() {
                        return nil
                }
                state.JobID = job.ID
                outcome, resolved := terminalNodeState(job)
                if !resolved {
                        return nil
                }
                state.Status = outcome.status
                state.Output = outcome.output
                state.Error = outcome.errText
                run.NodeStates[*job.NodeID] = state
                if run.Status.Terminal() {
                        // The run already finished (a sibling failed while this
                        // job was in flight): record the late outcome so the
                        // DAG reflects what actually happened, but do not
                        // re-advance a terminal run.
                        if err := e.store.RecordRunNodeStates(ctx, run); err != nil {
                                if errors.Is(err, storage.ErrStatusConflict) {
                                        lastErr = err
                                        select {
                                        case <-ctx.Done():
                                                return ctx.Err()
                                        case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
                                        }
                                        continue
                                }
                                return err
                        }
                        return nil
                }
                wf, err := e.store.GetWorkflow(ctx, run.WorkflowID)
                if err != nil {
                        return err
                }
                if err := e.advance(ctx, wf, run); err != nil {
                        if errors.Is(err, storage.ErrStatusConflict) {
                                lastErr = err
                                select {
                                case <-ctx.Done():
                                        return ctx.Err()
                                case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
                                }
                                continue
                        }
                        return err
                }
                return nil
        }
        return fmt.Errorf("run %s concurrent update retries exhausted: %w", *job.RunID, lastErr)
}

// nodeOutcome is the workflow-level resolution of a job's terminal status.
type nodeOutcome struct {
        status  model.NodeStateStatus
        output  json.RawMessage
        errText string
}

func terminalNodeState(job *model.Job) (nodeOutcome, bool) {
        switch job.Status {
        case model.StatusSucceeded:
                return nodeOutcome{status: model.NodeSucceeded, output: job.Result}, true
        case model.StatusDead:
                return nodeOutcome{status: model.NodeFailed, errText: deref(job.LastError)}, true
        case model.StatusCancelled:
                return nodeOutcome{status: model.NodeSkipped}, true
        }
        return nodeOutcome{}, false
}

func (e *Engine) RecoverRuns(ctx context.Context) (int, error) {
        runs, err := e.store.ListUnfinishedRuns(ctx, 100)
        if err != nil {
                return 0, err
        }
        recovered := 0
        for i := range runs {
                wf, err := e.store.GetWorkflow(ctx, runs[i].WorkflowID)
                if err != nil {
                        continue
                }
                before := summarize(&runs[i])
                if e.reconcileSubmitted(ctx, &runs[i]) {
                        if err := e.store.UpdateRun(ctx, &runs[i]); err != nil {
                                continue
                        }
                }
                if err := e.advance(ctx, wf, &runs[i]); err != nil {
                        continue
                }
                if summarize(&runs[i]) != before {
                        recovered++
                }
        }
        return recovered, nil
}

// reconcileSubmitted transitions nodes stuck in the "submitted" state to a
// resolved state based on the actual terminal status of their backing job.
// It covers the crash window between a job reaching a terminal state and
// OnJobTerminal firing, which would otherwise leave the run stalled forever.
func (e *Engine) reconcileSubmitted(ctx context.Context, run *model.WorkflowRun) bool {
        changed := false
        for id, state := range run.NodeStates {
                if state.Status != model.NodeSubmitted || state.JobID == "" {
                        continue
                }
                job, err := e.store.GetJob(ctx, state.JobID)
                if err != nil {
                        continue
                }
                switch job.Status {
                case model.StatusSucceeded:
                        state.Status = model.NodeSucceeded
                        state.Output = job.Result
                case model.StatusDead:
                        state.Status = model.NodeFailed
                        state.Error = deref(job.LastError)
                case model.StatusCancelled:
                        state.Status = model.NodeSkipped
                default:
                        continue
                }
                run.NodeStates[id] = state
                changed = true
        }
        return changed
}

func summarize(run *model.WorkflowRun) string {
        body, _ := json.Marshal(run.NodeStates)
        return string(run.Status) + ":" + string(body)
}

func deref(s *string) string {
        if s == nil {
                return ""
        }
        return *s
}

func strPtr(s string) *string {
        return &s
}

func (e *Engine) advance(ctx context.Context, wf *model.Workflow, run *model.WorkflowRun) error {
        nodeMap := wf.Definition.NodeMap()
        progressed := true
        failed := false
        submittedNow := make(map[string]bool)
        for progressed {
                progressed = false
                for _, node := range wf.Definition.Nodes {
                        state := run.NodeStates[node.ID]
                        if state.Status != model.NodePending {
                                continue
                        }
                        ready, err := nodeReady(node, nodeMap, run.NodeStates)
                        if err != nil {
                                return err
                        }
                        if !ready.decided {
                                continue
                        }
                        if ready.skip {
                                state.Status = model.NodeSkipped
                                run.NodeStates[node.ID] = state
                                progressed = true
                                continue
                        }
                        in := model.SubmitInput{
                                Type:           node.JobType,
                                Payload:        node.Payload,
                                Priority:       model.PriorityNormal,
                                MaxRetries:     model.DefaultMaxRetries,
                                RunID:          &run.ID,
                                NodeID:         &node.ID,
                                IdempotencyKey: strPtr(fmt.Sprintf("wf:%s:%s", run.ID, node.ID)),
                        }
                        job, err := e.submit(ctx, in)
                        if err != nil {
                                return fmt.Errorf("submit node %s: %w", node.ID, err)
                        }
                        state.Status = model.NodeSubmitted
                        state.JobID = job.ID
                        run.NodeStates[node.ID] = state
                        submittedNow[node.ID] = true
                        progressed = true
                }
        }
        for _, node := range wf.Definition.Nodes {
                if run.NodeStates[node.ID].Status == model.NodeFailed {
                        failed = true
                }
        }
        if failed {
                run.Status = model.RunFailed
                now := nowUTC()
                run.FinishedAt = &now
                cancelInFlight(ctx, e.cancel, wf, run, submittedNow)
        } else if allResolved(wf.Definition, run.NodeStates) {
                run.Status = model.RunSucceeded
                now := nowUTC()
                run.FinishedAt = &now
        } else {
                run.Status = model.RunRunning
        }
        return e.store.UpdateRun(ctx, run)
}

type readiness struct {
        decided bool
        skip    bool
}

func nodeReady(node model.NodeSpec, nodeMap map[string]model.NodeSpec, states map[string]model.NodeState) (readiness, error) {
        for _, dep := range node.DepsUnique() {
                depState, ok := states[dep]
                if !ok {
                        return readiness{}, fmt.Errorf("missing state for dependency %s", dep)
                }
                switch depState.Status {
                case model.NodeSucceeded, model.NodeSkipped:
                        continue
                case model.NodeFailed:
                        if node.When != nil && node.When.Node == dep && node.When.OnFailure {
                                continue
                        }
                        return readiness{decided: true, skip: true}, nil
                default:
                        return readiness{}, nil
                }
        }
        if node.When != nil {
                return evalCondition(node.When, states)
        }
        return readiness{decided: true, skip: false}, nil
}

func evalCondition(cond *model.Condition, states map[string]model.NodeState) (readiness, error) {
        state, ok := states[cond.Node]
        if !ok {
                return readiness{}, fmt.Errorf("condition references unknown node %s", cond.Node)
        }
        if state.Status == model.NodeFailed {
                if cond.OnFailure {
                        return readiness{decided: true, skip: false}, nil
                }
                return readiness{decided: true, skip: true}, nil
        }
        if state.Status != model.NodeSucceeded {
                return readiness{}, nil
        }
        if cond.Equals != nil {
                if jsonEqual(state.Output, cond.Equals) {
                        return readiness{decided: true, skip: false}, nil
                }
                return readiness{decided: true, skip: true}, nil
        }
        if cond.NotEquals != nil {
                if !jsonEqual(state.Output, cond.NotEquals) {
                        return readiness{decided: true, skip: false}, nil
                }
                return readiness{decided: true, skip: true}, nil
        }
        if cond.OnFailure {
                return readiness{decided: true, skip: true}, nil
        }
        return readiness{decided: true, skip: false}, nil
}

func jsonEqual(a, b json.RawMessage) bool {
        var va, vb any
        if err := json.Unmarshal(a, &va); err != nil {
                return false
        }
        if err := json.Unmarshal(b, &vb); err != nil {
                return false
        }
        return bytes.Equal(mustJSON(va), mustJSON(vb))
}

func mustJSON(v any) []byte {
        body, err := json.Marshal(v)
        if err != nil {
                return nil
        }
        return body
}

func allResolved(def model.WorkflowDefinition, states map[string]model.NodeState) bool {
        for _, node := range def.Nodes {
                if !states[node.ID].Status.Resolved() {
                        return false
                }
        }
        return true
}

// cancelInFlight stops the in-flight node jobs of a failed run: their
// results can no longer affect the outcome. Nodes submitted during the same
// advance pass (failure-compensation nodes) are excluded: they exist
// precisely because of the failure. Jobs still queued or awaiting retry are
// cancelled outright; jobs that already reached a terminal state record
// their real outcome; running jobs cannot be interrupted and are left
// submitted so OnJobTerminal records their late outcome when they finish.
// Without this, a failed run's in-flight jobs burn worker capacity for
// nothing and the run's node states stay "submitted" forever.
func cancelInFlight(ctx context.Context, cancel Canceler, wf *model.Workflow, run *model.WorkflowRun, submittedNow map[string]bool) {
        for _, node := range wf.Definition.Nodes {
                if submittedNow[node.ID] {
                        continue
                }
                state := run.NodeStates[node.ID]
                if state.Status != model.NodeSubmitted {
                        continue
                }
                if cancel == nil {
                        state.Status = model.NodeSkipped
                        run.NodeStates[node.ID] = state
                        continue
                }
                updated, err := cancel(ctx, state.JobID)
                switch {
                case err == nil:
                        state.Status = model.NodeSkipped
                case errors.Is(err, storage.ErrStatusConflict) && updated != nil:
                        switch updated.Status {
                        case model.StatusRunning:
                                continue
                        case model.StatusSucceeded:
                                state.Status = model.NodeSucceeded
                                state.Output = updated.Result
                        case model.StatusDead:
                                state.Status = model.NodeFailed
                                state.Error = deref(updated.LastError)
                        default:
                                state.Status = model.NodeSkipped
                        }
                default:
                        // Storage error: resolve the node anyway so the run
                        // finishes; the backing job is cleaned up by the
                        // coordinator's stale-queued sweep if still queued.
                        state.Status = model.NodeSkipped
                }
                run.NodeStates[node.ID] = state
        }
}
