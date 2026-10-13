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

type Engine struct {
        store    *storage.Store
        submit   Submitter
}

func NewEngine(store *storage.Store, submit Submitter) *Engine {
        return &Engine{store: store, submit: submit}
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
                if run.Status.Terminal() {
                        return nil
                }
                wf, err := e.store.GetWorkflow(ctx, run.WorkflowID)
                if err != nil {
                        return err
                }
                state, ok := run.NodeStates[*job.NodeID]
                if !ok || state.Status.Resolved() {
                        return nil
                }
                state.JobID = job.ID
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
                        return nil
                }
                run.NodeStates[*job.NodeID] = state
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
