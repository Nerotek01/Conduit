package storage

import (
        "context"
        "encoding/json"
        "errors"
        "fmt"

        "github.com/jackc/pgx/v5"

        "github.com/Nerotek01/Conduit/internal/model"
)

func (s *Store) CreateWorkflow(ctx context.Context, def model.WorkflowDefinition) (*model.Workflow, error) {
        if err := def.Validate(); err != nil {
                return nil, fmt.Errorf("validate workflow: %w", err)
        }
        body, err := json.Marshal(def)
        if err != nil {
                return nil, fmt.Errorf("marshal workflow: %w", err)
        }
        row := s.pool.QueryRow(ctx, `
INSERT INTO workflows (name, definition) VALUES ($1, $2)
RETURNING id, name, definition, created_at`, def.Name, body)
        var wf model.Workflow
        if err := row.Scan(&wf.ID, &wf.Name, &wf.Definition, &wf.CreatedAt); err != nil {
                return nil, fmt.Errorf("insert workflow: %w", err)
        }
        return &wf, nil
}

func (s *Store) GetWorkflow(ctx context.Context, id string) (*model.Workflow, error) {
        if err := validUUID(id); err != nil {
                return nil, err
        }
        row := s.pool.QueryRow(ctx, `SELECT id, name, definition, created_at FROM workflows WHERE id = $1`, id)
        var wf model.Workflow
        if err := row.Scan(&wf.ID, &wf.Name, &wf.Definition, &wf.CreatedAt); err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: workflow %s", ErrNotFound, id)
                }
                return nil, fmt.Errorf("get workflow: %w", err)
        }
        return &wf, nil
}

func (s *Store) CreateRun(ctx context.Context, workflowID string, states map[string]model.NodeState) (*model.WorkflowRun, error) {
        if err := validUUID(workflowID); err != nil {
                return nil, err
        }
        if _, err := s.GetWorkflow(ctx, workflowID); err != nil {
                return nil, err
        }
        body, err := json.Marshal(states)
        if err != nil {
                return nil, fmt.Errorf("marshal node states: %w", err)
        }
        row := s.pool.QueryRow(ctx, `
INSERT INTO workflow_runs (workflow_id, status, node_states)
VALUES ($1, 'running', $2)
RETURNING id, workflow_id, status, node_states, version, started_at, finished_at`,
                workflowID, body)
        return scanRun(row)
}

func scanRun(row pgx.Row) (*model.WorkflowRun, error) {
        var r model.WorkflowRun
        var states []byte
        if err := row.Scan(&r.ID, &r.WorkflowID, &r.Status, &states, &r.Version, &r.StartedAt, &r.FinishedAt); err != nil {
                if errors.Is(err, pgx.ErrNoRows) {
                        return nil, fmt.Errorf("%w: run", ErrNotFound)
                }
                return nil, err
        }
        if len(states) > 0 {
                if err := json.Unmarshal(states, &r.NodeStates); err != nil {
                        return nil, fmt.Errorf("unmarshal node states: %w", err)
                }
        }
        if r.NodeStates == nil {
                r.NodeStates = map[string]model.NodeState{}
        }
        return &r, nil
}

func (s *Store) UpdateRun(ctx context.Context, run *model.WorkflowRun) error {
        body, err := json.Marshal(run.NodeStates)
        if err != nil {
                return fmt.Errorf("marshal node states: %w", err)
        }
        tag, err := s.pool.Exec(ctx, `
UPDATE workflow_runs SET status = $2, node_states = $3, finished_at = $4, version = version + 1
WHERE id = $1 AND version = $5 AND status IN ('pending','running')`,
                run.ID, string(run.Status), body, run.FinishedAt, run.Version)
        if err != nil {
                return fmt.Errorf("update run: %w", err)
        }
        if tag.RowsAffected() == 0 {
                return fmt.Errorf("%w: run %s modified concurrently or terminal", ErrStatusConflict, run.ID)
        }
        run.Version++
        return nil
}

func (s *Store) GetRun(ctx context.Context, id string) (*model.WorkflowRun, error) {
        if err := validUUID(id); err != nil {
                return nil, err
        }
        row := s.pool.QueryRow(ctx, `
SELECT id, workflow_id, status, node_states, version, started_at, finished_at
FROM workflow_runs WHERE id = $1`, id)
        return scanRun(row)
}

func (s *Store) ListRuns(ctx context.Context, workflowID string, limit int) ([]model.WorkflowRun, error) {
        if err := validUUID(workflowID); err != nil {
                return nil, err
        }
        if limit < 1 || limit > 100 {
                limit = 20
        }
        rows, err := s.pool.Query(ctx, `
SELECT id, workflow_id, status, node_states, version, started_at, finished_at
FROM workflow_runs WHERE workflow_id = $1
ORDER BY started_at DESC LIMIT $2`, workflowID, limit)
        if err != nil {
                return nil, fmt.Errorf("list runs: %w", err)
        }
        defer rows.Close()
        runs := make([]model.WorkflowRun, 0, limit)
        for rows.Next() {
                r := model.WorkflowRun{}
                var states []byte
                if err := rows.Scan(&r.ID, &r.WorkflowID, &r.Status, &states, &r.Version, &r.StartedAt, &r.FinishedAt); err != nil {
                        return nil, fmt.Errorf("scan run: %w", err)
                }
                if len(states) > 0 {
                        if err := json.Unmarshal(states, &r.NodeStates); err != nil {
                                return nil, fmt.Errorf("unmarshal node states: %w", err)
                        }
                }
                runs = append(runs, r)
        }
        return runs, rows.Err()
}

// RecordRunNodeStates persists a late node outcome on a run that may already
// be terminal. Status and finished_at are immutable here (UpdateRun refuses
// terminal runs by design); only the node map and the CAS version advance so
// concurrent late outcomes still serialize.
func (s *Store) RecordRunNodeStates(ctx context.Context, run *model.WorkflowRun) error {
        body, err := json.Marshal(run.NodeStates)
        if err != nil {
                return fmt.Errorf("marshal node states: %w", err)
        }
        tag, err := s.pool.Exec(ctx, `
UPDATE workflow_runs SET node_states = $3, version = version + 1
WHERE id = $1 AND version = $2`,
                run.ID, run.Version, body)
        if err != nil {
                return fmt.Errorf("record run node states: %w", err)
        }
        if tag.RowsAffected() == 0 {
                return fmt.Errorf("%w: run %s modified concurrently", ErrStatusConflict, run.ID)
        }
        run.Version++
        return nil
}

func (s *Store) ListUnfinishedRuns(ctx context.Context, limit int) ([]model.WorkflowRun, error) {
        if limit < 1 || limit > 500 {
                limit = 100
        }
        rows, err := s.pool.Query(ctx, `
SELECT id, workflow_id, status, node_states, version, started_at, finished_at
FROM workflow_runs WHERE status IN ('pending','running')
ORDER BY started_at LIMIT $1`, limit)
        if err != nil {
                return nil, fmt.Errorf("list unfinished runs: %w", err)
        }
        defer rows.Close()
        runs := make([]model.WorkflowRun, 0, limit)
        for rows.Next() {
                r := model.WorkflowRun{}
                var states []byte
                if err := rows.Scan(&r.ID, &r.WorkflowID, &r.Status, &states, &r.Version, &r.StartedAt, &r.FinishedAt); err != nil {
                        return nil, fmt.Errorf("scan run: %w", err)
                }
                if len(states) > 0 {
                        if err := json.Unmarshal(states, &r.NodeStates); err != nil {
                                return nil, fmt.Errorf("unmarshal node states: %w", err)
                        }
                }
                runs = append(runs, r)
        }
        return runs, rows.Err()
}

func (s *Store) ListWorkflows(ctx context.Context, limit int) ([]model.Workflow, error) {
        if limit < 1 || limit > 200 {
                limit = 50
        }
        rows, err := s.pool.Query(ctx, `SELECT id, name, definition, created_at FROM workflows ORDER BY created_at DESC LIMIT $1`, limit)
        if err != nil {
                return nil, fmt.Errorf("list workflows: %w", err)
        }
        defer rows.Close()
        out := make([]model.Workflow, 0, 8)
        for rows.Next() {
                var wf model.Workflow
                if err := rows.Scan(&wf.ID, &wf.Name, &wf.Definition, &wf.CreatedAt); err != nil {
                        return nil, fmt.Errorf("scan workflow: %w", err)
                }
                out = append(out, wf)
        }
        return out, rows.Err()
}
