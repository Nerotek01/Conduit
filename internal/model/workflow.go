package model

import (
        "encoding/json"
        "fmt"
        "time"
)

type WorkflowDefinition struct {
        Name  string    `json:"name"`
        Nodes []NodeSpec `json:"nodes"`
}

type NodeSpec struct {
        ID        string          `json:"id"`
        JobType   string          `json:"job_type"`
        Payload   json.RawMessage `json:"payload,omitempty"`
        DependsOn []string        `json:"depends_on,omitempty"`
        When      *Condition      `json:"when,omitempty"`
}

type Condition struct {
        Node      string          `json:"node"`
        Equals    json.RawMessage `json:"equals,omitempty"`
        NotEquals json.RawMessage `json:"not_equals,omitempty"`
        OnFailure bool            `json:"on_failure,omitempty"`
}

type Workflow struct {
        ID         string             `json:"id"`
        Name       string             `json:"name"`
        Definition WorkflowDefinition `json:"definition"`
        CreatedAt  time.Time          `json:"created_at"`
}

type RunStatus string

const (
        RunPending   RunStatus = "pending"
        RunRunning   RunStatus = "running"
        RunSucceeded RunStatus = "succeeded"
        RunFailed    RunStatus = "failed"
        RunCancelled RunStatus = "cancelled"
)

func (s RunStatus) Terminal() bool {
        return s == RunSucceeded || s == RunFailed || s == RunCancelled
}

type NodeStateStatus string

const (
        NodePending   NodeStateStatus = "pending"
        NodeSubmitted NodeStateStatus = "submitted"
        NodeRunning   NodeStateStatus = "running"
        NodeSucceeded  NodeStateStatus = "succeeded"
        NodeFailed    NodeStateStatus = "failed"
        NodeSkipped   NodeStateStatus = "skipped"
)

func (s NodeStateStatus) Resolved() bool {
        return s == NodeSucceeded || s == NodeFailed || s == NodeSkipped
}

type NodeState struct {
        Status NodeStateStatus `json:"status"`
        JobID  string          `json:"job_id,omitempty"`
        Output json.RawMessage `json:"output,omitempty"`
        Error  string          `json:"error,omitempty"`
}

type WorkflowRun struct {
        ID         string                `json:"id"`
        WorkflowID string                `json:"workflow_id"`
        Status     RunStatus             `json:"status"`
        NodeStates map[string]NodeState  `json:"node_states"`
        Version    int                   `json:"-"`
        StartedAt  time.Time             `json:"started_at"`
        FinishedAt *time.Time            `json:"finished_at,omitempty"`
}

type Schedule struct {
        ID         string          `json:"id"`
        JobType    string          `json:"job_type"`
        Payload    json.RawMessage `json:"payload"`
        Cron       string          `json:"cron"`
        Timezone   string          `json:"timezone,omitempty"`
        Priority   int             `json:"priority"`
        MaxRetries int             `json:"max_retries"`
        NextRun    *time.Time      `json:"next_run,omitempty"`
        Active     bool            `json:"active"`
        CreatedAt  time.Time       `json:"created_at"`
        // DueAt carries the pre-claim next_run returned by ClaimDueSchedules.
        // It identifies the exact occurrence being fired so the idempotency
        // key of the spawned job is deterministic across retries and crashes.
        DueAt time.Time `json:"-"`
}

type WorkerInfo struct {
        ID             string    `json:"id"`
        Hostname       string    `json:"hostname"`
        Status         string    `json:"status"`
        LastHeartbeat  time.Time `json:"last_heartbeat"`
        Concurrency    int       `json:"concurrency"`
        StartedAt      time.Time `json:"started_at"`
}

func (d *WorkflowDefinition) NodeMap() map[string]NodeSpec {
        m := make(map[string]NodeSpec, len(d.Nodes))
        for _, n := range d.Nodes {
                m[n.ID] = n
        }
        return m
}

const MaxNodePayloadBytes = 1 << 20

func (d *WorkflowDefinition) Validate() error {
        if d.Name == "" || len(d.Name) > 128 {
                return fmt.Errorf("workflow name must be 1-128 characters")
        }
        if len(d.Nodes) == 0 || len(d.Nodes) > 256 {
                return fmt.Errorf("workflow must contain 1-256 nodes")
        }
        // Pass 1: collect every node id first so dependency and condition
        // references may point at nodes declared later in the definition.
        // A single pass would reject valid DAGs whose nodes are listed out of
        // topological order with a misleading "unknown node" error.
        seen := make(map[string]bool, len(d.Nodes))
        for _, n := range d.Nodes {
                if n.ID == "" || len(n.ID) > 64 {
                        return fmt.Errorf("node id must be 1-64 characters")
                }
                if seen[n.ID] {
                        return fmt.Errorf("duplicate node id %q", n.ID)
                }
                seen[n.ID] = true
        }
        for _, n := range d.Nodes {
                if !JobTypePattern.MatchString(n.JobType) {
                        return fmt.Errorf("node %q has invalid job type %q", n.ID, n.JobType)
                }
                if int64(len(n.Payload)) > MaxNodePayloadBytes {
                        return fmt.Errorf("node %q payload exceeds limit", n.ID)
                }
                for _, dep := range n.DependsOn {
                        if dep == n.ID {
                                return fmt.Errorf("node %q depends on itself", n.ID)
                        }
                        if !seen[dep] {
                                return fmt.Errorf("node %q depends on unknown node %q", n.ID, dep)
                        }
                }
                if n.When != nil {
                        if n.When.Node == "" {
                                return fmt.Errorf("node %q condition has empty node reference", n.ID)
                        }
                        if !seen[n.When.Node] {
                                return fmt.Errorf("node %q condition references unknown node %q", n.ID, n.When.Node)
                        }
                        if n.When.Equals != nil && n.When.NotEquals != nil {
                                return fmt.Errorf("node %q condition cannot set both equals and not_equals", n.ID)
                        }
                }
        }
        return detectCycle(d)
}

func detectCycle(d *WorkflowDefinition) error {
        deps := make(map[string][]string, len(d.Nodes))
        degree := make(map[string]int, len(d.Nodes))
        for _, n := range d.Nodes {
                degree[n.ID] = len(n.DepsUnique())
                for _, dep := range n.DepsUnique() {
                        deps[dep] = append(deps[dep], n.ID)
                }
        }
        queue := make([]string, 0, len(d.Nodes))
        for id, deg := range degree {
                if deg == 0 {
                        queue = append(queue, id)
                }
        }
        visited := 0
        for len(queue) > 0 {
                cur := queue[len(queue)-1]
                queue = queue[:len(queue)-1]
                visited++
                for _, next := range deps[cur] {
                        degree[next]--
                        if degree[next] == 0 {
                                queue = append(queue, next)
                        }
                }
        }
        if visited != len(d.Nodes) {
                return fmt.Errorf("workflow definition contains a cycle")
        }
        return nil
}

func (n NodeSpec) DepsUnique() []string {
        seen := make(map[string]bool, len(n.DependsOn))
        out := make([]string, 0, len(n.DependsOn))
        for _, d := range n.DependsOn {
                if !seen[d] {
                        seen[d] = true
                        out = append(out, d)
                }
        }
        return out
}
