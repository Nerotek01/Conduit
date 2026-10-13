package sdk

import (
        "bytes"
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "io"
        "net/http"
        "net/url"
        "time"

        "github.com/Nerotek01/Conduit/internal/model"
)

var (
        ErrJobNotFound   = errors.New("job not found")
        ErrConflict      = errors.New("conflict")
        ErrInvalidInput  = errors.New("invalid input")
        ErrUnreachable   = errors.New("conduit unreachable")
)

type Client struct {
        base string
        http *http.Client
}

type Config struct {
        BaseURL    string
        HTTPClient *http.Client
}

func New(cfg Config) *Client {
        hc := cfg.HTTPClient
        if hc == nil {
                hc = &http.Client{Timeout: 10 * time.Second}
        }
        return &Client{
                base: cfg.BaseURL,
                http: hc,
        }
}

type SubmitOptions struct {
        Payload        json.RawMessage
        Priority       int
        MaxRetries     int
        IdempotencyKey string
        Delay          time.Duration
        Timeout        time.Duration
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
        var reader io.Reader
        if body != nil {
                encoded, err := json.Marshal(body)
                if err != nil {
                        return fmt.Errorf("encode request: %w", err)
                }
                reader = bytes.NewReader(encoded)
        }
        req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
        if err != nil {
                return fmt.Errorf("build request: %w", err)
        }
        if body != nil {
                req.Header.Set("Content-Type", "application/json")
        }
        resp, err := c.http.Do(req)
        if err != nil {
                return fmt.Errorf("%w: %w", ErrUnreachable, err)
        }
        defer resp.Body.Close()
        raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
        if err != nil {
                return fmt.Errorf("read response: %w", err)
        }
        if resp.StatusCode >= 400 {
                var errBody struct {
                        Error string `json:"error"`
                }
                _ = json.Unmarshal(raw, &errBody)
                switch resp.StatusCode {
                case http.StatusNotFound:
                        return fmt.Errorf("%w: %s", ErrJobNotFound, errBody.Error)
                case http.StatusConflict:
                        return fmt.Errorf("%w: %s", ErrConflict, errBody.Error)
                case http.StatusBadRequest:
                        return fmt.Errorf("%w: %s", ErrInvalidInput, errBody.Error)
                default:
                        return fmt.Errorf("http %d: %s", resp.StatusCode, errBody.Error)
                }
        }
        if out == nil {
                return nil
        }
        if err := json.Unmarshal(raw, out); err != nil {
                return fmt.Errorf("decode response: %w", err)
        }
        return nil
}

func (c *Client) SubmitJob(ctx context.Context, jobType string, opts SubmitOptions) (*model.Job, error) {
        body := map[string]any{
                "type":    jobType,
                "payload": normalize(opts.Payload),
        }
        if opts.Priority != 0 {
                body["priority"] = opts.Priority
        }
        if opts.MaxRetries != 0 {
                body["max_retries"] = opts.MaxRetries
        }
        if opts.IdempotencyKey != "" {
                body["idempotency_key"] = opts.IdempotencyKey
        }
        if opts.Delay > 0 {
                body["delay_ms"] = opts.Delay.Milliseconds()
        }
        if opts.Timeout > 0 {
                body["timeout_ms"] = opts.Timeout.Milliseconds()
        }
        var res struct {
                Job       *model.Job `json:"job"`
                Duplicate bool       `json:"duplicate"`
        }
        if err := c.do(ctx, http.MethodPost, "/v1/jobs", body, &res); err != nil {
                return nil, err
        }
        return res.Job, nil
}

func (c *Client) GetJob(ctx context.Context, id string) (*model.Job, error) {
        var res struct {
                Job *model.Job `json:"job"`
        }
        if err := c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &res); err != nil {
                return nil, err
        }
        return res.Job, nil
}

func (c *Client) CancelJob(ctx context.Context, id string) (*model.Job, error) {
        var res struct {
                Job *model.Job `json:"job"`
        }
        if err := c.do(ctx, http.MethodDelete, "/v1/jobs/"+url.PathEscape(id), nil, &res); err != nil {
                return nil, err
        }
        return res.Job, nil
}

type ListFilter struct {
        Status string
        Type   string
        Limit  int
}

func (c *Client) ListJobs(ctx context.Context, filter ListFilter) ([]model.Job, error) {
        q := url.Values{}
        if filter.Status != "" {
                q.Set("status", filter.Status)
        }
        if filter.Type != "" {
                q.Set("type", filter.Type)
        }
        if filter.Limit > 0 {
                q.Set("limit", fmt.Sprintf("%d", filter.Limit))
        }
        var res struct {
                Jobs []model.Job `json:"jobs"`
        }
        path := "/v1/jobs"
        if enc := q.Encode(); enc != "" {
                path += "?" + enc
        }
        if err := c.do(ctx, http.MethodGet, path, nil, &res); err != nil {
                return nil, err
        }
        return res.Jobs, nil
}

func (c *Client) QueueDepth(ctx context.Context) (map[string]int64, error) {
        var res struct {
                Streams map[string]int64 `json:"streams"`
        }
        if err := c.do(ctx, http.MethodGet, "/v1/queues", nil, &res); err != nil {
                return nil, err
        }
        return res.Streams, nil
}

func (c *Client) CreateWorkflow(ctx context.Context, def model.WorkflowDefinition) (*model.Workflow, error) {
        var res struct {
                Workflow *model.Workflow `json:"workflow"`
        }
        if err := c.do(ctx, http.MethodPost, "/v1/workflows", def, &res); err != nil {
                return nil, err
        }
        return res.Workflow, nil
}

func (c *Client) RunWorkflow(ctx context.Context, workflowID string) (*model.WorkflowRun, error) {
        var res struct {
                Run *model.WorkflowRun `json:"run"`
        }
        if err := c.do(ctx, http.MethodPost, "/v1/workflows/"+url.PathEscape(workflowID)+"/run", nil, &res); err != nil {
                return nil, err
        }
        return res.Run, nil
}

func (c *Client) ListRuns(ctx context.Context, workflowID string) ([]model.WorkflowRun, error) {
        var res struct {
                Runs []model.WorkflowRun `json:"runs"`
        }
        if err := c.do(ctx, http.MethodGet, "/v1/workflows/"+url.PathEscape(workflowID)+"/runs", nil, &res); err != nil {
                return nil, err
        }
        return res.Runs, nil
}

func normalize(p json.RawMessage) json.RawMessage {
        if len(p) == 0 {
                return json.RawMessage(`{}`)
        }
        return p
}
