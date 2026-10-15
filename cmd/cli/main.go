package main

import (
        "bytes"
        "encoding/json"
        "flag"
        "fmt"
        "io"
        "net/http"
        "net/url"
        "os"
        "strings"
        "time"
)

type client struct {
        base   string
        http   *http.Client
}

func newClient(base string) *client {
        return &client{
                base: strings.TrimRight(base, "/"),
                http: &http.Client{Timeout: 15 * time.Second},
        }
}

func (c *client) do(method, path string, body any) (int, []byte, error) {
        var reader io.Reader
        if body != nil {
                encoded, err := json.Marshal(body)
                if err != nil {
                        return 0, nil, err
                }
                reader = bytes.NewReader(encoded)
        }
        req, err := http.NewRequest(method, c.base+path, reader)
        if err != nil {
                return 0, nil, err
        }
        if body != nil {
                req.Header.Set("Content-Type", "application/json")
        }
        resp, err := c.http.Do(req)
        if err != nil {
                return 0, nil, err
        }
        defer resp.Body.Close()
        raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
        if err != nil {
                return resp.StatusCode, nil, err
        }
        return resp.StatusCode, raw, nil
}

func main() {
        base := envOr("CONDUIT_HTTP_ADDR", "http://localhost:8080")
        if strings.HasPrefix(base, ":") {
                base = "http://localhost" + base
        } else if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
                base = "http://" + base
        }
        c := newClient(base)

        commands := map[string]func(*client, []string) error{
                "submit":       cmdSubmit,
                "get":          cmdGet,
                "list":         cmdList,
                "cancel":       cmdCancel,
                "queues":       cmdQueues,
                "workers":      cmdWorkers,
                "workflow":     cmdWorkflow,
                "schedule":     cmdSchedule,
        }
        args := os.Args[1:]
        if len(args) == 0 {
                usage(commands)
                os.Exit(2)
        }
        run, ok := commands[args[0]]
        if !ok {
                fmt.Fprintln(os.Stderr, "unknown command:", args[0])
                usage(commands)
                os.Exit(2)
        }
        if err := run(c, args[1:]); err != nil {
                fmt.Fprintln(os.Stderr, "error:", err)
                os.Exit(1)
        }
}

func usage(commands map[string]func(*client, []string) error) {
        fmt.Fprintln(os.Stderr, "conduit cli commands:")
        for name := range commands {
                fmt.Fprintf(os.Stderr, "  %s\n", name)
        }
}

func envOr(key, def string) string {
        if v := os.Getenv(key); v != "" {
                return v
        }
        return def
}

func cmdSubmit(c *client, args []string) error {
        fs := flag.NewFlagSet("submit", flag.ExitOnError)
        jobType := fs.String("type", "echo", "job type")
        priority := fs.Int("priority", 2, "priority 1-4")
        delay := fs.Duration("delay", 0, "delay before execution")
        maxRetries := fs.Int("max-retries", 3, "maximum retries")
        idem := fs.String("idempotency-key", "", "idempotency key")
        payload := fs.String("payload", `{}`, "json payload")
        if err := fs.Parse(args); err != nil {
                return err
        }
        if !json.Valid([]byte(*payload)) {
                return fmt.Errorf("payload must be valid json")
        }
        body := map[string]any{
                "type":    *jobType,
                "payload": json.RawMessage(*payload),
                "priority": *priority,
                "max_retries": *maxRetries,
        }
        if *idem != "" {
                body["idempotency_key"] = *idem
        }
        if *delay > 0 {
                body["delay_ms"] = (*delay).Milliseconds()
        }
        status, raw, err := c.do("POST", "/v1/jobs", body)
        if err != nil {
                return err
        }
        return printResponse(status, raw)
}

func cmdGet(c *client, args []string) error {
        if len(args) != 1 {
                return fmt.Errorf("usage: get <job-id>")
        }
        status, raw, err := c.do("GET", "/v1/jobs/"+url.PathEscape(args[0]), nil)
        if err != nil {
                return err
        }
        return printResponse(status, raw)
}

func cmdList(c *client, args []string) error {
        fs := flag.NewFlagSet("list", flag.ExitOnError)
        status := fs.String("status", "", "filter by status")
        jobType := fs.String("type", "", "filter by job type")
        limit := fs.Int("limit", 20, "result limit")
        if err := fs.Parse(args); err != nil {
                return err
        }
        q := url.Values{}
        q.Set("limit", fmt.Sprintf("%d", *limit))
        if *status != "" {
                q.Set("status", *status)
        }
        if *jobType != "" {
                q.Set("type", *jobType)
        }
        code, raw, err := c.do("GET", "/v1/jobs?"+q.Encode(), nil)
        if err != nil {
                return err
        }
        return printResponse(code, raw)
}

func cmdCancel(c *client, args []string) error {
        if len(args) != 1 {
                return fmt.Errorf("usage: cancel <job-id>")
        }
        status, raw, err := c.do("DELETE", "/v1/jobs/"+url.PathEscape(args[0]), nil)
        if err != nil {
                return err
        }
        return printResponse(status, raw)
}

func cmdQueues(c *client, _ []string) error {
        status, raw, err := c.do("GET", "/v1/queues", nil)
        if err != nil {
                return err
        }
        return printResponse(status, raw)
}

func cmdWorkers(c *client, _ []string) error {
        status, raw, err := c.do("GET", "/v1/workers", nil)
        if err != nil {
                return err
        }
        return printResponse(status, raw)
}

func cmdWorkflow(c *client, args []string) error {
        if len(args) == 0 {
                return fmt.Errorf("usage: workflow <create|run|runs|get> ...")
        }
        switch args[0] {
        case "create":
                if len(args) != 2 {
                        return fmt.Errorf("usage: workflow create <definition.json>")
                }
                body, err := os.ReadFile(args[1])
                if err != nil {
                        return err
                }
                if !json.Valid(body) {
                        return fmt.Errorf("definition file is not valid json")
                }
                status, raw, err := c.do("POST", "/v1/workflows", json.RawMessage(body))
                if err != nil {
                        return err
                }
                return printResponse(status, raw)
        case "get":
                if len(args) != 2 {
                        return fmt.Errorf("usage: workflow get <workflow-id>")
                }
                status, raw, err := c.do("GET", "/v1/workflows/"+url.PathEscape(args[1]), nil)
                if err != nil {
                        return err
                }
                return printResponse(status, raw)
        case "run":
                if len(args) != 2 {
                        return fmt.Errorf("usage: workflow run <workflow-id>")
                }
                status, raw, err := c.do("POST", "/v1/workflows/"+url.PathEscape(args[1])+"/run", nil)
                if err != nil {
                        return err
                }
                return printResponse(status, raw)
        case "runs":
                if len(args) != 2 {
                        return fmt.Errorf("usage: workflow runs <workflow-id>")
                }
                status, raw, err := c.do("GET", "/v1/workflows/"+url.PathEscape(args[1])+"/runs", nil)
                if err != nil {
                        return err
                }
                return printResponse(status, raw)
        default:
                return fmt.Errorf("unknown workflow subcommand: %s", args[0])
        }
}

func cmdSchedule(c *client, args []string) error {
        if len(args) == 0 {
                return fmt.Errorf("usage: schedule <create|list|delete> ...")
        }
        switch args[0] {
        case "create":
                fs := flag.NewFlagSet("schedule create", flag.ExitOnError)
                jobType := fs.String("type", "echo", "job type")
                cronExpr := fs.String("cron", "", "cron expression")
                timezone := fs.String("tz", "UTC", "timezone")
                priority := fs.Int("priority", 2, "priority 1-4")
                payload := fs.String("payload", `{}`, "json payload")
                if err := fs.Parse(args[1:]); err != nil {
                        return err
                }
                if *cronExpr == "" {
                        return fmt.Errorf("cron expression is required")
                }
                body := map[string]any{
                        "job_type": *jobType,
                        "cron":     *cronExpr,
                        "timezone": *timezone,
                        "priority": *priority,
                        "payload":  json.RawMessage(*payload),
                }
                status, raw, err := c.do("POST", "/v1/schedules", body)
                if err != nil {
                        return err
                }
                return printResponse(status, raw)
        case "list":
                status, raw, err := c.do("GET", "/v1/schedules", nil)
                if err != nil {
                        return err
                }
                return printResponse(status, raw)
        case "delete":
                if len(args) != 2 {
                        return fmt.Errorf("usage: schedule delete <schedule-id>")
                }
                status, raw, err := c.do("DELETE", "/v1/schedules/"+url.PathEscape(args[1]), nil)
                if err != nil {
                        return err
                }
                return printResponse(status, raw)
        default:
                return fmt.Errorf("unknown schedule subcommand: %s", args[0])
        }
}

func printResponse(status int, raw []byte) error {
        if status >= 400 {
                return fmt.Errorf("http %d: %s", status, string(raw))
        }
        var pretty bytes.Buffer
        if err := json.Indent(&pretty, raw, "", "  "); err == nil {
                fmt.Println(pretty.String())
        } else {
                fmt.Println(string(raw))
        }
        return nil
}
