// Command demo_worker is a realistic multi-handler worker built on the SDK.
// It registers the business job types used by the demo stack (see
// examples/README and the dashboard screenshots) and processes them with
// simulated work: each handler sleeps briefly and returns a plausible result.
//
// Run several instances with distinct worker IDs:
//
//      CONDUIT_WORKER_ID=demo-worker-1 go run ./examples/demo_worker
package main

import (
        "context"
        "encoding/json"
        "fmt"
        "math/rand/v2"
        "time"

        "github.com/Nerotek01/Conduit/pkg/sdk"
)

func main() {
        cfg, err := sdk.WorkerConfigFromEnv()
        if err != nil {
                panic(err)
        }

        // email.send renders and hands an email to a (simulated) provider.
        w, err := sdk.NewWorker(cfg)
        if err != nil {
                panic(err)
        }
        w.MustRegister("email.send", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
                var payload struct {
                        To      string `json:"to"`
                        Subject string `json:"subject"`
                }
                if err := json.Unmarshal(job.Payload, &payload); err != nil {
                        return nil, sdk.Permanent(err)
                }
                work(60)
                return json.RawMessage(fmt.Sprintf(`{"message_id":"msg_%s","delivered":true}`, shortID(job.ID))), nil
        })

        // image.resize produces a set of derived sizes from a source asset.
        w.MustRegister("image.resize", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
                var payload struct {
                        Source   string   `json:"source"`
                        Variants []string `json:"variants"`
                }
                if err := json.Unmarshal(job.Payload, &payload); err != nil {
                        return nil, sdk.Permanent(err)
                }
                work(200)
                return json.RawMessage(fmt.Sprintf(`{"source":%q,"resized":%d,"bytes":%d}`,
                        payload.Source, len(payload.Variants), 48_000+len(payload.Variants)*12_000)), nil
        })

        // report.generate renders an analytics report and stores it. The optional
        // duration_ms payload simulates heavy rendering so the demo stack can
        // show jobs in the running state.
        w.MustRegister("report.generate", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
                var payload struct {
                        Kind       string `json:"kind"`
                        DurationMS int64  `json:"duration_ms"`
                }
                if err := json.Unmarshal(job.Payload, &payload); err != nil {
                        return nil, sdk.Permanent(err)
                }
                if payload.DurationMS > 0 {
                        if payload.DurationMS > 60_000 {
                                return nil, sdk.Permanent(fmt.Errorf("duration_ms exceeds the 60s cap"))
                        }
                        select {
                        case <-time.After(time.Duration(payload.DurationMS) * time.Millisecond):
                        case <-ctx.Done():
                                return nil, ctx.Err()
                        }
                } else {
                        work(300)
                }
                return json.RawMessage(`{"report":"weekly_analytics.pdf","pages":14,"storage":"s3://reports/"}`), nil
        })

        // payment.charge authorizes a payment against a (simulated) gateway.
        w.MustRegister("payment.charge", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
                var payload struct {
                        Amount   int    `json:"amount_cents"`
                        Currency string `json:"currency"`
                }
                if err := json.Unmarshal(job.Payload, &payload); err != nil {
                        return nil, sdk.Permanent(err)
                }
                work(120)
                return json.RawMessage(fmt.Sprintf(`{"authorization":"auth_%s","captured":%d,"currency":%q}`,
                        shortID(job.ID), payload.Amount, payload.Currency)), nil
        })

        // inventory.reserve holds stock for an order.
        w.MustRegister("inventory.reserve", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
                work(90)
                return json.RawMessage(`{"reserved":true,"ttl":"15m"}`), nil
        })

        // webhook.deliver posts an event to a partner endpoint.
        w.MustRegister("webhook.deliver", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
                var payload struct {
                        URL string `json:"url"`
                }
                if err := json.Unmarshal(job.Payload, &payload); err != nil {
                        return nil, sdk.Permanent(err)
                }
                work(80)
                return json.RawMessage(`{"status":202,"retries_used":0}`), nil
        })

        // cleanup.temp purges expired artifacts from object storage.
        w.MustRegister("cleanup.temp", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
                work(150)
                return json.RawMessage(`{"deleted":128,"freed_bytes":53687091}`), nil
        })

        if err := w.RunForever(); err != nil {
                panic(err)
        }
}

// work simulates CPU/IO latency so the dashboard shows realistic durations.
func work(ms int) {
        time.Sleep(time.Duration(ms+randInt(40)) * time.Millisecond)
}

func randInt(n int) int {
        return rand.IntN(n)
}

func shortID(id string) string {
        if len(id) > 8 {
                return id[:8]
        }
        return id
}
