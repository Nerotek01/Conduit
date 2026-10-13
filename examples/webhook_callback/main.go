package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Nerotek01/Conduit/pkg/sdk"
)

func main() {
	cfg, err := sdk.WorkerConfigFromEnv()
	if err != nil {
		panic(err)
	}
	w, err := sdk.NewWorker(cfg)
	if err != nil {
		panic(err)
	}
	w.MustRegister("webhook.deliver", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
		var payload struct {
			URL     string          `json:"url"`
			Event   string          `json:"event"`
			Body    json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return nil, sdk.Permanent(fmt.Errorf("invalid webhook payload: %w", err))
		}
		if payload.URL == "" || len(payload.URL) > 2048 {
			return nil, sdk.Permanent(fmt.Errorf("invalid url"))
		}
		if !allowedScheme(payload.URL) {
			return nil, sdk.Permanent(fmt.Errorf("url scheme not allowed"))
		}
		wrapped, err := json.Marshal(map[string]any{
			"event":     payload.Event,
			"job_id":    job.ID,
			"attempt":   job.Attempts,
			"delivered": time.Now().UTC().Format(time.RFC3339Nano),
			"body":      payload.Body,
		})
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, payload.URL, bytes.NewReader(wrapped))
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "conduit-webhook/0.1")
		req.Header.Set("X-Conduit-Job-Id", job.ID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("deliver webhook: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			return nil, fmt.Errorf("upstream returned %d", resp.StatusCode)
		}
		if resp.StatusCode >= 400 {
			return nil, sdk.Permanent(fmt.Errorf("upstream rejected with %d", resp.StatusCode))
		}
		return json.RawMessage(fmt.Sprintf(`{"delivered":true,"status":%d}`, resp.StatusCode)), nil
	})
	if err := w.RunForever(); err != nil {
		panic(err)
	}
}

func allowedScheme(rawURL string) bool {
	return len(rawURL) >= 8 &&
		(rawURL[:8] == "https://" || (len(rawURL) >= 7 && rawURL[:7] == "http://"))
}
