package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	w.MustRegister("email.send", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
		var payload struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
			Body    string `json:"body"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return nil, sdk.Permanent(err)
		}
		if payload.To == "" || !validEmail(payload.To) {
			return nil, sdk.Permanent(fmt.Errorf("invalid recipient %q", payload.To))
		}
		time.Sleep(50 * time.Millisecond)
		return json.RawMessage(`{"queued":true}`), nil
	})
	if err := w.RunForever(); err != nil {
		panic(err)
	}
}

func validEmail(addr string) bool {
	at := -1
	for i, r := range addr {
		if r == '@' {
			if at != -1 {
				return false
			}
			at = i
		}
	}
	return at > 0 && at < len(addr)-1 && len(addr) <= 254
}
