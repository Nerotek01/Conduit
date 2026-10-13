package main

import (
	"context"
	"encoding/json"
	"fmt"

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
	w.MustRegister("image.resize", func(ctx context.Context, job *sdk.Job) (json.RawMessage, error) {
		var payload struct {
			Source   string `json:"source"`
			Width    int    `json:"width"`
			Height   int    `json:"height"`
			Quality  int    `json:"quality"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return nil, sdk.Permanent(fmt.Errorf("invalid resize payload: %w", err))
		}
		if payload.Width < 1 || payload.Width > 10000 || payload.Height < 1 || payload.Height > 10000 {
			return nil, sdk.Permanent(fmt.Errorf("dimensions out of range"))
		}
		if payload.Source == "" || len(payload.Source) > 2048 {
			return nil, sdk.Permanent(fmt.Errorf("invalid source"))
		}
		result, err := json.Marshal(map[string]any{
			"source":   payload.Source,
			"width":    payload.Width,
			"height":   payload.Height,
			"quality":  clampQuality(payload.Quality),
			"bytes":    payload.Width * payload.Height / 3,
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	})
	if err := w.RunForever(); err != nil {
		panic(err)
	}
}

func clampQuality(q int) int {
	if q < 1 {
		return 80
	}
	if q > 100 {
		return 100
	}
	return q
}
