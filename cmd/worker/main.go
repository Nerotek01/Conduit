package main

import (
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "net/http"
        "os"
        "os/signal"
        "syscall"
        "time"

        "github.com/redis/go-redis/v9"

        "github.com/Nerotek01/Conduit/internal/config"
        "github.com/Nerotek01/Conduit/internal/events"
        "github.com/Nerotek01/Conduit/internal/metrics"
        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/observability"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
        "github.com/Nerotek01/Conduit/internal/version"
        "github.com/Nerotek01/Conduit/internal/worker"
        "github.com/Nerotek01/Conduit/internal/workflow"
)

func main() {
        cfg, err := config.Load()
        if err != nil {
                fmt.Fprintln(os.Stderr, "config:", err)
                os.Exit(1)
        }
        logger := observability.NewLogger(cfg.LogLevel, "worker")
        logger.Info("starting conduit worker", "version", version.Version, "concurrency", cfg.Concurrency)

        ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
        defer stop()

        store, err := storage.New(ctx, storage.Options{DSN: cfg.PGDSN})
        if err != nil {
                logger.Error("postgres init failed", "error", err)
                os.Exit(1)
        }
        defer store.Close()

        redisClient := redis.NewClient(&redis.Options{
                Addr:            cfg.RedisAddr,
                Password:        cfg.RedisPassword,
                DB:              cfg.RedisDB,
                ConnMaxLifetime: time.Hour,
                MinIdleConns:   2,
                DialTimeout:     5 * time.Second,
                ReadTimeout:     3 * time.Second,
                WriteTimeout:    3 * time.Second,
        })
        if err := redisClient.Ping(ctx).Err(); err != nil {
                logger.Error("redis init failed", "error", err)
                os.Exit(1)
        }
        q := queue.New(redisClient, cfg.QueuePrefix)
        defer q.Close()

        registry := worker.NewRegistry()
        if err := registerBuiltinHandlers(registry); err != nil {
                logger.Error("register builtin handlers failed", "error", err)
                os.Exit(1)
        }

        met := metrics.New()
        bus := events.NoopBus{}

        submitter := workflow.NewSubmitter(store, q)
        engine := workflow.NewEngine(store, submitter.Submit, submitter.Cancel)

        pool, err := worker.NewPool(store, q, registry, bus, met, logger, worker.Options{
                WorkerID:       cfg.WorkerID,
                Hostname:       cfg.Hostname,
                Concurrency:    cfg.Concurrency,
                Heartbeat:      cfg.HeartbeatInterval,
                Hook:           engine.OnJobTerminal,
                DefaultTimeout: cfg.JobTimeout,
                RetryBaseDelay: cfg.RetryBaseDelay,
                RetryMaxDelay:  cfg.RetryMaxDelay,
        })
        if err != nil {
                logger.Error("worker pool init failed", "error", err)
                os.Exit(1)
        }

        if cfg.MetricsAddr != "" {
                mux := http.NewServeMux()
                mux.Handle("GET /metrics", met.Handler())
                metricsServer := &http.Server{
                        Addr:              cfg.MetricsAddr,
                        Handler:           mux,
                        ReadHeaderTimeout: 5 * time.Second,
                }
                go func() {
                        if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
                                logger.Error("metrics server failed", "error", err)
                        }
                }()
                defer metricsServer.Close()
                logger.Info("metrics server listening", "addr", cfg.MetricsAddr)
        }

        if err := pool.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
                logger.Error("worker stopped with error", "error", err)
                os.Exit(1)
        }
        logger.Info("worker stopped", "worker_id", pool.ID())
}

func registerBuiltinHandlers(registry *worker.Registry) error {
        if err := registry.Register("echo", func(ctx context.Context, job *model.Job) (json.RawMessage, error) {
                return job.Payload, nil
        }); err != nil {
                return err
        }
        if err := registry.Register("sleep", func(ctx context.Context, job *model.Job) (json.RawMessage, error) {
                var payload struct {
                        Milliseconds int64 `json:"ms"`
                }
                if err := json.Unmarshal(job.Payload, &payload); err != nil {
                        return nil, fmt.Errorf("invalid sleep payload: %w", err)
                }
                if payload.Milliseconds < 0 || payload.Milliseconds > 60000 {
                        return nil, fmt.Errorf("sleep duration out of range")
                }
                select {
                case <-time.After(time.Duration(payload.Milliseconds) * time.Millisecond):
                        return json.RawMessage(`{"slept":true}`), nil
                case <-ctx.Done():
                        return nil, ctx.Err()
                }
        }); err != nil {
                return err
        }
        return registry.Register("fail", func(ctx context.Context, job *model.Job) (json.RawMessage, error) {
                var payload struct {
                        Attempts int `json:"fail_until_attempt"`
                }
                _ = json.Unmarshal(job.Payload, &payload)
                if job.Attempts <= payload.Attempts {
                        return nil, fmt.Errorf("planned failure on attempt %d", job.Attempts)
                }
                return json.RawMessage(`{"recovered":true}`), nil
        })
}
