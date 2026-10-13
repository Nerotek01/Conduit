package sdk

import (
        "context"
        "fmt"
        "os/signal"
        "syscall"
        "time"

        "github.com/redis/go-redis/v9"

        "github.com/Nerotek01/Conduit/internal/config"
        "github.com/Nerotek01/Conduit/internal/events"
        "github.com/Nerotek01/Conduit/internal/metrics"
        "github.com/Nerotek01/Conduit/internal/observability"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
        "github.com/Nerotek01/Conduit/internal/worker"
        "github.com/Nerotek01/Conduit/internal/workflow"
)

type Handler = worker.Handler

type WorkerConfig struct {
        PGDSN             string
        RedisAddr         string
        RedisPassword     string
        RedisDB           int
        QueuePrefix       string
        WorkerID          string
        Hostname          string
        Concurrency       int
        HeartbeatInterval time.Duration
        JobTimeout        time.Duration
        RetryBaseDelay    time.Duration
        RetryMaxDelay     time.Duration
        LogLevel          string
}

func WorkerConfigFromEnv() (WorkerConfig, error) {
        cfg, err := config.Load()
        if err != nil {
                return WorkerConfig{}, err
        }
        return WorkerConfig{
                PGDSN:             cfg.PGDSN,
                RedisAddr:         cfg.RedisAddr,
                RedisPassword:     cfg.RedisPassword,
                RedisDB:           cfg.RedisDB,
                QueuePrefix:       cfg.QueuePrefix,
                WorkerID:          cfg.WorkerID,
                Hostname:          cfg.Hostname,
                Concurrency:       cfg.Concurrency,
                HeartbeatInterval: cfg.HeartbeatInterval,
                JobTimeout:        cfg.JobTimeout,
                RetryBaseDelay:    cfg.RetryBaseDelay,
                RetryMaxDelay:     cfg.RetryMaxDelay,
                LogLevel:          cfg.LogLevel,
        }, nil
}

type Worker struct {
        cfg      WorkerConfig
        registry *worker.Registry
}

func NewWorker(cfg WorkerConfig) (*Worker, error) {
        if cfg.Concurrency < 1 || cfg.Concurrency > 512 {
                return nil, fmt.Errorf("concurrency must be between 1 and 512")
        }
        if cfg.PGDSN == "" {
                return nil, fmt.Errorf("pg dsn is required")
        }
        if cfg.RedisAddr == "" {
                return nil, fmt.Errorf("redis addr is required")
        }
        return &Worker{cfg: cfg, registry: worker.NewRegistry()}, nil
}

func (w *Worker) Register(jobType string, handler Handler) error {
        return w.registry.Register(jobType, handler)
}

func (w *Worker) MustRegister(jobType string, handler Handler) {
        if err := w.Register(jobType, handler); err != nil {
                panic(err)
        }
}

func (w *Worker) Run(ctx context.Context) error {
        if ctx == nil {
                signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
                defer stop()
                ctx = signalCtx
        }
        logger := observability.NewLogger(w.cfg.LogLevel, "worker-sdk")
        store, err := storage.New(ctx, storage.Options{DSN: w.cfg.PGDSN})
        if err != nil {
                return fmt.Errorf("postgres: %w", err)
        }
        defer store.Close()
        client := redis.NewClient(&redis.Options{
                Addr:            w.cfg.RedisAddr,
                Password:        w.cfg.RedisPassword,
                DB:              w.cfg.RedisDB,
                ConnMaxLifetime: time.Hour,
                DialTimeout:     5 * time.Second,
        })
        if err := client.Ping(ctx).Err(); err != nil {
                return fmt.Errorf("redis: %w", err)
        }
        q := queue.New(client, w.cfg.QueuePrefix)
        defer q.Close()

        submitter := workflow.NewSubmitter(store, q)
        engine := workflow.NewEngine(store, submitter.Submit)
        met := metrics.New()

        pool, err := worker.NewPool(store, q, w.registry, events.NoopBus{}, met, logger, worker.Options{
                WorkerID:       w.cfg.WorkerID,
                Hostname:       w.cfg.Hostname,
                Concurrency:    w.cfg.Concurrency,
                Heartbeat:      w.cfg.HeartbeatInterval,
                Hook:           engine.OnJobTerminal,
                DefaultTimeout: w.cfg.JobTimeout,
                RetryBaseDelay: w.cfg.RetryBaseDelay,
                RetryMaxDelay:  w.cfg.RetryMaxDelay,
        })
        if err != nil {
                return err
        }
        return pool.Run(ctx)
}

func (w *Worker) RunForever() error {
        return w.Run(nil)
}
