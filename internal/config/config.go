package config

import (
        "fmt"
        "os"
        "strconv"
        "time"
)

type Config struct {
        HTTPAddr          string
        GRPCAddr          string
        MetricsAddr       string
        PGDSN             string
        RedisAddr         string
        RedisPassword     string
        RedisDB           int
        QueuePrefix       string
        WorkerID          string
        Hostname          string
        Concurrency       int
        HeartbeatInterval time.Duration
        VisibilityTimeout time.Duration
        SchedulerInterval time.Duration
        ShutdownTimeout   time.Duration
        JobTimeout        time.Duration
        RetryBaseDelay    time.Duration
        RetryMaxDelay     time.Duration
        MaxPayloadBytes   int64
        MaxBodyBytes      int64
        WSMaxClients      int
        DashboardDir      string
        LogLevel          string
        OtelEndpoint      string
}

func str(key, def string) string {
        if v, ok := os.LookupEnv(key); ok && v != "" {
                return v
        }
        return def
}

func integer(key string, def int) (int, error) {
        v, ok := os.LookupEnv(key)
        if !ok || v == "" {
                return def, nil
        }
        n, err := strconv.Atoi(v)
        if err != nil {
                return 0, fmt.Errorf("invalid %s: %w", key, err)
        }
        return n, nil
}

func duration(key string, def time.Duration) (time.Duration, error) {
        v, ok := os.LookupEnv(key)
        if !ok || v == "" {
                return def, nil
        }
        d, err := time.ParseDuration(v)
        if err != nil {
                return 0, fmt.Errorf("invalid %s: %w", key, err)
        }
        return d, nil
}

func Load() (Config, error) {
        cfg := Config{
                HTTPAddr:      str("CONDUIT_HTTP_ADDR", ":8080"),
                GRPCAddr:      str("CONDUIT_GRPC_ADDR", ":9090"),
                MetricsAddr:   str("CONDUIT_METRICS_ADDR", ""),
                PGDSN:         str("CONDUIT_PG_DSN", "postgres://conduit:conduit@localhost:5432/conduit?sslmode=disable"),
                RedisAddr:     str("CONDUIT_REDIS_ADDR", "localhost:6379"),
                RedisPassword: str("CONDUIT_REDIS_PASSWORD", ""),
                QueuePrefix:   str("CONDUIT_QUEUE_PREFIX", "conduit"),
                WorkerID:      str("CONDUIT_WORKER_ID", ""),
                Hostname:      str("CONDUIT_HOSTNAME", ""),
                DashboardDir:  str("CONDUIT_DASHBOARD_DIR", ""),
                LogLevel:      str("CONDUIT_LOG_LEVEL", "info"),
                OtelEndpoint:  str("CONDUIT_OTEL_ENDPOINT", ""),
        }
        var err error
        if cfg.RedisDB, err = integer("CONDUIT_REDIS_DB", 0); err != nil {
                return cfg, err
        }
        if cfg.Concurrency, err = integer("CONDUIT_WORKER_CONCURRENCY", 8); err != nil {
                return cfg, err
        }
        if cfg.WSMaxClients, err = integer("CONDUIT_WS_MAX_CLIENTS", 100); err != nil {
                return cfg, err
        }
        var payload int
        if payload, err = integer("CONDUIT_MAX_PAYLOAD_KB", 256); err != nil {
                return cfg, err
        }
        cfg.MaxPayloadBytes = int64(payload) * 1024
        cfg.MaxBodyBytes = cfg.MaxPayloadBytes + 4096
        if cfg.HeartbeatInterval, err = duration("CONDUIT_HEARTBEAT_INTERVAL", 5*time.Second); err != nil {
                return cfg, err
        }
        if cfg.VisibilityTimeout, err = duration("CONDUIT_VISIBILITY_TIMEOUT", 60*time.Second); err != nil {
                return cfg, err
        }
        if cfg.SchedulerInterval, err = duration("CONDUIT_SCHEDULER_INTERVAL", time.Second); err != nil {
                return cfg, err
        }
        if cfg.ShutdownTimeout, err = duration("CONDUIT_SHUTDOWN_TIMEOUT", 30*time.Second); err != nil {
                return cfg, err
        }
        if cfg.JobTimeout, err = duration("CONDUIT_JOB_TIMEOUT", 10*time.Minute); err != nil {
                return cfg, err
        }
        if cfg.RetryBaseDelay, err = duration("CONDUIT_RETRY_BASE_DELAY", 500*time.Millisecond); err != nil {
                return cfg, err
        }
        if cfg.RetryMaxDelay, err = duration("CONDUIT_RETRY_MAX_DELAY", 5*time.Minute); err != nil {
                return cfg, err
        }
        return cfg, cfg.validate()
}

func (c Config) validate() error {
        if c.Concurrency < 1 || c.Concurrency > 512 {
                return fmt.Errorf("worker concurrency must be between 1 and 512")
        }
        if c.WSMaxClients < 1 || c.WSMaxClients > 10000 {
                return fmt.Errorf("ws max clients must be between 1 and 10000")
        }
        if c.MaxPayloadBytes < 1024 || c.MaxPayloadBytes > 16<<20 {
                return fmt.Errorf("max payload must be between 1KB and 16MB")
        }
        if c.HeartbeatInterval < 500*time.Millisecond {
                return fmt.Errorf("heartbeat interval must be at least 500ms")
        }
        if c.VisibilityTimeout < 5*time.Second {
                return fmt.Errorf("visibility timeout must be at least 5s")
        }
        if c.JobTimeout < time.Second || c.JobTimeout > 24*time.Hour {
                return fmt.Errorf("job timeout must be between 1s and 24h")
        }
        if c.RetryBaseDelay < 50*time.Millisecond {
                return fmt.Errorf("retry base delay must be at least 50ms")
        }
        if c.RetryMaxDelay < c.RetryBaseDelay {
                return fmt.Errorf("retry max delay must not be smaller than the base delay")
        }
        return nil
}
