package main

import (
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "net"
        "net/http"
        "os"
        "os/signal"
        "syscall"
        "time"

        "github.com/redis/go-redis/v9"
        "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
        "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
        "google.golang.org/grpc"

        conduitv1 "github.com/Nerotek01/Conduit/pkg/api/conduit/v1"
        "github.com/Nerotek01/Conduit/internal/api"
        "github.com/Nerotek01/Conduit/internal/config"
        "github.com/Nerotek01/Conduit/internal/events"
        "github.com/Nerotek01/Conduit/internal/metrics"
        "github.com/Nerotek01/Conduit/internal/observability"
        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
        "github.com/Nerotek01/Conduit/internal/version"
)

func main() {
        cfg, err := config.Load()
        if err != nil {
                fmt.Fprintln(os.Stderr, "config:", err)
                os.Exit(1)
        }
        logger := observability.NewLogger(cfg.LogLevel, "server")
        logger.Info("starting conduit server", "version", version.Version, "http", cfg.HTTPAddr, "grpc", cfg.GRPCAddr)

        ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
        defer stop()

        tracing, err := observability.InitTracing(ctx, cfg.OtelEndpoint, "conduit-server")
        if err != nil {
                logger.Error("tracing init failed", "error", err)
                os.Exit(1)
        }
        defer tracing.Shutdown(context.Background())

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

        met := metrics.New()
        bus := events.NewChanBus(512, met.EventsDropped.Inc)

        svc := api.NewService(store, q, bus, met, logger, cfg.MaxPayloadBytes, cfg.QueuePrefix)

        var traceWrap func(http.Handler) http.Handler
        if cfg.OtelEndpoint != "" {
                traceWrap = func(next http.Handler) http.Handler {
                        return otelhttp.NewHandler(next, "conduit-http")
                }
        }

        httpServer := api.NewServer(svc, api.ServerOptions{
                HTTPAddr:        cfg.HTTPAddr,
                DashboardDir:    cfg.DashboardDir,
                MaxBodyBytes:    cfg.MaxBodyBytes,
                WSMaxClients:    cfg.WSMaxClients,
                ShutdownTimeout: cfg.ShutdownTimeout,
                Logger:          logger,
                TraceWrap:       traceWrap,
        }, met)

        grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
        conduitv1.RegisterJobServiceServer(grpcServer, api.NewGRPCServer(svc))

        errCh := make(chan error, 2)
        go func() {
                lis, err := net.Listen("tcp", cfg.GRPCAddr)
                if err != nil {
                        logger.Error("grpc listen failed", "error", err)
                        errCh <- fmt.Errorf("grpc listen: %w", err)
                        return
                }
                logger.Info("grpc server listening", "addr", cfg.GRPCAddr)
                if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
                        logger.Error("grpc serve failed", "error", err)
                }
        }()

        snapshot := func() ([]byte, bool) {
                ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
                defer cancel()
                snap, err := svc.QueueSnapshot(ctx)
                if err != nil {
                        return nil, false
                }
                body, err := json.Marshal(events.Event{Kind: events.QueueDepth, Data: mustJSON(snap)})
                if err != nil {
                        return nil, false
                }
                return body, true
        }

        go func() {
                if err := httpServer.Start(ctx, bus, snapshot); err != nil {
                        errCh <- err
                }
        }()

        select {
        case <-ctx.Done():
                logger.Info("shutdown signal received")
        case err := <-errCh:
                if err != nil {
                        logger.Error("http server failed", "error", err)
                }
        }

        stopped := make(chan struct{})
        go func() {
                grpcServer.GracefulStop()
                close(stopped)
        }()
        select {
        case <-stopped:
        case <-time.After(cfg.ShutdownTimeout):
                grpcServer.Stop()
        }
        logger.Info("server stopped")
}

func mustJSON(v any) json.RawMessage {
        body, err := json.Marshal(v)
        if err != nil {
                return json.RawMessage(`{}`)
        }
        return body
}
