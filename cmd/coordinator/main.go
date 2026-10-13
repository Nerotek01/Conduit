package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nerotek01/Conduit/internal/config"
	"github.com/Nerotek01/Conduit/internal/coordinator"
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
	logger := observability.NewLogger(cfg.LogLevel, "coordinator")
	logger.Info("starting conduit coordinator", "version", version.Version)

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

	c := coordinator.New(store, q, redisClient, logger, coordinator.Options{
		RedisClient:       redisClient,
		QueuePrefix:       cfg.QueuePrefix,
		VisibilityTimeout: cfg.VisibilityTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SchedulerInterval: cfg.SchedulerInterval,
	})

	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("coordinator stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("coordinator stopped")
}
