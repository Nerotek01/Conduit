package testutil

import (
        "context"
        "fmt"
        "math/rand/v2"
        "os"
        "testing"
        "time"

        "github.com/redis/go-redis/v9"

        "github.com/Nerotek01/Conduit/internal/queue"
        "github.com/Nerotek01/Conduit/internal/storage"
)

func TestPGDSN() string {
        if v := os.Getenv("CONDUIT_TEST_PG_DSN"); v != "" {
                return v
        }
        return "postgres://conduit:conduit@127.0.0.1:5432/conduit_test?sslmode=disable"
}

func TestRedisAddr() string {
        if v := os.Getenv("CONDUIT_TEST_REDIS"); v != "" {
                return v
        }
        return "127.0.0.1:6379"
}

func NewStore(t *testing.T) *storage.Store {
        t.Helper()
        ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
        defer cancel()
        store, err := storage.New(ctx, storage.Options{DSN: TestPGDSN(), MaxConns: 8})
        if err != nil {
                t.Fatalf("connect test postgres: %v", err)
        }
        t.Cleanup(store.Close)
        return store
}

func UniquePrefix() string {
        return fmt.Sprintf("t%d", rand.Int64())
}

// PurgeOldRows deletes rows created more than 10 minutes ago. The shared
// test database is never reset between runs, so leftovers from previous
// invocations accumulate and eventually starve LIMIT-based queries and
// scheduler scans. The 10-minute threshold keeps parallel test packages in
// the same invocation safe: their rows are only seconds old when this runs.
func PurgeOldRows(t *testing.T, store *storage.Store) {
        t.Helper()
        ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
        defer cancel()
        statements := []string{
                `DELETE FROM jobs WHERE created_at < now() - interval '10 minutes'`,
                `DELETE FROM workers WHERE started_at < now() - interval '10 minutes'`,
                `DELETE FROM schedules WHERE created_at < now() - interval '10 minutes'`,
                `DELETE FROM workflow_runs WHERE started_at < now() - interval '10 minutes'`,
                `DELETE FROM workflows WHERE created_at < now() - interval '10 minutes'`,
        }
        for _, stmt := range statements {
                if _, err := store.Pool().Exec(ctx, stmt); err != nil {
                        t.Logf("purge (%.40s...): %v", stmt, err)
                }
        }
}

func NewQueue(t *testing.T) (*queue.RedisQueue, *redis.Client) {
        t.Helper()
        client := redis.NewClient(&redis.Options{
                Addr:        TestRedisAddr(),
                DialTimeout: 3 * time.Second,
                ReadTimeout: 2 * time.Second,
                WriteTimeout: 2 * time.Second,
        })
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := client.Ping(ctx).Err(); err != nil {
                t.Fatalf("connect test redis: %v", err)
        }
        t.Cleanup(func() { _ = client.Close() })
        prefix := UniquePrefix()
        q := queue.New(client, prefix)
        if err := q.EnsureGroups(ctx); err != nil {
                t.Fatalf("ensure groups: %v", err)
        }
        t.Cleanup(func() {
                ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
                defer cancel()
                for _, s := range q.Streams() {
                        _ = client.Del(ctx, s).Err()
                }
                _ = client.Del(ctx, q.DLQ()).Err()
        })
        return q, client
}

func Wait(t *testing.T, timeout time.Duration, interval time.Duration, cond func() bool, msg string) {
        t.Helper()
        deadline := time.Now().Add(timeout)
        for time.Now().Before(deadline) {
                if cond() {
                        return
                }
                time.Sleep(interval)
        }
        t.Fatalf("timeout after %s waiting for: %s", timeout, msg)
}
