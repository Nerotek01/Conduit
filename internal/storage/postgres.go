package storage

import (
        "context"
        "fmt"
        "io/fs"
        "sort"
        "time"

        "github.com/jackc/pgx/v5/pgxpool"

        "github.com/Nerotek01/Conduit/migrations"
)

type Store struct {
        pool *pgxpool.Pool
}

type Options struct {
        DSN             string
        MaxConns        int32
        MaxConnLifetime time.Duration
}

func New(ctx context.Context, opts Options) (*Store, error) {
        cfg, err := pgxpool.ParseConfig(opts.DSN)
        if err != nil {
                return nil, fmt.Errorf("parse postgres dsn: %w", err)
        }
        if opts.MaxConns > 0 {
                cfg.MaxConns = opts.MaxConns
        }
        if opts.MaxConnLifetime > 0 {
                cfg.MaxConnLifetime = opts.MaxConnLifetime
        } else {
                cfg.MaxConnLifetime = time.Hour
        }
        cfg.MaxConnIdleTime = 15 * time.Minute
        cfg.HealthCheckPeriod = 30 * time.Second

        pool, err := pgxpool.NewWithConfig(ctx, cfg)
        if err != nil {
                return nil, fmt.Errorf("create postgres pool: %w", err)
        }
        pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        defer cancel()
        if err := pool.Ping(pingCtx); err != nil {
                pool.Close()
                return nil, fmt.Errorf("ping postgres: %w", err)
        }
        s := &Store{pool: pool}
        if err := s.migrate(ctx); err != nil {
                pool.Close()
                return nil, fmt.Errorf("migrate: %w", err)
        }
        return s, nil
}

func (s *Store) Close() {
        s.pool.Close()
}

func (s *Store) Pool() *pgxpool.Pool {
        return s.pool
}

// migrateLockKey is hashed by PostgreSQL into the advisory-lock key space.
// Every Conduit process (server, worker, coordinator) runs migrations at boot;
// with docker compose they start in parallel, and without serialization two
// processes can both observe a missing schema_migrations row, both apply the
// same migration, and one then dies on a primary-key violation. A session-level
// advisory lock on a single dedicated connection makes concurrent boots
// strictly sequential instead.
const migrateLockKey = "conduit-migrate"

func (s *Store) migrate(ctx context.Context) error {
        // pg_advisory_lock is session-scoped, so the lock, every migration
        // statement, and the unlock must run on the SAME pooled connection.
        // Running them through the pool would spread them across connections and
        // leave the lock stranded on whichever session happened to take it.
        conn, err := s.pool.Acquire(ctx)
        if err != nil {
                return fmt.Errorf("acquire migration connection: %w", err)
        }
        defer conn.Release()
        if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext($1))`, migrateLockKey); err != nil {
                return fmt.Errorf("acquire migration lock: %w", err)
        }
        defer func() {
                relCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
                defer cancel()
                _, _ = conn.Exec(relCtx, `SELECT pg_advisory_unlock(hashtext($1))`, migrateLockKey)
        }()
        if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
                return fmt.Errorf("ensure migrations table: %w", err)
        }
        entries, err := fs.Glob(migrations.FS, "*.sql")
        if err != nil {
                return fmt.Errorf("list migrations: %w", err)
        }
        sort.Strings(entries)
        for _, name := range entries {
                var exists bool
                if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&exists); err != nil {
                        return fmt.Errorf("check migration %s: %w", name, err)
                }
                if exists {
                        continue
                }
                body, err := migrations.FS.ReadFile(name)
                if err != nil {
                        return fmt.Errorf("read migration %s: %w", name, err)
                }
                tx, err := conn.Begin(ctx)
                if err != nil {
                        return fmt.Errorf("begin migration tx: %w", err)
                }
                if _, err := tx.Exec(ctx, string(body)); err != nil {
                        _ = tx.Rollback(ctx)
                        return fmt.Errorf("apply migration %s: %w", name, err)
                }
                if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
                        _ = tx.Rollback(ctx)
                        return fmt.Errorf("record migration %s: %w", name, err)
                }
                if err := tx.Commit(ctx); err != nil {
                        return fmt.Errorf("commit migration %s: %w", name, err)
                }
        }
        return nil
}
