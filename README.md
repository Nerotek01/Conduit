# Conduit

Distributed job queue & workflow engine in Go — fault-tolerant, at-least-once delivery, real-time dashboard.

Conduit runs on **PostgreSQL** for durable state and **Redis Streams** for dispatch, with a React dashboard, Prometheus metrics, OpenTelemetry tracing, and a gRPC/REST/WebSocket API surface.

```
        ┌────────────┐   REST / gRPC / WS    ┌──────────────┐
        │  Clients   │◄──────────────────────│   API server │◄── React dashboard
        │  SDK       │                        └──────┬───────┘
        └────────────┘                               │
   POST /v1/jobs → PG jobs row (idempotent) → Redis XADD (4 priority streams)
                                                    │
        ┌────────────────────────────────────────────┘
        ▼
   XREADGROUP ──► worker pool (N concurrency) ──► handler ──► PG status CAS
        ▲                                                        │
        │ XAUTOCLAIM reclaim (visibility timeout)                ▼
   coordinator (leader lock) ── scheduler (due jobs + cron)   DLQ + events
```

## Highlights

- **At-least-once execution** with idempotency keys, status CAS on every transition, and `SKIP LOCKED` claim queries — no double-claim races.
- **Retries with exponential backoff + jitter**, permanent-error short-circuit, panic recovery (poison pills die immediately), and a dead-letter stream with reasons.
- **Per-job execution timeout** (`timeout_ms`, 1s–24h) plus a server-wide default; timed-out jobs fail with `context deadline exceeded` and follow the normal retry path.
- **DAG workflows**: conditional branches (`on_success` / `on_failure`), compensation handlers (`on_failure` nodes), idempotent node submission, and optimistic versioning (CAS) with automatic conflict retry.
- **Priority queues** (1=high … 4=bulk) as four Redis Streams read in one `XREADGROUP`.
- **Cron scheduling** with timezone support and `next_run` rollover that prevents double-fires.
- **Crash recovery everywhere**: heartbeat-based worker liveness, XAUTOCLAIM reaping of orphaned messages, requeue of orphaned running jobs, stale-queued re-enqueue with per-sweep leases (no re-enqueue amplification under backlog), and workflow run recovery.
- **Real-time dashboard** over WebSocket (with polling fallback): jobs, queues, workers, workflows with a live SVG DAG view.
- **Observability**: Prometheus `/metrics`, OpenTelemetry traces, structured slog, request IDs.

## Benchmark

Measured end-to-end on the development machine (2 vCPU Intel Xeon, PostgreSQL 17.4 + Redis 8.0.2 on localhost, worker concurrency 8, echo handler, 20s runs):

| Path | Throughput | Amortized latency |
|---|---|---|
| **End-to-end** (submit → dispatch → execute → finalize) | **~1,700–1,950 jobs/sec** | ~520–590 µs/job |
| Submit path (PG insert + Redis XADD) | ~1,900 jobs/sec | ~515 µs/op |
| REST `POST /v1/jobs` (full middleware + validation) | ~1,300–1,450 req/sec | ~700–780 µs/op |

The dispatch pipeline adds ~75–140 µs per job over the raw submit path (claim + execute + finalize + hook). The REST layer adds ~180–260 µs of JSON/validation/middleware overhead per request. Numbers are single-node; scale horizontally with more workers — state lives in PG/Redis, workers are stateless.

Reproduce with:

```bash
make bench   # go test ./internal/loadtest/ -bench=. -benchtime=30s
```

## Quick start

```bash
# 1. Start PostgreSQL + Redis (or point env vars at your own)
docker compose up -d postgres redis   # or: make run for the full stack

# 2. Run the API server (serves REST :8080, gRPC :9090, dashboard, metrics)
CONDUIT_PG_DSN="postgres://conduit:conduit@localhost:5432/conduit?sslmode=disable" \
CONDUIT_REDIS_ADDR=localhost:6379 \
go run ./cmd/server

# 3. Run workers (in another terminal)
go run ./cmd/worker

# 4. Submit a job
curl -X POST localhost:8080/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"echo","payload":{"hello":"world"},"priority":2,"max_retries":3}'
```

The dashboard ships inside the server binary: open http://localhost:8080/ (build it first with `make dashboard`).

## Configuration

All settings are environment variables; defaults shown:

| Variable | Default | Purpose |
|---|---|---|
| `CONDUIT_HTTP_ADDR` | `:8080` | REST/WS/dashboard listener |
| `CONDUIT_GRPC_ADDR` | `:9090` | gRPC listener |
| `CONDUIT_PG_DSN` | `postgres://conduit:conduit@localhost:5432/conduit?sslmode=disable` | PostgreSQL |
| `CONDUIT_REDIS_ADDR` | `localhost:6379` | Redis |
| `CONDUIT_WORKER_CONCURRENCY` | `8` | Per-worker job slots (1–512) |
| `CONDUIT_JOB_TIMEOUT` | `10m` | Default execution budget per job |
| `CONDUIT_RETRY_BASE_DELAY` | `500ms` | Exponential backoff base |
| `CONDUIT_RETRY_MAX_DELAY` | `5m` | Backoff ceiling |
| `CONDUIT_VISIBILITY_TIMEOUT` | `60s` | Redis pending reclaim threshold |
| `CONDUIT_HEARTBEAT_INTERVAL` | `5s` | Worker liveness heartbeat |
| `CONDUIT_MAX_PAYLOAD_KB` | `256` | Payload size cap (1KB–16MB) |
| `CONDUIT_OTEL_ENDPOINT` | *(off)* | OpenTelemetry OTLP endpoint |

## API

REST (JSON) and gRPC (`conduit.v1.JobService`) expose the same operations; a Go SDK lives in `pkg/sdk`.

```
POST   /v1/jobs                     submit { type, payload, priority, max_retries,
                                      idempotency_key, delay_ms, timeout_ms }
GET    /v1/jobs?status=&type=&limit=
                                      cursor-paginated listing; responses carry
                                      next_cursor + next_before_id - pass BOTH
                                      back to page without skipping rows that
                                      share a timestamp
GET    /v1/jobs/{id}                  job detail
DELETE /v1/jobs/{id}                  cancel (queued/scheduled/retrying only)
POST   /v1/workflows                  create DAG definition (nodes may be
                                      declared in any order)
GET    /v1/workflows                  list definitions
GET    /v1/workflows/{id}             definition detail
POST   /v1/workflows/{id}/run         start a run
GET    /v1/workflows/{id}/runs        run history
POST   /v1/schedules                  create cron schedule
GET    /v1/schedules                  list schedules
DELETE /v1/schedules/{id}             delete schedule
GET    /v1/queues                     stream depths + pending + status counts
GET    /v1/workers                    live workers
GET    /v1/ws                         WebSocket event stream (same-origin
                                      browsers; SDKs/CLI without an Origin
                                      header are always allowed)
GET    /metrics                       Prometheus
GET    /healthz                       liveness (checks PG + Redis)
```

### Workflow example

```json
POST /v1/workflows
{
  "name": "order.fulfillment",
  "nodes": [
    { "id": "charge",  "job_type": "payment.charge" },
    { "id": "email",   "job_type": "email.send",   "depends_on": ["charge"] },
    { "id": "refund",  "job_type": "payment.refund", "depends_on": ["charge"],
      "run_if": { "node": "charge", "status": "failed" } },
    { "id": "audit",   "job_type": "audit.log",   "depends_on": ["email", "refund"] }
  ]
}
```

`run_if` selects branches by upstream status; nodes without a condition run when their dependencies succeed. Node failure fails dependents; `on_failure` nodes act as compensations.

## Architecture notes

- **Idempotency**: unique partial index on `idempotency_key`; duplicate submits return the original job with `duplicate: true` (HTTP 200 vs 201).
- **No lost updates**: every worker→PG status write is a guarded `UPDATE ... WHERE status = 'running'`; workflow runs use a `version` column with compare-and-swap plus in-engine retry.
- **Exactly the workers you bring**: workers register via heartbeats; the coordinator marks them dead after 2× missed beats, XAUTOCLAIMs their pending messages after the visibility timeout, and requeues orphaned running jobs.
- **Lease-bounded recovery**: stale-queued re-enqueue refreshes `updated_at`, so a backlogged job is re-enqueued at most once per stale window instead of every sweep.
- **Crash-safe scheduling**: cron occurrences fire under a deterministic idempotency key (`sched:<id>:<due>`), and every scheduler tick heals schedules orphaned by a crash mid-fire - a schedule can skip at most the occurrence it was firing when it died, never stop entirely.
- **Serial migrations**: all processes take a PostgreSQL advisory lock around auto-migration, so `docker compose up` can boot server, worker, and coordinator simultaneously without racing on `schema_migrations`.
- **WS safety**: the WebSocket route is intentionally outside the request-timeout middleware (hijacked connections manage their own deadlines), the middleware forwards `http.Hijacker`/`Flusher`, and cross-origin browser upgrades are rejected (CSWSRF hardening; non-browser clients unaffected).

## Repository layout

```
cmd/           server, worker, coordinator, cli binaries
internal/      api (REST+gRPC+WS), worker pool, workflow engine, scheduler,
               coordinator, queue (Redis Streams), storage (pgx), retry,
               metrics, observability, config, model, testutil, loadtest
pkg/sdk/       Go client + worker runtime for embedding
pkg/api/       generated protobuf code (source of truth: api/proto)
migrations/    embedded SQL migrations (auto-applied at startup)
dashboard/     React + Vite dashboard
examples/      email, image-resize, webhook worker examples
deploy/        k8s manifests
```

## Testing

Integration tests run against real PostgreSQL and Redis (no mocks):

```bash
make test    # go test -p 1 ./...      (sequential: shared test database)
make race    # go test -p 1 -race ./...
```

Coverage includes: submit/claim/finish CAS paths, retry/backoff/dead-letter, panic pills, permanent errors, per-job timeouts, graceful drain, heartbeats, workflow DAG + conditional branches + CAS conflict convergence + recovery, cron firing/double-fire prevention, coordinator leadership/reaping/reconciliation, REST/gRPC/WS (including a regression test that the WS upgrade survives the production middleware chain), and the SDK.

## License

MIT
