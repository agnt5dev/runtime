# AGNT5 runtime

`agnt5-runtime` is the single-process SQL profile of the shared AGNT5
runtime. It supports PostgreSQL, a local SQLite file, and hosted Turso over
libSQL. It embeds migrations, journal processing, projections, pull-worker
coordination, a minimal HTTP gateway, health and metrics endpoints, tracing,
lease and timer recovery, and graceful shutdown.

This profile deliberately excludes RocksDB, object storage, managed
replication, control-plane, and fleet dependencies.

## Operational model

- One process owns one logical `AGNT5_RUNTIME_NAMESPACE`.
- PostgreSQL holds a namespace-scoped advisory lock on a dedicated session.
  SQLite and Turso use a renewable namespace lease based on database time.
  A second writer for the same namespace fails at startup.
- Partition-local journal offsets and append idempotency are allocated in one
  database transaction. SQLite/libSQL uses short immediate write
  transactions; remote transactions contain no network calls other than SQL.
- The configured database is the durable correctness mechanism. Notifications
  and process-local wakeups are latency hints only.
- Readiness stays false until storage is reachable and projection replay has
  reached the journal tail observed at startup.
- Restart resumes from the durable projection checkpoint.
- The PostgreSQL pool uses `AGNT5_POSTGRES_MAX_CONNECTIONS` sessions. The
  writer lock uses one additional dedicated session.
- Loss of the writer authority fails the runtime closed. SQLite/Turso stop when
  a lease renewal fails; PostgreSQL checks its advisory-lock session every 2
  seconds and exits when the session is gone or a check takes over 2 seconds.
- The profile is single-active, not HA; do not run active/active replicas
  against one namespace.

The initial profile is single-active and single-partition. It is not the
managed HA, archive, cold-tier, replication, control-plane, or fleet profile.

## Build

From the repository root:

```bash
cargo build --release -p agnt5-runtime
```

The resulting binary is:

```text
target/release/agnt5-runtime
```

## Published image

The image is named `agnt5-runtime`, and so is the binary inside it.

Each change to `main` publishes it, built the same way, as the Linux x86-64
`agnt5-runtime` container image (`.github/workflows/image.yml`):

```text
ghcr.io/agnt5dev/agnt5-runtime:sha-<full commit SHA>   # pin this
ghcr.io/agnt5dev/agnt5-runtime:main                     # latest main, moves
```

SDK test suites pin a `sha-` tag, so a runtime change reaches an SDK only when
that SDK moves its pin. To run the binary directly, as the SDK test fixtures
do, copy it out of the image and check it against the checksum stored beside
it:

```bash
IMAGE=ghcr.io/agnt5dev/agnt5-runtime:sha-<commit>
container=$(docker create "$IMAGE")
docker cp "$container:/usr/local/bin/agnt5-runtime" .
docker cp "$container:/usr/local/share/agnt5/agnt5-runtime.sha256" .
docker rm "$container"
sha256sum -c agnt5-runtime.sha256
export AGNT5_RUNTIME_BIN="$PWD/agnt5-runtime"
```

The image runs as a non-root user and listens on `127.0.0.1` like the binary.
To reach it from outside the container, set `AGNT5_HTTP_ADDR` and
`AGNT5_GRPC_ADDR` to `0.0.0.0` addresses.

## Configure and run

PostgreSQL remains the default for backwards compatibility:

```bash
export AGNT5_DATABASE_URL='postgresql://agnt5:password@127.0.0.1:5432/agnt5'
export AGNT5_RUNTIME_NAMESPACE='local'
export AGNT5_PROJECT_ID='local-project'
export AGNT5_DEPLOYMENT_ID='local-deployment'
export AGNT5_WORKSPACE_ID='local-workspace'
export AGNT5_API_KEY='replace-me'

cargo run -p agnt5-runtime
```

For a zero-service local runtime, select SQLite and provide a file path:

```bash
export AGNT5_DATABASE_BACKEND='sqlite'
export AGNT5_SQLITE_PATH='./agnt5-runtime.db'
export AGNT5_RUNTIME_NAMESPACE='local'

cargo run -p agnt5-runtime
```

For Turso, use the database URL and token issued by Turso:

```bash
export AGNT5_DATABASE_BACKEND='turso'
export AGNT5_DATABASE_URL='libsql://your-database.turso.io'
export AGNT5_DATABASE_AUTH_TOKEN='replace-with-a-database-token'
export AGNT5_RUNTIME_NAMESPACE='production'

cargo run -p agnt5-runtime
```

`TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are accepted as fallbacks for the
two AGNT5 database settings. Keep the token in a secret store; it is hidden in
CLI diagnostics and must never be committed.

Default listeners:

- HTTP: `127.0.0.1:34185`
- pull-worker gRPC: `127.0.0.1:34186`

Use `AGNT5_HTTP_ADDR` and `AGNT5_GRPC_ADDR` to change them. Listener values are
socket addresses such as `0.0.0.0:34185`.

Apply the embedded forward-only migrations and verify writer authority without
starting the servers:

```bash
cargo run -p agnt5-runtime -- --migrate-only
```

The full configuration surface is available from:

```bash
cargo run -p agnt5-runtime -- --help
```

Important operational settings include:

| Setting | Default | Meaning |
| --- | --- | --- |
| `AGNT5_DATABASE_BACKEND` | `postgres` | `postgres`, `sqlite`, or `turso` |
| `AGNT5_DATABASE_URL` | none | Required PostgreSQL or Turso URL |
| `AGNT5_DATABASE_AUTH_TOKEN` | none | Required Turso token |
| `AGNT5_SQLITE_PATH` | `./agnt5-runtime.db` | Local SQLite database file |
| `AGNT5_RUNTIME_NAMESPACE` | `agnt5-oss` | Storage and writer-authority namespace |
| `AGNT5_PROJECT_ID` | `default` | Only project accepted by this process |
| `AGNT5_DEPLOYMENT_ID` | `local` | Deployment lane used for submissions |
| `AGNT5_POSTGRES_MAX_CONNECTIONS` | `20` | Pool sessions, excluding the writer lock |
| `AGNT5_WRITER_LEASE_SECS` | `15` | SQLite/Turso writer-lease duration |
| `AGNT5_WRITER_HEARTBEAT_SECS` | `5` | SQLite/Turso lease renewal cadence |
| `AGNT5_CHECKPOINT_INTERVAL` | `1` | Projected records between checkpoints |
| `AGNT5_RUN_TIMEOUT_SECS` | `300` | Synchronous HTTP run/stream timeout |
| `AGNT5_CACHE_MAX_BYTES` | `268435456` | In-memory journal cache bound |
| `AGNT5_LEASE_MAX_ATTEMPTS` | `3` | Attempts before expired work is dead-lettered |
| `AGNT5_WORKER_SESSION_SIGNING_KEY` | public development key | Signs worker sessions and lease fences. Required, and must not be a public default, when either listener is beyond `127.0.0.1` |

## Health, metrics, and HTTP API

Liveness, readiness, and Prometheus metrics are public:

```text
GET /livez
GET /readyz
GET /metrics
```

The protected minimal gateway supports:

```text
POST /v1/{functions|workflows}/{component}/run
POST /v1/{functions|workflows}/{component}/submit
POST /v1/{functions|workflows}/{component}/stream
GET  /v1/runs
GET  /v1/runs/{run_id}
GET  /v1/runs/{run_id}/events
GET  /v1/status/{run_id}
GET  /v1/result/{run_id}
POST /v1/runs/{run_id}/{cancel|suspend|resume}
```

For example:

```bash
curl -fsS \
  -H 'X-API-Key: replace-me' \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://127.0.0.1:34185/v1/functions/ks_noop/run
```

The runtime refuses to start with either listener beyond `127.0.0.1` unless at
least one of `AGNT5_API_KEY` or `AGNT5_JWT_SECRET` is set.
The runtime refuses to start with either listener beyond `127.0.0.1` unless
`AGNT5_WORKER_SESSION_SIGNING_KEY` holds a secret of your own, for example one
from `openssl rand -hex 32`. The API key uses `X-API-Key`.
Bearer tokens use HS256, require a future `exp`, honor `nbf`, and must carry a
`project_id` claim naming the configured project.

## Pull workers

Run current Python, TypeScript, or Go SDK workers in pull mode against the gRPC
listener. Set both endpoints to the runtime's gRPC address:

```bash
export AGNT5_PROJECT_ID='local-project'
export AGNT5_DEPLOYMENT_ID='local-deployment'
export AGNT5_WORKER_MODE='pull'
export AGNT5_COORDINATOR_ENDPOINT='http://127.0.0.1:34186'
export AGNT5_ENGINE_URL='http://127.0.0.1:34186'
```

`AGNT5_ENGINE_URL` is required. Workers poll for runs and write journal events,
checkpoints and live updates through `EngineService`, which checks each write
against the worker's lease and session. Without it, the SDKs fall back to an
older execution service this runtime does not serve.

Use the HTTP API for run submission, status, results, cancellation,
suspension, and resume.

## Verification

Local SQLite/libSQL conformance and crash recovery require no external
service:

```bash
cargo test -p engine-libsql -- --test-threads=1
cargo test -p agnt5-runtime
```

With a PostgreSQL test database available at `AGNT5_POSTGRES_TEST_URL`:

```bash
cargo test -p engine-postgres --test journal_conformance -- --ignored --test-threads=1
cargo test -p engine-postgres --test crash_recovery -- --ignored --test-threads=1
cargo test -p agnt5-runtime
```

Remote Turso transport can be verified with a dedicated test database by
running the same binary with `AGNT5_DATABASE_BACKEND=turso`; never point
conformance or destructive namespace-cleanup tests at production data.

