# AGNT5 Runtime

AGNT5 Runtime is the open-source, single-node runtime for durable agentic
workflows. It is distributed as one `agnt5-runtime` binary and stores all
durable state in PostgreSQL.

## Product boundary

The community runtime is intentionally:

- one AGNT5 process backed by PostgreSQL;
- single-tenant and single-active-runtime;
- authenticated with JWT, without roles or permissions;
- compatible with every supported AGNT5 SDK;
- observable through OpenTelemetry.

High availability, replication, object-store archival, fleet management,
managed deployments, and eval services are not part of this repository.

## Repository layout

```text
crates/core         Stable runtime contracts and domain types
crates/postgres     PostgreSQL journal and materialized store
crates/processor    Journal processing and workflow projections
crates/coordinator  Worker sessions, polling, leases, and completion
crates/gateway      HTTP/gRPC APIs and JWT authentication
crates/telemetry    OpenTelemetry configuration
runtime             The only shipped binary
migrations          PostgreSQL migrations
tests               Backend conformance and crash-recovery tests
deploy              Docker Compose and packaging
```

Library crates are internal code boundaries. Users deploy only the
`agnt5-runtime` executable and PostgreSQL.

## Development

```bash
cargo fmt --check
cargo test --workspace
AGNT5_TEST_DATABASE_URL=postgres://agnt5:agnt5@localhost:5432/agnt5 \
  cargo test -p agnt5-postgres --features integration-tests --test postgres
AGNT5_DATABASE_URL=postgres://agnt5:agnt5@localhost:5432/agnt5 \
  cargo run -p agnt5-runtime
```

The binary exposes the SDK worker protocol over gRPC on port `34180` and the
submission API over HTTP on port `34181` by default. Override the listeners
with `AGNT5_GRPC_LISTEN` and `AGNT5_HTTP_LISTEN`.

The first workflow slice implements:

- `POST /v1/{component_type}/{component}/submit`
- `GET /v1/status/{run_id}`
- `GET /v1/result/{run_id}`
- worker registration, polling, lease renewal, and completion through
  `api.v1.EngineService`

Authentication is not enabled in this first slice. JWT validation is the next
gateway boundary; it will remain authentication-only and will not introduce
roles or permissions.

GitHub Actions caches Cargo dependencies and build outputs between runs. The
container build uses `cargo-chef` plus BuildKit's GitHub Actions cache so source
changes do not rebuild the full Rust dependency graph.

## Protocol releases

The canonical public schema is `proto/agnt5/protocol/v2`. After a protocol
change is merged to `main`, create and push an annotated tag whose version
matches the workspace version:

```bash
git tag -a protocol/v0.1.0-alpha.1 -m "Protocol v0.1.0-alpha.1"
git push origin protocol/v0.1.0-alpha.1
```

The protocol release workflow publishes the matching `agnt5-proto` crate,
creates `gen/go/v0.1.0-alpha.1`, and publishes a canonical descriptor set plus
its SHA-256 digest. See `proto/README.md` for bootstrap and trusted-publishing
configuration.
