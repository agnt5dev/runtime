# AGNT5 runtime

The single-process runtime for [AGNT5](https://agnt5.com) workflows and agents. It runs your
functions, workflows and agents durably: each step is recorded in a journal, a crashed worker's
runs move to a healthy one, and a sleep or a pause for a person's answer survives a restart.

It is one binary, `agnt5-runtime`, backed by PostgreSQL, a local SQLite file, or Turso.
Workers written with the AGNT5 Python, TypeScript or Go SDK connect to it and pull work.

## Run it

The published image pins each build by commit:

```bash
docker run --rm -p 34185:34185 -p 34186:34186 \
  -e AGNT5_DATABASE_BACKEND=sqlite \
  -e AGNT5_HTTP_ADDR=0.0.0.0:34185 -e AGNT5_GRPC_ADDR=0.0.0.0:34186 \
  -e AGNT5_API_KEY=replace-me -e AGNT5_WORKER_SESSION_SIGNING_KEY=replace-me-too \
  ghcr.io/agnt5dev/agnt5-runtime:main
```

Listening beyond loopback requires an API key or JWT secret, and a signing key of your own.

Or build it from source:

```bash
cargo build --release -p agnt5-runtime
AGNT5_DATABASE_BACKEND=sqlite ./target/release/agnt5-runtime
```

Configuration, the HTTP API, and how to point SDK workers at the runtime are in
[`server/README.md`](server/README.md).

## What is here

| Path | What it is |
| --- | --- |
| `server/` | The `agnt5-runtime` binary: configuration, HTTP API, startup and shutdown |
| `crates/service/` | The engine service workers talk to: polling, leases, journal writes |
| `crates/processor/` | Projects the journal into run state, timers and dispatch |
| `crates/core/` | Shared types, the journal contract, and the storage traits |
| `crates/libsql/`, `crates/postgres/` | Storage: SQLite and Turso, and PostgreSQL |
| `crates/proto/`, `protos/` | The worker protocol |

## Test it

```bash
cargo test
```

The PostgreSQL storage tests also run against a database you provide:

```bash
AGNT5_POSTGRES_TEST_URL=postgresql://user:password@127.0.0.1:5432/db \
  cargo test -p engine-postgres -- --include-ignored --test-threads=1
```

## Contributing

This repository is published from AGNT5's main repository, where the hosted platform is also
built. Issues and pull requests are welcome here; accepted changes are carried over to the main
repository and come back in the next publish.

## License

[Apache-2.0](LICENSE).
