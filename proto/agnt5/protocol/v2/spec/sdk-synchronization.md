# SDK synchronization and release contract

The runtime repository owns `agnt5.protocol.v2`. SDK repositories consume a
released artifact; they do not copy or independently edit v2 protobuf sources.

## Release projections

One protocol release version publishes:

- `agnt5-proto` on crates.io for Rust and `sdk-core`;
- `github.com/agnt5dev/runtime/gen/go` at the matching nested Go tag;
- a canonical `FileDescriptorSet` and SHA-256;
- `compatibility.json`, `capabilities.json`, and `error-mapping.json`;
- conformance fixtures and signing vectors; and
- a generated protocol lock matching `protocol-lock.schema.json`.

Python and TypeScript persistent workers consume v2 through a released
`agnt5-sdk-core`. Go consumes the released Go module directly. Pure Python,
TypeScript/Cloudflare, and Go endpoint implementations validate the same JSON
and signing fixtures.

## Protocol lock

Each SDK repository commits the lock downloaded from the protocol release. CI
verifies its schema, descriptor digest, package versions, and fixture digests.
Updating the lock is an explicit dependency-update pull request and never
publishes the SDK automatically.

SDK package versions remain independent. Each changelog states its supported
wire range and protocol artifact version. A package exposes its supported and
selected protocol versions in diagnostics.

## Dual-stack modes

SDKs expose `auto`, `v1`, and `v2` through a language API and
`AGNT5_PROTOCOL_MODE`. Explicit API configuration overrides the environment.
In early releases, `auto` prefers v1 unless runtime rollout policy selects v2.
After the cutover gate, `auto` prefers the highest mutually supported version.
Forced v1 remains the rollback path; forced v2 fails rather than falling back.

The runtime and SDK pin the selected protocol to a worker session. Endpoint
invocations pin both protocol version and manifest revision. A run MUST NOT
start under one protocol and commit under another.

## Required compatibility matrix

Every protocol-consuming SDK release tests:

1. old v1 SDK against a dual runtime;
2. dual SDK forced to v1 against a dual runtime;
3. dual SDK forced to v2 against a dual runtime;
4. dual SDK in auto mode against a v1-only runtime;
5. current SDK against the latest released runtime;
6. latest released SDK against the current runtime; and
7. current and previous endpoint signing/manifest versions during migration.

Schema/unit checks do not replace customer-path tests. Release candidates run
cross-SDK conformance, runtime crash recovery, and a deployed kitchen-sink
smoke using packaged Python, TypeScript, and Go SDKs.

## Repository ownership

- `runtime`: schema, behavior specification, artifacts, server conformance.
- `sdk-core`: shared Rust persistent-worker transport and v1/v2 adapter.
- `sdk-python`: Python APIs, native-core dependency, pure ASGI endpoint.
- `sdk-typescript`: TypeScript APIs, native-core dependency, Node/Cloudflare endpoint.
- `sdk-go`: direct Go worker/client transport and Go endpoint.
- `sdk-integrations`: dependency and compatibility validation only.

Each public SDK repository owns its CI, changelog, tag, and package publication.
The runtime release may notify or open dependency-update work, but it MUST NOT
centrally publish an SDK.
