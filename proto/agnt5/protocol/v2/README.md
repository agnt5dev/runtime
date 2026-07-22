# AGNT5 public protocol v2

`agnt5.protocol.v2` is the compatibility boundary between AGNT5 runtimes,
SDKs, persistent workers, and serverless endpoints. It contains application
execution semantics only. Community administration lives in
`agnt5.runtime.v1`; managed placement, tenancy, replication, quota, archival,
and fleet contracts are outside this module.

The package is pre-stable. The protobuf files are authoritative for wire shape;
the documents and registries in `spec/` are authoritative for behavior that
protobuf cannot express. The words MUST, MUST NOT, SHOULD, and MAY are used as
described by RFC 2119.

## Protocol selection

`ProtocolVersion.major` is `2` for this package. Minor version `0` is the
initial behavioral contract. A client supplies an inclusive minimum and
maximum version. The runtime selects exactly one mutually supported version;
it MUST NOT select a version outside the requested range.

SDKs expose `auto`, `v1`, and `v2` modes. In `auto`, a v1 fallback is permitted
only when the v2 capability method is explicitly unimplemented or the runtime
selects v1. Authentication, authorization, invalid-request, timeout, and
network failures MUST NOT trigger fallback. Once selected, the protocol is
pinned for the worker session and every execution issued through it.

## Contract areas

- `capabilities.proto` negotiates behavior and interoperability limits.
- `component.proto`, `execution_options.proto`, `run_policy.proto`, and
  `trigger.proto` declare portable components.
- `execution.proto` controls runs and exposes durable events and bounded live
  output.
- `worker.proto` and `dispatch.proto` define pull delivery, fencing, and
  outcome commit.
- `durable.proto` and `state.proto` define replayable operations and revisioned
  application state.
- `payload.proto` transfers values larger than the negotiated inline limit.
- `endpoint.proto` maps the same execution model onto signed HTTP endpoints.
- `errors.proto` supplies structured protocol-state errors.

Component and method schema documents use the dialect URI declared by
`ComponentDescriptor.schema_dialect`. Portable v2 uses JSON Schema draft
2020-12. Registered component versions and invocation targets MUST be explicit,
non-empty versions; there is no wire-level `latest` alias.

## Normative specifications

- [Lifecycle, fencing, idempotency, waits, and replay](spec/lifecycle.md)
- [Capabilities, components, triggers, and run policy](spec/declarations.md)
- [gRPC, HTTP, SSE, errors, payload transfer, and endpoint signing](spec/transports.md)
- [Protocol releases and SDK synchronization](spec/sdk-synchronization.md)
- [Capability registry](spec/capabilities.json)
- [Error mapping](spec/error-mapping.json)
- [Compatibility metadata](spec/compatibility.json)
- [SDK protocol-lock schema](spec/protocol-lock.schema.json)

The conformance fixtures under `tests/conformance/v2/fixtures` are part of a
protocol release. SDK implementations MUST validate them before claiming
support for the corresponding capability version.
