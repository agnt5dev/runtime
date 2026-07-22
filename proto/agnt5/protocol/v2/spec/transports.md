# Transport contract

This document is normative for gRPC, runtime HTTP/JSON/SSE, payload transfer,
and endpoint HTTP in `agnt5.protocol.v2` minor version 0.

## ProtoJSON

HTTP bodies use the protobuf JSON mapping with original snake_case field names.
Unknown fields are rejected. Enums use symbolic names, `bytes` use standard
base64, timestamps use RFC 3339, durations use protobuf duration strings, and
64-bit integers use decimal strings. Request bodies are UTF-8 JSON.

`Content-Type` is `application/json` for unary requests and responses.
Successful create/command operations return `200` unless the route below states
otherwise. Authentication uses transport headers; project, deployment, tenant,
storage-provider, role, and permission identities MUST NOT be added to public
message bodies.

## Runtime HTTP routes

The canonical HTTP projection is:

| Method and path | Protobuf operation |
| --- | --- |
| `POST /agnt5/v2/capabilities:get` | `ProtocolService.GetCapabilities` |
| `POST /agnt5/v2/events` | `EventService.PublishEvent` |
| `POST /agnt5/v2/runs` | `ExecutionService.StartRun` |
| `GET /agnt5/v2/runs/{run_id}` | `ExecutionService.GetRun` |
| `GET /agnt5/v2/runs/{run_id}/outcome` | `ExecutionService.GetRunOutcome` |
| `POST /agnt5/v2/runs/{run_id}:cancel` | `ExecutionService.CancelRun` |
| `POST /agnt5/v2/runs/{run_id}:signal` | `ExecutionService.SendSignal` |
| `POST /agnt5/v2/runs/{run_id}/waits/{wait_id}:resolve` | `ExecutionService.ResolveWait` |
| `GET /agnt5/v2/runs/{run_id}/events` | `ExecutionService.StreamRunEvents` |
| `GET /agnt5/v2/runs/{run_id}/output` | `ExecutionService.StreamRunOutput` |

Path values populate the corresponding request fields. Remaining request fields
come from the JSON body for `POST` operations and query parameters for `GET`
streams. `request_id`, `signal_id`, and `resolution_id` remain required even
when the HTTP client also supplies an `Idempotency-Key` header.

## SSE

Event and output streams use `text/event-stream`. Each frame contains:

```text
id: <opaque runtime cursor>
event: run.event | run.output
data: <single-line ProtoJSON StreamRunEventsResponse or StreamRunOutputResponse>
```

`Last-Event-ID` is equivalent to `after_cursor`; specifying both with different
values is invalid. Servers send heartbeats as SSE comments and MUST NOT invent
application events. A terminal run closes its durable event stream after all
accepted events are delivered. Live output MAY close earlier when retention or
producer lifetime ends.

## Structured errors

The mapping in `error-mapping.json` is authoritative. HTTP failures use that
status and a ProtoJSON-encoded `ProtocolError` body. `retryable` and
`retry_after` are explicit instructions for the failed call, not permission to
repeat a fenced operation with stale authority.

gRPC uses the mapped canonical status code and message. When structured details
are available, the server includes the binary protobuf encoding of
`ProtocolError` in trailing metadata key `agnt5-protocol-error-bin`. SDKs MUST
preserve an unknown error code and MUST NOT remap authentication or invalid
requests into availability errors.

## Payload transfer

`Payload.inline_data` is used up to `maximum_inline_payload_bytes`. Larger
values use `PayloadService`. A `PayloadReference.token` is an opaque bearer
capability scoped to the authenticated application boundary and payload bytes.
It MUST be sent only over authenticated TLS, MUST NOT be logged, and MUST NOT be
interpreted or converted into a provider URL.

For `PutPayload`, the first stream frame is metadata and later frames are
contiguous chunks starting at offset zero. The runtime verifies declared size
and SHA-256 before returning a reference. An interrupted upload is uncommitted;
the caller retries the full stream with the same request ID.

For `GetPayload`, the first response is metadata and later frames cover the
requested range contiguously. Expired references return
`PAYLOAD_REFERENCE_EXPIRED`. References embedded in durable run state MUST
remain resolvable for at least the retention of that state, or the runtime MUST
materialize the referenced bytes before accepting the durable record.

The canonical HTTP projection is:

| Method and path | Behavior |
| --- | --- |
| `POST /agnt5/v2/payloads` | Raw request bytes; metadata in `X-AGNT5-Payload-*` headers; returns ProtoJSON `PutPayloadResponse`. |
| `POST /agnt5/v2/payloads:resolve` | ProtoJSON `GetPayloadRequest`; returns raw bytes with canonical payload metadata headers. |

Required upload headers are `X-AGNT5-Request-ID` and
`X-AGNT5-Payload-SHA256` (lowercase hexadecimal). Optional headers are
`Content-Type`, `Content-Encoding`, and `X-AGNT5-Payload-TTL-Millis`.

## Serverless endpoint HTTP

Endpoints expose:

```text
GET  /.well-known/agnt5
POST /agnt5/invoke
```

The manifest GET is unsigned. An invocation uses the exact manifest revision
against which the runtime prepared it. A mismatch returns
`MANIFEST_REVISION_MISMATCH`; the runtime rediscovers before retrying.

The initial signature scheme is `agnt5-http-hmac-sha256.v1`. These headers are
required:

- `X-AGNT5-Signature-Version: agnt5-http-hmac-sha256.v1`
- `X-AGNT5-Timestamp: <Unix epoch milliseconds>`
- `X-AGNT5-Execution-ID: <InvokeEndpointRequest.execution_id>`
- `X-AGNT5-Signature: sha256=<lowercase hexadecimal digest>`

The signing input is the byte concatenation:

```text
timestamp + "." + execution_id + "." + exact_raw_request_body
```

The digest is HMAC-SHA-256 with the UTF-8 signing secret. Implementations use a
constant-time comparison, require the header and body execution IDs to match,
and accept at most five minutes of clock skew. A repeated identical invocation
is permitted because transport retries are expected; durable side effects stay
behind runtime-issued operation and execution fences.

The legacy `workerless-hmac-sha256.v1` scheme and `X-AGNT5-Attempt-ID` header
are not aliases for this scheme. An endpoint may advertise both during
migration and must verify each according to its own rules.

An endpoint with requested runtime-owned operations normally omits `outcome` so
the runtime can apply them and reinvoke with replay results. A suspended outcome
may accompany its matching wait-registration operation and is accepted
atomically. SDK-owned completed operations may accompany a terminal or yielded
outcome. Other combinations are invalid.
