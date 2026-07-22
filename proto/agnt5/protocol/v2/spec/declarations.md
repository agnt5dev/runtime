# Capabilities and declarations

This document is normative for capability negotiation, component descriptors,
triggers, schedules, and run policy in `agnt5.protocol.v2` minor version 0.

## Capability negotiation

Capability names and versions come from `capabilities.json`. Names are
case-sensitive. A request MUST NOT contain the same name twice. For each
requirement, a compatible runtime returns a capability with the same name and a
version greater than or equal to `minimum_version`.

If a required capability cannot be satisfied, negotiation fails with
`MISSING_CAPABILITY`. An unsupported optional capability is omitted. The
response lists every selected capability, including capabilities the runtime
requires clients to understand for the selected transport.

The runtime selects an inclusive protocol version supported by both parties.
An empty or inverted protocol range is `INVALID_REQUEST`. Every field in
`ProtocolLimits` returned by a selected runtime or endpoint MUST be positive;
zero does not mean unlimited. A runtime rejects requests above a negotiated
limit before partially applying them.

## Component identity and schemas

Component identity is `(type, name, version)`. Every value is required for
registration and targeting. Versions are opaque, case-sensitive strings. The
wire protocol has no implicit `latest`; SDK conveniences resolve a concrete
version before starting a run.

Schema bytes are UTF-8 JSON Schema documents in the descriptor's dialect. The
portable dialect is `https://json-schema.org/draft/2020-12/schema`. Invalid JSON
or a different dialect without a negotiated capability rejects registration.

Descriptor equivalence ignores protobuf map order and the ordering of methods
and triggers after they are keyed by unique `name` and `trigger_id`. JSON schema
documents are compared after RFC 8785 JSON canonicalization. Other repeated
policy entries are compared as unordered semantic entries. Conflicting
descriptors for the same identity return `COMPONENT_DESCRIPTOR_CONFLICT`.

`FUNCTION`, `WORKFLOW`, `AGENT`, `TOOL`, and `SCORER` use the component-level
input and output schemas. An `ENTITY` declares every callable method in
`methods`; its target includes both `method` and `instance_key`. Duplicate or
empty method names are invalid.

## Event triggers

`EventService.PublishEvent` is the portable ingestion boundary. `event_id`
deduplicates within the authenticated application boundary. Reusing it with an
equivalent envelope returns `ALREADY_APPLIED`; different content is a conflict.
Acceptance means the event is durable, not that every matching run has started.

An event expression evaluates against the ProtoJSON form of `TriggerEvent`.
The filter returns a boolean. `input_mapping` returns a JSON value satisfying
the component input schema. A missing mapping passes `payload` through.

When `batch_window` is present, `maximum_batch_size` is positive. Matching
events are ordered by runtime receipt order and the batch closes when the
window or size limit is reached. The mapping receives an array of event
envelopes. Without a mapping, the run input is an array of payload values.
Each batch has a stable idempotency identity derived from component version,
trigger ID, first event ID, and window boundary.

The delay expression returns a non-negative protobuf-duration string. Invalid
expression output rejects that activation without corrupting or dropping the
source event; the runtime records a diagnostic event.

## Schedules

Cron uses the declared versioned format and IANA time zone. A nonexistent local
time during a daylight-saving transition is skipped. A repeated local time
produces two occurrences because the underlying UTC instants differ. Interval
schedules require a non-zero `anchor_time` and a positive interval.

Occurrence identity derives from component version, trigger ID, and scheduled
UTC instant. Restart and re-registration MUST NOT change it. Misfire policy is:

- `SKIP`: ignore missed occurrences.
- `FIRE_ONCE`: create one run representing the missed range.
- `CATCH_UP`: create distinct occurrence runs up to `maximum_catch_up`.

`UNSPECIFIED` uses the runtime-advertised default and is included in the
effective descriptor diagnostics. Runs still pass through normal idempotency,
rate, concurrency, and permit admission.

## Run policy

Policy entries are component-authored declarations. Callers cannot weaken them.
The runtime combines them with operational limits and delivers the resolved
effective policy plus an opaque digest bound to the execution fence.

An input JSON pointer must resolve to a scalar. Effective policy contains the
canonical scalar `value`, encoded as a string: JSON strings use their value,
numbers use their RFC 8785 form, booleans use `true` or `false`, and null is
`null`.

A permit pool must exist when the component registers. Missing pools reject the
descriptor with `INVALID_REQUEST` and a `pool` detail. Concurrency and permits
are held only during active execution. Rate-limited runs remain durably accepted
and queued. Admission ordering is stable by eligibility instant and run ID; a
runtime MAY provide a different fairness capability only when negotiated.
