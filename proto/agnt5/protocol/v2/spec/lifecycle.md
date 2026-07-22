# Lifecycle and durability semantics

This document is normative for `agnt5.protocol.v2` minor version 0.

## Run state machine

The application-visible state machine is:

| Current | Command or event | Next |
| --- | --- | --- |
| absent | accepted `StartRun` | `QUEUED` |
| `QUEUED` | fenced worker or endpoint execution begins | `RUNNING` |
| `RUNNING` | retryable execution failure | `RETRYING` |
| `RETRYING` | retry delay and policy admission complete | `RUNNING` |
| `RUNNING` | registered wait is committed | `WAITING` |
| `WAITING` | timer, signal, or user-input wait resolves | `QUEUED` |
| `RUNNING` | cooperative yield commits | `QUEUED` |
| non-terminal | cancellation wins the commit race | `CANCELLED` |
| `RUNNING` | successful terminal commit | `COMPLETED` |
| `RUNNING` | failure exhausts retry policy | `FAILED` |

Lease, delivery, payload-transfer, projection, and partition states are not
public run states. A runtime MAY project the next state asynchronously, but an
acknowledged command MUST already be durable and later reads MUST converge to
the acknowledged result.

## Idempotency scope

All idempotency keys are scoped to the authenticated application boundary.
Reusing a key with byte-for-byte equivalent semantic input returns the original
result. Reusing it with different semantic input returns a conflict.

| Operation | Idempotency identity |
| --- | --- |
| Start run | `StartRunRequest.request_id` |
| Cancel run | `CancelRunRequest.request_id` |
| Signal | `SendSignalRequest.signal_id` |
| Resolve wait | `ResolveWaitRequest.resolution_id` |
| Worker poll | worker-session generation plus `poll_id` |
| Durable operation | run, execution, `operation_id`, and `sequence` |
| Event append | run, execution, `event_id`, and `sequence` |
| Outcome commit | run, execution fence, and `commit_id` |
| Payload upload | authenticated subject plus `PutPayloadMetadata.request_id` |

Transport retries never increment the user-code attempt. The attempt increments
only when runtime retry policy begins a new execution slice.

## Worker sessions and execution fences

Registering the same `worker_id` creates a new monotonically increasing session
generation and immediately fences the previous session. Session expiration or
replacement invalidates its polls, renewals, durable-operation commits, event
appends, and outcome commits. Work from an invalidated session becomes eligible
for redelivery only after the current lease expires or the runtime durably
revokes it.

`execution_token` is authority for one execution slice. It binds the worker
session generation, run, execution identity, attempt, component incarnation,
expected run revision, effective-policy digest, and lease generation. It MUST
be treated as a secret and MUST NOT be logged. Successful renewal extends the
lease without rotating the token. Expiration, supersession, cancellation, or a
new lease generation invalidates it.

A result transaction is accepted only while its execution token is current.
If cancellation and completion race, the first durable fenced transaction wins.
The loser receives `EXECUTION_SUPERSEDED`, `STALE_EXECUTION_TOKEN`, or an
idempotent disposition as appropriate; it MUST NOT be translated into success.

## Event order and cursors

`DurableEvent.sequence` is local to one execution slice, starts at one, and is
strictly contiguous. An identical retry of an accepted `event_id` and sequence
is idempotent. Reusing either identity with different content is a conflict.

The runtime assigns a separate run-wide opaque cursor when it accepts an event.
`StreamRunEvents` is ordered by that cursor, including events from retries and
resumed executions. Clients MUST order by cursor rather than by the
execution-local sequence. An expired cursor returns `EVENT_CURSOR_EXPIRED`;
the runtime MUST NOT silently skip retained history.

`CommitRunOutcomeRequest.final_events` immediately follow previously appended
events in execution-local sequence. `expected_last_event_sequence`, when
present, is the highest sequence after the final events are included. The
runtime atomically verifies contiguity, appends final events, and commits the
outcome.

Live `RunOutputEvent` values are non-authoritative and have bounded retention.
They are ordered and deduplicated within `stream_id`. `StreamRunOutput` exposes
the runtime cursor; an expired cursor returns `OUTPUT_CURSOR_EXPIRED`.

## Durable operations and replay

Operation sequences start at one and are contiguous across the run's
deterministic operation history. They remain stable across transport retries,
user-code retries, suspension, and replay. A checkpoint records the highest
operation sequence it incorporates; replay resumes at the next sequence.
`ApplyDurableOperations` validates the complete batch before mutating state.
Runtime-owned operations are durably resolved in the same transaction. An
SDK-owned memoized step returns `execution_required` until its completion token
is committed. `CommitDurableOperationResults` is atomic for the supplied batch.

A completion token binds the execution fence, operation identity, sequence,
and operation digest. A retry with the identical result returns the canonical
recorded result. A different result is `COMMIT_CONFLICT`.

On replay, the SDK MUST consume results in sequence and MUST NOT execute an
operation whose canonical result is already durable. Changing an operation ID,
sequence, or semantic input changes the operation and may execute new work.

## Checkpoints, suspension, and waits

`RegisterWaitOperation` is the authoritative creation of a durable wait.
`RunSuspended.wait_condition` MUST exactly match the registered condition for
the same `wait_id`. A pull worker applies the wait operation before committing
the suspended outcome. An endpoint returns the requested operation and
suspended outcome together; the runtime accepts them in one fenced transaction.

`SaveCheckpointOperation` durably advances the resume checkpoint while an
execution is active. A suspended or yielded outcome carries the final checkpoint
for that slice. If a save operation occurred, the outcome checkpoint MUST match
the latest saved checkpoint; otherwise the commit is invalid. This prevents two
competing checkpoint authorities.

The runtime supplies `replay_results` only for durable operations after
`checkpoint_through_operation_sequence`. A checkpoint with watermark `N` and
the later results are sufficient to reconstruct execution; earlier results MAY
be compacted according to retention policy. An SDK MUST reject a checkpoint
whose watermark is ahead of the runtime's durable contiguous sequence.

Signals are durable inbox entries. A signal received before its wait is
registered remains available. A signal wait consumes the earliest unconsumed
matching signal by runtime receipt order. Signal identity is deduplicated by
`signal_id`.

A user-input resolution is accepted only for a registered unresolved wait.
Repeating the same `resolution_id` and value is idempotent. A different value or
resolution for an already resolved wait returns `WAIT_ALREADY_RESOLVED`.

Timer readiness uses runtime time. Re-registration or process restart MUST NOT
shift `ready_at`. Resolution, run requeue, and consumption of the wait happen in
one durable transition.

## Retry and policy enforcement

Workers recommend retry behavior through `Failure.retry_directive`; the runtime
is authoritative. A failed outcome transitions to `RETRYING` only when the
effective retry policy permits another attempt. Otherwise it transitions to
`FAILED`.

Concurrency, rate, and permit policy is evaluated before every execution slice.
Waiting, yielded, retry-backoff, and terminal runs do not consume concurrency or
permits. Policy-key JSON pointers must resolve to a scalar JSON value; a missing,
structured, or non-JSON value is `INVALID_REQUEST`.
