# Pull Completion Lifecycle V1

Status: runtime contract

Capability: `pull_completion_lifecycle_v1`

A pull worker that finishes a non-streaming job normally appends `run.started`,
`<component>.started`, the handler's journal events, and
`<component>.completed|failed` as separate durable RPCs before it calls
`CompleteJob`. None of those records gate the handler's execution: the
memoization and fencing boundary is the checkpoint and activation RPCs, and the
terminal decision is the lease fence on the terminal record
(`docs/contracts/run-lease-fence-v1.md`). This contract lets a worker deliver
those bookkeeping records inside `CompleteJob` so a simple function costs
`PollJob` plus `CompleteJob`.

Worked sequences for each run shape, including the cases that do not bundle,
are in `docs/contracts/pull-lifecycle-bundling-scenarios.md`.

## Negotiation

The runtime advertises `pull_completion_lifecycle_v1` in
`RegisterWorkerSessionResponse.supported_protocol_capabilities`. It is never in
the runtime's required set: a production profile requires only the durability
capabilities. A worker folds lifecycle records only while the session that
holds the job negotiated the capability. A session that re-registers and loses
it flushes any held records inline before completing. Workers should expose a
kill switch (`AGNT5_PULL_COMPLETION_LIFECYCLE=disabled`) that stops advertising
the capability.

An SDK that never sets `lifecycle_records` behaves exactly as before. A
runtime that does not advertise the capability never receives the field.

## Wire surface

The authoritative schema is `protos/proto/api/v1/engine.proto`.

- `CompleteJobRequest.lifecycle_records` (`repeated Record`, field 12): the
  deferred records in emission order.
- `CompleteJobResponse.lifecycle_written_count` (`uint32`, field 2): records
  newly written by this call.

## What may be carried

Every record must belong to the completing project and run and be a
worker-owned, non-terminal lifecycle or handler event. This includes the
`session.created` bookkeeping event emitted by hosted agents to materialize
conversation sessions, under the same project/run scope and completion lease
fence. This exception does not admit `session.updated` or other session
mutations. The runtime rejects the whole request, appending nothing, when a
record is:

- terminal (`run.completed`, `run.cancelled`, a terminal `run.failed`);
- any other `run.*` transition except `run.started`;
- `workflow.paused` or `workflow.state.changed`;
- an activation record (metadata `activation_contract=durable_activation_v2`),
  which has its own RPCs and is rejected from bundles and from the public
  append paths;
- runtime-internal (`run.archived`, `run.terminalizing`, dispatch retry);
- outside the caps: 32 records or 256 KiB per request, or the journal's
  per-record size limit;
- timestamped before the previous record in the batch, or more than 60 s ahead
  of the runtime's receipt time.

Caller-supplied lease metadata and every reserved `agnt5_fence_*` key are
stripped. The runtime stamps `deployment_id`, component identity, `worker_id`,
`lease_id`, `lease_attempt`, `dispatch_mode=pull`,
`pull_completion_lifecycle=1`, and the record's index in the batch. Source
timestamps are preserved so `started_at`, `duration_ms`, and the pull latency
histograms stay truthful; they are observed at completion rather than at
start.

The `JobAssignment` metadata key `assignment_commit_offset` is
transport-authored authority context. SDKs must preserve it on direct
lifecycle appends and must not allow event metadata to override it. The
runtime uses the offset to read the committed `run.assigned` record when the
run projection still says `queued`; dropping it can incorrectly reject the
first `run.started` during normal projection lag.

## Ordering and atomicity

`CompleteJob` appends **one batch**: the validated lifecycle records in
emission order, then the lease-fenced terminal. The segment decides the
terminal's fence inside that write:

- fence wins: every record is written, lifecycle records at lower offsets
  than the terminal, so the late-lifecycle drop after a terminal never
  applies to them and every projection sees `run.started` before
  `run.completed`;
- fence loses (`LeaseFenceMismatch`, gRPC `FAILED_PRECONDITION`): nothing is
  written, the lifecycle records included. A fenced-out completion leaves no
  trace of the attempt in the journal.

The call returns as soon as the append is durable under the active storage
profile. There is no processor round trip and no publication wait; the
projection observes the records by tailing the log.

The runtime-authored `run.completed`, `run.failed`, or pause record preserves
the correlation id of the latest `run.started`: first from the carried bundle,
then from the run's record cache, and finally from durable run history when a
mixed-version worker appended the start separately. A legacy history with no
correlated start falls back to the run id. This keeps the start and
runtime-authored outcome in one observable lifecycle rather than creating a
synthetic completion span. Current bundled completions add no read round trip;
only the mixed-version cache-miss path performs the durable lookup.

## Idempotency

Each carried record gets a slot keyed by project, run, lease id, lease
attempt, event type, correlation id, and batch index
(`idempotency/pull.completion_lifecycle/v1`). The terminal gets its own slot
keyed by project, run, lease id, attempt, and event type
(`idempotency/run.lease_fence/v1`) with a fingerprint over its payload.

A `CompleteJob` retried after a lost acknowledgement resolves every record,
terminal included, to its earlier offset and writes nothing;
`lifecycle_written_count` is 0. The slots are checked before the fence, so
the retry never sees the lease its first attempt released. A retry that
carries a different outcome or payload for the same lease attempt is
`FAILED_PRECONDITION`. Because the bundle and the terminal are one write,
there is no partially-applied state to reconcile.

The runtime stamps `pull_completion_bundle_digest` on the fenced terminal. It
is a SHA-256 digest of the complete ordered lifecycle bundle and participates
in the terminal's strict idempotency fingerprint. Adding, removing, reordering,
or changing lifecycle records on a retry is therefore `FAILED_PRECONDITION`;
no newly presented lifecycle record is appended after an accepted terminal.

## What stays sequential

Step checkpoints, `BeginActivation`/`CompleteActivation`/`SuspendActivation`,
entity state, approvals, signals, and timers keep their own RPCs. A worker
must flush any held lifecycle records as a normal append before any of those
durable writes for the same run, so journal order still reflects execution
order. Streaming invocations keep per-event delivery. HITL resume still emits
no `run.started`.

A pause delivered through `CompleteJob` (`completion_event_type` of
`run.paused` or `workflow.paused`) is fenced like a terminal, releases the
lease, and leaves the run gate open for the resumed attempt. The run state
machine accepts `assigned -> paused` so a folded pause on a run that never
recorded `run.started` is a legal transition.

## Mixed versions and rollback

Old SDK, new runtime: identical behavior. New SDK, old runtime: the capability
is not advertised, so the SDK never holds records. Rolling a runtime back
while new SDKs are connected forces sessions to re-register without the
capability; the SDK treats that as a hold release and flushes inline.
