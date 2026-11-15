//! Run lifecycle projection.
//!
//! Materializes run state from journal events. Each event type updates
//! the Run proto message stored in the materialized store.

use engine_core::journal_contract::META_CONTINUATION_B64;
use engine_core::lease_fence::{
    lease_fence, resume_source_id, strip_reserved_fence_metadata, LeaseFenceKind,
    RUN_LEASE_RENEWED_EVENT, RUN_META_DISPATCH_GENERATION, RUN_META_LEASE_ATTEMPT,
    RUN_META_LEASE_SESSION_HASH,
};
use engine_core::partition_offset::decode_offset;
use engine_core::types::Record;
use engine_proto::{ModelUsage, Run};
use opentelemetry::KeyValue;
use prost::Message;
use tracing::warn;

use crate::metrics::processor_metrics;
use crate::projections::{ProjectionResult, StoreOp};
use crate::run_state_machine::{self, RunStatus};

const META_KEY_GATEWAY_RECEIVED_AT_NS: &str = "gateway_received_at_ns";
const WORKERLESS_OUTPUT_REF_METADATA_KEYS: &[&str] = &[
    "workerless_output_ref_kind",
    "workerless_output_ref",
    "workerless_output_size_bytes",
    "workerless_output_sha256",
    "workerless_output_content_type",
];

/// Step completion event types that trigger memoization.
/// Activation records carry their own memo through the activations
/// projection; only the legacy Checkpoint RPC still memoizes here.
const STEP_COMPLETION_EVENTS: &[&str] = &["checkpoint.created"];

/// LLM event types that carry token usage metadata in `record.metadata`.
const LM_EVENT_TYPES: &[&str] = &["lm.completed", "lm.call.completed", "lm.stream.completed"];

/// Project a record onto run state.
///
/// `existing_run` is the current serialized Run proto (None if first event for this run).
pub fn project(record: &Record, existing_run: Option<&[u8]>) -> ProjectionResult {
    let event_type = record.event_type.as_str();
    let mut ops = Vec::new();
    let existing_run_decoded = existing_run.and_then(|bytes| Run::decode(bytes).ok());
    let authorized_failed_retry = event_type == "run.queued"
        && existing_run_decoded
            .as_ref()
            .and_then(|run| {
                RunStatus::from_str(&run.status).map(|status| {
                    run_state_machine::is_authorized_failed_retry(
                        status,
                        run.attempt,
                        &record.metadata,
                    )
                })
            })
            .unwrap_or(false);

    // Step memo (independent of run state)
    if STEP_COMPLETION_EVENTS.contains(&event_type) {
        if let Some(sk) = record.step_key.as_ref() {
            ops.push(StoreOp::PutStepMemo {
                step_key: sk.clone(),
                data: record.data.to_vec(),
            });
        }
    }

    ops.push(StoreOp::PutEvent);

    // A terminal reservation is authority over one exact projected lifecycle
    // generation, not over a run ID forever. A completion, cancellation,
    // renewal, retry, or handoff that advanced the obligation before this
    // record reached the processor makes the reservation stale.
    if event_type == "run.terminalizing" {
        let expected = record
            .metadata
            .get(run_state_machine::TERMINAL_OBLIGATION_OFFSET_META)
            .and_then(|value| value.parse::<u64>().ok());
        let current = existing_run_decoded.as_ref().and_then(|run| {
            run.metadata
                .get(run_state_machine::TERMINAL_OBLIGATION_OFFSET_META)
                .and_then(|value| value.parse::<u64>().ok())
        });
        if expected.is_none() || expected != current {
            warn!(
                run_id = %record.run_id,
                expected_obligation_offset = ?expected,
                current_obligation_offset = ?current,
                "Ignoring stale terminal reservation"
            );
            return ProjectionResult { ops };
        }
    }

    // Supervisor-authored terminal outcomes must consume the reservation that
    // is currently projected. This closes the crash/handoff window between a
    // durable `run.terminalizing` record and its final outcome.
    if matches!(event_type, "run.completed" | "run.failed" | "run.cancelled") {
        if let Some(reservation_id) = record
            .metadata
            .get(run_state_machine::TERMINAL_RESERVATION_ID_META)
        {
            let reservation_matches = existing_run_decoded.as_ref().is_some_and(|run| {
                RunStatus::from_str(&run.status) == Some(RunStatus::Terminalizing)
                    && run
                        .metadata
                        .get(run_state_machine::TERMINAL_RESERVATION_ID_META)
                        == Some(reservation_id)
                    && record
                        .metadata
                        .get(run_state_machine::TERMINAL_OUTCOME_META)
                        .is_some_and(|outcome| {
                            run.metadata.get(run_state_machine::TERMINAL_OUTCOME_META)
                                == Some(outcome)
                        })
            });
            if !reservation_matches {
                warn!(
                    run_id = %record.run_id,
                    "Ignoring terminal outcome without the current reservation"
                );
                return ProjectionResult { ops };
            }
        }
    }

    // `run.archived` is a side-band ack event emitted
    // by the active partition owner after a successful S3 PUT. Every
    // replica projects it and deletes the matching outbox entry. The
    // event doesn't mutate Run state (Run already reached terminal
    // status before archival started), so we short-circuit out of the
    // run-state machine and emit only the outbox delete + PutEvent.
    //
    // Required record fields (set by the active owner at append time):
    //   metadata["partition_id"]   = u32 as decimal string
    //   metadata["terminal_offset"] = u64 as decimal string
    //   metadata["parquet_filename"] (optional, audit only)
    //   metadata["archived_at_ms"]  (optional, audit only)
    if event_type == "run.archived" {
        let partition_id = record
            .metadata
            .get("partition_id")
            .and_then(|s| s.parse::<u32>().ok());
        let terminal_offset = record
            .metadata
            .get("terminal_offset")
            .and_then(|s| s.parse::<u64>().ok());
        match (partition_id, terminal_offset) {
            (Some(partition_id), Some(terminal_offset)) => {
                ops.push(StoreOp::DeleteRunArchiveIntent {
                    partition_id,
                    project_id: record.project_id.clone(),
                    terminal_offset,
                    run_id: record.run_id.clone(),
                });
            }
            _ => {
                warn!(
                    run_id = %record.run_id,
                    "run.archived missing partition_id/terminal_offset metadata; outbox entry stays"
                );
            }
        }
        // Keep the hot-index entry until the retention worker strictly reads
        // and verifies the cold Run. The store removes CF_RUNS and its exact
        // hot-index key in the same compare-and-delete batch. The list merge
        // deduplicates the intentional hot/cold overlap.

        // Stamp the archive Parquet path into
        // `CF_RUNS_ARCHIVE` so the cold-`get_run` fallback can find
        // this run's row when CF_RUNS no longer holds it. The
        // drainer fills `parquet_filename` with the path it just
        // wrote (`engine/runs/.../runs-<node>-<uuidv7>.parquet`).
        // Missing metadata leaves no cold-read pointer. The current code still
        // emits a retention entry below, so the retention worker must be
        // hardened before it can safely delete this run's `CF_RUNS` row.
        // Store the *partition directory* (everything up to the last `/`)
        // rather than the specific `runs-<uuid>.parquet` filename.
        // The compaction scheduler can later merge N small files into
        // one `compacted-<uuid>.parquet` in the same directory — if we
        // kept the original filename, the pointer would go stale once
        // compaction deletes the originals. The reader appends `/*.parquet`
        // and uses a glob, which finds whichever file(s) currently hold
        // the run regardless of how many compaction passes have run.
        let archive_generation = match record.metadata.get("parquet_filename") {
            Some(path) if !path.is_empty() => {
                let partition_dir = path.rfind('/').map(|i| &path[..i]).unwrap_or(path);
                ops.push(StoreOp::PutRunArchiveLocation {
                    project_id: record.project_id.clone(),
                    run_id: record.run_id.clone(),
                    archive_location: partition_dir.to_string(),
                });
                Some(path.clone())
            }
            _ => {
                warn!(
                    run_id = %record.run_id,
                    "run.archived missing parquet_filename metadata; cold-get_run pointer not written"
                );
                None
            }
        };

        // Only a successful archive publication carries both an exact object
        // generation and a positive archive timestamp. Missing or malformed
        // provenance is fail-closed: preserve the hot row and emit no
        // retention entry.
        let archived_at_ms = record
            .metadata
            .get("archived_at_ms")
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|timestamp| *timestamp > 0 && *timestamp < i64::MAX);
        if let (Some(archive_generation), Some(archived_at_ms)) =
            (archive_generation, archived_at_ms)
        {
            ops.push(StoreOp::PutRunRetention {
                project_id: record.project_id.clone(),
                run_id: record.run_id.clone(),
                archived_at_ms,
                archive_generation,
            });
            crate::metrics::processor_metrics()
                .cf_runs_retention_writes
                .add(1, &[]);
        } else {
            warn!(
                run_id = %record.run_id,
                "run.archived lacks valid cold provenance; hot run retention not scheduled"
            );
        }
        return ProjectionResult { ops };
    }

    // Vortex publication schedules hot retention only after the durable
    // receipt has been journaled. Keep legacy ack projection above so an
    // upgrade can still replay historical records.
    if event_type == "run.query_archived" {
        let partition_id = record
            .metadata
            .get("partition_id")
            .and_then(|value| value.parse::<u32>().ok());
        let terminal_offset = record
            .metadata
            .get("terminal_offset")
            .and_then(|value| value.parse::<u64>().ok());
        match (partition_id, terminal_offset) {
            (Some(partition_id), Some(terminal_offset)) => {
                ops.push(StoreOp::DeleteRunQueryArchiveIntent {
                    partition_id,
                    project_id: record.project_id.clone(),
                    terminal_offset,
                    run_id: record.run_id.clone(),
                });
                ops.push(StoreOp::PutRunQueryArchiveReceipt {
                    project_id: record.project_id.clone(),
                    run_id: record.run_id.clone(),
                    row_version: terminal_offset,
                    receipt_data: record.data.to_vec(),
                });
                ops.push(StoreOp::DeleteRunArchiveIntent {
                    partition_id,
                    project_id: record.project_id.clone(),
                    terminal_offset,
                    run_id: record.run_id.clone(),
                });
                let archived_at_ms = record.timestamp_ns / 1_000_000;
                if archived_at_ms > 0 {
                    let directory = format!("query-lake/{}/{}", record.project_id, record.run_id);
                    ops.push(StoreOp::PutRunArchiveLocation {
                        project_id: record.project_id.clone(),
                        run_id: record.run_id.clone(),
                        archive_location: directory.clone(),
                    });
                    ops.push(StoreOp::PutRunRetention {
                        project_id: record.project_id.clone(),
                        run_id: record.run_id.clone(),
                        archived_at_ms,
                        archive_generation: format!("{directory}/{terminal_offset}"),
                    });
                    crate::metrics::processor_metrics()
                        .cf_runs_retention_writes
                        .add(1, &[]);
                }
            }
            _ => {
                warn!(
                    run_id = %record.run_id,
                    "run.query_archived missing partition_id/terminal_offset metadata; shadow intent stays"
                );
            }
        }
        return ProjectionResult { ops };
    }

    // Validate transition legality. Most illegal transitions remain
    // observe-only, but once a run is terminal, delayed duplicate/replayed
    // lifecycle events must not regress the materialized state.
    let mut skip_mutation_due_to_same_status_duplicate = false;
    let mut skip_mutation_due_to_terminal_regression = false;
    let mut skip_mutation_due_to_late_assigned_after_progress = false;
    let mut skip_mutation_due_to_stale_assignment_generation = false;
    if event_type == "run.assigned" {
        if let (Some(existing), Some(assigned_attempt)) = (
            existing_run_decoded.as_ref(),
            record
                .metadata
                .get(RUN_META_LEASE_ATTEMPT)
                .and_then(|value| value.parse::<u32>().ok()),
        ) {
            if assigned_attempt != existing.attempt {
                warn!(
                    run_id = %record.run_id,
                    assigned_attempt,
                    current_attempt = existing.attempt,
                    "Ignoring stale run assignment generation"
                );
                processor_metrics().illegal_transitions.add(
                    1,
                    &[
                        KeyValue::new("from", existing.status.clone()),
                        KeyValue::new("to", RunStatus::Assigned.as_str()),
                        KeyValue::new("event_type", event_type.to_string()),
                        KeyValue::new("reason", "stale_assignment_attempt"),
                    ],
                );
                skip_mutation_due_to_stale_assignment_generation = true;
            }
        }
    }
    if let Some(target) = run_state_machine::record_target_status(event_type, &record.metadata) {
        if let Some(existing_bytes) = existing_run {
            if let Ok(existing) = Run::decode(existing_bytes) {
                if let Some(current) = RunStatus::from_str(&existing.status) {
                    if !run_state_machine::can_transition(current, target)
                        && !authorized_failed_retry
                    {
                        // `run.assigned` is written by the processor after
                        // the coordinator accepts a fire-and-forget dispatch.
                        // A fast worker can append `run.started` or
                        // `workflow.paused` before that assigned record lands.
                        // Treat that as an ordering race, not a producer
                        // state-machine bug.
                        if is_late_assigned_after_worker_progress(event_type, current) {
                            if current != RunStatus::Started {
                                skip_mutation_due_to_late_assigned_after_progress = true;
                            }
                        } else {
                            // Producer-identification signals: the sweeper tags its
                            // dead-letter records with `dead_letter_reason` in the
                            // record metadata, and both the dispatch-error path
                            // and SDK failure emits carry `error_code` /
                            // `error_message` in the record payload.
                            let dead_letter_reason = record
                                .metadata
                                .get("dead_letter_reason")
                                .map(String::as_str)
                                .unwrap_or("");
                            let (error_code, error_message) = parse_error_fields(&record.data);
                            warn!(
                                run_id = %record.run_id,
                                from = %current,
                                to = %target,
                                event_type = %event_type,
                                dead_letter_reason = dead_letter_reason,
                                error_code = error_code.as_str(),
                                error_message = truncate_for_log(&error_message, 200),
                                "Illegal run state transition detected"
                            );
                            // Split the illegal-transition counter by producer
                            // signal so dashboards can identify which code path
                            // is writing the duplicate terminal.
                            let reason_label = if !dead_letter_reason.is_empty() {
                                dead_letter_reason.to_string()
                            } else if !error_code.is_empty() {
                                error_code.clone()
                            } else {
                                "unknown".to_string()
                            };
                            processor_metrics().illegal_transitions.add(
                                1,
                                &[
                                    KeyValue::new("from", current.as_str()),
                                    KeyValue::new("to", target.as_str()),
                                    KeyValue::new("event_type", event_type.to_string()),
                                    KeyValue::new("reason", reason_label),
                                ],
                            );
                            // Same-status duplicate/regression: record the event
                            // (journal is the source of truth and duplicates
                            // should be auditable) but skip the run-proto mutation
                            // so the existing lifecycle fields and lease fence are
                            // preserved intact. Also skip downstream lease-index
                            // and pending-index diffs; the first apply already
                            // moved the indexes.
                            if current == target {
                                skip_mutation_due_to_same_status_duplicate = true;
                            } else if current.is_terminal() {
                                skip_mutation_due_to_terminal_regression = true;
                            }
                        }
                    }
                }
            }
        } else if target != RunStatus::Queued {
            // Lifecycle event for an unknown run (no prior run.queued).
            warn!(
                run_id = %record.run_id,
                event_type = %event_type,
                "Run lifecycle event without prior run.queued"
            );
        }
    }

    if skip_mutation_due_to_same_status_duplicate
        || skip_mutation_due_to_terminal_regression
        || skip_mutation_due_to_late_assigned_after_progress
        || skip_mutation_due_to_stale_assignment_generation
    {
        // Duplicate and stale assignments follow a dispatch that already
        // pinned the run; only a late one can be its first dispatch.
        if skip_mutation_due_to_late_assigned_after_progress {
            if let Some(data) = pin_late_assignment(record, existing_run_decoded.as_ref()) {
                ops.push(StoreOp::PutRun(data));
            }
        }
        return ProjectionResult { ops };
    }

    // Run state mutation
    let mut run_data = match event_type {
        "run.queued" => Some(apply_queued(record, existing_run, authorized_failed_retry)),
        "run.assigned" => Some(apply_assigned(record, existing_run)),
        "run.started" => Some(apply_started(record, existing_run)),
        "run.completed" => Some(apply_completed(record, existing_run)),
        "run.failed" => Some(apply_failed(record, existing_run)),
        "run.paused" | "workflow.paused" => Some(apply_paused(record, existing_run)),
        "run.resumed" => Some(apply_resumed(record, existing_run)),
        "run.cancelled" => Some(apply_cancelled(record, existing_run)),
        "run.terminalizing" => Some(apply_terminalizing(record, existing_run)),
        "run.lease_expired" => Some(apply_lease_expired(record, existing_run)),
        "timer.scheduled" => apply_timer_scheduled(record, existing_run),
        RUN_LEASE_RENEWED_EVENT => apply_lease_renewed(record, existing_run),
        et if LM_EVENT_TYPES.contains(&et) => apply_model_usage(record, existing_run),
        _ => None,
    };
    // An observed run's span events also move its settle state, which the
    // terminal supervisor reads to end the run.
    if engine_core::journal_contract::is_observed_record(record) && !event_type.starts_with("run.")
    {
        if let Some(updated) = apply_observed_span(record, run_data.as_deref(), existing_run) {
            run_data = Some(updated);
        }
    }

    if run_state_machine::record_target_status(event_type, &record.metadata).is_some() {
        if let Some(data) = run_data.as_mut() {
            stamp_terminal_obligation_position(data, record);
        }
    }

    if let Some(ref data) = run_data {
        ops.push(StoreOp::PutRun(data.clone()));
        if let Ok(run) = Run::decode(data.as_slice()) {
            let (partition_id, _) = decode_offset(record.offset);
            match RunStatus::from_str(&run.status) {
                Some(
                    RunStatus::Queued
                    | RunStatus::Assigned
                    | RunStatus::Started
                    | RunStatus::RetryDelayed
                    | RunStatus::Terminalizing,
                ) => ops.push(StoreOp::PutTerminalObligation {
                    partition_id: partition_id as u32,
                    run_data: data.clone(),
                }),
                Some(RunStatus::Paused)
                    if run
                        .metadata
                        .contains_key(run_state_machine::WAIT_DEADLINE_MS_META) =>
                {
                    ops.push(StoreOp::PutTerminalObligation {
                        partition_id: partition_id as u32,
                        run_data: data.clone(),
                    })
                }
                Some(
                    RunStatus::Paused
                    | RunStatus::Completed
                    | RunStatus::Failed
                    | RunStatus::Cancelled,
                ) => ops.push(StoreOp::DeleteTerminalObligation {
                    partition_id: partition_id as u32,
                }),
                None => {}
            }
        }

        // On terminal-status transitions, write a
        // durable outbox entry atomically with PutRun. Every replica
        // does this so the active partition owner — current or future,
        // post-handover — can drain its slice without journal replay.
        // Same-terminal duplicates short-circuited above and never
        // reach this path, so the outbox isn't double-written.
        if matches!(event_type, "run.completed" | "run.cancelled")
            || (event_type == "run.failed"
                && run_state_machine::next_retry_attempt(&record.metadata).is_none())
        {
            let (partition_id, local_offset) = decode_offset(record.offset);
            ops.push(StoreOp::PutRunQueryArchiveIntent {
                partition_id: partition_id as u32,
                terminal_offset: local_offset,
                run_data: data.clone(),
            });
        }

        // Insert the hot-index entry on `run.queued`
        // (atomic with PutRun). The index is the local read-model for
        // the runs list — every replica maintains it by tailing the
        // journal, so any node can serve list queries from local state
        // without partition-leader fanout.
        //
        // We read workspace_id and submitted_at_ms from the FRESHLY-
        // PROJECTED Run proto (`data`), not from `record.metadata`,
        // because `apply_queued` already extracted both into the Run
        // and applied the canonicalisation (e.g. timestamp from
        // `record.timestamp_ns` if metadata is missing). This keeps
        // the index in sync with the materialised Run state.
        if event_type == "run.queued" {
            if let Ok(run) = Run::decode(data.as_slice()) {
                let submitted_at_ms = run
                    .submitted_at
                    .as_ref()
                    .map(|t| t.seconds * 1000 + (t.nanos as i64) / 1_000_000)
                    .unwrap_or_else(|| record.timestamp_ns / 1_000_000);
                if !run.workspace_id.is_empty() {
                    ops.push(StoreOp::PutRunHotIndex {
                        workspace_id: run.workspace_id,
                        submitted_at_ms,
                    });
                } else {
                    // Defensive: a queued event with no workspace_id is
                    // malformed (the gateway always stamps it). Log
                    // rather than emit a key-with-empty-prefix that
                    // would become orphaned across the workspace
                    // delete on archival.
                    warn!(
                        run_id = %record.run_id,
                        "run.queued missing workspace_id; hot-index entry skipped"
                    );
                }
            } else {
                warn!(
                    run_id = %record.run_id,
                    "could not decode just-projected Run proto; hot-index entry skipped"
                );
            }
        }
    }

    // --- Lease secondary index ops (diff-driven) ---
    //
    // Compare lease state between the existing Run (if any) and the new one
    // produced by the apply_* helpers. Emit Put/Remove LeaseIndex ops for
    // any transition. This handles every path uniformly:
    //   - run.assigned with lease metadata  → new lease  → PutLeaseIndex
    //   - run.completed/failed/cancelled     → old cleared → RemoveLeaseIndex
    //   - run.lease_expired                  → old cleared → RemoveLeaseIndex
    //   - run.started / component events    → unchanged  → nothing emitted
    //
    // Replay-safe: the StoreOps are idempotent, and unchanged leases emit
    // nothing rather than redundant writes.
    let old_lease = existing_run_decoded.as_ref().and_then(|r| {
        if r.lease_id.is_empty() {
            None
        } else {
            Some((r.lease_expires_at_ms, r.lease_worker_id.clone()))
        }
    });
    let new_lease = if let Some(bytes) = run_data.as_ref() {
        Run::decode(bytes.as_slice()).ok().and_then(|r| {
            if r.lease_id.is_empty() {
                None
            } else {
                Some((r.lease_expires_at_ms, r.lease_worker_id.clone()))
            }
        })
    } else {
        // No Run mutation means the lease is unchanged. Function, activation,
        // and checkpoint events must not remove the indexes of a live lease.
        old_lease.clone()
    };

    if let Some((old_exp, old_worker)) = &old_lease {
        let should_remove = match &new_lease {
            Some((new_exp, new_worker)) => new_exp != old_exp || new_worker != old_worker,
            None => true,
        };
        if should_remove {
            ops.push(StoreOp::RemoveLeaseIndex {
                expires_at_ms: *old_exp,
                worker_id: old_worker.clone(),
            });
        }
    }
    if let Some((new_exp, new_worker)) = new_lease {
        let should_insert = match &old_lease {
            Some((old_exp, old_worker)) => new_exp != *old_exp || new_worker != *old_worker,
            None => true,
        };
        if should_insert {
            ops.push(StoreOp::PutLeaseIndex {
                expires_at_ms: new_exp,
                worker_id: new_worker,
            });
        }
    }

    // Pending runs index: deployment-scoped workers, batch runs, and managed
    // AI prompt runs are pull-based. Add to index on run.queued;
    // remove on terminal events.
    let component_name = record
        .metadata
        .get("component_name")
        .cloned()
        .or_else(|| {
            existing_run_decoded
                .as_ref()
                .map(|run| run.component_name.clone())
        })
        .unwrap_or_default();
    let pull_claimable = is_pull_claimable(record, existing_run_decoded.as_ref());

    // Decode the NEW Run once — needed for priority-index dual-writes below.
    let new_run_decoded = run_data
        .as_ref()
        .and_then(|bytes| Run::decode(bytes.as_slice()).ok());
    let old_status = existing_run_decoded
        .as_ref()
        .and_then(|run| RunStatus::from_str(&run.status));
    let new_status = new_run_decoded
        .as_ref()
        .and_then(|run| RunStatus::from_str(&run.status));
    let should_increment_queue_counter =
        new_status == Some(RunStatus::Queued) && old_status != Some(RunStatus::Queued);
    let should_decrement_queue_counter =
        old_status == Some(RunStatus::Queued) && new_status != Some(RunStatus::Queued);

    if event_type == "run.queued" && pull_claimable && !component_name.is_empty() {
        if let Some(ref data) = run_data {
            ops.push(StoreOp::PutPendingRun {
                component_name: component_name.clone(),
                data: data.clone(),
            });
            // Shadow into both priority-aware indexes while retaining the
            // legacy pending entry for compatibility.
            if let Some(run) = new_run_decoded.as_ref() {
                ops.push(pending_priority_put(run, record.timestamp_ns));
                ops.push(pending_deployment_priority_put(run, record.timestamp_ns));
                if should_increment_queue_counter {
                    ops.push(queue_counter_delta(run, record.timestamp_ns, 1));
                }
            }
        }
    }

    // Lease expiry re-queues the run — pending index must be re-populated so
    // pollers/schedulers can pick it up again. Applies regardless of
    // batch-vs-non-batch because all dispatch is lease-backed.
    // Same logic for run.resumed: the run transitions back to "queued" and
    // pull-based pollers must see it; push-based dispatch is also driven by
    // the orchestrator's evaluate_resumed in parallel.
    if matches!(event_type, "run.lease_expired" | "run.resumed") && !component_name.is_empty() {
        if let Some(ref data) = run_data {
            ops.push(StoreOp::PutPendingRun {
                component_name: component_name.clone(),
                data: data.clone(),
            });
            if let Some(run) = new_run_decoded.as_ref() {
                ops.push(pending_priority_put(run, record.timestamp_ns));
                ops.push(pending_deployment_priority_put(run, record.timestamp_ns));
                if should_increment_queue_counter {
                    ops.push(queue_counter_delta(run, record.timestamp_ns, 1));
                }
            }
        }
    }

    // Remove from pending index when run reaches any terminal or active state
    if matches!(
        event_type,
        "run.assigned"
            | "run.started"
            | "run.paused"
            | "workflow.paused"
            | "run.completed"
            | "run.failed"
            | "run.cancelled"
            | "run.terminalizing"
    ) && pull_claimable
        && !component_name.is_empty()
    {
        ops.push(StoreOp::RemovePendingRun {
            component_name: component_name.clone(),
        });
        // Remove from the priority-aware CF using the OLD run's immutable key
        // fields (deployment/component/priority/submitted_at don't change
        // after run.queued, so the Put and Remove keys always match).
        if let Some(run) = existing_run_decoded.as_ref() {
            ops.push(pending_priority_remove(run, record.timestamp_ns));
            ops.push(pending_deployment_priority_remove(run, record.timestamp_ns));
            if should_decrement_queue_counter {
                ops.push(queue_counter_delta(run, record.timestamp_ns, -1));
            }
        }
    }

    ProjectionResult { ops }
}

/// Canonicalize `Priority` for the index key.
///
/// Older records may omit `priority`, so the proto default is
/// `PRIORITY_UNSPECIFIED = 0`. Treat that as `PRIORITY_NORMAL = 2` so the
/// indexes always receive a deterministic bucket.
fn canonical_priority(run: &Run) -> u8 {
    match run.priority {
        0 => 2, // UNSPECIFIED → NORMAL
        p => p as u8,
    }
}

/// Derive `(ready_at_ms, created_at_ms)` for the priority index.
///
/// Source of truth is the run's `submitted_at` timestamp, which is set in
/// `apply_queued` and preserved across transitions. Falls back to the current
/// record's `timestamp_ns` if the Run somehow lacks a submitted_at (this
/// should only happen on corrupt state, but better than panicking).
///
/// `ready_at_ms` tracks when the run is eligible to run. The current projection
/// sets it to `created_at_ms`, so newly queued entries are immediately ready.
fn pending_priority_times(run: &Run, fallback_ts_ns: i64) -> (i64, i64) {
    let created_at_ms = match run.submitted_at.as_ref() {
        Some(ts) => ts.seconds * 1_000 + (ts.nanos as i64) / 1_000_000,
        None => fallback_ts_ns / 1_000_000,
    };
    let ready_at_ms = run
        .metadata
        .get(run_state_machine::READY_AT_MS_META)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(created_at_ms)
        .max(created_at_ms);
    (ready_at_ms, created_at_ms)
}

fn is_pull_claimable(record: &Record, existing_run: Option<&Run>) -> bool {
    let is_deployment_scoped = record
        .metadata
        .get("deployment_id")
        .map(|deployment_id| !deployment_id.is_empty())
        .unwrap_or(false)
        || existing_run
            .map(|run| !run.deployment_id.is_empty())
            .unwrap_or(false);

    let is_batch = record.metadata.contains_key("batch_id")
        || existing_run
            .map(|run| run.metadata.contains_key("batch_id"))
            .unwrap_or(false);

    is_deployment_scoped || is_batch
}

fn pending_priority_put(run: &Run, fallback_ts_ns: i64) -> StoreOp {
    let (ready_at_ms, created_at_ms) = pending_priority_times(run, fallback_ts_ns);
    StoreOp::PutPendingPriority {
        deployment_id: run.deployment_id.clone(),
        component_type: engine_core::demand::component_type_from_run_label(&run.component_type)
            .map(|component_type| component_type as i32)
            .unwrap_or_default(),
        component_name: run.component_name.clone(),
        priority: canonical_priority(run),
        ready_at_ms,
        created_at_ms,
    }
}

fn pending_priority_remove(run: &Run, fallback_ts_ns: i64) -> StoreOp {
    let (ready_at_ms, created_at_ms) = pending_priority_times(run, fallback_ts_ns);
    StoreOp::RemovePendingPriority {
        deployment_id: run.deployment_id.clone(),
        component_type: engine_core::demand::component_type_from_run_label(&run.component_type)
            .map(|component_type| component_type as i32)
            .unwrap_or_default(),
        component_name: run.component_name.clone(),
        priority: canonical_priority(run),
        ready_at_ms,
        created_at_ms,
    }
}

fn pending_deployment_priority_put(run: &Run, fallback_ts_ns: i64) -> StoreOp {
    let (ready_at_ms, created_at_ms) = pending_priority_times(run, fallback_ts_ns);
    StoreOp::PutPendingDeploymentPriority {
        deployment_id: run.deployment_id.clone(),
        priority: canonical_priority(run),
        ready_at_ms,
        created_at_ms,
    }
}

fn pending_deployment_priority_remove(run: &Run, fallback_ts_ns: i64) -> StoreOp {
    let (ready_at_ms, created_at_ms) = pending_priority_times(run, fallback_ts_ns);
    StoreOp::RemovePendingDeploymentPriority {
        deployment_id: run.deployment_id.clone(),
        priority: canonical_priority(run),
        ready_at_ms,
        created_at_ms,
    }
}

fn queue_counter_delta(run: &Run, fallback_ts_ns: i64, delta: i64) -> StoreOp {
    let (ready_at_ms, created_at_ms) = pending_priority_times(run, fallback_ts_ns);
    StoreOp::UpdateQueueCounters {
        deployment_id: run.deployment_id.clone(),
        priority: canonical_priority(run),
        ready_at_ms,
        created_at_ms,
        delta,
    }
}

fn is_late_assigned_after_worker_progress(event_type: &str, current: RunStatus) -> bool {
    event_type == "run.assigned"
        && matches!(
            current,
            RunStatus::Started
                | RunStatus::Paused
                | RunStatus::Completed
                | RunStatus::Failed
                | RunStatus::Cancelled
        )
}

fn load_or_default(existing: Option<&[u8]>, record: &Record) -> Run {
    existing
        .and_then(|bytes| Run::decode(bytes).ok())
        .unwrap_or_else(|| Run {
            project_id: record.project_id.clone(),
            run_id: record.run_id.clone(),
            ..Default::default()
        })
}

fn stamp_dispatch_generation(run: &mut Run, record: &Record) {
    run.metadata.insert(
        RUN_META_DISPATCH_GENERATION.to_string(),
        engine_core::dispatch_generation::DispatchGeneration::from_journal_offset(record.offset)
            .into_string(),
    );
}

fn apply_queued(
    record: &Record,
    existing: Option<&[u8]>,
    authorized_failed_retry: bool,
) -> Vec<u8> {
    let mut run = if authorized_failed_retry {
        let mut run = load_or_default(existing, record);
        run.status = "queued".to_string();
        run.attempt = record
            .metadata
            .get("attempt")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(run.attempt);
        run.error_message.clear();
        run.error_code.clear();
        run.output_data.clear();
        run.assigned_at = None;
        run.started_at = None;
        run.completed_at = None;
        run.duration_ms = 0;
        clear_lease_fields(&mut run);
        run
    } else {
        Run {
            project_id: record.project_id.clone(),
            run_id: record.run_id.clone(),
            status: "queued".to_string(),
            submitted_at: Some(ns_to_timestamp(record.timestamp_ns)),
            gateway_received_at: metadata_timestamp_ns(record, META_KEY_GATEWAY_RECEIVED_AT_NS)
                .map(ns_to_timestamp),
            attempt: record
                .metadata
                .get("attempt")
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(0),
            // A new `run.queued` resets the row, but the run was already
            // dispatched, so it keeps its pinned first dispatch and doesn't
            // bill a second start.
            first_dispatch_offset: existing
                .and_then(|bytes| Run::decode(bytes).ok())
                .and_then(|run| run.first_dispatch_offset),
            ..Default::default()
        }
    };

    // The termination history is the projection's own: a retry keeps the
    // run's, and a new run starts without one whatever its caller sent.
    let lease_history = authorized_failed_retry
        .then(|| {
            run.metadata
                .get(run_state_machine::LEASE_WORKER_TERMINATIONS_META)
                .cloned()
        })
        .flatten();
    // Preserve the original run metadata so pull-based dispatch can rebuild
    // the same execution context the gateway submitted.
    run.metadata = record.metadata.clone();
    run_state_machine::strip_runtime_lease_loss_metadata(&mut run.metadata);
    if let Some(history) = lease_history {
        run.metadata.insert(
            run_state_machine::LEASE_WORKER_TERMINATIONS_META.to_string(),
            history,
        );
    }
    stamp_dispatch_generation(&mut run, record);

    // Extract component info from metadata
    if let Some(v) = record.metadata.get("component_name") {
        run.component_name = v.clone();
    }
    if let Some(v) = record.metadata.get("component_type") {
        run.component_type = v.clone();
    }
    if let Some(v) = record.metadata.get("deployment_id") {
        run.deployment_id = v.clone();
    }
    if let Some(v) = record.metadata.get("workspace_id") {
        run.workspace_id = v.clone();
    }
    // Priority is stamped by the gateway per endpoint.
    // Stored as the i32 discriminant; `canonical_priority` downstream still
    // maps UNSPECIFIED → NORMAL if this tag is missing on older records.
    if let Some(p) = record
        .metadata
        .get("priority")
        .and_then(|v| v.parse::<i32>().ok())
    {
        run.priority = p;
    }
    // Extract `Run.trace_id` from the W3C
    // `traceparent` header (`version-trace_id-span_id-flags`) the
    // gateway stamps in `ensure_traceparent` (component.rs:503).
    //
    // The legacy path read a literal `trace_id` metadata key that
    // nothing actually populates — the OTel propagator only emits
    // `traceparent` (and optionally `tracestate`), so `run.trace_id`
    // was empty on every run produced through the standard handler.
    //
    // span_id and trace flags survive in `run.metadata` itself
    // (preserved by `run.metadata = record.metadata.clone()` and
    // round-tripped to cold via D6's `metadata_json`), so consumers
    // that need them can re-parse the traceparent without us
    // duplicating fields onto the proto.
    if let Some(tp) = record.metadata.get("traceparent") {
        if let Some(trace_id) = parse_traceparent_trace_id(tp) {
            run.trace_id = trace_id;
        }
    }
    // Belt-and-suspenders: honor an explicit `trace_id` metadata key
    // if a caller stamps one (it's not in the W3C contract but a few
    // legacy code paths emit it directly).
    if let Some(v) = record.metadata.get("trace_id") {
        if !v.is_empty() {
            run.trace_id = v.clone();
        }
    }

    // Preserve raw input for retry recovery. If the payload wraps input in
    // {"input_data": ...}, extract the inner value. Otherwise store the raw data.
    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(input) = payload.get("input_data") {
            if let Ok(input_bytes) = serde_json::to_vec(input) {
                run.input_data = input_bytes;
            }
        } else if !record.data.is_empty() {
            run.input_data = record.data.to_vec();
        }
    } else if !record.data.is_empty() {
        run.input_data = record.data.to_vec();
    }

    run.encode_to_vec()
}

fn apply_assigned(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    pin_first_dispatch(&mut run, record);
    if !matches!(
        RunStatus::from_str(&run.status),
        Some(RunStatus::Started)
            | Some(RunStatus::Paused)
            | Some(RunStatus::Completed)
            | Some(RunStatus::Failed)
            | Some(RunStatus::Cancelled)
    ) {
        run.status = "assigned".to_string();
        run.assigned_at = Some(ns_to_timestamp(record.timestamp_ns));
    }

    // Populate lease fields from record metadata when present. If the
    // dispatcher did not attach lease metadata, the fields are left untouched
    // and no lease-index ops are emitted.
    if let Some(lease_id) = record.metadata.get("lease_id") {
        if !lease_id.is_empty() {
            let is_renewal = run.lease_id == *lease_id && !run.lease_id.is_empty();
            run.lease_id = lease_id.clone();
            if let Some(worker) = record.metadata.get("lease_worker_id") {
                run.lease_worker_id = worker.clone();
            }
            if let Some(expires) = record
                .metadata
                .get("lease_expires_at_ms")
                .and_then(|v| v.parse::<i64>().ok())
            {
                run.lease_expires_at_ms = expires;
            }
            if !is_renewal {
                run.lease_created_at_ms = record.timestamp_ns / 1_000_000;
                // Epoch increments monotonically with each fresh lease.
                run.lease_epoch = run.lease_epoch.saturating_add(1);
            }
            if let Some(timeout) = record
                .metadata
                .get("lease_timeout_ms")
                .and_then(|v| v.parse::<i64>().ok())
            {
                run.lease_timeout_ms = timeout;
            }
            // Persist the function's limit at grant time, before a worker can
            // die without returning its retry metadata in a completion.
            for key in [
                RUN_META_LEASE_SESSION_HASH,
                RUN_META_LEASE_ATTEMPT,
                "max_attempts",
            ] {
                if let Some(value) = record.metadata.get(key) {
                    run.metadata.insert(key.to_string(), value.clone());
                }
            }
            // `run.assigned` is the authoritative dispatch decision. Preserve
            // its explicit mode so the service does not mistake a lease-backed
            // PUSH assignment for a legacy PULL lease solely because both now
            // carry a session hash.
            if let Some(value) = record.metadata.get("dispatch_mode") {
                run.metadata
                    .insert("dispatch_mode".to_string(), value.clone());
            }
        }
    }

    run.encode_to_vec()
}

/// Apply `run.lease_expired`: clear lease state, return run to `queued`,
/// and increment `attempt`. Leaves `lease_epoch` untouched — it will
/// increment on the next successful lease.
fn apply_lease_expired(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = existing
        .and_then(|bytes| Run::decode(bytes).ok())
        .unwrap_or_default();
    // Keep why this attempt's worker died: a later attempt's lease loss builds
    // the run's final error from this projection. The reason is
    // read from the record; the history itself is always the run's own.
    // A lease taken back on a worker disconnect has no container reason yet,
    // so that attempt is recorded as disconnected rather than not at all.
    let disconnected = record
        .metadata
        .get(run_state_machine::LEASE_RECOVERY_REASON_META)
        .is_some_and(|reason| reason == run_state_machine::WORKER_DISCONNECT_RECOVERY);
    if let Some(reason) = record
        .metadata
        .get(run_state_machine::WORKER_TERMINATION_REASON_META)
        .filter(|reason| !reason.is_empty())
        .map(String::as_str)
        .or(disconnected.then_some(run_state_machine::WORKER_DISCONNECTED_REASON))
    {
        run_state_machine::record_lease_worker_termination(
            &mut run.metadata,
            run.attempt.saturating_add(1),
            reason,
        );
    }
    run.status = "queued".to_string();
    run.attempt = run.attempt.saturating_add(1);
    clear_lease_fields(&mut run);
    stamp_dispatch_generation(&mut run, record);
    run.encode_to_vec()
}

/// Apply `run.lease_renewed`: extend the projected expiry of the lease the
/// record was fenced by. The segment already decided the fence, so a renewal
/// for a lease the projection no longer shows is simply stale and ignored.
fn apply_lease_renewed(record: &Record, existing: Option<&[u8]>) -> Option<Vec<u8>> {
    let mut run = Run::decode(existing?).ok()?;
    let fence = lease_fence(record).ok().flatten()?;
    if fence.kind != LeaseFenceKind::LeaseRenewal
        || run.lease_id.is_empty()
        || run.lease_id != fence.lease_id
    {
        return None;
    }
    let expires_at_ms = engine_core::lease_fence::renewed_lease_expiry(record)?;
    if expires_at_ms <= run.lease_expires_at_ms {
        return None;
    }
    run.lease_expires_at_ms = expires_at_ms;
    if let Some(timeout) = record
        .metadata
        .get("lease_timeout_ms")
        .and_then(|value| value.parse::<i64>().ok())
    {
        run.lease_timeout_ms = timeout;
    }
    Some(run.encode_to_vec())
}

fn apply_started(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    run.status = "started".to_string();
    run.started_at = Some(ns_to_timestamp(record.timestamp_ns));
    pin_first_dispatch(&mut run, record);
    run.encode_to_vec()
}

/// Pins this dispatch record as the run's first dispatch, unless the run has
/// one. Billing counts the run's start there (`engine_core::billing`), so the
/// pin is set once and never moves. Observed runs aren't dispatched.
fn pin_first_dispatch(run: &mut Run, record: &Record) -> bool {
    if run.first_dispatch_offset.is_some()
        || engine_core::journal_contract::is_observed_record(record)
    {
        return false;
    }
    run.first_dispatch_offset = Some(record.offset.0);
    true
}

/// The row with a late `run.assigned` pinned, or `None` when nothing changes.
/// A worker can pause a run before the processor's assignment lands, and the
/// projection skips that assignment's status change, but it still dispatched
/// the run. A terminal row is never rewritten: retention deletes a hot row
/// only while it matches its archived copy.
fn pin_late_assignment(record: &Record, existing: Option<&Run>) -> Option<Vec<u8>> {
    let mut run = existing?.clone();
    if RunStatus::from_str(&run.status).is_some_and(|status| status.is_terminal()) {
        if run.first_dispatch_offset.is_none()
            && !engine_core::journal_contract::is_observed_record(record)
        {
            warn!(
                run_id = %record.run_id,
                event_type = %record.event_type,
                status = %run.status,
                "Run ended before any dispatch record was projected; its start isn't billed"
            );
        }
        return None;
    }
    pin_first_dispatch(&mut run, record).then(|| run.encode_to_vec())
}

fn apply_completed(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    run.status = "completed".to_string();
    run.completed_at = Some(ns_to_timestamp(record.timestamp_ns));

    // Calculate duration
    if let Some(ref started_at) = run.started_at {
        let start_ns = started_at.seconds * 1_000_000_000 + started_at.nanos as i64;
        run.duration_ms = (record.timestamp_ns - start_ns) / 1_000_000;
    }

    // Extract output from payload. Large workerless outputs are stored out of
    // band and projected as metadata so the Run proto can stay unchanged.
    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if payload.get("output_ref").is_some() {
            run.output_data.clear();
            copy_workerless_output_ref_metadata(&mut run, record);
        } else if let Some(output) = completed_output_payload(record, &payload) {
            if let Ok(output_bytes) = serde_json::to_vec(output) {
                run.output_data = output_bytes;
            }
        }
    }

    // Keep the completing worker on the run for attribution once the lease
    // fields below are cleared.
    if let Some(worker_id) = record.metadata.get("worker_id") {
        run.metadata
            .insert("worker_id".to_string(), worker_id.clone());
    }
    // Terminal state releases the lease. The diff-driven projection logic
    // picks up the cleared lease fields and emits RemoveLeaseIndex.
    clear_lease_fields(&mut run);

    run.encode_to_vec()
}

fn completed_output_payload<'a>(
    record: &Record,
    payload: &'a serde_json::Value,
) -> Option<&'a serde_json::Value> {
    if let Some(output) = payload.get("output_data").or_else(|| payload.get("output")) {
        return Some(output);
    }
    if record_component_type_is_scorer(record) && scorer_result_payload(payload) {
        return Some(payload);
    }
    None
}

fn copy_workerless_output_ref_metadata(run: &mut Run, record: &Record) {
    for key in WORKERLESS_OUTPUT_REF_METADATA_KEYS {
        if let Some(value) = record.metadata.get(*key).filter(|value| !value.is_empty()) {
            run.metadata.insert((*key).to_string(), value.clone());
        }
    }
}

fn record_component_type_is_scorer(record: &Record) -> bool {
    matches!(
        record
            .metadata
            .get("component_type")
            .map(String::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "scorer" | "scorers" | "component_type_scorer"
    )
}

fn scorer_result_payload(payload: &serde_json::Value) -> bool {
    payload.get("score").is_some() || payload.get("passed").is_some()
}

fn apply_failed(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    let retry_ready_at_ms =
        run_state_machine::retry_ready_at_ms(record.timestamp_ns, &record.metadata);
    run.status = if retry_ready_at_ms.is_some() {
        "retry_delayed"
    } else {
        "failed"
    }
    .to_string();
    run.completed_at = retry_ready_at_ms
        .is_none()
        .then(|| ns_to_timestamp(record.timestamp_ns));

    // Calculate duration
    if let Some(ref started_at) = run.started_at {
        let start_ns = started_at.seconds * 1_000_000_000 + started_at.nanos as i64;
        run.duration_ms = (record.timestamp_ns - start_ns) / 1_000_000;
    }

    run.metadata.remove("error_type");
    run.metadata.remove("error_stack");

    // Extract error info from payload
    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(msg) = payload.get("error_message").and_then(|v| v.as_str()) {
            run.error_message = msg.to_string();
        }
        if let Some(code) = payload.get("error_code").and_then(|v| v.as_str()) {
            run.error_code = code.to_string();
        }
        for key in ["error_type", "error_stack"] {
            if let Some(value) = payload.get(key).and_then(|value| value.as_str()) {
                run.metadata.insert(key.to_string(), value.to_string());
            }
        }
    }

    // A lost lease's cause describes the loss that ended this attempt, so it
    // mirrors this record instead of lingering from an earlier one.
    run.metadata
        .remove(run_state_machine::WORKER_TERMINATION_REASON_META);
    run.metadata
        .remove(run_state_machine::LEASE_RECOVERY_REASON_META);
    run.metadata.extend(public_metadata(record));
    if let Some(ready_at_ms) = retry_ready_at_ms {
        run.metadata.insert(
            run_state_machine::RETRY_READY_AT_MS_META.to_string(),
            ready_at_ms.to_string(),
        );
        run.metadata.insert(
            run_state_machine::READY_AT_MS_META.to_string(),
            ready_at_ms.to_string(),
        );
    }

    // Terminal state releases the lease.
    clear_lease_fields(&mut run);

    run.encode_to_vec()
}

/// Record metadata minus the runtime-owned fence keys and termination
/// history. Fence keys describe the lease that authored one record; copying
/// them onto the projected Run would make later runtime-authored records look
/// fenced by a dead lease. The history is written only by the projection, so
/// a record's possibly stale copy never replaces it.
fn public_metadata(record: &Record) -> std::collections::HashMap<String, String> {
    let mut metadata = record.metadata.clone();
    strip_reserved_fence_metadata(&mut metadata);
    metadata.remove(run_state_machine::LEASE_WORKER_TERMINATIONS_META);
    metadata
}

fn apply_terminalizing(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    run.status = "terminalizing".to_string();
    run.metadata.extend(public_metadata(record));
    clear_lease_fields(&mut run);
    run.encode_to_vec()
}

fn apply_paused(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    run.status = "paused".to_string();
    run.metadata.extend(public_metadata(record));
    run.metadata
        .remove(run_state_machine::WAIT_DEADLINE_MS_META);
    if let Some(timeout_ms) = record
        .metadata
        .get(run_state_machine::WAIT_TIMEOUT_MS_META)
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|timeout| *timeout >= 0)
    {
        let deadline = (record.timestamp_ns / 1_000_000).saturating_add(timeout_ms);
        run.metadata.insert(
            run_state_machine::WAIT_DEADLINE_MS_META.to_string(),
            deadline.to_string(),
        );
    }
    if let Some(source_id) = engine_core::lease_fence::resume_source_id(&record.metadata) {
        run.metadata.insert(
            engine_core::lease_fence::META_RESUME_SOURCE_ID.to_string(),
            source_id,
        );
    }
    // Pause hands the run off to wait for external input (HITL response,
    // signal, timer). Releasing the lease prevents the expiry sweeper
    // from re-dispatching the run while it sits paused — the diff-driven
    // projection logic emits RemoveLeaseIndex once these fields clear,
    // which deletes the secondary-index entry the sweeper scans.
    clear_lease_fields(&mut run);
    run.encode_to_vec()
}

/// Keep, on the paused run, the continuation its durable timer will resume
/// it with.
///
/// A timer suspension journals the continuation once, as data on its
/// `timer.scheduled` record, where it counts at its own size toward the
/// journal limit. With the continuation on the run row, the timer's
/// `run.resumed` and the supervisor's records copied from the run can leave
/// their metadata copy out when it would breach the limit
/// (`shed_oversized_continuation`); dispatch reads it from the row either
/// way. Only the timer of the run's current pause counts, by the same rule
/// that lets that timer resume the run.
fn apply_timer_scheduled(record: &Record, existing: Option<&[u8]>) -> Option<Vec<u8>> {
    let mut run = Run::decode(existing?).ok()?;
    if RunStatus::from_str(&run.status) != Some(RunStatus::Paused) {
        return None;
    }
    let payload = serde_json::from_slice::<serde_json::Value>(&record.data).ok()?;
    let resume_metadata = payload
        .get("schedule_payload")?
        .get("resume_metadata")?
        .as_object()?;
    let continuation = resume_metadata.get(META_CONTINUATION_B64)?.as_str()?;

    let current_pause = resume_source_id(&run.metadata)?;
    let mut timer_metadata = run.metadata.clone();
    timer_metadata.extend(
        resume_metadata.iter().filter_map(|(key, value)| {
            value.as_str().map(|value| (key.clone(), value.to_string()))
        }),
    );
    if resume_source_id(&timer_metadata).as_deref() != Some(current_pause.as_str()) {
        return None;
    }
    if run.metadata.get(META_CONTINUATION_B64).map(String::as_str) == Some(continuation) {
        return None;
    }
    run.metadata
        .insert(META_CONTINUATION_B64.to_string(), continuation.to_string());
    Some(run.encode_to_vec())
}

fn apply_resumed(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    // Resume puts the run back in the dispatch queue. The orchestrator's
    // evaluate_resumed (run on the pre-projection state, which is still
    // `paused`) emits a Dispatch action that mints a fresh lease and
    // writes run.assigned. Status starts at "queued" so the next
    // run.assigned is a legal queued→assigned transition, mirroring the
    // run.lease_expired re-queue path.
    run.status = "queued".to_string();
    run.metadata.remove(run_state_machine::WAIT_TIMEOUT_MS_META);
    run.metadata
        .remove(run_state_machine::WAIT_DEADLINE_MS_META);
    // Preserve resume/checkpoint metadata for pull workers. The pending
    // indexes store the projected Run, so without this merge PollJob would
    // dispatch the resumed workflow without user_response, pause_index,
    // step_events, or workflow_state.
    engine_core::lease_fence::strip_stale_resume_metadata(&mut run.metadata);
    let mut resumed = record.metadata.clone();
    resumed.remove(run_state_machine::WAIT_TIMEOUT_MS_META);
    resumed.remove(run_state_machine::WAIT_DEADLINE_MS_META);
    run_state_machine::strip_runtime_lease_loss_metadata(&mut resumed);
    run.metadata.extend(resumed);
    stamp_dispatch_generation(&mut run, record);
    run.encode_to_vec()
}

fn apply_cancelled(record: &Record, existing: Option<&[u8]>) -> Vec<u8> {
    let mut run = load_or_default(existing, record);
    run.status = "cancelled".to_string();
    run.completed_at = Some(ns_to_timestamp(record.timestamp_ns));

    if let Some(ref started_at) = run.started_at {
        let start_ns = started_at.seconds * 1_000_000_000 + started_at.nanos as i64;
        run.duration_ms = (record.timestamp_ns - start_ns) / 1_000_000;
    }

    // Terminal state releases the lease.
    clear_lease_fields(&mut run);

    run.encode_to_vec()
}

/// Accumulate per-LM-event metrics into the run's `model_usage` list,
/// keyed by `(provider, model)`. Each lm.* event increments the matching
/// entry's tokens, cost, and call_count; new (provider, model) pairs are
/// appended.
///
/// Returns None if the event has no usable metric data, or no existing run
/// was found (LM events without a prior run.queued can't be associated).
fn apply_model_usage(record: &Record, existing: Option<&[u8]>) -> Option<Vec<u8>> {
    let bytes = existing?;
    let mut run = Run::decode(bytes).ok()?;

    let parse_int = |key: &str| -> i64 {
        record
            .metadata
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
    };
    let input = parse_int("input_tokens");
    let output = parse_int("output_tokens");
    let cached = parse_int("cached_tokens");
    let cost = parse_int("cost_micros");

    if input == 0 && output == 0 && cached == 0 && cost == 0 {
        return None;
    }

    let provider = record.metadata.get("provider").cloned().unwrap_or_default();
    let model = record.metadata.get("model").cloned().unwrap_or_default();

    let entry = run
        .model_usage
        .iter_mut()
        .find(|m| m.provider == provider && m.model == model);

    match entry {
        Some(m) => {
            m.input_tokens = m.input_tokens.saturating_add(input);
            m.output_tokens = m.output_tokens.saturating_add(output);
            m.cached_tokens = m.cached_tokens.saturating_add(cached);
            m.cost_micros = m.cost_micros.saturating_add(cost);
            m.call_count = m.call_count.saturating_add(1);
        }
        None => {
            run.model_usage.push(ModelUsage {
                provider,
                model,
                input_tokens: input,
                output_tokens: output,
                cached_tokens: cached,
                cost_micros: cost,
                call_count: 1,
            });
        }
    }

    Some(run.encode_to_vec())
}

/// Update an observed run's settle state from one of its span events: when
/// the latest span arrived, how many spans failed, the run's input and
/// output, and, once its root span ends, the run's outcome. Returns `None`
/// when there is no run to update.
fn apply_observed_span(
    record: &Record,
    run_data: Option<&[u8]>,
    existing: Option<&[u8]>,
) -> Option<Vec<u8>> {
    use engine_core::journal_contract::{
        META_OBSERVED_OUTCOME, META_OBSERVED_RECEIVED_AT_MS, META_OBSERVED_ROOT,
        OBSERVED_INPUT_FIELD, OBSERVED_OUTPUT_FIELD,
    };
    use run_state_machine::{
        OBSERVED_ENDED_AT_NS_META, OBSERVED_FAILED_SPANS_META, OBSERVED_LAST_SPAN_MS_META,
        OBSERVED_OUTCOME_META, OBSERVED_ROOT_IO_META, OBSERVED_ROOT_KIND_META,
    };

    let mut run = Run::decode(run_data.or(existing)?).ok()?;
    if matches!(
        RunStatus::from_str(&run.status),
        Some(RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled)
    ) {
        return None;
    }
    let event_type = record.event_type.as_str();
    let parse = |value: Option<&String>| value.and_then(|v| v.parse::<i64>().ok());

    // Arrival time, which the gateway stamps; a record without it falls back
    // to the span's own time.
    let arrived_ms = parse(record.metadata.get(META_OBSERVED_RECEIVED_AT_MS))
        .unwrap_or(record.timestamp_ns / 1_000_000);
    let last_ms = parse(run.metadata.get(OBSERVED_LAST_SPAN_MS_META)).unwrap_or(0);
    run.metadata.insert(
        OBSERVED_LAST_SPAN_MS_META.to_string(),
        last_ms.max(arrived_ms).to_string(),
    );
    let ended_ns = parse(run.metadata.get(OBSERVED_ENDED_AT_NS_META)).unwrap_or(0);
    run.metadata.insert(
        OBSERVED_ENDED_AT_NS_META.to_string(),
        ended_ns.max(record.timestamp_ns).to_string(),
    );

    let failed = event_type.ends_with(".failed");
    let ended = failed || event_type.ends_with(".completed");
    if failed {
        let count = parse(run.metadata.get(OBSERVED_FAILED_SPANS_META)).unwrap_or(0);
        run.metadata.insert(
            OBSERVED_FAILED_SPANS_META.to_string(),
            (count + 1).to_string(),
        );
    }
    let payload = serde_json::from_slice::<serde_json::Value>(&record.data).ok();
    let field = |name: &str| {
        payload
            .as_ref()
            .and_then(|p| p.get(name))
            .filter(|v| !v.is_null())
            .and_then(|v| serde_json::to_vec(v).ok())
    };

    let root = record.metadata.get(META_OBSERVED_ROOT).map(String::as_str);
    let seen_true_root = run
        .metadata
        .get(OBSERVED_ROOT_KIND_META)
        .is_some_and(|kind| kind == "true");
    // A local root never overrides the trace's true root.
    let deciding_root = root.is_some_and(|kind| kind == "true" || !seen_true_root);
    let model_call = event_type.starts_with("lm.");

    // A span's input is on its start event and its output on its end event.
    // The run takes them from its root when the root has them, otherwise
    // from the first model call's input and the latest model call's output.
    let content = if ended {
        field(OBSERVED_OUTPUT_FIELD)
    } else {
        field(OBSERVED_INPUT_FIELD)
    };
    if let Some(content) = content {
        let slot = if ended {
            &mut run.output_data
        } else {
            &mut run.input_data
        };
        if deciding_root {
            *slot = content;
            run.metadata
                .insert(OBSERVED_ROOT_IO_META.to_string(), "true".to_string());
        } else if model_call
            && !run.metadata.contains_key(OBSERVED_ROOT_IO_META)
            && (ended || slot.is_empty())
        {
            *slot = content;
        }
    }
    if !ended {
        return Some(run.encode_to_vec());
    }

    if let Some(kind) = root {
        apply_observed_root_error(&mut run, payload.as_ref(), deciding_root);
        if deciding_root {
            let outcome = record
                .metadata
                .get(META_OBSERVED_OUTCOME)
                .cloned()
                .unwrap_or_else(|| if failed { "failed" } else { "completed" }.to_string());
            run.metadata
                .insert(OBSERVED_ROOT_KIND_META.to_string(), kind.to_string());
            run.metadata
                .insert(OBSERVED_OUTCOME_META.to_string(), outcome);
        }
    }
    Some(run.encode_to_vec())
}

/// Keep the error an observed run will fail with. The root that decides the
/// outcome sets it, but a real error from any root beats one the gateway had
/// to infer: in a trace across services, the HTTP root in front often fails
/// without saying why, while the service behind it does, and either can
/// arrive first.
fn apply_observed_root_error(run: &mut Run, payload: Option<&serde_json::Value>, deciding: bool) {
    use engine_core::journal_contract::OBSERVED_ERROR_INFERRED_FIELD;
    use run_state_machine::{
        OBSERVED_ERROR_CODE_META, OBSERVED_ERROR_INFERRED_META, OBSERVED_ERROR_MESSAGE_META,
    };

    let text = |key: &str| {
        payload
            .and_then(|p| p.get(key))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };
    let message = text("error_message");
    let inferred = payload
        .and_then(|p| p.get(OBSERVED_ERROR_INFERRED_FIELD))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let have_error = run.metadata.contains_key(OBSERVED_ERROR_MESSAGE_META);
    let have_real = have_error && !run.metadata.contains_key(OBSERVED_ERROR_INFERRED_META);
    let replace = match &message {
        None => deciding,
        Some(_) if inferred => !have_real && (deciding || !have_error),
        Some(_) => deciding || !have_real,
    };
    if !replace {
        return;
    }
    for (meta, value) in [
        (OBSERVED_ERROR_CODE_META, text("error_code")),
        (OBSERVED_ERROR_MESSAGE_META, message),
    ] {
        match value {
            Some(value) => run.metadata.insert(meta.to_string(), value),
            None => run.metadata.remove(meta),
        };
    }
    if inferred {
        run.metadata
            .insert(OBSERVED_ERROR_INFERRED_META.to_string(), "true".to_string());
    } else {
        run.metadata.remove(OBSERVED_ERROR_INFERRED_META);
    }
}

fn stamp_terminal_obligation_position(run_data: &mut Vec<u8>, record: &Record) {
    let Ok(mut run) = Run::decode(run_data.as_slice()) else {
        return;
    };
    let (_, local_offset) = decode_offset(record.offset);
    run.metadata.insert(
        run_state_machine::TERMINAL_OBLIGATION_OFFSET_META.to_string(),
        local_offset.to_string(),
    );
    run.metadata.insert(
        run_state_machine::TERMINAL_OBLIGATION_UPDATED_AT_MS_META.to_string(),
        (record.timestamp_ns / 1_000_000).to_string(),
    );
    *run_data = run.encode_to_vec();
}

/// Clear the five lease fields on the Run proto. Called from all terminal
/// apply_* helpers so the diff-driven projection logic emits RemoveLeaseIndex
/// when a lease-backed run completes / fails / is cancelled.
///
/// `lease_epoch` is NOT cleared — it records "how many leases has this run
/// had" across its lifetime and is useful for debugging.
fn clear_lease_fields(run: &mut Run) {
    run.lease_id.clear();
    run.lease_worker_id.clear();
    run.lease_expires_at_ms = 0;
    run.lease_created_at_ms = 0;
    run.lease_timeout_ms = 0;
    run.metadata.remove(RUN_META_LEASE_SESSION_HASH);
    run.metadata.remove(RUN_META_LEASE_ATTEMPT);
}

/// Convert nanosecond timestamp to protobuf Timestamp.
fn ns_to_timestamp(ns: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ns / 1_000_000_000,
        nanos: (ns % 1_000_000_000) as i32,
    }
}

fn metadata_timestamp_ns(record: &Record, key: &str) -> Option<i64> {
    record.metadata.get(key).and_then(|v| v.parse().ok())
}

/// Pull the trace_id out of a W3C traceparent
/// header (`version-trace_id-span_id-flags`).
///
/// Returns `None` for any malformed input — the projection then
/// leaves `Run.trace_id` empty rather than rejecting the record.
/// Validates the trace_id is exactly 32 lower-case hex chars
/// (the W3C requirement); doesn't validate the version byte or
/// span_id beyond presence (callers that want span_id and flags
/// should re-parse the metadata `traceparent` value directly —
/// keeping the helper narrow lets it stay correctness-focused).
pub(crate) fn parse_traceparent_trace_id(traceparent: &str) -> Option<String> {
    let mut parts = traceparent.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let span_id = parts.next()?;
    let _flags = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if version.len() != 2 || !version.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    // The all-zeros trace_id is reserved as "invalid" per the W3C spec.
    if trace_id == "00000000000000000000000000000000" {
        return None;
    }
    if span_id.len() != 16 || !span_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(trace_id.to_string())
}

/// Extract `error_code` / `error_message` from a JSON-encoded record payload.
///
/// Returns empty strings if the data isn't valid JSON or if the fields are
/// missing. Used by the illegal-transition warn site to tag the warning with
/// producer identity — `DISPATCH_FAILED` (processor dispatch-error path),
/// `DEAD_LETTER` (sweeper), or an SDK-emitted code.
fn parse_error_fields(data: &[u8]) -> (String, String) {
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(data) else {
        return (String::new(), String::new());
    };
    let code = payload
        .get("error_code")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let message = payload
        .get("error_message")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    (code, message)
}

/// Truncate a string to at most `max` chars for a log field, appending a
/// `…(+N)` suffix when truncated. Keeps a stray long stack trace from
/// swamping the warning log line.
fn truncate_for_log(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let head: String = s.chars().take(max).collect();
    let remaining = s.chars().count() - max;
    format!("{head}…(+{remaining})")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use engine_core::types::Offset;
    use std::collections::HashMap;

    fn make_record(event_type: &str, ts_ns: i64, data: &str) -> Record {
        Record {
            offset: Offset(0),
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            event_type: event_type.to_string(),
            data: Bytes::from(data.to_string()),
            timestamp_ns: ts_ns,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn observed(event_type: &str, ts_ns: i64, received_ms: i64, data: &str) -> Record {
        let mut record = make_record(event_type, ts_ns, data);
        record.metadata.insert(
            engine_core::journal_contract::META_ORIGIN.to_string(),
            engine_core::journal_contract::ORIGIN_OTLP.to_string(),
        );
        record.metadata.insert(
            engine_core::journal_contract::META_OBSERVED_RECEIVED_AT_MS.to_string(),
            received_ms.to_string(),
        );
        record
    }

    fn observed_started_run() -> Vec<u8> {
        // The gateway opens an observed run with no payload: its input comes
        // from the spans.
        let queued = project(&observed("run.queued", 1_000_000_000, 1_000, ""), None);
        let queued = queued.run_data().unwrap().to_vec();
        let started = project(
            &observed("run.started", 1_000_000_000, 1_000, "{}"),
            Some(&queued),
        );
        started.run_data().unwrap().to_vec()
    }

    fn project_observed(run: &[u8], record: &Record) -> Run {
        Run::decode(project(record, Some(run)).run_data().unwrap()).unwrap()
    }

    /// A model call: its input on the start event, its output on the end.
    fn model_call(run: &[u8], ts_ms: i64, received_ms: i64, input: &str, output: &str) -> Run {
        let started = observed(
            "lm.call.started",
            ts_ms * 1_000_000,
            received_ms,
            &format!(r#"{{"input_data":"{input}"}}"#),
        );
        let run = project_observed(run, &started).encode_to_vec();
        let completed = observed(
            "lm.call.completed",
            ts_ms * 1_000_000,
            received_ms,
            &format!(r#"{{"output_data":"{output}"}}"#),
        );
        project_observed(&run, &completed)
    }

    #[test]
    fn observed_spans_track_the_settle_state() {
        let run = observed_started_run();
        let run = model_call(&run, 2_000, 2_500, "hi", "first");
        assert_eq!(run.status, "started");
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_LAST_SPAN_MS_META],
            "2500"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ENDED_AT_NS_META],
            "2000000000"
        );
        assert!(!run
            .metadata
            .contains_key(run_state_machine::OBSERVED_ROOT_KIND_META));
        assert_eq!(run.input_data, br#""hi""#);
        assert_eq!(run.output_data, br#""first""#);

        // A later model call replaces the output but not the input.
        let run = model_call(&run.encode_to_vec(), 3_000, 3_000, "again", "second");
        assert_eq!(run.input_data, br#""hi""#);
        assert_eq!(run.output_data, br#""second""#);

        // A span that arrives late keeps the latest arrival time.
        let run = project_observed(
            &run.encode_to_vec(),
            &observed("tool_call.failed", 2_500_000_000, 2_800, "{}"),
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_LAST_SPAN_MS_META],
            "3000"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ENDED_AT_NS_META],
            "3000000000"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_FAILED_SPANS_META],
            "1"
        );
    }

    #[test]
    fn observed_true_root_decides_outcome_and_io() {
        let run = observed_started_run();
        let mut local = observed(
            "agent.completed",
            2_000_000_000,
            2_000,
            r#"{"output_data":"local out"}"#,
        );
        local.metadata.insert(
            engine_core::journal_contract::META_OBSERVED_ROOT.to_string(),
            "local".to_string(),
        );
        let run = project_observed(&run, &local);
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ROOT_KIND_META],
            "local"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_OUTCOME_META],
            "completed"
        );

        let mut root_start = observed(
            "workflow.step.started",
            2_900_000_000,
            3_000,
            r#"{"input_data":"q"}"#,
        );
        let mut root = observed(
            "workflow.step.failed",
            3_000_000_000,
            3_000,
            r#"{"error_code":"ValueError","error_message":"boom","output_data":"root out"}"#,
        );
        for record in [&mut root_start, &mut root] {
            record.metadata.insert(
                engine_core::journal_contract::META_OBSERVED_ROOT.to_string(),
                "true".to_string(),
            );
        }
        let run = project_observed(&run.encode_to_vec(), &root_start);
        let run = project_observed(&run.encode_to_vec(), &root);
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ROOT_KIND_META],
            "true"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_OUTCOME_META],
            "failed"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ERROR_CODE_META],
            "ValueError"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ERROR_MESSAGE_META],
            "boom"
        );
        assert_eq!(run.input_data, br#""q""#);
        assert_eq!(run.output_data, br#""root out""#);

        // A local root after the true root changes nothing, and neither does
        // a model call: the root's input and output stand.
        let run = project_observed(&run.encode_to_vec(), &local);
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ROOT_KIND_META],
            "true"
        );
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_OUTCOME_META],
            "failed"
        );
        let run = project_observed(
            &run.encode_to_vec(),
            &observed(
                "lm.call.completed",
                3_500_000_000,
                3_500,
                r#"{"output_data":"later"}"#,
            ),
        );
        assert_eq!(run.output_data, br#""root out""#);
    }

    /// A failed root's end event, as the gateway writes it.
    fn failed_root(kind: &str, ts_ms: i64, payload: &str) -> Record {
        let mut root = observed("workflow.step.failed", ts_ms * 1_000_000, ts_ms, payload);
        root.metadata.insert(
            engine_core::journal_contract::META_OBSERVED_ROOT.to_string(),
            kind.to_string(),
        );
        root
    }

    fn error_of(run: &Run) -> (&str, &str) {
        (
            run.metadata[run_state_machine::OBSERVED_ERROR_CODE_META].as_str(),
            run.metadata[run_state_machine::OBSERVED_ERROR_MESSAGE_META].as_str(),
        )
    }

    const GATEWAY_ROOT: &str = r#"{"error_code":"500","error_message":"span POST /chat ended in error","error_inferred":true}"#;
    const AGENT_ROOT: &str =
        r#"{"error_code":"RuntimeError","error_message":"order service unavailable"}"#;

    /// Two services in one trace: the gateway's HTTP root fails without
    /// saying why, the agent's local root says. Whichever arrives first, the
    /// run keeps the agent's error and the true root's outcome.
    #[test]
    fn a_real_root_error_beats_an_inferred_one_in_either_order() {
        for gateway_first in [true, false] {
            let mut run = observed_started_run();
            let order = if gateway_first {
                [("true", GATEWAY_ROOT), ("local", AGENT_ROOT)]
            } else {
                [("local", AGENT_ROOT), ("true", GATEWAY_ROOT)]
            };
            for (i, (kind, payload)) in order.into_iter().enumerate() {
                let record = failed_root(kind, 2_000 + i as i64, payload);
                run = project_observed(&run, &record).encode_to_vec();
            }
            let run = Run::decode(run.as_slice()).unwrap();
            assert_eq!(
                error_of(&run),
                ("RuntimeError", "order service unavailable"),
                "gateway first: {gateway_first}"
            );
            assert_eq!(
                run.metadata[run_state_machine::OBSERVED_ROOT_KIND_META],
                "true"
            );
            assert_eq!(
                run.metadata[run_state_machine::OBSERVED_OUTCOME_META],
                "failed"
            );
            assert!(!run
                .metadata
                .contains_key(run_state_machine::OBSERVED_ERROR_INFERRED_META));
        }
    }

    #[test]
    fn an_inferred_error_stands_when_no_root_says_more() {
        let run = observed_started_run();
        let run = project_observed(&run, &failed_root("true", 2_000, GATEWAY_ROOT));
        assert_eq!(error_of(&run), ("500", "span POST /chat ended in error"));
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_ERROR_INFERRED_META],
            "true"
        );
    }

    #[test]
    fn observed_root_outcome_follows_the_gateway_rule() {
        let run = observed_started_run();
        // The gateway can judge a root that recorded an error as completed,
        // or the reverse; its call wins over the event type.
        let mut root = observed("agent.failed", 2_000_000_000, 2_000, "{}");
        root.metadata.insert(
            engine_core::journal_contract::META_OBSERVED_ROOT.to_string(),
            "true".to_string(),
        );
        root.metadata.insert(
            engine_core::journal_contract::META_OBSERVED_OUTCOME.to_string(),
            "completed".to_string(),
        );
        let run = project_observed(&run, &root);
        assert_eq!(
            run.metadata[run_state_machine::OBSERVED_OUTCOME_META],
            "completed"
        );
    }

    #[test]
    fn observed_run_keeps_a_terminal_obligation_while_started() {
        let run = observed_started_run();
        let result = project(
            &observed("agent.started", 2_000_000_000, 2_000, "{}"),
            Some(&run),
        );
        assert!(result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutTerminalObligation { .. })));
    }

    #[test]
    fn native_span_events_leave_no_observed_state() {
        let queued = project(&make_record("run.queued", 1_000_000_000, "{}"), None);
        let queued = queued.run_data().unwrap().to_vec();
        let result = project(
            &make_record("lm.call.completed", 2_000_000_000, r#"{"output_data":"x"}"#),
            Some(&queued),
        );
        if let Some(data) = result.run_data() {
            let run = Run::decode(data).unwrap();
            assert!(!run
                .metadata
                .contains_key(run_state_machine::OBSERVED_LAST_SPAN_MS_META));
            assert!(run.output_data.is_empty());
        }
    }

    #[test]
    fn queued_creates_run() {
        let mut record = make_record(
            "run.queued",
            1_000_000_000, // 1s
            r#"{"input_data":{"x":1},"timeout_ms":30000}"#,
        );
        record
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        record.metadata.insert(
            "component_type".to_string(),
            "COMPONENT_TYPE_FUNCTION".to_string(),
        );

        let result = project(&record, None);
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.status, "queued");
        assert_eq!(run.component_name, "my_func");
        assert_eq!(run.project_id, "t1");
        assert_eq!(run.run_id, "r1");
        assert!(run.submitted_at.is_some());
        assert!(!run.input_data.is_empty());
    }

    #[test]
    fn queued_projects_gateway_received_at_from_metadata() {
        let mut record = make_record("run.queued", 2_000_000_000, "{}");
        record.metadata.insert(
            META_KEY_GATEWAY_RECEIVED_AT_NS.to_string(),
            "1500000000".to_string(),
        );

        let result = project(&record, None);
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        let gateway_received_at = run.gateway_received_at.unwrap();

        assert_eq!(gateway_received_at.seconds, 1);
        assert_eq!(gateway_received_at.nanos, 500_000_000);
    }

    #[test]
    fn queued_reads_priority_and_deployment_from_metadata() {
        // The gateway stamps `priority` (as i32 digit) and
        // `deployment_id` on run.queued metadata. The projection should
        // populate the corresponding Run fields so the priority-aware
        // pending index bucket is correct for this run.
        let mut record = make_record("run.queued", 1_000_000_000, "{}");
        record
            .metadata
            .insert("priority".to_string(), "3".to_string()); // BATCH
        record
            .metadata
            .insert("deployment_id".to_string(), "dep-abc".to_string());

        let result = project(&record, None);
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.priority, 3, "Run.priority should reflect BATCH");
        assert_eq!(run.deployment_id, "dep-abc");
    }

    #[test]
    fn queued_missing_priority_metadata_leaves_field_unspecified() {
        // Records from clients that omit priority have no priority
        // metadata — the projection must leave Run.priority at 0 (UNSPECIFIED)
        // so the canonicalization fallback in the pending-index write path
        // kicks in.
        let record = make_record("run.queued", 1_000_000_000, "{}");
        let result = project(&record, None);
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(
            run.priority, 0,
            "missing priority metadata should leave Run.priority unspecified"
        );
    }

    #[test]
    fn queued_invalid_priority_metadata_is_ignored() {
        // Garbled metadata must not crash the projection; field stays 0.
        let mut record = make_record("run.queued", 1_000_000_000, "{}");
        record
            .metadata
            .insert("priority".to_string(), "not-a-number".to_string());
        let result = project(&record, None);
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.priority, 0);
    }

    #[test]
    fn queued_propagates_trace_context() {
        let mut record = make_record("run.queued", 1_000_000_000, "{}");
        record.metadata.insert(
            "traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );
        record
            .metadata
            .insert("baggage".to_string(), "userId=alice".to_string());

        let result = project(&record, None);
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(
            run.metadata.get("traceparent").map(String::as_str),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert_eq!(
            run.metadata.get("baggage").map(String::as_str),
            Some("userId=alice")
        );
    }

    #[test]
    fn queued_preserves_batch_metadata_for_pull_workers() {
        let mut record = make_record("run.queued", 1_000_000_000, "{}");
        record
            .metadata
            .insert("batch_id".to_string(), "batch-123".to_string());
        record
            .metadata
            .insert("batch_index".to_string(), "2".to_string());
        record
            .metadata
            .insert("session_id".to_string(), "sess-1".to_string());

        let result = project(&record, None);
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(
            run.metadata.get("batch_id").map(String::as_str),
            Some("batch-123")
        );
        assert_eq!(
            run.metadata.get("batch_index").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            run.metadata.get("session_id").map(String::as_str),
            Some("sess-1")
        );
    }

    #[test]
    fn started_updates_status() {
        let queued = make_record("run.queued", 1_000_000_000, "{}");
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let started = make_record("run.started", 2_000_000_000, "{}");
        let result = project(&started, Some(&queued_data));
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.status, "started");
        assert!(run.started_at.is_some());
    }

    #[test]
    fn batch_started_removes_pending_run_using_existing_metadata() {
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let started = make_record("run.started", 2_000_000_000, "{}");
        let result = project(&started, Some(&queued_data));

        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemovePendingRun { component_name } if component_name == "my_func"
        )));
    }

    #[test]
    fn ai_worker_queued_without_batch_id_does_not_add_pull_pending_run() {
        // Managed AI prompt runs are handled by push-mode AI workers. Without
        // a batch id or deployment id, they should not create a parked-poll
        // pending entry that no worker session can claim.
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued.metadata.insert(
            "component_name".to_string(),
            "agnt5_prompt_executor".to_string(),
        );
        queued
            .metadata
            .insert("execution_mode".to_string(), "ai_worker".to_string());

        let result = project(&queued, None);
        assert!(!result.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutPendingRun { component_name, .. } if component_name == "agnt5_prompt_executor"
        )));
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutPendingPriority { .. })));
        assert!(!result.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutPendingDeploymentPriority { deployment_id, .. } if deployment_id.is_empty()
        )));
    }

    #[test]
    fn ai_worker_started_without_batch_id_does_not_remove_pull_pending_run() {
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued.metadata.insert(
            "component_name".to_string(),
            "agnt5_prompt_executor".to_string(),
        );
        queued
            .metadata
            .insert("execution_mode".to_string(), "ai_worker".to_string());
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        // The run.started record itself carries no execution_mode — the
        // removal gate must read it from the existing Run metadata.
        let started = make_record("run.started", 2_000_000_000, "{}");
        let result = project(&started, Some(&queued_data));
        assert!(!result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemovePendingRun { component_name } if component_name == "agnt5_prompt_executor"
        )));
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::RemovePendingPriority { .. })));
        assert!(!result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemovePendingDeploymentPriority { deployment_id, .. } if deployment_id.is_empty()
        )));
    }

    #[test]
    fn non_deployment_non_batch_queued_skips_pending_index() {
        // Runs outside a deployment or batch lane must not enter the pull
        // pending index.
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        let result = project(&queued, None);
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutPendingRun { .. })));
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutPendingPriority { .. })));
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutPendingDeploymentPriority { .. })));
    }

    #[test]
    fn deployment_scoped_queued_adds_pending_index_without_batch_id() {
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());

        let result = project(&queued, None);
        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutPendingRun { component_name, .. } if component_name == "my_func"
        )));
        assert_put_priority(&result.ops, "dep-1", "my_func", 2, 1000);
        assert_put_deployment_priority(&result.ops, "dep-1", 2, 1000);
    }

    #[test]
    fn completed_calculates_duration() {
        let queued = make_record("run.queued", 1_000_000_000, "{}");
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let started = make_record("run.started", 2_000_000_000, "{}");
        let started_data = project(&started, Some(&queued_data))
            .run_data()
            .unwrap()
            .to_vec();

        let completed = make_record(
            "run.completed",
            5_000_000_000, // 5s
            r#"{"output_data":{"result":42}}"#,
        );
        let result = project(&completed, Some(&started_data));
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.status, "completed");
        assert_eq!(run.duration_ms, 3000); // 5s - 2s = 3s
        assert!(!run.output_data.is_empty());
    }

    #[test]
    fn completed_scorer_result_without_wrapper_sets_output() {
        let started = make_record("run.started", 1_000_000_000, "{}");
        let started_data = project(&started, None).run_data().unwrap().to_vec();

        let mut completed = make_record(
            "run.completed",
            2_000_000_000,
            r#"{"score":1.0,"passed":true,"label":"valid"}"#,
        );
        completed
            .metadata
            .insert("component_type".to_string(), "scorer".to_string());

        let result = project(&completed, Some(&started_data));
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        let output: serde_json::Value = serde_json::from_slice(&run.output_data).unwrap();

        assert_eq!(run.status, "completed");
        assert_eq!(output["score"], 1.0);
        assert_eq!(output["passed"], true);
    }

    #[test]
    fn completed_output_ref_sets_metadata_without_inline_output() {
        let started = make_record("run.started", 1_000_000_000, "{}");
        let started_data = project(&started, None).run_data().unwrap().to_vec();

        let mut completed = make_record(
            "run.completed",
            2_000_000_000,
            r#"{"output_ref":{"kind":"agnt5.object_store.ref.v1","ref":"workerless/out.json","size_bytes":12,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","content_type":"application/json"}}"#,
        );
        completed.metadata.insert(
            "workerless_output_ref_kind".to_string(),
            "agnt5.object_store.ref.v1".to_string(),
        );
        completed.metadata.insert(
            "workerless_output_ref".to_string(),
            "workerless/out.json".to_string(),
        );
        completed
            .metadata
            .insert("workerless_output_size_bytes".to_string(), "12".to_string());
        completed.metadata.insert(
            "workerless_output_sha256".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        );
        completed.metadata.insert(
            "workerless_output_content_type".to_string(),
            "application/json".to_string(),
        );

        let result = project(&completed, Some(&started_data));
        let run = Run::decode(result.run_data().unwrap()).unwrap();

        assert_eq!(run.status, "completed");
        assert!(run.output_data.is_empty());
        assert_eq!(
            run.metadata
                .get("workerless_output_ref")
                .map(String::as_str),
            Some("workerless/out.json")
        );
        assert_eq!(
            run.metadata
                .get("workerless_output_size_bytes")
                .map(String::as_str),
            Some("12")
        );
    }

    #[test]
    fn failed_extracts_error() {
        let started = make_record("run.started", 1_000_000_000, "{}");
        let started_data = project(&started, None).run_data().unwrap().to_vec();

        let failed = make_record(
            "run.failed",
            2_000_000_000,
            r#"{"error_message":"timeout","error_code":"DEADLINE_EXCEEDED"}"#,
        );
        let result = project(&failed, Some(&started_data));
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.status, "failed");
        assert_eq!(run.error_message, "timeout");
        assert_eq!(run.error_code, "DEADLINE_EXCEEDED");
    }

    #[test]
    fn lease_loss_history_belongs_to_the_projection() {
        let history = |run: &Run| run_state_machine::lease_worker_terminations(&run.metadata);
        let forged = r#"[[1,"forged"]]"#.to_string();

        // A new run ignores lease-loss keys its caller sent (/batch metadata).
        let mut submitted = make_record("run.queued", 1_000_000_000, "{}");
        submitted.metadata.insert(
            run_state_machine::LEASE_WORKER_TERMINATIONS_META.to_string(),
            forged.clone(),
        );
        submitted.metadata.insert(
            run_state_machine::WORKER_TERMINATION_REASON_META.to_string(),
            "OOMKilled".to_string(),
        );
        let fresh = Run::decode(project(&submitted, None).run_data().unwrap()).unwrap();
        assert!(history(&fresh).is_empty());
        assert!(!fresh
            .metadata
            .contains_key(run_state_machine::WORKER_TERMINATION_REASON_META));

        // A failure record can't replace the history, and an earlier cause
        // doesn't linger when the record names none.
        let mut killed_twice = Run {
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: "started".to_string(),
            attempt: 2,
            ..Default::default()
        };
        run_state_machine::record_lease_worker_termination(
            &mut killed_twice.metadata,
            1,
            "OOMKilled",
        );
        run_state_machine::record_lease_worker_termination(
            &mut killed_twice.metadata,
            2,
            "OOMKilled",
        );
        killed_twice.metadata.insert(
            run_state_machine::WORKER_TERMINATION_REASON_META.to_string(),
            "OOMKilled".to_string(),
        );
        let mut failed = make_record(
            "run.failed",
            2_000_000_000,
            r#"{"error_code":"VALUE_ERROR","error_message":"bad input"}"#,
        );
        failed.metadata.insert(
            run_state_machine::LEASE_WORKER_TERMINATIONS_META.to_string(),
            r#"[[1,"OOMKilled"]]"#.to_string(),
        );
        let after_failure = Run::decode(
            project(&failed, Some(&killed_twice.encode_to_vec()))
                .run_data()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            history(&after_failure),
            vec![(1, "OOMKilled".to_string()), (2, "OOMKilled".to_string())]
        );
        assert!(!after_failure
            .metadata
            .contains_key(run_state_machine::WORKER_TERMINATION_REASON_META));

        // An authorized retry keeps the run's history, not the record's.
        let mut delayed = after_failure.clone();
        delayed.status = "retry_delayed".to_string();
        let mut retry = make_record("run.queued", 3_000_000_000, "{}");
        for (key, value) in [
            ("attempt", "3"),
            ("max_attempts", "5"),
            (
                run_state_machine::RETRY_SOURCE_META,
                run_state_machine::RETRY_SOURCE_RUN_FAILED,
            ),
        ] {
            retry.metadata.insert(key.to_string(), value.to_string());
        }
        retry.metadata.insert(
            run_state_machine::LEASE_WORKER_TERMINATIONS_META.to_string(),
            forged,
        );
        let retried = Run::decode(
            project(&retry, Some(&delayed.encode_to_vec()))
                .run_data()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(retried.status, "queued");
        assert_eq!(
            history(&retried),
            vec![(1, "OOMKilled".to_string()), (2, "OOMKilled".to_string())]
        );
    }

    #[test]
    fn authorized_retry_advances_retry_delayed_run() {
        let failed_run = Run {
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: "retry_delayed".to_string(),
            attempt: 0,
            lease_epoch: 1,
            error_message: "temporary failure".to_string(),
            error_code: "UNAVAILABLE".to_string(),
            started_at: Some(ns_to_timestamp(1_000_000_000)),
            completed_at: Some(ns_to_timestamp(2_000_000_000)),
            duration_ms: 1_000,
            ..Default::default()
        }
        .encode_to_vec();
        let mut retry = make_record(
            "run.queued",
            3_000_000_000,
            r#"{"input_data":{"message":"retry"}}"#,
        );
        retry
            .metadata
            .insert("attempt".to_string(), "1".to_string());
        retry
            .metadata
            .insert("max_attempts".to_string(), "3".to_string());
        retry.metadata.insert(
            run_state_machine::RETRY_SOURCE_META.to_string(),
            run_state_machine::RETRY_SOURCE_RUN_FAILED.to_string(),
        );

        let result = project(&retry, Some(&failed_run));
        let run =
            Run::decode(result.run_data().expect("authorized retry must update run")).unwrap();

        assert_eq!(run.status, "queued");
        assert_eq!(run.attempt, 1);
        assert_eq!(run.lease_epoch, 1);
        assert!(run.error_message.is_empty());
        assert!(run.error_code.is_empty());
        assert!(run.assigned_at.is_none());
        assert!(run.started_at.is_none());
        assert!(run.completed_at.is_none());
        assert_eq!(run.duration_ms, 0);
    }

    #[test]
    fn unmarked_or_stale_retry_cannot_reopen_failed_run() {
        let failed_run = Run {
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: "failed".to_string(),
            attempt: 1,
            ..Default::default()
        }
        .encode_to_vec();
        let mut retry = make_record("run.queued", 3_000_000_000, "{}");
        retry
            .metadata
            .insert("attempt".to_string(), "1".to_string());
        retry
            .metadata
            .insert("max_attempts".to_string(), "3".to_string());

        assert!(project(&retry, Some(&failed_run)).run_data().is_none());

        retry.metadata.insert(
            run_state_machine::RETRY_SOURCE_META.to_string(),
            run_state_machine::RETRY_SOURCE_RUN_FAILED.to_string(),
        );
        assert!(project(&retry, Some(&failed_run)).run_data().is_none());
    }

    #[test]
    fn retry_metadata_does_not_authorize_other_failed_transitions() {
        let failed_run = Run {
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: "failed".to_string(),
            attempt: 0,
            ..Default::default()
        }
        .encode_to_vec();
        let mut started = make_record("run.started", 3_000_000_000, "{}");
        started
            .metadata
            .insert("attempt".to_string(), "1".to_string());
        started
            .metadata
            .insert("max_attempts".to_string(), "3".to_string());
        started.metadata.insert(
            run_state_machine::RETRY_SOURCE_META.to_string(),
            run_state_machine::RETRY_SOURCE_RUN_FAILED.to_string(),
        );

        assert!(project(&started, Some(&failed_run)).run_data().is_none());
    }

    #[test]
    fn duplicate_terminal_failed_is_idempotent() {
        // Regression test for the illegal `failed → failed` transition warning.
        // When a second `run.failed` record lands for an already-failed run
        // (e.g. expiry sweeper races with the worker's own terminal emit),
        // the projection must:
        //   1. still record the duplicate via PutEvent (journal is source of
        //      truth; duplicates need to be auditable),
        //   2. NOT emit PutRun — the existing terminal fields must be
        //      preserved intact,
        //   3. NOT emit any lease/pending index diff ops — the first apply
        //      already cleared them.
        // Walk the run through started → failed to set up realistic stored
        // terminal fields.
        let started = make_record("run.started", 1_000_000_000, "{}");
        let started_data = project(&started, None).run_data().unwrap().to_vec();

        let first_failed = make_record(
            "run.failed",
            2_000_000_000,
            r#"{"error_message":"first failure","error_code":"FIRST"}"#,
        );
        let first_result = project(&first_failed, Some(&started_data));
        let failed_data = first_result.run_data().unwrap().to_vec();
        let first_run = Run::decode(failed_data.as_slice()).unwrap();
        assert_eq!(first_run.status, "failed");
        assert_eq!(first_run.error_message, "first failure");
        assert_eq!(first_run.error_code, "FIRST");
        let first_completed_at = first_run.completed_at;
        let first_duration_ms = first_run.duration_ms;

        // Second run.failed with a different payload — simulates a duplicate
        // terminal emit (e.g. sweeper dead-letter after worker already wrote
        // its own run.failed). The second record carries a dead_letter_reason
        // tag in metadata so the enriched warn/metric has a producer signal.
        let mut second_failed = make_record(
            "run.failed",
            9_999_999_999,
            r#"{"error_message":"sweeper dead-letter","error_code":"DEAD_LETTER"}"#,
        );
        second_failed
            .metadata
            .insert("dead_letter_reason".to_string(), "lease_expiry".to_string());

        let second_result = project(&second_failed, Some(&failed_data));

        // PutEvent must still fire so the duplicate appears in the journal.
        assert!(
            second_result.has_store_event(),
            "duplicate terminal must still emit PutEvent; ops = {:?}",
            second_result.ops
        );
        // PutRun must NOT fire — this is the whole point of the invariant.
        assert!(
            second_result.run_data().is_none(),
            "duplicate terminal must NOT emit PutRun; ops = {:?}",
            second_result.ops
        );
        // Lease / pending index diffs must not fire either — the first
        // terminal already released them, so a duplicate shouldn't churn
        // the secondary indexes.
        assert!(
            !second_result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex { .. }
                    | StoreOp::RemoveLeaseIndex { .. }
                    | StoreOp::PutPendingRun { .. }
                    | StoreOp::RemovePendingRun { .. }
                    | StoreOp::PutPendingPriority { .. }
                    | StoreOp::RemovePendingPriority { .. }
                    | StoreOp::PutPendingDeploymentPriority { .. }
                    | StoreOp::RemovePendingDeploymentPriority { .. }
                    | StoreOp::UpdateQueueCounters { .. }
            )),
            "duplicate terminal must not emit any index diff ops; ops = {:?}",
            second_result.ops
        );

        // Sanity: re-decoding the stored Run (unchanged bytes) still shows
        // the first failure's fields. We reassert on `first_run` since the
        // projection did not produce new Run bytes.
        assert_eq!(first_run.error_message, "first failure");
        assert_eq!(first_run.error_code, "FIRST");
        assert_eq!(first_run.completed_at, first_completed_at);
        assert_eq!(first_run.duration_ms, first_duration_ms);
    }

    fn at(event_type: &str, offset: u64) -> Record {
        let mut record = make_record(event_type, offset as i64 * 1_000_000_000, "{}");
        record.offset = Offset(offset);
        record
    }

    /// Projects `records` in order and returns the run each one leaves.
    fn project_all(records: &[Record]) -> Vec<Option<Run>> {
        let mut data: Option<Vec<u8>> = None;
        records
            .iter()
            .map(|record| {
                if let Some(next) = project(record, data.as_deref()).run_data() {
                    data = Some(next.to_vec());
                }
                data.as_deref().map(|bytes| Run::decode(bytes).unwrap())
            })
            .collect()
    }

    fn first_dispatch(records: &[Record]) -> Option<u64> {
        project_all(records)
            .last()
            .cloned()
            .flatten()
            .and_then(|run| run.first_dispatch_offset)
    }

    #[test]
    fn first_dispatch_is_the_first_assignment_and_never_moves() {
        let runs = project_all(&[
            at("run.queued", 1),
            at("run.assigned", 2),
            at("run.started", 3),
            at("run.lease_expired", 4),
            at("run.assigned", 5),
        ]);
        assert_eq!(runs[0].as_ref().unwrap().first_dispatch_offset, None);
        for run in &runs[1..] {
            assert_eq!(run.as_ref().unwrap().first_dispatch_offset, Some(2));
        }
    }

    #[test]
    fn a_start_that_lands_before_its_assignment_is_the_first_dispatch() {
        assert_eq!(
            first_dispatch(&[
                at("run.queued", 1),
                at("run.started", 2),
                at("run.assigned", 3),
                at("run.completed", 4),
            ]),
            Some(2)
        );
    }

    #[test]
    fn a_reset_by_run_queued_keeps_the_first_dispatch() {
        // A new `run.queued` builds the row from scratch.
        let assigned = project_all(&[at("run.queued", 1), at("run.assigned", 2)])
            .pop()
            .flatten()
            .unwrap()
            .encode_to_vec();
        let requeued = project(&at("run.queued", 3), Some(&assigned));
        let requeued = Run::decode(requeued.run_data().unwrap()).unwrap();
        assert_eq!(requeued.first_dispatch_offset, Some(2));
    }

    #[test]
    fn a_late_assignment_pins_a_live_row_but_never_a_terminal_one() {
        // A workflow paused by a fast worker before any dispatch record.
        let paused = project_all(&[at("run.queued", 1), at("workflow.paused", 2)])
            .pop()
            .flatten()
            .unwrap();
        assert_eq!(paused.first_dispatch_offset, None);
        let late = project(&at("run.assigned", 3), Some(&paused.encode_to_vec()));
        let pinned = Run::decode(late.run_data().expect("pinned row")).unwrap();
        assert_eq!(pinned.first_dispatch_offset, Some(3));
        assert_eq!(pinned.status, "paused");
        assert!(pinned.lease_id.is_empty());

        // A run that ended before any dispatch record: rewriting its row
        // would stop retention from deleting it.
        let completed = project_all(&[at("run.queued", 1), at("run.completed", 2)])
            .pop()
            .flatten()
            .unwrap();
        let late = project(&at("run.assigned", 3), Some(&completed.encode_to_vec()));
        assert!(late.run_data().is_none(), "ops = {:?}", late.ops);
    }

    #[test]
    fn an_observed_run_has_no_dispatch() {
        assert_eq!(
            first_dispatch(&[
                observed("run.queued", 1_000_000_000, 1_000, ""),
                observed("run.started", 2_000_000_000, 2_000, ""),
            ]),
            None
        );
    }

    #[test]
    fn late_assigned_after_completed_does_not_regress_terminal_run() {
        let queued = make_record("run.queued", 1_000_000_000, "{}");
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let started = make_record("run.started", 2_000_000_000, "{}");
        let started_data = project(&started, Some(&queued_data))
            .run_data()
            .unwrap()
            .to_vec();

        let completed = make_record(
            "run.completed",
            3_000_000_000,
            r#"{"output_data":{"ok":true}}"#,
        );
        let completed_result = project(&completed, Some(&started_data));
        let completed_data = completed_result.run_data().unwrap().to_vec();
        let completed_run = Run::decode(completed_data.as_slice()).unwrap();
        assert_eq!(completed_run.status, "completed");

        let late_assigned = make_record("run.assigned", 4_000_000_000, "{}");
        let late_result = project(&late_assigned, Some(&completed_data));

        assert!(
            late_result.has_store_event(),
            "late lifecycle event must remain auditable; ops = {:?}",
            late_result.ops
        );
        assert!(
            late_result.run_data().is_none(),
            "late run.assigned must not overwrite completed state; ops = {:?}",
            late_result.ops
        );
        assert!(
            !late_result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex { .. }
                    | StoreOp::RemoveLeaseIndex { .. }
                    | StoreOp::PutPendingRun { .. }
                    | StoreOp::RemovePendingRun { .. }
                    | StoreOp::PutPendingPriority { .. }
                    | StoreOp::RemovePendingPriority { .. }
                    | StoreOp::PutPendingDeploymentPriority { .. }
                    | StoreOp::RemovePendingDeploymentPriority { .. }
                    | StoreOp::UpdateQueueCounters { .. }
            )),
            "late run.assigned must not churn secondary indexes; ops = {:?}",
            late_result.ops
        );
    }

    #[test]
    fn late_assigned_after_started_preserves_started_status_and_adds_lease() {
        let queued = make_record("run.queued", 1_000_000_000, "{}");
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let started = make_record("run.started", 2_000_000_000, "{}");
        let started_result = project(&started, Some(&queued_data));
        let started_data = started_result.run_data().unwrap().to_vec();
        let started_run = Run::decode(started_data.as_slice()).unwrap();
        assert_eq!(started_run.status, "started");
        assert!(started_run.started_at.is_some());
        assert!(started_run.lease_id.is_empty());

        let mut late_assigned = make_record("run.assigned", 3_000_000_000, "{}");
        late_assigned
            .metadata
            .insert("lease_id".to_string(), "01HRACE".to_string());
        late_assigned
            .metadata
            .insert("lease_worker_id".to_string(), "worker-race".to_string());
        late_assigned
            .metadata
            .insert("lease_expires_at_ms".to_string(), "123456".to_string());
        late_assigned
            .metadata
            .insert("lease_timeout_ms".to_string(), "60000".to_string());

        let late_result = project(&late_assigned, Some(&started_data));
        let run = Run::decode(late_result.run_data().unwrap()).unwrap();

        assert_eq!(run.status, "started");
        assert_eq!(run.started_at, started_run.started_at);
        assert_eq!(run.lease_id, "01HRACE");
        assert_eq!(run.lease_worker_id, "worker-race");
        assert_eq!(run.lease_expires_at_ms, 123456);
        assert_eq!(run.lease_timeout_ms, 60000);
        assert_eq!(run.lease_epoch, 1);
        assert!(
            late_result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex {
                    expires_at_ms: 123456,
                    worker_id,
                } if worker_id == "worker-race"
            )),
            "late assigned should still index the lease; ops = {:?}",
            late_result.ops
        );
    }

    #[test]
    fn late_assigned_after_paused_does_not_restore_lease() {
        let queued = make_record("run.queued", 1_000_000_000, "{}");
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let started = make_record("run.started", 2_000_000_000, "{}");
        let started_data = project(&started, Some(&queued_data))
            .run_data()
            .unwrap()
            .to_vec();

        let paused = make_record("workflow.paused", 3_000_000_000, "{}");
        let paused_data = project(&paused, Some(&started_data))
            .run_data()
            .unwrap()
            .to_vec();
        let paused_run = Run::decode(paused_data.as_slice()).unwrap();
        assert_eq!(paused_run.status, "paused");
        assert!(paused_run.lease_id.is_empty());

        let mut late_assigned = make_record("run.assigned", 4_000_000_000, "{}");
        late_assigned
            .metadata
            .insert("lease_id".to_string(), "01HPAUSED".to_string());
        late_assigned
            .metadata
            .insert("lease_worker_id".to_string(), "worker-paused".to_string());
        late_assigned
            .metadata
            .insert("lease_expires_at_ms".to_string(), "234567".to_string());

        let late_result = project(&late_assigned, Some(&paused_data));

        assert!(
            late_result.has_store_event(),
            "late assigned must remain auditable; ops = {:?}",
            late_result.ops
        );
        assert!(
            late_result.run_data().is_none(),
            "late assigned must not overwrite paused state; ops = {:?}",
            late_result.ops
        );
        assert!(
            !late_result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex { .. }
                    | StoreOp::RemoveLeaseIndex { .. }
                    | StoreOp::PutPendingRun { .. }
                    | StoreOp::RemovePendingRun { .. }
                    | StoreOp::PutPendingPriority { .. }
                    | StoreOp::RemovePendingPriority { .. }
                    | StoreOp::PutPendingDeploymentPriority { .. }
                    | StoreOp::RemovePendingDeploymentPriority { .. }
                    | StoreOp::UpdateQueueCounters { .. }
            )),
            "late assigned after pause must not churn indexes; ops = {:?}",
            late_result.ops
        );
    }

    #[test]
    fn legacy_checkpoint_extracts_memo() {
        let mut record = make_record("checkpoint.created", 1_000_000_000, r#"{"result":42}"#);
        record.step_key = Some("step.calculate".to_string());

        let result = project(&record, None);
        assert!(result.run_data().is_none()); // Not a run lifecycle event
        let (key, data) = result.step_memo().unwrap();
        assert_eq!(key, "step.calculate");
        assert_eq!(data, br#"{"result":42}"#);
    }

    #[test]
    fn unknown_event_type_stores_event_only() {
        let record = make_record("lm.message.delta", 1_000_000_000, "{}");
        let result = project(&record, None);
        assert!(result.run_data().is_none());
        assert!(result.step_memo().is_none());
        assert!(result.has_store_event());
    }

    // --- Lease-aware projection tests ---

    #[test]
    fn assigned_with_lease_metadata_emits_put_lease_index() {
        // First queue the run so we have an existing Run proto to transition from.
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        // Now assign with lease metadata attached.
        let mut assigned = make_record("run.assigned", 2_000_000_000, "{}");
        assigned
            .metadata
            .insert("lease_id".to_string(), "01HXYZ123".to_string());
        assigned
            .metadata
            .insert("lease_worker_id".to_string(), "worker-1".to_string());
        assigned
            .metadata
            .insert("lease_expires_at_ms".to_string(), "120000".to_string());
        assigned
            .metadata
            .insert("lease_timeout_ms".to_string(), "60000".to_string());

        let result = project(&assigned, Some(&queued_data));

        // Run proto should carry lease fields.
        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.status, "assigned");
        assert_eq!(run.assigned_at.as_ref().unwrap().seconds, 2);
        assert_eq!(run.lease_id, "01HXYZ123");
        assert_eq!(run.lease_worker_id, "worker-1");
        assert_eq!(run.lease_expires_at_ms, 120000);
        assert_eq!(run.lease_timeout_ms, 60000);
        assert_eq!(run.lease_epoch, 1); // first lease

        // A PutLeaseIndex op should be emitted with matching values.
        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex {
                    expires_at_ms: 120000,
                    worker_id,
                } if worker_id == "worker-1"
            )),
            "expected PutLeaseIndex op; ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn duplicate_assigned_does_not_replace_existing_lease() {
        let existing = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            status: "assigned".into(),
            lease_id: "lease-first".into(),
            lease_worker_id: "worker-1".into(),
            lease_expires_at_ms: 120_000,
            lease_timeout_ms: 60_000,
            lease_epoch: 1,
            ..Default::default()
        }
        .encode_to_vec();

        let mut duplicate = make_record("run.assigned", 2_000_000_000, "{}");
        duplicate
            .metadata
            .insert("lease_id".to_string(), "lease-second".to_string());
        duplicate
            .metadata
            .insert("lease_worker_id".to_string(), "worker-2".to_string());
        duplicate
            .metadata
            .insert("lease_expires_at_ms".to_string(), "180000".to_string());
        duplicate
            .metadata
            .insert("lease_timeout_ms".to_string(), "60000".to_string());

        let result = project(&duplicate, Some(&existing));

        assert!(result.has_store_event());
        assert!(
            result.run_data().is_none(),
            "duplicate assignment must not rewrite the Run proto; ops = {:?}",
            result.ops
        );
        assert!(
            !result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex { .. }
                    | StoreOp::RemoveLeaseIndex { .. }
                    | StoreOp::PutPendingRun { .. }
                    | StoreOp::RemovePendingRun { .. }
                    | StoreOp::PutPendingPriority { .. }
                    | StoreOp::RemovePendingPriority { .. }
                    | StoreOp::PutPendingDeploymentPriority { .. }
                    | StoreOp::RemovePendingDeploymentPriority { .. }
                    | StoreOp::UpdateQueueCounters { .. }
            )),
            "duplicate assignment must not churn indexes; ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn stale_assignment_attempt_does_not_claim_newer_retry_generation() {
        let existing = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            status: "queued".into(),
            attempt: 3,
            ..Default::default()
        }
        .encode_to_vec();

        let mut stale = make_record("run.assigned", 2_000_000_000, "{}");
        stale
            .metadata
            .insert("lease_id".to_string(), "lease-attempt-2".to_string());
        stale
            .metadata
            .insert(RUN_META_LEASE_ATTEMPT.to_string(), "2".to_string());
        stale
            .metadata
            .insert("lease_expires_at_ms".to_string(), "180000".to_string());

        let result = project(&stale, Some(&existing));

        assert!(result.has_store_event());
        assert!(
            result.run_data().is_none(),
            "stale assignment must leave the retry generation queued; ops = {:?}",
            result.ops
        );
        assert!(
            !result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex { .. }
                    | StoreOp::RemoveLeaseIndex { .. }
                    | StoreOp::PutPendingRun { .. }
                    | StoreOp::RemovePendingRun { .. }
                    | StoreOp::PutPendingPriority { .. }
                    | StoreOp::RemovePendingPriority { .. }
                    | StoreOp::PutPendingDeploymentPriority { .. }
                    | StoreOp::RemovePendingDeploymentPriority { .. }
                    | StoreOp::UpdateQueueCounters { .. }
            )),
            "stale assignment must not mutate queue or lease indexes; ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn assigned_without_lease_metadata_is_backward_compat() {
        // A plain run.assigned (no lease_id in metadata) must behave exactly
        // as before: no lease fields populated, no lease index ops emitted.
        let queued = make_record("run.queued", 1_000_000_000, "{}");
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let assigned = make_record("run.assigned", 2_000_000_000, "{}");
        let result = project(&assigned, Some(&queued_data));

        let run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(run.status, "assigned");
        assert!(run.lease_id.is_empty());
        assert_eq!(run.lease_epoch, 0);

        assert!(
            !result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex { .. } | StoreOp::RemoveLeaseIndex { .. }
            )),
            "no lease index ops should be emitted; ops = {:?}",
            result.ops
        );
    }

    /// Build a Run proto with an active lease, in the given non-terminal
    /// status. Used by terminal-event tests to set up the "pre-terminal"
    /// projection state without running the full queued/assigned path.
    fn run_with_active_lease(status: &str, lease_worker: &str, expires_at_ms: i64) -> Vec<u8> {
        let mut run = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            status: status.into(),
            lease_id: "01HXYZ456".into(),
            lease_worker_id: lease_worker.into(),
            lease_expires_at_ms: expires_at_ms,
            lease_created_at_ms: 1_000,
            lease_timeout_ms: 60_000,
            lease_epoch: 2,
            ..Default::default()
        };
        run.started_at = Some(ns_to_timestamp(1_000_000_000));
        run.encode_to_vec()
    }

    #[test]
    fn completed_clears_lease_and_emits_remove_index() {
        let started_data = run_with_active_lease("started", "worker-2", 999_999);

        let completed = make_record(
            "run.completed",
            5_000_000_000,
            r#"{"output_data":{"result":42}}"#,
        );
        let result = project(&completed, Some(&started_data));

        let new_run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(new_run.status, "completed");
        // Lease fields cleared on terminal transition.
        assert!(new_run.lease_id.is_empty());
        assert!(new_run.lease_worker_id.is_empty());
        assert_eq!(new_run.lease_expires_at_ms, 0);
        assert_eq!(new_run.lease_timeout_ms, 0);
        // Epoch preserved for observability.
        assert_eq!(new_run.lease_epoch, 2);

        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::RemoveLeaseIndex {
                    expires_at_ms: 999_999,
                    worker_id,
                } if worker_id == "worker-2"
            )),
            "expected RemoveLeaseIndex(999999, worker-2); ops = {:?}",
            result.ops
        );
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutLeaseIndex { .. })),
            "no PutLeaseIndex should fire on completion; ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn events_without_run_mutations_preserve_lease_indexes() {
        let started_data = run_with_active_lease("started", "worker-oom", 300_000);
        for event_type in [
            "function.started",
            "function.completed",
            "checkpoint.created",
            "lm.completed",
            "custom.event",
        ] {
            let record = make_record(event_type, 2_000_000_000, "{}");
            let result = project(&record, Some(&started_data));
            assert!(result.has_store_event());
            assert!(result.run_data().is_none());
            assert!(
                !result.ops.iter().any(|op| matches!(
                    op,
                    StoreOp::PutLeaseIndex { .. } | StoreOp::RemoveLeaseIndex { .. }
                )),
                "{event_type} must keep the worker's active lease indexed: {:?}",
                result.ops
            );
        }
    }

    #[test]
    fn failed_clears_lease_and_emits_remove_index() {
        let started_data = run_with_active_lease("started", "worker-3", 123_456);

        let failed = make_record(
            "run.failed",
            2_000_000_000,
            r#"{"error_message":"boom","error_code":"INTERNAL"}"#,
        );
        let result = project(&failed, Some(&started_data));

        let new_run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(new_run.status, "failed");
        assert_eq!(new_run.error_message, "boom");
        assert!(new_run.lease_id.is_empty());

        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::RemoveLeaseIndex {
                    expires_at_ms: 123_456,
                    worker_id,
                } if worker_id == "worker-3"
            )),
            "expected RemoveLeaseIndex(123456, worker-3); ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn failed_preserves_current_error_class_and_stack() {
        let mut started =
            Run::decode(run_with_active_lease("started", "worker-3", 123_456).as_slice()).unwrap();
        started
            .metadata
            .insert("error_type".into(), "OldError".into());
        started
            .metadata
            .insert("error_stack".into(), "old stack".into());
        let typed = make_record(
            "run.failed",
            2_000_000_000,
            r#"{"error_message":"boom","error_code":"INTERNAL","error_type":"TypeError","error_stack":"TypeError: boom at handler"}"#,
        );
        let result = project(&typed, Some(&started.encode_to_vec()));
        let failed = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(
            failed.metadata.get("error_type").map(String::as_str),
            Some("TypeError")
        );
        assert_eq!(
            failed.metadata.get("error_stack").map(String::as_str),
            Some("TypeError: boom at handler")
        );

        let untyped = make_record(
            "run.failed",
            2_000_000_000,
            r#"{"error_message":"new failure"}"#,
        );
        let result = project(&untyped, Some(&started.encode_to_vec()));
        let failed = Run::decode(result.run_data().unwrap()).unwrap();
        assert!(!failed.metadata.contains_key("error_type"));
        assert!(!failed.metadata.contains_key("error_stack"));
    }

    #[test]
    fn cancelled_clears_lease_and_emits_remove_index() {
        let assigned_data = run_with_active_lease("assigned", "worker-4", 555);

        let cancelled = make_record("run.cancelled", 4_000_000_000, "{}");
        let result = project(&cancelled, Some(&assigned_data));

        let new_run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(new_run.status, "cancelled");
        assert!(new_run.lease_id.is_empty());

        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::RemoveLeaseIndex {
                    expires_at_ms: 555,
                    worker_id,
                } if worker_id == "worker-4"
            )),
            "expected RemoveLeaseIndex(555, worker-4); ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn terminal_without_lease_emits_no_lease_ops() {
        // A run that was never lease-backed must not
        // produce any lease index ops on terminal transitions.
        let queued = make_record("run.queued", 1_000_000_000, "{}");
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let completed = make_record("run.completed", 2_000_000_000, "{}");
        let result = project(&completed, Some(&queued_data));

        assert!(
            !result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutLeaseIndex { .. } | StoreOp::RemoveLeaseIndex { .. }
            )),
            "no lease ops expected for a non-lease-backed run; ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn lease_expired_keeps_the_worker_termination_of_each_attempt() {
        // Attempt 2 of a run (0-based 1) whose worker was OOM-killed.
        let run = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            status: "started".into(),
            lease_id: "lease-2".into(),
            lease_worker_id: "worker-2".into(),
            attempt: 1,
            ..Default::default()
        };
        let mut killed = make_record("run.lease_expired", 3_000_000_000, "{}");
        killed.metadata.insert(
            run_state_machine::WORKER_TERMINATION_REASON_META.to_string(),
            "OOMKilled".to_string(),
        );
        // The history is the run's own; a value on the record is ignored.
        killed.metadata.insert(
            run_state_machine::LEASE_WORKER_TERMINATIONS_META.to_string(),
            r#"[[9,"forged"]]"#.to_string(),
        );
        let after_kill = Run::decode(
            project(&killed, Some(&run.encode_to_vec()))
                .run_data()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(after_kill.attempt, 2);
        assert_eq!(
            run_state_machine::lease_worker_terminations(&after_kill.metadata),
            vec![(2, "OOMKilled".to_string())]
        );

        // A plain timeout on the next attempt adds nothing and keeps the kill.
        let mut timed_out = after_kill.clone();
        timed_out.status = "started".into();
        let timeout = make_record("run.lease_expired", 4_000_000_000, "{}");
        let after_timeout = Run::decode(
            project(&timeout, Some(&timed_out.encode_to_vec()))
                .run_data()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            run_state_machine::lease_worker_terminations(&after_timeout.metadata),
            vec![(2, "OOMKilled".to_string())]
        );
    }

    #[test]
    fn lease_expired_on_a_worker_disconnect_records_the_attempt() {
        // A push dispatch's worker dropped its stream on attempt 1 (0-based 0):
        // disconnect recovery names no container reason, so the attempt used
        // to leave no trace and a later failure didn't mention it.
        let run = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            status: "started".into(),
            lease_id: "lease-1".into(),
            lease_worker_id: "worker-1".into(),
            attempt: 0,
            ..Default::default()
        };
        let mut dropped = make_record("run.lease_expired", 3_000_000_000, "{}");
        dropped.metadata.insert(
            run_state_machine::LEASE_RECOVERY_REASON_META.to_string(),
            run_state_machine::WORKER_DISCONNECT_RECOVERY.to_string(),
        );
        let after = Run::decode(
            project(&dropped, Some(&run.encode_to_vec()))
                .run_data()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            run_state_machine::lease_worker_terminations(&after.metadata),
            vec![(1, run_state_machine::WORKER_DISCONNECTED_REASON.to_string())]
        );

        // A container reason on the record still wins over the disconnect.
        let mut killed = dropped.clone();
        killed.metadata.insert(
            run_state_machine::WORKER_TERMINATION_REASON_META.to_string(),
            "OOMKilled".to_string(),
        );
        let after_kill = Run::decode(
            project(&killed, Some(&run.encode_to_vec()))
                .run_data()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            run_state_machine::lease_worker_terminations(&after_kill.metadata),
            vec![(1, "OOMKilled".to_string())]
        );

        // Another recovery reason, or none, records nothing.
        let mut other = make_record("run.lease_expired", 3_000_000_000, "{}");
        other.metadata.insert(
            run_state_machine::LEASE_RECOVERY_REASON_META.to_string(),
            "timeout".to_string(),
        );
        let after_other = Run::decode(
            project(&other, Some(&run.encode_to_vec()))
                .run_data()
                .unwrap(),
        )
        .unwrap();
        assert!(run_state_machine::lease_worker_terminations(&after_other.metadata).is_empty());
    }

    #[test]
    fn lease_expired_re_queues_and_bumps_attempt() {
        // Build an assigned run with active lease.
        let mut run = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            component_name: "my_func".into(),
            status: "assigned".into(),
            lease_id: "01HXYZ789".into(),
            lease_worker_id: "worker-3".into(),
            lease_expires_at_ms: 100,
            lease_epoch: 5,
            attempt: 0,
            ..Default::default()
        };
        run.metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let assigned_data = run.encode_to_vec();

        // Sweeper fires run.lease_expired.
        let mut expired = make_record("run.lease_expired", 3_000_000_000, "{}");
        expired.offset = Offset(30);
        let result = project(&expired, Some(&assigned_data));

        // New Run has lease cleared, status=queued, attempt bumped.
        let new_run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(new_run.status, "queued");
        assert_eq!(new_run.attempt, 1);
        assert_eq!(
            new_run
                .metadata
                .get(RUN_META_DISPATCH_GENERATION)
                .map(String::as_str),
            Some("30")
        );
        assert!(new_run.lease_id.is_empty());
        assert!(new_run.lease_worker_id.is_empty());
        assert_eq!(new_run.lease_expires_at_ms, 0);
        // Epoch is preserved; it bumps on the NEXT lease creation.
        assert_eq!(new_run.lease_epoch, 5);

        // RemoveLeaseIndex should fire with the OLD values (expires=100, worker-3).
        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::RemoveLeaseIndex {
                    expires_at_ms: 100,
                    worker_id,
                } if worker_id == "worker-3"
            )),
            "expected RemoveLeaseIndex(100, worker-3); ops = {:?}",
            result.ops
        );

        // PutPendingRun should re-queue the run so pollers see it again.
        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutPendingRun { component_name, .. } if component_name == "my_func"
            )),
            "expected PutPendingRun(my_func); ops = {:?}",
            result.ops
        );

        // No PutLeaseIndex (the run is back in queued, no active lease).
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutLeaseIndex { .. })),
            "no PutLeaseIndex should fire when re-queueing; ops = {:?}",
            result.ops
        );
    }

    // --- Priority-aware pending-index dual-write tests ---

    /// Assert the projection emitted a `PutPendingPriority` with the given
    /// canonical fields. Dumps all ops on failure for debuggability.
    fn assert_put_priority(
        ops: &[StoreOp],
        expected_deployment: &str,
        expected_component: &str,
        expected_priority: u8,
        expected_created_at_ms: i64,
    ) {
        let found = ops.iter().any(|op| {
            matches!(
                op,
                StoreOp::PutPendingPriority {
                    deployment_id,
                    component_type: _,
                    component_name,
                    priority,
                    ready_at_ms,
                    created_at_ms,
                } if deployment_id == expected_deployment
                    && component_name == expected_component
                    && *priority == expected_priority
                    && *ready_at_ms == expected_created_at_ms
                    && *created_at_ms == expected_created_at_ms
            )
        });
        assert!(
            found,
            "expected PutPendingPriority(deployment={expected_deployment}, component={expected_component}, priority={expected_priority}, ready_at={expected_created_at_ms}, created_at={expected_created_at_ms}); ops = {ops:#?}"
        );
    }

    /// Assert the projection emitted a `PutPendingDeploymentPriority`.
    fn assert_put_deployment_priority(
        ops: &[StoreOp],
        expected_deployment: &str,
        expected_priority: u8,
        expected_created_at_ms: i64,
    ) {
        let found = ops.iter().any(|op| {
            matches!(
                op,
                StoreOp::PutPendingDeploymentPriority {
                    deployment_id,
                    priority,
                    ready_at_ms,
                    created_at_ms,
                } if deployment_id == expected_deployment
                    && *priority == expected_priority
                    && *ready_at_ms == expected_created_at_ms
                    && *created_at_ms == expected_created_at_ms
            )
        });
        assert!(
            found,
            "expected PutPendingDeploymentPriority(deployment={expected_deployment}, priority={expected_priority}, ready_at={expected_created_at_ms}, created_at={expected_created_at_ms}); ops = {ops:#?}"
        );
    }

    fn assert_queue_counter_delta(
        ops: &[StoreOp],
        expected_deployment: &str,
        expected_priority: u8,
        expected_created_at_ms: i64,
        expected_delta: i64,
    ) {
        let found = ops.iter().any(|op| {
            matches!(
                op,
                StoreOp::UpdateQueueCounters {
                    deployment_id,
                    priority,
                    ready_at_ms,
                    created_at_ms,
                    delta,
                } if deployment_id == expected_deployment
                    && *priority == expected_priority
                    && *ready_at_ms == expected_created_at_ms
                    && *created_at_ms == expected_created_at_ms
                    && *delta == expected_delta
            )
        });
        assert!(
            found,
            "expected UpdateQueueCounters(deployment={expected_deployment}, priority={expected_priority}, ready_at={expected_created_at_ms}, created_at={expected_created_at_ms}, delta={expected_delta}); ops = {ops:#?}"
        );
    }

    #[test]
    fn queued_batch_dual_writes_pending_priority() {
        // A batch /submit with no explicit priority should land in the
        // NORMAL bucket (canonicalized from UNSPECIFIED) with created_at_ms
        // derived from the submitted_at timestamp.
        let mut queued = make_record("run.queued", 2_500_000_000, "{}"); // 2.5s
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());
        queued
            .metadata
            .insert("batch_id".to_string(), "batch-123".to_string());

        let result = project(&queued, None);

        // The legacy index write remains for compatibility.
        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutPendingRun { component_name, .. } if component_name == "my_func"
        )));

        // New shadow write: NORMAL (2), created_at_ms = 2500.
        assert_put_priority(&result.ops, "dep-1", "my_func", 2, 2500);
        assert_put_deployment_priority(&result.ops, "dep-1", 2, 2500);
        assert_queue_counter_delta(&result.ops, "dep-1", 2, 2500, 1);
    }

    #[test]
    fn unscoped_queued_run_does_not_write_pending_indexes() {
        // Runs without a deployment, batch id, or AI worker marker don't hit
        // the pending index at all.
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());

        let result = project(&queued, None);

        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutPendingRun { .. })),
            "unscoped run should not write old pending index; ops = {:?}",
            result.ops
        );
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutPendingPriority { .. })),
            "unscoped run should not write new pending index either; ops = {:?}",
            result.ops
        );
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutPendingDeploymentPriority { .. })),
            "unscoped run should not write deployment pending index either; ops = {:?}",
            result.ops
        );
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::UpdateQueueCounters { .. })),
            "unscoped run should not update queue counters either; ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn queued_deployment_run_dual_writes_pending_priority_without_batch_id() {
        let mut queued = make_record("run.queued", 2_500_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());

        let result = project(&queued, None);

        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutPendingRun { component_name, .. } if component_name == "my_func"
        )));
        assert_put_priority(&result.ops, "dep-1", "my_func", 2, 2500);
        assert_put_deployment_priority(&result.ops, "dep-1", 2, 2500);
        assert_queue_counter_delta(&result.ops, "dep-1", 2, 2500, 1);
    }

    #[test]
    fn terminal_batch_emits_remove_pending_priority_with_matching_key() {
        // Build queued run state so we know the exact submitted_at the
        // RemovePendingPriority must echo back.
        let mut queued = make_record("run.queued", 3_000_000_000, "{}"); // 3s
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());
        queued
            .metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let queued_result = project(&queued, None);
        let queued_data = queued_result.run_data().unwrap().to_vec();

        // Sanity: the PutPendingPriority landed with created_at_ms=3000.
        assert_put_priority(&queued_result.ops, "dep-1", "my_func", 2, 3000);
        assert_put_deployment_priority(&queued_result.ops, "dep-1", 2, 3000);
        assert_queue_counter_delta(&queued_result.ops, "dep-1", 2, 3000, 1);

        // Now transition to completed. The RemovePendingPriority MUST use
        // the same (deployment, component, priority, created_at_ms) tuple
        // from the existing Run proto, regardless of fallback_ts_ns at the
        // terminal event.
        let completed = make_record("run.completed", 9_999_999_999, "{}");
        let completed_result = project(&completed, Some(&queued_data));

        // Old remove still there.
        assert!(completed_result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemovePendingRun { component_name } if component_name == "my_func"
        )));

        // New remove must match the original Put key exactly.
        let found = completed_result.ops.iter().any(|op| {
            matches!(
                op,
                StoreOp::RemovePendingPriority {
                    deployment_id,
                    component_type: _,
                    component_name,
                    priority: 2,
                    ready_at_ms: 3000,
                    created_at_ms: 3000,
                } if deployment_id == "dep-1" && component_name == "my_func"
            )
        });
        assert!(
            found,
            "expected RemovePendingPriority matching the original Put key; ops = {:?}",
            completed_result.ops
        );

        let found = completed_result.ops.iter().any(|op| {
            matches!(
                op,
                StoreOp::RemovePendingDeploymentPriority {
                    deployment_id,
                    priority: 2,
                    ready_at_ms: 3000,
                    created_at_ms: 3000,
                } if deployment_id == "dep-1"
            )
        });
        assert!(
            found,
            "expected RemovePendingDeploymentPriority matching the original Put key; ops = {:?}",
            completed_result.ops
        );
        assert_queue_counter_delta(&completed_result.ops, "dep-1", 2, 3000, -1);
    }

    #[test]
    fn started_after_assigned_does_not_decrement_queue_counter_again() {
        let mut queued = make_record("run.queued", 3_000_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());
        queued
            .metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let queued_data = project(&queued, None).run_data().unwrap().to_vec();

        let assigned = make_record("run.assigned", 4_000_000_000, "{}");
        let assigned_result = project(&assigned, Some(&queued_data));
        assert_queue_counter_delta(&assigned_result.ops, "dep-1", 2, 3000, -1);
        let assigned_data = assigned_result.run_data().unwrap().to_vec();

        let started = make_record("run.started", 5_000_000_000, "{}");
        let started_result = project(&started, Some(&assigned_data));
        assert!(
            !started_result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::UpdateQueueCounters { .. })),
            "started after assigned must not double-decrement counters; ops = {:?}",
            started_result.ops
        );
    }

    #[test]
    fn paused_deployment_run_removes_pending_index_and_queue_counter() {
        let mut queued = make_record("run.queued", 3_000_000_000, "{}");
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());
        let queued_result = project(&queued, None);
        let queued_data = queued_result.run_data().unwrap().to_vec();
        assert_put_priority(&queued_result.ops, "dep-1", "my_func", 2, 3000);
        assert_put_deployment_priority(&queued_result.ops, "dep-1", 2, 3000);
        assert_queue_counter_delta(&queued_result.ops, "dep-1", 2, 3000, 1);

        let paused = make_record("workflow.paused", 5_000_000_000, "{}");
        let paused_result = project(&paused, Some(&queued_data));

        assert!(paused_result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemovePendingRun { component_name } if component_name == "my_func"
        )));
        assert!(paused_result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemovePendingPriority {
                deployment_id,
                component_type: _,
                component_name,
                priority: 2,
                ready_at_ms: 3000,
                created_at_ms: 3000,
            } if deployment_id == "dep-1" && component_name == "my_func"
        )));
        assert!(paused_result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemovePendingDeploymentPriority {
                deployment_id,
                priority: 2,
                ready_at_ms: 3000,
                created_at_ms: 3000,
            } if deployment_id == "dep-1"
        )));
        assert_queue_counter_delta(&paused_result.ops, "dep-1", 2, 3000, -1);
    }

    // --- Pause / resume lease lifecycle ---

    #[test]
    fn paused_clears_lease_and_emits_remove_index() {
        // The bug: apply_paused used to leave the lease fields populated,
        // so the secondary lease index entry persisted. The expiry sweeper
        // would then find the expired lease ~lease_timeout_ms after pause
        // and re-dispatch the paused run, looping the workflow forever.
        // After the fix, pause must release the lease the same way a
        // terminal event does.
        let started_data = run_with_active_lease("started", "worker-9", 777_777);

        let mut paused = make_record("workflow.paused", 5_000_000_000, "{}");
        paused
            .metadata
            .insert("lease_id".to_string(), "01HXYZ456".to_string());
        let result = project(&paused, Some(&started_data));

        let new_run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(new_run.status, "paused");
        // Lease fields cleared on pause.
        assert!(new_run.lease_id.is_empty());
        assert!(new_run.lease_worker_id.is_empty());
        assert_eq!(new_run.lease_expires_at_ms, 0);
        assert_eq!(new_run.lease_timeout_ms, 0);
        // Epoch preserved for observability.
        assert_eq!(new_run.lease_epoch, 2);
        assert_eq!(
            new_run
                .metadata
                .get(engine_core::lease_fence::META_RESUME_SOURCE_ID)
                .map(String::as_str),
            Some("lease:01HXYZ456")
        );

        // Diff-driven projection must emit RemoveLeaseIndex so the
        // sweeper's CF_LEASES_BY_EXPIRY scan no longer finds this entry.
        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::RemoveLeaseIndex {
                    expires_at_ms: 777_777,
                    worker_id,
                } if worker_id == "worker-9"
            )),
            "expected RemoveLeaseIndex(777777, worker-9); ops = {:?}",
            result.ops
        );
        // No PutLeaseIndex on pause.
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutLeaseIndex { .. })),
            "no PutLeaseIndex should fire on pause; ops = {:?}",
            result.ops
        );
    }

    #[test]
    fn run_paused_event_also_clears_lease() {
        // Both `workflow.paused` and `run.paused` route through apply_paused;
        // make sure `run.paused` shares the same lease-clearing behavior.
        let started_data = run_with_active_lease("started", "worker-10", 1_234);
        let paused = make_record("run.paused", 6_000_000_000, "{}");
        let result = project(&paused, Some(&started_data));

        let new_run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(new_run.status, "paused");
        assert!(new_run.lease_id.is_empty());
        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::RemoveLeaseIndex { expires_at_ms: 1_234, worker_id } if worker_id == "worker-10"
        )));
    }

    #[test]
    fn resumed_re_queues_paused_run() {
        // After the fix, resume puts the run back into "queued" status so
        // the next run.assigned is a legal queued→assigned transition and
        // the orchestrator can mint a fresh lease.
        let mut paused_run = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            component_name: "my_func".into(),
            status: "paused".into(),
            lease_epoch: 3,
            ..Default::default()
        };
        // Submitted_at preserved across pause/resume so the priority
        // index keeps a stable key.
        paused_run.submitted_at = Some(ns_to_timestamp(1_500_000_000));
        paused_run.metadata.extend([
            ("lease_id".to_string(), "lease-old".to_string()),
            ("lease_worker_id".to_string(), "worker-old".to_string()),
            ("worker_session_id".to_string(), "session-old".to_string()),
            (
                engine_core::lease_fence::META_FENCE_KIND.to_string(),
                "pull_completion".to_string(),
            ),
            (
                "workerless_checkpoint".to_string(),
                r#"{"steps":{"draft":true}}"#.to_string(),
            ),
        ]);
        let paused_data = paused_run.encode_to_vec();

        let mut resumed = make_record("run.resumed", 7_000_000_000, "{}");
        resumed
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        resumed
            .metadata
            .insert("user_response".to_string(), "Alice".to_string());
        resumed
            .metadata
            .insert("pause_index".to_string(), "0".to_string());
        resumed
            .metadata
            .insert("step_events".to_string(), "[]".to_string());
        resumed.metadata.insert(
            engine_core::lease_fence::META_RESUME_SOURCE_ID.to_string(),
            "lease:lease-new".to_string(),
        );

        let result = project(&resumed, Some(&paused_data));

        let new_run = Run::decode(result.run_data().unwrap()).unwrap();
        assert_eq!(
            new_run.status, "queued",
            "resume must re-queue, not jump straight to started"
        );
        // No lease fields populated yet — orchestrator will mint one next.
        assert!(new_run.lease_id.is_empty());
        // Epoch preserved across the re-queue; the next mint bumps it.
        assert_eq!(new_run.lease_epoch, 3);
        assert_eq!(
            new_run.metadata.get("user_response").map(String::as_str),
            Some("Alice")
        );
        assert_eq!(
            new_run.metadata.get("pause_index").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            new_run.metadata.get("step_events").map(String::as_str),
            Some("[]")
        );
        assert_eq!(
            new_run
                .metadata
                .get("workerless_checkpoint")
                .map(String::as_str),
            Some(r#"{"steps":{"draft":true}}"#)
        );
        assert_eq!(
            new_run
                .metadata
                .get(engine_core::lease_fence::META_RESUME_SOURCE_ID)
                .map(String::as_str),
            Some("lease:lease-new")
        );
        for key in [
            "lease_id",
            "lease_worker_id",
            "worker_session_id",
            engine_core::lease_fence::META_FENCE_KIND,
        ] {
            assert!(
                !new_run.metadata.contains_key(key),
                "queued run metadata must not retain {key}"
            );
        }
    }

    #[test]
    fn resumed_batch_run_re_puts_pending_index() {
        // Pull-based batch workers depend on the pending index. After
        // resume, the batch run must re-appear in CF_RUNS_PENDING (and the
        // priority-aware shadow CF) so the next PollJob picks it up.
        let mut paused_run = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            component_name: "my_func".into(),
            deployment_id: "dep-1".into(),
            status: "paused".into(),
            priority: 3, // BATCH
            ..Default::default()
        };
        paused_run.submitted_at = Some(ns_to_timestamp(1_500_000_000)); // 1.5s
        paused_run
            .metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let paused_data = paused_run.encode_to_vec();

        let mut resumed = make_record("run.resumed", 7_000_000_000, "{}");
        resumed
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());

        let result = project(&resumed, Some(&paused_data));

        // PutPendingRun for the legacy CF.
        assert!(
            result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutPendingRun { component_name, .. } if component_name == "my_func"
            )),
            "expected PutPendingRun(my_func) on resume; ops = {:?}",
            result.ops
        );
        // PutPendingPriority for the priority-aware shadow CF, with the
        // same key fields the original Put used.
        assert_put_priority(&result.ops, "dep-1", "my_func", 3, 1500);
        assert_put_deployment_priority(&result.ops, "dep-1", 3, 1500);
        assert_queue_counter_delta(&result.ops, "dep-1", 3, 1500, 1);
    }

    #[test]
    fn full_pause_resume_round_trip_keeps_indexes_consistent() {
        // End-to-end: queued → assigned (with lease) → paused → resumed.
        // After resume, the lease index must be empty and the pending
        // index must hold a fresh entry. This pins the regression so a
        // future change can't re-introduce the loop.
        let mut queued = make_record("run.queued", 1_000_000_000, "{}");
        queued.offset = Offset(10);
        queued
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        queued
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());
        queued
            .metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let after_queued = project(&queued, None).run_data().unwrap().to_vec();
        assert_eq!(
            Run::decode(after_queued.as_slice()).unwrap().metadata[RUN_META_DISPATCH_GENERATION],
            "10"
        );

        let mut assigned = make_record("run.assigned", 2_000_000_000, "{}");
        assigned
            .metadata
            .insert("lease_id".to_string(), "01HXYZAA".to_string());
        assigned
            .metadata
            .insert("lease_worker_id".to_string(), "worker-X".to_string());
        assigned
            .metadata
            .insert("lease_expires_at_ms".to_string(), "120000".to_string());
        assigned
            .metadata
            .insert("lease_timeout_ms".to_string(), "120000".to_string());
        let after_assigned = project(&assigned, Some(&after_queued))
            .run_data()
            .unwrap()
            .to_vec();
        let assigned_run = Run::decode(after_assigned.as_slice()).unwrap();
        assert_eq!(assigned_run.status, "assigned");
        assert_eq!(assigned_run.lease_id, "01HXYZAA");

        // Pause: must release the lease.
        let paused = make_record("workflow.paused", 3_000_000_000, "{}");
        let pause_result = project(&paused, Some(&after_assigned));
        let paused_run = Run::decode(pause_result.run_data().unwrap()).unwrap();
        assert_eq!(paused_run.status, "paused");
        assert!(paused_run.lease_id.is_empty(), "pause must clear lease_id");
        assert!(
            pause_result.ops.iter().any(|op| matches!(
                op,
                StoreOp::RemoveLeaseIndex { expires_at_ms: 120000, worker_id } if worker_id == "worker-X"
            )),
            "pause must remove lease index; ops = {:?}",
            pause_result.ops
        );
        let after_paused = pause_result.run_data().unwrap().to_vec();

        // Resume: re-queue and re-populate pending index.
        let mut resumed = make_record("run.resumed", 4_000_000_000, "{}");
        resumed.offset = Offset(20);
        resumed
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        let resume_result = project(&resumed, Some(&after_paused));
        let resumed_run = Run::decode(resume_result.run_data().unwrap()).unwrap();
        assert_eq!(resumed_run.status, "queued");
        assert_eq!(resumed_run.metadata[RUN_META_DISPATCH_GENERATION], "20");
        assert!(resumed_run.lease_id.is_empty());
        assert!(
            resume_result.ops.iter().any(|op| matches!(
                op,
                StoreOp::PutPendingRun { component_name, .. } if component_name == "my_func"
            )),
            "resume must re-put pending index; ops = {:?}",
            resume_result.ops
        );
        // No new lease index entry yet; that lands on the next run.assigned.
        assert!(
            !resume_result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutLeaseIndex { .. })),
            "resume must not mint a lease itself; ops = {:?}",
            resume_result.ops
        );
    }

    #[test]
    fn lease_expired_redual_writes_pending_priority() {
        // After lease expiry the run re-enters pending. The priority index
        // must be repopulated with the same key fields (deployment/component/
        // priority/submitted_at are immutable) so poll paths see it again.
        let mut run = Run {
            project_id: "t1".into(),
            run_id: "r1".into(),
            component_name: "my_func".into(),
            deployment_id: "dep-1".into(),
            status: "assigned".into(),
            lease_id: "01HXYZ789".into(),
            lease_worker_id: "worker-3".into(),
            lease_expires_at_ms: 100,
            lease_epoch: 5,
            attempt: 0,
            submitted_at: Some(ns_to_timestamp(1_500_000_000)), // 1.5s
            // BATCH priority set by ingress.
            priority: 3,
            ..Default::default()
        };
        run.metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let assigned_data = run.encode_to_vec();

        let expired = make_record("run.lease_expired", 7_000_000_000, "{}");
        let result = project(&expired, Some(&assigned_data));

        // Priority index repopulated with BATCH (3), created_at=1500.
        assert_put_priority(&result.ops, "dep-1", "my_func", 3, 1500);
        assert_put_deployment_priority(&result.ops, "dep-1", 3, 1500);
        assert_queue_counter_delta(&result.ops, "dep-1", 3, 1500, 1);
    }

    #[test]
    fn canonical_priority_maps_unspecified_to_normal() {
        let run = Run::default();
        assert_eq!(canonical_priority(&run), 2);
        let run = Run {
            priority: 1,
            ..Default::default()
        };
        assert_eq!(canonical_priority(&run), 1);
        let run = Run {
            priority: 3,
            ..Default::default()
        };
        assert_eq!(canonical_priority(&run), 3);
    }

    #[test]
    fn pending_priority_carries_the_canonical_component_type() {
        let run = Run {
            deployment_id: "deployment-a".into(),
            component_name: "score".into(),
            component_type: "scorers".into(),
            ..Default::default()
        };
        assert!(matches!(
            pending_priority_put(&run, 0),
            StoreOp::PutPendingPriority {
                component_type,
                ..
            } if component_type == engine_proto::ComponentType::Scorer as i32
        ));
    }

    #[test]
    fn pull_claimable_policy_includes_batch_from_record_or_existing_run() {
        let mut record = make_record("run.queued", 1_000_000_000, "{}");
        assert!(!is_pull_claimable(&record, None));

        record
            .metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        assert!(is_pull_claimable(&record, None));

        let mut existing = Run::default();
        existing
            .metadata
            .insert("batch_id".to_string(), "batch-1".to_string());
        let record = make_record("run.completed", 2_000_000_000, "{}");
        assert!(is_pull_claimable(&record, Some(&existing)));
    }

    #[test]
    fn pull_claimable_policy_excludes_ai_worker_without_batch_or_deployment() {
        let mut record = make_record("run.queued", 1_000_000_000, "{}");
        record
            .metadata
            .insert("execution_mode".to_string(), "ai_worker".to_string());
        assert!(!is_pull_claimable(&record, None));

        let mut existing = Run::default();
        existing
            .metadata
            .insert("execution_mode".to_string(), "ai_worker".to_string());
        let record = make_record("run.started", 2_000_000_000, "{}");
        assert!(!is_pull_claimable(&record, Some(&existing)));
    }

    #[test]
    fn pull_claimable_policy_includes_deployment_scoped_runs() {
        let mut record = make_record("run.queued", 1_000_000_000, "{}");
        record
            .metadata
            .insert("deployment_id".to_string(), "dep-1".to_string());
        assert!(is_pull_claimable(&record, None));

        let existing = Run {
            deployment_id: "dep-1".to_string(),
            ..Default::default()
        };
        let record = make_record("run.started", 2_000_000_000, "{}");
        assert!(is_pull_claimable(&record, Some(&existing)));
    }

    #[test]
    fn pull_claimable_policy_excludes_unscoped_run() {
        let record = make_record("run.queued", 1_000_000_000, "{}");
        let existing = Run::default();
        assert!(!is_pull_claimable(&record, Some(&existing)));
    }

    #[test]
    fn pending_priority_times_reads_submitted_at() {
        // submitted_at = 12.345s, expect 12345 ms.
        let run = Run {
            submitted_at: Some(prost_types::Timestamp {
                seconds: 12,
                nanos: 345_000_000,
            }),
            ..Default::default()
        };
        let (ready, created) = pending_priority_times(&run, 99_000_000_000);
        assert_eq!(created, 12_345);
        assert_eq!(ready, created, "ready_at must mirror created_at in 3b");
    }

    #[test]
    fn pending_priority_times_falls_back_to_record_ts() {
        let run = Run::default();
        let (ready, created) = pending_priority_times(&run, 4_000_000_000);
        assert_eq!(created, 4_000);
        assert_eq!(ready, created);
    }

    // --- Archive-outbox projection ---

    /// Build an existing-Run-bytes blob in non-terminal state so the
    /// terminal apply path runs (instead of the same-terminal-duplicate
    /// short-circuit).
    fn running_run_bytes() -> Vec<u8> {
        let run = Run {
            workspace_id: "w1".to_string(),
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: "running".to_string(),
            ..Default::default()
        };
        run.encode_to_vec()
    }

    #[test]
    fn terminal_event_emits_outbox_alongside_run() {
        // Terminal-status transitions must emit PutRun and the Vortex
        // archive intent
        // so the durable archive write commits in the same WriteBatch as
        // the Run write. partition_id and terminal_offset come from
        // record.offset via decode_offset; project_id and run_id come
        // from the record.
        let mut record = make_record(
            "run.completed",
            5_000_000_000,
            r#"{"output_data":{"result":"ok"}}"#,
        );
        // partition 7, local offset 42 → encoded offset.
        record.offset = engine_core::partition_offset::encode_offset(7, 42).unwrap();

        let result = project(&record, Some(&running_run_bytes()));

        let mut put_run_seen = false;
        let mut put_outbox = None;
        for op in &result.ops {
            if matches!(op, StoreOp::PutRun(_)) {
                put_run_seen = true;
            }
            if let StoreOp::PutRunQueryArchiveIntent {
                partition_id,
                terminal_offset,
                run_data,
            } = op
            {
                put_outbox = Some((*partition_id, *terminal_offset, run_data.clone()));
            }
        }
        assert!(put_run_seen, "expected PutRun on terminal");
        let (partition_id, terminal_offset, run_data) =
            put_outbox.expect("expected PutRunQueryArchiveIntent on terminal");
        assert_eq!(partition_id, 7);
        assert_eq!(terminal_offset, 42);
        // Outbox value is the freshly-projected Run proto — drain decodes
        // it without needing CF_RUNS.
        let decoded = Run::decode(run_data.as_slice()).expect("outbox value is Run proto");
        assert_eq!(decoded.run_id, "r1");
        assert_eq!(decoded.status, "completed");
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutRunArchiveIntent { .. })));
    }

    #[test]
    fn retryable_failure_does_not_emit_terminal_outbox() {
        let mut record = make_record(
            "run.failed",
            5_000_000_000,
            r#"{"error_message":"temporary","error_code":"UNAVAILABLE"}"#,
        );
        record
            .metadata
            .insert("attempt".to_string(), "0".to_string());
        record
            .metadata
            .insert("max_attempts".to_string(), "3".to_string());
        record.metadata.insert(
            run_state_machine::RETRY_INITIAL_BACKOFF_MS_META.to_string(),
            "250".to_string(),
        );

        let result = project(&record, Some(&running_run_bytes()));

        let projected = Run::decode(result.run_data().expect("retry-delayed run")).unwrap();
        assert_eq!(projected.status, "retry_delayed");
        assert_eq!(
            projected
                .metadata
                .get(run_state_machine::RETRY_READY_AT_MS_META)
                .map(String::as_str),
            Some("5250")
        );

        assert!(result.ops.iter().any(|op| matches!(op, StoreOp::PutRun(_))));
        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutTerminalObligation {
                partition_id: 0,
                run_data,
            } if Run::decode(run_data.as_slice()).is_ok_and(|run| {
                run.status == "retry_delayed"
                    && run.metadata
                        .get(run_state_machine::TERMINAL_OBLIGATION_OFFSET_META)
                        .map(String::as_str)
                        == Some("0")
            })
        )));
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutRunQueryArchiveIntent { .. })),
            "a retryable failure must not be archived as the final run"
        );
    }

    #[test]
    fn timed_pause_keeps_a_durable_obligation_and_resume_clears_deadline() {
        let queued = project(&make_record("run.queued", 1_000_000_000, "{}"), None);
        let queued = queued.run_data().unwrap().to_vec();
        let started = project(
            &make_record("run.started", 2_000_000_000, "{}"),
            Some(&queued),
        );
        let started = started.run_data().unwrap().to_vec();
        let mut pause = make_record("workflow.paused", 3_000_000_000, "{}");
        pause
            .metadata
            .insert("wait_timeout_ms".into(), "1000".into());
        let paused = project(&pause, Some(&started));
        assert!(paused
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutTerminalObligation { .. })));
        let paused_data = paused.run_data().unwrap().to_vec();
        let run = Run::decode(paused_data.as_slice()).unwrap();
        assert_eq!(
            run.metadata.get("wait_deadline_ms").map(String::as_str),
            Some("4000")
        );
        let mut resumed = make_record("run.resumed", 3_500_000_000, "{}");
        resumed.metadata = pause.metadata;
        let resumed = project(&resumed, Some(&paused_data));
        let data = resumed.run_data().unwrap().to_vec();
        let run = Run::decode(data.as_slice()).unwrap();
        assert!(!run.metadata.contains_key("wait_timeout_ms"));
        assert!(!run.metadata.contains_key("wait_deadline_ms"));
    }

    fn paused_run(resume_source: &str) -> Vec<u8> {
        let queued = project(&make_record("run.queued", 1_000_000_000, "{}"), None);
        let started = project(
            &make_record("run.started", 2_000_000_000, "{}"),
            queued.run_data(),
        );
        let mut pause = make_record("workflow.paused", 3_000_000_000, "{}");
        pause.metadata.insert(
            engine_core::lease_fence::META_RESUME_SOURCE_ID.into(),
            resume_source.into(),
        );
        project(&pause, started.run_data())
            .run_data()
            .unwrap()
            .to_vec()
    }

    fn timer_scheduled(resume_source: &str, continuation: &str) -> Record {
        let data = serde_json::json!({
            "fire_at_ns": 4_000_000_000_i64,
            "schedule_payload": {
                "resume_metadata": {
                    engine_core::lease_fence::META_RESUME_SOURCE_ID: resume_source,
                    "timer_id": "timer-1",
                    "continuation_b64": continuation,
                }
            }
        });
        let mut record = make_record("timer.scheduled", 3_000_000_000, &data.to_string());
        record.metadata.insert("timer_id".into(), "timer-1".into());
        record
    }

    #[test]
    fn a_timer_suspension_keeps_its_continuation_on_the_paused_run() {
        let paused = paused_run("lease:lease-2");

        let scheduled = project(
            &timer_scheduled("lease:lease-2", "Y29udGludWF0aW9u"),
            Some(&paused),
        );
        let run = Run::decode(scheduled.run_data().expect("the run row changes")).unwrap();
        assert_eq!(run.status, "paused");
        assert_eq!(
            run.metadata.get(META_CONTINUATION_B64).map(String::as_str),
            Some("Y29udGludWF0aW9u")
        );
        assert!(
            !scheduled
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutPendingRun { .. })),
            "keeping the continuation must not make the paused run claimable"
        );

        // A resume that leaves its copy out keeps the row's continuation.
        let mut resumed = make_record("run.resumed", 4_000_000_000, "{}");
        resumed.metadata.insert(
            engine_core::lease_fence::META_RESUME_SOURCE_ID.into(),
            "lease:lease-2".into(),
        );
        let resumed = project(&resumed, scheduled.run_data());
        let run = Run::decode(resumed.run_data().unwrap()).unwrap();
        assert_eq!(run.status, "queued");
        assert_eq!(
            run.metadata.get(META_CONTINUATION_B64).map(String::as_str),
            Some("Y29udGludWF0aW9u")
        );
    }

    #[test]
    fn only_the_current_pause_timer_sets_the_continuation() {
        let paused = paused_run("lease:lease-2");
        let stale = project(&timer_scheduled("lease:lease-1", "c3RhbGU"), Some(&paused));
        assert!(stale.run_data().is_none(), "a timer from an older pause");

        let running = project(
            &make_record("run.started", 2_000_000_000, "{}"),
            project(&make_record("run.queued", 1_000_000_000, "{}"), None).run_data(),
        );
        let not_paused = project(
            &timer_scheduled("lease:lease-2", "Y29udGludWF0aW9u"),
            running.run_data(),
        );
        assert!(not_paused.run_data().is_none(), "a run that is not paused");

        let workerless = make_record(
            "timer.scheduled",
            3_000_000_000,
            r#"{"schedule_payload":{"resume_metadata":{"lease_id":"lease-2"}}}"#,
        );
        assert!(
            project(&workerless, Some(&paused)).run_data().is_none(),
            "a timer without a continuation"
        );
    }

    #[test]
    fn active_and_terminal_transitions_update_terminal_obligation_index() {
        let mut queued = make_record("run.queued", 5_000_000_000, "{}");
        queued.offset = engine_core::partition_offset::encode_offset(4, 12).unwrap();
        let queued_result = project(&queued, None);
        let obligation = queued_result
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutTerminalObligation {
                    partition_id,
                    run_data,
                } => Some((*partition_id, run_data)),
                _ => None,
            })
            .expect("queued run must create terminal obligation");
        assert_eq!(obligation.0, 4);
        let active = Run::decode(obligation.1.as_slice()).unwrap();
        assert_eq!(active.status, "queued");
        assert_eq!(
            active
                .metadata
                .get(run_state_machine::TERMINAL_OBLIGATION_OFFSET_META)
                .map(String::as_str),
            Some("12")
        );
        assert_eq!(
            active
                .metadata
                .get(run_state_machine::TERMINAL_OBLIGATION_UPDATED_AT_MS_META)
                .map(String::as_str),
            Some("5000")
        );

        let mut completed = make_record("run.completed", 6_000_000_000, "{}");
        completed.offset = engine_core::partition_offset::encode_offset(4, 13).unwrap();
        let completed_result = project(&completed, Some(active.encode_to_vec().as_slice()));
        assert!(completed_result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::DeleteTerminalObligation { partition_id: 4 })));
        assert!(!completed_result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutTerminalObligation { .. })));
    }

    #[test]
    fn terminal_reservation_is_fenced_to_exact_obligation_generation() {
        let mut queued = make_record("run.queued", 5_000_000_000, "{}");
        queued.offset = engine_core::partition_offset::encode_offset(4, 12).unwrap();
        let queued_result = project(&queued, None);
        let active = Run::decode(queued_result.run_data().expect("queued Run")).unwrap();

        let mut stale = make_record("run.terminalizing", 6_000_000_000, "{}");
        stale.offset = engine_core::partition_offset::encode_offset(4, 13).unwrap();
        stale.metadata.insert(
            run_state_machine::TERMINAL_OBLIGATION_OFFSET_META.to_string(),
            "11".to_string(),
        );
        stale.metadata.insert(
            run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
            "reservation-stale".to_string(),
        );
        stale.metadata.insert(
            run_state_machine::TERMINAL_OUTCOME_META.to_string(),
            "timeout".to_string(),
        );
        let stale_result = project(&stale, Some(active.encode_to_vec().as_slice()));
        assert!(stale_result.run_data().is_none());

        let mut current = stale;
        current.metadata.insert(
            run_state_machine::TERMINAL_OBLIGATION_OFFSET_META.to_string(),
            "12".to_string(),
        );
        current.metadata.insert(
            run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
            "reservation-current".to_string(),
        );
        let current_result = project(&current, Some(active.encode_to_vec().as_slice()));
        let terminalizing = Run::decode(
            current_result
                .run_data()
                .expect("current reservation must project"),
        )
        .unwrap();
        assert_eq!(terminalizing.status, "terminalizing");
        assert_eq!(
            terminalizing
                .metadata
                .get(run_state_machine::TERMINAL_RESERVATION_ID_META)
                .map(String::as_str),
            Some("reservation-current")
        );
    }

    #[test]
    fn terminal_outcome_must_consume_current_reservation() {
        let mut terminalizing = Run {
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: "terminalizing".to_string(),
            ..Default::default()
        };
        terminalizing.metadata.insert(
            run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
            "reservation-current".to_string(),
        );
        terminalizing.metadata.insert(
            run_state_machine::TERMINAL_OUTCOME_META.to_string(),
            "timeout".to_string(),
        );

        let mut stale = make_record("run.failed", 6_000_000_000, "{}");
        stale.metadata.insert(
            run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
            "reservation-stale".to_string(),
        );
        stale.metadata.insert(
            run_state_machine::TERMINAL_OUTCOME_META.to_string(),
            "timeout".to_string(),
        );
        stale
            .metadata
            .insert("workerless_retryable".to_string(), "false".to_string());
        assert!(
            project(&stale, Some(terminalizing.encode_to_vec().as_slice()))
                .run_data()
                .is_none()
        );

        stale.metadata.insert(
            run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
            "reservation-current".to_string(),
        );
        stale.metadata.insert(
            run_state_machine::TERMINAL_OUTCOME_META.to_string(),
            "stranded".to_string(),
        );
        assert!(
            project(&stale, Some(terminalizing.encode_to_vec().as_slice()))
                .run_data()
                .is_none(),
            "a matching reservation cannot change its reserved outcome"
        );

        stale.metadata.insert(
            run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
            "reservation-current".to_string(),
        );
        stale.metadata.insert(
            run_state_machine::TERMINAL_OUTCOME_META.to_string(),
            "timeout".to_string(),
        );
        let accepted = project(&stale, Some(terminalizing.encode_to_vec().as_slice()));
        let failed = Run::decode(accepted.run_data().expect("reserved terminal outcome")).unwrap();
        assert_eq!(failed.status, "failed");
    }

    #[test]
    fn duplicate_terminal_does_not_re_emit_outbox() {
        // The same-terminal-duplicate guard short-circuits the projection
        // before PutRun, and the outbox emit gates on PutRun's presence.
        // A replay of the same terminal record must not produce a second
        // outbox entry — which would otherwise pile up under journal
        // replay or sweeper races.
        let mut record = make_record(
            "run.failed",
            6_000_000_000,
            r#"{"error":{"message":"boom","code":"E_BOOM"}}"#,
        );
        record.offset = engine_core::partition_offset::encode_offset(0, 99).unwrap();

        // First terminal: emits PutRun + PutRunQueryArchiveIntent.
        let first = project(&record, Some(&running_run_bytes()));
        assert!(first.ops.iter().any(|op| matches!(op, StoreOp::PutRun(_))));
        assert!(first
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutRunQueryArchiveIntent { .. })));

        // Build the resulting Run bytes (already in `failed`) — the
        // second projection sees this as the "existing run" and the
        // duplicate guard short-circuits.
        let put_run_data = first
            .ops
            .iter()
            .find_map(|op| {
                if let StoreOp::PutRun(d) = op {
                    Some(d.clone())
                } else {
                    None
                }
            })
            .expect("first projection emitted PutRun");

        let second = project(&record, Some(&put_run_data));
        assert!(
            !second.ops.iter().any(|op| matches!(op, StoreOp::PutRun(_))),
            "duplicate terminal must NOT re-emit PutRun"
        );
        assert!(
            !second
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutRunQueryArchiveIntent { .. })),
            "duplicate terminal must NOT re-emit PutRunQueryArchiveIntent"
        );
    }

    #[test]
    fn non_terminal_events_do_not_emit_outbox() {
        // run.started / run.assigned / etc. mutate Run state (so PutRun
        // fires) but should NOT produce outbox entries — only terminal
        // states get archived.
        for et in &["run.started", "run.assigned", "run.paused", "run.resumed"] {
            let mut record = make_record(et, 1_000_000_000, "{}");
            record.offset = engine_core::partition_offset::encode_offset(3, 7).unwrap();
            let result = project(&record, Some(&running_run_bytes()));
            assert!(
                !result
                    .ops
                    .iter()
                    .any(|op| matches!(op, StoreOp::PutRunQueryArchiveIntent { .. })),
                "event {et} should not emit PutRunQueryArchiveIntent"
            );
        }
    }

    #[test]
    fn run_archived_emits_delete_outbox() {
        // The active partition owner emits run.archived with metadata
        // fields after a successful S3 PUT. Every replica projects it
        // and queues a DeleteRunArchiveIntent using the metadata-supplied
        // partition_id / terminal_offset — NOT decode_offset(record.offset)
        // (the run.archived record's own offset is unrelated to the
        // terminal record's offset).
        let mut record = make_record("run.archived", 9_000_000_000, "");
        record.offset = engine_core::partition_offset::encode_offset(2, 500).unwrap();
        record
            .metadata
            .insert("partition_id".to_string(), "7".to_string());
        record
            .metadata
            .insert("terminal_offset".to_string(), "42".to_string());

        let result = project(&record, None);
        let delete = result
            .ops
            .iter()
            .find_map(|op| {
                if let StoreOp::DeleteRunArchiveIntent {
                    partition_id,
                    project_id,
                    terminal_offset,
                    run_id,
                } = op
                {
                    Some((
                        *partition_id,
                        project_id.clone(),
                        *terminal_offset,
                        run_id.clone(),
                    ))
                } else {
                    None
                }
            })
            .expect("run.archived should emit DeleteRunArchiveIntent");
        assert_eq!(delete.0, 7); // metadata, not decoded from record.offset
        assert_eq!(delete.1, "t1");
        assert_eq!(delete.2, 42);
        assert_eq!(delete.3, "r1");

        // Side check: run.archived doesn't mutate Run state.
        assert!(!result.ops.iter().any(|op| matches!(op, StoreOp::PutRun(_))));
    }

    #[test]
    fn run_archived_archive_pointer_is_directory_not_filename() {
        // CF_RUNS_ARCHIVE must store the partition *directory*, not the
        // specific `.parquet` filename, so the pointer survives compaction
        // replacing the original file with a merged one. The reader
        // appends `/*.parquet` to form a glob.
        let mut record = make_record("run.archived", 9_000_000_000, "");
        record
            .metadata
            .insert("partition_id".to_string(), "7".to_string());
        record
            .metadata
            .insert("terminal_offset".to_string(), "42".to_string());
        record.metadata.insert(
            "parquet_filename".to_string(),
            "engine/runs/workspace=ws/project=p1/deployment=d1/component=fn/day=2026-01-01/runs-node-abc.parquet".to_string(),
        );

        let result = project(&record, None);
        let archive = result
            .ops
            .iter()
            .find_map(|op| {
                if let StoreOp::PutRunArchiveLocation {
                    archive_location, ..
                } = op
                {
                    Some(archive_location.clone())
                } else {
                    None
                }
            })
            .expect("run.archived must emit PutRunArchiveLocation");

        // Must be the directory (no trailing slash, no filename).
        assert_eq!(
            archive,
            "engine/runs/workspace=ws/project=p1/deployment=d1/component=fn/day=2026-01-01",
            "CF_RUNS_ARCHIVE must store partition directory, not specific filename"
        );
        assert!(
            !archive.ends_with(".parquet"),
            "filename must not be in the stored path"
        );
    }

    #[test]
    fn run_archived_without_archive_generation_emits_no_retention_index() {
        // A stripped or malformed archive event must never authorize deletion
        // of the only readable hot row.
        let mut record = make_record("run.archived", 9_000_000_000, "");
        record.offset = engine_core::partition_offset::encode_offset(2, 500).unwrap();
        record.metadata.insert(
            "archived_at_ms".to_string(),
            "1_700_000_000_000".to_string(),
        );
        let result = project(&record, None);
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutRunRetention { .. })));
    }

    #[test]
    fn run_archived_emits_versioned_retention_with_complete_provenance() {
        let mut record = make_record("run.archived", 9_000_000_000, "");
        record.offset = engine_core::partition_offset::encode_offset(2, 500).unwrap();
        record
            .metadata
            .insert("archived_at_ms".to_string(), "1700000000000".to_string());
        let generation =
            "engine/runs/workspace=ws/project=t1/day=2026-01-01/runs-generation.parquet";
        record
            .metadata
            .insert("parquet_filename".to_string(), generation.to_string());
        let result = project(&record, None);
        let retention = result
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutRunRetention {
                    archived_at_ms,
                    archive_generation,
                    ..
                } => Some((*archived_at_ms, archive_generation.as_str())),
                _ => None,
            })
            .expect("run.archived must emit PutRunRetention");
        assert_eq!(retention.0, 1_700_000_000_000_i64);
        assert_eq!(retention.1, generation);
    }

    #[test]
    fn run_archived_with_missing_metadata_is_observed_but_no_op() {
        // If the active owner forgets to stamp metadata, projection
        // logs a warning and emits no DeleteRunArchiveIntent — the entry
        // stays so a future correctly-stamped retry can drain it.
        let record = make_record("run.archived", 9_000_000_000, "");
        let result = project(&record, None);
        assert!(!result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::DeleteRunArchiveIntent { .. })));
        // PutEvent still fires — the journal is the source of truth.
        assert!(result.ops.iter().any(|op| matches!(op, StoreOp::PutEvent)));
    }

    #[test]
    fn run_query_archived_schedules_retention_and_retires_legacy_intent() {
        let mut record = make_record(
            "run.query_archived",
            9_000_000_000,
            r#"{"receipt_version":1,"segment":"segment-a"}"#,
        );
        record.offset = engine_core::partition_offset::encode_offset(2, 500).unwrap();
        record
            .metadata
            .insert("partition_id".to_string(), "7".to_string());
        record
            .metadata
            .insert("terminal_offset".to_string(), "42".to_string());

        let result = project(&record, None);
        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::DeleteRunQueryArchiveIntent {
                partition_id: 7,
                project_id,
                terminal_offset: 42,
                run_id,
            } if project_id == "t1" && run_id == "r1"
        )));
        assert!(result.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutRunQueryArchiveReceipt {
                project_id,
                run_id,
                row_version: 42,
                receipt_data,
            } if project_id == "t1"
                && run_id == "r1"
                && receipt_data == record.data.as_ref()
        )));
        assert!(result.ops.iter().any(|op| matches!(op, StoreOp::PutEvent)));
        assert!(result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::DeleteRunArchiveIntent { .. })));
        assert!(result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutRunRetention {
            archived_at_ms: 9_000, archive_generation, ..
        } if archive_generation == "query-lake/t1/r1/42")));
        assert!(!result.ops.iter().any(|op| matches!(op, StoreOp::PutRun(_))));
    }

    // --- Hot-index projection ---

    #[test]
    fn queued_emits_put_hot_index_alongside_put_run() {
        // `run.queued` must emit PutRun + PutRunHotIndex in the same op
        // sequence so the atomic batch commits both. workspace_id and
        // submitted_at_ms come from the just-projected Run, NOT
        // record.metadata directly — `apply_queued` already
        // canonicalised them, so the index key matches what subsequent
        // get_run / list_runs paths see.
        let mut record = make_record(
            "run.queued",
            1_000_000_000, // 1s
            r#"{"input_data":{"x":1},"timeout_ms":30000}"#,
        );
        record
            .metadata
            .insert("workspace_id".to_string(), "ws-a".to_string());
        record
            .metadata
            .insert("component_name".to_string(), "my_func".to_string());
        record
            .metadata
            .insert("component_type".to_string(), "function".to_string());

        let result = project(&record, None);
        let mut put_run_seen = false;
        let mut put_hot = None;
        for op in &result.ops {
            if matches!(op, StoreOp::PutRun(_)) {
                put_run_seen = true;
            }
            if let StoreOp::PutRunHotIndex {
                workspace_id,
                submitted_at_ms,
            } = op
            {
                put_hot = Some((workspace_id.clone(), *submitted_at_ms));
            }
        }
        assert!(put_run_seen, "expected PutRun");
        let (workspace_id, submitted_at_ms) = put_hot.expect("queued must emit PutRunHotIndex");
        assert_eq!(workspace_id, "ws-a");
        // record.timestamp_ns == 1_000_000_000 (1s in ns) →
        // apply_queued sets Run.submitted_at = 1s. The projection
        // re-derives submitted_at_ms from the Run proto, so it should
        // be 1000ms.
        assert_eq!(submitted_at_ms, 1000);
    }

    #[test]
    fn queued_without_workspace_id_skips_hot_index() {
        // The gateway always stamps workspace_id; if it's absent
        // (legacy auth path or test fixture mistake), we log+skip the
        // PutRunHotIndex rather than emit an empty-prefix key that would
        // be unreachable across workspace deletion semantics.
        let record = make_record("run.queued", 1_000_000_000, r#"{"input_data":{}}"#);
        let result = project(&record, None);
        assert!(result.ops.iter().any(|op| matches!(op, StoreOp::PutRun(_))));
        assert!(
            !result
                .ops
                .iter()
                .any(|op| matches!(op, StoreOp::PutRunHotIndex { .. })),
            "PutRunHotIndex must be skipped when workspace_id is missing"
        );
    }

    #[test]
    fn non_queued_lifecycle_does_not_emit_hot_index() {
        // run.assigned / run.started / run.completed mutate Run state
        // (so PutRun fires) but should NOT emit a fresh PutRunHotIndex —
        // the index entry was placed at queued time and stays until
        // run.archived deletes it.
        let existing = Run {
            run_id: "r1".to_string(),
            workspace_id: "ws-a".to_string(),
            project_id: "t1".to_string(),
            status: "running".to_string(),
            ..Default::default()
        };
        let existing_bytes = existing.encode_to_vec();

        for et in &["run.assigned", "run.started", "run.completed", "run.failed"] {
            let record = make_record(et, 2_000_000_000, "{}");
            let result = project(&record, Some(&existing_bytes));
            assert!(
                !result
                    .ops
                    .iter()
                    .any(|op| matches!(op, StoreOp::PutRunHotIndex { .. })),
                "event {et} must not re-emit PutRunHotIndex"
            );
        }
    }

    #[test]
    fn run_archived_keeps_hot_index_until_verified_retention() {
        let mut record = make_record("run.archived", 9_000_000_000, "");
        record
            .metadata
            .insert("partition_id".to_string(), "7".to_string());
        record
            .metadata
            .insert("terminal_offset".to_string(), "42".to_string());
        record
            .metadata
            .insert("workspace_id".to_string(), "ws-a".to_string());
        record
            .metadata
            .insert("submitted_at_ms".to_string(), "1000".to_string());

        let result = project(&record, None);
        assert!(result
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::DeleteRunArchiveIntent { .. })));
    }

    // --- traceparent parsing ---

    #[test]
    fn parse_traceparent_extracts_trace_id_from_w3c_header() {
        let trace_id =
            parse_traceparent_trace_id("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        assert_eq!(
            trace_id.as_deref(),
            Some("0af7651916cd43dd8448eb211c80319c")
        );
    }

    #[test]
    fn parse_traceparent_rejects_malformed_input() {
        // Not enough fields.
        assert_eq!(parse_traceparent_trace_id(""), None);
        assert_eq!(parse_traceparent_trace_id("00"), None);
        assert_eq!(parse_traceparent_trace_id("00-foo"), None);
        // Wrong trace_id length.
        assert_eq!(
            parse_traceparent_trace_id("00-0af7-b7ad6b7169203331-01"),
            None
        );
        // Non-hex trace_id.
        assert_eq!(
            parse_traceparent_trace_id("00-0af7651916cd43dd8448eb211c80319Z-b7ad6b7169203331-01"),
            None
        );
        // Too many fields.
        assert_eq!(
            parse_traceparent_trace_id(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra"
            ),
            None
        );
        // All-zero trace_id is the W3C "invalid" sentinel.
        assert_eq!(
            parse_traceparent_trace_id("00-00000000000000000000000000000000-b7ad6b7169203331-01"),
            None
        );
        // Wrong span_id length.
        assert_eq!(
            parse_traceparent_trace_id("00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920-01"),
            None
        );
    }

    #[test]
    fn run_queued_pulls_trace_id_from_traceparent() {
        let mut record = make_record("run.queued", 1_000_000_000, "");
        record.metadata.insert(
            "traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );
        // Run the projection through the same path the processor uses.
        let result = project(&record, None);
        let put = result
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutRun(bytes) => Some(bytes),
                _ => None,
            })
            .expect("apply_queued must emit PutRun");
        let run = Run::decode(put.as_slice()).unwrap();
        assert_eq!(run.trace_id, "0af7651916cd43dd8448eb211c80319c");
        // The full traceparent stays in metadata so consumers can
        // reconstruct span_id / flags.
        assert_eq!(
            run.metadata.get("traceparent").map(|s| s.as_str()),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn run_queued_explicit_trace_id_overrides_traceparent() {
        // Some legacy callers stamp `trace_id` directly. If both are
        // present, the explicit value wins for compatibility.
        let mut record = make_record("run.queued", 1_000_000_000, "");
        record.metadata.insert(
            "traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );
        record
            .metadata
            .insert("trace_id".to_string(), "explicit-trace".to_string());
        let result = project(&record, None);
        let put = result
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutRun(bytes) => Some(bytes),
                _ => None,
            })
            .unwrap();
        let run = Run::decode(put.as_slice()).unwrap();
        assert_eq!(run.trace_id, "explicit-trace");
    }
}
