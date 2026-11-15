//! Deterministic trigger identity.
//!
//! Per ADR-004, a trigger's logical id is derived from
//! (project, deployment, component, trigger spec) — never from worker id —
//! so horizontally replicated workers advertise the same logical trigger
//! and event fanout dedupes correctly. Both the gateway's read-side
//! normalization and the coordinator's CF_TRIGGERS sync must mint ids
//! through this one function so the derivation can never drift.

use uuid::Uuid;

use crate::TriggerSpec;

const TRIGGER_ID_NAMESPACE: u128 = 0x8c0a_80df_9cf0_4d8e_8b04_8f8c_399f_2a9d;

/// Mint the deterministic trigger id for a declared trigger. Stable across
/// worker replicas and re-registrations: same (project, deployment,
/// component, spec) always yields the same id. `trigger.trigger_id` itself
/// is not part of the derivation.
pub fn deterministic_trigger_id(
    project_id: &str,
    deployment_id: &str,
    component_type: i32,
    component_name: &str,
    trigger: &TriggerSpec,
) -> String {
    let name = format!(
        "agnt5-trigger-v1\0{project_id}\0{deployment_id}\0{component_type}\0{component_name}\0{}\0{}\0{}\0{}\0{}\0{}",
        trigger.trigger_type,
        trigger.event_name,
        trigger.filter_expression,
        trigger.input_mapping,
        trigger.batch_window_ms,
        trigger.delay_expression,
    );
    Uuid::new_v5(&Uuid::from_u128(TRIGGER_ID_NAMESPACE), name.as_bytes()).to_string()
}
