//! What the runtime reports about its data root.
//!
//! The runtime server prepares the data root before anything else opens a
//! store, and the gateway's readiness check reports it. The two live in
//! different crates, so the summary passes through here rather than through
//! every constructor between them.

use std::sync::{OnceLock, RwLock};

fn status_cell() -> &'static RwLock<Option<serde_json::Value>> {
    static STATUS: OnceLock<RwLock<Option<serde_json::Value>>> = OnceLock::new();
    STATUS.get_or_init(|| RwLock::new(None))
}

/// Replace the data root summary that readiness reports.
pub fn set_status(status: serde_json::Value) {
    if let Ok(mut slot) = status_cell().write() {
        *slot = Some(status);
    }
}

/// The data root summary, or `None` when the runtime runs from a plain data
/// directory.
pub fn status() -> Option<serde_json::Value> {
    status_cell().read().ok().and_then(|slot| slot.clone())
}
