//! Structured error codes for runtime boundary errors.
//!
//! Each code is a stable handle (`RT_<area>_E<nnn>`) for a specific failure
//! mode that crosses a process boundary — gateway responses, dispatch
//! failures, storage/replication errors operators page on, etc. Internal
//! errors should stay as plain `anyhow`/`thiserror` chains; they reach
//! Sentry through the existing `tracing::error!` funnel.
//!
//! ## Usage
//!
//! At a boundary error site, build a `RuntimeError`, log it, and (optionally)
//! capture to Sentry with rich tags:
//!
//! ```ignore
//! use runtime_common::error_codes::{RuntimeError, codes};
//!
//! let err = RuntimeError::new(&codes::RT_DSP_E001)
//!     .with_cause(cause)
//!     .with_context("deployment_id", deployment_id.to_string());
//!
//! tracing::error!(error = %err, "dispatch failed");
//! err.capture();  // adds runtime.error_code tag + fingerprint
//! ```
//!
//! `Display` formats as `[RT_DSP_E001] <title>: <cause>` so the existing
//! `sentry_tracing` layer also picks up a usable message even without the
//! explicit `capture()` call.

use std::collections::BTreeMap;
use std::fmt;

/// Architectural area a code belongs to. Becomes the
/// `runtime.error_category` Sentry tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Gateway / ingress / auth / cardinality.
    Gateway,
    /// Dispatch / lease / routing / coordinator.
    Dispatch,
    /// Journal / WAL / projections.
    Journal,
    /// Journal / object store / RocksDB / query lake.
    Storage,
    /// Replication / quorum / HA.
    Replication,
    /// Worker registration / lifecycle.
    Worker,
    /// Boot / recovery / readiness.
    Boot,
}

impl ErrorCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Dispatch => "dispatch",
            Self::Journal => "journal",
            Self::Storage => "storage",
            Self::Replication => "replication",
            Self::Worker => "worker",
            Self::Boot => "boot",
        }
    }
}

/// Static definition of a runtime error code. Held as `&'static` references
/// so call sites pay no allocation cost.
#[derive(Debug)]
pub struct RuntimeErrorCode {
    pub code: &'static str,
    pub category: ErrorCategory,
    pub title: &'static str,
    pub description: &'static str,
    pub runbook_url: &'static str,
}

/// A boundary error tagged with a stable code, optional cause, and free-form
/// context for Sentry.
#[derive(Debug)]
pub struct RuntimeError {
    pub code: &'static RuntimeErrorCode,
    pub cause: Option<anyhow::Error>,
    pub context: BTreeMap<String, String>,
}

impl RuntimeError {
    pub fn new(code: &'static RuntimeErrorCode) -> Self {
        Self {
            code,
            cause: None,
            context: BTreeMap::new(),
        }
    }

    pub fn with_cause<E: Into<anyhow::Error>>(mut self, cause: E) -> Self {
        self.cause = Some(cause.into());
        self
    }

    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// Capture this error to Sentry with stable tags and a fingerprint based
    /// on the code, so issues group by failure mode rather than message text.
    /// No-op when Sentry is not initialized.
    pub fn capture(&self) {
        let code = self.code;
        sentry::with_scope(
            |scope| {
                scope.set_tag("runtime.error_code", code.code);
                scope.set_tag("runtime.error_category", code.category.as_str());
                scope.set_fingerprint(Some(["runtime", code.code].as_slice()));

                let mut extras: BTreeMap<String, sentry::protocol::Value> = BTreeMap::new();
                extras.insert("title".into(), code.title.into());
                extras.insert("description".into(), code.description.into());
                extras.insert("runbook_url".into(), code.runbook_url.into());
                if let Some(cause) = &self.cause {
                    extras.insert("cause".into(), format!("{cause:#}").into());
                }
                for (k, v) in &self.context {
                    extras.insert(format!("ctx.{k}"), v.clone().into());
                }
                scope.set_context("runtime_error", sentry::protocol::Context::Other(extras));
            },
            || {
                sentry::capture_message(&self.to_string(), sentry::Level::Error);
            },
        );
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code.code, self.code.title)?;
        if let Some(cause) = &self.cause {
            write!(f, ": {cause}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_ref()
            .map(|e| e.as_ref() as &dyn std::error::Error)
    }
}

/// Starter registry of codes at boundary failure modes. Add more here as
/// new failure modes show up in Sentry / on-call — do not pre-enumerate.
pub mod codes {
    use super::{ErrorCategory, RuntimeErrorCode};

    pub static RT_BOOT_E001: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_BOOT_E001",
        category: ErrorCategory::Boot,
        title: "Node is recovering",
        description: "The node is reachable but has not finished boot. The response carries the boot state, the step in flight, and how many partitions are serving; /boot-status carries the same per partition.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_BOOT_E001",
    };

    pub static RT_GW_E001: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_GW_E001",
        category: ErrorCategory::Gateway,
        title: "Tenant cardinality limit exceeded",
        description:
            "Tenant exceeded the per-window cardinality budget for a high-cardinality label.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_GW_E001",
    };

    pub static RT_GW_E002: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_GW_E002",
        category: ErrorCategory::Gateway,
        title: "Service key scope mismatch",
        description:
            "Service key is not authorized for the requested project / environment / deployment.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_GW_E002",
    };

    pub static RT_DSP_E001: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_DSP_E001",
        category: ErrorCategory::Dispatch,
        title: "Dispatch lease conflict",
        description: "Could not acquire a lease for the target component; another worker holds it or capacity is exhausted.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_DSP_E001",
    };

    pub static RT_DSP_E002: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_DSP_E002",
        category: ErrorCategory::Dispatch,
        title: "No eligible worker for deployment",
        description: "No connected worker matches the (tenant, deployment, component) routing key.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_DSP_E002",
    };

    pub static RT_JNL_E001: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_JNL_E001",
        category: ErrorCategory::Journal,
        title: "Journal apply lag breach",
        description: "Projection lag exceeded the configured threshold; reads may be stale.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_JNL_E001",
    };

    pub static RT_STO_E001: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_STO_E001",
        category: ErrorCategory::Storage,
        title: "Segment seal failed",
        description: "Failed to seal a WAL segment to immutable storage; new appends are blocked.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_STO_E001",
    };

    pub static RT_STO_E002: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_STO_E002",
        category: ErrorCategory::Storage,
        title: "Object store upload failed",
        description: "S3-compatible upload (segment, snapshot, or Parquet archive) did not succeed after retries.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_STO_E002",
    };

    pub static RT_REP_E001: RuntimeErrorCode = RuntimeErrorCode {
        code: "RT_REP_E001",
        category: ErrorCategory::Replication,
        title: "Replication quorum loss",
        description:
            "Partition lost write quorum; writes are paused until enough followers are reachable.",
        runbook_url: "https://docs.agnt5.dev/errors/RT_REP_E001",
    };

    /// All registered codes, in declaration order. Useful for tests that
    /// assert codes are unique and for emitting a registry dump.
    pub static ALL: &[&RuntimeErrorCode] = &[
        &RT_GW_E001,
        &RT_GW_E002,
        &RT_DSP_E001,
        &RT_DSP_E002,
        &RT_JNL_E001,
        &RT_STO_E001,
        &RT_STO_E002,
        &RT_REP_E001,
    ];
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn codes_are_unique() {
        let mut seen = HashSet::new();
        for c in codes::ALL {
            assert!(seen.insert(c.code), "duplicate code: {}", c.code);
        }
    }

    #[test]
    fn display_includes_code_and_cause() {
        let err = RuntimeError::new(&codes::RT_DSP_E001)
            .with_cause(anyhow::anyhow!("lease held by worker-7"));
        let s = err.to_string();
        assert!(s.starts_with("[RT_DSP_E001]"), "got: {s}");
        assert!(s.contains("lease held by worker-7"), "got: {s}");
    }
}
