//! Canonical identity for one runnable generation of a run.
//!
//! New generations are derived from the journal offset of the lifecycle
//! record that made the run runnable (`run.queued`, `run.resumed`, or
//! requeueing `run.lease_expired`). Older projected rows may predate that
//! metadata; their explicit compatibility fallback stays centralized here so
//! queue readers, offers, grants, leases, and retries cannot disagree.

use std::collections::HashMap;
use std::fmt;

use crate::lease_fence::{META_RESUME_SOURCE_ID, RUN_META_DISPATCH_GENERATION};
use crate::types::Offset;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DispatchGeneration(String);

impl DispatchGeneration {
    pub fn from_journal_offset(offset: Offset) -> Self {
        Self(offset.0.to_string())
    }

    pub fn from_value(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for DispatchGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchGenerationOrigin {
    Journal,
    LegacyResume,
    LegacyAttempt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedDispatchGeneration {
    pub generation: DispatchGeneration,
    pub origin: DispatchGenerationOrigin,
}

/// Resolve the generation carried by a projected run.
///
/// `Journal` is the normal path for every newly projected runnable record.
/// The two legacy forms are intentionally namespaced so a resume source can
/// never collide with an attempt-only row.
pub fn resolve_projected_dispatch_generation(
    metadata: &HashMap<String, String>,
    attempt: u32,
) -> ResolvedDispatchGeneration {
    if let Some(generation) = metadata
        .get(RUN_META_DISPATCH_GENERATION)
        .and_then(|value| DispatchGeneration::from_value(value.clone()))
    {
        return ResolvedDispatchGeneration {
            generation,
            origin: DispatchGenerationOrigin::Journal,
        };
    }
    if let Some(source_id) = metadata
        .get(META_RESUME_SOURCE_ID)
        .filter(|value| !value.is_empty())
    {
        return ResolvedDispatchGeneration {
            generation: DispatchGeneration(format!("legacy-resume:{source_id}")),
            origin: DispatchGenerationOrigin::LegacyResume,
        };
    }
    ResolvedDispatchGeneration {
        generation: DispatchGeneration(format!("legacy-attempt:{attempt}")),
        origin: DispatchGenerationOrigin::LegacyAttempt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_generation_wins_over_every_legacy_fallback() {
        let metadata = HashMap::from([
            (RUN_META_DISPATCH_GENERATION.to_string(), "42".to_string()),
            (META_RESUME_SOURCE_ID.to_string(), "pause-7".to_string()),
        ]);
        let resolved = resolve_projected_dispatch_generation(&metadata, 3);
        assert_eq!(resolved.generation.as_str(), "42");
        assert_eq!(resolved.origin, DispatchGenerationOrigin::Journal);
        assert_eq!(
            DispatchGeneration::from_journal_offset(Offset(42)),
            resolved.generation
        );
    }

    #[test]
    fn legacy_fallbacks_are_explicit_and_non_overlapping() {
        let resumed = resolve_projected_dispatch_generation(
            &HashMap::from([(META_RESUME_SOURCE_ID.to_string(), "pause-7".to_string())]),
            3,
        );
        assert_eq!(resumed.generation.as_str(), "legacy-resume:pause-7");
        assert_eq!(resumed.origin, DispatchGenerationOrigin::LegacyResume);

        let attempted = resolve_projected_dispatch_generation(&HashMap::new(), 3);
        assert_eq!(attempted.generation.as_str(), "legacy-attempt:3");
        assert_eq!(attempted.origin, DispatchGenerationOrigin::LegacyAttempt);
    }
}
