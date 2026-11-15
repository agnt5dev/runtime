//! Shared input contract for the Runs list filters.

use std::collections::BTreeSet;

pub const MAX_DEPLOYMENT_FILTER_IDS: usize = 100;
pub const MAX_DEPLOYMENT_FILTER_BYTES: usize = 8192;

/// Every status the engine writes to `Run.status`. Mirrors
/// `engine_processor::run_state_machine::RunStatus`; a test there keeps the
/// two in step.
pub const RUN_STATUSES: [&str; 9] = [
    "queued",
    "assigned",
    "started",
    "retry_delayed",
    "terminalizing",
    "paused",
    "completed",
    "failed",
    "cancelled",
];

/// Statuses a run can be archived with. The runs projection writes archive
/// intents only for these, so a filter without any of them can never match an
/// archived row.
pub const ARCHIVED_RUN_STATUSES: [&str; 3] = ["completed", "failed", "cancelled"];

/// Parse the `status` filter: one status or a comma-separated set, matched as
/// a union. Absent or blank input means no constraint. An unknown status is an
/// error rather than an empty result, so a misspelled filter never scans.
pub fn normalize_statuses(raw: Option<&str>) -> Result<Vec<String>, String> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(Vec::new());
    };
    let mut normalized = BTreeSet::new();
    for status in raw.split(',').map(str::trim) {
        if !RUN_STATUSES.contains(&status) {
            return Err(format!(
                "unknown run status {status:?}; expected one or more of {}",
                RUN_STATUSES.join(", ")
            ));
        }
        normalized.insert(status.to_owned());
    }
    Ok(normalized.into_iter().collect())
}

/// Whether some status in a normalized filter set can appear in the archive.
/// An empty set is unconstrained, so it can.
pub fn statuses_reach_archive(statuses: &[String]) -> bool {
    statuses.is_empty()
        || statuses
            .iter()
            .any(|status| ARCHIVED_RUN_STATUSES.contains(&status.as_str()))
}

/// Empty repeated input means no constraint. A supplied empty identifier is
/// invalid, so malformed input can never broaden a constrained request.
/// Uses the same identifier alphabet and length as the archive query layer.
pub fn normalize_deployment_ids<'a>(
    ids: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<String>, &'static str> {
    normalize_identifier_set(
        ids,
        "deployment_ids accepts at most 100 IDs and 8192 bytes",
        "deployment_ids must contain non-empty IDs of at most 256 ASCII letters, digits, hyphens or underscores",
    )
}

/// The component-name set Studio's search resolves a partial name into.
/// Matched by equality like `component_name`, so the archive can still prune
/// its `component=` partitions. Same bounds and alphabet as the deployment
/// set, which is also what the archive query layer accepts for one name.
pub fn normalize_component_names<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<String>, &'static str> {
    normalize_identifier_set(
        names,
        "component_names accepts at most 100 names and 8192 bytes",
        "component_names must contain non-empty names of at most 256 ASCII letters, digits, hyphens or underscores",
    )
}

fn normalize_identifier_set<'a>(
    ids: impl IntoIterator<Item = &'a str>,
    too_large: &'static str,
    invalid: &'static str,
) -> Result<Vec<String>, &'static str> {
    let mut normalized = BTreeSet::new();
    let mut bytes = 0usize;
    for (index, raw) in ids.into_iter().enumerate() {
        bytes = bytes
            .saturating_add(raw.len())
            .saturating_add(usize::from(index > 0));
        if index >= MAX_DEPLOYMENT_FILTER_IDS || bytes > MAX_DEPLOYMENT_FILTER_BYTES {
            return Err(too_large);
        }
        let id = raw.trim();
        if id.is_empty()
            || id.len() > 256
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(invalid);
        }
        normalized.insert(id.to_owned());
    }
    Ok(normalized.into_iter().collect())
}

/// Check the duration window: milliseconds, min inclusive and max exclusive,
/// either bound optional. A negative bound or an empty window (min >= max) is
/// an error rather than an empty result, so a malformed filter never scans.
///
/// Every listing backend reads the window the same way: only runs with a
/// `completed_at` are eligible, because a run still executing has no final
/// duration yet, and a finished run's duration is `Run.duration_ms`.
pub fn validate_duration_window(
    min_duration_ms: Option<i64>,
    max_duration_ms: Option<i64>,
) -> Result<(), &'static str> {
    if min_duration_ms.is_some_and(|min| min < 0) || max_duration_ms.is_some_and(|max| max < 0) {
        return Err("min_duration_ms and max_duration_ms must not be negative");
    }
    if matches!((min_duration_ms, max_duration_ms), (Some(min), Some(max)) if min >= max) {
        return Err("min_duration_ms must be less than max_duration_ms");
    }
    Ok(())
}

/// Whether a finished run's duration falls in a window that
/// `validate_duration_window` accepted.
pub fn duration_in_window(
    duration_ms: i64,
    min_duration_ms: Option<i64>,
    max_duration_ms: Option<i64>,
) -> bool {
    min_duration_ms.is_none_or(|min| duration_ms >= min)
        && max_duration_ms.is_none_or(|max| duration_ms < max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_set_normalizes_and_rejects_invalid_or_oversized_input() {
        assert_eq!(
            normalize_deployment_ids([" b ", "a", "a"]).unwrap(),
            ["a", "b"]
        );
        assert!(normalize_deployment_ids([]).unwrap().is_empty());
        for bad in ["", " ", "../a", "a,b", "a'b", "a/b", "å"] {
            assert!(normalize_deployment_ids([bad]).is_err(), "{bad:?}");
        }
        assert!(normalize_deployment_ids(["a".repeat(257).as_str()]).is_err());
        assert!(normalize_deployment_ids(std::iter::repeat_n("a", 101)).is_err());
        let long = "a".repeat(256);
        assert!(normalize_deployment_ids(std::iter::repeat_n(long.as_str(), 33)).is_err());
        assert!(normalize_deployment_ids(std::iter::repeat_n("a", 100)).is_ok());
    }

    #[test]
    fn component_set_normalizes_and_rejects_invalid_or_oversized_input() {
        assert_eq!(
            normalize_component_names([" ks_analyze_text ", "analyze", "analyze"]).unwrap(),
            ["analyze", "ks_analyze_text"]
        );
        assert!(normalize_component_names([]).unwrap().is_empty());
        for bad in ["", " ", "../a", "a,b", "a'b", "a/b", "a.b", "å"] {
            let err = normalize_component_names([bad]).unwrap_err();
            assert!(err.starts_with("component_names"), "{bad:?}: {err}");
        }
        assert!(normalize_component_names(["a".repeat(257).as_str()]).is_err());
        assert!(normalize_component_names(std::iter::repeat_n("a", 101)).is_err());
        assert!(normalize_component_names(std::iter::repeat_n("a", 100)).is_ok());
    }

    #[test]
    fn duration_window_rejects_negative_and_empty_windows() {
        assert!(validate_duration_window(None, None).is_ok());
        assert!(validate_duration_window(Some(0), None).is_ok());
        assert!(validate_duration_window(None, Some(100)).is_ok());
        assert!(validate_duration_window(Some(100), Some(1_000)).is_ok());
        for (min, max) in [
            (Some(-1), None),
            (None, Some(-1)),
            (Some(100), Some(100)),
            (Some(1_000), Some(100)),
        ] {
            assert!(
                validate_duration_window(min, max).is_err(),
                "{min:?}..{max:?}"
            );
        }
    }

    #[test]
    fn duration_window_is_min_inclusive_and_max_exclusive() {
        assert!(duration_in_window(100, Some(100), Some(1_000)));
        assert!(duration_in_window(999, Some(100), Some(1_000)));
        assert!(!duration_in_window(1_000, Some(100), Some(1_000)));
        assert!(!duration_in_window(99, Some(100), Some(1_000)));
        assert!(duration_in_window(0, None, Some(100)));
        assert!(duration_in_window(i64::MAX, Some(10_000), None));
    }

    #[test]
    fn status_set_normalizes_and_rejects_unknown_statuses() {
        assert!(normalize_statuses(None).unwrap().is_empty());
        assert!(normalize_statuses(Some("  ")).unwrap().is_empty());
        assert_eq!(normalize_statuses(Some("failed")).unwrap(), ["failed"]);
        assert_eq!(
            normalize_statuses(Some(" started,assigned ,started")).unwrap(),
            ["assigned", "started"]
        );
        for bad in [
            "running",
            "pending",
            "started,running",
            "started,",
            "STARTED",
        ] {
            let err = normalize_statuses(Some(bad)).unwrap_err();
            assert!(err.contains("expected one or more of"), "{bad:?}: {err}");
        }
    }

    #[test]
    fn only_terminal_statuses_reach_the_archive() {
        let set = |s: &str| normalize_statuses(Some(s)).unwrap();
        assert!(statuses_reach_archive(&[]));
        assert!(statuses_reach_archive(&set("completed")));
        assert!(statuses_reach_archive(&set("started,failed")));
        assert!(!statuses_reach_archive(&set(
            "queued,assigned,started,retry_delayed,terminalizing,paused"
        )));
    }
}
