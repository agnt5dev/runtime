//! Active durability profile and startup-safety evaluation.

use std::path::Path;
use std::sync::OnceLock;

use opentelemetry::metrics::Gauge;
use opentelemetry::KeyValue;
use serde::Serialize;

pub const DURABILITY_PROFILE_ENV: &str = "AGNT5_DURABILITY_PROFILE";
pub const BREAK_GLASS_ACK_ENV: &str = "AGNT5_DURABILITY_BREAK_GLASS_ACK";
pub const BREAK_GLASS_ACK_VALUE: &str = "I_ACKNOWLEDGE_DURABILITY_DEGRADATION";
pub const DURABILITY_DEGRADED_REASONS: [&str; 4] = [
    "journal_fsync_disabled",
    "durability_storage_ephemeral",
    "lease_backed_push_disabled",
    "write_quorum_below_majority",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityProfile {
    Development,
    Production,
    BreakGlass,
}

impl DurabilityProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Production => "production",
            Self::BreakGlass => "break_glass",
        }
    }

    pub fn from_env() -> Result<Self, String> {
        Self::parse(
            &std::env::var(DURABILITY_PROFILE_ENV).unwrap_or_else(|_| "development".to_string()),
        )
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "development" | "dev" => Ok(Self::Development),
            "production" | "prod" => Ok(Self::Production),
            "break-glass" | "break_glass" => Ok(Self::BreakGlass),
            _ => Err(format!(
                "{DURABILITY_PROFILE_ENV} must be development, production, or break-glass; got {value:?}"
            )),
        }
    }

    /// Capabilities a production profile expects workers to honor for its
    /// durability guarantees. Optional protocol upgrades such as the pull
    /// completion lifecycle bundle are supported but never required.
    pub fn required_protocol_capabilities(self) -> Vec<String> {
        if self == Self::Production {
            durability_protocol_capabilities()
        } else {
            Vec::new()
        }
    }
}

/// Prevent a production deployment marker from silently falling back to the
/// development durability contract when profile wiring drifts.
pub fn validate_environment_profile(
    environment: &str,
    profile: DurabilityProfile,
) -> Result<(), String> {
    if environment.trim().eq_ignore_ascii_case("production")
        && profile == DurabilityProfile::Development
    {
        return Err(format!(
            "AGNT5_ENVIRONMENT=production requires {DURABILITY_PROFILE_ENV}=production or break-glass"
        ));
    }
    Ok(())
}

/// Publish the active, startup-validated durability contract using only
/// closed-set labels. This is called after telemetry initialization.
pub fn record_durability_status(status: &DurabilityStatus) {
    let metrics = durability_metrics();
    let profile = status.profile.as_str();
    let profile_labels = [KeyValue::new("profile", profile)];
    metrics.profile.record(1, &profile_labels);
    metrics
        .startup_validated
        .record(u64::from(status.startup_validated), &profile_labels);
    metrics.production_guarantees_eligible.record(
        u64::from(status.production_guarantees_eligible),
        &profile_labels,
    );

    for reason in DURABILITY_DEGRADED_REASONS {
        metrics.degraded.record(
            u64::from(
                status
                    .degraded_reasons
                    .iter()
                    .any(|candidate| candidate == reason),
            ),
            &[
                KeyValue::new("profile", profile),
                KeyValue::new("reason", reason),
            ],
        );
    }

    for capability in supported_protocol_capabilities() {
        metrics.capability.record(
            1,
            &[
                KeyValue::new("profile", profile),
                KeyValue::new("capability", capability.clone()),
                KeyValue::new(
                    "required",
                    status
                        .required_protocol_capabilities
                        .contains(&capability)
                        .to_string(),
                ),
            ],
        );
    }
}

struct DurabilityMetrics {
    profile: Gauge<u64>,
    startup_validated: Gauge<u64>,
    production_guarantees_eligible: Gauge<u64>,
    degraded: Gauge<u64>,
    capability: Gauge<u64>,
}

fn durability_metrics() -> &'static DurabilityMetrics {
    static METRICS: OnceLock<DurabilityMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = opentelemetry::global::meter("agnt5.runtime");
        DurabilityMetrics {
            profile: meter
                .u64_gauge("agnt5.runtime.durability.profile")
                .with_description("Active runtime durability profile; value is always 1.")
                .build(),
            startup_validated: meter
                .u64_gauge("agnt5.runtime.durability.startup_validated")
                .with_description("Whether engine-owned durability inputs passed startup validation.")
                .build(),
            production_guarantees_eligible: meter
                .u64_gauge("agnt5.runtime.durability.production_guarantees_eligible")
                .with_description("Whether the active runtime configuration is eligible for production durability claims.")
                .build(),
            degraded: meter
                .u64_gauge("agnt5.runtime.durability.degraded")
                .with_description("Fixed durability degradation reasons, recorded as 0 or 1.")
                .build(),
            capability: meter
                .u64_gauge("agnt5.runtime.durability.capability")
                .with_description("Supported worker protocol capabilities and whether the active profile requires them.")
                .build(),
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityInputs<'a> {
    pub sync_writes: bool,
    pub data_dir: &'a Path,
    pub lease_backed_push: bool,
    pub cluster_size: Option<usize>,
    pub write_quorum: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DurabilityStatus {
    pub profile: DurabilityProfile,
    pub startup_validated: bool,
    pub production_guarantees_eligible: bool,
    pub break_glass_acknowledged: bool,
    pub supported_protocol_capabilities: Vec<String>,
    pub required_protocol_capabilities: Vec<String>,
    pub degraded_reasons: Vec<String>,
}

impl DurabilityStatus {
    /// Gateway-only processes publish the profile and protocol contract while
    /// leaving engine-owned storage/quorum validation explicitly unearned.
    pub fn remote_engine(profile: DurabilityProfile) -> Self {
        Self {
            profile,
            startup_validated: false,
            production_guarantees_eligible: false,
            break_glass_acknowledged: false,
            supported_protocol_capabilities: supported_protocol_capabilities(),
            required_protocol_capabilities: profile.required_protocol_capabilities(),
            degraded_reasons: Vec::new(),
        }
    }

    pub fn development_default() -> Self {
        Self::remote_engine(DurabilityProfile::Development)
    }
}

pub fn evaluate_durability_profile(
    profile: DurabilityProfile,
    inputs: &DurabilityInputs<'_>,
    break_glass_ack: Option<&str>,
) -> Result<DurabilityStatus, String> {
    let mut degraded_reasons = Vec::new();
    if !inputs.sync_writes {
        degraded_reasons.push("journal_fsync_disabled".to_string());
    }
    if is_ephemeral_data_path(inputs.data_dir) {
        degraded_reasons.push("durability_storage_ephemeral".to_string());
    }
    if !inputs.lease_backed_push {
        degraded_reasons.push("lease_backed_push_disabled".to_string());
    }
    if let (Some(cluster_size), Some(write_quorum)) = (inputs.cluster_size, inputs.write_quorum) {
        let majority = cluster_size / 2 + 1;
        if write_quorum < majority {
            degraded_reasons.push("write_quorum_below_majority".to_string());
        }
    }

    let break_glass_acknowledged = validate_break_glass_ack(profile, break_glass_ack)?;
    if profile == DurabilityProfile::Production && !degraded_reasons.is_empty() {
        return Err(format!(
            "production durability profile rejected unsafe configuration: {}",
            degraded_reasons.join(",")
        ));
    }

    Ok(DurabilityStatus {
        profile,
        startup_validated: true,
        production_guarantees_eligible: profile == DurabilityProfile::Production,
        break_glass_acknowledged,
        supported_protocol_capabilities: supported_protocol_capabilities(),
        required_protocol_capabilities: profile.required_protocol_capabilities(),
        degraded_reasons,
    })
}

pub fn validate_break_glass_ack(
    profile: DurabilityProfile,
    break_glass_ack: Option<&str>,
) -> Result<bool, String> {
    let acknowledged = profile == DurabilityProfile::BreakGlass
        && break_glass_ack.map(str::trim) == Some(BREAK_GLASS_ACK_VALUE);
    if profile == DurabilityProfile::BreakGlass && !acknowledged {
        return Err(format!(
            "break-glass durability profile requires {BREAK_GLASS_ACK_ENV}={BREAK_GLASS_ACK_VALUE}"
        ));
    }
    Ok(acknowledged)
}

/// Capabilities that carry a durability guarantee.
pub fn durability_protocol_capabilities() -> Vec<String> {
    vec![
        engine_core::activation::DURABLE_ACTIVATION_V1_CAPABILITY.to_string(),
        engine_core::activation::DURABLE_SUSPENSION_V1_CAPABILITY.to_string(),
    ]
}

/// Every worker protocol capability this runtime negotiates: the durability
/// set plus additive optimizations a worker may opt into.
pub fn supported_protocol_capabilities() -> Vec<String> {
    let mut capabilities = durability_protocol_capabilities();
    capabilities.push(engine_core::activation::PULL_COMPLETION_LIFECYCLE_V1_CAPABILITY.to_string());
    capabilities.push(engine_core::activation::SERVER_SLOT_SCALING_V1_CAPABILITY.to_string());
    capabilities
}

fn is_ephemeral_data_path(path: &Path) -> bool {
    ["/tmp", "/private/tmp", "/var/tmp", "/dev/shm", "/run/shm"]
        .into_iter()
        .any(|prefix| path.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn safe_inputs() -> DurabilityInputs<'static> {
        DurabilityInputs {
            sync_writes: true,
            data_dir: Path::new("/var/lib/agnt5-runtime"),
            lease_backed_push: true,
            cluster_size: Some(3),
            write_quorum: Some(2),
        }
    }

    #[test]
    fn production_rejects_each_unsafe_durability_setting() {
        let cases = [
            (
                DurabilityInputs {
                    sync_writes: false,
                    ..safe_inputs()
                },
                "journal_fsync_disabled",
            ),
            (
                DurabilityInputs {
                    data_dir: Path::new("/tmp/agnt5-engine"),
                    ..safe_inputs()
                },
                "durability_storage_ephemeral",
            ),
            (
                DurabilityInputs {
                    data_dir: Path::new("/dev/shm/agnt5-engine"),
                    ..safe_inputs()
                },
                "durability_storage_ephemeral",
            ),
            (
                DurabilityInputs {
                    lease_backed_push: false,
                    ..safe_inputs()
                },
                "lease_backed_push_disabled",
            ),
            (
                DurabilityInputs {
                    write_quorum: Some(1),
                    ..safe_inputs()
                },
                "write_quorum_below_majority",
            ),
        ];

        for (inputs, reason) in cases {
            let error = evaluate_durability_profile(DurabilityProfile::Production, &inputs, None)
                .unwrap_err();
            assert!(error.contains(reason), "{error}");
        }
    }

    #[test]
    fn production_requires_worker_protocol_capabilities_and_earns_claim() {
        let status =
            evaluate_durability_profile(DurabilityProfile::Production, &safe_inputs(), None)
                .unwrap();
        assert!(status.production_guarantees_eligible);
        assert_eq!(status.required_protocol_capabilities.len(), 2);
        assert!(status.degraded_reasons.is_empty());
    }

    #[test]
    fn development_reports_degradation_without_earning_production_claim() {
        let status = evaluate_durability_profile(
            DurabilityProfile::Development,
            &DurabilityInputs {
                sync_writes: false,
                data_dir: Path::new("/tmp/agnt5-engine"),
                lease_backed_push: false,
                cluster_size: Some(3),
                write_quorum: Some(1),
            },
            None,
        )
        .unwrap();
        assert_eq!(status.degraded_reasons.len(), 4);
        assert!(!status.production_guarantees_eligible);
        assert!(status.required_protocol_capabilities.is_empty());
    }

    #[test]
    fn break_glass_requires_exact_ack_and_stays_degraded() {
        let inputs = DurabilityInputs {
            sync_writes: false,
            ..safe_inputs()
        };
        assert!(
            evaluate_durability_profile(DurabilityProfile::BreakGlass, &inputs, None,).is_err()
        );

        let status = evaluate_durability_profile(
            DurabilityProfile::BreakGlass,
            &inputs,
            Some(BREAK_GLASS_ACK_VALUE),
        )
        .unwrap();
        assert!(status.break_glass_acknowledged);
        assert!(!status.production_guarantees_eligible);
        assert_eq!(status.degraded_reasons, vec!["journal_fsync_disabled"]);
    }

    #[test]
    fn observability_dimensions_are_fixed_and_complete() {
        assert_eq!(
            DURABILITY_DEGRADED_REASONS,
            [
                "journal_fsync_disabled",
                "durability_storage_ephemeral",
                "lease_backed_push_disabled",
                "write_quorum_below_majority",
            ]
        );
        assert_eq!(supported_protocol_capabilities().len(), 4);
        assert_eq!(durability_protocol_capabilities().len(), 2);
        assert!(supported_protocol_capabilities().contains(
            &engine_core::activation::PULL_COMPLETION_LIFECYCLE_V1_CAPABILITY.to_string()
        ));
        assert!(supported_protocol_capabilities()
            .contains(&engine_core::activation::SERVER_SLOT_SCALING_V1_CAPABILITY.to_string()));
        assert!(!DurabilityProfile::Production
            .required_protocol_capabilities()
            .contains(
                &engine_core::activation::PULL_COMPLETION_LIFECYCLE_V1_CAPABILITY.to_string()
            ));
        assert!(!DurabilityProfile::Production
            .required_protocol_capabilities()
            .contains(&engine_core::activation::SERVER_SLOT_SCALING_V1_CAPABILITY.to_string()));
        assert_eq!(DurabilityProfile::Production.as_str(), "production");
    }

    #[test]
    fn production_environment_cannot_silently_use_development_profile() {
        assert!(
            validate_environment_profile("production", DurabilityProfile::Development).is_err()
        );
        assert!(validate_environment_profile("production", DurabilityProfile::Production).is_ok());
        assert!(validate_environment_profile("production", DurabilityProfile::BreakGlass).is_ok());
        assert!(
            validate_environment_profile("development", DurabilityProfile::Development).is_ok()
        );
    }
}
