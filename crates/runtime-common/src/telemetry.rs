//! OpenTelemetry traces + metrics + logs initialization for dataplane binaries.
//!
//! Reads `OTEL_EXPORTER_OTLP_ENDPOINT` (same var as platform/SDK).
//! When set, exports traces, metrics, and logs via OTLP/gRPC to the collector.
//! When empty, runs with console-only tracing (no OTEL overhead).
//!
//! Sentry is initialized separately via [`init_sentry`]; when `SENTRY_DSN` is
//! set, errors and panics are reported to Sentry, and the [`init`] subscriber
//! gains a `sentry_tracing` layer so error-level tracing events flow through.

use std::sync::OnceLock;
use std::time::Duration;
use std::{borrow::Cow, collections::BTreeMap};

use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Stored globally so `shutdown()` can flush the batch log processor.
static LOG_PROVIDER: OnceLock<SdkLoggerProvider> = OnceLock::new();

/// Initialize tracing + metrics + logs.
///
/// If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, exports via OTLP/gRPC and
/// returns `true`. Otherwise sets up console-only logging and returns
/// `false`. Callers can use the return value to decide whether to
/// construct metric instruments (there's no point building counters
/// that will only ever write to a noop meter).
///
/// `service_version` is forwarded verbatim as the `service.version`
/// resource attribute. Callers typically pass `env!("CARGO_PKG_VERSION")`
/// from their own crate so each binary reports its own version.
pub fn init(service_name: &str, service_version: &str, node_id: &str) -> bool {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default();

    if endpoint.is_empty() {
        // Console-only mode (still wire Sentry layer; no-op if not initialized).
        let sentry_layer = sentry_tracing::layer().event_filter(|md| match *md.level() {
            tracing::Level::ERROR => sentry_tracing::EventFilter::Event,
            tracing::Level::WARN | tracing::Level::INFO => sentry_tracing::EventFilter::Breadcrumb,
            _ => sentry_tracing::EventFilter::Ignore,
        });
        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(tracing_subscriber::fmt::layer())
            .with(sentry_layer)
            .init();
        info!("Telemetry: console-only (OTEL_EXPORTER_OTLP_ENDPOINT not set)");
        return false;
    }

    // Build resource with service identity
    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("service.version", service_version.to_string()),
            KeyValue::new("node.id", node_id.to_string()),
        ])
        .build();

    // Trace provider
    let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .expect("failed to create trace exporter");

    // Belt-and-braces filter: drops h2/HTTP2 transport spans at export time
    // even if the subscriber-level filter is misconfigured. See span_filter.rs.
    let filtered_exporter = crate::span_filter::FilteringSpanExporter::new(trace_exporter);

    let trace_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(filtered_exporter)
        .build();

    // Get tracer before setting as global (same pattern as sdk-core)
    let tracer = trace_provider.tracer("agnt5-engine");
    global::set_tracer_provider(trace_provider);

    // Metric provider
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .expect("failed to create metric exporter");

    let metric_reader = opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
        .with_interval(Duration::from_secs(15))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(metric_reader)
        .build();

    global::set_meter_provider(meter_provider);

    // Log provider
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .expect("failed to create log exporter");

    let log_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();

    LOG_PROVIDER
        .set(log_provider)
        .expect("LOG_PROVIDER already initialized");

    // Bridge tracing events → OTEL logs
    let log_bridge = OpenTelemetryTracingBridge::new(LOG_PROVIDER.get().unwrap());

    // Wire tracing-subscriber → OpenTelemetry
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Console filter: controlled by RUST_LOG env var
    let console_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // OTEL span filter: silence h2/hyper/tonic/tower transport spans so we don't
    // export ~177K spans/sec of HTTP/2 frame-level noise to the trace backend.
    // Override at runtime via OTEL_SPAN_FILTER (same syntax as RUST_LOG).
    let otel_span_filter =
        EnvFilter::try_new(std::env::var("OTEL_SPAN_FILTER").unwrap_or_else(|_| {
            "info,h2=error,hyper=error,tonic=warn,tower=warn,h2::proto=off,h2::codec=off".into()
        }))
        .expect("invalid OTEL_SPAN_FILTER");

    // OTEL log filter: export info+ but suppress noisy transport crates.
    // Override at runtime via OTEL_LOG_FILTER.
    let otel_log_filter = EnvFilter::try_new(
        std::env::var("OTEL_LOG_FILTER")
            .unwrap_or_else(|_| "info,h2=error,hyper=error,tonic=warn,tower=warn".into()),
    )
    .expect("invalid OTEL_LOG_FILTER");

    // Sentry layer: forwards error-level tracing events as Sentry events and
    // breadcrumbs (initialized in `init_sentry`; this layer is a no-op if the
    // Sentry hub has no client). EventFilter::Event upgrades errors to
    // captures; everything else becomes a breadcrumb attached to the next
    // captured event.
    let sentry_layer = sentry_tracing::layer().event_filter(|md| match *md.level() {
        tracing::Level::ERROR => sentry_tracing::EventFilter::Event,
        tracing::Level::WARN | tracing::Level::INFO => sentry_tracing::EventFilter::Breadcrumb,
        _ => sentry_tracing::EventFilter::Ignore,
    });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(console_filter))
        .with(otel_layer.with_filter(otel_span_filter))
        .with(log_bridge.with_filter(otel_log_filter))
        .with(sentry_layer)
        .init();

    // W3C trace context propagation (matches SDK + platform)
    global::set_text_map_propagator(TraceContextPropagator::new());

    info!(endpoint = %endpoint, "Telemetry: OpenTelemetry initialized (OTLP/gRPC)");

    true
}

/// Shutdown telemetry providers, flushing any pending logs.
pub fn shutdown() {
    tracing::info!("Telemetry shutdown");
    if let Some(lp) = LOG_PROVIDER.get() {
        if let Err(e) = lp.shutdown() {
            eprintln!("Failed to shutdown log provider: {e}");
        }
    }
}

/// Re-exported so callers can hold the guard without depending on the sentry
/// crate directly.
pub type SentryGuard = sentry::ClientInitGuard;

fn select_sentry_release(explicit: Option<String>) -> Option<Cow<'static, str>> {
    explicit
        .filter(|release| !release.is_empty())
        .map(Cow::Owned)
        .or_else(|| sentry::release_name!())
}

/// Capture one route-template-scoped gateway 5xx event.
///
/// Callers must pass a matched route template, never a raw URI, so run IDs,
/// project IDs, and other customer identifiers cannot enter grouping keys.
pub fn capture_gateway_server_error(method: &str, route: &str, status: u16) {
    let status_text = status.to_string();
    sentry::with_scope(
        |scope| {
            scope.set_tag("http.method", method);
            scope.set_tag("http.route", route);
            scope.set_tag("http.status_code", &status_text);
            scope.set_fingerprint(Some(
                ["gateway-http-5xx", method, route, status_text.as_str()].as_slice(),
            ));

            let mut context = BTreeMap::new();
            context.insert("method".to_string(), method.into());
            context.insert("route".to_string(), route.into());
            context.insert("status".to_string(), status.into());
            scope.set_context(
                "gateway_response",
                sentry::protocol::Context::Other(context),
            );
        },
        || {
            sentry::capture_message(
                &format!("gateway request failed: {method} {route} returned {status}"),
                sentry::Level::Error,
            );
        },
    );
}

/// Safe, structured evidence for a runtime state divergence.
///
/// This intentionally has no record payload, request metadata, fence token,
/// or session credential fields. Digests, offsets, lifecycle fields, and node
/// identities are sufficient to compare replicas without exporting customer
/// data to Sentry.
#[derive(Debug, Clone)]
pub struct RuntimeDivergenceEvent {
    pub component: &'static str,
    pub divergence_type: String,
    pub partition_id: u32,
    pub offset: Option<u64>,
    pub range_start: Option<u64>,
    pub range_end_exclusive: Option<u64>,
    pub epoch: Option<u64>,
    pub local_node_id: Option<String>,
    pub peer_node_id: Option<String>,
    pub expected_digest_v1: Option<String>,
    pub actual_digest_v1: Option<String>,
    pub differing_fields: Option<String>,
    pub projection_checkpoint: Option<u64>,
    pub projection_applied: Option<u64>,
    pub projection_generation_id: Option<String>,
    pub projection_provenance_state: Option<String>,
    pub projection_source_contract_version: Option<u32>,
    pub diagnostic_reason: Option<String>,
    pub disposition: Option<String>,
    pub committed_tail: Option<u64>,
}

/// Capture one explicitly fingerprinted divergence event.
///
/// Call this at the first-detection or startup-admission boundary. Callers are
/// responsible for deduplicating repeated hot-path observations.
pub fn capture_runtime_divergence(event: &RuntimeDivergenceEvent) {
    let partition_id = event.partition_id.to_string();
    sentry::with_scope(
        |scope| {
            scope.set_tag("runtime.component", event.component);
            scope.set_tag("runtime.divergence_type", &event.divergence_type);
            scope.set_tag("runtime.partition_id", &partition_id);
            scope.set_fingerprint(Some(
                [
                    "runtime-state-divergence",
                    event.component,
                    event.divergence_type.as_str(),
                    partition_id.as_str(),
                ]
                .as_slice(),
            ));

            let mut context = BTreeMap::new();
            context.insert("component".to_string(), event.component.into());
            context.insert(
                "divergence_type".to_string(),
                event.divergence_type.clone().into(),
            );
            context.insert("partition_id".to_string(), event.partition_id.into());
            if let Some(value) = event.offset {
                context.insert("offset".to_string(), value.into());
            }
            if let Some(value) = event.range_start {
                context.insert("range_start".to_string(), value.into());
            }
            if let Some(value) = event.range_end_exclusive {
                context.insert("range_end_exclusive".to_string(), value.into());
            }
            if let Some(value) = event.epoch {
                context.insert("epoch".to_string(), value.into());
            }
            if let Some(value) = event.local_node_id.as_ref() {
                context.insert("local_node_id".to_string(), value.clone().into());
            }
            if let Some(value) = event.peer_node_id.as_ref() {
                context.insert("peer_node_id".to_string(), value.clone().into());
            }
            if let Some(value) = event.expected_digest_v1.as_ref() {
                context.insert("expected_digest_v1".to_string(), value.clone().into());
            }
            if let Some(value) = event.actual_digest_v1.as_ref() {
                context.insert("actual_digest_v1".to_string(), value.clone().into());
            }
            if let Some(value) = event.differing_fields.as_ref() {
                context.insert("differing_fields".to_string(), value.clone().into());
            }
            if let Some(value) = event.projection_checkpoint {
                context.insert("projection_checkpoint".to_string(), value.into());
            }
            if let Some(value) = event.projection_applied {
                context.insert("projection_applied".to_string(), value.into());
            }
            if let Some(value) = event.projection_generation_id.as_ref() {
                context.insert("projection_generation_id".to_string(), value.clone().into());
            }
            if let Some(value) = event.projection_provenance_state.as_ref() {
                context.insert(
                    "projection_provenance_state".to_string(),
                    value.clone().into(),
                );
            }
            if let Some(value) = event.projection_source_contract_version {
                context.insert(
                    "projection_source_contract_version".to_string(),
                    value.into(),
                );
            }
            if let Some(value) = event.diagnostic_reason.as_ref() {
                context.insert("diagnostic_reason".to_string(), value.clone().into());
            }
            if let Some(value) = event.disposition.as_ref() {
                context.insert("disposition".to_string(), value.clone().into());
            }
            if let Some(value) = event.committed_tail {
                context.insert("committed_tail".to_string(), value.into());
            }
            scope.set_context(
                "runtime_divergence",
                sentry::protocol::Context::Other(context),
            );
        },
        || {
            sentry::capture_message(
                &format!(
                    "runtime state divergence: {} partition {}",
                    event.divergence_type, event.partition_id
                ),
                sentry::Level::Error,
            );
        },
    );
}

/// Initialize Sentry error reporting. Must be called from `main` *before*
/// the tokio runtime starts so the panic hook installs cleanly, and the
/// returned guard must be bound for the lifetime of the process — dropping
/// it flushes pending events.
///
/// No-op when `SENTRY_DSN` is empty/unset (returns `None`).
///
/// Environment variables (standard Sentry names; the k8s manifest maps
/// these from Infisical-managed secret keys like `RUNTIME_SENTRY_DSN`):
/// - `SENTRY_DSN` — required to enable; unset disables Sentry entirely.
/// - `SENTRY_ENVIRONMENT` — overrides the `environment` argument.
/// - `SENTRY_TRACES_SAMPLE_RATE` — fraction of transactions/spans to
///   capture, in `[0.0, 1.0]`. Defaults to `0.0` (errors only). The
///   sentry-tracing layer converts every `tracing` span into a Sentry
///   span when this is > 0; the gateway also wraps each HTTP request
///   in a Sentry transaction via `sentry-tower`. **Production should
///   use a low value (e.g. 0.01–0.05)** — the runtime emits enough
///   spans/sec that 1.0 will saturate the Sentry quota.
///
/// `service_name` is attached as a tag so events from engine/gateway/
/// coordinator can be filtered server-side.
pub fn init_sentry(service_name: &str, environment: &str) -> Option<SentryGuard> {
    let dsn = std::env::var("SENTRY_DSN").ok().filter(|s| !s.is_empty())?;

    let environment = std::env::var("SENTRY_ENVIRONMENT")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| environment.to_string());

    let traces_sample_rate = std::env::var("SENTRY_TRACES_SAMPLE_RATE")
        .ok()
        .and_then(|s| s.parse::<f32>().ok())
        .unwrap_or(0.0);
    let release = select_sentry_release(std::env::var("SENTRY_RELEASE").ok());

    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release,
            environment: Some(environment.into()),
            traces_sample_rate,
            attach_stacktrace: true,
            // We rely on OpenTelemetry for traces and don't want Sentry
            // capturing user IPs / request bodies by default.
            send_default_pii: false,
            ..Default::default()
        },
    ));

    sentry::configure_scope(|scope| {
        scope.set_tag("service", service_name);
    });

    eprintln!("[telemetry] Sentry initialized service={service_name}");
    Some(guard)
}

#[cfg(test)]
mod tests {
    use super::select_sentry_release;

    #[test]
    fn explicit_sentry_release_overrides_crate_version() {
        assert_eq!(
            select_sentry_release(Some("agnt5-runtime@build-sha".to_string())).as_deref(),
            Some("agnt5-runtime@build-sha")
        );
    }

    #[test]
    fn empty_sentry_release_uses_compile_time_fallback() {
        assert!(select_sentry_release(Some(String::new())).is_some());
    }
}
