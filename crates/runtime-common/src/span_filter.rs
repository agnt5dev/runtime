//! Span exporter wrapper that drops HTTP/2 transport-layer spans before they
//! leave the process.
//!
//! Belt-and-braces companion to the `EnvFilter` set on the OTel span layer in
//! [`crate::telemetry`]. The subscriber filter blocks span *creation* (so the
//! CPU cost of recording fields and propagating contexts is avoided), and this
//! exporter wrapper guarantees nothing slips through if a future change
//! removes/relaxes the filter, or if a third-party crate emits spans with a
//! `target` we did not anticipate.
//!
//! Mirrors `sdk/sdk-core/src/span_filter.rs` so the runtime and the SDK use
//! the same drop list. Keep them in sync.

use std::fmt;

use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};

/// Wraps another `SpanExporter` and drops spans whose name matches a known
/// transport-layer pattern (h2, hyper, tonic, tower internals).
#[derive(Debug)]
pub struct FilteringSpanExporter<E: SpanExporter> {
    inner: E,
}

impl<E: SpanExporter> FilteringSpanExporter<E> {
    pub fn new(inner: E) -> Self {
        Self { inner }
    }

    /// Returns `true` if the span name matches a known transport-internal
    /// pattern that should never reach the trace backend.
    fn should_filter_span(span_name: &str) -> bool {
        let name = span_name.to_lowercase();
        FILTER_PATTERNS.iter().any(|p| name.contains(p))
    }
}

/// Patterns matched against the lowercased span name. Add new entries here when
/// a noisy crate slips through; do not weaken the patterns in place — they are
/// the last line of defense.
const FILTER_PATTERNS: &[&str] = &[
    // Generic h2 / hyper / tonic markers
    ".h2",
    "recv.h2",
    "send.h2",
    "http2.framer",
    "transport: http2",
    "grpc.io/server/bidi_stream",
    "grpc.io/client/bidi_stream",
    // Specific h2 frame-level spans observed in production
    "try_reclaim_frame",
    "pop_frame",
    "framedwrite",
    "framedread",
    "popped",
    "stream flow",
    "connection flow",
    "poll_ready",
    "poll_next",
    "poll",
    "reserve_capacity",
    "try_assign_capacity",
    "prioritize::queue_frame",
    "assign_connection_capacity",
    "send_data",
    "hpack::",
    "recv_stream_window_update",
    "updating stream flow",
    "updating connection flow",
    "decode_frame",
];

impl<E: SpanExporter + fmt::Debug> SpanExporter for FilteringSpanExporter<E> {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = Result<(), OTelSdkError>> + Send {
        let filtered: Vec<SpanData> = batch
            .into_iter()
            .filter(|span| !Self::should_filter_span(&span.name))
            .collect();
        self.inner.export(filtered)
    }

    fn shutdown(&mut self) -> Result<(), OTelSdkError> {
        self.inner.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestExporter = opentelemetry_otlp::SpanExporter;

    #[test]
    fn drops_h2_frame_spans() {
        for name in [
            "try_reclaim_frame",
            "FramedWrite::flush",
            "FramedRead::poll_next",
            "updating stream flow",
            "updating connection flow",
            "hpack::decode",
            "hpack::encode",
            "Prioritize::queue_frame",
            "pop_frame",
            "popped",
        ] {
            assert!(
                FilteringSpanExporter::<TestExporter>::should_filter_span(name),
                "expected to drop span name {name:?}"
            );
        }
    }

    #[test]
    fn keeps_application_spans() {
        for name in [
            "engine.append_batch",
            "GET /api/v1/deployments/:id",
            "coordinator.dispatch_invocation",
            "workflow.process_data",
        ] {
            assert!(
                !FilteringSpanExporter::<TestExporter>::should_filter_span(name),
                "expected to keep span name {name:?}"
            );
        }
    }
}
