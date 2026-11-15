//! Where the processor reports finished runs and model usage, and how model
//! calls get a price.
//!
//! The processor sees every terminal run and every model call as it projects
//! the journal. The hosted runtime sends those to the control plane for
//! billing and analytics, and prices model calls from a pricing table it
//! fetches. Both sit behind these two traits so the processor itself carries
//! no HTTP client and knows no control plane. The OSS runtime passes neither,
//! and nothing is reported or priced.

use async_trait::async_trait;
use engine_core::types::Record;

/// Receives finished runs and model calls as the processor projects them.
#[async_trait]
pub trait UsageReporter: Send + Sync {
    /// A run reached a terminal state. `run_data` is the encoded `Run`.
    async fn run_finished(&self, run_data: &[u8], project_id: &str);

    /// A model call was journaled.
    async fn model_call(&self, event: LLMEventSummary);
}

/// Prices model calls recorded in the journal.
pub trait ModelPricing: Send + Sync {
    /// A copy of a model-call record with its cost added, or `None` when the
    /// record is not a model call or no price is known for its model.
    fn enrich_record(&self, record: &Record) -> Option<Record>;
}

/// One model call, as reported for billing and analytics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LLMEventSummary {
    pub run_id: String,
    pub project_id: String,
    pub deployment_id: String,
    pub component_name: String,
    pub provider: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    /// Input tokens written to a prompt cache; a subset of `input_tokens`.
    pub cache_write_tokens: i64,
    pub cost_micros: i64,
    pub latency_ms: i64,
    pub step_key: Option<String>,
    pub timestamp_ns: i64,
    /// `native`, or `otlp` for an observed run built from an app's traces.
    pub origin: String,
}

/// Where a run came from, as analytics records it.
pub fn run_origin(metadata: &std::collections::HashMap<String, String>) -> &'static str {
    if engine_core::journal_contract::is_observed_metadata(metadata) {
        engine_core::journal_contract::ORIGIN_OTLP
    } else {
        "native"
    }
}
