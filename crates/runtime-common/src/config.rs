//! Runtime configuration parsed from CLI args and environment.

use clap::{Parser, ValueEnum};

/// Which components this runtime instance runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Target {
    /// Run all components: Gateway + Engine + Coordinator (monolith mode)
    All,
    /// Run only the Engine (journal, projections, queries, replication)
    Engine,
    /// Run only the Gateway (HTTP API, SSE streaming, auth)
    Gateway,
    /// Run only the Coordinator (worker lifecycle, dispatch)
    Coordinator,
}

/// AGNT5 Runtime — single binary for the entire runtime.
///
/// Supports three deployment modes:
/// - Solo: 1 instance, --target=all, no HA
/// - HA: 3 instances, --target=all, quorum replication
/// - Managed: separate --target per component, scale independently
#[derive(Debug, Parser)]
#[command(name = "agnt5-runtime", about = "AGNT5 Runtime Server")]
pub struct RuntimeArgs {
    /// Which components to run
    #[arg(long, value_enum, default_value = "all", env = "AGNT5_RUNTIME_TARGET")]
    pub target: Target,
}
