//! Shared types, backend traits, and configuration for the AGNT5 dataplane.
//!
//! This crate defines the abstractions that allow Gateway, Coordinator, and Engine
//! to run either in-process (`--target=all`) or as separate services (disaggregated).

pub mod authority;
pub mod backend;
pub mod boot;
pub mod boot_service;
pub mod claim_registry;
pub mod config;
pub mod data_root;
pub mod durability;
pub mod error;
pub mod error_codes;
pub mod grpc_coordinator;
pub mod grpc_engine;
pub mod lifecycle;
pub mod profiling;
pub mod span_filter;
pub mod telemetry;
pub mod util;

pub use boot::{BootProgress, BootState, BootStatus, BootStep};
pub use claim_registry::{ClaimEntry, ClaimRegistry};
pub use grpc_coordinator::GrpcCoordinatorBackend;
pub use grpc_engine::GrpcEngineBackend;
pub use lifecycle::NodeLifecycleState;

pub mod work;

pub mod delivery;

pub mod invocation;
