//! Small shared utilities used across runtime binaries and crates.

/// Strip the `http://` or `https://` scheme from a URL, returning the
/// remainder. Used when passing S3 endpoints to libraries that expect a
/// bare `host[:port]`.
pub fn strip_scheme(url: &str) -> String {
    url.trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

/// Determine this node's hostname for telemetry + cluster identity.
///
/// Precedence: `HOSTNAME` → `POD_NAME` → `"standalone-0"`. Reads env vars
/// directly (not cached) so tests can override per-call.
pub fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("POD_NAME"))
        .unwrap_or_else(|_| "standalone-0".to_string())
}

/// Derive a stable coordinator identity.
///
/// Precedence: `AGNT5_COORDINATOR_ID` → `ENGINE_NODE_ID` → `hostname()`.
/// In a single-node dev loop this collapses to `"standalone-0"`; in a
/// cluster each pod reports its own pod name.
pub fn coordinator_id() -> String {
    std::env::var("AGNT5_COORDINATOR_ID")
        .or_else(|_| std::env::var("ENGINE_NODE_ID"))
        .unwrap_or_else(|_| hostname())
}

/// Number of outbound gRPC clients to keep per remote runtime backend.
///
/// Each client owns its own HTTP/2 connection. A pool avoids forcing
/// disaggregated gateway/coordinator traffic through one local client and one
/// h2 session under load.
pub fn grpc_client_pool_size() -> usize {
    std::env::var("AGNT5_RUNTIME_GRPC_CLIENT_POOL_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|size| *size > 0)
        .unwrap_or(8)
}
