//! HTTP/gRPC ingress and JWT authentication.
//!
//! A valid JWT grants access to the single-tenant runtime. This crate does not
//! define roles, scopes, or permissions.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JwtConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
}
