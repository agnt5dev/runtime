use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum DatabaseBackend {
    Postgres,
    Sqlite,
    Turso,
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "agnt5-runtime",
    about = "Single-process AGNT5 runtime backed by PostgreSQL, SQLite, or Turso"
)]
pub struct Config {
    /// Persistence backend for this runtime.
    #[arg(
        long,
        env = "AGNT5_DATABASE_BACKEND",
        value_enum,
        default_value = "postgres"
    )]
    pub database_backend: DatabaseBackend,

    /// PostgreSQL or Turso/libSQL connection URL.
    #[arg(long, env = "AGNT5_DATABASE_URL", hide_env_values = true)]
    pub database_url: Option<String>,

    /// Local SQLite database file used by the `sqlite` backend.
    #[arg(long, env = "AGNT5_SQLITE_PATH", default_value = "./agnt5-runtime.db")]
    pub sqlite_path: PathBuf,

    /// Turso database token. `TURSO_AUTH_TOKEN` is accepted as a fallback.
    #[arg(long, env = "AGNT5_DATABASE_AUTH_TOKEN", hide_env_values = true)]
    pub database_auth_token: Option<String>,

    /// Logical storage and advisory-lock namespace.
    #[arg(long, env = "AGNT5_RUNTIME_NAMESPACE", default_value = "agnt5-oss")]
    pub namespace: String,

    /// The only project accepted by this runtime.
    #[arg(long, env = "AGNT5_PROJECT_ID", default_value = "default")]
    pub project_id: String,

    /// Deployment lane assigned to every HTTP submission.
    #[arg(long, env = "AGNT5_DEPLOYMENT_ID", default_value = "local")]
    pub deployment_id: String,

    /// Workspace label retained in public run records.
    #[arg(long, env = "AGNT5_WORKSPACE_ID", default_value = "local")]
    pub workspace_id: String,

    /// HTTP API listen address.
    #[arg(long, env = "AGNT5_HTTP_ADDR", default_value = "127.0.0.1:34185")]
    pub http_addr: SocketAddr,

    /// Engine gRPC listen address used by pull workers.
    #[arg(long, env = "AGNT5_GRPC_ADDR", default_value = "127.0.0.1:34186")]
    pub grpc_addr: SocketAddr,

    /// Stable runtime node identifier used in status and telemetry.
    #[arg(long, env = "AGNT5_NODE_ID", default_value = "runtime-1")]
    pub node_id: String,

    /// Maximum PostgreSQL pool size. The advisory lock uses one additional
    /// dedicated connection.
    #[arg(long, env = "AGNT5_POSTGRES_MAX_CONNECTIONS", default_value_t = 20)]
    pub max_connections: u32,

    /// Duration of the renewable SQLite/Turso single-writer lease.
    #[arg(long, env = "AGNT5_WRITER_LEASE_SECS", default_value_t = 15)]
    pub writer_lease_secs: u64,

    /// SQLite/Turso writer-lease renewal cadence.
    #[arg(long, env = "AGNT5_WRITER_HEARTBEAT_SECS", default_value_t = 5)]
    pub writer_heartbeat_secs: u64,

    /// Projection checkpoint cadence in records.
    #[arg(long, env = "AGNT5_CHECKPOINT_INTERVAL", default_value_t = 1)]
    pub checkpoint_interval: u64,

    /// Maximum synchronous `/run` and `/stream` lifetime.
    #[arg(long, env = "AGNT5_RUN_TIMEOUT_SECS", default_value_t = 300)]
    pub run_timeout_secs: u64,

    /// Maximum in-memory journal cache size.
    #[arg(
        long,
        env = "AGNT5_CACHE_MAX_BYTES",
        default_value_t = 256 * 1024 * 1024
    )]
    pub cache_max_bytes: u64,

    /// Optional static API key for the HTTP surface.
    #[arg(long, env = "AGNT5_API_KEY", hide_env_values = true)]
    pub api_key: Option<String>,

    /// Optional HS256 secret for bearer JWTs on the HTTP surface.
    #[arg(long, env = "AGNT5_JWT_SECRET", hide_env_values = true)]
    pub jwt_secret: Option<String>,

    /// Delivery attempts allowed before an expired lease is dead-lettered.
    #[arg(long, env = "AGNT5_LEASE_MAX_ATTEMPTS", default_value_t = 3)]
    pub lease_max_attempts: u32,

    /// Apply embedded migrations, acquire/release the writer lock, and exit.
    #[arg(long, default_value_t = false)]
    pub migrate_only: bool,
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_with_signing_key(&engine_core::lease_fence::worker_session_signing_key())
    }

    /// Validate with an explicit worker-session signing key, which the runtime
    /// otherwise reads from `AGNT5_WORKER_SESSION_SIGNING_KEY`.
    pub fn validate_with_signing_key(&self, signing_key: &str) -> anyhow::Result<()> {
        for (name, value) in [
            ("runtime namespace", self.namespace.as_str()),
            ("project ID", self.project_id.as_str()),
            ("deployment ID", self.deployment_id.as_str()),
            ("workspace ID", self.workspace_id.as_str()),
            ("node ID", self.node_id.as_str()),
        ] {
            if value.trim().is_empty() {
                anyhow::bail!("{name} must not be empty");
            }
        }
        match self.database_backend {
            DatabaseBackend::Postgres if self.database_url().is_none() => {
                anyhow::bail!("AGNT5_DATABASE_URL is required for the PostgreSQL backend")
            }
            DatabaseBackend::Turso if self.database_url().is_none() => {
                anyhow::bail!(
                    "AGNT5_DATABASE_URL or TURSO_DATABASE_URL is required for the Turso backend"
                )
            }
            DatabaseBackend::Turso if self.database_auth_token().is_none() => {
                anyhow::bail!(
                    "AGNT5_DATABASE_AUTH_TOKEN or TURSO_AUTH_TOKEN is required for the Turso backend"
                )
            }
            DatabaseBackend::Sqlite if self.sqlite_path.as_os_str().is_empty() => {
                anyhow::bail!("SQLite database path must not be empty")
            }
            _ => {}
        }
        if self.database_backend == DatabaseBackend::Postgres && self.max_connections == 0 {
            anyhow::bail!("PostgreSQL max connections must be greater than zero");
        }
        if self.database_backend != DatabaseBackend::Postgres {
            if self.writer_lease_secs == 0 {
                anyhow::bail!("writer lease duration must be greater than zero");
            }
            if self.writer_heartbeat_secs == 0 {
                anyhow::bail!("writer heartbeat interval must be greater than zero");
            }
            if self.writer_heartbeat_secs >= self.writer_lease_secs {
                anyhow::bail!("writer heartbeat interval must be shorter than the lease duration");
            }
        }
        if self.checkpoint_interval == 0 {
            anyhow::bail!("checkpoint interval must be greater than zero");
        }
        if self.run_timeout_secs == 0 {
            anyhow::bail!("run timeout must be greater than zero");
        }
        if self
            .api_key
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            anyhow::bail!("AGNT5_API_KEY must not be empty when configured");
        }
        if self
            .jwt_secret
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            anyhow::bail!("AGNT5_JWT_SECRET must not be empty when configured");
        }
        // The signing key signs worker sessions and lease fences. Its
        // defaults are public in the source, so anyone who can reach a
        // runtime using one can forge both. Allow them only while both
        // listeners stay on this machine.
        if self.listens_beyond_loopback() && is_public_signing_key(signing_key) {
            anyhow::bail!(
                "{} must be set to a secret of your own when AGNT5_HTTP_ADDR or AGNT5_GRPC_ADDR \
                 listens beyond 127.0.0.1 (for example, generate one with `openssl rand -hex 32`)",
                engine_core::lease_fence::WORKER_SESSION_SIGNING_KEY_ENV
            );
        }
        // Without either credential the HTTP API answers anyone who can reach
        // it, so that is allowed only while nothing else can.
        if self.listens_beyond_loopback() && self.api_key.is_none() && self.jwt_secret.is_none() {
            anyhow::bail!(
                "AGNT5_API_KEY or AGNT5_JWT_SECRET is required when AGNT5_HTTP_ADDR or \
                 AGNT5_GRPC_ADDR listens beyond 127.0.0.1"
            );
        }
        Ok(())
    }

    /// Whether either listener accepts connections from other machines.
    pub fn listens_beyond_loopback(&self) -> bool {
        !self.http_addr.ip().is_loopback() || !self.grpc_addr.ip().is_loopback()
    }

    pub fn database_url(&self) -> Option<String> {
        self.database_url
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                (self.database_backend == DatabaseBackend::Turso)
                    .then(|| std::env::var("TURSO_DATABASE_URL").ok())
                    .flatten()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
    }

    pub fn database_auth_token(&self) -> Option<String> {
        self.database_auth_token
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var("TURSO_AUTH_TOKEN")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
    }
}

fn is_public_signing_key(key: &str) -> bool {
    let key = key.trim();
    key.is_empty()
        || key == engine_core::lease_fence::DEFAULT_WORKER_SESSION_SIGNING_KEY
        || key == engine_core::lease_fence::DEFAULT_LOCAL_HA_WORKER_SESSION_SIGNING_KEY
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_KEY: &str = engine_core::lease_fence::DEFAULT_WORKER_SESSION_SIGNING_KEY;

    fn sqlite_config(extra: &[&str]) -> Config {
        let mut args = vec!["agnt5-runtime", "--database-backend", "sqlite"];
        args.extend_from_slice(extra);
        let mut config = Config::try_parse_from(args).expect("parse SQLite configuration");
        // Credentials also fall back to the environment, and CI sets
        // AGNT5_API_KEY for its own jobs. Keep only the ones a test passes.
        if !extra.contains(&"--api-key") {
            config.api_key = None;
        }
        if !extra.contains(&"--jwt-secret") {
            config.jwt_secret = None;
        }
        config
    }

    #[test]
    fn default_signing_key_is_allowed_on_loopback() {
        sqlite_config(&[])
            .validate_with_signing_key(DEFAULT_KEY)
            .expect("loopback listeners may use the public development key");
    }

    #[test]
    fn public_signing_keys_are_refused_beyond_loopback() {
        for args in [
            ["--grpc-addr", "0.0.0.0:34186", "--api-key", "key"],
            ["--http-addr", "0.0.0.0:34185", "--api-key", "key"],
        ] {
            let config = sqlite_config(&args);
            for key in [
                DEFAULT_KEY,
                engine_core::lease_fence::DEFAULT_LOCAL_HA_WORKER_SESSION_SIGNING_KEY,
                "  ",
            ] {
                let error = config
                    .validate_with_signing_key(key)
                    .expect_err("public signing key must be refused beyond loopback");
                assert!(error
                    .to_string()
                    .contains(engine_core::lease_fence::WORKER_SESSION_SIGNING_KEY_ENV));
            }
        }
    }

    #[test]
    fn own_signing_key_is_allowed_beyond_loopback() {
        sqlite_config(&[
            "--grpc-addr",
            "0.0.0.0:34186",
            "--http-addr",
            "0.0.0.0:34185",
            "--api-key",
            "key",
        ])
        .validate_with_signing_key("a-secret-of-our-own")
        .expect("an operator-supplied key may listen beyond loopback");
    }

    #[test]
    fn http_credentials_are_required_beyond_loopback() {
        let error = sqlite_config(&["--http-addr", "0.0.0.0:34185"])
            .validate_with_signing_key("a-secret-of-our-own")
            .expect_err("an unauthenticated API must stay on loopback");
        assert!(error.to_string().contains("AGNT5_API_KEY"));

        sqlite_config(&["--http-addr", "0.0.0.0:34185", "--jwt-secret", "jwt"])
            .validate_with_signing_key("a-secret-of-our-own")
            .expect("a JWT secret alone is enough");
        sqlite_config(&[])
            .validate_with_signing_key("a-secret-of-our-own")
            .expect("loopback may run without credentials");
    }

    #[test]
    fn postgres_remains_the_default_backend() {
        let config = Config::try_parse_from([
            "agnt5-runtime",
            "--database-url",
            "postgresql://localhost/agnt5",
        ])
        .expect("parse PostgreSQL defaults");
        assert_eq!(config.database_backend, DatabaseBackend::Postgres);
        config.validate().expect("validate PostgreSQL defaults");
    }

    #[test]
    fn sqlite_requires_heartbeat_shorter_than_lease() {
        let config = Config::try_parse_from([
            "agnt5-runtime",
            "--database-backend",
            "sqlite",
            "--writer-lease-secs",
            "5",
            "--writer-heartbeat-secs",
            "5",
        ])
        .expect("parse SQLite configuration");
        assert!(config.validate().is_err());
    }

    #[test]
    fn turso_accepts_explicit_url_and_token() {
        let config = Config::try_parse_from([
            "agnt5-runtime",
            "--database-backend",
            "turso",
            "--database-url",
            "libsql://example.turso.io",
            "--database-auth-token",
            "secret",
        ])
        .expect("parse Turso configuration");
        assert_eq!(config.database_backend, DatabaseBackend::Turso);
        config.validate().expect("validate Turso configuration");
    }
}
