mod auth;
mod config;
mod execution_service;
mod health;
mod http;
mod lease_sweeper;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use engine_cache::{CacheConfig, RecordCache};
use engine_core::storage_contract::{
    DispatchOutboxStore, ProjectionCheckpointStore, ProjectionCommitter, ProjectionStateReader,
    TerminalObligationStore,
};
use engine_core::traits::Segment;
use engine_libsql::{LibsqlDatabase, LibsqlStorage, LibsqlWriterLease};
use engine_postgres::{PostgresStorage, PostgresWriterLock};
use engine_processor::timer_poller::{TimerPoller, TimerPollerConfig};
use engine_processor::{run_single_partition_processor, PartitionProcessorExit};
use engine_proto::engine_service_server::EngineServiceServer;
use engine_proto::execution_engine_service_server::ExecutionEngineServiceServer;
use engine_service::in_process_engine::InProcessEngine;
use engine_service::{EngineServiceImpl, EngineServiceStore};
use runtime_common::backend::EngineBackend;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::auth::AuthConfig;
use crate::config::{Config, DatabaseBackend};
use crate::health::{RuntimeHealth, StorageHealth};
use crate::http::AppState;

const PROCESSOR_CHECKPOINT: &str = "processor-partition-0";
/// How often the PostgreSQL writer lock is checked, and how long one check may
/// take. Together they bound how long the runtime can keep writing after a
/// dropped session lets a second runtime take the lock.
const POSTGRES_WRITER_LOCK_CHECK_INTERVAL: Duration = Duration::from_secs(2);
const POSTGRES_WRITER_LOCK_CHECK_TIMEOUT: Duration = Duration::from_secs(2);
/// After a task fails, most often because writer authority was lost, the
/// runtime waits at most this long for its other tasks to stop and then
/// aborts them. An open event stream or server-sent-events client would
/// otherwise hold graceful shutdown open forever, keeping a runtime that may
/// no longer write alive and in the way of its supervisor's restart.
const FAILURE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// How long releasing writer authority may take on the way out. The release
/// runs on the same connection a lost lock was held on, which may be the one
/// that stopped answering; the lock is freed anyway when the process exits
/// and that connection closes.
const WRITER_RELEASE_TIMEOUT: Duration = Duration::from_secs(2);

/// The single-writer authority a runtime holds, and how it is kept alive.
enum WriterAuthority {
    /// SQLite or Turso lease, renewed on every heartbeat.
    LibsqlLease {
        lease: Arc<LibsqlWriterLease>,
        heartbeat: Duration,
    },
    /// PostgreSQL advisory lock on a dedicated session, checked on a timer
    /// because PostgreSQL releases it silently when that session drops.
    PostgresLock(Arc<PostgresWriterLock>),
}

trait OssStorage:
    EngineServiceStore
    + ProjectionCommitter
    + ProjectionStateReader
    + ProjectionCheckpointStore
    + DispatchOutboxStore
    + TerminalObligationStore
    + StorageHealth
    + Send
    + Sync
    + 'static
{
}

impl<T> OssStorage for T where
    T: EngineServiceStore
        + ProjectionCommitter
        + ProjectionStateReader
        + ProjectionCheckpointStore
        + DispatchOutboxStore
        + TerminalObligationStore
        + StorageHealth
        + Send
        + Sync
        + 'static
{
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    config.validate()?;
    let metrics = engine_service::telemetry::init("agnt5-runtime", &config.node_id);

    let result = run(config, metrics).await;
    engine_service::telemetry::shutdown();
    result
}

async fn run(
    config: Config,
    metrics: Option<engine_service::telemetry::EngineMetrics>,
) -> Result<()> {
    match config.database_backend {
        DatabaseBackend::Postgres => run_postgres(config, metrics).await,
        DatabaseBackend::Sqlite | DatabaseBackend::Turso => run_libsql(config, metrics).await,
    }
}

async fn run_postgres(
    config: Config,
    metrics: Option<engine_service::telemetry::EngineMetrics>,
) -> Result<()> {
    let database_url = config
        .database_url()
        .context("PostgreSQL database URL is required")?;
    let writer_lock = Arc::new(
        PostgresWriterLock::acquire(&database_url, config.namespace.clone())
            .await
            .context("acquire single-active PostgreSQL writer authority")?,
    );
    info!(namespace = %config.namespace, backend = "postgres", "acquired writer authority");

    let storage = match PostgresStorage::connect(
        &database_url,
        config.namespace.clone(),
        config.max_connections,
    )
    .await
    {
        Ok(storage) => Arc::new(storage),
        Err(error) => {
            let _ = writer_lock.release().await;
            return Err(anyhow::anyhow!(error)).context("initialize PostgreSQL runtime profile");
        }
    };
    if config.migrate_only {
        info!(backend = "postgres", "embedded migrations are current");
        writer_lock
            .release()
            .await
            .context("release PostgreSQL writer authority")?;
        return Ok(());
    }

    let journal = Arc::new(storage.journal(0));
    let result = run_runtime(
        config,
        storage,
        journal,
        metrics,
        Some(WriterAuthority::PostgresLock(writer_lock.clone())),
    )
    .await;
    if let Err(error) = release_within(WRITER_RELEASE_TIMEOUT, writer_lock.release()).await {
        if result.is_ok() {
            return Err(error).context("release PostgreSQL writer authority");
        }
        warn!(%error, "failed to release PostgreSQL writer authority after runtime error");
    }
    result
}

async fn run_libsql(
    config: Config,
    metrics: Option<engine_service::telemetry::EngineMetrics>,
) -> Result<()> {
    let database = match config.database_backend {
        DatabaseBackend::Sqlite => LibsqlDatabase::local(config.sqlite_path.clone()),
        DatabaseBackend::Turso => LibsqlDatabase::remote(
            config
                .database_url()
                .context("Turso database URL is required")?,
            config
                .database_auth_token()
                .context("Turso database auth token is required")?,
        ),
        DatabaseBackend::Postgres => unreachable!("PostgreSQL uses its dedicated profile"),
    };
    let backend = database.kind();
    let storage = Arc::new(
        LibsqlStorage::connect(database, config.namespace.clone())
            .await
            .with_context(|| format!("initialize {backend} runtime profile"))?,
    );
    let lease_duration = Duration::from_secs(config.writer_lease_secs);
    let lease_owner = format!("{}:{}", config.node_id, Uuid::new_v4());
    let writer_lease = Arc::new(
        LibsqlWriterLease::acquire(&storage, lease_owner, lease_duration)
            .await
            .with_context(|| format!("acquire single-active {backend} writer authority"))?,
    );
    info!(
        namespace = %config.namespace,
        backend,
        generation = writer_lease.generation(),
        "acquired writer authority"
    );
    if config.migrate_only {
        info!(backend, "embedded migrations are current");
        writer_lease
            .release()
            .await
            .with_context(|| format!("release {backend} writer authority"))?;
        return Ok(());
    }

    let journal = Arc::new(storage.journal(0));
    let heartbeat = Duration::from_secs(config.writer_heartbeat_secs);
    let result = run_runtime(
        config,
        storage,
        journal,
        metrics,
        Some(WriterAuthority::LibsqlLease {
            lease: writer_lease.clone(),
            heartbeat,
        }),
    )
    .await;
    if let Err(error) = release_within(WRITER_RELEASE_TIMEOUT, writer_lease.release()).await {
        if result.is_ok() {
            return Err(error).with_context(|| format!("release {backend} writer authority"));
        }
        warn!(%error, backend, "failed to release writer authority after runtime error");
    }
    result
}

async fn run_runtime<S, J>(
    config: Config,
    storage: Arc<S>,
    journal: Arc<J>,
    metrics: Option<engine_service::telemetry::EngineMetrics>,
    writer_authority: Option<WriterAuthority>,
) -> Result<()>
where
    S: OssStorage,
    J: Segment + 'static,
{
    let initial_tail = journal
        .tail_offset()
        .await
        .context("read startup journal tail")?;
    let cancel = CancellationToken::new();
    let cache = Arc::new(RecordCache::new(CacheConfig {
        max_bytes: config.cache_max_bytes,
        ..Default::default()
    }));
    let service_store: Arc<dyn EngineServiceStore> = storage.clone();
    let mut service = EngineServiceImpl::new(
        journal.clone(),
        cache,
        Some(service_store),
        config.node_id.clone(),
    )
    .with_poll_shutdown(cancel.clone())
    .with_execution_scope(config.project_id.clone(), config.deployment_id.clone())
    // One process has no peer nodes, so nothing legitimate sends the peer
    // runtime-authored header; honouring it would let any caller that knows
    // the signing key write records only the runtime may write.
    .without_peer_runtime_mark();
    if let Some(metrics) = metrics {
        service = service.with_metrics(metrics);
    }
    let service = Arc::new(service);
    let backend: Arc<dyn EngineBackend> = Arc::new(InProcessEngine::new(service.clone()));
    service.set_completion_append_backend(backend.clone());

    let storage_health: Arc<dyn StorageHealth> = storage.clone();
    let journal_health: Arc<dyn Segment> = journal.clone();
    let health = Arc::new(RuntimeHealth::new(storage_health, journal_health));
    let auth = AuthConfig::new(
        config.api_key.clone(),
        config.jwt_secret.clone(),
        config.project_id.clone(),
    );
    if !auth.enabled() {
        warn!(
            http_addr = %config.http_addr,
            "HTTP authentication is disabled; keep the runtime on a trusted interface"
        );
    }
    let http_app = http::router(AppState {
        engine: backend.clone(),
        health: health.clone(),
        auth,
        project_id: Arc::from(config.project_id.clone()),
        deployment_id: Arc::from(config.deployment_id.clone()),
        workspace_id: Arc::from(config.workspace_id.clone()),
        run_timeout: Duration::from_secs(config.run_timeout_secs),
    });

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(engine_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .context("build gRPC reflection service")?;
    let grpc_service = EngineServiceServer::from_arc(service);
    let execution_service =
        ExecutionEngineServiceServer::new(execution_service::OssExecutionService::new(
            backend,
            config.project_id.clone(),
            config.deployment_id.clone(),
        ));
    let http_listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("bind HTTP listener {}", config.http_addr))?;
    info!(
        http_addr = %config.http_addr,
        grpc_addr = %config.grpc_addr,
        namespace = %config.namespace,
        project_id = %config.project_id,
        deployment_id = %config.deployment_id,
        storage_backend = health.backend(),
        startup_tail = initial_tail.0,
        "AGNT5 OSS runtime starting"
    );

    let mut tasks = JoinSet::<Result<&'static str>>::new();

    tasks.spawn(run_processor(
        config.node_id.clone(),
        journal.clone(),
        storage.clone(),
        config.checkpoint_interval,
        initial_tail.0,
        health.clone(),
        cancel.child_token(),
    ));

    let timer_poller = TimerPoller::new(
        journal.clone(),
        storage.clone(),
        TimerPollerConfig::default(),
    );
    let timer_cancel = cancel.child_token();
    tasks.spawn(async move {
        timer_poller
            .run(timer_cancel)
            .await
            .map_err(anyhow::Error::from)?;
        Ok("timer poller")
    });

    let sweeper_cancel = cancel.child_token();
    let sweeper_journal = journal.clone();
    let sweeper_storage = storage.clone();
    let lease_max_attempts = config.lease_max_attempts;
    tasks.spawn(async move {
        lease_sweeper::run(
            sweeper_journal,
            sweeper_storage,
            lease_max_attempts,
            sweeper_cancel,
        )
        .await;
        Ok("lease sweeper")
    });

    match writer_authority {
        Some(WriterAuthority::LibsqlLease { lease, heartbeat }) => {
            let lease_cancel = cancel.child_token();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = lease_cancel.cancelled() => return Ok("writer lease heartbeat"),
                        _ = tokio::time::sleep(heartbeat) => {
                            lease
                                .renew()
                                .await
                                .context("writer authority renewal failed")?;
                        }
                    }
                }
            });
        }
        Some(WriterAuthority::PostgresLock(lock)) => {
            let lock_cancel = cancel.child_token();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = lock_cancel.cancelled() => return Ok("writer lock check"),
                        _ = tokio::time::sleep(POSTGRES_WRITER_LOCK_CHECK_INTERVAL) => {
                            tokio::time::timeout(POSTGRES_WRITER_LOCK_CHECK_TIMEOUT, lock.verify())
                                .await
                                .map_err(|_| anyhow::anyhow!(
                                    "PostgreSQL writer lock check timed out; stopping so a second runtime cannot write alongside this one"
                                ))?
                                .context("PostgreSQL writer lock lost; stopping so a second runtime cannot write alongside this one")?;
                        }
                    }
                }
            });
        }
        None => {}
    }

    let http_cancel = cancel.child_token();
    tasks.spawn(async move {
        axum::serve(http_listener, http_app)
            .with_graceful_shutdown(http_cancel.cancelled_owned())
            .await
            .context("HTTP server failed")?;
        Ok("HTTP server")
    });

    let grpc_cancel = cancel.child_token();
    let grpc_addr = config.grpc_addr;
    tasks.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(reflection)
            .add_service(grpc_service)
            .add_service(execution_service)
            .serve_with_shutdown(grpc_addr, grpc_cancel.cancelled_owned())
            .await
            .context("gRPC server failed")?;
        Ok("gRPC server")
    });

    let exit_reason = tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            info!("shutdown signal received");
            None
        }
        task = tasks.join_next() => {
            match task {
                Some(Ok(Ok(name))) => Some(anyhow::anyhow!("{name} exited before shutdown")),
                Some(Ok(Err(error))) => Some(error),
                Some(Err(error)) => Some(anyhow::anyhow!("runtime task panicked: {error}")),
                None => Some(anyhow::anyhow!("all runtime tasks exited unexpectedly")),
            }
        }
    };

    health.set_ready(false);
    cancel.cancel();
    // A requested shutdown drains gracefully. After a failure the drain is
    // bounded, so a lost writer authority cannot keep this process serving.
    let drain_bound = exit_reason.is_some().then_some(FAILURE_DRAIN_TIMEOUT);
    drain_tasks(&mut tasks, drain_bound).await;
    info!("AGNT5 OSS runtime stopped");
    match exit_reason {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Wait for every runtime task to stop. With a bound, tasks still running
/// when it passes are aborted.
async fn drain_tasks(tasks: &mut JoinSet<Result<&'static str>>, bound: Option<Duration>) {
    let deadline = bound.map(|bound| tokio::time::Instant::now() + bound);
    loop {
        let next = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(next) => next,
                Err(_) => {
                    warn!(
                        remaining = tasks.len(),
                        "runtime tasks did not stop in time after a failure; aborting them"
                    );
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return;
                }
            },
            None => tasks.join_next().await,
        };
        match next {
            None => return,
            Some(Ok(Ok(name))) => info!(task = name, "runtime task stopped"),
            Some(Ok(Err(error))) => error!(%error, "runtime task failed during shutdown"),
            Some(Err(error)) => error!(%error, "runtime task panicked during shutdown"),
        }
    }
}

/// Release writer authority, giving up after `timeout`.
async fn release_within<E>(
    timeout: Duration,
    release: impl std::future::Future<Output = std::result::Result<(), E>>,
) -> Result<()>
where
    E: std::fmt::Display,
{
    match tokio::time::timeout(timeout, release).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(anyhow::anyhow!("{error}")),
        Err(_) => Err(anyhow::anyhow!(
            "writer authority release did not finish within {timeout:?}"
        )),
    }
}

async fn run_processor<S, J>(
    node_id: String,
    journal: Arc<J>,
    storage: Arc<S>,
    checkpoint_interval: u64,
    startup_tail: u64,
    health: Arc<RuntimeHealth>,
    cancel: CancellationToken,
) -> Result<&'static str>
where
    S: OssStorage,
    J: Segment + 'static,
{
    health.set_processor_alive(true);
    let readiness_cancel = cancel.child_token();
    let readiness_health = health.clone();
    let readiness_storage = storage.clone();
    let readiness = tokio::spawn(async move {
        loop {
            let processed_cursor = readiness_storage
                .load_projection_checkpoint(PROCESSOR_CHECKPOINT)
                .await
                .ok()
                .flatten()
                .map(|offset| offset.0.saturating_add(1))
                .unwrap_or(0);
            if processed_cursor >= startup_tail {
                readiness_health.set_ready(true);
                info!(
                    startup_tail,
                    processed_cursor, "projection replay reached the startup journal tail"
                );
                return;
            }
            tokio::select! {
                _ = readiness_cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }
    });

    let journal_port: Arc<dyn Segment> = journal;
    let committer: Arc<dyn ProjectionCommitter> = storage.clone();
    let exit = run_single_partition_processor(
        node_id,
        journal_port,
        storage,
        committer,
        checkpoint_interval,
        cancel.clone(),
        None,
        None,
    )
    .await
    .context("partition processor failed to start")?;
    readiness.abort();
    health.set_processor_alive(false);
    match exit {
        PartitionProcessorExit::Cancelled if cancel.is_cancelled() => Ok("partition processor"),
        PartitionProcessorExit::Failed(error) => {
            Err(anyhow::anyhow!("partition processor failed: {error}"))
        }
        other => Err(anyhow::anyhow!(
            "partition processor exited unexpectedly: {other:?}"
        )),
    }
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C")?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.context("listen for Ctrl-C")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The OSS runtime publishes its proto descriptors through gRPC
    /// reflection, and it builds from the public protos alone. This checks
    /// that the hosted cluster protocols are compiled in exactly when
    /// `engine-proto/managed` is on. A workspace-wide build turns that feature
    /// on; the OSS-only build in Runtime CI, which also checks with
    /// `cargo tree` that the feature stays off, is where they must be absent.
    #[test]
    fn hosted_protos_are_compiled_only_with_the_managed_feature() {
        let descriptors = engine_proto::FILE_DESCRIPTOR_SET;
        let contains = |needle: &str| {
            descriptors
                .windows(needle.len())
                .any(|w| w == needle.as_bytes())
        };

        for public in ["api/v1/engine.proto", "api/v1/worker_routing.proto"] {
            assert!(contains(public), "{public} missing from the build");
        }
        for hosted in [
            "api/v1/coordinator_gossip.proto",
            "api/v1/runtime_dispatch.proto",
            "api/v1/replication.proto",
        ] {
            assert_eq!(
                contains(hosted),
                engine_proto::HOSTED_PROTOS,
                "{hosted} compiled in: {}, managed feature on: {}",
                contains(hosted),
                engine_proto::HOSTED_PROTOS,
            );
        }
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[tokio::test]
    async fn a_bounded_drain_aborts_a_task_that_never_stops() {
        let mut tasks = JoinSet::<Result<&'static str>>::new();
        tasks.spawn(async { Ok("finished") });
        // Stands in for a server still holding an open streaming client.
        tasks.spawn(std::future::pending());
        let started = std::time::Instant::now();
        drain_tasks(&mut tasks, Some(Duration::from_millis(100))).await;
        assert!(tasks.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "drain took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn an_unbounded_drain_waits_for_every_task() {
        let mut tasks = JoinSet::<Result<&'static str>>::new();
        tasks.spawn(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("slow but finishing")
        });
        tasks.spawn(async { Err(anyhow::anyhow!("failed")) });
        drain_tasks(&mut tasks, None).await;
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn a_release_on_an_unresponsive_connection_gives_up() {
        let started = std::time::Instant::now();
        let error = release_within(
            Duration::from_millis(100),
            std::future::pending::<std::result::Result<(), String>>(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("did not finish"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn a_release_error_is_reported() {
        let error = release_within(Duration::from_secs(1), async {
            Err::<(), _>("unlock failed")
        })
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "unlock failed");
    }
}
