use std::{net::SocketAddr, sync::Arc};

use agnt5_coordinator::{CheckpointService, Coordinator};
use agnt5_core::RuntimeIdentity;
use agnt5_postgres::{PostgresConfig, PostgresMaterializedStore, PostgresSegment, RuntimeLock};
use agnt5_processor::Processor;
use agnt5_proto::api::v1::engine_service_server::EngineServiceServer;
use agnt5_proto::api::v1::execution_engine_service_server::ExecutionEngineServiceServer;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let project_id = std::env::var("AGNT5_PROJECT_ID").unwrap_or_else(|_| "default".into());
    let identity = RuntimeIdentity::new(project_id)?;
    let database_url =
        std::env::var("AGNT5_DATABASE_URL").map_err(|_| "AGNT5_DATABASE_URL is required")?;
    let http_addr: SocketAddr = env_addr("AGNT5_HTTP_LISTEN", "0.0.0.0:34181")?;
    let grpc_addr: SocketAddr = env_addr("AGNT5_GRPC_LISTEN", "0.0.0.0:34180")?;

    let pool = agnt5_postgres::connect(&PostgresConfig::new(database_url.clone())).await?;
    agnt5_postgres::migrate(&pool).await?;
    let runtime_lock = RuntimeLock::acquire(&database_url).await?;
    let segment = Arc::new(PostgresSegment::open(pool.clone(), 0).await?);
    let store = Arc::new(PostgresMaterializedStore::new(pool));

    let processor = Processor::new(Arc::clone(&segment), Arc::clone(&store));
    let processor_task = tokio::spawn(async move {
        if let Err(error) = processor.run().await {
            error!(%error, "processor stopped");
        }
    });

    let http_router = agnt5_gateway::router(
        identity.project_id.clone(),
        Arc::clone(&segment),
        Arc::clone(&store),
    );
    let coordinator = Coordinator::new(identity.project_id.clone(), segment, store);
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;

    info!(version = env!("CARGO_PKG_VERSION"), project_id = %identity.project_id, %http_addr, %grpc_addr, "runtime started");
    let http = axum::serve(http_listener, http_router);
    let grpc = tonic::transport::Server::builder()
        .add_service(EngineServiceServer::new(coordinator))
        .add_service(ExecutionEngineServiceServer::new(CheckpointService))
        .serve(grpc_addr);

    tokio::select! {
        result = http => result?,
        result = grpc => result?,
        _ = tokio::signal::ctrl_c() => info!("shutdown requested"),
    }

    processor_task.abort();
    runtime_lock.release().await?;
    Ok(())
}

fn env_addr(name: &str, default: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    Ok(std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse()?)
}
