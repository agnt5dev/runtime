use agnt5_core::RuntimeIdentity;
use agnt5_postgres::{PostgresConfig, RuntimeLock};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project_id = std::env::var("AGNT5_PROJECT_ID").unwrap_or_else(|_| "default".into());
    let identity = RuntimeIdentity::new(project_id)?;
    let database_url =
        std::env::var("AGNT5_DATABASE_URL").map_err(|_| "AGNT5_DATABASE_URL is required")?;
    let config = PostgresConfig::new(database_url.clone());

    let pool = agnt5_postgres::connect(&config).await?;
    agnt5_postgres::migrate(&pool).await?;
    let runtime_lock = RuntimeLock::acquire(&database_url).await?;

    println!(
        "agnt5-runtime {} owns project {}",
        env!("CARGO_PKG_VERSION"),
        identity.project_id
    );
    tokio::signal::ctrl_c().await?;
    runtime_lock.release().await?;
    Ok(())
}
