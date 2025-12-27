use std::net::SocketAddr;

use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use lance_fly::server::LanceFlyService;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "lance-fly")]
#[command(about = "A minimal Flight server backed by Lance datasets.", long_about = None)]
struct Args {
    /// Server bind address.
    #[arg(long, default_value = "127.0.0.1:50051")]
    bind: SocketAddr,

    /// Optional Lance dataset path to open on startup.
    #[arg(long)]
    dataset: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    if let Some(dataset_path) = args.dataset.as_deref() {
        info!(dataset_path, "opening Lance dataset");
        match lance::dataset::Dataset::open(dataset_path).await {
            Ok(dataset) => match dataset.latest_version_id().await {
                Ok(version) => {
                    info!(dataset_path, version, "opened Lance dataset");
                }
                Err(err) => {
                    warn!(dataset_path, error = %err, "opened Lance dataset but failed to fetch version id");
                }
            },
            Err(err) => {
                warn!(dataset_path, error = %err, "failed to open Lance dataset");
                return Err(err).context("open Lance dataset");
            }
        }
    }

    info!(bind = %args.bind, "starting Flight server");

    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(LanceFlyService::new(args.dataset)))
        .serve(args.bind)
        .await
        .context("serve Flight server")?;

    Ok(())
}
