//! igc-net gRPC service — minimal POC server.
//!
//! Exposes FetchArtifact (restricted + public), ProvisionPrivateAccessKey,
//! and RevokePrivateAccess over gRPC.  Bind address defaults to [::1]:50051.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use igc_net::{FlatFileStore, PrivateAccessKeyStore, SeqNumStore};

mod service;

pub mod proto {
    tonic::include_proto!("igc_net.v0");
}

use proto::igc_net_server::IgcNetServer;
use service::{IgcNetService, NodeContext};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "igc-net-grpc", about = "igc-net gRPC service (POC)")]
struct Cli {
    /// Root data directory.
    #[arg(long, default_value = "~/.igc-net")]
    data_dir: String,

    /// gRPC bind address.
    #[arg(long, default_value = "[::1]:50051")]
    grpc_addr: SocketAddr,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "igc_net_grpc=info,igc_net=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();
    let data_dir = expand_tilde(&cli.data_dir);

    // Load or generate the node secret key (reuses the same node.key as the CLI).
    let store = FlatFileStore::open(&data_dir);
    store.init().await.context("failed to initialise blob store")?;

    let node_secret_key = load_or_generate_node_key(&store)?;
    let node_id = node_secret_key.public().to_string();

    let ctx = Arc::new(NodeContext {
        node_id: node_id.clone(),
        node_secret_key,
        store,
        private_access_key_store: PrivateAccessKeyStore::for_data_dir(&data_dir),
        seq_num_store: SeqNumStore::for_data_dir(&data_dir),
    });

    let service = IgcNetService::new(ctx);

    info!(node_id = %node_id, addr = %cli.grpc_addr, "igc-net gRPC service starting");

    Server::builder()
        .add_service(IgcNetServer::new(service))
        .serve_with_shutdown(cli.grpc_addr, shutdown_signal())
        .await
        .context("gRPC server error")?;

    info!("igc-net gRPC service stopped");
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn load_or_generate_node_key(store: &FlatFileStore) -> Result<iroh::SecretKey> {
    if let Some(bytes) = store.load_key_bytes().context("failed to read node.key")? {
        return Ok(iroh::SecretKey::from_bytes(&bytes));
    }
    let key = iroh::SecretKey::generate(&mut rand::rng());
    store
        .save_key_bytes(&key.to_bytes())
        .context("failed to write node.key")?;
    Ok(key)
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl-C");
}
