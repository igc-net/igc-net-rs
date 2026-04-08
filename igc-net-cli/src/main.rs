//! igc-net — command-line node for the igc-net protocol.
//!
//! All subcommands accept `--data-dir` (default: `$HOME/.igc-net`).

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use igc_net::{
    Blake3Hex, FetchPolicy, FlatFileStore, FlightMetadata, IgcIrohNode, IndexerConfig,
    announce_topic_id, publish, run_indexer,
};

// ── CLI definition ────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "igc-net",
    about = "Open P2P protocol for publishing and exchanging IGC flight logs",
    version
)]
struct Cli {
    /// Root data directory (env: IGC_NET_DATA_DIR).
    #[arg(long, global = true, value_name = "PATH")]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Publish an IGC file to the network.
    Announce {
        /// Path to the IGC file to publish.
        file: PathBuf,

        /// Stay alive for N seconds after publishing so peers can fetch blobs.
        ///
        /// Defaults to 0 (exit immediately after publishing).  Set to a non-zero
        /// value in integration tests so indexers have time to download the blobs
        /// before the serving node closes.  Also prints a pre-publish pause of
        /// 2 s when set, giving bootstrapped indexers time to join the gossip
        /// swarm before the announcement is broadcast.
        #[arg(long, default_value = "0")]
        linger: u64,
    },

    /// Run an indexer node (Ctrl-C to stop).
    #[command(name = "runindex")]
    RunIndex {
        /// Fetch policy: metadata-only | eager | geo:<min_lat>,<max_lat>,<min_lon>,<max_lon>
        #[arg(long, default_value = "metadata-only")]
        policy: String,

        /// Comma-separated iroh node IDs (hex-encoded Ed25519 public keys)
        /// to bootstrap the gossip swarm from.
        #[arg(long, value_delimiter = ',')]
        bootstrap: Vec<String>,

        /// Pre-populate the address book for direct loopback connections.
        ///
        /// Format: `<node_id_hex>@<ip>:<port>`  e.g. `abc...@127.0.0.1:12345`
        ///
        /// Use together with `--bootstrap` in tests and private networks where
        /// relay-based peer discovery is unavailable or undesirable.
        #[arg(long)]
        peer_addr: Vec<String>,
    },

    /// Fetch a raw IGC blob by its BLAKE3 hash.
    Fetch {
        /// 64-char BLAKE3 hex hash of the IGC file.
        igc_hash: String,
        /// Output file path (default: <igc_hash>.igc).
        #[arg(long, short)]
        out: Option<PathBuf>,
    },

    /// Inspect an IGC file or metadata blob offline (no node required).
    Inspect {
        /// Path to an `.igc` file or `meta.json` metadata blob.
        file: PathBuf,
    },

    /// List all flights in the local index.
    List,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("igc_net=info".parse()?),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let data_dir = resolve_data_dir(cli.data_dir)?;

    match cli.command {
        Command::Announce { file, linger } => cmd_announce(data_dir, file, linger).await,
        Command::RunIndex { policy, bootstrap, peer_addr } => {
            cmd_runindex(data_dir, policy, bootstrap, peer_addr).await
        }
        Command::Fetch { igc_hash, out } => cmd_fetch(data_dir, igc_hash, out).await,
        Command::Inspect { file } => cmd_inspect(file),
        Command::List => cmd_list(data_dir),
    }
}

// ── Subcommand implementations ────────────────────────────────────────────────

/// `igc-net announce <file.igc> [--linger <secs>]`
async fn cmd_announce(data_dir: PathBuf, file: PathBuf, linger: u64) -> Result<()> {
    let filename = file
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    let bytes = std::fs::read(&file).with_context(|| format!("cannot read {}", file.display()))?;

    let node = IgcIrohNode::start(&data_dir).await?;

    // Print loopback addr early so test tooling can bootstrap indexers before
    // the announcement is broadcast.
    eprintln!("node_addr: {}", node.loopback_addr_str()?);

    // When --linger is set, pause before publishing so bootstrapped peers have
    // time to join the gossip swarm and receive the announcement.
    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    let result = publish(&node, bytes, filename.as_deref()).await?;

    println!("igc_hash:    {}", result.igc_hash);
    println!("meta_hash:   {}", result.meta_hash);
    println!("igc_ticket:  {}", result.igc_ticket);
    println!("meta_ticket: {}", result.meta_ticket);

    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(linger)).await;
    }

    node.close().await;
    Ok(())
}

/// `igc-net runindex [--policy ...] [--bootstrap <node_id>,...] [--peer-addr <addr>]`
async fn cmd_runindex(
    data_dir: PathBuf,
    policy_str: String,
    bootstrap_strs: Vec<String>,
    peer_addrs: Vec<String>,
) -> Result<()> {
    let policy = parse_policy(&policy_str)?;

    let bootstrap_keys: Vec<iroh::PublicKey> = bootstrap_strs
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().context(format!("invalid bootstrap node ID: {s}")))
        .collect::<Result<_>>()?;

    let node = IgcIrohNode::start(&data_dir).await?;

    // Pre-populate address book for direct loopback connections (no relay needed).
    for addr_str in &peer_addrs {
        let ep_addr = parse_endpoint_addr(addr_str)?;
        node.add_peer_addr(ep_addr);
    }

    eprintln!("Running indexer — node_id: {}", node.node_id());
    eprintln!("node_addr: {}", node.loopback_addr_str()?);
    eprintln!("Announce topic: {}", hex::encode(announce_topic_id()));
    if !bootstrap_keys.is_empty() {
        eprintln!("Bootstrap peers: {}", bootstrap_keys.len());
    }
    eprintln!("Press Ctrl-C to stop.");

    tokio::select! {
        res = run_indexer(&node, IndexerConfig::simple(policy, bootstrap_keys)) => {
            res?;
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("shutting down");
        }
    }
    node.close().await;
    Ok(())
}

/// `igc-net fetch <igc_hash> [--out <file>]`
async fn cmd_fetch(data_dir: PathBuf, igc_hash: String, out: Option<PathBuf>) -> Result<()> {
    anyhow::ensure!(igc_hash.len() == 64, "igc_hash must be 64 hex chars");

    let store = FlatFileStore::open(&data_dir);
    store.init().await?;

    if let Some(bytes) = store.get(&igc_hash).await? {
        let out_path = out.unwrap_or_else(|| PathBuf::from(format!("{igc_hash}.igc")));
        std::fs::write(&out_path, &bytes)
            .with_context(|| format!("cannot write {}", out_path.display()))?;
        println!("Written {} bytes to {}", bytes.len(), out_path.display());
    } else {
        anyhow::bail!(
            "Flight {igc_hash} not in local store.\n\
             Hint: run `igc-net runindex` to index remote flights."
        );
    }
    Ok(())
}

/// `igc-net inspect <file>`
fn cmd_inspect(file: PathBuf) -> Result<()> {
    let bytes = std::fs::read(&file).with_context(|| format!("cannot read {}", file.display()))?;

    let ext = file
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if ext == "json" {
        let meta: FlightMetadata =
            serde_json::from_slice(&bytes).context("not a valid metadata JSON blob")?;
        meta.validate().context("metadata validation failed")?;
        println!("{}", serde_json::to_string_pretty(&meta)?);
    } else {
        let igc_hash = Blake3Hex::from_hash(blake3::hash(&bytes));
        println!("igc_hash (BLAKE3): {igc_hash}");

        let meta = FlightMetadata::from_igc_bytes(&bytes, igc_hash, None, None);
        println!("{}", serde_json::to_string_pretty(&meta)?);
    }
    Ok(())
}

/// `igc-net list`
fn cmd_list(data_dir: PathBuf) -> Result<()> {
    let store = FlatFileStore::open(&data_dir);

    let mut count = 0usize;
    for record in store.iter_index()? {
        let r = record?;
        println!("{}\t{}\t{}", r.igc_hash, r.meta_hash, r.recorded_at);
        count += 1;
    }
    eprintln!("{count} flight(s) in index");
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn resolve_data_dir(opt: Option<PathBuf>) -> Result<PathBuf> {
    // Check env var first
    if let Some(p) = opt {
        return Ok(p);
    }
    if let Some(val) = std::env::var_os("IGC_NET_DATA_DIR") {
        return Ok(PathBuf::from(val));
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("XDG_DATA_HOME"))
        .map(PathBuf::from)
        .context("cannot determine home directory; set --data-dir or IGC_NET_DATA_DIR")?;
    Ok(home.join(".igc-net"))
}

fn parse_endpoint_addr(s: &str) -> Result<iroh::EndpointAddr> {
    let (node_id_str, socket_str) = s
        .split_once('@')
        .context("--peer-addr must be in format <node_id_hex>@<ip>:<port>")?;
    let node_id: iroh::PublicKey = node_id_str
        .parse()
        .context("invalid node_id in --peer-addr")?;
    let socket_addr: std::net::SocketAddr = socket_str
        .parse()
        .context("invalid socket address in --peer-addr")?;
    Ok(iroh::EndpointAddr::new(node_id).with_ip_addr(socket_addr))
}

fn parse_policy(s: &str) -> Result<FetchPolicy> {
    match s {
        "metadata-only" | "metadata_only" => Ok(FetchPolicy::MetadataOnly),
        "eager" => Ok(FetchPolicy::Eager),
        _ if s.starts_with("geo:") => {
            let parts: Vec<&str> = s[4..].split(',').collect();
            anyhow::ensure!(
                parts.len() == 4,
                "geo policy format: geo:<min_lat>,<max_lat>,<min_lon>,<max_lon>"
            );
            let p = |v: &str| v.trim().parse::<f64>().context("expected float");
            Ok(FetchPolicy::GeoFiltered {
                min_lat: p(parts[0])?,
                max_lat: p(parts[1])?,
                min_lon: p(parts[2])?,
                max_lon: p(parts[3])?,
            })
        }
        _ => anyhow::bail!("unknown policy {s:?}; use metadata-only, eager, or geo:<bbox>"),
    }
}
