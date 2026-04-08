# Getting Started with `igc-net` in Rust

## Package and Import Names

Add the library package:

```toml
[dependencies]
igc-net = "0.1"
```

Import it as:

```rust
use igc_net::{FetchPolicy, IgcIrohNode, IndexerConfig, publish, run_indexer};
```

## Current Scope

Implemented today:

- base `igc-net` publish/index/fetch flow
- base metadata schema (`igc-net/metadata`, `schema_version = 1`)
- flat-file local store

Not implemented today:

- Part II analytics / `IGC_META_DOC`

## Start a Node

```rust
use igc_net::{IgcIrohNode, NodeError};

#[tokio::main]
async fn main() -> Result<(), NodeError> {
    let node = IgcIrohNode::start("./data").await?;
    println!("node_id={}", node.node_id());
    node.close().await;
    Ok(())
}
```

## Publish an IGC File

```rust
use igc_net::{IgcIrohNode, NodeError, PublishError, publish};

#[derive(Debug)]
enum AppError {
    Io(std::io::Error),
    Node(NodeError),
    Publish(PublishError),
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<NodeError> for AppError {
    fn from(err: NodeError) -> Self {
        Self::Node(err)
    }
}

impl From<PublishError> for AppError {
    fn from(err: PublishError) -> Self {
        Self::Publish(err)
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let node = IgcIrohNode::start("./data").await?;
    let bytes = std::fs::read("flight.igc")?;
    let result = publish(&node, bytes, Some("flight.igc")).await?;

    println!("igc_hash={}", result.igc_hash);
    println!("meta_hash={}", result.meta_hash);
    node.close().await;
    Ok(())
}
```

## Run an Indexer

```rust
use igc_net::{FetchPolicy, IgcIrohNode, IndexerConfig, IndexerError, NodeError, run_indexer};

#[derive(Debug)]
enum AppError {
    Node(NodeError),
    Indexer(IndexerError),
}

impl From<NodeError> for AppError {
    fn from(err: NodeError) -> Self {
        Self::Node(err)
    }
}

impl From<IndexerError> for AppError {
    fn from(err: IndexerError) -> Self {
        Self::Indexer(err)
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let node = IgcIrohNode::start("./data").await?;
    let cfg = IndexerConfig::simple(FetchPolicy::MetadataOnly, vec![]);
    run_indexer(&node, cfg).await?;
    node.close().await;
    Ok(())
}
```

Available fetch policies:

- `FetchPolicy::MetadataOnly`
- `FetchPolicy::Eager`
- `FetchPolicy::GeoFiltered { ... }`

## Local Data

The default CLI data directory is:

- `--data-dir <path>` if provided
- otherwise `IGC_NET_DATA_DIR` if set
- otherwise `$HOME/.igc-net` (with `XDG_DATA_HOME` used as a fallback source
  for the base directory when `HOME` is unavailable)

The flat-file store persists:

- blobs under BLAKE3-addressed paths
- `index.ndjson` for append-only source records
- `node.key` for the persisted Ed25519 node identity

## CLI

The reference CLI package is `igc-net-cli`, and the binary name is `igc-net`.

Commands:

- `igc-net announce <file.igc> [--linger <secs>]`
- `igc-net runindex [--policy <p>] [--bootstrap <ids>] [--peer-addr <addr>]`
- `igc-net fetch <igc_hash> [--out <file>]`
- `igc-net inspect <file>`
- `igc-net list`

## Canonical Protocol Docs

The Rust crate follows the `igc-net` protocol specification. The spec repo is
the canonical source of truth for:

- topic derivation strings
- metadata schema identifiers
- wire-format semantics
- analytics extension semantics
