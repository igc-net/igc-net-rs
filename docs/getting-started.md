# Getting Started with `igc-net` in Rust

## Package and Import Names

Add the library package:

```toml
[dependencies]
igc-net = "0.3"
```

Import it as:

```rust
use igc_net::{FetchPolicy, IgcIrohNode, IndexerConfig, publish, run_indexer};
```

## Current Scope

Verified in the current branch:

- publish / index / fetch for raw IGC blobs
- metadata parsing and validation for `igc-net/metadata`
- flat-file local storage plus append-only index
- encrypted pilot key custody under `pilot-keys/`
- `pilot-auth-did-record` governance storage, selection, and pull-style replay
- authoritative Ed25519 `did:key` support
- `PilotProfileCredential` VC-JWT issuance and verification on the
  authoritative `did:key` path
- optional `did:web` parsing and live-resolution support for non-authoritative
  issuer / alias inspection

Not implemented for the first release:

- `identity-recovery`
- broader governance families beyond the identity path
- full serving-node v0.3 conformance
- offline-tolerant `did:web` verification

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

## Local Data and Identity Storage

The default CLI data directory is:

- `--data-dir <path>` if provided
- otherwise `IGC_NET_DATA_DIR` if set
- otherwise `$HOME/.igc-net` (with `XDG_DATA_HOME` used only as a fallback base
  path source when `HOME` is unavailable)

The flat-file store persists:

- blobs under BLAKE3-addressed paths
- `index.ndjson` for append-only source records
- `node.key` for the persisted Ed25519 node identity
- `pilot-keys/` for encrypted pilot identity custody:
  - `pilot_id.json`
  - `pilot_auth/current.json`
  - `pilot_auth/archive/`
- `governance/pilot-auth-did-records/` for observed identity governance history

## Security Model and Limits

Verified facts:

- pilot identity keys are distinct from `node.key`
- pilot key files are encrypted at rest with AES-256-GCM
- the sealing key is derived from `node.key` via HKDF with the
  `igc-net-pilot-keys-v1` label

Critical limits:

- this is still local filesystem custody, not an OS-backed secret store
- compromise of `node.key` also compromises the ability to decrypt stored pilot
  key material
- metadata blobs and the flat-file store are plaintext by design; only pilot
  key files are encrypted at rest
- cross-store pilot-auth rotation is rollback-compensated, not transactionally
  atomic

## `did:web` Semantics

Verified facts:

- `did:web` support is optional
- live verification uses HTTPS fetch and fails closed on network or document
  failure
- `did:web` remains non-authoritative for pilot binding

Implication:

- `PilotProfileCredential` remains authoritative only when issued by the
  governance-controlled active `pilot_auth_did`, which is currently a `did:key`
- a `did:web` alias or mirror never overrides `pilot_id` ->
  `pilot-auth-did-record` -> current `did:key`

## CLI

The reference CLI package is `igc-net-cli`, and the binary name is `igc-net`.

Current commands:

- `igc-net announce <file.igc> [--linger <secs>]`
- `igc-net runindex [--policy <p>] [--bootstrap <ids>] [--peer-addr <addr>]`
- `igc-net fetch <igc_hash> [--out <file>]`
- `igc-net inspect <file>`
- `igc-net list`
- `igc-net pilot-auth-status`
- `igc-net did-key-inspect <did:key>`
- `igc-net pilot-auth-issue-initial [--created-at <ts>]`
- `igc-net pilot-auth-rotate [--created-at <ts>]`
- `igc-net pilot-auth-record-inspect <file.json>`
- `igc-net pilot-auth-record-verify <file.json>`
- `igc-net pilot-profile-issue --jti <id> [--name <s>] [--country <CC>] [--audience <aud>] [--expires-in-seconds <n>] [--out <file>]`
- `igc-net pilot-profile-verify <jwt-or-file> [--expected-audience <aud>]`

Typical offline identity flow:

1. `igc-net pilot-auth-status`
2. `igc-net pilot-auth-issue-initial > initial-record.json`
3. `igc-net pilot-auth-record-verify initial-record.json`
4. `igc-net pilot-profile-issue --jti urn:uuid:... --name "Alice Example" --country NO --out profile.jwt`
5. `igc-net pilot-profile-verify profile.jwt`
6. `igc-net pilot-auth-rotate > rotated-record.json`
7. `igc-net pilot-profile-verify profile.jwt`

Step 7 should report the old VC as invalid because the active `pilot_auth_did`
changed.

## Release Regression Target

Use the focused identity regression target:

```bash
make identity-regression
```

This runs:

- `cargo test -p igc-net --lib`
- `cargo test -p igc-net --lib --no-default-features`
- `cargo test -p igc-net-cli`

## Conformance Boundary

This release is a partial v0.3 identity implementation.

It does not claim:

- full v0.3 conformance
- support for `identity-recovery`
- production-grade secret-store integration
- offline-verifiable `did:web`

## Canonical Protocol Docs

The Rust crate follows the `igc-net` protocol specification. The spec repo is
the canonical source of truth for:

- topic derivation strings
- governance and identity semantics
- metadata schema identifiers
- wire-format semantics
- threat model and conformance statements
