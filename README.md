# `igc-net-rs`

Rust reference implementation of the `igc-net` protocol.

Public package identities:

- library package: `igc-net`
- Rust crate import: `igc_net`
- CLI package: `igc-net-cli`
- CLI binary: `igc-net`

## Status

Implemented:

- base publish/index/fetch protocol
- base metadata schema (`igc-net/metadata`, `schema_version = 1`)
- metadata validation for canonical timestamps, dates, bbox bounds, and
  coordinate ranges
- flat-file blob store and append-only source index
- typed identifier and typed error surfaces in the public library API
- loopback integration tests for metadata-only indexing, eager fetch,
  multi-publisher re-announcement, geo-filtered fetch, and deduplication
- crates.io publication for both `igc-net` and `igc-net-cli`

Not yet implemented:

- Part II analytics / `IGC_META_DOC`

## Repository Boundary

This repository is Rust-specific. It does not define the protocol.

The canonical protocol and schema definitions live in the separate `igc-net`
specification repository. This Rust repository tracks those documents and acts
as the reference implementation for the currently implemented subset.

## Current Workspace Layout

```text
igc-net-rs/
├── docs/
├── igc-net/
└── igc-net-cli/
```

## Compatibility Policy

- supported family:
  - `iroh = 0.97.x`
  - `iroh-gossip = 0.97.x`
  - `iroh-blobs = 0.99.x`
- minimum supported Rust version: `1.94`
- upstream minor-version bumps require:
  - changelog review
  - local test pass
  - doc updates if behavior or compatibility policy changes

## Documentation

- [docs/getting-started.md](docs/getting-started.md) — Rust consumer quickstart
- [docs/metadata-schema.md](docs/metadata-schema.md) — Rust mapping of the
  canonical metadata schema
- crates.io package pages:
  - [`igc-net`](https://crates.io/crates/igc-net)
  - [`igc-net-cli`](https://crates.io/crates/igc-net-cli)
- API documentation:
  - [`docs.rs/igc-net`](https://docs.rs/igc-net)
  - [`docs.rs/igc-net-cli`](https://docs.rs/igc-net-cli)

## Installation

Install the Rust CLI:

```bash
cargo install igc-net-cli
```

Add the library to your Rust project:

```toml
[dependencies]
igc-net = "0.1"
```

## Notes

- topic IDs are embedded protocol constants; they are not derived dynamically
  from arbitrary local strings
- metadata serialization is intended to match the protocol spec literally,
  including canonical timestamps and lowercase BLAKE3 hashes
- `IgcIrohNode::start` returns `Self`, and `publish` / `run_indexer` take
  `&IgcIrohNode`; shared ownership is a caller concern, not forced by the API
- Part II analytics is specified at the protocol level but not yet implemented
  here
