//! Publish a raw IGC file to the igc-net network.
//!
//! See the igc-net protocol specification for the announcement wire format.

use iroh_blobs::{BlobFormat, Hash};
use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, NodeIdHex};
use crate::metadata::{FlightMetadata, MetadataError};
use crate::node::{IgcIrohNode, NodeError};
use crate::store::{IndexRecord, IndexRecordSource};
use crate::util::canonical_utc_now;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("node error: {0}")]
    Node(#[from] NodeError),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("announcement too large: {0} bytes (max 1024)")]
    AnnouncementTooLarge(usize),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("metadata: {0}")]
    Metadata(#[from] MetadataError),
    #[error("failed to add blob to iroh store: {0}")]
    BlobAdd(String),
    #[error("failed to broadcast announcement: {0}")]
    Broadcast(String),
}

// ── Result type ───────────────────────────────────────────────────────────────

/// Result of a successful publish.
#[derive(Debug, Clone)]
pub struct PublishResult {
    /// 64-char BLAKE3 hex of the raw IGC file.
    pub igc_hash: Blake3Hex,
    /// 64-char BLAKE3 hex of the metadata JSON blob.
    pub meta_hash: Blake3Hex,
    /// Serialised `BlobTicket` for the raw IGC file.
    pub igc_ticket: String,
    /// Serialised `BlobTicket` for the metadata blob.
    pub meta_ticket: String,
}

// ── Announcement wire format ──────────────────────────────────────────────────

/// JSON announcement sent over gossip (specs_igc.md §3.2).
#[derive(Debug, Serialize, Deserialize)]
struct Announcement {
    igc_hash: Blake3Hex,
    meta_hash: Blake3Hex,
    node_id: NodeIdHex,
    igc_ticket: String,
    meta_ticket: String,
}

// ── publish() ─────────────────────────────────────────────────────────────────

/// Publish a raw IGC file to the igc-net gossip network.
///
/// # Steps
/// 1. BLAKE3(igc_bytes) → `igc_hash`
/// 2. Reuse the latest locally-published metadata blob for this `igc_hash` if present
/// 3. Otherwise: `FlightMetadata::from_igc_bytes()` → metadata struct
/// 4. `metadata.to_blob_bytes()` → `meta_bytes`; BLAKE3(meta_bytes) → `meta_hash`
/// 5. `FlatFileStore::put()` both blobs
/// 6. Add both blobs to iroh-blobs → generate `BlobTicket`s
/// 7. Build and size-check the announcement JSON
/// 8. Broadcast on gossip `TOPIC_ID`
/// 9. `FlatFileStore::append_index()`
pub async fn publish(
    node: &IgcIrohNode,
    igc_bytes: Vec<u8>,
    original_filename: Option<&str>,
) -> Result<PublishResult, PublishError> {
    // ── 1. Compute igc_hash ───────────────────────────────────────────────────
    let igc_hash_blake3 = blake3::hash(&igc_bytes);
    let igc_hash_bytes = *igc_hash_blake3.as_bytes();
    let igc_hash = Blake3Hex::from_hash(igc_hash_blake3);

    // ── 2-4. Reuse existing local metadata when possible ─────────────────────
    let (meta_hash, meta_bytes) = match node
        .store()
        .latest_local_publish(&igc_hash, node.node_id())?
    {
        Some(existing) => match node.store().get(&existing.meta_hash).await? {
            Some(meta_bytes) => {
                tracing::debug!(%igc_hash, meta_hash = %existing.meta_hash, "reusing existing local metadata blob");
                (existing.meta_hash, meta_bytes)
            }
            None => build_metadata_blob(
                &igc_bytes,
                igc_hash.clone(),
                original_filename,
                node.node_id().clone(),
            )?,
        },
        None => build_metadata_blob(
            &igc_bytes,
            igc_hash.clone(),
            original_filename,
            node.node_id().clone(),
        )?,
    };
    let meta_hash_blake3 = blake3::hash(&meta_bytes);
    let meta_hash_bytes = *meta_hash_blake3.as_bytes();

    // ── 5. Store in FlatFileStore ─────────────────────────────────────────────
    node.store().put(&igc_bytes).await?;
    node.store().put(&meta_bytes).await?;

    // ── 6. Register with iroh-blobs and create tickets ────────────────────────
    let igc_ticket = import_and_ticket(node, igc_bytes.clone(), igc_hash_bytes).await?;
    let meta_ticket = import_and_ticket(node, meta_bytes.clone(), meta_hash_bytes).await?;

    // ── 7. Build announcement JSON (≤ 1024 bytes) ─────────────────────────────
    let announcement = Announcement {
        igc_hash: igc_hash.clone(),
        meta_hash: meta_hash.clone(),
        node_id: node.node_id().clone(),
        igc_ticket: igc_ticket.clone(),
        meta_ticket: meta_ticket.clone(),
    };
    let announcement_bytes = build_announcement(&announcement)?;

    // ── 8. Broadcast on gossip ────────────────────────────────────────────────
    // Reuse the node's persistent announce-topic sender rather than creating
    // a new subscription per publish call.
    node.announce_sender()
        .broadcast(announcement_bytes.into())
        .await
        .map_err(|e| PublishError::Broadcast(e.to_string()))?;

    tracing::info!(%igc_hash, %meta_hash, "published flight");

    // ── 9. Append to index ────────────────────────────────────────────────────
    let recorded_at = canonical_utc_now();
    node.store()
        .append_index_if_absent(&IndexRecord {
            source: IndexRecordSource::LocalPublish,
            igc_hash: igc_hash.clone(),
            meta_hash: meta_hash.clone(),
            node_id: node.node_id().clone(),
            igc_ticket: igc_ticket.clone(),
            meta_ticket: meta_ticket.clone(),
            recorded_at,
        })
        .await?;

    Ok(PublishResult {
        igc_hash,
        meta_hash,
        igc_ticket,
        meta_ticket,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Import bytes into iroh-blobs so they can be served to peers.
/// Returns a `BlobTicket` string.
async fn import_and_ticket(
    node: &IgcIrohNode,
    bytes: Vec<u8>,
    hash_bytes: [u8; 32],
) -> Result<String, PublishError> {
    // Add to iroh-blobs — it will compute the BLAKE3 hash internally and store.
    // We hold a temp_tag to keep the blob alive during this session.
    let _tag = node
        .fs_store
        .blobs()
        .add_bytes(bytes)
        .temp_tag()
        .await
        .map_err(|e| PublishError::BlobAdd(e.to_string()))?;

    make_ticket(node, hash_bytes).await
}

/// Create a `BlobTicket` string for a blob already in the iroh-blobs store.
async fn make_ticket(
    node: &IgcIrohNode,
    hash_bytes: [u8; 32],
) -> Result<String, PublishError> {
    let hash = Hash::from_bytes(hash_bytes);
    let addr = node.endpoint.addr();
    let ticket = iroh_blobs::ticket::BlobTicket::new(addr, hash, BlobFormat::Raw);
    Ok(ticket.to_string())
}

/// Serialise and size-check the announcement.
fn build_announcement(ann: &Announcement) -> Result<Vec<u8>, PublishError> {
    let json = serde_json::to_vec(ann)?;
    if json.len() > 1024 {
        return Err(PublishError::AnnouncementTooLarge(json.len()));
    }
    Ok(json)
}

/// Find the `meta_hash` for a known `igc_hash` from the local index.
fn build_metadata_blob(
    igc_bytes: &[u8],
    igc_hash: Blake3Hex,
    original_filename: Option<&str>,
    node_id: NodeIdHex,
) -> Result<(Blake3Hex, Vec<u8>), PublishError> {
    let meta =
        FlightMetadata::from_igc_bytes(igc_bytes, igc_hash, original_filename, Some(node_id));
    meta.validate()?;
    let meta_bytes = meta.to_blob_bytes()?;
    let meta_hash = Blake3Hex::from_hash(blake3::hash(&meta_bytes));
    Ok((meta_hash, meta_bytes))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{Blake3Hex, NodeIdHex};

    #[test]
    fn announcement_json_is_valid_and_small() {
        let ann = Announcement {
            igc_hash: Blake3Hex::parse("a".repeat(64)).unwrap(),
            meta_hash: Blake3Hex::parse("b".repeat(64)).unwrap(),
            node_id: NodeIdHex::parse("c".repeat(64)).unwrap(),
            igc_ticket: "igc_ticket_placeholder_string".to_string(),
            meta_ticket: "meta_ticket_placeholder_string".to_string(),
        };
        let bytes = build_announcement(&ann).unwrap();
        assert!(bytes.len() <= 1024, "announcement must be ≤ 1024 bytes");
        let _: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    }

    #[test]
    fn build_metadata_blob_produces_canonical_metadata() {
        let (meta_hash, meta_bytes) = build_metadata_blob(
            b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n",
            Blake3Hex::parse("a".repeat(64)).unwrap(),
            Some("test.igc"),
            NodeIdHex::parse("c".repeat(64)).unwrap(),
        )
        .unwrap();
        assert_eq!(meta_hash.len(), 64);
        let meta: FlightMetadata = serde_json::from_slice(&meta_bytes).unwrap();
        assert_eq!(meta.schema, "igc-net/metadata");
        assert!(meta.validate().is_ok());
    }
}
