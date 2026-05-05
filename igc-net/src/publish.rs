//! Publish a raw IGC file to the igc-net network.
//!
//! See the igc-net protocol specification for the announcement wire format.

use iroh_blobs::{BlobFormat, Hash};
use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, NodeIdHex};
use crate::metadata::{FlightMetadata, MetadataError};
use crate::node::{IgcIrohNode, NodeError};
use crate::store::{IndexRecord, IndexRecordSource, PublicationMode};
use crate::util::canonical_utc_now;

const ARTIFACT_ANNOUNCEMENT_SCHEMA: &str = "igc-net/announcement";
const ARTIFACT_ANNOUNCEMENT_VERSION: u8 = 1;

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

/// Result of publishing a protected flight.
#[derive(Debug, Clone)]
pub struct ProtectedPublishResult {
    /// 64-char BLAKE3 hex of the raw IGC file.
    pub raw_igc_hash: Blake3Hex,
    /// 64-char BLAKE3 hex of the sanitized public IGC artifact.
    pub protected_hash: Blake3Hex,
    /// Serialised `BlobTicket` for the sanitized public artifact.
    pub protected_ticket: String,
    /// Serialised `BlobTicket` for the raw companion artifact.
    pub raw_companion_ticket: String,
}

/// Result of publishing a private flight existence record.
#[derive(Debug, Clone)]
pub struct PrivatePublishResult {
    /// 64-char BLAKE3 hex of the raw IGC file.
    pub raw_igc_hash: Blake3Hex,
    /// Serialised `BlobTicket` for the restricted raw IGC artifact.
    pub raw_igc_ticket: String,
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

#[derive(Debug, Serialize, Deserialize)]
struct ArtifactAnnouncement {
    schema: String,
    schema_version: u8,
    record_id: Blake3Hex,
    raw_igc_hash: Blake3Hex,
    publication_mode: PublicationMode,
    tickets: Vec<String>,
    node_id: NodeIdHex,
    #[serde(skip_serializing_if = "Option::is_none")]
    protected_hash: Option<Blake3Hex>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    companion_tickets: Vec<String>,
    signature: String,
    created_at: String,
}

#[derive(Serialize)]
struct ArtifactAnnouncementIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    raw_igc_hash: &'a Blake3Hex,
    publication_mode: &'a PublicationMode,
    tickets: &'a [String],
    node_id: &'a NodeIdHex,
    protected_hash: Option<&'a Blake3Hex>,
    companion_tickets: &'a [String],
    created_at: &'a str,
}

#[derive(Serialize)]
struct ArtifactAnnouncementSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    publication_mode: &'a PublicationMode,
    tickets: &'a [String],
    node_id: &'a NodeIdHex,
    protected_hash: Option<&'a Blake3Hex>,
    companion_tickets: &'a [String],
    created_at: &'a str,
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

/// Publish a protected flight to local blob storage and iroh-blobs.
///
/// This deliberately does not use the legacy public announcement format:
/// legacy announcements identify `igc_ticket` by `raw_igc_hash`, which would
/// announce the raw companion as a public artifact. It broadcasts the v0.3
/// mode-aware announcement where `tickets` point to the sanitized artifact.
/// Raw companion tickets are returned to the local gRPC caller; they are omitted
/// from gossip until the announcement size budget is revised.
pub async fn publish_protected(
    node: &IgcIrohNode,
    igc_bytes: Vec<u8>,
) -> Result<ProtectedPublishResult, PublishError> {
    let raw_igc_hash_blake3 = blake3::hash(&igc_bytes);
    let raw_igc_hash_bytes = *raw_igc_hash_blake3.as_bytes();
    let raw_igc_hash = Blake3Hex::from_hash(raw_igc_hash_blake3);

    let sanitized_igc_bytes = sanitize_protected_igc(&igc_bytes);
    let protected_hash_blake3 = blake3::hash(&sanitized_igc_bytes);
    let protected_hash_bytes = *protected_hash_blake3.as_bytes();
    let protected_hash = Blake3Hex::from_hash(protected_hash_blake3);

    node.store().put(&igc_bytes).await?;
    node.store().put(&sanitized_igc_bytes).await?;

    let protected_ticket =
        import_and_ticket(node, sanitized_igc_bytes, protected_hash_bytes).await?;
    let raw_companion_ticket = import_and_ticket(node, igc_bytes, raw_igc_hash_bytes).await?;

    let announcement = build_artifact_announcement(
        &node.node_secret_key(),
        raw_igc_hash.clone(),
        PublicationMode::Protected,
        vec![protected_ticket.clone()],
        node.node_id().clone(),
        Some(protected_hash.clone()),
        Vec::new(),
        canonical_utc_now(),
    )?;
    let announcement_bytes = build_artifact_announcement_bytes(&announcement)?;
    node.announce_sender()
        .broadcast(announcement_bytes.into())
        .await
        .map_err(|e| PublishError::Broadcast(e.to_string()))?;

    tracing::info!(%raw_igc_hash, %protected_hash, "published protected flight");

    Ok(ProtectedPublishResult {
        raw_igc_hash,
        protected_hash,
        protected_ticket,
        raw_companion_ticket,
    })
}

/// Publish a private flight to local blob storage and announce its restricted
/// raw artifact locator with the v0.3 mode-aware announcement shape.
pub async fn publish_private(
    node: &IgcIrohNode,
    igc_bytes: Vec<u8>,
) -> Result<PrivatePublishResult, PublishError> {
    let raw_igc_hash_blake3 = blake3::hash(&igc_bytes);
    let raw_igc_hash_bytes = *raw_igc_hash_blake3.as_bytes();
    let raw_igc_hash = Blake3Hex::from_hash(raw_igc_hash_blake3);

    node.store().put(&igc_bytes).await?;
    let raw_igc_ticket = import_and_ticket(node, igc_bytes, raw_igc_hash_bytes).await?;

    let announcement = build_artifact_announcement(
        &node.node_secret_key(),
        raw_igc_hash.clone(),
        PublicationMode::Private,
        vec![raw_igc_ticket.clone()],
        node.node_id().clone(),
        None,
        Vec::new(),
        canonical_utc_now(),
    )?;
    let announcement_bytes = build_artifact_announcement_bytes(&announcement)?;
    node.announce_sender()
        .broadcast(announcement_bytes.into())
        .await
        .map_err(|e| PublishError::Broadcast(e.to_string()))?;

    tracing::info!(%raw_igc_hash, "published private flight");

    Ok(PrivatePublishResult {
        raw_igc_hash,
        raw_igc_ticket,
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
async fn make_ticket(node: &IgcIrohNode, hash_bytes: [u8; 32]) -> Result<String, PublishError> {
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

fn build_artifact_announcement(
    node_secret_key: &iroh::SecretKey,
    raw_igc_hash: Blake3Hex,
    publication_mode: PublicationMode,
    tickets: Vec<String>,
    node_id: NodeIdHex,
    protected_hash: Option<Blake3Hex>,
    companion_tickets: Vec<String>,
    created_at: String,
) -> Result<ArtifactAnnouncement, PublishError> {
    let record_id = derive_artifact_announcement_record_id(
        &raw_igc_hash,
        &publication_mode,
        &tickets,
        &node_id,
        protected_hash.as_ref(),
        &companion_tickets,
        &created_at,
    )?;
    let signing_bytes = artifact_announcement_signing_payload(
        &record_id,
        &raw_igc_hash,
        &publication_mode,
        &tickets,
        &node_id,
        protected_hash.as_ref(),
        &companion_tickets,
        &created_at,
    )?;
    let signature = hex::encode(node_secret_key.sign(&signing_bytes).to_bytes());

    Ok(ArtifactAnnouncement {
        schema: ARTIFACT_ANNOUNCEMENT_SCHEMA.to_string(),
        schema_version: ARTIFACT_ANNOUNCEMENT_VERSION,
        record_id,
        raw_igc_hash,
        publication_mode,
        tickets,
        node_id,
        protected_hash,
        companion_tickets,
        signature,
        created_at,
    })
}

fn build_artifact_announcement_bytes(ann: &ArtifactAnnouncement) -> Result<Vec<u8>, PublishError> {
    let json = serde_json::to_vec(ann)?;
    if json.len() > 1024 {
        return Err(PublishError::AnnouncementTooLarge(json.len()));
    }
    Ok(json)
}

fn derive_artifact_announcement_record_id(
    raw_igc_hash: &Blake3Hex,
    publication_mode: &PublicationMode,
    tickets: &[String],
    node_id: &NodeIdHex,
    protected_hash: Option<&Blake3Hex>,
    companion_tickets: &[String],
    created_at: &str,
) -> Result<Blake3Hex, PublishError> {
    let payload = ArtifactAnnouncementIdPayload {
        schema: ARTIFACT_ANNOUNCEMENT_SCHEMA,
        schema_version: ARTIFACT_ANNOUNCEMENT_VERSION,
        raw_igc_hash,
        publication_mode,
        tickets,
        node_id,
        protected_hash,
        companion_tickets,
        created_at,
    };
    let bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&bytes)))
}

fn artifact_announcement_signing_payload(
    record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    publication_mode: &PublicationMode,
    tickets: &[String],
    node_id: &NodeIdHex,
    protected_hash: Option<&Blake3Hex>,
    companion_tickets: &[String],
    created_at: &str,
) -> Result<Vec<u8>, PublishError> {
    let payload = ArtifactAnnouncementSigningPayload {
        schema: ARTIFACT_ANNOUNCEMENT_SCHEMA,
        schema_version: ARTIFACT_ANNOUNCEMENT_VERSION,
        record_id,
        raw_igc_hash,
        publication_mode,
        tickets,
        node_id,
        protected_hash,
        companion_tickets,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
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

/// Apply the normative protected-mode IGC sanitization rewrite table.
pub fn sanitize_protected_igc(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let mut start = 0usize;

    while start < input.len() {
        let mut end = start;
        while end < input.len() && input[end] != b'\n' {
            end += 1;
        }
        if end < input.len() {
            end += 1;
        }
        rewrite_igc_line(&input[start..end], &mut output);
        start = end;
    }

    output
}

fn rewrite_igc_line(line: &[u8], output: &mut Vec<u8>) {
    let (body, ending) = match line.strip_suffix(b"\r\n") {
        Some(body) => (body, &b"\r\n"[..]),
        None => match line.strip_suffix(b"\n") {
            Some(body) => (body, &b"\n"[..]),
            None => (line, &b""[..]),
        },
    };

    if let Some(prefix) = protected_rewrite_prefix(body) {
        output.extend_from_slice(prefix);
        output.extend_from_slice(b":REDACTED");
        output.extend_from_slice(ending);
    } else {
        output.extend_from_slice(line);
    }
}

fn protected_rewrite_prefix(line_body: &[u8]) -> Option<&'static [u8]> {
    const PREFIXES: [&[u8]; 7] = [
        b"HFPLT",
        b"HFCID",
        b"HFGID",
        b"HFRFW",
        b"HFFTYFRTYPE",
        b"HOPLT",
        b"HOCID",
    ];

    PREFIXES
        .into_iter()
        .find(|prefix| line_body.starts_with(prefix))
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

    #[test]
    fn sanitize_protected_igc_rewrites_only_listed_headers_and_preserves_endings() {
        let input = b"HFPLTPILOT:Alice\r\nHFCIDCOMPETITION:ABC\nHFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLXXXHFPLTKEEP\r\nHOCIDXYZ";

        let sanitized = sanitize_protected_igc(input);

        assert_eq!(
            sanitized,
            b"HFPLT:REDACTED\r\nHFCID:REDACTED\nHFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLXXXHFPLTKEEP\r\nHOCID:REDACTED"
        );
    }

    #[test]
    fn sanitize_protected_igc_preserves_line_count() {
        let input = b"HFPLTPILOT:Alice\nHFGIDGLIDER:XYZ\nB1300004730000N00837000EA0030003000\n";
        let sanitized = sanitize_protected_igc(input);

        assert_eq!(
            input.iter().filter(|byte| **byte == b'\n').count(),
            sanitized.iter().filter(|byte| **byte == b'\n').count()
        );
    }

    #[test]
    fn protected_artifact_announcement_uses_mode_aware_shape_and_node_signature() {
        let node_key = iroh::SecretKey::from_bytes(&[7; 32]);
        let raw_igc_hash = Blake3Hex::parse("a".repeat(64)).unwrap();
        let protected_hash = Blake3Hex::parse("b".repeat(64)).unwrap();
        let node_id = NodeIdHex::from_public_key(node_key.public());

        let announcement = build_artifact_announcement(
            &node_key,
            raw_igc_hash.clone(),
            PublicationMode::Protected,
            vec!["protected-ticket".to_string()],
            node_id.clone(),
            Some(protected_hash.clone()),
            vec!["raw-companion-ticket".to_string()],
            "2026-05-01T09:14:00Z".to_string(),
        )
        .unwrap();

        assert_eq!(announcement.schema, "igc-net/announcement");
        assert_eq!(announcement.raw_igc_hash, raw_igc_hash);
        assert_eq!(announcement.publication_mode, PublicationMode::Protected);
        assert_eq!(announcement.tickets, vec!["protected-ticket"]);
        assert_eq!(announcement.protected_hash, Some(protected_hash));
        assert_eq!(announcement.companion_tickets, vec!["raw-companion-ticket"]);

        let bytes = build_artifact_announcement_bytes(&announcement).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("igc_ticket").is_none());
        assert!(value.get("meta_ticket").is_none());
        assert!(value.get("raw_igc_hash").is_some());

        let signing_bytes = artifact_announcement_signing_payload(
            &announcement.record_id,
            &announcement.raw_igc_hash,
            &announcement.publication_mode,
            &announcement.tickets,
            &announcement.node_id,
            announcement.protected_hash.as_ref(),
            &announcement.companion_tickets,
            &announcement.created_at,
        )
        .unwrap();
        let signature_bytes: [u8; 64] = hex::decode(&announcement.signature)
            .unwrap()
            .try_into()
            .unwrap();
        let signature = iroh::Signature::from_bytes(&signature_bytes);
        node_key
            .public()
            .verify(&signing_bytes, &signature)
            .unwrap();
        assert_eq!(announcement.node_id, node_id);
    }
}
