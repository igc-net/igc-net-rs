//! Publish a raw IGC file to the igc-net network.
//!
//! See the igc-net protocol specification for the announcement wire format.

use iroh_blobs::{BlobFormat, Hash};

use crate::artifact_announcement::{ArtifactAnnouncement, ArtifactAnnouncementError};
use crate::id::{Blake3Hex, NodeIdHex};
use crate::igc::g_record_present;
use crate::metadata::{FlightMetadata, MetadataError};
use crate::node::{IgcIrohNode, NodeError};
use crate::store::{IndexRecord, IndexRecordSource, PublicationMode};
use crate::util::canonical_utc_now;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("node error: {0}")]
    Node(#[from] NodeError),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("announcement: {0}")]
    Announcement(String),
    #[error("metadata: {0}")]
    Metadata(#[from] MetadataError),
    #[error("failed to add blob to iroh store: {0}")]
    BlobAdd(String),
    #[error("failed to broadcast announcement: {0}")]
    Broadcast(String),
}

impl From<ArtifactAnnouncementError> for PublishError {
    fn from(error: ArtifactAnnouncementError) -> Self {
        Self::Announcement(error.to_string())
    }
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
    /// True when the raw IGC bytes contain at least one G-record line.
    pub g_record_present: bool,
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
    /// True when the raw IGC bytes contain at least one G-record line.
    pub g_record_present: bool,
}

/// Result of publishing a private flight existence record.
#[derive(Debug, Clone)]
pub struct PrivatePublishResult {
    /// 64-char BLAKE3 hex of the raw IGC file.
    pub raw_igc_hash: Blake3Hex,
    /// Serialised `BlobTicket` for the restricted raw IGC artifact.
    pub raw_igc_ticket: String,
    /// True when the raw IGC bytes contain at least one G-record line.
    pub g_record_present: bool,
}

// ── publish() ─────────────────────────────────────────────────────────────────

/// Publish a raw IGC file to the igc-net gossip network.
///
/// # Steps
/// 1. BLAKE3(igc_bytes) → `igc_hash`
/// 2. Derive `g_record_present`
/// 3. Reuse or build the local metadata blob
/// 4. Store blobs locally and in iroh-blobs
/// 5. Broadcast a public artifact announcement
/// 6. Update local store records
pub async fn publish(
    node: &IgcIrohNode,
    igc_bytes: Vec<u8>,
    original_filename: Option<&str>,
) -> Result<PublishResult, PublishError> {
    // ── 1-2. Compute content hash and signature-presence flag ────────────────
    let (igc_hash, igc_hash_bytes, g_record_present) = raw_igc_identity(&igc_bytes);

    // ── 3. Reuse existing local metadata when possible ───────────────────────
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

    // ── 4. Store locally and register with iroh-blobs ────────────────────────
    node.store().put(&igc_bytes).await?;
    node.store().put(&meta_bytes).await?;

    let igc_ticket = import_and_ticket(node, igc_bytes.clone(), igc_hash_bytes).await?;
    let meta_ticket = import_and_ticket(node, meta_bytes.clone(), meta_hash_bytes).await?;

    // ── 5. Build and broadcast artifact announcement ─────────────────────────
    let announcement = ArtifactAnnouncement::signed(
        &node.node_secret_key(),
        igc_hash.clone(),
        PublicationMode::Public,
        vec![igc_ticket.clone()],
        node.node_id().clone(),
        None,
        Vec::new(),
        Some(g_record_present),
        canonical_utc_now(),
    )?;
    broadcast_artifact_announcement(node, &announcement).await?;

    tracing::info!(%igc_hash, %meta_hash, "published flight");

    // ── 6. Update local store records ────────────────────────────────────────
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
        g_record_present,
    })
}

/// Publish a protected flight to local blob storage and iroh-blobs.
///
/// The public announcement points to the sanitized artifact. Raw companion
/// tickets are returned to the local gRPC caller and omitted from gossip.
pub async fn publish_protected(
    node: &IgcIrohNode,
    igc_bytes: Vec<u8>,
) -> Result<ProtectedPublishResult, PublishError> {
    let (raw_igc_hash, raw_igc_hash_bytes, g_record_present) = raw_igc_identity(&igc_bytes);

    let sanitized_igc_bytes = sanitize_protected_igc(&igc_bytes);
    let protected_hash_blake3 = blake3::hash(&sanitized_igc_bytes);
    let protected_hash_bytes = *protected_hash_blake3.as_bytes();
    let protected_hash = Blake3Hex::from_hash(protected_hash_blake3);

    node.store().put(&igc_bytes).await?;
    node.store().put(&sanitized_igc_bytes).await?;

    let protected_ticket =
        import_and_ticket(node, sanitized_igc_bytes, protected_hash_bytes).await?;
    let raw_companion_ticket = import_and_ticket(node, igc_bytes, raw_igc_hash_bytes).await?;

    let announcement = ArtifactAnnouncement::signed(
        &node.node_secret_key(),
        raw_igc_hash.clone(),
        PublicationMode::Protected,
        vec![protected_ticket.clone()],
        node.node_id().clone(),
        Some(protected_hash.clone()),
        Vec::new(),
        Some(g_record_present),
        canonical_utc_now(),
    )?;
    broadcast_artifact_announcement(node, &announcement).await?;

    tracing::info!(%raw_igc_hash, %protected_hash, "published protected flight");

    Ok(ProtectedPublishResult {
        raw_igc_hash,
        protected_hash,
        protected_ticket,
        raw_companion_ticket,
        g_record_present,
    })
}

/// Publish a private flight to local blob storage and announce its restricted
/// raw artifact locator.
pub async fn publish_private(
    node: &IgcIrohNode,
    igc_bytes: Vec<u8>,
) -> Result<PrivatePublishResult, PublishError> {
    let (raw_igc_hash, raw_igc_hash_bytes, g_record_present) = raw_igc_identity(&igc_bytes);

    node.store().put(&igc_bytes).await?;
    let raw_igc_ticket = import_and_ticket(node, igc_bytes, raw_igc_hash_bytes).await?;

    let announcement = ArtifactAnnouncement::signed(
        &node.node_secret_key(),
        raw_igc_hash.clone(),
        PublicationMode::Private,
        vec![raw_igc_ticket.clone()],
        node.node_id().clone(),
        None,
        Vec::new(),
        Some(g_record_present),
        canonical_utc_now(),
    )?;
    broadcast_artifact_announcement(node, &announcement).await?;

    tracing::info!(%raw_igc_hash, "published private flight");

    Ok(PrivatePublishResult {
        raw_igc_hash,
        raw_igc_ticket,
        g_record_present,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn raw_igc_identity(igc_bytes: &[u8]) -> (Blake3Hex, [u8; 32], bool) {
    let hash = blake3::hash(igc_bytes);
    (
        Blake3Hex::from_hash(hash),
        *hash.as_bytes(),
        g_record_present(igc_bytes),
    )
}

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

async fn broadcast_artifact_announcement(
    node: &IgcIrohNode,
    ann: &ArtifactAnnouncement,
) -> Result<(), PublishError> {
    let announcement_bytes = ann.to_gossip_bytes()?;
    node.announce_sender()
        .broadcast(announcement_bytes.into())
        .await
        .map_err(|e| PublishError::Broadcast(e.to_string()))
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
    use crate::artifact_announcement::signing_payload;
    use crate::id::{Blake3Hex, NodeIdHex};

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

        let announcement = ArtifactAnnouncement::signed(
            &node_key,
            raw_igc_hash.clone(),
            PublicationMode::Protected,
            vec!["protected-ticket".to_string()],
            node_id.clone(),
            Some(protected_hash.clone()),
            vec!["raw-companion-ticket".to_string()],
            Some(true),
            "2026-05-01T09:14:00Z".to_string(),
        )
        .unwrap();

        assert_eq!(announcement.schema, "igc-net/announcement");
        assert_eq!(announcement.raw_igc_hash, raw_igc_hash);
        assert_eq!(announcement.publication_mode, PublicationMode::Protected);
        assert_eq!(announcement.tickets, vec!["protected-ticket"]);
        assert_eq!(announcement.protected_hash, Some(protected_hash));
        assert_eq!(announcement.companion_tickets, vec!["raw-companion-ticket"]);

        let bytes = announcement.to_gossip_bytes().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("igc_ticket").is_none());
        assert!(value.get("meta_ticket").is_none());
        assert!(value.get("raw_igc_hash").is_some());

        let signing_bytes = signing_payload(
            &announcement.record_id,
            &announcement.raw_igc_hash,
            &announcement.publication_mode,
            &announcement.tickets,
            &announcement.node_id,
            announcement.protected_hash.as_ref(),
            &announcement.companion_tickets,
            announcement.g_record_present,
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
