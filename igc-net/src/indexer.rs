//! Gossip indexer: listen for flight announcements and fetch blobs.
//!
//! Reference indexer for the igc-net publish/announce flow.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use iroh_gossip::TopicId;
use iroh_gossip::api::Event;
use tokio::sync::{Mutex, Semaphore};

use crate::id::{Blake3Hex, NodeIdHex};
use crate::metadata::FlightMetadata;
use crate::node::IgcIrohNode;
use crate::store::{FlatFileStore, IndexRecord, IndexRecordSource};
use crate::topic::announce_topic_id;
use crate::util::canonical_utc_now;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("gossip: {0}")]
    Gossip(String),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("failed to download blob: {0}")]
    BlobDownload(String),
    #[error("failed to read downloaded blob: {0}")]
    BlobRead(String),
}

#[derive(Debug, thiserror::Error)]
enum AnnouncementError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("announcement exceeds 1024-byte limit: {0} bytes")]
    TooLarge(usize),
    #[error("invalid {ticket} ticket: {message}")]
    InvalidTicket {
        ticket: &'static str,
        message: String,
    },
    #[error("{ticket} ticket hash mismatch")]
    TicketHashMismatch { ticket: &'static str },
    #[error("{ticket} ticket node mismatch")]
    TicketNodeMismatch { ticket: &'static str },
    #[error("metadata JSON: {0}")]
    MetadataJson(serde_json::Error),
    #[error("metadata: {0}")]
    Metadata(#[from] crate::metadata::MetadataError),
    #[error("metadata igc_hash mismatch")]
    MetadataIgcHashMismatch,
    #[error("meta_hash mismatch")]
    MetaHashMismatch,
    #[error("igc_hash mismatch")]
    IgcHashMismatch,
}

#[derive(Debug)]
enum AnnouncementDisposition {
    Ignored(String),
    Indexed { fetched_igc: bool },
}

struct ValidatedAnnouncement {
    ann: Announcement,
    igc_ticket: iroh_blobs::ticket::BlobTicket,
    meta_ticket: iroh_blobs::ticket::BlobTicket,
}

#[derive(Clone)]
struct IndexerHandle {
    endpoint: iroh::Endpoint,
    fs_store: iroh_blobs::store::fs::FsStore,
    store: Arc<FlatFileStore>,
}

impl IndexerHandle {
    fn store(&self) -> &FlatFileStore {
        self.store.as_ref()
    }
}

// ── FetchPolicy ───────────────────────────────────────────────────────────────

/// Determines what the indexer stores after receiving an announcement.
#[derive(Debug, Clone)]
pub enum FetchPolicy {
    /// Store only the metadata JSON blob; fetch the raw IGC on explicit request.
    MetadataOnly,
    /// Fetch and store every announced raw IGC blob.
    Eager,
    /// Fetch only flights whose bbox overlaps this geographic region.
    GeoFiltered {
        min_lat: f64,
        max_lat: f64,
        min_lon: f64,
        max_lon: f64,
    },
}

// ── RateLimitConfig ───────────────────────────────────────────────────────────

/// Per-source flood protection for inbound gossip indexing.
///
/// Limits apply per unknown `publisher_node_id` on rolling windows.
/// Trusted nodes (listed in `trusted_node_ids`) bypass all limits.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum number of announcements accepted per publisher per rolling hour.
    pub blobs_per_hour: u32,
    /// Maximum total megabytes accepted per publisher per rolling 24 hours.
    pub mb_per_day: f64,
    /// Node IDs exempt from all rate limits.
    pub trusted_node_ids: HashSet<NodeIdHex>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            blobs_per_hour: 100,
            mb_per_day: 200.0,
            trusted_node_ids: HashSet::new(),
        }
    }
}

// ── IndexerConfig ─────────────────────────────────────────────────────────────

/// Configuration bundle for [`run_indexer`].
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Blob fetch policy applied to each accepted announcement.
    pub policy: FetchPolicy,
    /// Known bootstrap peers (iroh public keys) to seed the gossip swarm.
    ///
    /// Empty in production (relay-based discovery); populate in integration
    /// tests for loopback connections without relay infrastructure.
    pub bootstrap: Vec<iroh::PublicKey>,
    /// Optional per-source flood protection.
    ///
    /// `None` disables rate limiting (all announcements are accepted).
    pub rate_limit: Option<RateLimitConfig>,
}

impl IndexerConfig {
    /// Convenience constructor: policy + bootstrap, no rate limiting.
    pub fn simple(policy: FetchPolicy, bootstrap: Vec<iroh::PublicKey>) -> Self {
        Self {
            policy,
            bootstrap,
            rate_limit: None,
        }
    }
}

// ── Internal rate-limit state ─────────────────────────────────────────────────

struct PublisherStats {
    blobs_this_hour: u32,
    hour_window_start: Instant,
    bytes_today: u64,
    day_window_start: Instant,
}

impl Default for PublisherStats {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            blobs_this_hour: 0,
            hour_window_start: now,
            bytes_today: 0,
            day_window_start: now,
        }
    }
}

type RateLimitState = Arc<Mutex<HashMap<NodeIdHex, PublisherStats>>>;

const DEFAULT_MAX_CONCURRENT_ANNOUNCEMENTS: usize = 64;

// ── Wire format ───────────────────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct Announcement {
    igc_hash: Blake3Hex,
    meta_hash: Blake3Hex,
    #[allow(dead_code)]
    node_id: NodeIdHex,
    igc_ticket: String,
    meta_ticket: String,
}

// ── run_indexer() ─────────────────────────────────────────────────────────────

/// Subscribe to the announce gossip topic and process incoming announcements.
///
/// Runs until the node shuts down or an unrecoverable gossip error occurs.
/// Each announcement is processed in a spawned task so the gossip loop is
/// never blocked by network fetches.
///
/// Pass [`IndexerConfig::simple`] for tests or production without rate limiting.
/// Use a full [`IndexerConfig`] with `rate_limit` set to enable per-source
/// flood protection in reference node deployments.
pub async fn run_indexer(
    node: &IgcIrohNode,
    config: IndexerConfig,
) -> Result<(), IndexerError> {
    let topic = TopicId::from_bytes(announce_topic_id());
    let handle = Arc::new(IndexerHandle {
        endpoint: node.endpoint.clone(),
        fs_store: node.fs_store.clone(),
        store: Arc::clone(&node.store),
    });

    // When bootstrap peers are given (e.g., in integration tests), wait until
    // at least one peer has joined before starting the event loop.  With an
    // empty bootstrap list (production), return immediately and rely on peers
    // discovering us via the relay.
    let mut stream = if config.bootstrap.is_empty() {
        node.gossip
            .subscribe(topic, config.bootstrap)
            .await
            .map_err(|e| IndexerError::Gossip(e.to_string()))?
    } else {
        node.gossip
            .subscribe_and_join(topic, config.bootstrap)
            .await
            .map_err(|e| IndexerError::Gossip(e.to_string()))?
    };

    tracing::info!("indexer started — listening for flight announcements");

    let rl_state: Option<RateLimitState> = config
        .rate_limit
        .as_ref()
        .map(|_| Arc::new(Mutex::new(HashMap::new())));
    let permits = Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_ANNOUNCEMENTS));

    while let Some(item) = stream.next().await {
        let event = match item {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("gossip stream error: {e}");
                return Err(IndexerError::Gossip(e.to_string()));
            }
        };

        if let Event::Received(msg) = event {
            let payload = msg.content.clone();
            let handle = Arc::clone(&handle);
            let policy = config.policy.clone();
            let rl_cfg = config.rate_limit.clone();
            let rl_state = rl_state.clone();
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .map_err(|_| IndexerError::Gossip("announcement semaphore closed".to_string()))?;
            tokio::spawn(async move {
                let _permit = permit;
                match handle_announcement(&handle, &payload, &policy, rl_cfg.as_ref(), rl_state)
                    .await
                {
                    Ok(AnnouncementDisposition::Ignored(reason)) => {
                        tracing::debug!(%reason, "announcement ignored");
                    }
                    Ok(AnnouncementDisposition::Indexed { fetched_igc }) => {
                        tracing::debug!(fetched_igc, "announcement indexed");
                    }
                    Err(e) => {
                        tracing::warn!("announcement handling failed: {e}");
                    }
                }
            });
        }
    }

    Ok(())
}

// ── Internal announcement handling ───────────────────────────────────────────

async fn handle_announcement(
    node: &IndexerHandle,
    payload: &[u8],
    policy: &FetchPolicy,
    rl_cfg: Option<&RateLimitConfig>,
    rl_state: Option<RateLimitState>,
) -> Result<AnnouncementDisposition, IndexerError> {
    if let Err(e) = validate_payload_size(payload) {
        return Ok(AnnouncementDisposition::Ignored(
            e.to_string(),
        ));
    }

    // ── 1. Parse JSON ─────────────────────────────────────────────────────────
    let ann: Announcement = match serde_json::from_slice(payload) {
        Ok(a) => a,
        Err(e) => {
            return Ok(AnnouncementDisposition::Ignored(format!(
                "malformed announcement: {e}"
            )));
        }
    };

    // ── 2. Validate announcement invariants ───────────────────────────────────
    let ann = match validate_announcement(ann) {
        Ok(ann) => ann,
        Err(e) => return Ok(AnnouncementDisposition::Ignored(e.to_string())),
    };

    // ── 3. Rate limit ─────────────────────────────────────────────────────────
    if let (Some(cfg), Some(state)) = (rl_cfg, rl_state.as_ref())
        && !cfg.trusted_node_ids.contains(&ann.ann.node_id)
    {
        let mut map = state.lock().await;
        let stats = map.entry(ann.ann.node_id.clone()).or_default();
        let now = Instant::now();

        // Slide the hour window
        if now.duration_since(stats.hour_window_start) >= Duration::from_secs(3600) {
            stats.blobs_this_hour = 0;
            stats.hour_window_start = now;
        }
        // Slide the day window
        if now.duration_since(stats.day_window_start) >= Duration::from_secs(86400) {
            stats.bytes_today = 0;
            stats.day_window_start = now;
        }

        if stats.blobs_this_hour >= cfg.blobs_per_hour {
            tracing::debug!(
                node_id = %ann.ann.node_id,
                limit = cfg.blobs_per_hour,
                "rate limit exceeded (blobs/hour) — dropping announcement"
            );
            return Ok(AnnouncementDisposition::Ignored(
                "rate limit exceeded (blobs/hour)".to_string(),
            ));
        }
        let mb_today = stats.bytes_today as f64 / (1024.0 * 1024.0);
        if mb_today >= cfg.mb_per_day {
            tracing::debug!(
                node_id = %ann.ann.node_id,
                limit = cfg.mb_per_day,
                "rate limit exceeded (MB/day) — dropping announcement"
            );
            return Ok(AnnouncementDisposition::Ignored(
                "rate limit exceeded (MB/day)".to_string(),
            ));
        }

        stats.blobs_this_hour += 1;
    }

    // ── 4. Dedup ──────────────────────────────────────────────────────────────
    if node
        .store()
        .has_index_record(&ann.ann.meta_hash, &ann.ann.node_id)?
    {
        tracing::trace!(igc_hash = %ann.ann.igc_hash, "already indexed — skipping");
        return Ok(AnnouncementDisposition::Ignored(
            "already indexed".to_string(),
        ));
    }

    if node.store().has_meta_hash(&ann.ann.meta_hash)? {
        record_remote_announcement(node, &ann.ann).await?;
        tracing::debug!(igc_hash = %ann.ann.igc_hash, node_id = %ann.ann.node_id, "known metadata from new serving peer");
        return Ok(AnnouncementDisposition::Indexed { fetched_igc: false });
    }

    tracing::debug!(igc_hash = %ann.ann.igc_hash, "new announcement received");

    // ── 5. Fetch metadata blob ────────────────────────────────────────────────
    let meta_bytes = fetch_blob(node, &ann.meta_ticket).await?;

    // Verify hash
    let actual_meta_hash = Blake3Hex::from_hash(blake3::hash(&meta_bytes));
    if actual_meta_hash != ann.ann.meta_hash {
        tracing::warn!(
            expected = %ann.ann.meta_hash,
            actual   = %actual_meta_hash,
            "meta_hash mismatch — discarding"
        );
        return Ok(AnnouncementDisposition::Ignored(
            AnnouncementError::MetaHashMismatch.to_string(),
        ));
    }

    // ── 6. Validate metadata ──────────────────────────────────────────────────
    let meta: FlightMetadata = match serde_json::from_slice(&meta_bytes) {
        Ok(m) => m,
        Err(e) => {
            return Ok(AnnouncementDisposition::Ignored(
                AnnouncementError::MetadataJson(e).to_string(),
            ));
        }
    };
    if let Err(e) = meta.validate() {
        return Ok(AnnouncementDisposition::Ignored(e.to_string()));
    }
    if meta.igc_hash != ann.ann.igc_hash {
        tracing::warn!(
            expected = %ann.ann.igc_hash,
            actual = %meta.igc_hash,
            "metadata igc_hash mismatch — discarding"
        );
        return Ok(AnnouncementDisposition::Ignored(
            AnnouncementError::MetadataIgcHashMismatch.to_string(),
        ));
    }

    // ── 7. Store metadata blob ────────────────────────────────────────────────
    node.store().put(&meta_bytes).await?;
    // Track bytes accepted from this publisher for the MB/day rate limit.
    record_bytes_accepted(&rl_state, &ann.ann.node_id, meta_bytes.len() as u64).await;

    // ── 8. Apply fetch policy ─────────────────────────────────────────────────
    let should_fetch_igc = match policy {
        FetchPolicy::MetadataOnly => false,
        FetchPolicy::Eager => true,
        FetchPolicy::GeoFiltered {
            min_lat,
            max_lat,
            min_lon,
            max_lon,
        } => meta.bbox.as_ref().is_some_and(|bb| {
            bb.max_lat >= *min_lat
                && bb.min_lat <= *max_lat
                && bb.max_lon >= *min_lon
                && bb.min_lon <= *max_lon
        }),
    };

    if should_fetch_igc {
        let igc_bytes = fetch_blob(node, &ann.igc_ticket).await?;

        let actual_igc_hash = Blake3Hex::from_hash(blake3::hash(&igc_bytes));
        if actual_igc_hash != ann.ann.igc_hash {
            tracing::warn!(
                expected = %ann.ann.igc_hash,
                actual   = %actual_igc_hash,
                "igc_hash mismatch — discarding"
            );
            return Ok(AnnouncementDisposition::Ignored(
                AnnouncementError::IgcHashMismatch.to_string(),
            ));
        }

        node.store().put(&igc_bytes).await?;
        record_bytes_accepted(&rl_state, &ann.ann.node_id, igc_bytes.len() as u64).await;
        tracing::info!(igc_hash = %ann.ann.igc_hash, "fetched raw IGC blob");
    }

    // ── 9. Append source record ───────────────────────────────────────────────
    record_remote_announcement(node, &ann.ann).await?;

    tracing::info!(igc_hash = %ann.ann.igc_hash, "indexed flight");
    Ok(AnnouncementDisposition::Indexed {
        fetched_igc: should_fetch_igc,
    })
}

/// Accumulate bytes into the rate-limit state for a publisher.  No-op when
/// rate limiting is disabled (`rl_state` is `None`).
async fn record_bytes_accepted(rl_state: &Option<RateLimitState>, node_id: &NodeIdHex, bytes: u64) {
    if let Some(state) = rl_state {
        let mut map = state.lock().await;
        if let Some(stats) = map.get_mut(node_id) {
            stats.bytes_today += bytes;
        }
    }
}

fn validate_payload_size(payload: &[u8]) -> Result<(), AnnouncementError> {
    if payload.len() <= 1024 {
        Ok(())
    } else {
        Err(AnnouncementError::TooLarge(payload.len()))
    }
}

fn validate_announcement(ann: Announcement) -> Result<ValidatedAnnouncement, AnnouncementError> {
    let igc_ticket: iroh_blobs::ticket::BlobTicket =
        ann.igc_ticket
            .parse::<iroh_blobs::ticket::BlobTicket>()
            .map_err(|e| AnnouncementError::InvalidTicket {
                ticket: "igc",
                message: e.to_string(),
            })?;
    let meta_ticket: iroh_blobs::ticket::BlobTicket =
        ann.meta_ticket
            .parse::<iroh_blobs::ticket::BlobTicket>()
            .map_err(|e| AnnouncementError::InvalidTicket {
                ticket: "meta",
                message: e.to_string(),
            })?;

    if Blake3Hex::from_bytes(igc_ticket.hash().as_bytes()) != ann.igc_hash {
        return Err(AnnouncementError::TicketHashMismatch { ticket: "igc" });
    }
    if Blake3Hex::from_bytes(meta_ticket.hash().as_bytes()) != ann.meta_hash {
        return Err(AnnouncementError::TicketHashMismatch { ticket: "meta" });
    }
    if NodeIdHex::from_public_key(igc_ticket.addr().id) != ann.node_id {
        return Err(AnnouncementError::TicketNodeMismatch { ticket: "igc" });
    }
    if NodeIdHex::from_public_key(meta_ticket.addr().id) != ann.node_id {
        return Err(AnnouncementError::TicketNodeMismatch { ticket: "meta" });
    }

    Ok(ValidatedAnnouncement {
        ann,
        igc_ticket,
        meta_ticket,
    })
}

async fn record_remote_announcement(
    node: &IndexerHandle,
    ann: &Announcement,
) -> Result<(), crate::store::StoreError> {
    let _was_appended = node
        .store()
        .append_index_if_absent(&IndexRecord {
            source: IndexRecordSource::RemoteAnnouncement,
            igc_hash: ann.igc_hash.clone(),
            meta_hash: ann.meta_hash.clone(),
            node_id: ann.node_id.clone(),
            igc_ticket: ann.igc_ticket.clone(),
            meta_ticket: ann.meta_ticket.clone(),
            recorded_at: canonical_utc_now(),
        })
        .await?;
    Ok(())
}

/// Download a blob from the network using a serialised `BlobTicket`.
async fn fetch_blob(
    node: &IndexerHandle,
    ticket: &iroh_blobs::ticket::BlobTicket,
) -> Result<Vec<u8>, IndexerError> {
    let hash = ticket.hash();
    let peer_id = ticket.addr().id;

    // Download into our iroh-blobs store, using the peer as the provider.
    let downloader = node.fs_store.downloader(&node.endpoint);
    downloader
        .download(hash, vec![peer_id])
        .await
        .map_err(|e| IndexerError::BlobDownload(e.to_string()))?;

    // Read the bytes back from the local store.
    let bytes = node
        .fs_store
        .blobs()
        .get_bytes(hash)
        .await
        .map_err(|e| IndexerError::BlobRead(e.to_string()))?;

    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{Blake3Hex, NodeIdHex};

    #[test]
    fn validate_announcement_rejects_ticket_hash_mismatch() {
        let ann = Announcement {
            igc_hash: Blake3Hex::parse("a".repeat(64)).unwrap(),
            meta_hash: Blake3Hex::parse("b".repeat(64)).unwrap(),
            node_id: NodeIdHex::parse("c".repeat(64)).unwrap(),
            igc_ticket: "blob_ticket_placeholder".to_string(),
            meta_ticket: "blob_ticket_placeholder".to_string(),
        };
        assert!(validate_announcement(ann).is_err());
    }

    #[test]
    fn deserialize_announcement_rejects_short_igc_hash() {
        let json = format!(
            r#"{{"igc_hash":"abc","meta_hash":"{}","node_id":"{}","igc_ticket":"","meta_ticket":""}}"#,
            "b".repeat(64),
            "c".repeat(64)
        );
        let result: Result<Announcement, _> = serde_json::from_str(&json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_announcement_rejects_uppercase_meta_hash() {
        let json = format!(
            r#"{{"igc_hash":"{}","meta_hash":"{}","node_id":"{}","igc_ticket":"","meta_ticket":""}}"#,
            "a".repeat(64),
            "B".repeat(64),
            "c".repeat(64)
        );
        let result: Result<Announcement, _> = serde_json::from_str(&json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_announcement_rejects_short_node_id() {
        let json = format!(
            r#"{{"igc_hash":"{}","meta_hash":"{}","node_id":"too_short","igc_ticket":"","meta_ticket":""}}"#,
            "a".repeat(64),
            "b".repeat(64)
        );
        let result: Result<Announcement, _> = serde_json::from_str(&json);
        assert!(result.is_err());
    }

    #[test]
    fn rate_limit_config_default_values() {
        let cfg = RateLimitConfig::default();
        assert_eq!(cfg.blobs_per_hour, 100);
        assert!((cfg.mb_per_day - 200.0).abs() < f64::EPSILON);
        assert!(cfg.trusted_node_ids.is_empty());
    }

    #[test]
    fn malformed_json_is_silently_ignored() {
        // Verify that serde_json::from_slice fails gracefully for non-JSON.
        let result: Result<Announcement, _> = serde_json::from_slice(b"not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn announcement_missing_required_field_fails_parse() {
        // JSON with igc_hash but missing meta_hash, node_id, tickets.
        let json = r#"{"igc_hash":"aaaa"}"#;
        let result: Result<Announcement, _> = serde_json::from_slice(json.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn oversized_announcement_is_rejected() {
        assert!(matches!(
            validate_payload_size(&vec![0_u8; 1025]),
            Err(AnnouncementError::TooLarge(1025))
        ));
    }
}
