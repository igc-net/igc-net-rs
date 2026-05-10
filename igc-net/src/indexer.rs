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

use crate::artifact_announcement::ArtifactAnnouncement;
use crate::id::{Blake3Hex, NodeIdHex};
use crate::igc::g_record_present;
use crate::node::IgcIrohNode;
use crate::store::{ArtifactRegistryRecord, FlatFileStore, PublicationMode};
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

#[derive(Debug)]
enum AnnouncementDisposition {
    Ignored(String),
    Indexed { fetched_igc: bool },
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
    /// Index announcements without fetching artifact bytes.
    IndexOnly,
    /// Fetch and store announced artifact bytes when public tickets are present.
    Eager,
    /// Fetch announced artifact bytes only when filter metadata is available
    /// and overlaps this region.
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
pub async fn run_indexer(node: &IgcIrohNode, config: IndexerConfig) -> Result<(), IndexerError> {
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
    let ann = match ArtifactAnnouncement::parse_and_validate(payload) {
        Ok(ann) => ann,
        Err(e) => return Ok(AnnouncementDisposition::Ignored(e.to_string())),
    };
    handle_artifact_announcement(node, ann, policy, rl_cfg, rl_state).await
}

async fn handle_artifact_announcement(
    node: &IndexerHandle,
    ann: crate::artifact_announcement::ValidatedArtifactAnnouncement,
    policy: &FetchPolicy,
    rl_cfg: Option<&RateLimitConfig>,
    rl_state: Option<RateLimitState>,
) -> Result<AnnouncementDisposition, IndexerError> {
    if let Some(reason) = apply_rate_limit(&ann.ann.node_id, rl_cfg, rl_state.as_ref()).await {
        return Ok(AnnouncementDisposition::Ignored(reason));
    }

    let mut fetched_public_artifact = false;
    let mut authoritative_g_record_present = None;
    match ann.ann.publication_mode {
        PublicationMode::Public if should_fetch_v03_public_artifact(policy) => {
            let Some(bytes) = fetch_and_store_announced_artifact(
                node,
                &ann.tickets[0],
                &ann.ann.raw_igc_hash,
                &rl_state,
                &ann.ann.node_id,
            )
            .await?
            else {
                return Ok(AnnouncementDisposition::Ignored(
                    "igc_hash mismatch".to_string(),
                ));
            };
            let computed = g_record_present(&bytes);
            if ann
                .ann
                .g_record_present
                .is_some_and(|announced| announced != computed)
            {
                tracing::warn!(
                    raw_igc_hash = %ann.ann.raw_igc_hash,
                    announced = ann.ann.g_record_present,
                    computed,
                    "announcement g_record_present mismatch; local computation wins"
                );
            }
            authoritative_g_record_present = Some(computed);
            fetched_public_artifact = true;
        }
        PublicationMode::Protected if should_fetch_v03_public_artifact(policy) => {
            let Some(_) = fetch_and_store_announced_artifact(
                node,
                &ann.tickets[0],
                ann.ann
                    .protected_hash
                    .as_ref()
                    .expect("protected hash was checked during validation"),
                &rl_state,
                &ann.ann.node_id,
            )
            .await?
            else {
                return Ok(AnnouncementDisposition::Ignored(
                    "igc_hash mismatch".to_string(),
                ));
            };
            fetched_public_artifact = true;
        }
        PublicationMode::Private | PublicationMode::Public | PublicationMode::Protected => {}
    }

    let mut record = ArtifactRegistryRecord {
        raw_igc_hash: ann.ann.raw_igc_hash.clone(),
        pilot_id: None,
        publication_mode: ann.ann.publication_mode.clone(),
        protected_hash: ann.ann.protected_hash.clone(),
        has_raw_igc: ann.ann.publication_mode == PublicationMode::Public && fetched_public_artifact,
        has_protected_sanitized_igc: ann.ann.publication_mode == PublicationMode::Protected
            && fetched_public_artifact,
        has_protected_raw_companion: false,
        serving_node_ids: vec![ann.ann.node_id.clone()],
        g_record_present: authoritative_g_record_present.or(ann.ann.g_record_present),
        recorded_at: canonical_utc_now(),
    };

    if let Some(existing) = node
        .store()
        .artifact_registry_record(&record.raw_igc_hash)?
    {
        let same_serving_claim = existing.publication_mode == record.publication_mode
            && existing.protected_hash == record.protected_hash
            && existing.g_record_present == record.g_record_present
            && existing.serving_node_ids.contains(&ann.ann.node_id);
        if same_serving_claim {
            return Ok(AnnouncementDisposition::Ignored(
                "already indexed".to_string(),
            ));
        }
        if existing.publication_mode == record.publication_mode
            && existing.protected_hash == record.protected_hash
            && existing.g_record_present == record.g_record_present
        {
            record.serving_node_ids = existing.serving_node_ids;
            record.serving_node_ids.push(ann.ann.node_id.clone());
            record.serving_node_ids.sort();
            record.serving_node_ids.dedup();
            record.has_raw_igc |= existing.has_raw_igc;
            record.has_protected_sanitized_igc |= existing.has_protected_sanitized_igc;
            record.has_protected_raw_companion |= existing.has_protected_raw_companion;
        }
    }

    node.store()
        .append_artifact_registry_record(&record)
        .await?;
    tracing::info!(
        raw_igc_hash = %record.raw_igc_hash,
        mode = ?record.publication_mode,
        "indexed artifact announcement"
    );
    Ok(AnnouncementDisposition::Indexed {
        fetched_igc: fetched_public_artifact,
    })
}

async fn fetch_and_store_announced_artifact(
    node: &IndexerHandle,
    ticket: &iroh_blobs::ticket::BlobTicket,
    expected_hash: &Blake3Hex,
    rl_state: &Option<RateLimitState>,
    publisher_node_id: &NodeIdHex,
) -> Result<Option<Vec<u8>>, IndexerError> {
    let bytes = fetch_blob(node, ticket).await?;
    let actual_hash = Blake3Hex::from_hash(blake3::hash(&bytes));
    if actual_hash != *expected_hash {
        return Ok(None);
    }
    node.store().put(&bytes).await?;
    record_bytes_accepted(rl_state, publisher_node_id, bytes.len() as u64).await;
    Ok(Some(bytes))
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

async fn apply_rate_limit(
    node_id: &NodeIdHex,
    rl_cfg: Option<&RateLimitConfig>,
    rl_state: Option<&RateLimitState>,
) -> Option<String> {
    let (Some(cfg), Some(state)) = (rl_cfg, rl_state) else {
        return None;
    };
    if cfg.trusted_node_ids.contains(node_id) {
        return None;
    }

    let mut map = state.lock().await;
    let stats = map.entry(node_id.clone()).or_default();
    let now = Instant::now();

    if now.duration_since(stats.hour_window_start) >= Duration::from_secs(3600) {
        stats.blobs_this_hour = 0;
        stats.hour_window_start = now;
    }
    if now.duration_since(stats.day_window_start) >= Duration::from_secs(86400) {
        stats.bytes_today = 0;
        stats.day_window_start = now;
    }

    if stats.blobs_this_hour >= cfg.blobs_per_hour {
        tracing::debug!(
            node_id = %node_id,
            limit = cfg.blobs_per_hour,
            "rate limit exceeded (blobs/hour) — dropping announcement"
        );
        return Some("rate limit exceeded (blobs/hour)".to_string());
    }
    let mb_today = stats.bytes_today as f64 / (1024.0 * 1024.0);
    if mb_today >= cfg.mb_per_day {
        tracing::debug!(
            node_id = %node_id,
            limit = cfg.mb_per_day,
            "rate limit exceeded (MB/day) — dropping announcement"
        );
        return Some("rate limit exceeded (MB/day)".to_string());
    }

    stats.blobs_this_hour += 1;
    None
}

fn should_fetch_v03_public_artifact(policy: &FetchPolicy) -> bool {
    matches!(policy, FetchPolicy::Eager)
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
    use crate::artifact_announcement::ArtifactAnnouncementError;

    #[test]
    fn rate_limit_config_default_values() {
        let cfg = RateLimitConfig::default();
        assert_eq!(cfg.blobs_per_hour, 100);
        assert!((cfg.mb_per_day - 200.0).abs() < f64::EPSILON);
        assert!(cfg.trusted_node_ids.is_empty());
    }

    #[test]
    fn malformed_json_is_silently_ignored() {
        assert!(matches!(
            ArtifactAnnouncement::parse_and_validate(b"not json at all"),
            Err(ArtifactAnnouncementError::Json(_))
        ));
    }

    #[test]
    fn announcement_missing_required_field_fails_parse() {
        let json = r#"{"schema":"igc-net/announcement"}"#;
        assert!(matches!(
            ArtifactAnnouncement::parse_and_validate(json.as_bytes()),
            Err(ArtifactAnnouncementError::Json(_))
        ));
    }

    #[test]
    fn oversized_announcement_is_rejected() {
        assert!(matches!(
            ArtifactAnnouncement::parse_and_validate(&vec![0_u8; 1025]),
            Err(ArtifactAnnouncementError::TooLarge(1025))
        ));
    }
}
