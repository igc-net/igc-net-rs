//! Content-addressed flat-file blob store.
//!
//! Layout uses a simple local BLAKE3-addressed store:
//!
//! ```text
//! {root}/
//!   blobs/<first-2-blake3-hex>/<full-64-char-blake3-hex>   ← raw blob bytes
//!   index.ndjson                                            ← append-only flight index
//!   node.key                                                ← 32-byte Ed25519 secret key
//! ```

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::id::{Blake3Hex, IdentifierError, NodeIdHex};

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("identifier: {0}")]
    Identifier(#[from] IdentifierError),
    #[error("lock poisoned: {0}")]
    PoisonedLock(&'static str),
}

// ── IndexRecord ───────────────────────────────────────────────────────────────

/// Origin of an index record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexRecordSource {
    LocalPublish,
    RemoteAnnouncement,
}

/// One line in `index.ndjson`.
///
/// Records are append-only. When multiple records describe the same
/// `(meta_hash, node_id)` pair, the latest record is authoritative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexRecord {
    /// Whether this record was created by a local publish or a remote announce.
    pub source: IndexRecordSource,
    /// 64-char BLAKE3 hex of the raw IGC file.
    pub igc_hash: Blake3Hex,
    /// 64-char BLAKE3 hex of the metadata JSON blob.
    pub meta_hash: Blake3Hex,
    /// Serving node identity for this announcement.
    pub node_id: NodeIdHex,
    /// Latest known ticket for the IGC blob from this serving node.
    pub igc_ticket: String,
    /// Latest known ticket for the metadata blob from this serving node.
    pub meta_ticket: String,
    /// RFC 3339 UTC timestamp of when this node first published the flight.
    pub recorded_at: String,
}

// ── FlatFileStore ─────────────────────────────────────────────────────────────

/// Content-addressed flat-file blob store keyed by BLAKE3.
///
/// An in-memory cache of `(meta_hash, node_id)` pairs and known `meta_hash`
/// values is maintained to avoid O(n) linear scans of `index.ndjson` on the
/// indexer hot path.  The cache is populated during [`init`] and updated by
/// [`append_index`].
pub struct FlatFileStore {
    root: PathBuf,
    /// Cached `(meta_hash, node_id)` pairs — dedup key per the protocol spec.
    dedup_cache: RwLock<HashSet<(Blake3Hex, NodeIdHex)>>,
    /// Cached set of known `meta_hash` values.
    meta_hash_cache: RwLock<HashSet<Blake3Hex>>,
    /// Cached latest local publish record per `(igc_hash, node_id)`.
    latest_local_publish_cache: RwLock<HashMap<(Blake3Hex, NodeIdHex), IndexRecord>>,
    /// Cached in-order copy of all index records.
    index_records_cache: RwLock<Vec<IndexRecord>>,
    /// Cached remote discovery events paired with their line sequence number.
    discovery_events_cache: RwLock<Vec<(u64, IndexRecord)>>,
    /// Serializes index file appends and dedup checks that must be atomic.
    append_lock: Mutex<()>,
}

type DedupKey = (Blake3Hex, NodeIdHex);
type LatestLocalPublishMap = HashMap<DedupKey, IndexRecord>;

impl FlatFileStore {
    /// Open (or create) a store rooted at `root`.
    ///
    /// Directories are created lazily by [`init`].
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            dedup_cache: RwLock::new(HashSet::new()),
            meta_hash_cache: RwLock::new(HashSet::new()),
            latest_local_publish_cache: RwLock::new(HashMap::new()),
            index_records_cache: RwLock::new(Vec::new()),
            discovery_events_cache: RwLock::new(Vec::new()),
            append_lock: Mutex::new(()),
        }
    }

    /// Create the required directory structure and populate the in-memory
    /// dedup cache from any existing `index.ndjson`.
    pub async fn init(&self) -> Result<(), StoreError> {
        fs::create_dir_all(self.blobs_dir()).await?;
        self.reload_cache()?;
        Ok(())
    }

    /// Rebuild the in-memory caches from `index.ndjson`.
    fn reload_cache(&self) -> Result<(), StoreError> {
        let mut dedup = self.dedup_cache.write().map_err(|_| StoreError::PoisonedLock("dedup_cache"))?;
        let mut metas = self.meta_hash_cache.write().map_err(|_| StoreError::PoisonedLock("meta_hash_cache"))?;
        let mut latest_local = self
            .latest_local_publish_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("latest_local_publish_cache"))?;
        let mut index_records = self
            .index_records_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("index_records_cache"))?;
        let mut discovery_events = self
            .discovery_events_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("discovery_events_cache"))?;
        dedup.clear();
        metas.clear();
        latest_local.clear();
        index_records.clear();
        discovery_events.clear();
        for (seq, record) in self.iter_index_file()?.enumerate() {
            let r = record?;
            dedup.insert((r.meta_hash.clone(), r.node_id.clone()));
            metas.insert(r.meta_hash.clone());
            if r.source == IndexRecordSource::LocalPublish {
                latest_local.insert((r.igc_hash.clone(), r.node_id.clone()), r.clone());
            } else {
                discovery_events.push((seq as u64, r.clone()));
            }
            index_records.push(r);
        }
        Ok(())
    }

    // ── Internal path helpers ─────────────────────────────────────────────────

    fn blobs_dir(&self) -> PathBuf {
        self.root.join("blobs")
    }

    fn blob_path(&self, blake3_hex: &Blake3Hex) -> PathBuf {
        self.blobs_dir()
            .join(&blake3_hex.as_str()[..2])
            .join(blake3_hex.as_str())
    }

    fn index_path(&self) -> PathBuf {
        self.root.join("index.ndjson")
    }

    fn key_path(&self) -> PathBuf {
        self.root.join("node.key")
    }

    // ── Blob operations ───────────────────────────────────────────────────────

    /// Return the filesystem path for a blob without reading it.
    ///
    /// Returns `Some(path)` if the blob exists locally, `None` otherwise.
    pub fn resolve_path(&self, blake3_hex: &str) -> Result<Option<PathBuf>, StoreError> {
        let blake3_hex = Blake3Hex::parse(blake3_hex)?;
        let path = self.blob_path(&blake3_hex);
        Ok(if path.exists() { Some(path) } else { None })
    }

    /// Hash `bytes` with BLAKE3 and store under `blobs/`.
    ///
    /// Returns the 64-char hex key.  Idempotent: if the blob already exists
    /// the write is skipped (content-addressable deduplication).
    pub async fn put(&self, bytes: &[u8]) -> Result<Blake3Hex, StoreError> {
        let hex = Blake3Hex::from_hash(blake3::hash(bytes));
        let path = self.blob_path(&hex);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                file.write_all(bytes).await?;
                file.flush().await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(StoreError::Io(e)),
        }
        Ok(hex)
    }

    /// Read a blob by its 64-char BLAKE3 hex key.  Returns `None` if not found.
    pub async fn get(&self, blake3_hex: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let blake3_hex = Blake3Hex::parse(blake3_hex)?;
        let path = self.blob_path(&blake3_hex);
        match fs::read(&path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Check existence without reading the full blob.
    pub fn contains(&self, blake3_hex: &str) -> Result<bool, StoreError> {
        let blake3_hex = Blake3Hex::parse(blake3_hex)?;
        Ok(self.blob_path(&blake3_hex).exists())
    }

    // ── Index operations ──────────────────────────────────────────────────────

    /// Append one record to `index.ndjson` (one JSON object per line).
    ///
    /// Also updates the in-memory dedup and meta_hash caches.
    pub async fn append_index(&self, record: &IndexRecord) -> Result<(), StoreError> {
        let _append_guard = self.append_lock.lock().await;
        self.append_index_unlocked(record).await
    }

    /// Append one record only if the `(meta_hash, node_id)` pair is absent.
    ///
    /// Returns `true` when a new record was appended, `false` when the record
    /// was already present in the dedup cache.
    pub async fn append_index_if_absent(&self, record: &IndexRecord) -> Result<bool, StoreError> {
        let _append_guard = self.append_lock.lock().await;
        if self
            .dedup_read()?
            .contains(&(record.meta_hash.clone(), record.node_id.clone()))
        {
            return Ok(false);
        }
        self.append_index_unlocked(record).await?;
        Ok(true)
    }

    async fn append_index_unlocked(&self, record: &IndexRecord) -> Result<(), StoreError> {
        let mut line = serde_json::to_string(record)?;
        line.push('\n');

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.index_path())
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.flush().await?;

        // Update in-memory caches.
        self.dedup_write()?
            .insert((record.meta_hash.clone(), record.node_id.clone()));
        self.meta_hash_write()?.insert(record.meta_hash.clone());
        if record.source == IndexRecordSource::LocalPublish {
            self.latest_local_publish_write()?.insert(
                (record.igc_hash.clone(), record.node_id.clone()),
                record.clone(),
            );
        } else {
            let seq = self.index_records_read()?.len() as u64;
            self.discovery_events_write()?.push((seq, record.clone()));
        }
        self.index_records_write()?.push(record.clone());

        Ok(())
    }

    /// Iterate all records from the in-memory index cache.
    pub fn iter_index(
        &self,
    ) -> Result<impl Iterator<Item = Result<IndexRecord, StoreError>>, StoreError> {
        let records = self.index_records_read()?.clone();
        Ok(Box::new(records.into_iter().map(Ok))
            as Box<dyn Iterator<Item = Result<IndexRecord, StoreError>>>)
    }

    /// Iterate all records in `index.ndjson` (synchronous, for startup only).
    fn iter_index_file(
        &self,
    ) -> Result<impl Iterator<Item = Result<IndexRecord, StoreError>>, StoreError> {
        use std::io::{BufRead, BufReader};

        let path = self.index_path();
        // Return empty iterator if the index file does not exist yet.
        if !path.exists() {
            let v: Vec<Result<IndexRecord, StoreError>> = Vec::new();
            return Ok(Box::new(v.into_iter())
                as Box<dyn Iterator<Item = Result<IndexRecord, StoreError>>>);
        }

        let file = std::fs::File::open(&path).map_err(StoreError::Io)?;
        let reader = BufReader::new(file);
        Ok(Box::new(reader.lines().map(|line| {
            let line = line.map_err(StoreError::Io)?;
            serde_json::from_str::<IndexRecord>(&line).map_err(StoreError::Json)
        }))
            as Box<
                dyn Iterator<Item = Result<IndexRecord, StoreError>>,
            >)
    }

    /// True if the exact `(meta_hash, node_id)` pair is already recorded.
    ///
    /// Uses the in-memory dedup cache — O(1) after [`init`].
    pub fn has_index_record(&self, meta_hash: &str, node_id: &str) -> Result<bool, StoreError> {
        let meta_hash = Blake3Hex::parse(meta_hash)?;
        let node_id = NodeIdHex::parse(node_id)?;
        Ok(self.dedup_read()?.contains(&(meta_hash, node_id)))
    }

    /// True if any record is known for this metadata blob.
    ///
    /// Uses the in-memory meta_hash cache — O(1) after [`init`].
    pub fn has_meta_hash(&self, meta_hash: &str) -> Result<bool, StoreError> {
        let meta_hash = Blake3Hex::parse(meta_hash)?;
        Ok(self.meta_hash_read()?.contains(&meta_hash))
    }

    /// Return all `RemoteAnnouncement` index records at or after position `since_seq`.
    ///
    /// `since_seq` is a 0-based line number in `index.ndjson`.  The discovery
    /// worker persists the last processed seq and resumes from there on restart,
    /// providing at-least-once delivery across restarts.
    ///
    /// Returns `(seq, record)` pairs ordered by ascending seq.
    pub fn discovery_events_since(
        &self,
        since_seq: u64,
    ) -> Result<Vec<(u64, IndexRecord)>, StoreError> {
        let events = self.discovery_events_read()?;
        let start = events.partition_point(|(seq, _)| *seq < since_seq);
        Ok(events[start..].to_vec())
    }

    /// Return the latest local publish record for an IGC hash from this node.
    pub fn latest_local_publish(
        &self,
        igc_hash: &Blake3Hex,
        node_id: &NodeIdHex,
    ) -> Result<Option<IndexRecord>, StoreError> {
        Ok(self
            .latest_local_publish_read()?
            .get(&(igc_hash.clone(), node_id.clone()))
            .cloned())
    }

    // ── Key management ────────────────────────────────────────────────────────

    /// Load the raw 32-byte secret key from `node.key`, or return `None` if
    /// the file does not exist.
    pub fn load_key_bytes(&self) -> Result<Option<[u8; 32]>, StoreError> {
        use std::io::Read;
        let path = self.key_path();
        if !path.exists() {
            return Ok(None);
        }
        let mut bytes = [0u8; 32];
        std::fs::File::open(&path)
            .and_then(|mut f| f.read_exact(&mut bytes))
            .map_err(StoreError::Io)?;
        Ok(Some(bytes))
    }

    /// Persist a 32-byte secret key to `node.key` with mode 0600.
    pub fn save_key_bytes(&self, bytes: &[u8; 32]) -> Result<(), StoreError> {
        write_key_file(&self.key_path(), bytes)
    }

    fn dedup_read(&self) -> Result<RwLockReadGuard<'_, HashSet<DedupKey>>, StoreError> {
        self.dedup_cache
            .read()
            .map_err(|_| StoreError::PoisonedLock("dedup_cache"))
    }

    fn dedup_write(&self) -> Result<RwLockWriteGuard<'_, HashSet<DedupKey>>, StoreError> {
        self.dedup_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("dedup_cache"))
    }

    fn meta_hash_read(&self) -> Result<RwLockReadGuard<'_, HashSet<Blake3Hex>>, StoreError> {
        self.meta_hash_cache
            .read()
            .map_err(|_| StoreError::PoisonedLock("meta_hash_cache"))
    }

    fn meta_hash_write(&self) -> Result<RwLockWriteGuard<'_, HashSet<Blake3Hex>>, StoreError> {
        self.meta_hash_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("meta_hash_cache"))
    }

    fn latest_local_publish_read(
        &self,
    ) -> Result<RwLockReadGuard<'_, LatestLocalPublishMap>, StoreError> {
        self.latest_local_publish_cache
            .read()
            .map_err(|_| StoreError::PoisonedLock("latest_local_publish_cache"))
    }

    fn latest_local_publish_write(
        &self,
    ) -> Result<RwLockWriteGuard<'_, LatestLocalPublishMap>, StoreError> {
        self.latest_local_publish_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("latest_local_publish_cache"))
    }

    fn index_records_read(&self) -> Result<RwLockReadGuard<'_, Vec<IndexRecord>>, StoreError> {
        self.index_records_cache
            .read()
            .map_err(|_| StoreError::PoisonedLock("index_records_cache"))
    }

    fn index_records_write(&self) -> Result<RwLockWriteGuard<'_, Vec<IndexRecord>>, StoreError> {
        self.index_records_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("index_records_cache"))
    }

    fn discovery_events_read(
        &self,
    ) -> Result<RwLockReadGuard<'_, Vec<(u64, IndexRecord)>>, StoreError> {
        self.discovery_events_cache
            .read()
            .map_err(|_| StoreError::PoisonedLock("discovery_events_cache"))
    }

    fn discovery_events_write(
        &self,
    ) -> Result<RwLockWriteGuard<'_, Vec<(u64, IndexRecord)>>, StoreError> {
        self.discovery_events_cache
            .write()
            .map_err(|_| StoreError::PoisonedLock("discovery_events_cache"))
    }
}

// ── Platform helpers ──────────────────────────────────────────────────────────

#[cfg(unix)]
fn write_key_file(path: &Path, bytes: &[u8; 32]) -> Result<(), StoreError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(StoreError::Io)?;
    file.write_all(bytes).map_err(StoreError::Io)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_key_file(path: &Path, bytes: &[u8; 32]) -> Result<(), StoreError> {
    use std::io::Write;
    let mut file = std::fs::File::create(path).map_err(StoreError::Io)?;
    file.write_all(bytes).map_err(StoreError::Io)?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{Blake3Hex, IdentifierError, NodeIdHex};

    async fn temp_store() -> (FlatFileStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = FlatFileStore::open(dir.path());
        store.init().await.unwrap();
        (store, dir)
    }

    fn hash(ch: char) -> Blake3Hex {
        Blake3Hex::parse(ch.to_string().repeat(64)).unwrap()
    }

    fn node_id(ch: char) -> NodeIdHex {
        NodeIdHex::parse(ch.to_string().repeat(64)).unwrap()
    }

    #[tokio::test]
    async fn put_get_round_trip() {
        let (store, _dir) = temp_store().await;
        let data = b"hello igc-net";
        let hex = store.put(data).await.unwrap();
        assert_eq!(hex.len(), 64);
        let got = store.get(&hex).await.unwrap().unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn put_is_idempotent() {
        let (store, _dir) = temp_store().await;
        let data = b"same content";
        let h1 = store.put(data).await.unwrap();
        let h2 = store.put(data).await.unwrap();
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn contains_false_before_put_true_after() {
        let (store, _dir) = temp_store().await;
        let data = b"check contains";
        let hex = Blake3Hex::from_hash(blake3::hash(data));
        assert!(!store.contains(&hex).unwrap());
        store.put(data).await.unwrap();
        assert!(store.contains(&hex).unwrap());
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (store, _dir) = temp_store().await;
        let hex = hash('a');
        let result = store.get(&hex).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn invalid_hash_is_rejected_by_lookup_apis() {
        let (store, _dir) = temp_store().await;
        assert!(matches!(
            store.contains("bad-hash"),
            Err(StoreError::Identifier(IdentifierError::Blake3Hex(_)))
        ));
        assert!(matches!(
            store.resolve_path("bad-hash"),
            Err(StoreError::Identifier(IdentifierError::Blake3Hex(_)))
        ));
        assert!(matches!(
            store.get("bad-hash").await,
            Err(StoreError::Identifier(IdentifierError::Blake3Hex(_)))
        ));
    }

    #[tokio::test]
    async fn index_round_trip() {
        let (store, _dir) = temp_store().await;
        let rec = IndexRecord {
            source: IndexRecordSource::LocalPublish,
            igc_hash: hash('a'),
            meta_hash: hash('b'),
            node_id: node_id('c'),
            igc_ticket: "igc_ticket".to_string(),
            meta_ticket: "meta_ticket".to_string(),
            recorded_at: "2026-03-22T12:00:00Z".to_string(),
        };
        store.append_index(&rec).await.unwrap();
        store.append_index(&rec).await.unwrap();

        let records: Vec<_> = store.iter_index().unwrap().collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].as_ref().unwrap().igc_hash, hash('a'));
    }

    #[tokio::test]
    async fn has_index_record_uses_meta_hash_and_node_id() {
        let (store, _dir) = temp_store().await;
        store
            .append_index(&IndexRecord {
                source: IndexRecordSource::RemoteAnnouncement,
                igc_hash: hash('a'),
                meta_hash: hash('b'),
                node_id: node_id('c'),
                igc_ticket: "igc_ticket_1".to_string(),
                meta_ticket: "meta_ticket_1".to_string(),
                recorded_at: "2026-03-22T12:00:00Z".to_string(),
            })
            .await
            .unwrap();

        assert!(
            store
                .has_index_record(&"b".repeat(64), &"c".repeat(64))
                .unwrap()
        );
        assert!(
            !store
                .has_index_record(&"b".repeat(64), &"d".repeat(64))
                .unwrap()
        );
        assert!(store.has_meta_hash(&"b".repeat(64)).unwrap());
    }

    #[tokio::test]
    async fn latest_local_publish_returns_last_matching_record() {
        let (store, _dir) = temp_store().await;
        for recorded_at in ["2026-03-22T12:00:00Z", "2026-03-22T12:05:00Z"] {
            store
                .append_index(&IndexRecord {
                    source: IndexRecordSource::LocalPublish,
                    igc_hash: hash('a'),
                    meta_hash: hash('b'),
                    node_id: node_id('c'),
                    igc_ticket: format!("igc_ticket_{recorded_at}"),
                    meta_ticket: format!("meta_ticket_{recorded_at}"),
                    recorded_at: recorded_at.to_string(),
                })
                .await
                .unwrap();
        }

        let latest = store
            .latest_local_publish(&hash('a'), &node_id('c'))
            .unwrap()
            .unwrap();
        assert_eq!(latest.recorded_at, "2026-03-22T12:05:00Z");
    }

    #[tokio::test]
    async fn iter_index_on_empty_store_returns_empty() {
        let (store, _dir) = temp_store().await;
        let records: Vec<_> = store.iter_index().unwrap().collect();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn key_persistence() {
        let (store, _dir) = temp_store().await;
        assert!(store.load_key_bytes().unwrap().is_none());

        let key = [42u8; 32];
        store.save_key_bytes(&key).unwrap();

        let loaded = store.load_key_bytes().unwrap().unwrap();
        assert_eq!(loaded, key);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn key_file_has_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let (store, dir) = temp_store().await;
        store.save_key_bytes(&[0u8; 32]).unwrap();
        let meta = std::fs::metadata(dir.path().join("node.key")).unwrap();
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "node.key must have mode 0600, got {mode:o}");
    }
}
