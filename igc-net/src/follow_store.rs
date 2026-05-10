//! Persistent follow/unfollow store with in-memory caches.
//!
//! Implements the follow storage layout described in `specs/70-groups-and-social.md §8`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use crate::follow::{FollowRecord, FollowRecordError, UnfollowRecord};
use crate::id::PilotId;
use crate::util::write_json_file_atomic as write_json_file_atomic_impl;

const FOLLOWS_DIRNAME: &str = "follows";
const FOLLOW_RECORDS_DIRNAME: &str = "follow-records";
const UNFOLLOW_RECORDS_DIRNAME: &str = "unfollow-records";

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum FollowStoreError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("follow record: {0}")]
    Record(#[from] FollowRecordError),
    #[error("pilot {0} is already following {1}")]
    AlreadyFollowing(String, String),
    #[error("pilot {0} is not following {1}")]
    NotFollowing(String, String),
    #[error("missing parent directory")]
    MissingParentDirectory,
}

// ── FollowStore ───────────────────────────────────────────────────────────────

pub struct FollowStore {
    root: PathBuf,
    /// follower → set of followees
    following: RwLock<HashMap<PilotId, HashSet<PilotId>>>,
    /// followee → set of followers
    followers: RwLock<HashMap<PilotId, HashSet<PilotId>>>,
}

impl FollowStore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            following: RwLock::new(HashMap::new()),
            followers: RwLock::new(HashMap::new()),
        }
    }

    pub fn for_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::open(data_dir.as_ref().join(FOLLOWS_DIRNAME))
    }

    fn init_dirs(&self) -> Result<(), FollowStoreError> {
        std::fs::create_dir_all(self.follow_records_dir())?;
        std::fs::create_dir_all(self.unfollow_records_dir())?;
        Ok(())
    }

    /// Load all persisted records into the in-memory cache.  Call once at startup.
    pub fn init(&self) -> Result<(), FollowStoreError> {
        self.init_dirs()?;
        let mut follows = self.load_all_json::<FollowRecord>(self.follow_records_dir())?;
        follows.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for r in follows {
            r.validate()?;
            self.apply_follow(&r);
        }
        let mut unfollow = self.load_all_json::<UnfollowRecord>(self.unfollow_records_dir())?;
        unfollow.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for r in unfollow {
            r.validate()?;
            self.apply_unfollow(&r);
        }
        Ok(())
    }

    // ── Write operations ──────────────────────────────────────────────────────

    pub fn follow_pilot(&self, record: FollowRecord) -> Result<(), FollowStoreError> {
        self.init_dirs()?;
        record.validate()?;
        if self.is_following(&record.follower_pilot_id, &record.followee_pilot_id) {
            return Err(FollowStoreError::AlreadyFollowing(
                record.follower_pilot_id.to_string(),
                record.followee_pilot_id.to_string(),
            ));
        }
        let path = self.follow_records_dir().join(format!("{}.json", record.record_id));
        if !path.exists() {
            write_json_file_atomic(&path, &record)?;
            self.apply_follow(&record);
        }
        Ok(())
    }

    pub fn unfollow_pilot(&self, record: UnfollowRecord) -> Result<(), FollowStoreError> {
        self.init_dirs()?;
        record.validate()?;
        if !self.is_following(&record.follower_pilot_id, &record.followee_pilot_id) {
            return Err(FollowStoreError::NotFollowing(
                record.follower_pilot_id.to_string(),
                record.followee_pilot_id.to_string(),
            ));
        }
        let path = self.unfollow_records_dir().join(format!("{}.json", record.record_id));
        if !path.exists() {
            write_json_file_atomic(&path, &record)?;
            self.apply_unfollow(&record);
        }
        Ok(())
    }

    // ── Query operations ──────────────────────────────────────────────────────

    pub fn list_following(&self, pilot_id: &PilotId) -> Vec<PilotId> {
        self.following
            .read()
            .unwrap()
            .get(pilot_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn list_followers(&self, pilot_id: &PilotId) -> Vec<PilotId> {
        self.followers
            .read()
            .unwrap()
            .get(pilot_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn is_following(&self, follower: &PilotId, followee: &PilotId) -> bool {
        self.following
            .read()
            .unwrap()
            .get(follower)
            .map(|s| s.contains(followee))
            .unwrap_or(false)
    }

    // ── Private apply helpers ─────────────────────────────────────────────────

    fn apply_follow(&self, record: &FollowRecord) {
        self.following
            .write()
            .unwrap()
            .entry(record.follower_pilot_id.clone())
            .or_default()
            .insert(record.followee_pilot_id.clone());
        self.followers
            .write()
            .unwrap()
            .entry(record.followee_pilot_id.clone())
            .or_default()
            .insert(record.follower_pilot_id.clone());
    }

    fn apply_unfollow(&self, record: &UnfollowRecord) {
        if let Some(set) = self.following.write().unwrap().get_mut(&record.follower_pilot_id) {
            set.remove(&record.followee_pilot_id);
        }
        if let Some(set) = self.followers.write().unwrap().get_mut(&record.followee_pilot_id) {
            set.remove(&record.follower_pilot_id);
        }
    }

    // ── Directory paths ───────────────────────────────────────────────────────

    fn follow_records_dir(&self) -> PathBuf { self.root.join(FOLLOW_RECORDS_DIRNAME) }
    fn unfollow_records_dir(&self) -> PathBuf { self.root.join(UNFOLLOW_RECORDS_DIRNAME) }

    fn load_all_json<T: serde::de::DeserializeOwned>(
        &self,
        dir: PathBuf,
    ) -> Result<Vec<T>, FollowStoreError> {
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for entry in std::fs::read_dir(&dir)?.filter_map(Result::ok) {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            records.push(serde_json::from_slice(&std::fs::read(&path)?)?);
        }
        Ok(records)
    }
}

fn write_json_file_atomic<T: serde::Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), FollowStoreError> {
    write_json_file_atomic_impl(
        path,
        value,
        |parent| {
            std::fs::create_dir_all(parent)?;
            Ok(())
        },
        |tmp_path, bytes| {
            std::fs::write(tmp_path, bytes)?;
            Ok(())
        },
        FollowStoreError::MissingParentDirectory,
    )
}
