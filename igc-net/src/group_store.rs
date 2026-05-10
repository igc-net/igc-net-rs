//! Persistent group membership store with in-memory caches.
//!
//! Implements the storage layout described in `specs/75-groups-and-social.md §8`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use crate::group::{
    GroupCreationRecord, GroupRecordError, GroupType, PrivateGroupMemberAddRecord,
    PrivateGroupMemberRemoveRecord, PublicGroupAcceptRecord, PublicGroupInviteRecord,
    PublicGroupLeaveRecord,
};
use crate::id::{GroupId, PilotId};
use crate::util::write_json_file_atomic as write_json_file_atomic_impl;

const GROUPS_DIRNAME: &str = "groups";
const CREATIONS_DIRNAME: &str = "creations";
const PRIVATE_ADDS_DIRNAME: &str = "private-member-adds";
const PRIVATE_REMOVES_DIRNAME: &str = "private-member-removes";
const PUBLIC_INVITES_DIRNAME: &str = "public-invites";
const PUBLIC_ACCEPTS_DIRNAME: &str = "public-accepts";
const PUBLIC_LEAVES_DIRNAME: &str = "public-leaves";

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum GroupStoreError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("group record: {0}")]
    Record(#[from] GroupRecordError),
    #[error("group {0} not found")]
    GroupNotFound(String),
    #[error("group {0} is not a private group")]
    NotPrivateGroup(String),
    #[error("group {0} is not a public group")]
    NotPublicGroup(String),
    #[error("signer {0} is not the group owner")]
    NotGroupOwner(String),
    #[error("pilot {0} is already a member of group {1}")]
    AlreadyMember(String, String),
    #[error("pilot {0} has no pending invitation to group {1}")]
    NoPendingInvitation(String, String),
    #[error("pilot {0} is not a member of group {1}")]
    NotMember(String, String),
    #[error("signer {0} is not an existing member of group {1}")]
    NotExistingMember(String, String),
    #[error("missing parent directory")]
    MissingParentDirectory,
    #[error("group name too long (max 128 UTF-8 characters)")]
    NameTooLong,
    #[error("group name {0:?} is already taken")]
    DuplicateName(String),
}

// ── GroupMembership (returned by list_pilot_groups) ───────────────────────────

#[derive(Debug, Clone)]
pub struct GroupMembership {
    pub group_id: GroupId,
    pub group_type: GroupType,
    pub name: Option<String>,
    pub creator_pilot_id: PilotId,
    pub is_owner: bool,
}

// ── GroupStore ────────────────────────────────────────────────────────────────

pub struct GroupStore {
    root: PathBuf,
    /// group_id → GroupCreationRecord
    meta: RwLock<HashMap<GroupId, GroupCreationRecord>>,
    /// private group_id → set of member PilotIds (excludes the owner)
    private_members: RwLock<HashMap<GroupId, HashSet<PilotId>>>,
    /// PilotId → private group_ids where the pilot is the owner
    private_owned: RwLock<HashMap<PilotId, HashSet<GroupId>>>,
    /// PilotId → private group_ids where the pilot is a member (not owner)
    private_as_member: RwLock<HashMap<PilotId, HashSet<GroupId>>>,
    /// public group_id → set of full members (have accepted)
    public_members: RwLock<HashMap<GroupId, HashSet<PilotId>>>,
    /// PilotId → public group_ids where pilot is a full member
    pilot_public_groups: RwLock<HashMap<PilotId, HashSet<GroupId>>>,
    /// PilotId → public group_ids where pilot has a pending invitation
    pending_invitations: RwLock<HashMap<PilotId, HashSet<GroupId>>>,
}

impl GroupStore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            meta: RwLock::new(HashMap::new()),
            private_members: RwLock::new(HashMap::new()),
            private_owned: RwLock::new(HashMap::new()),
            private_as_member: RwLock::new(HashMap::new()),
            public_members: RwLock::new(HashMap::new()),
            pilot_public_groups: RwLock::new(HashMap::new()),
            pending_invitations: RwLock::new(HashMap::new()),
        }
    }

    pub fn for_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::open(data_dir.as_ref().join(GROUPS_DIRNAME))
    }

    fn init_dirs(&self) -> Result<(), GroupStoreError> {
        // One-shot migration: rename legacy personal-* directories to private-*.
        let legacy_adds = self.root.join("personal-member-adds");
        let legacy_removes = self.root.join("personal-member-removes");
        if legacy_adds.exists() {
            std::fs::rename(&legacy_adds, self.private_adds_dir())?;
        }
        if legacy_removes.exists() {
            std::fs::rename(&legacy_removes, self.private_removes_dir())?;
        }
        std::fs::create_dir_all(self.creations_dir())?;
        std::fs::create_dir_all(self.private_adds_dir())?;
        std::fs::create_dir_all(self.private_removes_dir())?;
        std::fs::create_dir_all(self.public_invites_dir())?;
        std::fs::create_dir_all(self.public_accepts_dir())?;
        std::fs::create_dir_all(self.public_leaves_dir())?;
        Ok(())
    }

    /// Load all persisted records into the in-memory cache.  Call once at startup.
    pub fn init(&self) -> Result<(), GroupStoreError> {
        self.init_dirs()?;
        // Process in dependency order: creations → adds → removes → invites → accepts → leaves.
        for record in self.load_all_json::<GroupCreationRecord>(self.creations_dir())? {
            record.validate()?;
            self.apply_creation(&record);
        }
        let mut adds = self.load_all_json::<PrivateGroupMemberAddRecord>(self.private_adds_dir())?;
        adds.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for record in adds {
            record.validate()?;
            self.apply_private_add(&record);
        }
        let mut removes = self.load_all_json::<PrivateGroupMemberRemoveRecord>(self.private_removes_dir())?;
        removes.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for record in removes {
            record.validate()?;
            self.apply_private_remove(&record);
        }
        let mut invites = self.load_all_json::<PublicGroupInviteRecord>(self.public_invites_dir())?;
        invites.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for record in invites {
            record.validate()?;
            self.apply_public_invite(&record);
        }
        let mut accepts = self.load_all_json::<PublicGroupAcceptRecord>(self.public_accepts_dir())?;
        accepts.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for record in accepts {
            record.validate()?;
            self.apply_public_accept(&record);
        }
        let mut leaves = self.load_all_json::<PublicGroupLeaveRecord>(self.public_leaves_dir())?;
        leaves.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        for record in leaves {
            record.validate()?;
            self.apply_public_leave(&record);
        }
        Ok(())
    }

    // ── Write operations ──────────────────────────────────────────────────────

    pub fn create_group(&self, record: GroupCreationRecord) -> Result<(), GroupStoreError> {
        self.init_dirs()?;
        record.validate()?;
        if let Some(name) = &record.name {
            if name.chars().count() > 128 {
                return Err(GroupStoreError::NameTooLong);
            }
            if self.lookup_by_name(name).is_some() {
                return Err(GroupStoreError::DuplicateName(name.clone()));
            }
        }
        let path = self.creations_dir().join(format!("{}.json", record.group_id.id_hex()));
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, &record)?;
        self.apply_creation(&record);
        Ok(())
    }

    pub fn add_private_group_member(
        &self,
        record: PrivateGroupMemberAddRecord,
    ) -> Result<(), GroupStoreError> {
        self.init_dirs()?;
        record.validate()?;
        self.check_private_group_owner(&record.group_id, &record.added_by_pilot_id)?;
        if self.is_private_member(&record.group_id, &record.member_pilot_id) {
            return Err(GroupStoreError::AlreadyMember(
                record.member_pilot_id.to_string(),
                record.group_id.to_string(),
            ));
        }
        let path = self.private_adds_dir().join(format!("{}.json", record.record_id));
        if !path.exists() {
            write_json_file_atomic(&path, &record)?;
            self.apply_private_add(&record);
        }
        Ok(())
    }

    pub fn remove_private_group_member(
        &self,
        record: PrivateGroupMemberRemoveRecord,
    ) -> Result<(), GroupStoreError> {
        self.init_dirs()?;
        record.validate()?;
        self.check_private_group_owner(&record.group_id, &record.removed_by_pilot_id)?;
        if !self.is_private_member(&record.group_id, &record.member_pilot_id) {
            return Err(GroupStoreError::NotMember(
                record.member_pilot_id.to_string(),
                record.group_id.to_string(),
            ));
        }
        let path = self.private_removes_dir().join(format!("{}.json", record.record_id));
        if !path.exists() {
            write_json_file_atomic(&path, &record)?;
            self.apply_private_remove(&record);
        }
        Ok(())
    }

    pub fn invite_to_public_group(
        &self,
        record: PublicGroupInviteRecord,
    ) -> Result<(), GroupStoreError> {
        self.init_dirs()?;
        record.validate()?;
        self.check_public_group_member(&record.group_id, &record.invited_by_pilot_id)?;
        if self.is_public_member(&record.group_id, &record.invited_pilot_id) {
            return Err(GroupStoreError::AlreadyMember(
                record.invited_pilot_id.to_string(),
                record.group_id.to_string(),
            ));
        }
        let path = self.public_invites_dir().join(format!("{}.json", record.record_id));
        if !path.exists() {
            write_json_file_atomic(&path, &record)?;
            self.apply_public_invite(&record);
        }
        Ok(())
    }

    pub fn accept_group_invitation(
        &self,
        record: PublicGroupAcceptRecord,
    ) -> Result<(), GroupStoreError> {
        self.init_dirs()?;
        record.validate()?;
        if !self.has_pending_invitation(&record.group_id, &record.member_pilot_id) {
            return Err(GroupStoreError::NoPendingInvitation(
                record.member_pilot_id.to_string(),
                record.group_id.to_string(),
            ));
        }
        let path = self.public_accepts_dir().join(format!("{}.json", record.record_id));
        if !path.exists() {
            write_json_file_atomic(&path, &record)?;
            self.apply_public_accept(&record);
        }
        Ok(())
    }

    pub fn leave_group(&self, record: PublicGroupLeaveRecord) -> Result<(), GroupStoreError> {
        self.init_dirs()?;
        record.validate()?;
        if !self.is_public_member(&record.group_id, &record.member_pilot_id) {
            return Err(GroupStoreError::NotMember(
                record.member_pilot_id.to_string(),
                record.group_id.to_string(),
            ));
        }
        let path = self.public_leaves_dir().join(format!("{}.json", record.record_id));
        if !path.exists() {
            write_json_file_atomic(&path, &record)?;
            self.apply_public_leave(&record);
        }
        Ok(())
    }

    // ── Query operations ──────────────────────────────────────────────────────

    /// True when `requester` is a member of any private group owned by `owner`.
    pub fn pilot_has_private_group_access(&self, requester: &PilotId, owner: &PilotId) -> bool {
        let member_groups = self.private_as_member.read().unwrap();
        let owned_groups = self.private_owned.read().unwrap();
        if let (Some(member_set), Some(owner_set)) =
            (member_groups.get(requester), owned_groups.get(owner))
        {
            return member_set.intersection(owner_set).next().is_some();
        }
        false
    }

    /// True when both pilots are confirmed members of at least one common public group.
    pub fn pilots_share_public_group(&self, pilot_a: &PilotId, pilot_b: &PilotId) -> bool {
        let groups = self.pilot_public_groups.read().unwrap();
        if let (Some(a_groups), Some(b_groups)) = (groups.get(pilot_a), groups.get(pilot_b)) {
            return a_groups.intersection(b_groups).next().is_some();
        }
        false
    }

    /// List all groups a pilot is associated with (owned, member-of, or public member).
    pub fn list_pilot_groups(&self, pilot_id: &PilotId) -> Vec<GroupMembership> {
        let meta = self.meta.read().unwrap();
        let private_owned = self.private_owned.read().unwrap();
        let private_as_member = self.private_as_member.read().unwrap();
        let pilot_public = self.pilot_public_groups.read().unwrap();

        let mut group_ids: HashSet<GroupId> = HashSet::new();
        if let Some(owned) = private_owned.get(pilot_id) {
            group_ids.extend(owned.iter().cloned());
        }
        if let Some(memberships) = private_as_member.get(pilot_id) {
            group_ids.extend(memberships.iter().cloned());
        }
        if let Some(public) = pilot_public.get(pilot_id) {
            group_ids.extend(public.iter().cloned());
        }

        let owned_set = private_owned.get(pilot_id).cloned().unwrap_or_default();

        group_ids
            .into_iter()
            .filter_map(|gid| {
                meta.get(&gid).map(|m| GroupMembership {
                    group_id: gid.clone(),
                    group_type: m.group_type.clone(),
                    name: m.name.clone(),
                    creator_pilot_id: m.creator_pilot_id.clone(),
                    is_owner: owned_set.contains(&gid),
                })
            })
            .collect()
    }

    /// List all confirmed members of a public group.
    pub fn list_group_members(&self, group_id: &GroupId) -> Vec<PilotId> {
        let public_members = self.public_members.read().unwrap();
        public_members
            .get(group_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// List all private-group members added by `owner`.
    pub fn list_private_group_members(&self, group_id: &GroupId) -> Vec<PilotId> {
        let private_members = self.private_members.read().unwrap();
        private_members
            .get(group_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// List public groups with a pending invitation for `pilot_id`.
    pub fn list_pending_invitations(&self, pilot_id: &PilotId) -> Vec<GroupId> {
        let pending = self.pending_invitations.read().unwrap();
        pending
            .get(pilot_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn group_meta(&self, group_id: &GroupId) -> Option<GroupCreationRecord> {
        self.meta.read().unwrap().get(group_id).cloned()
    }

    /// Find a group by its exact name (case-sensitive). Returns `None` if no group matches.
    pub fn lookup_by_name(&self, name: &str) -> Option<GroupCreationRecord> {
        self.meta
            .read()
            .unwrap()
            .values()
            .find(|r| r.name.as_deref() == Some(name))
            .cloned()
    }

    // ── Private apply helpers (update caches) ─────────────────────────────────

    fn apply_creation(&self, record: &GroupCreationRecord) {
        let mut meta = self.meta.write().unwrap();
        meta.entry(record.group_id.clone())
            .or_insert_with(|| record.clone());

        match record.group_type {
            GroupType::Private => {
                let mut owned = self.private_owned.write().unwrap();
                owned
                    .entry(record.creator_pilot_id.clone())
                    .or_default()
                    .insert(record.group_id.clone());
                self.private_members
                    .write()
                    .unwrap()
                    .entry(record.group_id.clone())
                    .or_default();
            }
            GroupType::Public => {
                // Creator is automatically a full member of the public group.
                let mut pub_members = self.public_members.write().unwrap();
                pub_members
                    .entry(record.group_id.clone())
                    .or_default()
                    .insert(record.creator_pilot_id.clone());
                let mut pilot_pub = self.pilot_public_groups.write().unwrap();
                pilot_pub
                    .entry(record.creator_pilot_id.clone())
                    .or_default()
                    .insert(record.group_id.clone());
            }
        }
    }

    fn apply_private_add(&self, record: &PrivateGroupMemberAddRecord) {
        self.private_members
            .write()
            .unwrap()
            .entry(record.group_id.clone())
            .or_default()
            .insert(record.member_pilot_id.clone());
        self.private_as_member
            .write()
            .unwrap()
            .entry(record.member_pilot_id.clone())
            .or_default()
            .insert(record.group_id.clone());
    }

    fn apply_private_remove(&self, record: &PrivateGroupMemberRemoveRecord) {
        if let Some(set) = self.private_members.write().unwrap().get_mut(&record.group_id) {
            set.remove(&record.member_pilot_id);
        }
        if let Some(set) = self.private_as_member.write().unwrap().get_mut(&record.member_pilot_id) {
            set.remove(&record.group_id);
        }
    }

    fn apply_public_invite(&self, record: &PublicGroupInviteRecord) {
        self.pending_invitations
            .write()
            .unwrap()
            .entry(record.invited_pilot_id.clone())
            .or_default()
            .insert(record.group_id.clone());
    }

    fn apply_public_accept(&self, record: &PublicGroupAcceptRecord) {
        // Remove from pending.
        if let Some(set) = self.pending_invitations.write().unwrap().get_mut(&record.member_pilot_id) {
            set.remove(&record.group_id);
        }
        // Add to full members.
        self.public_members
            .write()
            .unwrap()
            .entry(record.group_id.clone())
            .or_default()
            .insert(record.member_pilot_id.clone());
        self.pilot_public_groups
            .write()
            .unwrap()
            .entry(record.member_pilot_id.clone())
            .or_default()
            .insert(record.group_id.clone());
    }

    fn apply_public_leave(&self, record: &PublicGroupLeaveRecord) {
        if let Some(set) = self.public_members.write().unwrap().get_mut(&record.group_id) {
            set.remove(&record.member_pilot_id);
        }
        if let Some(set) = self.pilot_public_groups.write().unwrap().get_mut(&record.member_pilot_id) {
            set.remove(&record.group_id);
        }
    }

    // ── Validation helpers ────────────────────────────────────────────────────

    fn check_private_group_owner(
        &self,
        group_id: &GroupId,
        pilot_id: &PilotId,
    ) -> Result<(), GroupStoreError> {
        let meta = self.meta.read().unwrap();
        let record = meta
            .get(group_id)
            .ok_or_else(|| GroupStoreError::GroupNotFound(group_id.to_string()))?;
        if record.group_type != GroupType::Private {
            return Err(GroupStoreError::NotPrivateGroup(group_id.to_string()));
        }
        if &record.creator_pilot_id != pilot_id {
            return Err(GroupStoreError::NotGroupOwner(pilot_id.to_string()));
        }
        Ok(())
    }

    fn check_public_group_member(
        &self,
        group_id: &GroupId,
        pilot_id: &PilotId,
    ) -> Result<(), GroupStoreError> {
        let meta = self.meta.read().unwrap();
        let record = meta
            .get(group_id)
            .ok_or_else(|| GroupStoreError::GroupNotFound(group_id.to_string()))?;
        if record.group_type != GroupType::Public {
            return Err(GroupStoreError::NotPublicGroup(group_id.to_string()));
        }
        drop(meta);
        if !self.is_public_member(group_id, pilot_id) {
            return Err(GroupStoreError::NotExistingMember(
                pilot_id.to_string(),
                group_id.to_string(),
            ));
        }
        Ok(())
    }

    fn is_private_member(&self, group_id: &GroupId, pilot_id: &PilotId) -> bool {
        self.private_members
            .read()
            .unwrap()
            .get(group_id)
            .map(|s| s.contains(pilot_id))
            .unwrap_or(false)
    }

    fn is_public_member(&self, group_id: &GroupId, pilot_id: &PilotId) -> bool {
        self.public_members
            .read()
            .unwrap()
            .get(group_id)
            .map(|s| s.contains(pilot_id))
            .unwrap_or(false)
    }

    fn has_pending_invitation(&self, group_id: &GroupId, pilot_id: &PilotId) -> bool {
        self.pending_invitations
            .read()
            .unwrap()
            .get(pilot_id)
            .map(|s| s.contains(group_id))
            .unwrap_or(false)
    }

    // ── Directory paths ───────────────────────────────────────────────────────

    fn creations_dir(&self) -> PathBuf { self.root.join(CREATIONS_DIRNAME) }
    fn private_adds_dir(&self) -> PathBuf { self.root.join(PRIVATE_ADDS_DIRNAME) }
    fn private_removes_dir(&self) -> PathBuf { self.root.join(PRIVATE_REMOVES_DIRNAME) }
    fn public_invites_dir(&self) -> PathBuf { self.root.join(PUBLIC_INVITES_DIRNAME) }
    fn public_accepts_dir(&self) -> PathBuf { self.root.join(PUBLIC_ACCEPTS_DIRNAME) }
    fn public_leaves_dir(&self) -> PathBuf { self.root.join(PUBLIC_LEAVES_DIRNAME) }

    // ── Generic file helpers ──────────────────────────────────────────────────

    fn load_all_json<T: serde::de::DeserializeOwned>(
        &self,
        dir: PathBuf,
    ) -> Result<Vec<T>, GroupStoreError> {
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for entry in std::fs::read_dir(&dir)?.filter_map(Result::ok) {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let bytes = std::fs::read(&path)?;
            records.push(serde_json::from_slice(&bytes)?);
        }
        Ok(records)
    }
}

fn write_json_file_atomic<T: serde::Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), GroupStoreError> {
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
        GroupStoreError::MissingParentDirectory,
    )
}
