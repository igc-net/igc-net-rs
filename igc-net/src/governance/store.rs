use std::path::{Path, PathBuf};

use crate::governance::lookup::GovernanceLookup;
use crate::governance::selection::{GovernanceSelectionError, select_pilot_auth_did_state};
use crate::id::PilotId;
use crate::util::write_json_file_atomic as write_json_file_atomic_impl;

use super::record::{PilotAuthDidRecord, PilotAuthDidRecordError};
use super::state::PilotAuthDidState;
use super::sync::{PilotAuthDidSyncError, PilotAuthDidSyncRequest, PilotAuthDidSyncResponse};

const GOVERNANCE_DIRNAME: &str = "governance";
const PILOT_AUTH_DID_RECORDS_DIRNAME: &str = "pilot-auth-did-records";

#[derive(Debug, thiserror::Error)]
pub enum GovernanceStoreError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("pilot-auth-did-record: {0}")]
    Record(#[from] PilotAuthDidRecordError),
    #[error("selection: {0}")]
    Selection(#[from] GovernanceSelectionError),
    #[error("sync: {0}")]
    Sync(#[from] PilotAuthDidSyncError),
    #[error("governance record path has no parent directory")]
    MissingParentDirectory,
}

#[derive(Debug, Clone)]
pub struct GovernanceStore {
    root: PathBuf,
}

impl GovernanceStore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn for_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::open(data_dir.as_ref().join(GOVERNANCE_DIRNAME))
    }

    pub fn root_dir(&self) -> &Path {
        &self.root
    }

    pub fn init(&self) -> Result<(), GovernanceStoreError> {
        std::fs::create_dir_all(self.pilot_auth_did_records_root())?;
        Ok(())
    }

    pub fn persist_pilot_auth_did_record(
        &self,
        record: &PilotAuthDidRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.pilot_auth_did_record_path(&record.pilot_id, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_pilot_auth_did_records(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Vec<PilotAuthDidRecord>, GovernanceStoreError> {
        self.init()?;
        let dir = self.pilot_auth_did_pilot_dir(pilot_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut paths = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let file_type = entry.file_type().ok()?;
                if !file_type.is_file() {
                    return None;
                }
                let path = entry.path();
                (path.extension().and_then(|ext| ext.to_str()) == Some("json")).then_some(path)
            })
            .collect::<Vec<_>>();
        paths.sort();

        let mut records = Vec::with_capacity(paths.len());
        for path in paths {
            let record: PilotAuthDidRecord = serde_json::from_slice(&std::fs::read(&path)?)?;
            record.validate()?;
            records.push(record);
        }
        records.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        Ok(records)
    }

    pub fn resolve_pilot_auth_did_state(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PilotAuthDidState, GovernanceStoreError> {
        let records = self.load_pilot_auth_did_records(pilot_id)?;
        Ok(select_pilot_auth_did_state(pilot_id, &records)?)
    }

    pub fn prepare_pilot_auth_did_sync(
        &self,
        request: &PilotAuthDidSyncRequest,
    ) -> Result<PilotAuthDidSyncResponse, GovernanceStoreError> {
        let records = self
            .load_pilot_auth_did_records(&request.pilot_id)?
            .into_iter()
            .filter(|record| !request.knows(&record.record_id))
            .collect::<Vec<_>>();
        Ok(PilotAuthDidSyncResponse::new(
            request.pilot_id.clone(),
            records,
        ))
    }

    pub fn build_pilot_auth_did_sync_request(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PilotAuthDidSyncRequest, GovernanceStoreError> {
        let known_record_ids = self
            .load_pilot_auth_did_records(pilot_id)?
            .into_iter()
            .map(|record| record.record_id)
            .collect();
        Ok(PilotAuthDidSyncRequest::new(
            pilot_id.clone(),
            known_record_ids,
        ))
    }

    pub fn apply_pilot_auth_did_sync(
        &self,
        response: &PilotAuthDidSyncResponse,
    ) -> Result<usize, GovernanceStoreError> {
        response.validate()?;
        let mut applied = 0usize;
        for record in &response.records {
            let path = self.pilot_auth_did_record_path(&record.pilot_id, &record.record_id);
            let existed = path.exists();
            self.persist_pilot_auth_did_record(record)?;
            if !existed {
                applied += 1;
            }
        }
        Ok(applied)
    }

    fn pilot_auth_did_records_root(&self) -> PathBuf {
        self.root.join(PILOT_AUTH_DID_RECORDS_DIRNAME)
    }

    fn pilot_auth_did_pilot_dir(&self, pilot_id: &PilotId) -> PathBuf {
        self.pilot_auth_did_records_root()
            .join(pilot_id.public_key_hex())
    }

    fn pilot_auth_did_record_path(
        &self,
        pilot_id: &PilotId,
        record_id: &crate::id::Blake3Hex,
    ) -> PathBuf {
        self.pilot_auth_did_pilot_dir(pilot_id)
            .join(format!("{record_id}.json"))
    }
}

impl GovernanceLookup for GovernanceStore {
    fn load_pilot_auth_did_records(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Vec<PilotAuthDidRecord>, GovernanceStoreError> {
        self.load_pilot_auth_did_records(pilot_id)
    }

    fn resolve_pilot_auth_did_state(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PilotAuthDidState, GovernanceStoreError> {
        self.resolve_pilot_auth_did_state(pilot_id)
    }
}

fn write_json_file_atomic<T: serde::Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), GovernanceStoreError> {
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
        GovernanceStoreError::MissingParentDirectory,
    )
}

#[cfg(test)]
mod tests {
    use crate::identity::DidKey;
    use crate::{
        PilotAuthDidStateStatus, PilotAuthDidSyncError, governance::sync::PilotAuthDidSyncRequest,
    };

    use super::*;

    fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    fn temp_store() -> (GovernanceStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = GovernanceStore::for_data_dir(dir.path());
        store.init().unwrap();
        (store, dir)
    }

    #[test]
    fn persist_and_load_round_trip() {
        let (store, _dir) = temp_store();
        let record = PilotAuthDidRecord::issue(
            &deterministic_secret_key(91),
            DidKey::from_public_key(deterministic_secret_key(92).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();

        store.persist_pilot_auth_did_record(&record).unwrap();
        let loaded = store.load_pilot_auth_did_records(&record.pilot_id).unwrap();
        assert_eq!(loaded, vec![record]);
    }

    #[test]
    fn rauth_08_state_resolution_distinguishes_absent_and_authoritative() {
        let (store, _dir) = temp_store();
        let pilot_id = PilotId::from_public_key(deterministic_secret_key(101).public());
        let absent = store.resolve_pilot_auth_did_state(&pilot_id).unwrap();
        assert_eq!(
            absent.status(),
            super::super::state::PilotAuthDidStateStatus::Absent
        );

        let record = PilotAuthDidRecord::issue(
            &deterministic_secret_key(101),
            DidKey::from_public_key(deterministic_secret_key(102).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        store.persist_pilot_auth_did_record(&record).unwrap();

        let authoritative = store
            .resolve_pilot_auth_did_state(&record.pilot_id)
            .unwrap();
        assert_eq!(
            authoritative.status(),
            super::super::state::PilotAuthDidStateStatus::Authoritative
        );
    }

    #[test]
    fn simulated_crash_temp_file_does_not_corrupt_previous_state() {
        let (store, _dir) = temp_store();
        let record = PilotAuthDidRecord::issue(
            &deterministic_secret_key(111),
            DidKey::from_public_key(deterministic_secret_key(112).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        store.persist_pilot_auth_did_record(&record).unwrap();

        let pilot_dir = store.pilot_auth_did_pilot_dir(&record.pilot_id);
        std::fs::create_dir_all(&pilot_dir).unwrap();
        std::fs::write(pilot_dir.join(".current.json.tmp-crash"), b"{bad json").unwrap();

        let loaded = store.load_pilot_auth_did_records(&record.pilot_id).unwrap();
        assert_eq!(loaded, vec![record]);
    }

    #[test]
    fn rgsync_02_restart_after_missing_rotation_can_recover_authoritative_state() {
        let (publisher, _publisher_dir) = temp_store();
        let (rejoining, _rejoining_dir) = temp_store();

        let pilot_root = deterministic_secret_key(121);
        let initial = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(122).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let rotated = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(123).public()),
            Some(initial.record_id.clone()),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        publisher.persist_pilot_auth_did_record(&initial).unwrap();
        publisher.persist_pilot_auth_did_record(&rotated).unwrap();
        rejoining.persist_pilot_auth_did_record(&initial).unwrap();

        let before = rejoining
            .resolve_pilot_auth_did_state(&initial.pilot_id)
            .unwrap();
        assert_eq!(before.status(), PilotAuthDidStateStatus::Authoritative);
        assert_eq!(
            before.authoritative.as_ref().unwrap().record_id,
            initial.record_id
        );

        let request = PilotAuthDidSyncRequest::from_state(&before);
        let response = publisher.prepare_pilot_auth_did_sync(&request).unwrap();
        assert_eq!(response.records, vec![rotated.clone()]);

        let applied = rejoining.apply_pilot_auth_did_sync(&response).unwrap();
        assert_eq!(applied, 1);

        let after = rejoining
            .resolve_pilot_auth_did_state(&initial.pilot_id)
            .unwrap();
        assert_eq!(after.status(), PilotAuthDidStateStatus::Authoritative);
        assert_eq!(after.authoritative.unwrap().record_id, rotated.record_id);
    }

    #[test]
    fn rauth_12_partial_chain_state_is_tentative_until_catch_up_completes() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(131);
        let initial = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(132).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let rotated = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(133).public()),
            Some(initial.record_id.clone()),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        store.persist_pilot_auth_did_record(&rotated).unwrap();
        let state = store
            .resolve_pilot_auth_did_state(&rotated.pilot_id)
            .unwrap();

        assert_eq!(state.status(), PilotAuthDidStateStatus::Tentative);
        assert!(state.requires_catch_up());
        assert_eq!(state.tentative_record_ids, vec![rotated.record_id]);
    }

    #[test]
    fn rgsync_03_completed_catch_up_upgrades_tentative_state_to_authoritative() {
        let (authority, _authority_dir) = temp_store();
        let (rejoining, _rejoining_dir) = temp_store();

        let pilot_root = deterministic_secret_key(141);
        let initial = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(142).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let rotated = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(143).public()),
            Some(initial.record_id.clone()),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        authority.persist_pilot_auth_did_record(&initial).unwrap();
        authority.persist_pilot_auth_did_record(&rotated).unwrap();
        rejoining.persist_pilot_auth_did_record(&rotated).unwrap();

        let before = rejoining
            .resolve_pilot_auth_did_state(&rotated.pilot_id)
            .unwrap();
        assert_eq!(before.status(), PilotAuthDidStateStatus::Tentative);

        let request = PilotAuthDidSyncRequest::from_state(&before);
        let response = authority.prepare_pilot_auth_did_sync(&request).unwrap();
        assert_eq!(response.records, vec![initial.clone()]);

        let applied = rejoining.apply_pilot_auth_did_sync(&response).unwrap();
        assert_eq!(applied, 1);

        let after = rejoining
            .resolve_pilot_auth_did_state(&rotated.pilot_id)
            .unwrap();
        assert_eq!(after.status(), PilotAuthDidStateStatus::Authoritative);
        assert_eq!(after.authoritative.unwrap().record_id, rotated.record_id);
    }

    #[test]
    fn apply_sync_rejects_mixed_pilot_batches() {
        let (store, _dir) = temp_store();
        let pilot_a = deterministic_secret_key(151);
        let pilot_b = deterministic_secret_key(152);
        let record = PilotAuthDidRecord::issue(
            &pilot_b,
            DidKey::from_public_key(deterministic_secret_key(153).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let response = PilotAuthDidSyncResponse::new(
            PilotId::from_public_key(pilot_a.public()),
            vec![record.clone()],
        );

        let err = store.apply_pilot_auth_did_sync(&response).unwrap_err();
        assert!(matches!(
            err,
            GovernanceStoreError::Sync(PilotAuthDidSyncError::MixedPilotRecord {
                expected,
                found,
                record_id
            }) if expected == PilotId::from_public_key(pilot_a.public())
                && found == record.pilot_id
                && record_id == record.record_id
        ));
    }

    #[test]
    fn build_sync_request_uses_full_local_history() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(161);
        let initial = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(162).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let rotated = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(163).public()),
            Some(initial.record_id.clone()),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        store.persist_pilot_auth_did_record(&initial).unwrap();
        store.persist_pilot_auth_did_record(&rotated).unwrap();

        let request = store
            .build_pilot_auth_did_sync_request(&initial.pilot_id)
            .unwrap();
        let mut expected = vec![initial.record_id, rotated.record_id];
        expected.sort();
        assert_eq!(request.known_record_ids, expected);
    }
}
