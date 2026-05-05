use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};

use crate::governance::lookup::GovernanceLookup;
use crate::governance::selection::{
    GovernanceSelectionError, select_pilot_auth_did_state, select_private_access_rotation_state,
};
use crate::id::{Blake3Hex, PilotId};
use crate::util::write_json_file_atomic as write_json_file_atomic_impl;

use super::record::{
    ClaimApprovalRecord, ClaimChallengeRecord, ClaimResolutionOutcome, ClaimResolutionRecord,
    DeletionRequestRecord, FlightGovernanceRecordError, IdentityRecoveryRecord, OwnerClaimRecord,
    PilotAuthDidRecord, PilotAuthDidRecordError, PrivateAccessRotationRecord,
    PrivateAccessRotationRecordError, PublicationModeRecord, RosterUpdateAction,
    RosterUpdateRecord,
};
use super::state::{
    FlightGovernanceState, FlightGovernanceStatus, PilotAuthDidState, PrivateAccessRotationState,
};
use super::sync::{
    GovernanceRecord, GovernanceRecordParseError, PilotAuthDidSyncError, PilotAuthDidSyncRequest,
    PilotAuthDidSyncResponse,
};

const GOVERNANCE_DIRNAME: &str = "governance";
const PILOT_AUTH_DID_RECORDS_DIRNAME: &str = "pilot-auth-did-records";
const PRIVATE_ACCESS_ROTATION_RECORDS_DIRNAME: &str = "private-access-rotation-records";
const FLIGHT_GOVERNANCE_STATES_DIRNAME: &str = "flight-governance-states";
const OWNER_CLAIM_RECORDS_DIRNAME: &str = "owner-claims";
const CLAIM_APPROVAL_RECORDS_DIRNAME: &str = "claim-approvals";
const CLAIM_CHALLENGE_RECORDS_DIRNAME: &str = "claim-challenges";
const CLAIM_RESOLUTION_RECORDS_DIRNAME: &str = "claim-resolutions";
const IDENTITY_RECOVERY_RECORDS_DIRNAME: &str = "identity-recoveries";
const ROSTER_UPDATE_RECORDS_DIRNAME: &str = "roster-updates";
const PUBLICATION_MODE_RECORDS_DIRNAME: &str = "publication-modes";
const DELETION_REQUEST_RECORDS_DIRNAME: &str = "deletion-requests";
const TRUSTED_RESOLVERS_FILENAME: &str = "trusted-resolvers.ndjson";

#[derive(Debug, thiserror::Error)]
pub enum GovernanceStoreError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("time parse: {0}")]
    TimeParse(#[from] chrono::ParseError),
    #[error("pilot-auth-did-record: {0}")]
    Record(#[from] PilotAuthDidRecordError),
    #[error("private-access-rotation-record: {0}")]
    PrivateAccessRotationRecord(#[from] PrivateAccessRotationRecordError),
    #[error("flight governance record: {0}")]
    FlightGovernanceRecord(#[from] FlightGovernanceRecordError),
    #[error("selection: {0}")]
    Selection(#[from] GovernanceSelectionError),
    #[error("sync: {0}")]
    Sync(#[from] PilotAuthDidSyncError),
    #[error("governance record parse: {0}")]
    GovernanceRecordParse(#[from] GovernanceRecordParseError),
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
        std::fs::create_dir_all(self.private_access_rotation_records_root())?;
        std::fs::create_dir_all(self.flight_governance_states_root())?;
        std::fs::create_dir_all(self.owner_claim_records_root())?;
        std::fs::create_dir_all(self.claim_approval_records_root())?;
        std::fs::create_dir_all(self.claim_challenge_records_root())?;
        std::fs::create_dir_all(self.claim_resolution_records_root())?;
        std::fs::create_dir_all(self.identity_recovery_records_root())?;
        std::fs::create_dir_all(self.roster_update_records_root())?;
        std::fs::create_dir_all(self.publication_mode_records_root())?;
        std::fs::create_dir_all(self.deletion_request_records_root())?;
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

    pub fn persist_private_access_rotation_record(
        &self,
        record: &PrivateAccessRotationRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.private_access_rotation_record_path(&record.pilot_id, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_private_access_rotation_records(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Vec<PrivateAccessRotationRecord>, GovernanceStoreError> {
        self.init()?;
        let dir = self.private_access_rotation_pilot_dir(pilot_id);
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
            let record: PrivateAccessRotationRecord =
                serde_json::from_slice(&std::fs::read(&path)?)?;
            record.validate()?;
            records.push(record);
        }
        records.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        Ok(records)
    }

    pub fn resolve_private_access_rotation_state(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PrivateAccessRotationState, GovernanceStoreError> {
        let records = self.load_private_access_rotation_records(pilot_id)?;
        Ok(select_private_access_rotation_state(pilot_id, &records)?)
    }

    pub fn persist_flight_governance_state(
        &self,
        state: &FlightGovernanceState,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        let path = self.flight_governance_state_path(&state.raw_igc_hash);
        write_json_file_atomic(&path, state)
    }

    pub fn load_flight_governance_state(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Option<FlightGovernanceState>, GovernanceStoreError> {
        self.init()?;
        let path = self.flight_governance_state_path(raw_igc_hash);
        if !path.exists() {
            return Ok(None);
        }
        let state: FlightGovernanceState = serde_json::from_slice(&std::fs::read(&path)?)?;
        Ok(Some(state))
    }

    pub fn persist_owner_claim_record(
        &self,
        record: &OwnerClaimRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.owner_claim_record_path(&record.raw_igc_hash, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_owner_claim_records(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Vec<OwnerClaimRecord>, GovernanceStoreError> {
        self.load_flight_record_dir(self.owner_claim_flight_dir(raw_igc_hash))
    }

    pub fn persist_claim_approval_record(
        &self,
        record: &ClaimApprovalRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.claim_approval_record_path(&record.raw_igc_hash, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_claim_approval_records(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Vec<ClaimApprovalRecord>, GovernanceStoreError> {
        self.load_flight_record_dir(self.claim_approval_flight_dir(raw_igc_hash))
    }

    pub fn persist_claim_challenge_record(
        &self,
        record: &ClaimChallengeRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.claim_challenge_record_path(&record.raw_igc_hash, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_claim_challenge_records(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Vec<ClaimChallengeRecord>, GovernanceStoreError> {
        self.load_flight_record_dir(self.claim_challenge_flight_dir(raw_igc_hash))
    }

    pub fn persist_claim_resolution_record(
        &self,
        record: &ClaimResolutionRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.claim_resolution_record_path(&record.raw_igc_hash, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_claim_resolution_records(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Vec<ClaimResolutionRecord>, GovernanceStoreError> {
        self.load_flight_record_dir(self.claim_resolution_flight_dir(raw_igc_hash))
    }

    pub fn persist_identity_recovery_record(
        &self,
        record: &IdentityRecoveryRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.identity_recovery_record_path(&record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_identity_recovery_records(
        &self,
    ) -> Result<Vec<IdentityRecoveryRecord>, GovernanceStoreError> {
        self.init()?;
        let dir = self.identity_recovery_records_root();
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
            let record: IdentityRecoveryRecord = serde_json::from_slice(&std::fs::read(&path)?)?;
            record.validate()?;
            records.push(record);
        }
        records.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        Ok(records)
    }

    pub fn trust_resolver(&self, resolver_id: &str) -> Result<(), GovernanceStoreError> {
        self.init()?;
        validate_trusted_resolver_id(resolver_id)?;
        let mut trusted = self.load_trusted_resolvers()?;
        if !trusted.insert(resolver_id.to_string()) {
            return Ok(());
        }
        let path = self.trusted_resolvers_path();
        let mut rows = trusted.into_iter().collect::<Vec<_>>();
        rows.sort();
        let contents = rows
            .into_iter()
            .map(|resolver_id| serde_json::json!({ "resolver_id": resolver_id }).to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(path, format!("{contents}\n"))?;
        Ok(())
    }

    pub fn load_trusted_resolvers(&self) -> Result<HashSet<String>, GovernanceStoreError> {
        self.init()?;
        let path = self.trusted_resolvers_path();
        if !path.exists() {
            return Ok(HashSet::new());
        }
        let mut trusted = HashSet::new();
        for line in std::fs::read_to_string(path)?.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row: TrustedResolverRow = serde_json::from_str(line)?;
            validate_trusted_resolver_id(&row.resolver_id)?;
            trusted.insert(row.resolver_id);
        }
        Ok(trusted)
    }

    pub fn persist_roster_update_record(
        &self,
        record: &RosterUpdateRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.roster_update_record_path(&record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_roster_update_records(
        &self,
    ) -> Result<Vec<RosterUpdateRecord>, GovernanceStoreError> {
        self.init()?;
        let dir = self.roster_update_records_root();
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
            let record: RosterUpdateRecord = serde_json::from_slice(&std::fs::read(&path)?)?;
            record.validate()?;
            records.push(record);
        }
        records.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        Ok(records)
    }

    pub fn persist_publication_mode_record(
        &self,
        record: &PublicationModeRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.publication_mode_record_path(&record.raw_igc_hash, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_publication_mode_records(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Vec<PublicationModeRecord>, GovernanceStoreError> {
        self.load_flight_record_dir(self.publication_mode_flight_dir(raw_igc_hash))
    }

    pub fn resolve_publication_mode_record(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Option<PublicationModeRecord>, GovernanceStoreError> {
        let records = self.load_publication_mode_records(raw_igc_hash)?;
        Ok(select_publication_mode_tip(&records))
    }

    pub fn effective_trusted_resolvers(&self) -> Result<HashSet<String>, GovernanceStoreError> {
        let mut trusted = self.load_trusted_resolvers()?;
        for record in self.load_roster_update_records()? {
            if !trusted.contains(&record.signer_id) {
                continue;
            }
            match record.action {
                RosterUpdateAction::Add => {
                    trusted.insert(record.resolver_id);
                }
                RosterUpdateAction::Remove => {
                    trusted.remove(&record.resolver_id);
                }
            }
        }
        Ok(trusted)
    }

    pub fn persist_deletion_request_record(
        &self,
        record: &DeletionRequestRecord,
    ) -> Result<(), GovernanceStoreError> {
        self.init()?;
        record.validate()?;
        let path = self.deletion_request_record_path(&record.raw_igc_hash, &record.record_id);
        if path.exists() {
            return Ok(());
        }
        write_json_file_atomic(&path, record)
    }

    pub fn load_deletion_request_records(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Vec<DeletionRequestRecord>, GovernanceStoreError> {
        self.load_flight_record_dir(self.deletion_request_flight_dir(raw_igc_hash))
    }

    pub fn resolve_flight_governance_state(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Option<FlightGovernanceState>, GovernanceStoreError> {
        self.resolve_flight_governance_state_at(raw_igc_hash, &crate::util::canonical_utc_now())
    }

    pub fn resolve_flight_governance_state_at(
        &self,
        raw_igc_hash: &Blake3Hex,
        now: &str,
    ) -> Result<Option<FlightGovernanceState>, GovernanceStoreError> {
        let now = DateTime::parse_from_rfc3339(now)?.with_timezone(&Utc);
        let applied = self.load_flight_governance_state(raw_igc_hash)?;
        let trusted_resolvers = self.effective_trusted_resolvers()?;
        let claims = self.load_owner_claim_records(raw_igc_hash)?;
        let approvals = self
            .load_claim_approval_records(raw_igc_hash)?
            .into_iter()
            .filter(|record| trusted_resolvers.contains(&record.resolver_id))
            .collect::<Vec<_>>();
        let challenges = self
            .load_claim_challenge_records(raw_igc_hash)?
            .into_iter()
            .filter(|record| trusted_resolvers.contains(&record.challenger_resolver_id))
            .collect::<Vec<_>>();
        let resolutions = self
            .load_claim_resolution_records(raw_igc_hash)?
            .into_iter()
            .filter(|record| trusted_resolvers.contains(&record.resolver_id))
            .collect::<Vec<_>>();
        let identity_recoveries = self
            .load_identity_recovery_records()?
            .into_iter()
            .filter(|record| trusted_resolvers.contains(&record.resolver_id))
            .collect::<Vec<_>>();
        let resolved = resolve_raw_flight_governance(
            raw_igc_hash,
            &claims,
            &approvals,
            &challenges,
            &resolutions,
            &identity_recoveries,
            now,
        );
        let deletion_requests = self.load_deletion_request_records(raw_igc_hash)?;
        if let Some(state) =
            apply_deletion_requests(resolved.clone().or(applied.clone()), &deletion_requests)
        {
            return Ok(Some(state));
        }

        if resolved.is_some() {
            return Ok(resolved);
        }

        Ok(applied)
    }

    pub fn apply_governance_record(
        &self,
        record: &GovernanceRecord,
    ) -> Result<bool, GovernanceStoreError> {
        self.init()?;
        match record {
            GovernanceRecord::OwnerClaim(record) => {
                let path = self.owner_claim_record_path(&record.raw_igc_hash, &record.record_id);
                let existed = path.exists();
                self.persist_owner_claim_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::ClaimApproval(record) => {
                let path = self.claim_approval_record_path(&record.raw_igc_hash, &record.record_id);
                let existed = path.exists();
                self.persist_claim_approval_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::ClaimChallenge(record) => {
                let path =
                    self.claim_challenge_record_path(&record.raw_igc_hash, &record.record_id);
                let existed = path.exists();
                self.persist_claim_challenge_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::ClaimResolution(record) => {
                let path =
                    self.claim_resolution_record_path(&record.raw_igc_hash, &record.record_id);
                let existed = path.exists();
                self.persist_claim_resolution_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::IdentityRecovery(record) => {
                let path = self.identity_recovery_record_path(&record.record_id);
                let existed = path.exists();
                self.persist_identity_recovery_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::DeletionRequest(record) => {
                let path =
                    self.deletion_request_record_path(&record.raw_igc_hash, &record.record_id);
                let existed = path.exists();
                self.persist_deletion_request_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::PrivateAccessRotation(record) => {
                let path =
                    self.private_access_rotation_record_path(&record.pilot_id, &record.record_id);
                let existed = path.exists();
                self.persist_private_access_rotation_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::PilotAuthDid(record) => {
                let path = self.pilot_auth_did_record_path(&record.pilot_id, &record.record_id);
                let existed = path.exists();
                self.persist_pilot_auth_did_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::RosterUpdate(record) => {
                let path = self.roster_update_record_path(&record.record_id);
                let existed = path.exists();
                self.persist_roster_update_record(record)?;
                Ok(!existed)
            }
            GovernanceRecord::PublicationMode(record) => {
                let path =
                    self.publication_mode_record_path(&record.raw_igc_hash, &record.record_id);
                let existed = path.exists();
                self.persist_publication_mode_record(record)?;
                Ok(!existed)
            }
        }
    }

    pub fn apply_governance_record_json(&self, bytes: &[u8]) -> Result<bool, GovernanceStoreError> {
        let record = GovernanceRecord::from_slice(bytes)?;
        self.apply_governance_record(&record)
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

    fn private_access_rotation_records_root(&self) -> PathBuf {
        self.root.join(PRIVATE_ACCESS_ROTATION_RECORDS_DIRNAME)
    }

    fn flight_governance_states_root(&self) -> PathBuf {
        self.root.join(FLIGHT_GOVERNANCE_STATES_DIRNAME)
    }

    fn owner_claim_records_root(&self) -> PathBuf {
        self.root.join(OWNER_CLAIM_RECORDS_DIRNAME)
    }

    fn claim_approval_records_root(&self) -> PathBuf {
        self.root.join(CLAIM_APPROVAL_RECORDS_DIRNAME)
    }

    fn claim_challenge_records_root(&self) -> PathBuf {
        self.root.join(CLAIM_CHALLENGE_RECORDS_DIRNAME)
    }

    fn claim_resolution_records_root(&self) -> PathBuf {
        self.root.join(CLAIM_RESOLUTION_RECORDS_DIRNAME)
    }

    fn identity_recovery_records_root(&self) -> PathBuf {
        self.root.join(IDENTITY_RECOVERY_RECORDS_DIRNAME)
    }

    fn roster_update_records_root(&self) -> PathBuf {
        self.root.join(ROSTER_UPDATE_RECORDS_DIRNAME)
    }

    fn publication_mode_records_root(&self) -> PathBuf {
        self.root.join(PUBLICATION_MODE_RECORDS_DIRNAME)
    }

    fn deletion_request_records_root(&self) -> PathBuf {
        self.root.join(DELETION_REQUEST_RECORDS_DIRNAME)
    }

    fn trusted_resolvers_path(&self) -> PathBuf {
        self.root.join(TRUSTED_RESOLVERS_FILENAME)
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

    fn private_access_rotation_pilot_dir(&self, pilot_id: &PilotId) -> PathBuf {
        self.private_access_rotation_records_root()
            .join(pilot_id.public_key_hex())
    }

    fn private_access_rotation_record_path(
        &self,
        pilot_id: &PilotId,
        record_id: &crate::id::Blake3Hex,
    ) -> PathBuf {
        self.private_access_rotation_pilot_dir(pilot_id)
            .join(format!("{record_id}.json"))
    }

    fn flight_governance_state_path(&self, raw_igc_hash: &Blake3Hex) -> PathBuf {
        self.flight_governance_states_root()
            .join(&raw_igc_hash.as_str()[..2])
            .join(format!("{raw_igc_hash}.json"))
    }

    fn owner_claim_flight_dir(&self, raw_igc_hash: &Blake3Hex) -> PathBuf {
        self.owner_claim_records_root()
            .join(&raw_igc_hash.as_str()[..2])
            .join(raw_igc_hash.as_str())
    }

    fn owner_claim_record_path(&self, raw_igc_hash: &Blake3Hex, record_id: &Blake3Hex) -> PathBuf {
        self.owner_claim_flight_dir(raw_igc_hash)
            .join(format!("{record_id}.json"))
    }

    fn claim_approval_flight_dir(&self, raw_igc_hash: &Blake3Hex) -> PathBuf {
        self.claim_approval_records_root()
            .join(&raw_igc_hash.as_str()[..2])
            .join(raw_igc_hash.as_str())
    }

    fn claim_approval_record_path(
        &self,
        raw_igc_hash: &Blake3Hex,
        record_id: &Blake3Hex,
    ) -> PathBuf {
        self.claim_approval_flight_dir(raw_igc_hash)
            .join(format!("{record_id}.json"))
    }

    fn claim_challenge_flight_dir(&self, raw_igc_hash: &Blake3Hex) -> PathBuf {
        self.claim_challenge_records_root()
            .join(&raw_igc_hash.as_str()[..2])
            .join(raw_igc_hash.as_str())
    }

    fn claim_challenge_record_path(
        &self,
        raw_igc_hash: &Blake3Hex,
        record_id: &Blake3Hex,
    ) -> PathBuf {
        self.claim_challenge_flight_dir(raw_igc_hash)
            .join(format!("{record_id}.json"))
    }

    fn claim_resolution_flight_dir(&self, raw_igc_hash: &Blake3Hex) -> PathBuf {
        self.claim_resolution_records_root()
            .join(&raw_igc_hash.as_str()[..2])
            .join(raw_igc_hash.as_str())
    }

    fn claim_resolution_record_path(
        &self,
        raw_igc_hash: &Blake3Hex,
        record_id: &Blake3Hex,
    ) -> PathBuf {
        self.claim_resolution_flight_dir(raw_igc_hash)
            .join(format!("{record_id}.json"))
    }

    fn identity_recovery_record_path(&self, record_id: &Blake3Hex) -> PathBuf {
        self.identity_recovery_records_root()
            .join(format!("{record_id}.json"))
    }

    fn roster_update_record_path(&self, record_id: &Blake3Hex) -> PathBuf {
        self.roster_update_records_root()
            .join(format!("{record_id}.json"))
    }

    fn publication_mode_flight_dir(&self, raw_igc_hash: &Blake3Hex) -> PathBuf {
        self.publication_mode_records_root()
            .join(&raw_igc_hash.as_str()[..2])
            .join(raw_igc_hash.as_str())
    }

    fn publication_mode_record_path(
        &self,
        raw_igc_hash: &Blake3Hex,
        record_id: &Blake3Hex,
    ) -> PathBuf {
        self.publication_mode_flight_dir(raw_igc_hash)
            .join(format!("{record_id}.json"))
    }

    fn deletion_request_flight_dir(&self, raw_igc_hash: &Blake3Hex) -> PathBuf {
        self.deletion_request_records_root()
            .join(&raw_igc_hash.as_str()[..2])
            .join(raw_igc_hash.as_str())
    }

    fn deletion_request_record_path(
        &self,
        raw_igc_hash: &Blake3Hex,
        record_id: &Blake3Hex,
    ) -> PathBuf {
        self.deletion_request_flight_dir(raw_igc_hash)
            .join(format!("{record_id}.json"))
    }

    fn load_flight_record_dir<T>(&self, dir: PathBuf) -> Result<Vec<T>, GovernanceStoreError>
    where
        T: serde::de::DeserializeOwned + ValidatedFlightGovernanceRecord,
    {
        self.init()?;
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
            let record: T = serde_json::from_slice(&std::fs::read(&path)?)?;
            record.validate_flight_governance_record()?;
            records.push(record);
        }
        records.sort_by(|left, right| left.record_id().cmp(right.record_id()));
        Ok(records)
    }
}

trait ValidatedFlightGovernanceRecord {
    fn validate_flight_governance_record(&self) -> Result<(), FlightGovernanceRecordError>;
    fn record_id(&self) -> &Blake3Hex;
}

impl ValidatedFlightGovernanceRecord for OwnerClaimRecord {
    fn validate_flight_governance_record(&self) -> Result<(), FlightGovernanceRecordError> {
        self.validate()
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }
}

impl ValidatedFlightGovernanceRecord for ClaimApprovalRecord {
    fn validate_flight_governance_record(&self) -> Result<(), FlightGovernanceRecordError> {
        self.validate()
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }
}

impl ValidatedFlightGovernanceRecord for ClaimChallengeRecord {
    fn validate_flight_governance_record(&self) -> Result<(), FlightGovernanceRecordError> {
        self.validate()
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }
}

impl ValidatedFlightGovernanceRecord for ClaimResolutionRecord {
    fn validate_flight_governance_record(&self) -> Result<(), FlightGovernanceRecordError> {
        self.validate()
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }
}

impl ValidatedFlightGovernanceRecord for DeletionRequestRecord {
    fn validate_flight_governance_record(&self) -> Result<(), FlightGovernanceRecordError> {
        self.validate()
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }
}

impl ValidatedFlightGovernanceRecord for PublicationModeRecord {
    fn validate_flight_governance_record(&self) -> Result<(), FlightGovernanceRecordError> {
        self.validate()
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }
}

#[derive(serde::Deserialize)]
struct TrustedResolverRow {
    resolver_id: String,
}

fn apply_deletion_requests(
    applied: Option<FlightGovernanceState>,
    deletion_requests: &[DeletionRequestRecord],
) -> Option<FlightGovernanceState> {
    let mut state = applied?;
    if state.status == FlightGovernanceStatus::Deleted {
        return Some(state);
    }

    let owner = state.owner_pilot_id.as_ref()?;
    if deletion_requests
        .iter()
        .any(|request| &request.pilot_id == owner)
    {
        state.status = FlightGovernanceStatus::Deleted;
        state.baseline_ready = true;
        if let Some(latest_deletion) = deletion_requests
            .iter()
            .filter(|request| &request.pilot_id == owner)
            .max_by(|left, right| left.record_id.cmp(&right.record_id))
        {
            state.recorded_at = latest_deletion.created_at.clone();
        }
    }

    Some(state)
}

fn resolve_raw_flight_governance(
    raw_igc_hash: &Blake3Hex,
    claims: &[OwnerClaimRecord],
    approvals: &[ClaimApprovalRecord],
    challenges: &[ClaimChallengeRecord],
    resolutions: &[ClaimResolutionRecord],
    identity_recoveries: &[IdentityRecoveryRecord],
    now: DateTime<Utc>,
) -> Option<FlightGovernanceState> {
    let claim_by_id = claims
        .iter()
        .map(|claim| (claim.record_id.clone(), claim))
        .collect::<HashMap<_, _>>();

    if let Some(mut state) = resolve_active_resolution(raw_igc_hash, &claim_by_id, resolutions) {
        apply_identity_recoveries(&mut state, identity_recoveries);
        return Some(state);
    }

    let approved_claims = approvals
        .iter()
        .filter_map(|approval| claim_by_id.get(&approval.claim_record_id).copied())
        .collect::<Vec<_>>();
    let mut approved_owners = approved_claims
        .iter()
        .map(|claim| claim.pilot_id.clone())
        .collect::<Vec<_>>();
    approved_owners.sort();
    approved_owners.dedup();

    if approved_owners.len() > 1 {
        let mut state = FlightGovernanceState {
            raw_igc_hash: raw_igc_hash.clone(),
            owner_pilot_id: None,
            status: FlightGovernanceStatus::Contested,
            baseline_ready: true,
            recorded_at: approvals
                .iter()
                .map(|approval| approval.created_at.as_str())
                .max()
                .unwrap_or_default()
                .to_string(),
        };
        apply_identity_recoveries(&mut state, identity_recoveries);
        return Some(state);
    }

    if let Some(claim) = approved_claims.first() {
        let active_challenges = challenges
            .iter()
            .filter(|challenge| challenge.claim_record_id == claim.record_id)
            .filter(|challenge| !challenge_expired(challenge, now))
            .collect::<Vec<_>>();
        if !active_challenges.is_empty() {
            let mut state = FlightGovernanceState {
                raw_igc_hash: raw_igc_hash.clone(),
                owner_pilot_id: Some(claim.pilot_id.clone()),
                status: FlightGovernanceStatus::Contested,
                baseline_ready: true,
                recorded_at: active_challenges
                    .iter()
                    .map(|challenge| challenge.created_at.as_str())
                    .max()
                    .unwrap_or_default()
                    .to_string(),
            };
            apply_identity_recoveries(&mut state, identity_recoveries);
            return Some(state);
        }
    }

    if let Some(claim) = approved_claims.first() {
        let mut state = FlightGovernanceState::approved_owner(
            raw_igc_hash.clone(),
            claim.pilot_id.clone(),
            approvals
                .iter()
                .filter(|approval| approval.claim_record_id == claim.record_id)
                .map(|approval| approval.created_at.as_str())
                .max()
                .unwrap_or_default(),
        );
        apply_identity_recoveries(&mut state, identity_recoveries);
        return Some(state);
    }

    let mut state = resolve_pending_owner_claim(raw_igc_hash, claims)?;
    apply_identity_recoveries(&mut state, identity_recoveries);
    Some(state)
}

fn challenge_expired(challenge: &ClaimChallengeRecord, now: DateTime<Utc>) -> bool {
    let Ok(created_at) = DateTime::parse_from_rfc3339(&challenge.created_at) else {
        return false;
    };
    now >= created_at.with_timezone(&Utc) + Duration::days(15)
}

fn apply_identity_recoveries(
    state: &mut FlightGovernanceState,
    identity_recoveries: &[IdentityRecoveryRecord],
) {
    if state.status != FlightGovernanceStatus::Approved {
        return;
    }
    let Some(owner) = state.owner_pilot_id.as_mut() else {
        return;
    };
    let Some(recovery) = identity_recoveries
        .iter()
        .filter(|record| &record.old_pilot_id == owner)
        .min_by(|left, right| left.record_id.cmp(&right.record_id))
    else {
        return;
    };
    *owner = recovery.new_pilot_id.clone();
    state.recorded_at = recovery.created_at.clone();
}

fn resolve_active_resolution(
    raw_igc_hash: &Blake3Hex,
    claim_by_id: &HashMap<Blake3Hex, &OwnerClaimRecord>,
    resolutions: &[ClaimResolutionRecord],
) -> Option<FlightGovernanceState> {
    let superseded = resolutions
        .iter()
        .flat_map(|resolution| resolution.supersedes.iter().cloned())
        .collect::<HashSet<_>>();
    let active = resolutions
        .iter()
        .filter(|resolution| !superseded.contains(&resolution.record_id))
        .min_by(|left, right| left.record_id.cmp(&right.record_id))?;
    let claim = claim_by_id.get(&active.claim_record_id)?;
    let status = match active.resolution {
        ClaimResolutionOutcome::Approved => FlightGovernanceStatus::Approved,
        ClaimResolutionOutcome::Rejected => FlightGovernanceStatus::Rejected,
        ClaimResolutionOutcome::Superseded => FlightGovernanceStatus::Superseded,
        ClaimResolutionOutcome::Revoked => FlightGovernanceStatus::Revoked,
    };
    Some(FlightGovernanceState {
        raw_igc_hash: raw_igc_hash.clone(),
        owner_pilot_id: Some(claim.pilot_id.clone()),
        status,
        baseline_ready: true,
        recorded_at: active.created_at.clone(),
    })
}

fn resolve_pending_owner_claim(
    raw_igc_hash: &Blake3Hex,
    claims: &[OwnerClaimRecord],
) -> Option<FlightGovernanceState> {
    match claims {
        [] => None,
        [claim] => Some(FlightGovernanceState {
            raw_igc_hash: raw_igc_hash.clone(),
            owner_pilot_id: Some(claim.pilot_id.clone()),
            status: FlightGovernanceStatus::Pending,
            baseline_ready: true,
            recorded_at: claim.created_at.clone(),
        }),
        _ => Some(FlightGovernanceState {
            raw_igc_hash: raw_igc_hash.clone(),
            owner_pilot_id: None,
            status: FlightGovernanceStatus::Pending,
            baseline_ready: true,
            recorded_at: claims
                .iter()
                .map(|claim| claim.created_at.as_str())
                .max()
                .unwrap_or_default()
                .to_string(),
        }),
    }
}

fn validate_trusted_resolver_id(resolver_id: &str) -> Result<(), GovernanceStoreError> {
    if resolver_id.len() == 64
        && resolver_id
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Ok(());
    }
    Err(GovernanceStoreError::FlightGovernanceRecord(
        FlightGovernanceRecordError::ResolverIdEncoding(resolver_id.to_string()),
    ))
}

fn select_publication_mode_tip(records: &[PublicationModeRecord]) -> Option<PublicationModeRecord> {
    let by_id = records
        .iter()
        .map(|record| (record.record_id.clone(), record))
        .collect::<HashMap<_, _>>();
    records
        .iter()
        .filter(|record| publication_mode_chain_complete(record, &by_id))
        .filter(|record| {
            !records
                .iter()
                .any(|other| other.supersedes.as_ref() == Some(&record.record_id))
        })
        .min_by(|left, right| left.record_id.cmp(&right.record_id))
        .cloned()
}

fn publication_mode_chain_complete(
    record: &PublicationModeRecord,
    by_id: &HashMap<Blake3Hex, &PublicationModeRecord>,
) -> bool {
    let mut seen = HashSet::new();
    let mut current = record;
    while let Some(parent_id) = &current.supersedes {
        if !seen.insert(parent_id.clone()) {
            return false;
        }
        let Some(parent) = by_id.get(parent_id).copied() else {
            return false;
        };
        current = parent;
    }
    true
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

    fn load_private_access_rotation_records(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Vec<PrivateAccessRotationRecord>, GovernanceStoreError> {
        self.load_private_access_rotation_records(pilot_id)
    }

    fn resolve_private_access_rotation_state(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PrivateAccessRotationState, GovernanceStoreError> {
        self.resolve_private_access_rotation_state(pilot_id)
    }

    fn load_flight_governance_state(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Option<FlightGovernanceState>, GovernanceStoreError> {
        self.load_flight_governance_state(raw_igc_hash)
    }

    fn resolve_flight_governance_state(
        &self,
        raw_igc_hash: &Blake3Hex,
    ) -> Result<Option<FlightGovernanceState>, GovernanceStoreError> {
        self.resolve_flight_governance_state(raw_igc_hash)
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
    use crate::{
        ClaimApprovalRecord, ClaimChallengeRecord, ClaimResolutionOutcome, ClaimResolutionRecord,
        DeletionRequestRecord, FlightGovernanceState, FlightGovernanceStatus,
        IdentityRecoveryBasis, IdentityRecoveryRecord, OwnerClaimRecord, PilotAuthDidStateStatus,
        PilotAuthDidSyncError, PrivateAccessRotationRecord, PrivateAccessRotationStateStatus,
        PublicationMode, PublicationModeRecord, ResolverProfile, RosterUpdateAction,
        RosterUpdateRecord, governance::sync::PilotAuthDidSyncRequest, identity::DidKey,
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

    fn resolver_profile(name: &str) -> ResolverProfile {
        ResolverProfile {
            display_name: name.to_string(),
            service_url: format!("https://{name}.example.org"),
            privacy_policy_url: format!("https://{name}.example.org/privacy"),
            public_key_url: format!("https://{name}.example.org/.well-known/igc-net-resolver-key"),
        }
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
    fn private_access_rotation_round_trip_and_resolution() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(151);
        let first = PrivateAccessRotationRecord::issue(
            &pilot_root,
            deterministic_secret_key(152).public(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let second = PrivateAccessRotationRecord::issue(
            &pilot_root,
            deterministic_secret_key(153).public(),
            Some(first.record_id.clone()),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        store
            .persist_private_access_rotation_record(&first)
            .unwrap();
        store
            .persist_private_access_rotation_record(&second)
            .unwrap();

        let loaded = store
            .load_private_access_rotation_records(&first.pilot_id)
            .unwrap();
        assert_eq!(loaded.len(), 2);

        let state = store
            .resolve_private_access_rotation_state(&first.pilot_id)
            .unwrap();
        assert_eq!(
            state.status(),
            PrivateAccessRotationStateStatus::Authoritative
        );
        assert_eq!(state.authoritative.unwrap(), second);
    }

    #[test]
    fn private_access_rotation_missing_parent_is_tentative() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(154);
        let child = PrivateAccessRotationRecord::issue(
            &pilot_root,
            deterministic_secret_key(155).public(),
            Some(Blake3Hex::parse("a".repeat(64)).unwrap()),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        store
            .persist_private_access_rotation_record(&child)
            .unwrap();

        let state = store
            .resolve_private_access_rotation_state(&child.pilot_id)
            .unwrap();
        assert_eq!(state.status(), PrivateAccessRotationStateStatus::Tentative);
        assert_eq!(state.tentative_record_ids, vec![child.record_id]);
    }

    #[test]
    fn flight_governance_state_round_trip() {
        let (store, _dir) = temp_store();
        let raw_igc_hash = Blake3Hex::parse("b".repeat(64)).unwrap();
        let owner = PilotId::from_public_key(deterministic_secret_key(156).public());
        let state = FlightGovernanceState::approved_owner(
            raw_igc_hash.clone(),
            owner.clone(),
            "2026-05-01T09:14:00Z",
        );

        store.persist_flight_governance_state(&state).unwrap();

        let loaded = store
            .load_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(loaded, state);
        assert_eq!(loaded.status, FlightGovernanceStatus::Approved);
        assert!(loaded.restricted_serving_ready_for(&owner));
    }

    #[test]
    fn absent_flight_governance_state_returns_none() {
        let (store, _dir) = temp_store();
        assert!(
            store
                .load_flight_governance_state(&Blake3Hex::parse("c".repeat(64)).unwrap())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn owner_claim_resolves_to_pending_flight_governance_state() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(157);
        let raw_igc_hash = Blake3Hex::parse("d".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();

        store.persist_owner_claim_record(&claim).unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Pending);
        assert_eq!(resolved.owner_pilot_id, Some(claim.pilot_id));
    }

    #[test]
    fn owner_deletion_request_resolves_applied_state_to_deleted() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(158);
        let raw_igc_hash = Blake3Hex::parse("e".repeat(64)).unwrap();
        let owner = PilotId::from_public_key(pilot_root.public());
        store
            .persist_flight_governance_state(&FlightGovernanceState::approved_owner(
                raw_igc_hash.clone(),
                owner,
                "2026-05-01T09:14:00Z",
            ))
            .unwrap();
        let deletion =
            DeletionRequestRecord::issue(&pilot_root, raw_igc_hash.clone(), "2026-05-01T10:14:00Z")
                .unwrap();

        store.persist_deletion_request_record(&deletion).unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Deleted);
        assert!(resolved.serving_blocked());
    }

    #[test]
    fn publication_mode_records_select_complete_chain_tip() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(159);
        let raw_igc_hash = Blake3Hex::parse("9".repeat(64)).unwrap();
        let first = PublicationModeRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            PublicationMode::Public,
            None,
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let second = PublicationModeRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            PublicationMode::Private,
            None,
            Some(first.record_id.clone()),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        let incomplete = PublicationModeRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            PublicationMode::Protected,
            Some(Blake3Hex::parse("a".repeat(64)).unwrap()),
            Some(Blake3Hex::parse("b".repeat(64)).unwrap()),
            "2026-05-01T11:14:00Z",
        )
        .unwrap();

        store.persist_publication_mode_record(&first).unwrap();
        store.persist_publication_mode_record(&second).unwrap();
        store.persist_publication_mode_record(&incomplete).unwrap();

        let loaded = store.load_publication_mode_records(&raw_igc_hash).unwrap();
        assert_eq!(loaded.len(), 3);
        let resolved = store
            .resolve_publication_mode_record(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved, second);
    }

    #[test]
    fn publication_mode_record_missing_parent_has_no_tip() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(160);
        let raw_igc_hash = Blake3Hex::parse("a".repeat(64)).unwrap();
        let incomplete = PublicationModeRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            PublicationMode::Private,
            None,
            Some(Blake3Hex::parse("b".repeat(64)).unwrap()),
            "2026-05-01T09:14:00Z",
        )
        .unwrap();

        store.persist_publication_mode_record(&incomplete).unwrap();

        assert!(
            store
                .resolve_publication_mode_record(&raw_igc_hash)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn trusted_approval_resolves_claim_to_approved_owner() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(162);
        let resolver = deterministic_secret_key(163);
        let raw_igc_hash = Blake3Hex::parse("f".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &resolver,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_claim_approval_record(&approval).unwrap();
        store.trust_resolver(&approval.resolver_id).unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Approved);
        assert_eq!(resolved.owner_pilot_id, Some(claim.pilot_id));
    }

    #[test]
    fn untrusted_approval_is_ignored() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(164);
        let resolver = deterministic_secret_key(165);
        let raw_igc_hash = Blake3Hex::parse("1".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &resolver,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_claim_approval_record(&approval).unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Pending);
    }

    #[test]
    fn trusted_challenge_resolves_approved_claim_to_contested() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(166);
        let resolver_a = deterministic_secret_key(167);
        let resolver_b = deterministic_secret_key(168);
        let raw_igc_hash = Blake3Hex::parse("2".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &resolver_a,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        let challenge = ClaimChallengeRecord::issue(
            &resolver_b,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "ownership_dispute",
            "2026-05-01T11:14:00Z",
        )
        .unwrap();

        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_claim_approval_record(&approval).unwrap();
        store.persist_claim_challenge_record(&challenge).unwrap();
        store.trust_resolver(&approval.resolver_id).unwrap();
        store
            .trust_resolver(&challenge.challenger_resolver_id)
            .unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Contested);
        assert!(resolved.serving_blocked());
    }

    #[test]
    fn challenge_lapses_after_fifteen_days_without_resolution() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(180);
        let resolver_a = deterministic_secret_key(181);
        let resolver_b = deterministic_secret_key(182);
        let raw_igc_hash = Blake3Hex::parse("7".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &resolver_a,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        let challenge = ClaimChallengeRecord::issue(
            &resolver_b,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "ownership_dispute",
            "2026-05-01T11:14:00Z",
        )
        .unwrap();

        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_claim_approval_record(&approval).unwrap();
        store.persist_claim_challenge_record(&challenge).unwrap();
        store.trust_resolver(&approval.resolver_id).unwrap();
        store
            .trust_resolver(&challenge.challenger_resolver_id)
            .unwrap();

        let contested = store
            .resolve_flight_governance_state_at(&raw_igc_hash, "2026-05-16T11:13:59Z")
            .unwrap()
            .unwrap();
        assert_eq!(contested.status, FlightGovernanceStatus::Contested);

        let approved = store
            .resolve_flight_governance_state_at(&raw_igc_hash, "2026-05-16T11:14:00Z")
            .unwrap()
            .unwrap();
        assert_eq!(approved.status, FlightGovernanceStatus::Approved);
        assert_eq!(approved.owner_pilot_id, Some(claim.pilot_id));
    }

    #[test]
    fn identity_recovery_transfers_approved_owner_state() {
        let (store, _dir) = temp_store();
        let old_pilot_root = deterministic_secret_key(183);
        let new_pilot_root = deterministic_secret_key(184);
        let resolver = deterministic_secret_key(185);
        let raw_igc_hash = Blake3Hex::parse("8".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &old_pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &resolver,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        let recovery = IdentityRecoveryRecord::issue(
            &resolver,
            claim.pilot_id.clone(),
            PilotId::from_public_key(new_pilot_root.public()),
            IdentityRecoveryBasis::KeyLossRecovery,
            "2026-05-02T09:14:00Z",
        )
        .unwrap();

        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_claim_approval_record(&approval).unwrap();
        store.persist_identity_recovery_record(&recovery).unwrap();
        store.trust_resolver(&approval.resolver_id).unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Approved);
        assert_eq!(resolved.owner_pilot_id, Some(recovery.new_pilot_id));
        assert_eq!(resolved.recorded_at, recovery.created_at);
    }

    #[test]
    fn trusted_resolution_rejects_claim_and_blocks_serving() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(169);
        let resolver = deterministic_secret_key(170);
        let raw_igc_hash = Blake3Hex::parse("3".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let resolution = ClaimResolutionRecord::issue(
            &resolver,
            raw_igc_hash.clone(),
            claim.record_id.clone(),
            ClaimResolutionOutcome::Rejected,
            vec!["manual_review".to_string()],
            Vec::new(),
            "2026-05-01T12:14:00Z",
        )
        .unwrap();

        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_claim_resolution_record(&resolution).unwrap();
        store.trust_resolver(&resolution.resolver_id).unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Rejected);
        assert!(resolved.serving_blocked());
    }

    #[test]
    fn generic_governance_record_ingestion_persists_and_deduplicates() {
        let (store, _dir) = temp_store();
        let pilot_root = deterministic_secret_key(171);
        let resolver = deterministic_secret_key(172);
        let raw_igc_hash = Blake3Hex::parse("4".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &resolver,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        let publication_mode = PublicationModeRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            PublicationMode::Private,
            None,
            None,
            "2026-05-01T11:14:00Z",
        )
        .unwrap();

        assert!(
            store
                .apply_governance_record_json(&serde_json::to_vec(&claim).unwrap())
                .unwrap()
        );
        assert!(
            !store
                .apply_governance_record_json(&serde_json::to_vec(&claim).unwrap())
                .unwrap()
        );
        assert!(
            store
                .apply_governance_record(&GovernanceRecord::ClaimApproval(approval.clone()))
                .unwrap()
        );
        assert!(
            store
                .apply_governance_record_json(&serde_json::to_vec(&publication_mode).unwrap())
                .unwrap()
        );
        assert!(
            !store
                .apply_governance_record_json(&serde_json::to_vec(&publication_mode).unwrap())
                .unwrap()
        );
        store.trust_resolver(&approval.resolver_id).unwrap();

        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Approved);
        assert_eq!(resolved.owner_pilot_id, Some(claim.pilot_id));
        assert_eq!(
            store
                .resolve_publication_mode_record(&raw_igc_hash)
                .unwrap(),
            Some(publication_mode)
        );
    }

    #[test]
    fn roster_update_add_authorizes_resolver_records() {
        let (store, _dir) = temp_store();
        let bootstrap_resolver = deterministic_secret_key(173);
        let added_resolver = deterministic_secret_key(174);
        let pilot_root = deterministic_secret_key(175);
        let raw_igc_hash = Blake3Hex::parse("5".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let update = RosterUpdateRecord::issue(
            &bootstrap_resolver,
            RosterUpdateAction::Add,
            added_resolver.public().to_string(),
            Some(resolver_profile("added-resolver")),
            "2026-05-01T09:30:00Z",
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &added_resolver,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        store
            .trust_resolver(&bootstrap_resolver.public().to_string())
            .unwrap();
        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_roster_update_record(&update).unwrap();
        store.persist_claim_approval_record(&approval).unwrap();

        assert!(
            store
                .effective_trusted_resolvers()
                .unwrap()
                .contains(&approval.resolver_id)
        );
        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Approved);
    }

    #[test]
    fn roster_update_remove_deauthorizes_resolver_records() {
        let (store, _dir) = temp_store();
        let bootstrap_resolver = deterministic_secret_key(176);
        let removed_resolver = deterministic_secret_key(177);
        let pilot_root = deterministic_secret_key(178);
        let raw_igc_hash = Blake3Hex::parse("6".repeat(64)).unwrap();
        let claim = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();
        let remove = RosterUpdateRecord::issue(
            &bootstrap_resolver,
            RosterUpdateAction::Remove,
            removed_resolver.public().to_string(),
            None,
            "2026-05-01T09:30:00Z",
        )
        .unwrap();
        let approval = ClaimApprovalRecord::issue(
            &removed_resolver,
            claim.record_id.clone(),
            raw_igc_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();

        store
            .trust_resolver(&bootstrap_resolver.public().to_string())
            .unwrap();
        store
            .trust_resolver(&removed_resolver.public().to_string())
            .unwrap();
        store.persist_owner_claim_record(&claim).unwrap();
        store.persist_roster_update_record(&remove).unwrap();
        store.persist_claim_approval_record(&approval).unwrap();

        assert!(
            !store
                .effective_trusted_resolvers()
                .unwrap()
                .contains(&approval.resolver_id)
        );
        let resolved = store
            .resolve_flight_governance_state(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.status, FlightGovernanceStatus::Pending);
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
