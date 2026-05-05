use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, PilotId};

use super::record::{
    ClaimApprovalRecord, ClaimChallengeRecord, ClaimResolutionRecord, DeletionRequestRecord,
    IdentityRecoveryRecord, OwnerClaimRecord, PilotAuthDidRecord, PrivateAccessRotationRecord,
    PublicationModeRecord, RosterUpdateRecord,
};
use super::state::PilotAuthDidState;

#[derive(Debug, thiserror::Error)]
pub enum PilotAuthDidSyncError {
    #[error("sync response record {record_id} belongs to pilot_id {found}, expected {expected}")]
    MixedPilotRecord {
        expected: PilotId,
        found: PilotId,
        record_id: Blake3Hex,
    },
    #[error("sync response contains duplicate record_id {0}")]
    DuplicateRecordId(Blake3Hex),
    #[error("sync response contains invalid record: {0}")]
    InvalidRecord(#[from] super::record::PilotAuthDidRecordError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotAuthDidGossipAnnouncement {
    pub pilot_id: PilotId,
    pub record_id: Blake3Hex,
}

#[derive(Debug, thiserror::Error)]
pub enum GovernanceRecordParseError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("governance record is missing string schema field")]
    MissingSchema,
    #[error("unsupported governance record schema: {0}")]
    UnsupportedSchema(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GovernanceRecord {
    OwnerClaim(OwnerClaimRecord),
    ClaimApproval(ClaimApprovalRecord),
    ClaimChallenge(ClaimChallengeRecord),
    ClaimResolution(ClaimResolutionRecord),
    IdentityRecovery(IdentityRecoveryRecord),
    DeletionRequest(DeletionRequestRecord),
    PrivateAccessRotation(PrivateAccessRotationRecord),
    PublicationMode(PublicationModeRecord),
    PilotAuthDid(PilotAuthDidRecord),
    RosterUpdate(RosterUpdateRecord),
}

impl GovernanceRecord {
    pub fn from_slice(bytes: &[u8]) -> Result<Self, GovernanceRecordParseError> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        Self::from_value(value)
    }

    pub fn from_value(value: serde_json::Value) -> Result<Self, GovernanceRecordParseError> {
        let schema = value
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .ok_or(GovernanceRecordParseError::MissingSchema)?;
        match schema {
            "igc-net/claim" => Ok(Self::OwnerClaim(serde_json::from_value(value)?)),
            "igc-net/claim-approval" => Ok(Self::ClaimApproval(serde_json::from_value(value)?)),
            "igc-net/claim-challenge" => Ok(Self::ClaimChallenge(serde_json::from_value(value)?)),
            "igc-net/claim-resolution" => Ok(Self::ClaimResolution(serde_json::from_value(value)?)),
            "igc-net/identity-recovery" => {
                Ok(Self::IdentityRecovery(serde_json::from_value(value)?))
            }
            "igc-net/deletion-request" => Ok(Self::DeletionRequest(serde_json::from_value(value)?)),
            "igc-net/private-access-rotation-record" => {
                Ok(Self::PrivateAccessRotation(serde_json::from_value(value)?))
            }
            "igc-net/publication-mode-record" => {
                Ok(Self::PublicationMode(serde_json::from_value(value)?))
            }
            "igc-net/pilot-auth-did-record" => {
                Ok(Self::PilotAuthDid(serde_json::from_value(value)?))
            }
            "igc-net/roster-update" => Ok(Self::RosterUpdate(serde_json::from_value(value)?)),
            schema => Err(GovernanceRecordParseError::UnsupportedSchema(
                schema.to_string(),
            )),
        }
    }
}

impl PilotAuthDidGossipAnnouncement {
    pub fn new(pilot_id: PilotId, record_id: Blake3Hex) -> Self {
        Self {
            pilot_id,
            record_id,
        }
    }

    pub fn from_record(record: &PilotAuthDidRecord) -> Self {
        Self::new(record.pilot_id.clone(), record.record_id.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotAuthDidSyncRequest {
    pub pilot_id: PilotId,
    pub known_record_ids: Vec<Blake3Hex>,
}

impl PilotAuthDidSyncRequest {
    pub fn new(pilot_id: PilotId, known_record_ids: Vec<Blake3Hex>) -> Self {
        let mut request = Self {
            pilot_id,
            known_record_ids,
        };
        request.normalize();
        request
    }

    pub fn from_state(state: &PilotAuthDidState) -> Self {
        Self::new(state.pilot_id.clone(), state.known_record_ids())
    }

    pub fn knows(&self, record_id: &Blake3Hex) -> bool {
        self.known_record_ids.binary_search(record_id).is_ok()
    }

    fn normalize(&mut self) {
        self.known_record_ids.sort();
        self.known_record_ids.dedup();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotAuthDidSyncResponse {
    pub pilot_id: PilotId,
    pub records: Vec<PilotAuthDidRecord>,
}

impl PilotAuthDidSyncResponse {
    pub fn new(pilot_id: PilotId, mut records: Vec<PilotAuthDidRecord>) -> Self {
        records.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        records.dedup_by(|left, right| left.record_id == right.record_id);
        Self { pilot_id, records }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn validate(&self) -> Result<(), PilotAuthDidSyncError> {
        let mut previous_record_id: Option<&Blake3Hex> = None;
        for record in &self.records {
            if record.pilot_id != self.pilot_id {
                return Err(PilotAuthDidSyncError::MixedPilotRecord {
                    expected: self.pilot_id.clone(),
                    found: record.pilot_id.clone(),
                    record_id: record.record_id.clone(),
                });
            }
            record.validate()?;
            if previous_record_id == Some(&record.record_id) {
                return Err(PilotAuthDidSyncError::DuplicateRecordId(
                    record.record_id.clone(),
                ));
            }
            previous_record_id = Some(&record.record_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::DidKey;

    use super::*;

    fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    #[test]
    fn validate_rejects_mixed_pilot_records() {
        let record = PilotAuthDidRecord::issue(
            &deterministic_secret_key(1),
            DidKey::from_public_key(deterministic_secret_key(2).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let response = PilotAuthDidSyncResponse::new(
            PilotId::from_public_key(deterministic_secret_key(3).public()),
            vec![record.clone()],
        );
        let err = response.validate().unwrap_err();
        assert!(matches!(
            err,
            PilotAuthDidSyncError::MixedPilotRecord { expected, found, record_id }
            if expected == PilotId::from_public_key(deterministic_secret_key(3).public())
                && found == record.pilot_id
                && record_id == record.record_id
        ));
    }
}
