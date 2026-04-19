use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, PilotId};

use super::record::PilotAuthDidRecord;
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
