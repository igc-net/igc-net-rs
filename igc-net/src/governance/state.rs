use crate::id::{Blake3Hex, PilotId};

use super::record::{PilotAuthDidRecord, PrivateAccessRotationRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlightGovernanceStatus {
    Pending,
    Approved,
    Contested,
    Rejected,
    Superseded,
    Revoked,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FlightGovernanceState {
    pub raw_igc_hash: Blake3Hex,
    pub owner_pilot_id: Option<PilotId>,
    pub status: FlightGovernanceStatus,
    pub baseline_ready: bool,
    pub recorded_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PilotAuthDidStateStatus {
    Absent,
    Tentative,
    Authoritative,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PilotAuthDidState {
    pub pilot_id: PilotId,
    pub authoritative: Option<PilotAuthDidRecord>,
    pub tentative_record_ids: Vec<Blake3Hex>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateAccessRotationStateStatus {
    Absent,
    Tentative,
    Authoritative,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateAccessRotationState {
    pub pilot_id: PilotId,
    pub authoritative: Option<PrivateAccessRotationRecord>,
    pub tentative_record_ids: Vec<Blake3Hex>,
}

impl FlightGovernanceStatus {
    pub fn blocks_serving(self) -> bool {
        matches!(
            self,
            Self::Contested | Self::Rejected | Self::Superseded | Self::Revoked | Self::Deleted
        )
    }
}

impl FlightGovernanceState {
    pub fn approved_owner(
        raw_igc_hash: Blake3Hex,
        owner_pilot_id: PilotId,
        recorded_at: impl Into<String>,
    ) -> Self {
        Self {
            raw_igc_hash,
            owner_pilot_id: Some(owner_pilot_id),
            status: FlightGovernanceStatus::Approved,
            baseline_ready: true,
            recorded_at: recorded_at.into(),
        }
    }

    pub fn serving_blocked(&self) -> bool {
        self.status.blocks_serving()
    }

    pub fn restricted_serving_ready_for(&self, pilot_id: &PilotId) -> bool {
        self.baseline_ready
            && self.status == FlightGovernanceStatus::Approved
            && self.owner_pilot_id.as_ref() == Some(pilot_id)
    }
}

impl PilotAuthDidState {
    pub fn absent(pilot_id: PilotId) -> Self {
        Self {
            pilot_id,
            authoritative: None,
            tentative_record_ids: Vec::new(),
        }
    }

    pub fn status(&self) -> PilotAuthDidStateStatus {
        if self.authoritative.is_some() {
            PilotAuthDidStateStatus::Authoritative
        } else if self.tentative_record_ids.is_empty() {
            PilotAuthDidStateStatus::Absent
        } else {
            PilotAuthDidStateStatus::Tentative
        }
    }

    pub fn requires_catch_up(&self) -> bool {
        !self.tentative_record_ids.is_empty()
    }

    pub fn is_high_trust_authoritative(&self) -> bool {
        self.authoritative.is_some() && !self.requires_catch_up()
    }

    pub fn known_record_ids(&self) -> Vec<Blake3Hex> {
        let mut record_ids = self.tentative_record_ids.clone();
        if let Some(record) = &self.authoritative {
            record_ids.push(record.record_id.clone());
        }
        record_ids.sort();
        record_ids.dedup();
        record_ids
    }
}

impl PrivateAccessRotationState {
    pub fn absent(pilot_id: PilotId) -> Self {
        Self {
            pilot_id,
            authoritative: None,
            tentative_record_ids: Vec::new(),
        }
    }

    pub fn status(&self) -> PrivateAccessRotationStateStatus {
        if self.authoritative.is_some() {
            PrivateAccessRotationStateStatus::Authoritative
        } else if self.tentative_record_ids.is_empty() {
            PrivateAccessRotationStateStatus::Absent
        } else {
            PrivateAccessRotationStateStatus::Tentative
        }
    }

    pub fn requires_catch_up(&self) -> bool {
        !self.tentative_record_ids.is_empty()
    }

    pub fn is_authoritative(&self) -> bool {
        self.authoritative.is_some() && !self.requires_catch_up()
    }
}
