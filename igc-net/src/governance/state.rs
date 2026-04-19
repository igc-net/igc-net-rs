use crate::id::{Blake3Hex, PilotId};

use super::record::PilotAuthDidRecord;

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
