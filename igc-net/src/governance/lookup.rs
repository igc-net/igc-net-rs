use crate::id::PilotId;

use super::record::PilotAuthDidRecord;
use super::state::PilotAuthDidState;
use super::store::GovernanceStoreError;

pub trait GovernanceLookup {
    fn load_pilot_auth_did_records(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Vec<PilotAuthDidRecord>, GovernanceStoreError>;

    fn resolve_pilot_auth_did_state(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PilotAuthDidState, GovernanceStoreError>;
}
