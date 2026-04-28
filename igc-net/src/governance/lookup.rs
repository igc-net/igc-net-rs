use crate::id::PilotId;

use super::record::{PilotAuthDidRecord, PrivateAccessRotationRecord};
use super::state::{PilotAuthDidState, PrivateAccessRotationState};
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

    fn load_private_access_rotation_records(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Vec<PrivateAccessRotationRecord>, GovernanceStoreError>;

    fn resolve_private_access_rotation_state(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PrivateAccessRotationState, GovernanceStoreError>;
}
