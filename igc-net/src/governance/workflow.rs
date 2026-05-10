use crate::DidKey;
use crate::PilotIdentity;

use super::lookup::GovernanceLookup;
use super::record::{PilotAuthDidRecord, PilotAuthDidRecordError};
use super::selection::{GovernanceSelectionError, select_pilot_auth_did_state};
use super::state::PilotAuthDidStateStatus;
use super::store::GovernanceStoreError;

#[derive(Debug, thiserror::Error)]
pub enum PilotAuthDidWorkflowError {
    #[error("governance: {0}")]
    Governance(#[from] GovernanceStoreError),
    #[error("record: {0}")]
    Record(#[from] PilotAuthDidRecordError),
    #[error("selection: {0}")]
    Selection(#[from] GovernanceSelectionError),
    #[error(
        "cannot issue initial pilot-auth-did-record because authoritative state already exists"
    )]
    InitialRecordAlreadyExists,
    #[error("cannot rotate pilot_auth_did without an authoritative local governance state")]
    MissingAuthoritativeState,
    #[error("cannot rotate pilot_auth_did from tentative governance state")]
    TentativeGovernanceState,
    #[error(
        "local active pilot_auth_did {local} does not match authoritative governance DID {authoritative}"
    )]
    ActiveDidMismatch {
        authoritative: DidKey,
        local: DidKey,
    },
    #[error("rotation candidate reuses the current authoritative pilot_auth_did")]
    RotationDidUnchanged,
    #[error("rotation candidate did not become authoritative under local validation")]
    CandidateNotAuthoritative,
}

pub fn issue_initial_pilot_auth_did_record(
    governance: &impl GovernanceLookup,
    pilot_identity: &PilotIdentity,
    created_at: impl Into<String>,
) -> Result<PilotAuthDidRecord, PilotAuthDidWorkflowError> {
    let state = governance.resolve_pilot_auth_did_state(&pilot_identity.pilot_id())?;
    match state.status() {
        PilotAuthDidStateStatus::Absent => {}
        PilotAuthDidStateStatus::Tentative => {
            return Err(PilotAuthDidWorkflowError::TentativeGovernanceState);
        }
        PilotAuthDidStateStatus::Authoritative => {
            return Err(PilotAuthDidWorkflowError::InitialRecordAlreadyExists);
        }
    }

    let record = PilotAuthDidRecord::issue(
        &pilot_identity.pilot_id_secret_key(),
        pilot_identity.active_pilot_auth_did(),
        None,
        created_at,
    )?;
    let prospective =
        select_pilot_auth_did_state(&pilot_identity.pilot_id(), std::slice::from_ref(&record))?;
    match prospective.authoritative.as_ref() {
        Some(candidate) if candidate.record_id == record.record_id => {}
        _ => {
            return Err(PilotAuthDidWorkflowError::CandidateNotAuthoritative);
        }
    }

    Ok(record)
}

pub fn rotate_pilot_auth_did_record(
    governance: &impl GovernanceLookup,
    current_pilot_identity: &PilotIdentity,
    next_active_pilot_auth_secret_key: &iroh::SecretKey,
    created_at: impl Into<String>,
) -> Result<PilotAuthDidRecord, PilotAuthDidWorkflowError> {
    let state = governance.resolve_pilot_auth_did_state(&current_pilot_identity.pilot_id())?;
    let authoritative = match state.status() {
        PilotAuthDidStateStatus::Absent => {
            return Err(PilotAuthDidWorkflowError::MissingAuthoritativeState);
        }
        PilotAuthDidStateStatus::Tentative => {
            return Err(PilotAuthDidWorkflowError::TentativeGovernanceState);
        }
        PilotAuthDidStateStatus::Authoritative => state
            .authoritative
            .clone()
            .expect("authoritative status must include authoritative record"),
    };

    let local_active_did = current_pilot_identity.active_pilot_auth_did();
    if authoritative.pilot_auth_did != local_active_did {
        return Err(PilotAuthDidWorkflowError::ActiveDidMismatch {
            authoritative: authoritative.pilot_auth_did,
            local: local_active_did,
        });
    }

    let next_active_pilot_auth_did =
        DidKey::from_public_key(next_active_pilot_auth_secret_key.public());
    if next_active_pilot_auth_did == authoritative.pilot_auth_did {
        return Err(PilotAuthDidWorkflowError::RotationDidUnchanged);
    }
    let record = PilotAuthDidRecord::issue(
        &current_pilot_identity.pilot_id_secret_key(),
        next_active_pilot_auth_did,
        Some(authoritative.record_id.clone()),
        created_at,
    )?;

    let mut records = governance.load_pilot_auth_did_records(&current_pilot_identity.pilot_id())?;
    records.push(record.clone());
    let prospective = select_pilot_auth_did_state(&current_pilot_identity.pilot_id(), &records)?;
    match prospective.authoritative.as_ref() {
        Some(candidate) if candidate.record_id == record.record_id => {}
        _ => {
            return Err(PilotAuthDidWorkflowError::CandidateNotAuthoritative);
        }
    }

    Ok(record)
}

#[cfg(test)]
mod tests {
    use crate::PilotKeyStore;
    use crate::governance::store::GovernanceStore;
    use crate::identity::DidKey;

    use super::*;

    fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    fn temp_governance_store() -> (GovernanceStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = GovernanceStore::for_data_dir(dir.path());
        store.init().unwrap();
        (store, dir)
    }

    fn temp_pilot_identity(
        byte: u8,
    ) -> (
        PilotKeyStore,
        PilotIdentity,
        tempfile::TempDir,
        iroh::SecretKey,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let key_store = PilotKeyStore::open(dir.path().join("pilot"));
        key_store.init().unwrap();
        let node_secret = deterministic_secret_key(byte);
        let identity = key_store.generate(&node_secret).unwrap();
        (key_store, identity, dir, node_secret)
    }

    #[test]
    fn initial_issuance_requires_absent_state() {
        let (governance, _dir) = temp_governance_store();
        let (_keys, identity, _keys_dir, _node_secret) = temp_pilot_identity(201);

        let record =
            issue_initial_pilot_auth_did_record(&governance, &identity, "2026-05-01T09:14:00Z")
                .unwrap();
        assert_eq!(record.supersedes, None);
        assert_eq!(record.pilot_auth_did, identity.active_pilot_auth_did());
    }

    #[test]
    fn initial_issuance_rejects_existing_authority() {
        let (governance, _dir) = temp_governance_store();
        let (_keys, identity, _keys_dir, _node_secret) = temp_pilot_identity(202);
        let existing = PilotAuthDidRecord::issue(
            &identity.pilot_id_secret_key(),
            identity.active_pilot_auth_did(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        governance.persist_pilot_auth_did_record(&existing).unwrap();

        let err =
            issue_initial_pilot_auth_did_record(&governance, &identity, "2026-05-01T09:15:00Z")
                .unwrap_err();
        assert!(matches!(
            err,
            PilotAuthDidWorkflowError::InitialRecordAlreadyExists
        ));
    }

    #[test]
    fn rotation_requires_authoritative_state_match() {
        let (governance, _dir) = temp_governance_store();
        let (_keys, identity, _keys_dir, _node_secret) = temp_pilot_identity(203);
        let next = deterministic_secret_key(204);

        let err =
            rotate_pilot_auth_did_record(&governance, &identity, &next, "2026-05-01T10:14:00Z")
                .unwrap_err();
        assert!(matches!(
            err,
            PilotAuthDidWorkflowError::MissingAuthoritativeState
        ));
    }

    #[test]
    fn rotation_refuses_tentative_local_state() {
        let (governance, _dir) = temp_governance_store();
        let (_keys, identity, _keys_dir, _node_secret) = temp_pilot_identity(205);
        let incomplete = PilotAuthDidRecord::issue(
            &identity.pilot_id_secret_key(),
            identity.active_pilot_auth_did(),
            Some(crate::Blake3Hex::parse("d".repeat(64)).unwrap()),
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        governance
            .persist_pilot_auth_did_record(&incomplete)
            .unwrap();

        let err = rotate_pilot_auth_did_record(
            &governance,
            &identity,
            &deterministic_secret_key(206),
            "2026-05-01T10:14:00Z",
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PilotAuthDidWorkflowError::TentativeGovernanceState
        ));
    }

    #[test]
    fn rotation_candidate_becomes_authoritative() {
        let (governance, _dir) = temp_governance_store();
        let (_keys, identity, _keys_dir, _node_secret) = temp_pilot_identity(207);
        let initial = PilotAuthDidRecord::issue(
            &identity.pilot_id_secret_key(),
            identity.active_pilot_auth_did(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        governance.persist_pilot_auth_did_record(&initial).unwrap();

        let next = deterministic_secret_key(208);
        let rotated =
            rotate_pilot_auth_did_record(&governance, &identity, &next, "2026-05-01T10:14:00Z")
                .unwrap();

        assert_eq!(rotated.supersedes, Some(initial.record_id));
        assert_eq!(
            rotated.pilot_auth_did,
            DidKey::from_public_key(next.public())
        );
    }

    #[test]
    fn rotation_rejects_unchanged_did() {
        let (governance, _dir) = temp_governance_store();
        let (_keys, identity, _keys_dir, _node_secret) = temp_pilot_identity(210);
        let initial = PilotAuthDidRecord::issue(
            &identity.pilot_id_secret_key(),
            identity.active_pilot_auth_did(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        governance.persist_pilot_auth_did_record(&initial).unwrap();

        let err = rotate_pilot_auth_did_record(
            &governance,
            &identity,
            &identity.active_pilot_auth_secret_key(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PilotAuthDidWorkflowError::RotationDidUnchanged
        ));
    }

    #[test]
    fn key_store_can_replace_active_pilot_auth_and_archive_previous_key() {
        let (key_store, identity, _dir, node_secret) = temp_pilot_identity(209);
        let next = key_store
            .generate_next_active_pilot_auth_secret_key(&node_secret)
            .unwrap();
        let previous_public_key_hex = identity.active_pilot_auth_public_key_hex();

        let updated = key_store
            .replace_active_pilot_auth(&node_secret, &next)
            .unwrap();

        assert_eq!(updated.pilot_id(), identity.pilot_id());
        assert_eq!(
            updated.active_pilot_auth_public_key_hex(),
            next.public().to_string()
        );
        let status = key_store.inspect().unwrap();
        assert_eq!(status.archived_key_count, 1);
        assert!(
            status
                .archive_dir
                .join(format!("{previous_public_key_hex}.json"))
                .exists()
        );
    }
}
