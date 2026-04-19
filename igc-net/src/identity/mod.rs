pub mod did_key;
pub mod did_web;
pub mod vc_jwt;

pub use did_key::{DidKey, DidKeyError};
#[cfg(feature = "did-web")]
pub use did_web::ReqwestDidWebResolver;
pub use did_web::{
    DidWeb, DidWebError, DidWebResolutionError, DidWebResolver, NoDidWebResolver,
    ResolvedDidWebVerificationMethod,
};
pub use vc_jwt::{
    Clock, FixedClock, JwtAudience, PilotProfileCredentialClaims, PilotProfileCredentialError,
    PilotProfileCredentialJoseHeader, PilotProfileCredentialJwt, PilotProfileCredentialRequest,
    PilotProfileCredentialSubject, PilotProfileCredentialSubjectDraft, PilotProfileCredentialVc,
    SystemClock, issue_pilot_profile_credential, verify_pilot_profile_credential,
};

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::collections::BTreeMap;

    use crate::governance::{GovernanceStore, PilotAuthDidRecord};
    use crate::keys::PilotIdentity;

    use super::{DidKey, PilotProfileCredentialRequest, PilotProfileCredentialSubjectDraft};

    pub(crate) fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    pub(crate) fn temp_governance_store() -> (GovernanceStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = GovernanceStore::for_data_dir(dir.path());
        store.init().unwrap();
        (store, dir)
    }

    pub(crate) fn seed_identity_and_governance(
        pilot_byte: u8,
        auth_byte: u8,
    ) -> (PilotIdentity, GovernanceStore, tempfile::TempDir) {
        let pilot_root = deterministic_secret_key(pilot_byte);
        let pilot_auth = deterministic_secret_key(auth_byte);
        let identity = PilotIdentity::from_secret_keys(pilot_root.clone(), pilot_auth.clone());
        let (store, dir) = temp_governance_store();
        let initial = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(pilot_auth.public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        store.persist_pilot_auth_did_record(&initial).unwrap();
        (identity, store, dir)
    }

    pub(crate) fn sample_request() -> PilotProfileCredentialRequest {
        PilotProfileCredentialRequest {
            subject: PilotProfileCredentialSubjectDraft {
                name: Some("Alice Example".to_string()),
                country: Some("NO".to_string()),
                additional_fields: BTreeMap::new(),
            },
            jti: "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479".to_string(),
            audience: None,
            expires_in_seconds: None,
        }
    }
}
