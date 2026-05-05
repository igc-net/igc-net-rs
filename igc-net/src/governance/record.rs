use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, PilotId};
use crate::identity::{DidKey, DidKeyError};
use crate::store::PublicationMode;
use crate::util::is_canonical_utc_timestamp;

const PILOT_AUTH_DID_RECORD_SCHEMA: &str = "igc-net/pilot-auth-did-record";
const PILOT_AUTH_DID_RECORD_VERSION: u8 = 1;
const PRIVATE_ACCESS_ROTATION_RECORD_SCHEMA: &str = "igc-net/private-access-rotation-record";
const PRIVATE_ACCESS_ROTATION_RECORD_VERSION: u8 = 1;
const OWNER_CLAIM_RECORD_SCHEMA: &str = "igc-net/claim";
const OWNER_CLAIM_RECORD_VERSION: u8 = 1;
const OWNER_CLAIM_TYPE: &str = "owner";
const CLAIM_APPROVAL_RECORD_SCHEMA: &str = "igc-net/claim-approval";
const CLAIM_APPROVAL_RECORD_VERSION: u8 = 1;
const CLAIM_CHALLENGE_RECORD_SCHEMA: &str = "igc-net/claim-challenge";
const CLAIM_CHALLENGE_RECORD_VERSION: u8 = 1;
const CLAIM_RESOLUTION_RECORD_SCHEMA: &str = "igc-net/claim-resolution";
const CLAIM_RESOLUTION_RECORD_VERSION: u8 = 1;
const IDENTITY_RECOVERY_RECORD_SCHEMA: &str = "igc-net/identity-recovery";
const IDENTITY_RECOVERY_RECORD_VERSION: u8 = 1;
const ROSTER_UPDATE_RECORD_SCHEMA: &str = "igc-net/roster-update";
const ROSTER_UPDATE_RECORD_VERSION: u8 = 1;
const PUBLICATION_MODE_RECORD_SCHEMA: &str = "igc-net/publication-mode-record";
const PUBLICATION_MODE_RECORD_VERSION: u8 = 1;
const DELETION_REQUEST_RECORD_SCHEMA: &str = "igc-net/deletion-request";
const DELETION_REQUEST_RECORD_VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum PilotAuthDidRecordError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("identifier: {0}")]
    Identifier(#[from] crate::id::IdentifierError),
    #[error("did:key: {0}")]
    DidKey(#[from] DidKeyError),
    #[error("schema must be {PILOT_AUTH_DID_RECORD_SCHEMA:?}, got {0:?}")]
    Schema(String),
    #[error("schema_version must be {PILOT_AUTH_DID_RECORD_VERSION}, got {0}")]
    SchemaVersion(u8),
    #[error("created_at is not canonical UTC RFC3339 seconds format: {0:?}")]
    CreatedAt(String),
    #[error("signature must be 128 lowercase hex chars")]
    SignatureEncoding,
    #[error("pilot_id does not contain a valid Ed25519 public key: {0}")]
    PilotIdPublicKey(String),
    #[error("record_id mismatch: expected {expected}, found {found}")]
    RecordIdMismatch {
        expected: Blake3Hex,
        found: Blake3Hex,
    },
    #[error("signature verification failed")]
    SignatureVerification,
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateAccessRotationRecordError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("identifier: {0}")]
    Identifier(#[from] crate::id::IdentifierError),
    #[error("schema must be {PRIVATE_ACCESS_ROTATION_RECORD_SCHEMA:?}, got {0:?}")]
    Schema(String),
    #[error("schema_version must be {PRIVATE_ACCESS_ROTATION_RECORD_VERSION}, got {0}")]
    SchemaVersion(u8),
    #[error("created_at is not canonical UTC RFC3339 seconds format: {0:?}")]
    CreatedAt(String),
    #[error("private_access_public_key must be 64 lowercase hex chars")]
    PrivateAccessPublicKeyEncoding,
    #[error("signature must be 128 lowercase hex chars")]
    SignatureEncoding,
    #[error("pilot_id does not contain a valid Ed25519 public key: {0}")]
    PilotIdPublicKey(String),
    #[error("record_id mismatch: expected {expected}, found {found}")]
    RecordIdMismatch {
        expected: Blake3Hex,
        found: Blake3Hex,
    },
    #[error("signature verification failed")]
    SignatureVerification,
}

#[derive(Debug, thiserror::Error)]
pub enum FlightGovernanceRecordError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("identifier: {0}")]
    Identifier(#[from] crate::id::IdentifierError),
    #[error("schema must be one of the supported flight governance schemas, got {0:?}")]
    Schema(String),
    #[error("schema_version is unsupported: {0}")]
    SchemaVersion(u8),
    #[error("claim_type must be {OWNER_CLAIM_TYPE:?}, got {0:?}")]
    ClaimType(String),
    #[error("resolution value is unsupported: {0:?}")]
    Resolution(String),
    #[error("created_at is not canonical UTC RFC3339 seconds format: {0:?}")]
    CreatedAt(String),
    #[error("signature must be 128 lowercase hex chars")]
    SignatureEncoding,
    #[error("resolver_id must be 64 lowercase hex chars: {0:?}")]
    ResolverIdEncoding(String),
    #[error("challenger_resolver_id must be 64 lowercase hex chars: {0:?}")]
    ChallengerResolverIdEncoding(String),
    #[error("signer_id must be 64 lowercase hex chars: {0:?}")]
    SignerIdEncoding(String),
    #[error("resolver_profile must be present for add and absent for remove")]
    RosterProfilePresence,
    #[error("old_pilot_id and new_pilot_id must be distinct")]
    IdentityRecoverySamePilot,
    #[error("protected_hash presence does not match publication_mode")]
    ProtectedHashPresence,
    #[error("pilot_id does not contain a valid Ed25519 public key: {0}")]
    PilotIdPublicKey(String),
    #[error("record_id mismatch: expected {expected}, found {found}")]
    RecordIdMismatch {
        expected: Blake3Hex,
        found: Blake3Hex,
    },
    #[error("signature verification failed")]
    SignatureVerification,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotAuthDidRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub pilot_id: PilotId,
    pub pilot_auth_did: DidKey,
    pub supersedes: Option<Blake3Hex>,
    pub created_at: String,
    pub signature: String,
}

impl PilotAuthDidRecord {
    pub fn issue(
        pilot_id_secret_key: &iroh::SecretKey,
        pilot_auth_did: DidKey,
        supersedes: Option<Blake3Hex>,
        created_at: impl Into<String>,
    ) -> Result<Self, PilotAuthDidRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(PilotAuthDidRecordError::CreatedAt(created_at));
        }

        let pilot_id = PilotId::from_public_key(pilot_id_secret_key.public());
        let record_id = derive_record_id(&pilot_id, &pilot_auth_did, &supersedes, &created_at)?;
        let signature_bytes = signing_payload(
            &pilot_id,
            &record_id,
            &pilot_auth_did,
            &supersedes,
            &created_at,
        )?;
        let signature = hex::encode(pilot_id_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: PILOT_AUTH_DID_RECORD_SCHEMA.to_string(),
            schema_version: PILOT_AUTH_DID_RECORD_VERSION,
            record_id,
            pilot_id,
            pilot_auth_did,
            supersedes,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), PilotAuthDidRecordError> {
        if self.schema != PILOT_AUTH_DID_RECORD_SCHEMA {
            return Err(PilotAuthDidRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != PILOT_AUTH_DID_RECORD_VERSION {
            return Err(PilotAuthDidRecordError::SchemaVersion(self.schema_version));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(PilotAuthDidRecordError::CreatedAt(self.created_at.clone()));
        }

        let expected_record_id = derive_record_id(
            &self.pilot_id,
            &self.pilot_auth_did,
            &self.supersedes,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(PilotAuthDidRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_signature_hex(&self.signature)?;
        let signing_bytes = signing_payload(
            &self.pilot_id,
            &self.record_id,
            &self.pilot_auth_did,
            &self.supersedes,
            &self.created_at,
        )?;
        pilot_id_public_key(&self.pilot_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| PilotAuthDidRecordError::SignatureVerification)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrivateAccessRotationRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub pilot_id: PilotId,
    pub private_access_public_key: String,
    pub supersedes: Option<Blake3Hex>,
    pub created_at: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerClaimRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub raw_igc_hash: Blake3Hex,
    pub claim_type: String,
    pub pilot_id: PilotId,
    pub signature: String,
    pub created_at: String,
    pub evidence: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimApprovalRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub claim_record_id: Blake3Hex,
    pub raw_igc_hash: Blake3Hex,
    pub resolver_id: String,
    pub signature: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimChallengeRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub claim_record_id: Blake3Hex,
    pub raw_igc_hash: Blake3Hex,
    pub challenger_resolver_id: String,
    pub signature: String,
    pub reason: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClaimResolutionOutcome {
    Approved,
    Rejected,
    Superseded,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaimResolutionRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub raw_igc_hash: Blake3Hex,
    pub claim_record_id: Blake3Hex,
    pub resolver_id: String,
    pub signature: String,
    pub resolution: ClaimResolutionOutcome,
    pub basis: Vec<String>,
    pub created_at: String,
    pub supersedes: Vec<Blake3Hex>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdentityRecoveryBasis {
    KeyLossRecovery,
    KeyCompromiseRecovery,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdentityRecoveryRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub old_pilot_id: PilotId,
    pub new_pilot_id: PilotId,
    pub resolver_id: String,
    pub basis: IdentityRecoveryBasis,
    pub created_at: String,
    pub signature: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RosterUpdateAction {
    Add,
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolverProfile {
    pub display_name: String,
    pub service_url: String,
    pub privacy_policy_url: String,
    pub public_key_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RosterUpdateRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub action: RosterUpdateAction,
    pub resolver_id: String,
    pub signer_id: String,
    pub resolver_profile: Option<ResolverProfile>,
    pub created_at: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicationModeRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub raw_igc_hash: Blake3Hex,
    pub publication_mode: PublicationMode,
    pub protected_hash: Option<Blake3Hex>,
    pub supersedes: Option<Blake3Hex>,
    pub pilot_id: PilotId,
    pub signature: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeletionRequestRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub raw_igc_hash: Blake3Hex,
    pub pilot_id: PilotId,
    pub signature: String,
    pub created_at: String,
}

impl PrivateAccessRotationRecord {
    pub fn issue(
        pilot_id_secret_key: &iroh::SecretKey,
        private_access_public_key: iroh::PublicKey,
        supersedes: Option<Blake3Hex>,
        created_at: impl Into<String>,
    ) -> Result<Self, PrivateAccessRotationRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(PrivateAccessRotationRecordError::CreatedAt(created_at));
        }

        let pilot_id = PilotId::from_public_key(pilot_id_secret_key.public());
        let private_access_public_key = private_access_public_key.to_string();
        let record_id = derive_private_access_rotation_record_id(
            &pilot_id,
            &private_access_public_key,
            &supersedes,
            &created_at,
        )?;
        let signature_bytes = private_access_rotation_signing_payload(
            &pilot_id,
            &record_id,
            &private_access_public_key,
            &supersedes,
            &created_at,
        )?;
        let signature = hex::encode(pilot_id_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: PRIVATE_ACCESS_ROTATION_RECORD_SCHEMA.to_string(),
            schema_version: PRIVATE_ACCESS_ROTATION_RECORD_VERSION,
            record_id,
            pilot_id,
            private_access_public_key,
            supersedes,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), PrivateAccessRotationRecordError> {
        if self.schema != PRIVATE_ACCESS_ROTATION_RECORD_SCHEMA {
            return Err(PrivateAccessRotationRecordError::Schema(
                self.schema.clone(),
            ));
        }
        if self.schema_version != PRIVATE_ACCESS_ROTATION_RECORD_VERSION {
            return Err(PrivateAccessRotationRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(PrivateAccessRotationRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }
        validate_private_access_public_key(&self.private_access_public_key)?;

        let expected_record_id = derive_private_access_rotation_record_id(
            &self.pilot_id,
            &self.private_access_public_key,
            &self.supersedes,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(PrivateAccessRotationRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_private_access_rotation_signature_hex(&self.signature)?;
        let signing_bytes = private_access_rotation_signing_payload(
            &self.pilot_id,
            &self.record_id,
            &self.private_access_public_key,
            &self.supersedes,
            &self.created_at,
        )?;
        pilot_id_public_key(&self.pilot_id)
            .map_err(|_| {
                PrivateAccessRotationRecordError::PilotIdPublicKey(self.pilot_id.to_string())
            })?
            .verify(&signing_bytes, &signature)
            .map_err(|_| PrivateAccessRotationRecordError::SignatureVerification)?;

        Ok(())
    }

    pub fn private_access_public_key(
        &self,
    ) -> Result<iroh::PublicKey, PrivateAccessRotationRecordError> {
        let bytes = decode_fixed_hex_32(&self.private_access_public_key)
            .map_err(|_| PrivateAccessRotationRecordError::PrivateAccessPublicKeyEncoding)?;
        iroh::PublicKey::from_bytes(&bytes)
            .map_err(|_| PrivateAccessRotationRecordError::PrivateAccessPublicKeyEncoding)
    }
}

impl OwnerClaimRecord {
    pub fn issue(
        pilot_id_secret_key: &iroh::SecretKey,
        raw_igc_hash: Blake3Hex,
        created_at: impl Into<String>,
        evidence: Vec<serde_json::Value>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }

        let pilot_id = PilotId::from_public_key(pilot_id_secret_key.public());
        let record_id =
            derive_owner_claim_record_id(&raw_igc_hash, &pilot_id, &created_at, &evidence)?;
        let signature_bytes = owner_claim_signing_payload(
            &record_id,
            &raw_igc_hash,
            &pilot_id,
            &created_at,
            &evidence,
        )?;
        let signature = hex::encode(pilot_id_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: OWNER_CLAIM_RECORD_SCHEMA.to_string(),
            schema_version: OWNER_CLAIM_RECORD_VERSION,
            record_id,
            raw_igc_hash,
            claim_type: OWNER_CLAIM_TYPE.to_string(),
            pilot_id,
            signature,
            created_at,
            evidence,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != OWNER_CLAIM_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != OWNER_CLAIM_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if self.claim_type != OWNER_CLAIM_TYPE {
            return Err(FlightGovernanceRecordError::ClaimType(
                self.claim_type.clone(),
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }

        let expected_record_id = derive_owner_claim_record_id(
            &self.raw_igc_hash,
            &self.pilot_id,
            &self.created_at,
            &self.evidence,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = owner_claim_signing_payload(
            &self.record_id,
            &self.raw_igc_hash,
            &self.pilot_id,
            &self.created_at,
            &self.evidence,
        )?;
        flight_pilot_id_public_key(&self.pilot_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

impl ClaimApprovalRecord {
    pub fn issue(
        resolver_secret_key: &iroh::SecretKey,
        claim_record_id: Blake3Hex,
        raw_igc_hash: Blake3Hex,
        created_at: impl Into<String>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }

        let resolver_id = resolver_secret_key.public().to_string();
        let record_id = derive_claim_approval_record_id(
            &claim_record_id,
            &raw_igc_hash,
            &resolver_id,
            &created_at,
        )?;
        let signature_bytes = claim_approval_signing_payload(
            &record_id,
            &claim_record_id,
            &raw_igc_hash,
            &resolver_id,
            &created_at,
        )?;
        let signature = hex::encode(resolver_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: CLAIM_APPROVAL_RECORD_SCHEMA.to_string(),
            schema_version: CLAIM_APPROVAL_RECORD_VERSION,
            record_id,
            claim_record_id,
            raw_igc_hash,
            resolver_id,
            signature,
            created_at,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != CLAIM_APPROVAL_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != CLAIM_APPROVAL_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }
        validate_resolver_id(&self.resolver_id)?;

        let expected_record_id = derive_claim_approval_record_id(
            &self.claim_record_id,
            &self.raw_igc_hash,
            &self.resolver_id,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = claim_approval_signing_payload(
            &self.record_id,
            &self.claim_record_id,
            &self.raw_igc_hash,
            &self.resolver_id,
            &self.created_at,
        )?;
        resolver_public_key(&self.resolver_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

impl ClaimChallengeRecord {
    pub fn issue(
        resolver_secret_key: &iroh::SecretKey,
        claim_record_id: Blake3Hex,
        raw_igc_hash: Blake3Hex,
        reason: impl Into<String>,
        created_at: impl Into<String>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }

        let challenger_resolver_id = resolver_secret_key.public().to_string();
        let reason = reason.into();
        let record_id = derive_claim_challenge_record_id(
            &claim_record_id,
            &raw_igc_hash,
            &challenger_resolver_id,
            &reason,
            &created_at,
        )?;
        let signature_bytes = claim_challenge_signing_payload(
            &record_id,
            &claim_record_id,
            &raw_igc_hash,
            &challenger_resolver_id,
            &reason,
            &created_at,
        )?;
        let signature = hex::encode(resolver_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: CLAIM_CHALLENGE_RECORD_SCHEMA.to_string(),
            schema_version: CLAIM_CHALLENGE_RECORD_VERSION,
            record_id,
            claim_record_id,
            raw_igc_hash,
            challenger_resolver_id,
            signature,
            reason,
            created_at,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != CLAIM_CHALLENGE_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != CLAIM_CHALLENGE_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }
        validate_challenger_resolver_id(&self.challenger_resolver_id)?;

        let expected_record_id = derive_claim_challenge_record_id(
            &self.claim_record_id,
            &self.raw_igc_hash,
            &self.challenger_resolver_id,
            &self.reason,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = claim_challenge_signing_payload(
            &self.record_id,
            &self.claim_record_id,
            &self.raw_igc_hash,
            &self.challenger_resolver_id,
            &self.reason,
            &self.created_at,
        )?;
        resolver_public_key(&self.challenger_resolver_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

impl ClaimResolutionRecord {
    pub fn issue(
        resolver_secret_key: &iroh::SecretKey,
        raw_igc_hash: Blake3Hex,
        claim_record_id: Blake3Hex,
        resolution: ClaimResolutionOutcome,
        basis: Vec<String>,
        supersedes: Vec<Blake3Hex>,
        created_at: impl Into<String>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }

        let resolver_id = resolver_secret_key.public().to_string();
        let record_id = derive_claim_resolution_record_id(
            &raw_igc_hash,
            &claim_record_id,
            &resolver_id,
            resolution,
            &basis,
            &created_at,
            &supersedes,
        )?;
        let signature_bytes = claim_resolution_signing_payload(
            &record_id,
            &raw_igc_hash,
            &claim_record_id,
            &resolver_id,
            resolution,
            &basis,
            &created_at,
            &supersedes,
        )?;
        let signature = hex::encode(resolver_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: CLAIM_RESOLUTION_RECORD_SCHEMA.to_string(),
            schema_version: CLAIM_RESOLUTION_RECORD_VERSION,
            record_id,
            raw_igc_hash,
            claim_record_id,
            resolver_id,
            signature,
            resolution,
            basis,
            created_at,
            supersedes,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != CLAIM_RESOLUTION_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != CLAIM_RESOLUTION_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }
        validate_resolver_id(&self.resolver_id)?;

        let expected_record_id = derive_claim_resolution_record_id(
            &self.raw_igc_hash,
            &self.claim_record_id,
            &self.resolver_id,
            self.resolution,
            &self.basis,
            &self.created_at,
            &self.supersedes,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = claim_resolution_signing_payload(
            &self.record_id,
            &self.raw_igc_hash,
            &self.claim_record_id,
            &self.resolver_id,
            self.resolution,
            &self.basis,
            &self.created_at,
            &self.supersedes,
        )?;
        resolver_public_key(&self.resolver_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

impl IdentityRecoveryRecord {
    pub fn issue(
        resolver_secret_key: &iroh::SecretKey,
        old_pilot_id: PilotId,
        new_pilot_id: PilotId,
        basis: IdentityRecoveryBasis,
        created_at: impl Into<String>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }
        if old_pilot_id == new_pilot_id {
            return Err(FlightGovernanceRecordError::IdentityRecoverySamePilot);
        }

        let resolver_id = resolver_secret_key.public().to_string();
        let record_id = derive_identity_recovery_record_id(
            &old_pilot_id,
            &new_pilot_id,
            &resolver_id,
            basis,
            &created_at,
        )?;
        let signature_bytes = identity_recovery_signing_payload(
            &record_id,
            &old_pilot_id,
            &new_pilot_id,
            &resolver_id,
            basis,
            &created_at,
        )?;
        let signature = hex::encode(resolver_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: IDENTITY_RECOVERY_RECORD_SCHEMA.to_string(),
            schema_version: IDENTITY_RECOVERY_RECORD_VERSION,
            record_id,
            old_pilot_id,
            new_pilot_id,
            resolver_id,
            basis,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != IDENTITY_RECOVERY_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != IDENTITY_RECOVERY_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if self.old_pilot_id == self.new_pilot_id {
            return Err(FlightGovernanceRecordError::IdentityRecoverySamePilot);
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }
        validate_resolver_id(&self.resolver_id)?;

        let expected_record_id = derive_identity_recovery_record_id(
            &self.old_pilot_id,
            &self.new_pilot_id,
            &self.resolver_id,
            self.basis,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = identity_recovery_signing_payload(
            &self.record_id,
            &self.old_pilot_id,
            &self.new_pilot_id,
            &self.resolver_id,
            self.basis,
            &self.created_at,
        )?;
        resolver_public_key(&self.resolver_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

impl RosterUpdateRecord {
    pub fn issue(
        signer_secret_key: &iroh::SecretKey,
        action: RosterUpdateAction,
        resolver_id: String,
        resolver_profile: Option<ResolverProfile>,
        created_at: impl Into<String>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }
        validate_resolver_id(&resolver_id)?;
        validate_roster_profile_presence(action, resolver_profile.as_ref())?;

        let signer_id = signer_secret_key.public().to_string();
        let record_id = derive_roster_update_record_id(
            action,
            &resolver_id,
            &signer_id,
            resolver_profile.as_ref(),
            &created_at,
        )?;
        let signature_bytes = roster_update_signing_payload(
            &record_id,
            action,
            &resolver_id,
            &signer_id,
            resolver_profile.as_ref(),
            &created_at,
        )?;
        let signature = hex::encode(signer_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: ROSTER_UPDATE_RECORD_SCHEMA.to_string(),
            schema_version: ROSTER_UPDATE_RECORD_VERSION,
            record_id,
            action,
            resolver_id,
            signer_id,
            resolver_profile,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != ROSTER_UPDATE_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != ROSTER_UPDATE_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }
        validate_resolver_id(&self.resolver_id)?;
        validate_signer_id(&self.signer_id)?;
        validate_roster_profile_presence(self.action, self.resolver_profile.as_ref())?;

        let expected_record_id = derive_roster_update_record_id(
            self.action,
            &self.resolver_id,
            &self.signer_id,
            self.resolver_profile.as_ref(),
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = roster_update_signing_payload(
            &self.record_id,
            self.action,
            &self.resolver_id,
            &self.signer_id,
            self.resolver_profile.as_ref(),
            &self.created_at,
        )?;
        signer_public_key(&self.signer_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

impl PublicationModeRecord {
    pub fn issue(
        pilot_id_secret_key: &iroh::SecretKey,
        raw_igc_hash: Blake3Hex,
        publication_mode: PublicationMode,
        protected_hash: Option<Blake3Hex>,
        supersedes: Option<Blake3Hex>,
        created_at: impl Into<String>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }
        validate_publication_mode_hash(&publication_mode, protected_hash.as_ref())?;

        let pilot_id = PilotId::from_public_key(pilot_id_secret_key.public());
        let record_id = derive_publication_mode_record_id(
            &raw_igc_hash,
            &publication_mode,
            protected_hash.as_ref(),
            &supersedes,
            &pilot_id,
            &created_at,
        )?;
        let signature_bytes = publication_mode_signing_payload(
            &record_id,
            &raw_igc_hash,
            &publication_mode,
            protected_hash.as_ref(),
            &supersedes,
            &pilot_id,
            &created_at,
        )?;
        let signature = hex::encode(pilot_id_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: PUBLICATION_MODE_RECORD_SCHEMA.to_string(),
            schema_version: PUBLICATION_MODE_RECORD_VERSION,
            record_id,
            raw_igc_hash,
            publication_mode,
            protected_hash,
            supersedes,
            pilot_id,
            signature,
            created_at,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != PUBLICATION_MODE_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != PUBLICATION_MODE_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }
        validate_publication_mode_hash(&self.publication_mode, self.protected_hash.as_ref())?;

        let expected_record_id = derive_publication_mode_record_id(
            &self.raw_igc_hash,
            &self.publication_mode,
            self.protected_hash.as_ref(),
            &self.supersedes,
            &self.pilot_id,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = publication_mode_signing_payload(
            &self.record_id,
            &self.raw_igc_hash,
            &self.publication_mode,
            self.protected_hash.as_ref(),
            &self.supersedes,
            &self.pilot_id,
            &self.created_at,
        )?;
        flight_pilot_id_public_key(&self.pilot_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

impl DeletionRequestRecord {
    pub fn issue(
        pilot_id_secret_key: &iroh::SecretKey,
        raw_igc_hash: Blake3Hex,
        created_at: impl Into<String>,
    ) -> Result<Self, FlightGovernanceRecordError> {
        let created_at = created_at.into();
        if !is_canonical_utc_timestamp(&created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(created_at));
        }

        let pilot_id = PilotId::from_public_key(pilot_id_secret_key.public());
        let record_id = derive_deletion_request_record_id(&raw_igc_hash, &pilot_id, &created_at)?;
        let signature_bytes =
            deletion_request_signing_payload(&record_id, &raw_igc_hash, &pilot_id, &created_at)?;
        let signature = hex::encode(pilot_id_secret_key.sign(&signature_bytes).to_bytes());

        let record = Self {
            schema: DELETION_REQUEST_RECORD_SCHEMA.to_string(),
            schema_version: DELETION_REQUEST_RECORD_VERSION,
            record_id,
            raw_igc_hash,
            pilot_id,
            signature,
            created_at,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FlightGovernanceRecordError> {
        if self.schema != DELETION_REQUEST_RECORD_SCHEMA {
            return Err(FlightGovernanceRecordError::Schema(self.schema.clone()));
        }
        if self.schema_version != DELETION_REQUEST_RECORD_VERSION {
            return Err(FlightGovernanceRecordError::SchemaVersion(
                self.schema_version,
            ));
        }
        if !is_canonical_utc_timestamp(&self.created_at) {
            return Err(FlightGovernanceRecordError::CreatedAt(
                self.created_at.clone(),
            ));
        }

        let expected_record_id = derive_deletion_request_record_id(
            &self.raw_igc_hash,
            &self.pilot_id,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(FlightGovernanceRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let signature = decode_flight_governance_signature_hex(&self.signature)?;
        let signing_bytes = deletion_request_signing_payload(
            &self.record_id,
            &self.raw_igc_hash,
            &self.pilot_id,
            &self.created_at,
        )?;
        flight_pilot_id_public_key(&self.pilot_id)?
            .verify(&signing_bytes, &signature)
            .map_err(|_| FlightGovernanceRecordError::SignatureVerification)?;

        Ok(())
    }
}

#[derive(Serialize)]
struct RecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    pilot_id: &'a PilotId,
    pilot_auth_did: &'a DidKey,
    supersedes: &'a Option<Blake3Hex>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct SigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    pilot_id: &'a PilotId,
    pilot_auth_did: &'a DidKey,
    supersedes: &'a Option<Blake3Hex>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PrivateAccessRotationRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    pilot_id: &'a PilotId,
    private_access_public_key: &'a str,
    supersedes: &'a Option<Blake3Hex>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PrivateAccessRotationSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    pilot_id: &'a PilotId,
    private_access_public_key: &'a str,
    supersedes: &'a Option<Blake3Hex>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct OwnerClaimRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    raw_igc_hash: &'a Blake3Hex,
    claim_type: &'static str,
    pilot_id: &'a PilotId,
    created_at: &'a str,
    evidence: &'a [serde_json::Value],
}

#[derive(Serialize)]
struct OwnerClaimSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    claim_type: &'static str,
    pilot_id: &'a PilotId,
    created_at: &'a str,
    evidence: &'a [serde_json::Value],
}

#[derive(Serialize)]
struct ClaimApprovalRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    claim_record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    resolver_id: &'a str,
    created_at: &'a str,
}

#[derive(Serialize)]
struct ClaimApprovalSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    claim_record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    resolver_id: &'a str,
    created_at: &'a str,
}

#[derive(Serialize)]
struct ClaimChallengeRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    claim_record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    challenger_resolver_id: &'a str,
    reason: &'a str,
    created_at: &'a str,
}

#[derive(Serialize)]
struct ClaimChallengeSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    claim_record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    challenger_resolver_id: &'a str,
    reason: &'a str,
    created_at: &'a str,
}

#[derive(Serialize)]
struct ClaimResolutionRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    raw_igc_hash: &'a Blake3Hex,
    claim_record_id: &'a Blake3Hex,
    resolver_id: &'a str,
    resolution: ClaimResolutionOutcome,
    basis: &'a [String],
    created_at: &'a str,
    supersedes: &'a [Blake3Hex],
}

#[derive(Serialize)]
struct ClaimResolutionSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    claim_record_id: &'a Blake3Hex,
    resolver_id: &'a str,
    resolution: ClaimResolutionOutcome,
    basis: &'a [String],
    created_at: &'a str,
    supersedes: &'a [Blake3Hex],
}

#[derive(Serialize)]
struct IdentityRecoveryRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    old_pilot_id: &'a PilotId,
    new_pilot_id: &'a PilotId,
    resolver_id: &'a str,
    basis: IdentityRecoveryBasis,
    created_at: &'a str,
}

#[derive(Serialize)]
struct IdentityRecoverySigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    old_pilot_id: &'a PilotId,
    new_pilot_id: &'a PilotId,
    resolver_id: &'a str,
    basis: IdentityRecoveryBasis,
    created_at: &'a str,
}

#[derive(Serialize)]
struct RosterUpdateRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    action: RosterUpdateAction,
    resolver_id: &'a str,
    signer_id: &'a str,
    resolver_profile: Option<&'a ResolverProfile>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct RosterUpdateSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    action: RosterUpdateAction,
    resolver_id: &'a str,
    signer_id: &'a str,
    resolver_profile: Option<&'a ResolverProfile>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PublicationModeRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    raw_igc_hash: &'a Blake3Hex,
    publication_mode: &'a PublicationMode,
    protected_hash: Option<&'a Blake3Hex>,
    supersedes: &'a Option<Blake3Hex>,
    pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PublicationModeSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    publication_mode: &'a PublicationMode,
    protected_hash: Option<&'a Blake3Hex>,
    supersedes: &'a Option<Blake3Hex>,
    pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct DeletionRequestRecordIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    raw_igc_hash: &'a Blake3Hex,
    pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct DeletionRequestSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    pilot_id: &'a PilotId,
    created_at: &'a str,
}

fn derive_record_id(
    pilot_id: &PilotId,
    pilot_auth_did: &DidKey,
    supersedes: &Option<Blake3Hex>,
    created_at: &str,
) -> Result<Blake3Hex, PilotAuthDidRecordError> {
    // The written spec says `record_id` is derived from `record_without_signature`,
    // but that becomes circular if `record_id` itself remains inside the payload.
    // The workable interpretation is to hash the unsigned payload without both
    // `record_id` and `signature`, then sign the payload that includes `record_id`.
    let payload = RecordIdPayload {
        schema: PILOT_AUTH_DID_RECORD_SCHEMA,
        schema_version: PILOT_AUTH_DID_RECORD_VERSION,
        pilot_id,
        pilot_auth_did,
        supersedes,
        created_at,
    };
    let canonical_bytes = canonical_json_bytes(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn signing_payload(
    pilot_id: &PilotId,
    record_id: &Blake3Hex,
    pilot_auth_did: &DidKey,
    supersedes: &Option<Blake3Hex>,
    created_at: &str,
) -> Result<Vec<u8>, PilotAuthDidRecordError> {
    let payload = SigningPayload {
        schema: PILOT_AUTH_DID_RECORD_SCHEMA,
        schema_version: PILOT_AUTH_DID_RECORD_VERSION,
        record_id,
        pilot_id,
        pilot_auth_did,
        supersedes,
        created_at,
    };
    canonical_json_bytes(&payload)
}

fn derive_private_access_rotation_record_id(
    pilot_id: &PilotId,
    private_access_public_key: &str,
    supersedes: &Option<Blake3Hex>,
    created_at: &str,
) -> Result<Blake3Hex, PrivateAccessRotationRecordError> {
    validate_private_access_public_key(private_access_public_key)?;
    let payload = PrivateAccessRotationRecordIdPayload {
        schema: PRIVATE_ACCESS_ROTATION_RECORD_SCHEMA,
        schema_version: PRIVATE_ACCESS_ROTATION_RECORD_VERSION,
        pilot_id,
        private_access_public_key,
        supersedes,
        created_at,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn private_access_rotation_signing_payload(
    pilot_id: &PilotId,
    record_id: &Blake3Hex,
    private_access_public_key: &str,
    supersedes: &Option<Blake3Hex>,
    created_at: &str,
) -> Result<Vec<u8>, PrivateAccessRotationRecordError> {
    validate_private_access_public_key(private_access_public_key)?;
    let payload = PrivateAccessRotationSigningPayload {
        schema: PRIVATE_ACCESS_ROTATION_RECORD_SCHEMA,
        schema_version: PRIVATE_ACCESS_ROTATION_RECORD_VERSION,
        record_id,
        pilot_id,
        private_access_public_key,
        supersedes,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_owner_claim_record_id(
    raw_igc_hash: &Blake3Hex,
    pilot_id: &PilotId,
    created_at: &str,
    evidence: &[serde_json::Value],
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    let payload = OwnerClaimRecordIdPayload {
        schema: OWNER_CLAIM_RECORD_SCHEMA,
        schema_version: OWNER_CLAIM_RECORD_VERSION,
        raw_igc_hash,
        claim_type: OWNER_CLAIM_TYPE,
        pilot_id,
        created_at,
        evidence,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn owner_claim_signing_payload(
    record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    pilot_id: &PilotId,
    created_at: &str,
    evidence: &[serde_json::Value],
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    let payload = OwnerClaimSigningPayload {
        schema: OWNER_CLAIM_RECORD_SCHEMA,
        schema_version: OWNER_CLAIM_RECORD_VERSION,
        record_id,
        raw_igc_hash,
        claim_type: OWNER_CLAIM_TYPE,
        pilot_id,
        created_at,
        evidence,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_claim_approval_record_id(
    claim_record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    resolver_id: &str,
    created_at: &str,
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    validate_resolver_id(resolver_id)?;
    let payload = ClaimApprovalRecordIdPayload {
        schema: CLAIM_APPROVAL_RECORD_SCHEMA,
        schema_version: CLAIM_APPROVAL_RECORD_VERSION,
        claim_record_id,
        raw_igc_hash,
        resolver_id,
        created_at,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn claim_approval_signing_payload(
    record_id: &Blake3Hex,
    claim_record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    resolver_id: &str,
    created_at: &str,
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    validate_resolver_id(resolver_id)?;
    let payload = ClaimApprovalSigningPayload {
        schema: CLAIM_APPROVAL_RECORD_SCHEMA,
        schema_version: CLAIM_APPROVAL_RECORD_VERSION,
        record_id,
        claim_record_id,
        raw_igc_hash,
        resolver_id,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_claim_challenge_record_id(
    claim_record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    challenger_resolver_id: &str,
    reason: &str,
    created_at: &str,
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    validate_challenger_resolver_id(challenger_resolver_id)?;
    let payload = ClaimChallengeRecordIdPayload {
        schema: CLAIM_CHALLENGE_RECORD_SCHEMA,
        schema_version: CLAIM_CHALLENGE_RECORD_VERSION,
        claim_record_id,
        raw_igc_hash,
        challenger_resolver_id,
        reason,
        created_at,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn claim_challenge_signing_payload(
    record_id: &Blake3Hex,
    claim_record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    challenger_resolver_id: &str,
    reason: &str,
    created_at: &str,
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    validate_challenger_resolver_id(challenger_resolver_id)?;
    let payload = ClaimChallengeSigningPayload {
        schema: CLAIM_CHALLENGE_RECORD_SCHEMA,
        schema_version: CLAIM_CHALLENGE_RECORD_VERSION,
        record_id,
        claim_record_id,
        raw_igc_hash,
        challenger_resolver_id,
        reason,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_claim_resolution_record_id(
    raw_igc_hash: &Blake3Hex,
    claim_record_id: &Blake3Hex,
    resolver_id: &str,
    resolution: ClaimResolutionOutcome,
    basis: &[String],
    created_at: &str,
    supersedes: &[Blake3Hex],
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    validate_resolver_id(resolver_id)?;
    let payload = ClaimResolutionRecordIdPayload {
        schema: CLAIM_RESOLUTION_RECORD_SCHEMA,
        schema_version: CLAIM_RESOLUTION_RECORD_VERSION,
        raw_igc_hash,
        claim_record_id,
        resolver_id,
        resolution,
        basis,
        created_at,
        supersedes,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn claim_resolution_signing_payload(
    record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    claim_record_id: &Blake3Hex,
    resolver_id: &str,
    resolution: ClaimResolutionOutcome,
    basis: &[String],
    created_at: &str,
    supersedes: &[Blake3Hex],
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    validate_resolver_id(resolver_id)?;
    let payload = ClaimResolutionSigningPayload {
        schema: CLAIM_RESOLUTION_RECORD_SCHEMA,
        schema_version: CLAIM_RESOLUTION_RECORD_VERSION,
        record_id,
        raw_igc_hash,
        claim_record_id,
        resolver_id,
        resolution,
        basis,
        created_at,
        supersedes,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_identity_recovery_record_id(
    old_pilot_id: &PilotId,
    new_pilot_id: &PilotId,
    resolver_id: &str,
    basis: IdentityRecoveryBasis,
    created_at: &str,
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    if old_pilot_id == new_pilot_id {
        return Err(FlightGovernanceRecordError::IdentityRecoverySamePilot);
    }
    validate_resolver_id(resolver_id)?;
    let payload = IdentityRecoveryRecordIdPayload {
        schema: IDENTITY_RECOVERY_RECORD_SCHEMA,
        schema_version: IDENTITY_RECOVERY_RECORD_VERSION,
        old_pilot_id,
        new_pilot_id,
        resolver_id,
        basis,
        created_at,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn identity_recovery_signing_payload(
    record_id: &Blake3Hex,
    old_pilot_id: &PilotId,
    new_pilot_id: &PilotId,
    resolver_id: &str,
    basis: IdentityRecoveryBasis,
    created_at: &str,
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    if old_pilot_id == new_pilot_id {
        return Err(FlightGovernanceRecordError::IdentityRecoverySamePilot);
    }
    validate_resolver_id(resolver_id)?;
    let payload = IdentityRecoverySigningPayload {
        schema: IDENTITY_RECOVERY_RECORD_SCHEMA,
        schema_version: IDENTITY_RECOVERY_RECORD_VERSION,
        record_id,
        old_pilot_id,
        new_pilot_id,
        resolver_id,
        basis,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_roster_update_record_id(
    action: RosterUpdateAction,
    resolver_id: &str,
    signer_id: &str,
    resolver_profile: Option<&ResolverProfile>,
    created_at: &str,
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    validate_resolver_id(resolver_id)?;
    validate_signer_id(signer_id)?;
    validate_roster_profile_presence(action, resolver_profile)?;
    let payload = RosterUpdateRecordIdPayload {
        schema: ROSTER_UPDATE_RECORD_SCHEMA,
        schema_version: ROSTER_UPDATE_RECORD_VERSION,
        action,
        resolver_id,
        signer_id,
        resolver_profile,
        created_at,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn roster_update_signing_payload(
    record_id: &Blake3Hex,
    action: RosterUpdateAction,
    resolver_id: &str,
    signer_id: &str,
    resolver_profile: Option<&ResolverProfile>,
    created_at: &str,
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    validate_resolver_id(resolver_id)?;
    validate_signer_id(signer_id)?;
    validate_roster_profile_presence(action, resolver_profile)?;
    let payload = RosterUpdateSigningPayload {
        schema: ROSTER_UPDATE_RECORD_SCHEMA,
        schema_version: ROSTER_UPDATE_RECORD_VERSION,
        record_id,
        action,
        resolver_id,
        signer_id,
        resolver_profile,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_publication_mode_record_id(
    raw_igc_hash: &Blake3Hex,
    publication_mode: &PublicationMode,
    protected_hash: Option<&Blake3Hex>,
    supersedes: &Option<Blake3Hex>,
    pilot_id: &PilotId,
    created_at: &str,
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    validate_publication_mode_hash(publication_mode, protected_hash)?;
    let payload = PublicationModeRecordIdPayload {
        schema: PUBLICATION_MODE_RECORD_SCHEMA,
        schema_version: PUBLICATION_MODE_RECORD_VERSION,
        raw_igc_hash,
        publication_mode,
        protected_hash,
        supersedes,
        pilot_id,
        created_at,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn publication_mode_signing_payload(
    record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    publication_mode: &PublicationMode,
    protected_hash: Option<&Blake3Hex>,
    supersedes: &Option<Blake3Hex>,
    pilot_id: &PilotId,
    created_at: &str,
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    validate_publication_mode_hash(publication_mode, protected_hash)?;
    let payload = PublicationModeSigningPayload {
        schema: PUBLICATION_MODE_RECORD_SCHEMA,
        schema_version: PUBLICATION_MODE_RECORD_VERSION,
        record_id,
        raw_igc_hash,
        publication_mode,
        protected_hash,
        supersedes,
        pilot_id,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn derive_deletion_request_record_id(
    raw_igc_hash: &Blake3Hex,
    pilot_id: &PilotId,
    created_at: &str,
) -> Result<Blake3Hex, FlightGovernanceRecordError> {
    let payload = DeletionRequestRecordIdPayload {
        schema: DELETION_REQUEST_RECORD_SCHEMA,
        schema_version: DELETION_REQUEST_RECORD_VERSION,
        raw_igc_hash,
        pilot_id,
        created_at,
    };
    let canonical_bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&canonical_bytes)))
}

fn deletion_request_signing_payload(
    record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    pilot_id: &PilotId,
    created_at: &str,
) -> Result<Vec<u8>, FlightGovernanceRecordError> {
    let payload = DeletionRequestSigningPayload {
        schema: DELETION_REQUEST_RECORD_SCHEMA,
        schema_version: DELETION_REQUEST_RECORD_VERSION,
        record_id,
        raw_igc_hash,
        pilot_id,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, PilotAuthDidRecordError> {
    Ok(json_canon::to_vec(value)?)
}

fn pilot_id_public_key(pilot_id: &PilotId) -> Result<iroh::PublicKey, PilotAuthDidRecordError> {
    let public_key_bytes = decode_fixed_hex_32(pilot_id.public_key_hex())
        .map_err(|_| PilotAuthDidRecordError::PilotIdPublicKey(pilot_id.to_string()))?;
    iroh::PublicKey::from_bytes(&public_key_bytes)
        .map_err(|_| PilotAuthDidRecordError::PilotIdPublicKey(pilot_id.to_string()))
}

fn decode_fixed_hex_32(value: &str) -> Result<[u8; 32], hex::FromHexError> {
    let bytes = hex::decode(value)?;
    bytes
        .try_into()
        .map_err(|_| hex::FromHexError::InvalidStringLength)
}

fn decode_signature_hex(value: &str) -> Result<iroh::Signature, PilotAuthDidRecordError> {
    if value.len() != 128
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(PilotAuthDidRecordError::SignatureEncoding);
    }
    let bytes = hex::decode(value).map_err(|_| PilotAuthDidRecordError::SignatureEncoding)?;
    let signature_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| PilotAuthDidRecordError::SignatureEncoding)?;
    Ok(iroh::Signature::from_bytes(&signature_bytes))
}

fn validate_private_access_public_key(value: &str) -> Result<(), PrivateAccessRotationRecordError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(PrivateAccessRotationRecordError::PrivateAccessPublicKeyEncoding);
    }
    decode_fixed_hex_32(value)
        .and_then(|bytes| {
            iroh::PublicKey::from_bytes(&bytes)
                .map(|_| bytes)
                .map_err(|_| hex::FromHexError::InvalidStringLength)
        })
        .map_err(|_| PrivateAccessRotationRecordError::PrivateAccessPublicKeyEncoding)?;
    Ok(())
}

fn validate_resolver_id(value: &str) -> Result<(), FlightGovernanceRecordError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(FlightGovernanceRecordError::ResolverIdEncoding(
            value.to_string(),
        ));
    }
    Ok(())
}

fn validate_challenger_resolver_id(value: &str) -> Result<(), FlightGovernanceRecordError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(FlightGovernanceRecordError::ChallengerResolverIdEncoding(
            value.to_string(),
        ));
    }
    Ok(())
}

fn validate_signer_id(value: &str) -> Result<(), FlightGovernanceRecordError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(FlightGovernanceRecordError::SignerIdEncoding(
            value.to_string(),
        ));
    }
    Ok(())
}

fn validate_roster_profile_presence(
    action: RosterUpdateAction,
    resolver_profile: Option<&ResolverProfile>,
) -> Result<(), FlightGovernanceRecordError> {
    match (action, resolver_profile) {
        (RosterUpdateAction::Add, Some(_)) | (RosterUpdateAction::Remove, None) => Ok(()),
        _ => Err(FlightGovernanceRecordError::RosterProfilePresence),
    }
}

fn validate_publication_mode_hash(
    mode: &PublicationMode,
    protected_hash: Option<&Blake3Hex>,
) -> Result<(), FlightGovernanceRecordError> {
    match (mode, protected_hash) {
        (PublicationMode::Protected, Some(_)) => Ok(()),
        (PublicationMode::Protected, None) => {
            Err(FlightGovernanceRecordError::ProtectedHashPresence)
        }
        (PublicationMode::Public | PublicationMode::Private, None) => Ok(()),
        (PublicationMode::Public | PublicationMode::Private, Some(_)) => {
            Err(FlightGovernanceRecordError::ProtectedHashPresence)
        }
    }
}

fn resolver_public_key(value: &str) -> Result<iroh::PublicKey, FlightGovernanceRecordError> {
    let public_key_bytes = decode_fixed_hex_32(value)
        .map_err(|_| FlightGovernanceRecordError::ResolverIdEncoding(value.to_string()))?;
    iroh::PublicKey::from_bytes(&public_key_bytes)
        .map_err(|_| FlightGovernanceRecordError::ResolverIdEncoding(value.to_string()))
}

fn signer_public_key(value: &str) -> Result<iroh::PublicKey, FlightGovernanceRecordError> {
    let public_key_bytes = decode_fixed_hex_32(value)
        .map_err(|_| FlightGovernanceRecordError::SignerIdEncoding(value.to_string()))?;
    iroh::PublicKey::from_bytes(&public_key_bytes)
        .map_err(|_| FlightGovernanceRecordError::SignerIdEncoding(value.to_string()))
}

fn decode_private_access_rotation_signature_hex(
    value: &str,
) -> Result<iroh::Signature, PrivateAccessRotationRecordError> {
    if value.len() != 128
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(PrivateAccessRotationRecordError::SignatureEncoding);
    }
    let bytes =
        hex::decode(value).map_err(|_| PrivateAccessRotationRecordError::SignatureEncoding)?;
    let signature_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| PrivateAccessRotationRecordError::SignatureEncoding)?;
    Ok(iroh::Signature::from_bytes(&signature_bytes))
}

fn flight_pilot_id_public_key(
    pilot_id: &PilotId,
) -> Result<iroh::PublicKey, FlightGovernanceRecordError> {
    let public_key_bytes = decode_fixed_hex_32(pilot_id.public_key_hex())
        .map_err(|_| FlightGovernanceRecordError::PilotIdPublicKey(pilot_id.to_string()))?;
    iroh::PublicKey::from_bytes(&public_key_bytes)
        .map_err(|_| FlightGovernanceRecordError::PilotIdPublicKey(pilot_id.to_string()))
}

fn decode_flight_governance_signature_hex(
    value: &str,
) -> Result<iroh::Signature, FlightGovernanceRecordError> {
    if value.len() != 128
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(FlightGovernanceRecordError::SignatureEncoding);
    }
    let bytes = hex::decode(value).map_err(|_| FlightGovernanceRecordError::SignatureEncoding)?;
    let signature_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| FlightGovernanceRecordError::SignatureEncoding)?;
    Ok(iroh::Signature::from_bytes(&signature_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    #[test]
    fn rauth_05_signed_record_round_trips_and_validates() {
        let pilot_root = deterministic_secret_key(41);
        let pilot_auth_did = DidKey::from_public_key(deterministic_secret_key(42).public());

        let record = PilotAuthDidRecord::issue(
            &pilot_root,
            pilot_auth_did.clone(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();

        record.validate().unwrap();
        assert_eq!(record.pilot_auth_did, pilot_auth_did);
    }

    #[test]
    fn rauth_03_rejects_invalid_did_key() {
        let err = DidKey::parse("did:key:z0bad").unwrap_err();
        assert!(matches!(err, DidKeyError::InvalidFormat(_)));
    }

    #[test]
    fn rthreat_06_record_id_mismatch_is_rejected() {
        let pilot_root = deterministic_secret_key(61);
        let mut record = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(62).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        record.record_id = Blake3Hex::parse("c".repeat(64)).unwrap();

        let err = record.validate().unwrap_err();
        assert!(matches!(
            err,
            PilotAuthDidRecordError::RecordIdMismatch { .. }
        ));
    }

    #[test]
    fn rauth_05_signature_failure_is_rejected() {
        let pilot_root = deterministic_secret_key(71);
        let mut record = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(deterministic_secret_key(72).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        record.signature = "0".repeat(128);

        let err = record.validate().unwrap_err();
        assert!(matches!(
            err,
            PilotAuthDidRecordError::SignatureVerification
        ));
    }

    #[test]
    fn created_at_must_be_canonical() {
        let err = PilotAuthDidRecord::issue(
            &deterministic_secret_key(81),
            DidKey::from_public_key(deterministic_secret_key(82).public()),
            None,
            "2026-05-01T09:14:00+00:00",
        )
        .unwrap_err();
        assert!(matches!(err, PilotAuthDidRecordError::CreatedAt(_)));
    }

    #[test]
    fn canonical_json_matches_rfc_8785_number_and_key_order_rules() {
        let value = serde_json::json!({
            "z": 1.0,
            "a": "\u{000f}",
            "m": -0.0,
        });

        let canonical = canonical_json_bytes(&value).unwrap();
        assert_eq!(canonical, br#"{"a":"\u000f","m":0,"z":1}"#);
    }

    #[test]
    fn private_access_rotation_record_round_trips_and_validates() {
        let pilot_root = deterministic_secret_key(91);
        let private_access_key = deterministic_secret_key(92);

        let record = PrivateAccessRotationRecord::issue(
            &pilot_root,
            private_access_key.public(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();

        record.validate().unwrap();
        assert_eq!(
            record.pilot_id,
            PilotId::from_public_key(pilot_root.public())
        );
        assert_eq!(
            record.private_access_public_key().unwrap(),
            private_access_key.public()
        );
    }

    #[test]
    fn private_access_rotation_rejects_signature_failure() {
        let pilot_root = deterministic_secret_key(93);
        let mut record = PrivateAccessRotationRecord::issue(
            &pilot_root,
            deterministic_secret_key(94).public(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        record.private_access_public_key = deterministic_secret_key(95).public().to_string();

        let err = record.validate().unwrap_err();
        assert!(matches!(
            err,
            PrivateAccessRotationRecordError::RecordIdMismatch { .. }
                | PrivateAccessRotationRecordError::SignatureVerification
        ));
    }

    #[test]
    fn owner_claim_record_round_trips_and_validates() {
        let pilot_root = deterministic_secret_key(101);
        let raw_igc_hash = Blake3Hex::parse("d".repeat(64)).unwrap();

        let record = OwnerClaimRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            "2026-05-01T09:14:00Z",
            Vec::new(),
        )
        .unwrap();

        record.validate().unwrap();
        assert_eq!(record.raw_igc_hash, raw_igc_hash);
        assert_eq!(record.claim_type, "owner");
        assert_eq!(
            record.pilot_id,
            PilotId::from_public_key(pilot_root.public())
        );
    }

    #[test]
    fn deletion_request_record_round_trips_and_validates() {
        let pilot_root = deterministic_secret_key(102);
        let raw_igc_hash = Blake3Hex::parse("e".repeat(64)).unwrap();

        let record =
            DeletionRequestRecord::issue(&pilot_root, raw_igc_hash.clone(), "2026-05-01T09:14:00Z")
                .unwrap();

        record.validate().unwrap();
        assert_eq!(record.raw_igc_hash, raw_igc_hash);
        assert_eq!(
            record.pilot_id,
            PilotId::from_public_key(pilot_root.public())
        );
    }

    #[test]
    fn publication_mode_record_round_trips_and_validates_hash_presence() {
        let pilot_root = deterministic_secret_key(103);
        let raw_igc_hash = Blake3Hex::parse("f".repeat(64)).unwrap();
        let protected_hash = Blake3Hex::parse("1".repeat(64)).unwrap();

        let record = PublicationModeRecord::issue(
            &pilot_root,
            raw_igc_hash.clone(),
            PublicationMode::Protected,
            Some(protected_hash.clone()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();

        record.validate().unwrap();
        assert_eq!(record.raw_igc_hash, raw_igc_hash);
        assert_eq!(record.publication_mode, PublicationMode::Protected);
        assert_eq!(record.protected_hash, Some(protected_hash));
        assert_eq!(
            record.pilot_id,
            PilotId::from_public_key(pilot_root.public())
        );

        let protected_without_hash = PublicationModeRecord::issue(
            &pilot_root,
            Blake3Hex::parse("2".repeat(64)).unwrap(),
            PublicationMode::Protected,
            None,
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap_err();
        assert!(matches!(
            protected_without_hash,
            FlightGovernanceRecordError::ProtectedHashPresence
        ));

        let public_with_hash = PublicationModeRecord::issue(
            &pilot_root,
            Blake3Hex::parse("3".repeat(64)).unwrap(),
            PublicationMode::Public,
            Some(Blake3Hex::parse("4".repeat(64)).unwrap()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap_err();
        assert!(matches!(
            public_with_hash,
            FlightGovernanceRecordError::ProtectedHashPresence
        ));
    }
}
