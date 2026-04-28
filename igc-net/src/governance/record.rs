use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, PilotId};
use crate::identity::{DidKey, DidKeyError};
use crate::util::is_canonical_utc_timestamp;

const PILOT_AUTH_DID_RECORD_SCHEMA: &str = "igc-net/pilot-auth-did-record";
const PILOT_AUTH_DID_RECORD_VERSION: u8 = 1;
const PRIVATE_ACCESS_ROTATION_RECORD_SCHEMA: &str = "igc-net/private-access-rotation-record";
const PRIVATE_ACCESS_ROTATION_RECORD_VERSION: u8 = 1;

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
}
