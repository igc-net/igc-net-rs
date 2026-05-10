//! Follow/unfollow record types.
//!
//! Implements the follow schemas described in `specs/70-groups-and-social.md §6`.

use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, PilotId};
use crate::util::{canonical_utc_now, is_canonical_utc_timestamp};

const FOLLOW_SCHEMA: &str = "igc-net/follow";
const FOLLOW_VERSION: u8 = 1;
const UNFOLLOW_SCHEMA: &str = "igc-net/unfollow";
const UNFOLLOW_VERSION: u8 = 1;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum FollowRecordError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("identifier: {0}")]
    Identifier(#[from] crate::id::IdentifierError),
    #[error("schema must be {expected:?}, got {found:?}")]
    Schema { expected: &'static str, found: String },
    #[error("schema_version must be {expected}, got {found}")]
    SchemaVersion { expected: u8, found: u8 },
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
    #[error("follower and followee must be distinct pilot IDs")]
    SelfFollow,
}

// ── FollowRecord ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FollowRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub follower_pilot_id: PilotId,
    pub followee_pilot_id: PilotId,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct FollowPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    follower_pilot_id: &'a PilotId,
    followee_pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct FollowSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    follower_pilot_id: &'a PilotId,
    followee_pilot_id: &'a PilotId,
    created_at: &'a str,
}

impl FollowRecord {
    pub fn issue(
        follower_secret_key: &iroh::SecretKey,
        followee_pilot_id: PilotId,
    ) -> Result<Self, FollowRecordError> {
        let created_at = canonical_utc_now();
        let follower_pilot_id = PilotId::from_public_key(follower_secret_key.public());
        if follower_pilot_id == followee_pilot_id {
            return Err(FollowRecordError::SelfFollow);
        }

        let id_payload = FollowPayload {
            schema: FOLLOW_SCHEMA,
            schema_version: FOLLOW_VERSION,
            follower_pilot_id: &follower_pilot_id,
            followee_pilot_id: &followee_pilot_id,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;
        let sign_payload = FollowSignPayload {
            schema: FOLLOW_SCHEMA,
            schema_version: FOLLOW_VERSION,
            record_id: &record_id,
            follower_pilot_id: &follower_pilot_id,
            followee_pilot_id: &followee_pilot_id,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(follower_secret_key, &sign_payload)?;

        let record = Self {
            schema: FOLLOW_SCHEMA.to_string(),
            schema_version: FOLLOW_VERSION,
            record_id,
            follower_pilot_id,
            followee_pilot_id,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FollowRecordError> {
        check_schema(&self.schema, FOLLOW_SCHEMA)?;
        check_schema_version(self.schema_version, FOLLOW_VERSION)?;
        check_created_at(&self.created_at)?;
        if self.follower_pilot_id == self.followee_pilot_id {
            return Err(FollowRecordError::SelfFollow);
        }

        let id_payload = FollowPayload {
            schema: FOLLOW_SCHEMA,
            schema_version: FOLLOW_VERSION,
            follower_pilot_id: &self.follower_pilot_id,
            followee_pilot_id: &self.followee_pilot_id,
            created_at: &self.created_at,
        };
        let expected = blake3_record_id(&id_payload)?;
        if self.record_id != expected {
            return Err(FollowRecordError::RecordIdMismatch { expected, found: self.record_id.clone() });
        }
        let sign_payload = FollowSignPayload {
            schema: FOLLOW_SCHEMA,
            schema_version: FOLLOW_VERSION,
            record_id: &self.record_id,
            follower_pilot_id: &self.follower_pilot_id,
            followee_pilot_id: &self.followee_pilot_id,
            created_at: &self.created_at,
        };
        verify_signature(&self.follower_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── UnfollowRecord ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnfollowRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub follower_pilot_id: PilotId,
    pub followee_pilot_id: PilotId,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct UnfollowPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    follower_pilot_id: &'a PilotId,
    followee_pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct UnfollowSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    follower_pilot_id: &'a PilotId,
    followee_pilot_id: &'a PilotId,
    created_at: &'a str,
}

impl UnfollowRecord {
    pub fn issue(
        follower_secret_key: &iroh::SecretKey,
        followee_pilot_id: PilotId,
    ) -> Result<Self, FollowRecordError> {
        let created_at = canonical_utc_now();
        let follower_pilot_id = PilotId::from_public_key(follower_secret_key.public());
        if follower_pilot_id == followee_pilot_id {
            return Err(FollowRecordError::SelfFollow);
        }

        let id_payload = UnfollowPayload {
            schema: UNFOLLOW_SCHEMA,
            schema_version: UNFOLLOW_VERSION,
            follower_pilot_id: &follower_pilot_id,
            followee_pilot_id: &followee_pilot_id,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;
        let sign_payload = UnfollowSignPayload {
            schema: UNFOLLOW_SCHEMA,
            schema_version: UNFOLLOW_VERSION,
            record_id: &record_id,
            follower_pilot_id: &follower_pilot_id,
            followee_pilot_id: &followee_pilot_id,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(follower_secret_key, &sign_payload)?;

        let record = Self {
            schema: UNFOLLOW_SCHEMA.to_string(),
            schema_version: UNFOLLOW_VERSION,
            record_id,
            follower_pilot_id,
            followee_pilot_id,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), FollowRecordError> {
        check_schema(&self.schema, UNFOLLOW_SCHEMA)?;
        check_schema_version(self.schema_version, UNFOLLOW_VERSION)?;
        check_created_at(&self.created_at)?;
        if self.follower_pilot_id == self.followee_pilot_id {
            return Err(FollowRecordError::SelfFollow);
        }

        let id_payload = UnfollowPayload {
            schema: UNFOLLOW_SCHEMA,
            schema_version: UNFOLLOW_VERSION,
            follower_pilot_id: &self.follower_pilot_id,
            followee_pilot_id: &self.followee_pilot_id,
            created_at: &self.created_at,
        };
        let expected = blake3_record_id(&id_payload)?;
        if self.record_id != expected {
            return Err(FollowRecordError::RecordIdMismatch { expected, found: self.record_id.clone() });
        }
        let sign_payload = UnfollowSignPayload {
            schema: UNFOLLOW_SCHEMA,
            schema_version: UNFOLLOW_VERSION,
            record_id: &self.record_id,
            follower_pilot_id: &self.follower_pilot_id,
            followee_pilot_id: &self.followee_pilot_id,
            created_at: &self.created_at,
        };
        verify_signature(&self.follower_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn blake3_record_id<T: serde::Serialize>(payload: &T) -> Result<Blake3Hex, FollowRecordError> {
    let bytes = json_canon::to_vec(payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&bytes)))
}

fn sign_payload_hex<T: serde::Serialize>(
    secret_key: &iroh::SecretKey,
    payload: &T,
) -> Result<String, FollowRecordError> {
    let bytes = json_canon::to_vec(payload)?;
    Ok(hex::encode(secret_key.sign(&bytes).to_bytes()))
}

fn pilot_id_public_key(pilot_id: &PilotId) -> Result<iroh::PublicKey, FollowRecordError> {
    let bytes = hex::decode(pilot_id.public_key_hex())
        .ok()
        .and_then(|b| <[u8; 32]>::try_from(b).ok())
        .ok_or_else(|| FollowRecordError::PilotIdPublicKey(pilot_id.to_string()))?;
    iroh::PublicKey::from_bytes(&bytes)
        .map_err(|_| FollowRecordError::PilotIdPublicKey(pilot_id.to_string()))
}

fn decode_signature_hex(value: &str) -> Result<iroh::Signature, FollowRecordError> {
    if value.len() != 128 || !value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(FollowRecordError::SignatureEncoding);
    }
    let bytes = hex::decode(value).map_err(|_| FollowRecordError::SignatureEncoding)?;
    let sig_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| FollowRecordError::SignatureEncoding)?;
    Ok(iroh::Signature::from_bytes(&sig_bytes))
}

fn verify_signature<T: serde::Serialize>(
    signer: &PilotId,
    signature_hex: &str,
    payload: &T,
) -> Result<(), FollowRecordError> {
    let pubkey = pilot_id_public_key(signer)?;
    let signature = decode_signature_hex(signature_hex)?;
    let bytes = json_canon::to_vec(payload)?;
    pubkey
        .verify(&bytes, &signature)
        .map_err(|_| FollowRecordError::SignatureVerification)
}

fn check_schema(found: &str, expected: &'static str) -> Result<(), FollowRecordError> {
    if found != expected {
        return Err(FollowRecordError::Schema { expected, found: found.to_string() });
    }
    Ok(())
}

fn check_schema_version(found: u8, expected: u8) -> Result<(), FollowRecordError> {
    if found != expected {
        return Err(FollowRecordError::SchemaVersion { expected, found });
    }
    Ok(())
}

fn check_created_at(value: &str) -> Result<(), FollowRecordError> {
    if !is_canonical_utc_timestamp(value) {
        return Err(FollowRecordError::CreatedAt(value.to_string()));
    }
    Ok(())
}
