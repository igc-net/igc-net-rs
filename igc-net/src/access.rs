//! Restricted-artifact fetch authorization.
//!
//! Implements the signed fetch proof described in `specs/60-keys-and-access.md §4`
//! and the server-side monotonic `seq_num` store described in R-ACCESS-11.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::util::is_lower_hex_64;

const FETCH_REQUEST_SCHEMA: &str = "igc-net/fetch-request";
const FETCH_REQUEST_SCHEMA_VERSION: u32 = 1;
const GROUP_FETCH_REQUEST_SCHEMA: &str = "igc-net/group-fetch-request";
const GROUP_FETCH_REQUEST_SCHEMA_VERSION: u32 = 1;
const SEQ_NUM_DIRNAME: &str = "seq-nums";
const GROUP_SEQ_NUM_DIRNAME: &str = "seq-nums-group";

// ── ArtifactClass ─────────────────────────────────────────────────────────────

/// The two restricted artifact classes that require a signed fetch proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactClass {
    ProtectedRawCompanion,
    PrivateRawIgc,
}

// ── FetchProof ────────────────────────────────────────────────────────────────

/// Signed fetch proof transmitted by a requester to authorize access to a
/// restricted artifact.  Corresponds to the wire JSON shape in §4.3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchProof {
    pub schema: String,
    pub schema_version: u32,
    pub raw_igc_hash: String,
    pub artifact_class: ArtifactClass,
    pub requester_key: String,
    pub seq_num: u64,
    pub signature: String,
}

/// Payload signed by the requester — all `FetchProof` fields except `signature`.
#[derive(Serialize)]
struct FetchProofPayload<'a> {
    schema: &'static str,
    schema_version: u32,
    raw_igc_hash: &'a str,
    artifact_class: &'a ArtifactClass,
    requester_key: &'a str,
    seq_num: u64,
}

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum FetchProofError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("raw_igc_hash must be 64 lowercase hex chars")]
    InvalidHash,
    #[error("seq_num must be ≥ 1")]
    SeqNumZero,
    #[error("signature must be 128 lowercase hex chars")]
    InvalidSignatureEncoding,
    #[error("requester_key must be a valid 64-char lowercase hex Ed25519 public key")]
    InvalidRequesterKey,
    #[error("signature verification failed")]
    SignatureVerification,
    #[error("requester_key does not match the authorized public key")]
    RequesterKeyMismatch,
    #[error("signed artifact_class does not match the expected artifact class")]
    ArtifactClassMismatch,
    #[error("seq_num {got} is not strictly greater than last seen {last_seen}")]
    SeqNumNotMonotonic { got: u64, last_seen: u64 },
}

// ── Signing ───────────────────────────────────────────────────────────────────

/// Build and sign a fetch proof for a restricted artifact.
///
/// `seq_num` must be ≥ 1 and strictly greater than the serving node's
/// last-accepted value for this `requester_key` (R-ACCESS-11).
pub fn sign_fetch_proof(
    raw_igc_hash: &str,
    artifact_class: ArtifactClass,
    seq_num: u64,
    private_key: &iroh::SecretKey,
) -> Result<FetchProof, FetchProofError> {
    if !is_lower_hex_64(raw_igc_hash) {
        return Err(FetchProofError::InvalidHash);
    }
    if seq_num == 0 {
        return Err(FetchProofError::SeqNumZero);
    }

    let requester_key = private_key.public().to_string();
    let payload = FetchProofPayload {
        schema: FETCH_REQUEST_SCHEMA,
        schema_version: FETCH_REQUEST_SCHEMA_VERSION,
        raw_igc_hash,
        artifact_class: &artifact_class,
        requester_key: &requester_key,
        seq_num,
    };
    let signing_bytes = json_canon::to_vec(&payload)?;
    let signature = hex::encode(private_key.sign(&signing_bytes).to_bytes());

    Ok(FetchProof {
        schema: FETCH_REQUEST_SCHEMA.to_string(),
        schema_version: FETCH_REQUEST_SCHEMA_VERSION,
        raw_igc_hash: raw_igc_hash.to_string(),
        artifact_class,
        requester_key,
        seq_num,
        signature,
    })
}

// ── Verification ──────────────────────────────────────────────────────────────

/// Verify a fetch proof received by a serving node.
///
/// Checks (in order):
/// 1. `requester_key` matches `authorized_public_key` (R-ACCESS-10)
/// 2. `artifact_class` matches `expected_artifact_class` (R-ACCESS-28)
/// 3. `seq_num ≥ 1` and `seq_num > last_seen_seq_num` (R-ACCESS-11)
/// 4. Ed25519 signature is valid (R-ACCESS-08)
///
/// The caller MUST call `SeqNumStore::advance` and durably persist the new
/// `seq_num` before transmitting any bytes (R-ACCESS-11, R-ACCESS-13).
pub fn verify_fetch_proof(
    proof: &FetchProof,
    authorized_public_key: &iroh::PublicKey,
    expected_artifact_class: &ArtifactClass,
    last_seen_seq_num: u64,
) -> Result<(), FetchProofError> {
    // R-ACCESS-10: requester_key must match the authorized key.
    if proof.requester_key != authorized_public_key.to_string() {
        return Err(FetchProofError::RequesterKeyMismatch);
    }

    // R-ACCESS-28: signed artifact_class must match.
    if &proof.artifact_class != expected_artifact_class {
        return Err(FetchProofError::ArtifactClassMismatch);
    }

    // R-ACCESS-11: seq_num must be positive and strictly increasing.
    if proof.seq_num == 0 {
        return Err(FetchProofError::SeqNumZero);
    }
    if proof.seq_num <= last_seen_seq_num {
        return Err(FetchProofError::SeqNumNotMonotonic {
            got: proof.seq_num,
            last_seen: last_seen_seq_num,
        });
    }

    // R-ACCESS-09: raw_igc_hash must be well-formed.
    if !is_lower_hex_64(&proof.raw_igc_hash) {
        return Err(FetchProofError::InvalidHash);
    }

    // R-ACCESS-08: Ed25519 signature must be valid.
    let signature = decode_signature_hex(&proof.signature)?;
    let payload = FetchProofPayload {
        schema: FETCH_REQUEST_SCHEMA,
        schema_version: FETCH_REQUEST_SCHEMA_VERSION,
        raw_igc_hash: &proof.raw_igc_hash,
        artifact_class: &proof.artifact_class,
        requester_key: &proof.requester_key,
        seq_num: proof.seq_num,
    };
    let signing_bytes = json_canon::to_vec(&payload)?;
    authorized_public_key
        .verify(&signing_bytes, &signature)
        .map_err(|_| FetchProofError::SignatureVerification)?;

    Ok(())
}

fn decode_signature_hex(value: &str) -> Result<iroh::Signature, FetchProofError> {
    if value.len() != 128
        || !value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(FetchProofError::InvalidSignatureEncoding);
    }
    let bytes = hex::decode(value).map_err(|_| FetchProofError::InvalidSignatureEncoding)?;
    let sig_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| FetchProofError::InvalidSignatureEncoding)?;
    Ok(iroh::Signature::from_bytes(&sig_bytes))
}

// ── GroupFetchProof ───────────────────────────────────────────────────────────

/// Signed group-based fetch proof.  The requester proves membership via their
/// root pilot identity key (the pubkey embedded in `requester_pilot_id`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupFetchProof {
    pub schema: String,
    pub schema_version: u32,
    pub raw_igc_hash: String,
    pub artifact_class: ArtifactClass,
    pub requester_pilot_id: String,
    pub group_id: String,
    pub seq_num: u64,
    pub signature: String,
}

/// Payload signed by the requester — all `GroupFetchProof` fields except `signature`.
#[derive(Serialize)]
struct GroupFetchProofPayload<'a> {
    schema: &'static str,
    schema_version: u32,
    raw_igc_hash: &'a str,
    artifact_class: &'a ArtifactClass,
    requester_pilot_id: &'a str,
    group_id: &'a str,
    seq_num: u64,
}

// ── GroupFetchProofError ──────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum GroupFetchProofError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("raw_igc_hash must be 64 lowercase hex chars")]
    InvalidHash,
    #[error("seq_num must be ≥ 1")]
    SeqNumZero,
    #[error("signature must be 128 lowercase hex chars")]
    InvalidSignatureEncoding,
    #[error("requester_pilot_id is not a valid PilotId (expected igcnet:id:<64-hex>)")]
    InvalidRequesterPilotId,
    #[error("group_id is not a valid GroupId (expected igcnet:group:<32-hex>)")]
    InvalidGroupId,
    #[error("signature verification failed")]
    SignatureVerification,
    #[error("signed artifact_class does not match the expected artifact class")]
    ArtifactClassMismatch,
    #[error("seq_num {got} is not strictly greater than last seen {last_seen}")]
    SeqNumNotMonotonic { got: u64, last_seen: u64 },
}

// ── Signing ───────────────────────────────────────────────────────────────────

/// Build and sign a group-based fetch proof using the pilot's root identity key.
pub fn sign_group_fetch_proof(
    raw_igc_hash: &str,
    artifact_class: ArtifactClass,
    requester_pilot_id: &str,
    group_id: &str,
    seq_num: u64,
    pilot_root_secret_key: &iroh::SecretKey,
) -> Result<GroupFetchProof, GroupFetchProofError> {
    if !is_lower_hex_64(raw_igc_hash) {
        return Err(GroupFetchProofError::InvalidHash);
    }
    if seq_num == 0 {
        return Err(GroupFetchProofError::SeqNumZero);
    }
    parse_pilot_id(requester_pilot_id)?;
    parse_group_id(group_id)?;

    let payload = GroupFetchProofPayload {
        schema: GROUP_FETCH_REQUEST_SCHEMA,
        schema_version: GROUP_FETCH_REQUEST_SCHEMA_VERSION,
        raw_igc_hash,
        artifact_class: &artifact_class,
        requester_pilot_id,
        group_id,
        seq_num,
    };
    let signing_bytes = json_canon::to_vec(&payload)?;
    let signature = hex::encode(pilot_root_secret_key.sign(&signing_bytes).to_bytes());

    Ok(GroupFetchProof {
        schema: GROUP_FETCH_REQUEST_SCHEMA.to_string(),
        schema_version: GROUP_FETCH_REQUEST_SCHEMA_VERSION,
        raw_igc_hash: raw_igc_hash.to_string(),
        artifact_class,
        requester_pilot_id: requester_pilot_id.to_string(),
        group_id: group_id.to_string(),
        seq_num,
        signature,
    })
}

// ── Verification ──────────────────────────────────────────────────────────────

/// Verify a group-fetch proof received by a serving node.
///
/// Checks (in order):
/// 1. `requester_pilot_id` is a valid PilotId
/// 2. `artifact_class` matches `expected_artifact_class`
/// 3. `seq_num ≥ 1` and `seq_num > last_seen_seq_num`
/// 4. Ed25519 signature is valid (pubkey extracted from `requester_pilot_id`)
pub fn verify_group_fetch_proof(
    proof: &GroupFetchProof,
    expected_artifact_class: &ArtifactClass,
    last_seen_seq_num: u64,
) -> Result<(), GroupFetchProofError> {
    let authorized_public_key = parse_pilot_id(&proof.requester_pilot_id)?;
    parse_group_id(&proof.group_id)?;

    if &proof.artifact_class != expected_artifact_class {
        return Err(GroupFetchProofError::ArtifactClassMismatch);
    }
    if proof.seq_num == 0 {
        return Err(GroupFetchProofError::SeqNumZero);
    }
    if proof.seq_num <= last_seen_seq_num {
        return Err(GroupFetchProofError::SeqNumNotMonotonic {
            got: proof.seq_num,
            last_seen: last_seen_seq_num,
        });
    }
    if !is_lower_hex_64(&proof.raw_igc_hash) {
        return Err(GroupFetchProofError::InvalidHash);
    }

    let signature = decode_signature_hex(&proof.signature)
        .map_err(|_| GroupFetchProofError::InvalidSignatureEncoding)?;
    let payload = GroupFetchProofPayload {
        schema: GROUP_FETCH_REQUEST_SCHEMA,
        schema_version: GROUP_FETCH_REQUEST_SCHEMA_VERSION,
        raw_igc_hash: &proof.raw_igc_hash,
        artifact_class: &proof.artifact_class,
        requester_pilot_id: &proof.requester_pilot_id,
        group_id: &proof.group_id,
        seq_num: proof.seq_num,
    };
    let signing_bytes = json_canon::to_vec(&payload)?;
    authorized_public_key
        .verify(&signing_bytes, &signature)
        .map_err(|_| GroupFetchProofError::SignatureVerification)?;

    Ok(())
}

fn parse_pilot_id(pilot_id: &str) -> Result<iroh::PublicKey, GroupFetchProofError> {
    let key_hex = pilot_id
        .strip_prefix("igcnet:id:")
        .filter(|h| is_lower_hex_64(h))
        .ok_or(GroupFetchProofError::InvalidRequesterPilotId)?;
    let bytes = hex::decode(key_hex).map_err(|_| GroupFetchProofError::InvalidRequesterPilotId)?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| GroupFetchProofError::InvalidRequesterPilotId)?;
    iroh::PublicKey::from_bytes(&arr).map_err(|_| GroupFetchProofError::InvalidRequesterPilotId)
}

fn parse_group_id(group_id: &str) -> Result<(), GroupFetchProofError> {
    let id_hex = group_id
        .strip_prefix("igcnet:group:")
        .ok_or(GroupFetchProofError::InvalidGroupId)?;
    if id_hex.len() != 32 || !id_hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(GroupFetchProofError::InvalidGroupId);
    }
    Ok(())
}

// ── SeqNumStore ───────────────────────────────────────────────────────────────

/// Server-side durable store for the last-accepted `seq_num` per requester key.
///
/// Backed by one JSON file per requester key under `{root}/`.  Writes are
/// atomic (tmp-file + rename) and fsynced before rename so that a crash after
/// bytes are transmitted cannot allow a replay.
pub struct SeqNumStore {
    root: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum SeqNumStoreError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("new seq_num {new} is not greater than stored {stored}")]
    NotMonotonic { new: u64, stored: u64 },
}

#[derive(Serialize, Deserialize)]
struct SeqNumRecord {
    seq_num: u64,
}

impl SeqNumStore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn for_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::open(data_dir.as_ref().join(SEQ_NUM_DIRNAME))
    }

    pub fn for_group_fetch_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::open(data_dir.as_ref().join(GROUP_SEQ_NUM_DIRNAME))
    }

    /// Return the last accepted `seq_num` for this requester key, or `0` if
    /// the key has never been seen.
    pub fn last_seen(&self, requester_key_hex: &str) -> Result<u64, SeqNumStoreError> {
        let path = self.seq_file_path(requester_key_hex);
        if !path.exists() {
            return Ok(0);
        }
        let bytes = std::fs::read(&path)?;
        let record: SeqNumRecord = serde_json::from_slice(&bytes)?;
        Ok(record.seq_num)
    }

    /// Durably advance the stored `seq_num` for this requester key.
    ///
    /// Fsync + atomic rename ensures the update survives a crash (R-ACCESS-11).
    /// Returns `SeqNumStoreError::NotMonotonic` if `new_seq_num <= current`.
    pub fn advance(
        &self,
        requester_key_hex: &str,
        new_seq_num: u64,
    ) -> Result<(), SeqNumStoreError> {
        let current = self.last_seen(requester_key_hex)?;
        if new_seq_num <= current {
            return Err(SeqNumStoreError::NotMonotonic {
                new: new_seq_num,
                stored: current,
            });
        }
        self.write_seq_num(requester_key_hex, new_seq_num)
    }

    fn seq_file_path(&self, requester_key_hex: &str) -> PathBuf {
        self.root.join(format!("{requester_key_hex}.json"))
    }

    fn write_seq_num(&self, requester_key_hex: &str, seq_num: u64) -> Result<(), SeqNumStoreError> {
        std::fs::create_dir_all(&self.root)?;
        let record = SeqNumRecord { seq_num };
        let data = serde_json::to_vec(&record)?;
        let tmp_name = format!(".{requester_key_hex}-{}.tmp", rand::random::<u64>());
        let tmp_path = self.root.join(tmp_name);
        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            file.write_all(&data)?;
            file.flush()?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, self.seq_file_path(requester_key_hex))?;
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    fn valid_hash() -> &'static str {
        "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    }

    // ── sign_fetch_proof ──────────────────────────────────────────────────────

    #[test]
    fn sign_round_trip_private_raw_igc() {
        let key = secret_key(1);
        let proof = sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 1, &key).unwrap();

        assert_eq!(proof.schema, "igc-net/fetch-request");
        assert_eq!(proof.schema_version, 1);
        assert_eq!(proof.raw_igc_hash, valid_hash());
        assert_eq!(proof.artifact_class, ArtifactClass::PrivateRawIgc);
        assert_eq!(proof.seq_num, 1);
        assert_eq!(proof.requester_key, key.public().to_string());
        assert_eq!(proof.signature.len(), 128);
    }

    #[test]
    fn sign_round_trip_protected_raw_companion() {
        let key = secret_key(2);
        let proof =
            sign_fetch_proof(valid_hash(), ArtifactClass::ProtectedRawCompanion, 42, &key).unwrap();
        assert_eq!(proof.artifact_class, ArtifactClass::ProtectedRawCompanion);
        assert_eq!(proof.seq_num, 42);
    }

    #[test]
    fn sign_rejects_invalid_hash() {
        let key = secret_key(3);
        assert!(matches!(
            sign_fetch_proof("not-a-hash", ArtifactClass::PrivateRawIgc, 1, &key),
            Err(FetchProofError::InvalidHash)
        ));
        assert!(matches!(
            sign_fetch_proof(&"a".repeat(63), ArtifactClass::PrivateRawIgc, 1, &key),
            Err(FetchProofError::InvalidHash)
        ));
        assert!(matches!(
            sign_fetch_proof(&"A".repeat(64), ArtifactClass::PrivateRawIgc, 1, &key),
            Err(FetchProofError::InvalidHash)
        ));
    }

    #[test]
    fn sign_rejects_zero_seq_num() {
        let key = secret_key(4);
        assert!(matches!(
            sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 0, &key),
            Err(FetchProofError::SeqNumZero)
        ));
    }

    // ── verify_fetch_proof ────────────────────────────────────────────────────

    #[test]
    fn verify_accepts_valid_proof() {
        let key = secret_key(10);
        let proof = sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 5, &key).unwrap();

        verify_fetch_proof(&proof, &key.public(), &ArtifactClass::PrivateRawIgc, 4).unwrap();
    }

    #[test]
    fn verify_rejects_wrong_requester_key() {
        let signer = secret_key(11);
        let other = secret_key(12);
        let proof =
            sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 1, &signer).unwrap();

        assert!(matches!(
            verify_fetch_proof(&proof, &other.public(), &ArtifactClass::PrivateRawIgc, 0),
            Err(FetchProofError::RequesterKeyMismatch)
        ));
    }

    #[test]
    fn verify_rejects_artifact_class_mismatch() {
        let key = secret_key(13);
        let proof = sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 1, &key).unwrap();

        assert!(matches!(
            verify_fetch_proof(
                &proof,
                &key.public(),
                &ArtifactClass::ProtectedRawCompanion,
                0
            ),
            Err(FetchProofError::ArtifactClassMismatch)
        ));
    }

    #[test]
    fn verify_rejects_replayed_seq_num() {
        let key = secret_key(14);
        let proof = sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 3, &key).unwrap();

        // seq_num 3 equal to last_seen 3 — rejected
        assert!(matches!(
            verify_fetch_proof(&proof, &key.public(), &ArtifactClass::PrivateRawIgc, 3),
            Err(FetchProofError::SeqNumNotMonotonic { .. })
        ));
        // seq_num 3 less than last_seen 4 — rejected
        assert!(matches!(
            verify_fetch_proof(&proof, &key.public(), &ArtifactClass::PrivateRawIgc, 4),
            Err(FetchProofError::SeqNumNotMonotonic { .. })
        ));
    }

    #[test]
    fn verify_rejects_zero_seq_num() {
        let key = secret_key(15);
        // Fabricate a proof with seq_num = 0 (bypassing sign_fetch_proof validation)
        let mut proof =
            sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 1, &key).unwrap();
        proof.seq_num = 0;
        assert!(matches!(
            verify_fetch_proof(&proof, &key.public(), &ArtifactClass::PrivateRawIgc, 0),
            Err(FetchProofError::SeqNumZero)
        ));
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let key = secret_key(16);
        let mut proof =
            sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 1, &key).unwrap();
        // Flip one hex digit in the signature.
        let last = proof.signature.pop().unwrap();
        proof.signature.push(if last == 'a' { 'b' } else { 'a' });

        assert!(matches!(
            verify_fetch_proof(&proof, &key.public(), &ArtifactClass::PrivateRawIgc, 0),
            Err(FetchProofError::SignatureVerification)
        ));
    }

    #[test]
    fn verify_rejects_tampered_seq_num() {
        let key = secret_key(17);
        let mut proof =
            sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 5, &key).unwrap();
        // Attacker bumps seq_num after signing — signature no longer valid.
        proof.seq_num = 6;

        assert!(matches!(
            verify_fetch_proof(&proof, &key.public(), &ArtifactClass::PrivateRawIgc, 0),
            Err(FetchProofError::SignatureVerification)
        ));
    }

    #[test]
    fn verify_rejects_tampered_artifact_class() {
        let key = secret_key(18);
        let mut proof =
            sign_fetch_proof(valid_hash(), ArtifactClass::PrivateRawIgc, 1, &key).unwrap();
        // Attacker swaps artifact_class after signing.
        proof.artifact_class = ArtifactClass::ProtectedRawCompanion;

        assert!(matches!(
            verify_fetch_proof(
                &proof,
                &key.public(),
                &ArtifactClass::ProtectedRawCompanion,
                0
            ),
            Err(FetchProofError::SignatureVerification)
        ));
    }

    #[test]
    fn proof_serializes_to_expected_json_field_names() {
        let key = secret_key(19);
        let proof =
            sign_fetch_proof(valid_hash(), ArtifactClass::ProtectedRawCompanion, 1, &key).unwrap();
        let json = serde_json::to_value(&proof).unwrap();
        assert_eq!(json["schema"], "igc-net/fetch-request");
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["artifact_class"], "protected_raw_companion");
        assert_eq!(json["seq_num"], 1);
        assert!(json["signature"].as_str().unwrap().len() == 128);
    }

    // ── SeqNumStore ───────────────────────────────────────────────────────────

    fn temp_seq_store() -> (SeqNumStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = SeqNumStore::open(dir.path());
        (store, dir)
    }

    const REQUESTER_KEY: &str = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

    #[test]
    fn last_seen_returns_zero_for_unknown_key() {
        let (store, _dir) = temp_seq_store();
        assert_eq!(store.last_seen(REQUESTER_KEY).unwrap(), 0);
    }

    #[test]
    fn advance_and_last_seen_round_trip() {
        let (store, _dir) = temp_seq_store();
        store.advance(REQUESTER_KEY, 1).unwrap();
        assert_eq!(store.last_seen(REQUESTER_KEY).unwrap(), 1);
        store.advance(REQUESTER_KEY, 100).unwrap();
        assert_eq!(store.last_seen(REQUESTER_KEY).unwrap(), 100);
    }

    #[test]
    fn advance_rejects_equal_seq_num() {
        let (store, _dir) = temp_seq_store();
        store.advance(REQUESTER_KEY, 5).unwrap();
        assert!(matches!(
            store.advance(REQUESTER_KEY, 5),
            Err(SeqNumStoreError::NotMonotonic { .. })
        ));
    }

    #[test]
    fn advance_rejects_lower_seq_num() {
        let (store, _dir) = temp_seq_store();
        store.advance(REQUESTER_KEY, 10).unwrap();
        assert!(matches!(
            store.advance(REQUESTER_KEY, 9),
            Err(SeqNumStoreError::NotMonotonic { .. })
        ));
    }

    #[test]
    fn seq_num_survives_store_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = SeqNumStore::open(dir.path());
            store.advance(REQUESTER_KEY, 42).unwrap();
        }
        let store = SeqNumStore::open(dir.path());
        assert_eq!(store.last_seen(REQUESTER_KEY).unwrap(), 42);
    }

    #[test]
    fn seq_num_is_per_requester_key() {
        let (store, _dir) = temp_seq_store();
        let key_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        store.advance(REQUESTER_KEY, 10).unwrap();
        store.advance(key_b, 3).unwrap();
        assert_eq!(store.last_seen(REQUESTER_KEY).unwrap(), 10);
        assert_eq!(store.last_seen(key_b).unwrap(), 3);
    }
}
