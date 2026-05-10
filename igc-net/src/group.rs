//! Group record types and signed-record helpers.
//!
//! Implements the group and social schemas described in `specs/75-groups-and-social.md`.

use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, GroupId, PilotId};
use crate::util::{canonical_utc_now, is_canonical_utc_timestamp};

// ── Schema constants ──────────────────────────────────────────────────────────

const GROUP_CREATION_SCHEMA: &str = "igc-net/group-creation";
const GROUP_CREATION_VERSION: u8 = 1;
const PRIVATE_MEMBER_ADD_SCHEMA: &str = "igc-net/private-group-member-add";
const PRIVATE_MEMBER_ADD_VERSION: u8 = 1;
const PRIVATE_MEMBER_REMOVE_SCHEMA: &str = "igc-net/private-group-member-remove";
const PRIVATE_MEMBER_REMOVE_VERSION: u8 = 1;
const PUBLIC_GROUP_INVITE_SCHEMA: &str = "igc-net/public-group-invite";
const PUBLIC_GROUP_INVITE_VERSION: u8 = 1;
const PUBLIC_GROUP_ACCEPT_SCHEMA: &str = "igc-net/public-group-accept";
const PUBLIC_GROUP_ACCEPT_VERSION: u8 = 1;
const PUBLIC_GROUP_LEAVE_SCHEMA: &str = "igc-net/public-group-leave";
const PUBLIC_GROUP_LEAVE_VERSION: u8 = 1;

// ── GroupType ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupType {
    Private,
    Public,
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum GroupRecordError {
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
    #[error("group_id mismatch: expected {expected}, found {found}")]
    GroupIdMismatch {
        expected: GroupId,
        found: GroupId,
    },
    #[error("signature verification failed")]
    SignatureVerification,
}

// ── GroupCreationRecord ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupCreationRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub group_id: GroupId,
    pub group_type: GroupType,
    pub creator_pilot_id: PilotId,
    pub name: Option<String>,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct GroupCreationPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    group_id: &'a GroupId,
    group_type: &'a GroupType,
    creator_pilot_id: &'a PilotId,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: &'a Option<String>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct GroupCreationSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    group_id: &'a GroupId,
    group_type: &'a GroupType,
    creator_pilot_id: &'a PilotId,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: &'a Option<String>,
    created_at: &'a str,
}

impl GroupCreationRecord {
    pub fn issue(
        pilot_id_secret_key: &iroh::SecretKey,
        group_type: GroupType,
        name: Option<String>,
    ) -> Result<Self, GroupRecordError> {
        let created_at = canonical_utc_now();
        let creator_pilot_id = PilotId::from_public_key(pilot_id_secret_key.public());
        let group_id = GroupId::derive(&group_type, &creator_pilot_id, &name, &created_at)?;

        let id_payload = GroupCreationPayload {
            schema: GROUP_CREATION_SCHEMA,
            schema_version: GROUP_CREATION_VERSION,
            group_id: &group_id,
            group_type: &group_type,
            creator_pilot_id: &creator_pilot_id,
            name: &name,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;

        let sign_payload = GroupCreationSignPayload {
            schema: GROUP_CREATION_SCHEMA,
            schema_version: GROUP_CREATION_VERSION,
            record_id: &record_id,
            group_id: &group_id,
            group_type: &group_type,
            creator_pilot_id: &creator_pilot_id,
            name: &name,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(pilot_id_secret_key, &sign_payload)?;

        let record = Self {
            schema: GROUP_CREATION_SCHEMA.to_string(),
            schema_version: GROUP_CREATION_VERSION,
            record_id,
            group_id,
            group_type,
            creator_pilot_id,
            name,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), GroupRecordError> {
        check_schema(&self.schema, GROUP_CREATION_SCHEMA)?;
        check_schema_version(self.schema_version, GROUP_CREATION_VERSION)?;
        check_created_at(&self.created_at)?;

        let expected_group_id =
            GroupId::derive(&self.group_type, &self.creator_pilot_id, &self.name, &self.created_at)?;
        if self.group_id != expected_group_id {
            return Err(GroupRecordError::GroupIdMismatch {
                expected: expected_group_id,
                found: self.group_id.clone(),
            });
        }

        let id_payload = GroupCreationPayload {
            schema: GROUP_CREATION_SCHEMA,
            schema_version: GROUP_CREATION_VERSION,
            group_id: &self.group_id,
            group_type: &self.group_type,
            creator_pilot_id: &self.creator_pilot_id,
            name: &self.name,
            created_at: &self.created_at,
        };
        let expected_record_id = blake3_record_id(&id_payload)?;
        if self.record_id != expected_record_id {
            return Err(GroupRecordError::RecordIdMismatch {
                expected: expected_record_id,
                found: self.record_id.clone(),
            });
        }

        let sign_payload = GroupCreationSignPayload {
            schema: GROUP_CREATION_SCHEMA,
            schema_version: GROUP_CREATION_VERSION,
            record_id: &self.record_id,
            group_id: &self.group_id,
            group_type: &self.group_type,
            creator_pilot_id: &self.creator_pilot_id,
            name: &self.name,
            created_at: &self.created_at,
        };
        verify_signature(&self.creator_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── PrivateGroupMemberAddRecord ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrivateGroupMemberAddRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub group_id: GroupId,
    pub member_pilot_id: PilotId,
    pub added_by_pilot_id: PilotId,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct PrivateMemberAddPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    added_by_pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PrivateMemberAddSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    added_by_pilot_id: &'a PilotId,
    created_at: &'a str,
}

impl PrivateGroupMemberAddRecord {
    pub fn issue(
        owner_secret_key: &iroh::SecretKey,
        group_id: GroupId,
        member_pilot_id: PilotId,
    ) -> Result<Self, GroupRecordError> {
        let created_at = canonical_utc_now();
        let added_by_pilot_id = PilotId::from_public_key(owner_secret_key.public());

        let id_payload = PrivateMemberAddPayload {
            schema: PRIVATE_MEMBER_ADD_SCHEMA,
            schema_version: PRIVATE_MEMBER_ADD_VERSION,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            added_by_pilot_id: &added_by_pilot_id,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;
        let sign_payload = PrivateMemberAddSignPayload {
            schema: PRIVATE_MEMBER_ADD_SCHEMA,
            schema_version: PRIVATE_MEMBER_ADD_VERSION,
            record_id: &record_id,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            added_by_pilot_id: &added_by_pilot_id,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(owner_secret_key, &sign_payload)?;

        let record = Self {
            schema: PRIVATE_MEMBER_ADD_SCHEMA.to_string(),
            schema_version: PRIVATE_MEMBER_ADD_VERSION,
            record_id,
            group_id,
            member_pilot_id,
            added_by_pilot_id,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), GroupRecordError> {
        check_schema(&self.schema, PRIVATE_MEMBER_ADD_SCHEMA)?;
        check_schema_version(self.schema_version, PRIVATE_MEMBER_ADD_VERSION)?;
        check_created_at(&self.created_at)?;

        let id_payload = PrivateMemberAddPayload {
            schema: PRIVATE_MEMBER_ADD_SCHEMA,
            schema_version: PRIVATE_MEMBER_ADD_VERSION,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            added_by_pilot_id: &self.added_by_pilot_id,
            created_at: &self.created_at,
        };
        let expected = blake3_record_id(&id_payload)?;
        if self.record_id != expected {
            return Err(GroupRecordError::RecordIdMismatch { expected, found: self.record_id.clone() });
        }
        let sign_payload = PrivateMemberAddSignPayload {
            schema: PRIVATE_MEMBER_ADD_SCHEMA,
            schema_version: PRIVATE_MEMBER_ADD_VERSION,
            record_id: &self.record_id,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            added_by_pilot_id: &self.added_by_pilot_id,
            created_at: &self.created_at,
        };
        verify_signature(&self.added_by_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── PrivateGroupMemberRemoveRecord ────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrivateGroupMemberRemoveRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub group_id: GroupId,
    pub member_pilot_id: PilotId,
    pub removed_by_pilot_id: PilotId,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct PrivateMemberRemovePayload<'a> {
    schema: &'static str,
    schema_version: u8,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    removed_by_pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PrivateMemberRemoveSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    removed_by_pilot_id: &'a PilotId,
    created_at: &'a str,
}

impl PrivateGroupMemberRemoveRecord {
    pub fn issue(
        owner_secret_key: &iroh::SecretKey,
        group_id: GroupId,
        member_pilot_id: PilotId,
    ) -> Result<Self, GroupRecordError> {
        let created_at = canonical_utc_now();
        let removed_by_pilot_id = PilotId::from_public_key(owner_secret_key.public());

        let id_payload = PrivateMemberRemovePayload {
            schema: PRIVATE_MEMBER_REMOVE_SCHEMA,
            schema_version: PRIVATE_MEMBER_REMOVE_VERSION,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            removed_by_pilot_id: &removed_by_pilot_id,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;
        let sign_payload = PrivateMemberRemoveSignPayload {
            schema: PRIVATE_MEMBER_REMOVE_SCHEMA,
            schema_version: PRIVATE_MEMBER_REMOVE_VERSION,
            record_id: &record_id,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            removed_by_pilot_id: &removed_by_pilot_id,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(owner_secret_key, &sign_payload)?;

        let record = Self {
            schema: PRIVATE_MEMBER_REMOVE_SCHEMA.to_string(),
            schema_version: PRIVATE_MEMBER_REMOVE_VERSION,
            record_id,
            group_id,
            member_pilot_id,
            removed_by_pilot_id,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), GroupRecordError> {
        check_schema(&self.schema, PRIVATE_MEMBER_REMOVE_SCHEMA)?;
        check_schema_version(self.schema_version, PRIVATE_MEMBER_REMOVE_VERSION)?;
        check_created_at(&self.created_at)?;

        let id_payload = PrivateMemberRemovePayload {
            schema: PRIVATE_MEMBER_REMOVE_SCHEMA,
            schema_version: PRIVATE_MEMBER_REMOVE_VERSION,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            removed_by_pilot_id: &self.removed_by_pilot_id,
            created_at: &self.created_at,
        };
        let expected = blake3_record_id(&id_payload)?;
        if self.record_id != expected {
            return Err(GroupRecordError::RecordIdMismatch { expected, found: self.record_id.clone() });
        }
        let sign_payload = PrivateMemberRemoveSignPayload {
            schema: PRIVATE_MEMBER_REMOVE_SCHEMA,
            schema_version: PRIVATE_MEMBER_REMOVE_VERSION,
            record_id: &self.record_id,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            removed_by_pilot_id: &self.removed_by_pilot_id,
            created_at: &self.created_at,
        };
        verify_signature(&self.removed_by_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── PublicGroupInviteRecord ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicGroupInviteRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub group_id: GroupId,
    pub invited_pilot_id: PilotId,
    pub invited_by_pilot_id: PilotId,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct PublicInvitePayload<'a> {
    schema: &'static str,
    schema_version: u8,
    group_id: &'a GroupId,
    invited_pilot_id: &'a PilotId,
    invited_by_pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PublicInviteSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    group_id: &'a GroupId,
    invited_pilot_id: &'a PilotId,
    invited_by_pilot_id: &'a PilotId,
    created_at: &'a str,
}

impl PublicGroupInviteRecord {
    pub fn issue(
        inviter_secret_key: &iroh::SecretKey,
        group_id: GroupId,
        invited_pilot_id: PilotId,
    ) -> Result<Self, GroupRecordError> {
        let created_at = canonical_utc_now();
        let invited_by_pilot_id = PilotId::from_public_key(inviter_secret_key.public());

        let id_payload = PublicInvitePayload {
            schema: PUBLIC_GROUP_INVITE_SCHEMA,
            schema_version: PUBLIC_GROUP_INVITE_VERSION,
            group_id: &group_id,
            invited_pilot_id: &invited_pilot_id,
            invited_by_pilot_id: &invited_by_pilot_id,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;
        let sign_payload = PublicInviteSignPayload {
            schema: PUBLIC_GROUP_INVITE_SCHEMA,
            schema_version: PUBLIC_GROUP_INVITE_VERSION,
            record_id: &record_id,
            group_id: &group_id,
            invited_pilot_id: &invited_pilot_id,
            invited_by_pilot_id: &invited_by_pilot_id,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(inviter_secret_key, &sign_payload)?;

        let record = Self {
            schema: PUBLIC_GROUP_INVITE_SCHEMA.to_string(),
            schema_version: PUBLIC_GROUP_INVITE_VERSION,
            record_id,
            group_id,
            invited_pilot_id,
            invited_by_pilot_id,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), GroupRecordError> {
        check_schema(&self.schema, PUBLIC_GROUP_INVITE_SCHEMA)?;
        check_schema_version(self.schema_version, PUBLIC_GROUP_INVITE_VERSION)?;
        check_created_at(&self.created_at)?;

        let id_payload = PublicInvitePayload {
            schema: PUBLIC_GROUP_INVITE_SCHEMA,
            schema_version: PUBLIC_GROUP_INVITE_VERSION,
            group_id: &self.group_id,
            invited_pilot_id: &self.invited_pilot_id,
            invited_by_pilot_id: &self.invited_by_pilot_id,
            created_at: &self.created_at,
        };
        let expected = blake3_record_id(&id_payload)?;
        if self.record_id != expected {
            return Err(GroupRecordError::RecordIdMismatch { expected, found: self.record_id.clone() });
        }
        let sign_payload = PublicInviteSignPayload {
            schema: PUBLIC_GROUP_INVITE_SCHEMA,
            schema_version: PUBLIC_GROUP_INVITE_VERSION,
            record_id: &self.record_id,
            group_id: &self.group_id,
            invited_pilot_id: &self.invited_pilot_id,
            invited_by_pilot_id: &self.invited_by_pilot_id,
            created_at: &self.created_at,
        };
        verify_signature(&self.invited_by_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── PublicGroupAcceptRecord ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicGroupAcceptRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub group_id: GroupId,
    pub member_pilot_id: PilotId,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct PublicAcceptPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PublicAcceptSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    created_at: &'a str,
}

impl PublicGroupAcceptRecord {
    pub fn issue(
        member_secret_key: &iroh::SecretKey,
        group_id: GroupId,
    ) -> Result<Self, GroupRecordError> {
        let created_at = canonical_utc_now();
        let member_pilot_id = PilotId::from_public_key(member_secret_key.public());

        let id_payload = PublicAcceptPayload {
            schema: PUBLIC_GROUP_ACCEPT_SCHEMA,
            schema_version: PUBLIC_GROUP_ACCEPT_VERSION,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;
        let sign_payload = PublicAcceptSignPayload {
            schema: PUBLIC_GROUP_ACCEPT_SCHEMA,
            schema_version: PUBLIC_GROUP_ACCEPT_VERSION,
            record_id: &record_id,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(member_secret_key, &sign_payload)?;

        let record = Self {
            schema: PUBLIC_GROUP_ACCEPT_SCHEMA.to_string(),
            schema_version: PUBLIC_GROUP_ACCEPT_VERSION,
            record_id,
            group_id,
            member_pilot_id,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), GroupRecordError> {
        check_schema(&self.schema, PUBLIC_GROUP_ACCEPT_SCHEMA)?;
        check_schema_version(self.schema_version, PUBLIC_GROUP_ACCEPT_VERSION)?;
        check_created_at(&self.created_at)?;

        let id_payload = PublicAcceptPayload {
            schema: PUBLIC_GROUP_ACCEPT_SCHEMA,
            schema_version: PUBLIC_GROUP_ACCEPT_VERSION,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            created_at: &self.created_at,
        };
        let expected = blake3_record_id(&id_payload)?;
        if self.record_id != expected {
            return Err(GroupRecordError::RecordIdMismatch { expected, found: self.record_id.clone() });
        }
        let sign_payload = PublicAcceptSignPayload {
            schema: PUBLIC_GROUP_ACCEPT_SCHEMA,
            schema_version: PUBLIC_GROUP_ACCEPT_VERSION,
            record_id: &self.record_id,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            created_at: &self.created_at,
        };
        verify_signature(&self.member_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── PublicGroupLeaveRecord ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicGroupLeaveRecord {
    pub schema: String,
    pub schema_version: u8,
    pub record_id: Blake3Hex,
    pub group_id: GroupId,
    pub member_pilot_id: PilotId,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct PublicLeavePayload<'a> {
    schema: &'static str,
    schema_version: u8,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    created_at: &'a str,
}

#[derive(Serialize)]
struct PublicLeaveSignPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    group_id: &'a GroupId,
    member_pilot_id: &'a PilotId,
    created_at: &'a str,
}

impl PublicGroupLeaveRecord {
    pub fn issue(
        member_secret_key: &iroh::SecretKey,
        group_id: GroupId,
    ) -> Result<Self, GroupRecordError> {
        let created_at = canonical_utc_now();
        let member_pilot_id = PilotId::from_public_key(member_secret_key.public());

        let id_payload = PublicLeavePayload {
            schema: PUBLIC_GROUP_LEAVE_SCHEMA,
            schema_version: PUBLIC_GROUP_LEAVE_VERSION,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            created_at: &created_at,
        };
        let record_id = blake3_record_id(&id_payload)?;
        let sign_payload = PublicLeaveSignPayload {
            schema: PUBLIC_GROUP_LEAVE_SCHEMA,
            schema_version: PUBLIC_GROUP_LEAVE_VERSION,
            record_id: &record_id,
            group_id: &group_id,
            member_pilot_id: &member_pilot_id,
            created_at: &created_at,
        };
        let signature = sign_payload_hex(member_secret_key, &sign_payload)?;

        let record = Self {
            schema: PUBLIC_GROUP_LEAVE_SCHEMA.to_string(),
            schema_version: PUBLIC_GROUP_LEAVE_VERSION,
            record_id,
            group_id,
            member_pilot_id,
            created_at,
            signature,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), GroupRecordError> {
        check_schema(&self.schema, PUBLIC_GROUP_LEAVE_SCHEMA)?;
        check_schema_version(self.schema_version, PUBLIC_GROUP_LEAVE_VERSION)?;
        check_created_at(&self.created_at)?;

        let id_payload = PublicLeavePayload {
            schema: PUBLIC_GROUP_LEAVE_SCHEMA,
            schema_version: PUBLIC_GROUP_LEAVE_VERSION,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            created_at: &self.created_at,
        };
        let expected = blake3_record_id(&id_payload)?;
        if self.record_id != expected {
            return Err(GroupRecordError::RecordIdMismatch { expected, found: self.record_id.clone() });
        }
        let sign_payload = PublicLeaveSignPayload {
            schema: PUBLIC_GROUP_LEAVE_SCHEMA,
            schema_version: PUBLIC_GROUP_LEAVE_VERSION,
            record_id: &self.record_id,
            group_id: &self.group_id,
            member_pilot_id: &self.member_pilot_id,
            created_at: &self.created_at,
        };
        verify_signature(&self.member_pilot_id, &self.signature, &sign_payload)?;
        Ok(())
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn blake3_record_id<T: serde::Serialize>(payload: &T) -> Result<Blake3Hex, GroupRecordError> {
    let bytes = json_canon::to_vec(payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&bytes)))
}

fn sign_payload_hex<T: serde::Serialize>(
    secret_key: &iroh::SecretKey,
    payload: &T,
) -> Result<String, GroupRecordError> {
    let bytes = json_canon::to_vec(payload)?;
    Ok(hex::encode(secret_key.sign(&bytes).to_bytes()))
}

fn pilot_id_public_key(pilot_id: &PilotId) -> Result<iroh::PublicKey, GroupRecordError> {
    let bytes = hex::decode(pilot_id.public_key_hex())
        .ok()
        .and_then(|b| <[u8; 32]>::try_from(b).ok())
        .ok_or_else(|| GroupRecordError::PilotIdPublicKey(pilot_id.to_string()))?;
    iroh::PublicKey::from_bytes(&bytes)
        .map_err(|_| GroupRecordError::PilotIdPublicKey(pilot_id.to_string()))
}

fn decode_signature_hex(value: &str) -> Result<iroh::Signature, GroupRecordError> {
    if value.len() != 128 || !value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(GroupRecordError::SignatureEncoding);
    }
    let bytes = hex::decode(value).map_err(|_| GroupRecordError::SignatureEncoding)?;
    let sig_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| GroupRecordError::SignatureEncoding)?;
    Ok(iroh::Signature::from_bytes(&sig_bytes))
}

fn verify_signature<T: serde::Serialize>(
    signer: &PilotId,
    signature_hex: &str,
    payload: &T,
) -> Result<(), GroupRecordError> {
    let pubkey = pilot_id_public_key(signer)?;
    let signature = decode_signature_hex(signature_hex)?;
    let bytes = json_canon::to_vec(payload)?;
    pubkey
        .verify(&bytes, &signature)
        .map_err(|_| GroupRecordError::SignatureVerification)
}

fn check_schema(found: &str, expected: &'static str) -> Result<(), GroupRecordError> {
    if found != expected {
        return Err(GroupRecordError::Schema {
            expected,
            found: found.to_string(),
        });
    }
    Ok(())
}

fn check_schema_version(found: u8, expected: u8) -> Result<(), GroupRecordError> {
    if found != expected {
        return Err(GroupRecordError::SchemaVersion { expected, found });
    }
    Ok(())
}

fn check_created_at(value: &str) -> Result<(), GroupRecordError> {
    if !is_canonical_utc_timestamp(value) {
        return Err(GroupRecordError::CreatedAt(value.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    #[test]
    fn group_creation_record_roundtrip() {
        let key = secret_key(1);
        let record = GroupCreationRecord::issue(&key, GroupType::Private, None).unwrap();
        assert!(record.group_id.as_str().starts_with("igcnet:group:"));
        assert_eq!(record.group_id.id_hex().len(), 32);
        record.validate().unwrap();
    }

    #[test]
    fn group_creation_record_named_roundtrip() {
        let key = secret_key(2);
        let record =
            GroupCreationRecord::issue(&key, GroupType::Private, Some("Test Club".to_string()))
                .unwrap();
        record.validate().unwrap();
    }

    #[test]
    fn group_creation_record_validate_catches_group_id_mismatch() {
        let key = secret_key(3);
        let mut record = GroupCreationRecord::issue(&key, GroupType::Private, None).unwrap();
        let other_record =
            GroupCreationRecord::issue(&secret_key(4), GroupType::Private, None).unwrap();
        record.group_id = other_record.group_id;
        let err = record.validate().unwrap_err();
        assert!(matches!(err, GroupRecordError::GroupIdMismatch { .. }));
    }
}
