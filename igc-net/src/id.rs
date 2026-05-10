use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::util::is_lower_hex_64;

macro_rules! declare_identifier {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
        parse($value:ident) $parse:block
        $($body:item)*
    ) => {
        $(#[$meta])*
        $vis struct $name(String);

        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let $value = value.into();
                $parse
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }

            $($body)*
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdentifierError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.into_string()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum IdentifierError {
    #[error("invalid blake3 hex (expected 64 lowercase hex chars): {0:?}")]
    Blake3Hex(String),
    #[error("invalid node ID hex (expected 64 lowercase hex chars): {0:?}")]
    NodeIdHex(String),
    #[error("invalid pilot ID (expected igcnet:id:<64 lowercase hex chars>): {0:?}")]
    PilotId(String),
    #[error("invalid group ID (expected igcnet:group:<32 lowercase hex chars>): {0:?}")]
    GroupId(String),
}

declare_identifier! {
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub struct Blake3Hex;
    parse(value) {
        if is_lower_hex_64(&value) {
            Ok(Self(value))
        } else {
            Err(IdentifierError::Blake3Hex(value))
        }
    }

    pub fn from_hash(hash: blake3::Hash) -> Self {
        Self(hex::encode(hash.as_bytes()))
    }

    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self(hex::encode(bytes))
    }
}

declare_identifier! {
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub struct NodeIdHex;
    parse(value) {
        if is_lower_hex_64(&value) {
            Ok(Self(value))
        } else {
            Err(IdentifierError::NodeIdHex(value))
        }
    }

    pub fn from_public_key(key: iroh::PublicKey) -> Self {
        Self(key.to_string())
    }
}

declare_identifier! {
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub struct GroupId;
    parse(value) {
        if let Some(id_hex) = value.strip_prefix(Self::PREFIX)
            && id_hex.len() == 32
            && id_hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            Ok(Self(value))
        } else {
            Err(IdentifierError::GroupId(value))
        }
    }

    pub const PREFIX: &str = "igcnet:group:";

    pub fn id_hex(&self) -> &str {
        self.0.strip_prefix(Self::PREFIX).expect("validated group_id prefix")
    }

    pub fn derive(
        group_type: &crate::group::GroupType,
        creator_pilot_id: &PilotId,
        name: &Option<String>,
        created_at: &str,
    ) -> Result<Self, serde_json::Error> {
        #[derive(serde::Serialize)]
        struct DerivePayload<'a> {
            schema: &'static str,
            schema_version: u8,
            group_type: &'a crate::group::GroupType,
            creator_pilot_id: &'a PilotId,
            name: &'a Option<String>,
            created_at: &'a str,
        }
        let payload = DerivePayload {
            schema: "igc-net/group-creation",
            schema_version: 1,
            group_type,
            creator_pilot_id,
            name,
            created_at,
        };
        let canonical = json_canon::to_vec(&payload)?;
        let hash = blake3::hash(&canonical);
        let hex = hex::encode(&hash.as_bytes()[..16]);
        Ok(Self(format!("{}{}", Self::PREFIX, hex)))
    }
}

declare_identifier! {
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub struct PilotId;
    parse(value) {
        if let Some(key_hex) = value.strip_prefix(Self::PREFIX)
            && is_lower_hex_64(key_hex)
        {
            Ok(Self(value))
        } else {
            Err(IdentifierError::PilotId(value))
        }
    }

    pub const PREFIX: &str = "igcnet:id:";

    pub fn public_key_hex(&self) -> &str {
        self.0
            .strip_prefix(Self::PREFIX)
            .expect("validated pilot_id prefix")
    }

    pub fn from_public_key(key: iroh::PublicKey) -> Self {
        Self(format!("{}{}", Self::PREFIX, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::GroupType;

    fn pilot(byte: u8) -> PilotId {
        PilotId::from_public_key(iroh::SecretKey::from_bytes(&[byte; 32]).public())
    }

    #[test]
    fn group_id_derive_has_correct_prefix_and_hex_length() {
        let id = GroupId::derive(&GroupType::Private, &pilot(1), &None, "2026-01-01T12:00:00Z").unwrap();
        assert!(id.as_str().starts_with(GroupId::PREFIX));
        assert_eq!(id.id_hex().len(), 32);
        assert!(id.id_hex().bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')));
    }

    #[test]
    fn group_id_derive_is_deterministic() {
        let creator = pilot(1);
        let name = Some("My Club".to_string());
        let ts = "2026-01-01T12:00:00Z";
        let id1 = GroupId::derive(&GroupType::Private, &creator, &name, ts).unwrap();
        let id2 = GroupId::derive(&GroupType::Private, &creator, &name, ts).unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn group_id_derive_differs_by_created_at() {
        let creator = pilot(1);
        let id1 = GroupId::derive(&GroupType::Private, &creator, &None, "2026-01-01T12:00:00Z").unwrap();
        let id2 = GroupId::derive(&GroupType::Private, &creator, &None, "2026-01-01T12:00:01Z").unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn group_id_derive_differs_by_group_type() {
        let creator = pilot(1);
        let ts = "2026-01-01T12:00:00Z";
        let id_private = GroupId::derive(&GroupType::Private, &creator, &None, ts).unwrap();
        let id_public = GroupId::derive(&GroupType::Public, &creator, &None, ts).unwrap();
        assert_ne!(id_private, id_public);
    }

    #[test]
    fn group_id_derive_differs_by_creator() {
        let ts = "2026-01-01T12:00:00Z";
        let id1 = GroupId::derive(&GroupType::Private, &pilot(1), &None, ts).unwrap();
        let id2 = GroupId::derive(&GroupType::Private, &pilot(2), &None, ts).unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn group_id_derive_differs_by_name() {
        let creator = pilot(1);
        let ts = "2026-01-01T12:00:00Z";
        let id_unnamed = GroupId::derive(&GroupType::Private, &creator, &None, ts).unwrap();
        let id_named = GroupId::derive(&GroupType::Private, &creator, &Some("Club".to_string()), ts).unwrap();
        assert_ne!(id_unnamed, id_named);
    }
}
