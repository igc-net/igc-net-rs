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
