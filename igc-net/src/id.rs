use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::util::is_lower_hex_64;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum IdentifierError {
    #[error("invalid blake3 hex (expected 64 lowercase hex chars): {0:?}")]
    Blake3Hex(String),
    #[error("invalid node ID hex (expected 64 lowercase hex chars): {0:?}")]
    NodeIdHex(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Blake3Hex(String);

impl Blake3Hex {
    pub fn parse(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        if is_lower_hex_64(&value) {
            Ok(Self(value))
        } else {
            Err(IdentifierError::Blake3Hex(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn from_hash(hash: blake3::Hash) -> Self {
        Self(hex::encode(hash.as_bytes()))
    }

    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self(hex::encode(bytes))
    }
}

impl fmt::Display for Blake3Hex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for Blake3Hex {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Borrow<str> for Blake3Hex {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for Blake3Hex {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<String> for Blake3Hex {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for Blake3Hex {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<Blake3Hex> for String {
    fn from(value: Blake3Hex) -> Self {
        value.into_string()
    }
}

impl Serialize for Blake3Hex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Blake3Hex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeIdHex(String);

impl NodeIdHex {
    pub fn parse(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        if is_lower_hex_64(&value) {
            Ok(Self(value))
        } else {
            Err(IdentifierError::NodeIdHex(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn from_public_key(key: iroh::PublicKey) -> Self {
        Self(key.to_string())
    }
}

impl fmt::Display for NodeIdHex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for NodeIdHex {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Borrow<str> for NodeIdHex {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for NodeIdHex {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<String> for NodeIdHex {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for NodeIdHex {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<NodeIdHex> for String {
    fn from(value: NodeIdHex) -> Self {
        value.into_string()
    }
}

impl Serialize for NodeIdHex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NodeIdHex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}
