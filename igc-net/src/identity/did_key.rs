use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

const DID_KEY_PREFIX: &str = "did:key:z";
const ED25519_MULTICODEC_PREFIX: [u8; 2] = [0xed, 0x01];
const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum DidKeyError {
    #[error("invalid did:key: {0:?}")]
    InvalidFormat(String),
    #[error("did:key is not an Ed25519 multicodec")]
    UnsupportedCodec,
    #[error("did:key does not contain a valid Ed25519 public key")]
    InvalidPublicKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DidKey(String);

impl DidKey {
    pub fn parse(value: impl Into<String>) -> Result<Self, DidKeyError> {
        let value = value.into();
        parse_did_key_public_key(&value)?;
        Ok(Self(value))
    }

    pub fn from_public_key(public_key: iroh::PublicKey) -> Self {
        let mut multicodec_bytes = Vec::with_capacity(34);
        multicodec_bytes.extend_from_slice(&ED25519_MULTICODEC_PREFIX);
        multicodec_bytes.extend_from_slice(public_key.as_bytes());
        Self(format!(
            "{DID_KEY_PREFIX}{}",
            encode_base58btc(&multicodec_bytes)
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn public_key(&self) -> iroh::PublicKey {
        parse_did_key_public_key(self.as_str()).expect("validated did:key")
    }

    pub fn method_specific_id(&self) -> &str {
        self.0
            .strip_prefix("did:key:")
            .expect("validated did:key prefix")
    }

    pub fn key_id_fragment(&self) -> &str {
        self.method_specific_id()
    }

    pub fn key_id(&self) -> String {
        format!("{}#{}", self.as_str(), self.key_id_fragment())
    }
}

impl fmt::Display for DidKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for DidKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Borrow<str> for DidKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for DidKey {
    type Err = DidKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<String> for DidKey {
    type Error = DidKeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for DidKey {
    type Error = DidKeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<DidKey> for String {
    fn from(value: DidKey) -> Self {
        value.into_string()
    }
}

impl Serialize for DidKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DidKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

fn parse_did_key_public_key(value: &str) -> Result<iroh::PublicKey, DidKeyError> {
    let encoded = value
        .strip_prefix(DID_KEY_PREFIX)
        .ok_or_else(|| DidKeyError::InvalidFormat(value.to_string()))?;
    if encoded.is_empty() {
        return Err(DidKeyError::InvalidFormat(value.to_string()));
    }

    let decoded =
        decode_base58btc(encoded).map_err(|_| DidKeyError::InvalidFormat(value.to_string()))?;
    if decoded.len() != 34 || decoded[..2] != ED25519_MULTICODEC_PREFIX {
        return Err(DidKeyError::UnsupportedCodec);
    }

    let public_key_bytes: [u8; 32] = decoded[2..]
        .try_into()
        .map_err(|_| DidKeyError::InvalidPublicKey)?;
    iroh::PublicKey::from_bytes(&public_key_bytes).map_err(|_| DidKeyError::InvalidPublicKey)
}

fn decode_base58btc(input: &str) -> Result<Vec<u8>, ()> {
    let mut bytes = Vec::<u8>::new();
    for ch in input.bytes() {
        let value = base58_value(ch).ok_or(())? as u32;
        let mut carry = value;
        for byte in bytes.iter_mut().rev() {
            let accum = (*byte as u32) * 58 + carry;
            *byte = (accum & 0xff) as u8;
            carry = accum >> 8;
        }
        while carry > 0 {
            bytes.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }

    let leading_zero_count = input.bytes().take_while(|byte| *byte == b'1').count();
    let mut decoded = vec![0u8; leading_zero_count];
    decoded.extend(bytes);
    Ok(decoded)
}

fn encode_base58btc(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    let mut digits = Vec::<u8>::new();
    for byte in bytes {
        let mut carry = *byte as u32;
        for digit in digits.iter_mut().rev() {
            let accum = (*digit as u32) * 256 + carry;
            *digit = (accum % 58) as u8;
            carry = accum / 58;
        }
        while carry > 0 {
            digits.insert(0, (carry % 58) as u8);
            carry /= 58;
        }
    }

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for _ in bytes.iter().take_while(|byte| **byte == 0) {
        encoded.push('1');
    }
    for digit in digits {
        encoded.push(BASE58_ALPHABET[digit as usize] as char);
    }
    encoded
}

fn base58_value(ch: u8) -> Option<u8> {
    BASE58_ALPHABET
        .iter()
        .position(|candidate| *candidate == ch)
        .map(|value| value as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_ed25519_did_key() {
        let secret_key = iroh::SecretKey::from_bytes(&[7u8; 32]);
        let did_key = DidKey::from_public_key(secret_key.public());

        let parsed = DidKey::parse(did_key.as_str()).unwrap();
        assert_eq!(parsed, did_key);
        assert_eq!(parsed.public_key(), secret_key.public());
    }

    #[test]
    fn rejects_non_base58_input() {
        let err = DidKey::parse("did:key:z0OIl").unwrap_err();
        assert!(matches!(err, DidKeyError::InvalidFormat(_)));
    }

    #[test]
    fn rejects_non_ed25519_multicodec() {
        let bad = format!("did:key:z{}", encode_base58btc(&[0x12, 0x20, 0u8]));
        let err = DidKey::parse(bad).unwrap_err();
        assert!(matches!(err, DidKeyError::UnsupportedCodec));
    }

    #[test]
    fn key_id_uses_did_url_fragment_form() {
        let did_key = DidKey::from_public_key(iroh::SecretKey::from_bytes(&[8u8; 32]).public());
        assert_eq!(
            did_key.key_id(),
            format!("{}#{}", did_key.as_str(), did_key.key_id_fragment())
        );
        assert_eq!(did_key.key_id_fragment(), did_key.method_specific_id());
    }
}
