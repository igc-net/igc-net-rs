use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::DidKey;

const DID_WEB_PREFIX: &str = "did:web:";

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum DidWebError {
    #[error("invalid did:web: {0}")]
    InvalidFormat(String),
    #[error("did:web contains invalid percent-encoding: {0}")]
    InvalidPercentEncoding(String),
    #[error("did:web host is invalid: {0}")]
    InvalidHost(String),
    #[error("did:web path segment is invalid: {0}")]
    InvalidPathSegment(String),
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum DidWebResolutionError {
    #[error("no did:web resolver was configured")]
    ResolverUnavailable,
    #[error("did:web fetch failed: {0}")]
    Fetch(String),
    #[error("did:web document was invalid: {0}")]
    InvalidDocument(String),
    #[error("did:web verification method not found: {0}")]
    VerificationMethodNotFound(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDidWebVerificationMethod {
    pub did: String,
    pub kid: Option<String>,
    pub public_key: iroh::PublicKey,
}

pub trait DidWebResolver {
    fn resolve_verification_method(
        &self,
        did: &str,
        kid: Option<&str>,
    ) -> Result<ResolvedDidWebVerificationMethod, DidWebResolutionError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoDidWebResolver;

impl DidWebResolver for NoDidWebResolver {
    fn resolve_verification_method(
        &self,
        _did: &str,
        _kid: Option<&str>,
    ) -> Result<ResolvedDidWebVerificationMethod, DidWebResolutionError> {
        Err(DidWebResolutionError::ResolverUnavailable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DidWeb(String);

impl DidWeb {
    pub fn parse(value: impl Into<String>) -> Result<Self, DidWebError> {
        let value = value.into();
        parse_did_web_components(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn document_url(&self) -> String {
        let components = parse_did_web_components(self.as_str()).expect("validated did:web");
        let mut url = format!("https://{}", components.host);
        if components.path.is_empty() {
            url.push_str("/.well-known/did.json");
        } else {
            for segment in components.path {
                url.push('/');
                url.push_str(&segment);
            }
            url.push_str("/did.json");
        }
        url
    }
}

impl fmt::Display for DidWeb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for DidWeb {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Borrow<str> for DidWeb {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for DidWeb {
    type Err = DidWebError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<String> for DidWeb {
    type Error = DidWebError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for DidWeb {
    type Error = DidWebError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<DidWeb> for String {
    fn from(value: DidWeb) -> Self {
        value.into_string()
    }
}

impl Serialize for DidWeb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DidWeb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DidWebDocument {
    pub id: DidWeb,
    #[serde(default, rename = "verificationMethod")]
    verification_methods: Vec<DidWebVerificationMethodEntry>,
    #[serde(default, rename = "assertionMethod")]
    assertion_methods: Vec<DidWebVerificationMethodReference>,
}

impl DidWebDocument {
    pub fn from_slice(bytes: &[u8]) -> Result<Self, DidWebResolutionError> {
        serde_json::from_slice(bytes)
            .map_err(|error| DidWebResolutionError::InvalidDocument(error.to_string()))
    }

    pub fn resolve_verification_method(
        &self,
        did: &DidWeb,
        kid: Option<&str>,
    ) -> Result<ResolvedDidWebVerificationMethod, DidWebResolutionError> {
        if self.id != *did {
            return Err(DidWebResolutionError::InvalidDocument(format!(
                "document id {found} does not match requested DID {expected}",
                found = self.id,
                expected = did,
            )));
        }

        let mut methods = BTreeMap::<String, DidWebVerificationMethodEntry>::new();
        for method in &self.verification_methods {
            let method_id = normalize_method_id(did, &method.id)?;
            if methods.insert(method_id, method.clone()).is_some() {
                return Err(DidWebResolutionError::InvalidDocument(
                    "duplicate verificationMethod id".to_string(),
                ));
            }
        }

        let mut assertion_method_ids = BTreeSet::<String>::new();
        for reference in &self.assertion_methods {
            match reference {
                DidWebVerificationMethodReference::Reference(id) => {
                    assertion_method_ids.insert(normalize_method_id(did, id)?);
                }
                DidWebVerificationMethodReference::Embedded(method) => {
                    let method_id = normalize_method_id(did, &method.id)?;
                    if methods.insert(method_id.clone(), method.clone()).is_some() {
                        return Err(DidWebResolutionError::InvalidDocument(
                            "duplicate assertionMethod id".to_string(),
                        ));
                    }
                    assertion_method_ids.insert(method_id);
                }
            }
        }

        if assertion_method_ids.is_empty() {
            return Err(DidWebResolutionError::InvalidDocument(
                "did:web document does not declare assertionMethod".to_string(),
            ));
        }

        let selected_id = match kid {
            Some(kid) => {
                let normalized = normalize_method_id(did, kid)?;
                if !assertion_method_ids.contains(&normalized) {
                    return Err(DidWebResolutionError::VerificationMethodNotFound(
                        normalized,
                    ));
                }
                normalized
            }
            None if assertion_method_ids.len() == 1 => assertion_method_ids
                .iter()
                .next()
                .expect("non-empty assertionMethod set")
                .clone(),
            None => {
                return Err(DidWebResolutionError::VerificationMethodNotFound(
                    "no kid provided and did:web document has multiple assertion methods"
                        .to_string(),
                ));
            }
        };

        let method = methods.get(&selected_id).ok_or_else(|| {
            DidWebResolutionError::VerificationMethodNotFound(selected_id.clone())
        })?;
        let public_key = method.public_key(did)?;

        Ok(ResolvedDidWebVerificationMethod {
            did: did.to_string(),
            kid: Some(selected_id),
            public_key,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum DidWebVerificationMethodReference {
    Reference(String),
    Embedded(DidWebVerificationMethodEntry),
}

#[derive(Debug, Clone, Deserialize)]
struct DidWebVerificationMethodEntry {
    id: String,
    #[serde(default, rename = "publicKeyJwk")]
    public_key_jwk: Option<DidWebPublicKeyJwk>,
    #[serde(default, rename = "publicKeyMultibase")]
    public_key_multibase: Option<String>,
}

impl DidWebVerificationMethodEntry {
    fn public_key(&self, did: &DidWeb) -> Result<iroh::PublicKey, DidWebResolutionError> {
        match (&self.public_key_jwk, &self.public_key_multibase) {
            (Some(_), Some(_)) => Err(DidWebResolutionError::InvalidDocument(
                "verification method must contain exactly one public key encoding".to_string(),
            )),
            (Some(jwk), None) => jwk.public_key(),
            (None, Some(multibase)) => public_key_from_multibase(multibase, did),
            (None, None) => Err(DidWebResolutionError::InvalidDocument(
                "verification method does not contain supported public key material".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct DidWebPublicKeyJwk {
    kty: String,
    crv: String,
    x: String,
    #[serde(default)]
    d: Option<String>,
}

impl DidWebPublicKeyJwk {
    fn public_key(&self) -> Result<iroh::PublicKey, DidWebResolutionError> {
        if self.kty != "OKP" || self.crv != "Ed25519" {
            return Err(DidWebResolutionError::InvalidDocument(
                "publicKeyJwk must be an OKP Ed25519 key".to_string(),
            ));
        }
        if self.d.is_some() {
            return Err(DidWebResolutionError::InvalidDocument(
                "publicKeyJwk must not include private key material".to_string(),
            ));
        }

        let decoded = URL_SAFE_NO_PAD
            .decode(self.x.as_bytes())
            .map_err(|error| DidWebResolutionError::InvalidDocument(error.to_string()))?;
        let public_key_bytes: [u8; 32] = decoded.try_into().map_err(|_| {
            DidWebResolutionError::InvalidDocument(
                "publicKeyJwk.x must decode to 32 bytes".to_string(),
            )
        })?;
        iroh::PublicKey::from_bytes(&public_key_bytes).map_err(|_| {
            DidWebResolutionError::InvalidDocument(
                "publicKeyJwk.x does not contain a valid Ed25519 key".to_string(),
            )
        })
    }
}

#[cfg(feature = "did-web")]
#[derive(Debug, Clone)]
pub struct ReqwestDidWebResolver {
    client: reqwest::blocking::Client,
}

#[cfg(feature = "did-web")]
impl Default for ReqwestDidWebResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "did-web")]
impl ReqwestDidWebResolver {
    pub fn new() -> Self {
        let client = reqwest::blocking::Client::builder()
            .https_only(true)
            .build()
            .expect("valid reqwest did:web client");
        Self { client }
    }
}

#[cfg(feature = "did-web")]
impl DidWebResolver for ReqwestDidWebResolver {
    fn resolve_verification_method(
        &self,
        did: &str,
        kid: Option<&str>,
    ) -> Result<ResolvedDidWebVerificationMethod, DidWebResolutionError> {
        resolve_verification_method_with_fetch(did, kid, |document_url| {
            let response = self
                .client
                .get(document_url)
                .send()
                .map_err(|error| DidWebResolutionError::Fetch(error.to_string()))?;
            let response = response
                .error_for_status()
                .map_err(|error| DidWebResolutionError::Fetch(error.to_string()))?;
            let bytes = response
                .bytes()
                .map_err(|error| DidWebResolutionError::Fetch(error.to_string()))?;
            Ok(bytes.to_vec())
        })
    }
}

fn resolve_verification_method_with_fetch<F>(
    did: &str,
    kid: Option<&str>,
    fetch: F,
) -> Result<ResolvedDidWebVerificationMethod, DidWebResolutionError>
where
    F: FnOnce(&str) -> Result<Vec<u8>, DidWebResolutionError>,
{
    let did = DidWeb::parse(did)
        .map_err(|error| DidWebResolutionError::InvalidDocument(error.to_string()))?;
    let document_bytes = fetch(&did.document_url())?;
    let document = DidWebDocument::from_slice(&document_bytes)?;
    document.resolve_verification_method(&did, kid)
}

fn public_key_from_multibase(
    multibase: &str,
    did: &DidWeb,
) -> Result<iroh::PublicKey, DidWebResolutionError> {
    let did_key = DidKey::parse(format!("did:key:{multibase}")).map_err(|error| {
        DidWebResolutionError::InvalidDocument(format!(
            "did:web verification method for {did} contains invalid publicKeyMultibase: {error}",
        ))
    })?;
    Ok(did_key.public_key())
}

fn normalize_method_id(did: &DidWeb, value: &str) -> Result<String, DidWebResolutionError> {
    if value.starts_with('#') {
        return Ok(format!("{did}{value}"));
    }
    if value.starts_with("did:") {
        return Ok(value.to_string());
    }
    Err(DidWebResolutionError::InvalidDocument(format!(
        "verification method id {value:?} is not a DID URL or fragment"
    )))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DidWebComponents {
    host: String,
    path: Vec<String>,
}

fn parse_did_web_components(value: &str) -> Result<DidWebComponents, DidWebError> {
    let encoded = value
        .strip_prefix(DID_WEB_PREFIX)
        .ok_or_else(|| DidWebError::InvalidFormat(value.to_string()))?;
    if encoded.is_empty() {
        return Err(DidWebError::InvalidFormat(value.to_string()));
    }

    let mut segments = encoded.split(':');
    let host = decode_component(
        segments
            .next()
            .ok_or_else(|| DidWebError::InvalidFormat(value.to_string()))?,
    )?;
    validate_host(&host)?;

    let mut path = Vec::new();
    for segment in segments {
        let decoded = decode_component(segment)?;
        validate_path_segment(&decoded)?;
        path.push(decoded);
    }

    Ok(DidWebComponents { host, path })
}

fn decode_component(value: &str) -> Result<String, DidWebError> {
    let mut decoded = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(DidWebError::InvalidPercentEncoding(value.to_string()));
            }
            let hi = decode_hex_digit(bytes[index + 1])
                .ok_or_else(|| DidWebError::InvalidPercentEncoding(value.to_string()))?;
            let lo = decode_hex_digit(bytes[index + 2])
                .ok_or_else(|| DidWebError::InvalidPercentEncoding(value.to_string()))?;
            decoded.push(((hi << 4) | lo) as char);
            index += 3;
        } else {
            decoded.push(bytes[index] as char);
            index += 1;
        }
    }
    Ok(decoded)
}

fn decode_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn validate_host(host: &str) -> Result<(), DidWebError> {
    if host.is_empty()
        || host.starts_with('.')
        || host.ends_with('.')
        || host.contains('/')
        || host.contains('?')
        || host.contains('#')
        || host.contains(' ')
    {
        return Err(DidWebError::InvalidHost(host.to_string()));
    }
    Ok(())
}

fn validate_path_segment(segment: &str) -> Result<(), DidWebError> {
    if segment.is_empty()
        || segment == "."
        || segment == ".."
        || segment.contains('/')
        || segment.contains('?')
        || segment.contains('#')
        || segment.contains('\\')
    {
        return Err(DidWebError::InvalidPathSegment(segment.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_did_document(id: &str, methods: &[(&str, iroh::PublicKey)]) -> serde_json::Value {
        let verification_methods = methods
            .iter()
            .map(|(method_id, public_key)| {
                serde_json::json!({
                    "id": method_id,
                    "type": "JsonWebKey2020",
                    "controller": id,
                    "publicKeyJwk": {
                        "kty": "OKP",
                        "crv": "Ed25519",
                        "x": URL_SAFE_NO_PAD.encode(public_key.as_bytes()),
                    }
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "id": id,
            "verificationMethod": verification_methods,
            "assertionMethod": methods.iter().map(|(method_id, _)| serde_json::Value::String((*method_id).to_string())).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn root_did_maps_to_well_known_url() {
        let did = DidWeb::parse("did:web:portal.example.org").unwrap();
        assert_eq!(
            did.document_url(),
            "https://portal.example.org/.well-known/did.json"
        );
    }

    #[test]
    fn path_did_maps_to_path_scoped_url() {
        let did = DidWeb::parse("did:web:portal.example.org:pilots:alice").unwrap();
        assert_eq!(
            did.document_url(),
            "https://portal.example.org/pilots/alice/did.json"
        );
    }

    #[test]
    fn percent_encoded_host_port_is_decoded_in_document_url() {
        let did = DidWeb::parse("did:web:localhost%3A8443:issuer").unwrap();
        assert_eq!(did.document_url(), "https://localhost:8443/issuer/did.json");
    }

    #[test]
    fn invalid_path_traversal_segment_is_rejected() {
        let err = DidWeb::parse("did:web:portal.example.org:%2E%2E").unwrap_err();
        assert!(matches!(err, DidWebError::InvalidPathSegment(segment) if segment == ".."));
    }

    #[test]
    fn resolves_assertion_method_from_public_key_jwk() {
        let secret_key = iroh::SecretKey::from_bytes(&[21u8; 32]);
        let did = DidWeb::parse("did:web:portal.example.org").unwrap();
        let document = DidWebDocument::from_slice(
            serde_json::to_vec(&sample_did_document(
                did.as_str(),
                &[("did:web:portal.example.org#issuer-1", secret_key.public())],
            ))
            .unwrap()
            .as_slice(),
        )
        .unwrap();

        let resolved = document
            .resolve_verification_method(&did, Some("did:web:portal.example.org#issuer-1"))
            .unwrap();
        assert_eq!(resolved.did, did.as_str());
        assert_eq!(
            resolved.kid.as_deref(),
            Some("did:web:portal.example.org#issuer-1")
        );
        assert_eq!(resolved.public_key, secret_key.public());
    }

    #[test]
    fn resolves_assertion_method_from_public_key_multibase() {
        let secret_key = iroh::SecretKey::from_bytes(&[22u8; 32]);
        let did = DidWeb::parse("did:web:portal.example.org").unwrap();
        let public_key_multibase = DidKey::from_public_key(secret_key.public())
            .method_specific_id()
            .to_string();
        let document = serde_json::json!({
            "id": did,
            "verificationMethod": [{
                "id": "did:web:portal.example.org#issuer-1",
                "type": "Ed25519VerificationKey2020",
                "controller": "did:web:portal.example.org",
                "publicKeyMultibase": public_key_multibase,
            }],
            "assertionMethod": ["#issuer-1"],
        });
        let document = DidWebDocument::from_slice(&serde_json::to_vec(&document).unwrap()).unwrap();

        let resolved = document
            .resolve_verification_method(&did, Some("#issuer-1"))
            .unwrap();
        assert_eq!(resolved.public_key, secret_key.public());
        assert_eq!(
            resolved.kid.as_deref(),
            Some("did:web:portal.example.org#issuer-1")
        );
    }

    #[test]
    fn multiple_assertion_methods_require_kid() {
        let first = iroh::SecretKey::from_bytes(&[31u8; 32]).public();
        let second = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
        let did = DidWeb::parse("did:web:portal.example.org").unwrap();
        let document = DidWebDocument::from_slice(
            &serde_json::to_vec(&sample_did_document(
                did.as_str(),
                &[
                    ("did:web:portal.example.org#old", first),
                    ("did:web:portal.example.org#new", second),
                ],
            ))
            .unwrap(),
        )
        .unwrap();

        let err = document
            .resolve_verification_method(&did, None)
            .unwrap_err();
        assert!(matches!(
            err,
            DidWebResolutionError::VerificationMethodNotFound(reason)
                if reason.contains("no kid provided")
        ));
    }

    #[test]
    fn removed_rotation_key_is_no_longer_resolvable() {
        let retained = iroh::SecretKey::from_bytes(&[41u8; 32]).public();
        let removed = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        let did = DidWeb::parse("did:web:portal.example.org").unwrap();

        let initial = DidWebDocument::from_slice(
            &serde_json::to_vec(&sample_did_document(
                did.as_str(),
                &[
                    ("did:web:portal.example.org#old", removed),
                    ("did:web:portal.example.org#new", retained),
                ],
            ))
            .unwrap(),
        )
        .unwrap();
        let rotated = DidWebDocument::from_slice(
            &serde_json::to_vec(&sample_did_document(
                did.as_str(),
                &[("did:web:portal.example.org#new", retained)],
            ))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            initial
                .resolve_verification_method(&did, Some("did:web:portal.example.org#old"))
                .unwrap()
                .public_key,
            removed
        );
        let err = rotated
            .resolve_verification_method(&did, Some("did:web:portal.example.org#old"))
            .unwrap_err();
        assert!(matches!(
            err,
            DidWebResolutionError::VerificationMethodNotFound(kid)
                if kid == "did:web:portal.example.org#old"
        ));
    }

    #[test]
    fn fetch_failure_is_reported() {
        let err = resolve_verification_method_with_fetch(
            "did:web:portal.example.org",
            Some("#issuer-1"),
            |_document_url| Err(DidWebResolutionError::Fetch("network down".to_string())),
        )
        .unwrap_err();
        assert!(matches!(err, DidWebResolutionError::Fetch(reason) if reason == "network down"));
    }

    #[test]
    fn malformed_document_is_rejected_during_fetch_resolution() {
        let err = resolve_verification_method_with_fetch(
            "did:web:portal.example.org",
            Some("#issuer-1"),
            |_document_url| {
                Ok(br#"{"id":"did:web:portal.example.org","assertionMethod":42}"#.to_vec())
            },
        )
        .unwrap_err();
        assert!(matches!(err, DidWebResolutionError::InvalidDocument(_)));
    }

    #[test]
    fn did_web_alias_is_non_authoritative_relative_to_governance() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../specs/fixtures/v0.3/pilot_did_mirror_precedence.json"
        ))
        .unwrap();
        let case = fixture.get("case").unwrap();
        let authoritative = case
            .get("authoritative_pilot_auth_did")
            .unwrap()
            .as_str()
            .unwrap();
        let alias = case.get("public_alias_did_web").unwrap().as_str().unwrap();

        assert!(DidKey::parse(authoritative).is_ok());
        assert!(DidWeb::parse(alias).is_ok());
        assert_ne!(authoritative, alias);
    }

    #[test]
    fn did_key_multibase_must_be_ed25519() {
        let did = DidWeb::parse("did:web:portal.example.org").unwrap();
        let document = serde_json::json!({
            "id": did,
            "verificationMethod": [{
                "id": "did:web:portal.example.org#issuer-1",
                "type": "UnknownKey",
                "controller": "did:web:portal.example.org",
                "publicKeyMultibase": "z3weFAN",
            }],
            "assertionMethod": ["#issuer-1"],
        });
        let document = DidWebDocument::from_slice(&serde_json::to_vec(&document).unwrap()).unwrap();
        let err = document
            .resolve_verification_method(&did, Some("#issuer-1"))
            .unwrap_err();
        assert!(
            matches!(err, DidWebResolutionError::InvalidDocument(message) if message.contains("invalid publicKeyMultibase"))
        );
    }
}
