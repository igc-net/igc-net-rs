use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::{DidKey, DidKeyError};
use crate::governance::GovernanceLookup;
use crate::id::{IdentifierError, PilotId};
use crate::keys::PilotIdentity;
use crate::util::is_iso_3166_alpha2_country_code;

const VC_JWT_TYP: &str = "vc+jwt";
const VC_JWT_ALG: &str = "EdDSA";
const VC_CONTEXT: &str = "https://www.w3.org/2018/credentials/v1";
const VC_TYPE: &str = "VerifiableCredential";
const PILOT_PROFILE_CREDENTIAL_TYPE: &str = "PilotProfileCredential";

pub trait Clock {
    fn now_unix_seconds(&self) -> i64;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_seconds(&self) -> i64 {
        Utc::now().timestamp()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedClock {
    now_unix_seconds: i64,
}

impl FixedClock {
    pub const fn new(now_unix_seconds: i64) -> Self {
        Self { now_unix_seconds }
    }
}

impl Clock for FixedClock {
    fn now_unix_seconds(&self) -> i64 {
        self.now_unix_seconds
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PilotProfileCredentialError {
    #[error("base64url: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("identifier: {0}")]
    Identifier(#[from] IdentifierError),
    #[error("did:key: {0}")]
    DidKey(#[from] DidKeyError),
    #[error("governance: {0}")]
    Governance(#[from] crate::governance::GovernanceStoreError),
    #[error("compact JWT must contain exactly 3 dot-separated segments")]
    MalformedCompactJwt,
    #[error("JOSE header alg must be {VC_JWT_ALG:?}, got {0:?}")]
    UnsupportedAlgorithm(String),
    #[error("JOSE header typ must be {VC_JWT_TYP:?}, got {0:?}")]
    UnsupportedType(String),
    #[error("JOSE header kid must equal issuer verification method {expected}, got {found}")]
    KidMismatch { expected: String, found: String },
    #[error("JWT signature must decode to 64 bytes")]
    SignatureEncoding,
    #[error("JWT signature verification failed")]
    SignatureVerification,
    #[error("credential jti must not be empty")]
    EmptyJti,
    #[error("credential audience must not be empty")]
    EmptyAudience,
    #[error("vc @context must include {VC_CONTEXT:?}")]
    MissingContext,
    #[error("vc type must include {VC_TYPE:?}")]
    MissingVerifiableCredentialType,
    #[error("vc type must include {PILOT_PROFILE_CREDENTIAL_TYPE:?}")]
    MissingPilotProfileCredentialType,
    #[error("credentialSubject.id must equal sub")]
    SubjectIdMismatch,
    #[error("credentialSubject.pilot_auth_did must equal iss")]
    SubjectDidMismatch,
    #[error("credentialSubject.country must use ISO 3166-1 alpha-2 uppercase syntax, got {0:?}")]
    InvalidCountry(String),
    #[error("exp must be greater than nbf when supplied")]
    InvalidExpiryWindow,
    #[error("credential is not yet valid until unix second {not_before}")]
    NotYetValid { not_before: i64 },
    #[error("credential expired at unix second {expires_at}")]
    Expired { expires_at: i64 },
    #[error("expected audience {expected:?} not present in aud claim")]
    AudienceMismatch { expected: String },
    #[error("no authoritative pilot_auth_did is available for pilot {0}")]
    GovernanceUnavailable(PilotId),
    #[error("pilot-auth-did governance state for {0} is incomplete and not high-trust")]
    GovernanceIncomplete(PilotId),
    #[error(
        "local active pilot_auth_did {local} does not match authoritative governance DID {authoritative}"
    )]
    IssuanceDidMismatch {
        local: DidKey,
        authoritative: DidKey,
    },
    #[error(
        "credential issuer DID {presented} does not match authoritative active DID {authoritative}"
    )]
    StaleCredential {
        presented: DidKey,
        authoritative: DidKey,
    },
    #[error(
        "credential issuer DID {presented} is a retired historical pilot_auth_did; current authoritative DID is {authoritative}"
    )]
    HistoricalCredential {
        presented: DidKey,
        authoritative: DidKey,
    },
    #[error("credential expiration overflowed supported unix time")]
    ExpiryOverflow,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotProfileCredentialJoseHeader {
    pub alg: String,
    pub typ: String,
    pub kid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotProfileCredentialClaims {
    pub iss: DidKey,
    pub sub: PilotId,
    pub nbf: i64,
    pub iat: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<JwtAudience>,
    pub jti: String,
    pub vc: PilotProfileCredentialVc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotProfileCredentialVc {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    #[serde(rename = "type")]
    pub credential_type: Vec<String>,
    #[serde(rename = "credentialSubject")]
    pub credential_subject: PilotProfileCredentialSubject,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotProfileCredentialSubject {
    pub id: PilotId,
    pub pilot_auth_did: DidKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(default, flatten)]
    pub additional_fields: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum JwtAudience {
    One(String),
    Many(Vec<String>),
}

impl JwtAudience {
    fn validate(&self) -> Result<(), PilotProfileCredentialError> {
        match self {
            Self::One(value) => {
                if value.is_empty() {
                    return Err(PilotProfileCredentialError::EmptyAudience);
                }
            }
            Self::Many(values) => {
                if values.is_empty() || values.iter().any(String::is_empty) {
                    return Err(PilotProfileCredentialError::EmptyAudience);
                }
            }
        }
        Ok(())
    }

    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::One(value) => value == expected,
            Self::Many(values) => values.iter().any(|value| value == expected),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PilotProfileCredentialSubjectDraft {
    pub name: Option<String>,
    pub country: Option<String>,
    pub additional_fields: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PilotProfileCredentialRequest {
    pub subject: PilotProfileCredentialSubjectDraft,
    pub jti: String,
    pub audience: Option<String>,
    pub expires_in_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PilotProfileCredentialJwt {
    compact: String,
    signing_input: String,
    signature: [u8; 64],
    header: PilotProfileCredentialJoseHeader,
    claims: PilotProfileCredentialClaims,
}

impl PilotProfileCredentialJwt {
    pub fn issue(
        pilot_auth_secret_key: &iroh::SecretKey,
        claims: PilotProfileCredentialClaims,
    ) -> Result<Self, PilotProfileCredentialError> {
        validate_claims(&claims)?;
        let header = PilotProfileCredentialJoseHeader {
            alg: VC_JWT_ALG.to_string(),
            typ: VC_JWT_TYP.to_string(),
            kid: claims.iss.key_id(),
        };
        validate_header(&header, &claims.iss)?;

        let header_segment = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
        let payload_segment = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let signing_input = format!("{header_segment}.{payload_segment}");
        let signature = pilot_auth_secret_key
            .sign(signing_input.as_bytes())
            .to_bytes();
        let signature_segment = URL_SAFE_NO_PAD.encode(signature);

        Ok(Self {
            compact: format!("{signing_input}.{signature_segment}"),
            signing_input,
            signature,
            header,
            claims,
        })
    }

    pub fn parse(compact: &str) -> Result<Self, PilotProfileCredentialError> {
        let mut segments = compact.split('.');
        let header_segment = segments
            .next()
            .ok_or(PilotProfileCredentialError::MalformedCompactJwt)?;
        let payload_segment = segments
            .next()
            .ok_or(PilotProfileCredentialError::MalformedCompactJwt)?;
        let signature_segment = segments
            .next()
            .ok_or(PilotProfileCredentialError::MalformedCompactJwt)?;
        if segments.next().is_some() {
            return Err(PilotProfileCredentialError::MalformedCompactJwt);
        }

        let header_bytes = URL_SAFE_NO_PAD.decode(header_segment)?;
        let payload_bytes = URL_SAFE_NO_PAD.decode(payload_segment)?;
        let signature_bytes = URL_SAFE_NO_PAD.decode(signature_segment)?;
        let signature: [u8; 64] = signature_bytes
            .try_into()
            .map_err(|_| PilotProfileCredentialError::SignatureEncoding)?;

        let header: PilotProfileCredentialJoseHeader = serde_json::from_slice(&header_bytes)?;
        let claims: PilotProfileCredentialClaims = serde_json::from_slice(&payload_bytes)?;
        validate_header(&header, &claims.iss)?;
        validate_claims(&claims)?;

        Ok(Self {
            compact: compact.to_string(),
            signing_input: format!("{header_segment}.{payload_segment}"),
            signature,
            header,
            claims,
        })
    }

    pub fn compact(&self) -> &str {
        &self.compact
    }

    pub fn header(&self) -> &PilotProfileCredentialJoseHeader {
        &self.header
    }

    pub fn claims(&self) -> &PilotProfileCredentialClaims {
        &self.claims
    }

    pub fn verify_signature(&self) -> Result<(), PilotProfileCredentialError> {
        let signature = iroh::Signature::from_bytes(&self.signature);
        self.claims
            .iss
            .public_key()
            .verify(self.signing_input.as_bytes(), &signature)
            .map_err(|_| PilotProfileCredentialError::SignatureVerification)
    }

    pub fn verify_authoritative<G: GovernanceLookup, C: Clock>(
        &self,
        governance: &G,
        clock: &C,
        expected_audience: Option<&str>,
    ) -> Result<(), PilotProfileCredentialError> {
        let now = clock.now_unix_seconds();
        if self.claims.nbf > now {
            return Err(PilotProfileCredentialError::NotYetValid {
                not_before: self.claims.nbf,
            });
        }
        if let Some(expires_at) = self.claims.exp
            && now >= expires_at
        {
            return Err(PilotProfileCredentialError::Expired { expires_at });
        }
        if let Some(expected) = expected_audience {
            match &self.claims.aud {
                Some(audience) if audience.contains(expected) => {}
                _ => {
                    return Err(PilotProfileCredentialError::AudienceMismatch {
                        expected: expected.to_string(),
                    });
                }
            }
        }

        let state = governance.resolve_pilot_auth_did_state(&self.claims.sub)?;
        if state.requires_catch_up() {
            return Err(PilotProfileCredentialError::GovernanceIncomplete(
                self.claims.sub.clone(),
            ));
        }
        let authoritative = state.authoritative.ok_or_else(|| {
            PilotProfileCredentialError::GovernanceUnavailable(self.claims.sub.clone())
        })?;
        if authoritative.pilot_auth_did != self.claims.iss {
            let records = governance.load_pilot_auth_did_records(&self.claims.sub)?;
            if issuer_is_retired_authoritative_did(&records, &authoritative, &self.claims.iss) {
                return Err(PilotProfileCredentialError::HistoricalCredential {
                    presented: self.claims.iss.clone(),
                    authoritative: authoritative.pilot_auth_did,
                });
            }
            return Err(PilotProfileCredentialError::StaleCredential {
                presented: self.claims.iss.clone(),
                authoritative: authoritative.pilot_auth_did,
            });
        }

        Ok(())
    }
}

pub fn issue_pilot_profile_credential<G: GovernanceLookup, C: Clock>(
    governance: &G,
    pilot_identity: &PilotIdentity,
    request: PilotProfileCredentialRequest,
    clock: &C,
) -> Result<PilotProfileCredentialJwt, PilotProfileCredentialError> {
    if request.jti.is_empty() {
        return Err(PilotProfileCredentialError::EmptyJti);
    }
    if let Some(audience) = &request.audience
        && audience.is_empty()
    {
        return Err(PilotProfileCredentialError::EmptyAudience);
    }

    let pilot_id = pilot_identity.pilot_id();
    let state = governance.resolve_pilot_auth_did_state(&pilot_id)?;
    if state.requires_catch_up() {
        return Err(PilotProfileCredentialError::GovernanceIncomplete(pilot_id));
    }
    let authoritative = state
        .authoritative
        .ok_or_else(|| PilotProfileCredentialError::GovernanceUnavailable(pilot_id.clone()))?;
    let local_active_did = pilot_identity.active_pilot_auth_did();
    if authoritative.pilot_auth_did != local_active_did {
        return Err(PilotProfileCredentialError::IssuanceDidMismatch {
            local: local_active_did,
            authoritative: authoritative.pilot_auth_did,
        });
    }

    let issued_at = clock.now_unix_seconds();
    let expires_at = match request.expires_in_seconds {
        Some(seconds) => Some(
            issued_at
                .checked_add(
                    seconds
                        .try_into()
                        .map_err(|_| PilotProfileCredentialError::ExpiryOverflow)?,
                )
                .ok_or(PilotProfileCredentialError::ExpiryOverflow)?,
        ),
        None => None,
    };
    let issuer_did = authoritative.pilot_auth_did;

    let claims = PilotProfileCredentialClaims {
        iss: issuer_did.clone(),
        sub: pilot_id.clone(),
        nbf: issued_at,
        iat: issued_at,
        exp: expires_at,
        aud: request.audience.map(JwtAudience::One),
        jti: request.jti,
        vc: PilotProfileCredentialVc {
            context: vec![VC_CONTEXT.to_string()],
            credential_type: vec![
                VC_TYPE.to_string(),
                PILOT_PROFILE_CREDENTIAL_TYPE.to_string(),
            ],
            credential_subject: PilotProfileCredentialSubject {
                id: pilot_id,
                pilot_auth_did: issuer_did,
                name: request.subject.name,
                country: request.subject.country,
                additional_fields: request.subject.additional_fields,
            },
        },
    };

    PilotProfileCredentialJwt::issue(&pilot_identity.active_pilot_auth_secret_key(), claims)
}

pub fn verify_pilot_profile_credential<G: GovernanceLookup, C: Clock>(
    compact_jwt: &str,
    governance: &G,
    clock: &C,
    expected_audience: Option<&str>,
) -> Result<PilotProfileCredentialJwt, PilotProfileCredentialError> {
    let jwt = PilotProfileCredentialJwt::parse(compact_jwt)?;
    jwt.verify_signature()?;
    jwt.verify_authoritative(governance, clock, expected_audience)?;
    Ok(jwt)
}

fn validate_header(
    header: &PilotProfileCredentialJoseHeader,
    issuer_did: &DidKey,
) -> Result<(), PilotProfileCredentialError> {
    if header.alg != VC_JWT_ALG {
        return Err(PilotProfileCredentialError::UnsupportedAlgorithm(
            header.alg.clone(),
        ));
    }
    if header.typ != VC_JWT_TYP {
        return Err(PilotProfileCredentialError::UnsupportedType(
            header.typ.clone(),
        ));
    }
    let expected_kid = issuer_did.key_id();
    if header.kid != expected_kid {
        return Err(PilotProfileCredentialError::KidMismatch {
            expected: expected_kid,
            found: header.kid.clone(),
        });
    }
    Ok(())
}

fn validate_claims(
    claims: &PilotProfileCredentialClaims,
) -> Result<(), PilotProfileCredentialError> {
    if claims.jti.is_empty() {
        return Err(PilotProfileCredentialError::EmptyJti);
    }
    if let Some(expires_at) = claims.exp
        && expires_at <= claims.nbf
    {
        return Err(PilotProfileCredentialError::InvalidExpiryWindow);
    }
    if let Some(audience) = &claims.aud {
        audience.validate()?;
    }
    if !claims.vc.context.iter().any(|value| value == VC_CONTEXT) {
        return Err(PilotProfileCredentialError::MissingContext);
    }
    if !claims
        .vc
        .credential_type
        .iter()
        .any(|value| value == VC_TYPE)
    {
        return Err(PilotProfileCredentialError::MissingVerifiableCredentialType);
    }
    if !claims
        .vc
        .credential_type
        .iter()
        .any(|value| value == PILOT_PROFILE_CREDENTIAL_TYPE)
    {
        return Err(PilotProfileCredentialError::MissingPilotProfileCredentialType);
    }
    if claims.vc.credential_subject.id != claims.sub {
        return Err(PilotProfileCredentialError::SubjectIdMismatch);
    }
    if claims.vc.credential_subject.pilot_auth_did != claims.iss {
        return Err(PilotProfileCredentialError::SubjectDidMismatch);
    }
    if let Some(country) = &claims.vc.credential_subject.country
        && !is_iso_3166_alpha2_country_code(country)
    {
        return Err(PilotProfileCredentialError::InvalidCountry(country.clone()));
    }
    Ok(())
}

fn issuer_is_retired_authoritative_did(
    records: &[crate::governance::PilotAuthDidRecord],
    authoritative: &crate::governance::PilotAuthDidRecord,
    issuer: &DidKey,
) -> bool {
    use std::collections::HashMap;

    let records_by_id = records
        .iter()
        .map(|record| (record.record_id.clone(), record))
        .collect::<HashMap<_, _>>();
    let mut current = authoritative;
    while let Some(previous_id) = current.supersedes.as_ref() {
        let Some(previous) = records_by_id.get(previous_id) else {
            return false;
        };
        if &previous.pilot_auth_did == issuer {
            return true;
        }
        current = previous;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::PilotAuthDidRecord;
    use crate::identity::DidKey;
    use crate::identity::test_helpers::{
        deterministic_secret_key, sample_request, seed_identity_and_governance,
        temp_governance_store,
    };

    #[test]
    fn fixture_shape_matches_models() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../specs/fixtures/v0.3/profile_vc_jwt.json"
        ))
        .unwrap();
        let header: PilotProfileCredentialJoseHeader =
            serde_json::from_value(fixture["header"].clone()).unwrap();
        let claims: PilotProfileCredentialClaims =
            serde_json::from_value(fixture["payload"].clone()).unwrap();

        validate_header(&header, &claims.iss).unwrap();
        validate_claims(&claims).unwrap();
        assert_eq!(claims.iss.key_id(), header.kid);
    }

    #[test]
    fn signed_jwt_round_trips_and_verifies() {
        let (identity, store, _dir) = seed_identity_and_governance(21, 22);
        let clock = FixedClock::new(1_745_572_800);
        let issued =
            issue_pilot_profile_credential(&store, &identity, sample_request(), &clock).unwrap();
        assert_eq!(
            issued.compact(),
            "eyJhbGciOiJFZERTQSIsInR5cCI6InZjK2p3dCIsImtpZCI6ImRpZDprZXk6ejZNa2p1dDFtdWU0WFJXRHJOa3ZFdXhjMk1MdUI5Y044THBYYTVMZks4N1c1SktiI3o2TWtqdXQxbXVlNFhSV0RyTmt2RXV4YzJNTHVCOWNOOExwWGE1TGZLODdXNUpLYiJ9.eyJpc3MiOiJkaWQ6a2V5Ono2TWtqdXQxbXVlNFhSV0RyTmt2RXV4YzJNTHVCOWNOOExwWGE1TGZLODdXNUpLYiIsInN1YiI6ImlnY25ldDppZDpkNTQyMDdkYTE5NDk3N2RjZjQ2YWRiZmVjMmJjMmU3NWI1MmQ1YThhNDIxODRmZWRmZGMwMDAyNGYwZTNlOGRhIiwibmJmIjoxNzQ1NTcyODAwLCJpYXQiOjE3NDU1NzI4MDAsImp0aSI6InVybjp1dWlkOmY0N2FjMTBiLTU4Y2MtNDM3Mi1hNTY3LTBlMDJiMmMzZDQ3OSIsInZjIjp7IkBjb250ZXh0IjpbImh0dHBzOi8vd3d3LnczLm9yZy8yMDE4L2NyZWRlbnRpYWxzL3YxIl0sInR5cGUiOlsiVmVyaWZpYWJsZUNyZWRlbnRpYWwiLCJQaWxvdFByb2ZpbGVDcmVkZW50aWFsIl0sImNyZWRlbnRpYWxTdWJqZWN0Ijp7ImlkIjoiaWdjbmV0OmlkOmQ1NDIwN2RhMTk0OTc3ZGNmNDZhZGJmZWMyYmMyZTc1YjUyZDVhOGE0MjE4NGZlZGZkYzAwMDI0ZjBlM2U4ZGEiLCJwaWxvdF9hdXRoX2RpZCI6ImRpZDprZXk6ejZNa2p1dDFtdWU0WFJXRHJOa3ZFdXhjMk1MdUI5Y044THBYYTVMZks4N1c1SktiIiwibmFtZSI6IkFsaWNlIEV4YW1wbGUiLCJjb3VudHJ5IjoiTk8ifX19.mWr58Qj7akzE-tfZJPF9JcSB3cxbzCmR3TaP5JgnrPvzN-AJG0qkqarc1pO3H3oqoI-Cvse6xGI1hYuGWVd4DA"
        );

        let parsed = PilotProfileCredentialJwt::parse(issued.compact()).unwrap();
        assert_eq!(parsed.claims(), issued.claims());
        parsed.verify_signature().unwrap();
        parsed.verify_authoritative(&store, &clock, None).unwrap();
    }

    #[test]
    fn rejects_wrong_jose_alg() {
        let issuer = DidKey::from_public_key(deterministic_secret_key(31).public());
        let header = PilotProfileCredentialJoseHeader {
            alg: "HS256".to_string(),
            typ: VC_JWT_TYP.to_string(),
            kid: issuer.key_id(),
        };

        let err = validate_header(&header, &issuer).unwrap_err();
        assert!(matches!(
            err,
            PilotProfileCredentialError::UnsupportedAlgorithm(value) if value == "HS256"
        ));
    }

    #[test]
    fn rejects_tentative_governance_for_high_trust_issue_and_verify() {
        let pilot_root = deterministic_secret_key(41);
        let pilot_auth = deterministic_secret_key(42);
        let identity = PilotIdentity::from_secret_keys(pilot_root.clone(), pilot_auth.clone());
        let (store, _dir) = temp_governance_store();
        let incomplete = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(pilot_auth.public()),
            Some(crate::id::Blake3Hex::parse("a".repeat(64)).unwrap()),
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        store.persist_pilot_auth_did_record(&incomplete).unwrap();
        let clock = FixedClock::new(1_745_572_800);

        let issue_err = issue_pilot_profile_credential(&store, &identity, sample_request(), &clock)
            .unwrap_err();
        assert!(matches!(
            issue_err,
            PilotProfileCredentialError::GovernanceIncomplete(pilot_id)
                if pilot_id == identity.pilot_id()
        ));

        let claims = PilotProfileCredentialClaims {
            iss: identity.active_pilot_auth_did(),
            sub: identity.pilot_id(),
            nbf: clock.now_unix_seconds(),
            iat: clock.now_unix_seconds(),
            exp: None,
            aud: None,
            jti: "tentative".to_string(),
            vc: PilotProfileCredentialVc {
                context: vec![VC_CONTEXT.to_string()],
                credential_type: vec![
                    VC_TYPE.to_string(),
                    PILOT_PROFILE_CREDENTIAL_TYPE.to_string(),
                ],
                credential_subject: PilotProfileCredentialSubject {
                    id: identity.pilot_id(),
                    pilot_auth_did: identity.active_pilot_auth_did(),
                    name: Some("Alice".to_string()),
                    country: Some("NO".to_string()),
                    additional_fields: BTreeMap::new(),
                },
            },
        };
        let jwt =
            PilotProfileCredentialJwt::issue(&identity.active_pilot_auth_secret_key(), claims)
                .unwrap();
        let verify_err = jwt.verify_authoritative(&store, &clock, None).unwrap_err();
        assert!(matches!(
            verify_err,
            PilotProfileCredentialError::GovernanceIncomplete(pilot_id)
                if pilot_id == identity.pilot_id()
        ));
    }

    #[test]
    fn stale_credential_is_rejected_after_rotation() {
        let (identity, store, _dir) = seed_identity_and_governance(51, 52);
        let clock = FixedClock::new(1_745_572_800);
        let issued =
            issue_pilot_profile_credential(&store, &identity, sample_request(), &clock).unwrap();
        issued.verify_authoritative(&store, &clock, None).unwrap();

        let rotated = PilotAuthDidRecord::issue(
            &identity.pilot_id_secret_key(),
            DidKey::from_public_key(deterministic_secret_key(53).public()),
            Some(
                store
                    .resolve_pilot_auth_did_state(&identity.pilot_id())
                    .unwrap()
                    .authoritative
                    .unwrap()
                    .record_id,
            ),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        store.persist_pilot_auth_did_record(&rotated).unwrap();

        let err = issued
            .verify_authoritative(&store, &clock, None)
            .unwrap_err();
        assert!(matches!(
            err,
            PilotProfileCredentialError::HistoricalCredential {
                presented,
                authoritative
            } if presented == identity.active_pilot_auth_did()
                && authoritative == rotated.pilot_auth_did
        ));
    }

    #[test]
    fn unknown_non_historical_issuer_is_reported_as_stale() {
        let (identity, store, _dir) = seed_identity_and_governance(54, 55);
        let clock = FixedClock::new(1_745_572_800);
        let unrelated_secret = deterministic_secret_key(56);
        let unrelated_did = DidKey::from_public_key(unrelated_secret.public());
        let jwt = PilotProfileCredentialJwt::issue(
            &unrelated_secret,
            PilotProfileCredentialClaims {
                iss: unrelated_did.clone(),
                sub: identity.pilot_id(),
                nbf: clock.now_unix_seconds(),
                iat: clock.now_unix_seconds(),
                exp: None,
                aud: None,
                jti: "unknown-stale".to_string(),
                vc: PilotProfileCredentialVc {
                    context: vec![VC_CONTEXT.to_string()],
                    credential_type: vec![
                        VC_TYPE.to_string(),
                        PILOT_PROFILE_CREDENTIAL_TYPE.to_string(),
                    ],
                    credential_subject: PilotProfileCredentialSubject {
                        id: identity.pilot_id(),
                        pilot_auth_did: unrelated_did,
                        name: Some("Alice".to_string()),
                        country: Some("NO".to_string()),
                        additional_fields: BTreeMap::new(),
                    },
                },
            },
        )
        .unwrap();

        let err = jwt.verify_authoritative(&store, &clock, None).unwrap_err();
        assert!(matches!(
            err,
            PilotProfileCredentialError::StaleCredential {
                presented,
                authoritative
            } if presented == DidKey::from_public_key(unrelated_secret.public())
                && authoritative == identity.active_pilot_auth_did()
        ));
    }

    #[test]
    fn exp_is_accepted_before_expiry_and_rejected_after() {
        let (identity, store, _dir) = seed_identity_and_governance(61, 62);
        let request = PilotProfileCredentialRequest {
            expires_in_seconds: Some(60),
            ..sample_request()
        };
        let issued_at = 1_745_572_800;
        let issued =
            issue_pilot_profile_credential(&store, &identity, request, &FixedClock::new(issued_at))
                .unwrap();

        issued
            .verify_authoritative(&store, &FixedClock::new(issued_at + 59), None)
            .unwrap();
        let err = issued
            .verify_authoritative(&store, &FixedClock::new(issued_at + 60), None)
            .unwrap_err();
        assert!(matches!(
            err,
            PilotProfileCredentialError::Expired {
                expires_at
            } if expires_at == issued_at + 60
        ));
    }

    #[test]
    fn audience_must_match_when_expected() {
        let (identity, store, _dir) = seed_identity_and_governance(71, 72);
        let request = PilotProfileCredentialRequest {
            audience: Some("portal-a".to_string()),
            ..sample_request()
        };
        let clock = FixedClock::new(1_745_572_800);
        let issued = issue_pilot_profile_credential(&store, &identity, request, &clock).unwrap();

        issued
            .verify_authoritative(&store, &clock, Some("portal-a"))
            .unwrap();
        let err = issued
            .verify_authoritative(&store, &clock, Some("portal-b"))
            .unwrap_err();
        assert!(matches!(
            err,
            PilotProfileCredentialError::AudienceMismatch { expected } if expected == "portal-b"
        ));
    }

    #[test]
    fn rejects_unknown_but_well_formed_country_code() {
        let (identity, store, _dir) = seed_identity_and_governance(81, 82);
        let clock = FixedClock::new(1_745_572_800);
        let request = PilotProfileCredentialRequest {
            subject: PilotProfileCredentialSubjectDraft {
                country: Some("ZZ".to_string()),
                ..PilotProfileCredentialSubjectDraft::default()
            },
            ..sample_request()
        };

        let err = issue_pilot_profile_credential(&store, &identity, request, &clock).unwrap_err();
        assert!(matches!(
            err,
            PilotProfileCredentialError::InvalidCountry(value) if value == "ZZ"
        ));
    }
}
