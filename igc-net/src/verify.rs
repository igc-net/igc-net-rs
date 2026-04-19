use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;

use crate::governance::GovernanceLookup;
use crate::id::PilotId;
use crate::identity::{
    Clock, DidWebResolutionError, DidWebResolver, PilotProfileCredentialError,
    PilotProfileCredentialJwt,
};

#[derive(Debug, thiserror::Error)]
pub enum HighTrustVerificationError {
    #[error("{0}")]
    Credential(#[from] PilotProfileCredentialError),
    #[error("unsupported issuer DID method for PilotProfileCredential: {issuer}")]
    UnsupportedIssuerMethod { issuer: String },
}

#[derive(Debug)]
pub enum PilotProfileCredentialVerification {
    ValidAuthoritative(PilotProfileCredentialJwt),
    Invalid {
        credential: Option<PilotProfileCredentialJwt>,
        error: HighTrustVerificationError,
    },
    UnverifiableGovernanceIncomplete {
        credential: PilotProfileCredentialJwt,
        pilot_id: PilotId,
    },
    UnverifiableGovernanceUnavailable {
        credential: PilotProfileCredentialJwt,
        pilot_id: PilotId,
    },
    UnverifiableDidWebResolution {
        issuer: String,
        error: DidWebResolutionError,
    },
}

pub struct PilotProfileCredentialVerifier<'a, G, C> {
    governance: &'a G,
    clock: &'a C,
    did_web_resolver: Option<&'a dyn DidWebResolver>,
    expected_audience: Option<&'a str>,
}

impl<'a, G, C> PilotProfileCredentialVerifier<'a, G, C>
where
    G: GovernanceLookup,
    C: Clock,
{
    pub fn new(governance: &'a G, clock: &'a C) -> Self {
        Self {
            governance,
            clock,
            did_web_resolver: None,
            expected_audience: None,
        }
    }

    pub fn with_did_web_resolver(mut self, resolver: &'a dyn DidWebResolver) -> Self {
        self.did_web_resolver = Some(resolver);
        self
    }

    pub fn with_expected_audience(mut self, expected_audience: &'a str) -> Self {
        self.expected_audience = Some(expected_audience);
        self
    }

    pub fn verify(&self, compact_jwt: &str) -> PilotProfileCredentialVerification {
        let raw = match RawCompactJwt::decode(compact_jwt) {
            Ok(raw) => raw,
            Err(error) => {
                return PilotProfileCredentialVerification::Invalid {
                    credential: None,
                    error: HighTrustVerificationError::Credential(error),
                };
            }
        };

        match issuer_method(&raw.claims.iss) {
            IssuerDidMethod::DidKey => self.verify_did_key(compact_jwt),
            IssuerDidMethod::DidWeb => {
                let resolution = match self.did_web_resolver {
                    Some(resolver) => resolver
                        .resolve_verification_method(&raw.claims.iss, raw.header.kid.as_deref()),
                    None => Err(DidWebResolutionError::ResolverUnavailable),
                };
                match resolution {
                    Ok(_) => PilotProfileCredentialVerification::Invalid {
                        credential: None,
                        error: HighTrustVerificationError::UnsupportedIssuerMethod {
                            issuer: raw.claims.iss,
                        },
                    },
                    Err(error) => {
                        PilotProfileCredentialVerification::UnverifiableDidWebResolution {
                            issuer: raw.claims.iss,
                            error,
                        }
                    }
                }
            }
            IssuerDidMethod::Other => PilotProfileCredentialVerification::Invalid {
                credential: None,
                error: HighTrustVerificationError::UnsupportedIssuerMethod {
                    issuer: raw.claims.iss,
                },
            },
        }
    }

    fn verify_did_key(&self, compact_jwt: &str) -> PilotProfileCredentialVerification {
        let jwt = match PilotProfileCredentialJwt::parse(compact_jwt) {
            Ok(jwt) => jwt,
            Err(error) => {
                return PilotProfileCredentialVerification::Invalid {
                    credential: None,
                    error: HighTrustVerificationError::Credential(error),
                };
            }
        };
        if let Err(error) = jwt.verify_signature() {
            return PilotProfileCredentialVerification::Invalid {
                credential: Some(jwt),
                error: HighTrustVerificationError::Credential(error),
            };
        }

        match jwt.verify_authoritative(self.governance, self.clock, self.expected_audience) {
            Ok(()) => PilotProfileCredentialVerification::ValidAuthoritative(jwt),
            Err(PilotProfileCredentialError::GovernanceIncomplete(pilot_id)) => {
                PilotProfileCredentialVerification::UnverifiableGovernanceIncomplete {
                    credential: jwt,
                    pilot_id,
                }
            }
            Err(PilotProfileCredentialError::GovernanceUnavailable(pilot_id)) => {
                PilotProfileCredentialVerification::UnverifiableGovernanceUnavailable {
                    credential: jwt,
                    pilot_id,
                }
            }
            Err(error) => PilotProfileCredentialVerification::Invalid {
                credential: Some(jwt),
                error: HighTrustVerificationError::Credential(error),
            },
        }
    }
}

pub fn verify_pilot_profile_credential_high_trust<G, C>(
    compact_jwt: &str,
    governance: &G,
    clock: &C,
) -> PilotProfileCredentialVerification
where
    G: GovernanceLookup,
    C: Clock,
{
    PilotProfileCredentialVerifier::new(governance, clock).verify(compact_jwt)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IssuerDidMethod {
    DidKey,
    DidWeb,
    Other,
}

fn issuer_method(issuer: &str) -> IssuerDidMethod {
    if issuer.starts_with("did:key:") {
        IssuerDidMethod::DidKey
    } else if issuer.starts_with("did:web:") {
        IssuerDidMethod::DidWeb
    } else {
        IssuerDidMethod::Other
    }
}

#[derive(Deserialize)]
struct RawJoseHeader {
    #[serde(default)]
    kid: Option<String>,
}

#[derive(Deserialize)]
struct RawClaims {
    iss: String,
}

struct RawCompactJwt {
    header: RawJoseHeader,
    claims: RawClaims,
}

impl RawCompactJwt {
    fn decode(compact_jwt: &str) -> Result<Self, PilotProfileCredentialError> {
        let mut segments = compact_jwt.split('.');
        let header_segment = segments
            .next()
            .ok_or(PilotProfileCredentialError::MalformedCompactJwt)?;
        let payload_segment = segments
            .next()
            .ok_or(PilotProfileCredentialError::MalformedCompactJwt)?;
        let _signature_segment = segments
            .next()
            .ok_or(PilotProfileCredentialError::MalformedCompactJwt)?;
        if segments.next().is_some() {
            return Err(PilotProfileCredentialError::MalformedCompactJwt);
        }

        let header_bytes = URL_SAFE_NO_PAD.decode(header_segment)?;
        let payload_bytes = URL_SAFE_NO_PAD.decode(payload_segment)?;
        Ok(Self {
            header: serde_json::from_slice(&header_bytes)?,
            claims: serde_json::from_slice(&payload_bytes)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::governance::PilotAuthDidRecord;
    use crate::identity::{
        DidKey, FixedClock, issue_pilot_profile_credential,
        test_helpers::{
            deterministic_secret_key, sample_request, seed_identity_and_governance,
            temp_governance_store,
        },
    };
    use crate::keys::PilotIdentity;

    struct FailingDidWebResolver;

    impl DidWebResolver for FailingDidWebResolver {
        fn resolve_verification_method(
            &self,
            _did: &str,
            _kid: Option<&str>,
        ) -> Result<crate::ResolvedDidWebVerificationMethod, DidWebResolutionError> {
            Err(DidWebResolutionError::Fetch("network down".to_string()))
        }
    }

    struct SuccessfulDidWebResolver(iroh::PublicKey);

    impl DidWebResolver for SuccessfulDidWebResolver {
        fn resolve_verification_method(
            &self,
            did: &str,
            kid: Option<&str>,
        ) -> Result<crate::ResolvedDidWebVerificationMethod, DidWebResolutionError> {
            Ok(crate::ResolvedDidWebVerificationMethod {
                did: did.to_string(),
                kid: kid.map(str::to_string),
                public_key: self.0,
            })
        }
    }

    #[test]
    fn valid_credential_is_reported_as_authoritative() {
        let (identity, store, _dir) = seed_identity_and_governance(91, 92);
        let clock = FixedClock::new(1_745_572_800);
        let jwt =
            issue_pilot_profile_credential(&store, &identity, sample_request(), &clock).unwrap();

        let outcome = PilotProfileCredentialVerifier::new(&store, &clock).verify(jwt.compact());
        assert!(matches!(
            outcome,
            PilotProfileCredentialVerification::ValidAuthoritative(_)
        ));
    }

    #[test]
    fn malformed_compact_jwt_is_invalid() {
        let (store, _dir) = temp_governance_store();
        let clock = FixedClock::new(1_745_572_800);

        let outcome = PilotProfileCredentialVerifier::new(&store, &clock).verify("not-a-jwt");
        assert!(matches!(
            outcome,
            PilotProfileCredentialVerification::Invalid {
                credential: None,
                error: HighTrustVerificationError::Credential(
                    PilotProfileCredentialError::MalformedCompactJwt
                )
            }
        ));
    }

    #[test]
    fn incomplete_governance_is_unverifiable() {
        let pilot_root = deterministic_secret_key(101);
        let pilot_auth = deterministic_secret_key(102);
        let identity = PilotIdentity::from_secret_keys(pilot_root.clone(), pilot_auth.clone());
        let (store, _dir) = temp_governance_store();
        let incomplete = PilotAuthDidRecord::issue(
            &pilot_root,
            DidKey::from_public_key(pilot_auth.public()),
            Some(crate::Blake3Hex::parse("a".repeat(64)).unwrap()),
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        store.persist_pilot_auth_did_record(&incomplete).unwrap();
        let clock = FixedClock::new(1_745_572_800);
        let claims = crate::PilotProfileCredentialClaims {
            iss: identity.active_pilot_auth_did(),
            sub: identity.pilot_id(),
            nbf: clock.now_unix_seconds(),
            iat: clock.now_unix_seconds(),
            exp: None,
            aud: None,
            jti: "tentative".to_string(),
            vc: crate::PilotProfileCredentialVc {
                context: vec!["https://www.w3.org/2018/credentials/v1".to_string()],
                credential_type: vec![
                    "VerifiableCredential".to_string(),
                    "PilotProfileCredential".to_string(),
                ],
                credential_subject: crate::PilotProfileCredentialSubject {
                    id: identity.pilot_id(),
                    pilot_auth_did: identity.active_pilot_auth_did(),
                    name: Some("Alice".to_string()),
                    country: Some("NO".to_string()),
                    additional_fields: BTreeMap::new(),
                },
            },
        };
        let jwt = crate::PilotProfileCredentialJwt::issue(
            &identity.active_pilot_auth_secret_key(),
            claims,
        )
        .unwrap();

        let outcome = PilotProfileCredentialVerifier::new(&store, &clock).verify(jwt.compact());
        assert!(matches!(
            outcome,
            PilotProfileCredentialVerification::UnverifiableGovernanceIncomplete {
                pilot_id,
                ..
            } if pilot_id == identity.pilot_id()
        ));
    }

    #[test]
    fn missing_governance_is_unverifiable() {
        let (identity, _store_with_state, _dir1) = seed_identity_and_governance(111, 112);
        let (empty_store, _dir2) = temp_governance_store();
        let clock = FixedClock::new(1_745_572_800);
        let jwt = crate::PilotProfileCredentialJwt::issue(
            &identity.active_pilot_auth_secret_key(),
            crate::PilotProfileCredentialClaims {
                iss: identity.active_pilot_auth_did(),
                sub: identity.pilot_id(),
                nbf: clock.now_unix_seconds(),
                iat: clock.now_unix_seconds(),
                exp: None,
                aud: None,
                jti: "missing".to_string(),
                vc: crate::PilotProfileCredentialVc {
                    context: vec!["https://www.w3.org/2018/credentials/v1".to_string()],
                    credential_type: vec![
                        "VerifiableCredential".to_string(),
                        "PilotProfileCredential".to_string(),
                    ],
                    credential_subject: crate::PilotProfileCredentialSubject {
                        id: identity.pilot_id(),
                        pilot_auth_did: identity.active_pilot_auth_did(),
                        name: Some("Alice".to_string()),
                        country: Some("NO".to_string()),
                        additional_fields: BTreeMap::new(),
                    },
                },
            },
        )
        .unwrap();

        let outcome =
            PilotProfileCredentialVerifier::new(&empty_store, &clock).verify(jwt.compact());
        assert!(matches!(
            outcome,
            PilotProfileCredentialVerification::UnverifiableGovernanceUnavailable {
                pilot_id,
                ..
            } if pilot_id == identity.pilot_id()
        ));
    }

    #[test]
    fn stale_credential_is_invalid() {
        let (identity, store, _dir) = seed_identity_and_governance(121, 122);
        let clock = FixedClock::new(1_745_572_800);
        let jwt =
            issue_pilot_profile_credential(&store, &identity, sample_request(), &clock).unwrap();
        let rotated = PilotAuthDidRecord::issue(
            &identity.pilot_id_secret_key(),
            DidKey::from_public_key(deterministic_secret_key(123).public()),
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

        let outcome = PilotProfileCredentialVerifier::new(&store, &clock).verify(jwt.compact());
        assert!(matches!(
            outcome,
            PilotProfileCredentialVerification::Invalid {
                error: HighTrustVerificationError::Credential(
                    PilotProfileCredentialError::HistoricalCredential { .. }
                ),
                ..
            }
        ));
    }

    #[test]
    fn did_web_resolution_failure_is_unverifiable() {
        let (store, _dir) = temp_governance_store();
        let clock = FixedClock::new(1_745_572_800);
        let header = serde_json::json!({
            "alg": "EdDSA",
            "typ": "vc+jwt",
            "kid": "did:web:pilot.example#key-1"
        });
        let payload = serde_json::json!({
            "iss": "did:web:pilot.example",
            "sub": "igcnet:id:d54207da194977dcf46adbfec2bc2e75b52d5a8a42184fedfdc00024f0e3e8da"
        });
        let compact = format!(
            "{}.{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap()),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap()),
            URL_SAFE_NO_PAD.encode([0u8; 64]),
        );

        let outcome = PilotProfileCredentialVerifier::new(&store, &clock)
            .with_did_web_resolver(&FailingDidWebResolver)
            .verify(&compact);
        assert!(matches!(
            outcome,
            PilotProfileCredentialVerification::UnverifiableDidWebResolution {
                issuer,
                error: DidWebResolutionError::Fetch(reason)
            } if issuer == "did:web:pilot.example" && reason == "network down"
        ));
    }

    #[test]
    fn resolved_did_web_issuer_is_still_invalid_for_pilot_profile_credential() {
        let (store, _dir) = temp_governance_store();
        let clock = FixedClock::new(1_745_572_800);
        let header = serde_json::json!({
            "alg": "EdDSA",
            "typ": "vc+jwt",
            "kid": "did:web:pilot.example#key-1"
        });
        let payload = serde_json::json!({
            "iss": "did:web:pilot.example",
            "sub": "igcnet:id:d54207da194977dcf46adbfec2bc2e75b52d5a8a42184fedfdc00024f0e3e8da"
        });
        let compact = format!(
            "{}.{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap()),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap()),
            URL_SAFE_NO_PAD.encode([0u8; 64]),
        );

        let outcome = PilotProfileCredentialVerifier::new(&store, &clock)
            .with_did_web_resolver(&SuccessfulDidWebResolver(
                iroh::SecretKey::from_bytes(&[88u8; 32]).public(),
            ))
            .verify(&compact);
        assert!(matches!(
            outcome,
            PilotProfileCredentialVerification::Invalid {
                credential: None,
                error: HighTrustVerificationError::UnsupportedIssuerMethod { issuer }
            } if issuer == "did:web:pilot.example"
        ));
    }
}
