//! `IgcNet` gRPC service implementation.
//!
//! POC scope: FetchArtifact (restricted + public), ProvisionPrivateAccessKey,
//! RevokePrivateAccess, GetNodeStatus.  Other RPCs return UNIMPLEMENTED.

use std::sync::Arc;

use std::pin::Pin;

use futures::Stream;
use tonic::{Request, Response, Status};

use igc_net::{
    ArtifactClass, FetchProof, FlatFileStore, PrivateAccessKeyStore, SeqNumStore,
    verify_fetch_proof,
};

use crate::proto::igc_net_server::IgcNet;
use crate::proto::{
    FetchArtifactRequest, FetchArtifactResponse, GetNodeStatusRequest, GetNodeStatusResponse,
    GovernanceSyncState, ProvisionPrivateAccessKeyRequest, ProvisionPrivateAccessKeyResponse,
    PublishFlightRequest, PublishFlightResponse, QueryIndexRequest, QueryIndexResponse,
    RevokePrivateAccessRequest, RevokePrivateAccessResponse, SubscribeEventsRequest,
};

// ── Artifact-class constants (proto i32 values) ───────────────────────────────

const PROTO_CLASS_PROTECTED_RAW_COMPANION: i32 = 3;
const PROTO_CLASS_PRIVATE_RAW_IGC: i32 = 4;

// ── NodeContext ───────────────────────────────────────────────────────────────

/// Shared, immutable context threaded through all RPC handlers.
pub struct NodeContext {
    pub node_id: String,
    pub node_secret_key: iroh::SecretKey,
    pub store: FlatFileStore,
    pub private_access_key_store: PrivateAccessKeyStore,
    pub seq_num_store: SeqNumStore,
}

// ── IgcNetService ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct IgcNetService {
    ctx: Arc<NodeContext>,
}

impl IgcNetService {
    pub fn new(ctx: Arc<NodeContext>) -> Self {
        Self { ctx }
    }
}

// ── RPC implementations ───────────────────────────────────────────────────────

#[tonic::async_trait]
impl IgcNet for IgcNetService {
    async fn get_node_status(
        &self,
        _request: Request<GetNodeStatusRequest>,
    ) -> Result<Response<GetNodeStatusResponse>, Status> {
        Ok(Response::new(GetNodeStatusResponse {
            protocol_version: "igc-net/v0.3".to_string(),
            api_version: "0".to_string(),
            node_id: self.ctx.node_id.clone(),
            ready: true,
            governance_sync_state: GovernanceSyncState::Ready as i32,
            latest_event_seq: 0,
            governance_baseline_ready: true,
        }))
    }

    async fn publish_flight(
        &self,
        _request: Request<PublishFlightRequest>,
    ) -> Result<Response<PublishFlightResponse>, Status> {
        Err(Status::unimplemented(
            "PublishFlight is handled by the igc-net-cli announce subcommand",
        ))
    }

    async fn fetch_artifact(
        &self,
        request: Request<FetchArtifactRequest>,
    ) -> Result<Response<FetchArtifactResponse>, Status> {
        let req = request.into_inner();

        // Validate raw_igc_hash format early.
        if req.raw_igc_hash.len() != 64
            || !req.raw_igc_hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(Status::invalid_argument("raw_igc_hash must be 64 lowercase hex chars"));
        }

        // For restricted artifact classes, verify the signed fetch proof.
        if let Some(rust_class) = restricted_artifact_class(req.artifact_class) {
            self.verify_restricted_fetch(&req, &rust_class)?;
            // Advance seq_num durably BEFORE handing bytes to caller (R-ACCESS-13).
            self.ctx
                .seq_num_store
                .advance(&req.requester_key, req.seq_num)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        // Retrieve the artifact bytes from the local blob store.
        // ProtectedRawCompanion requires a companion-index lookup not yet
        // implemented; return NOT_FOUND for now (post-POC hardening item).
        if req.artifact_class == PROTO_CLASS_PROTECTED_RAW_COMPANION {
            return Err(Status::unimplemented(
                "ProtectedRawCompanion fetch requires companion index (post-POC)",
            ));
        }

        let blob = self
            .ctx
            .store
            .get(&req.raw_igc_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("artifact not found in local store"))?;

        let artifact_hash = hex::encode(blake3::hash(&blob).as_bytes());

        Ok(Response::new(FetchArtifactResponse {
            artifact_bytes: blob,
            artifact_hash,
        }))
    }

    async fn query_index(
        &self,
        _request: Request<QueryIndexRequest>,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        Err(Status::unimplemented("QueryIndex is not yet implemented"))
    }

    type SubscribeEventsStream =
        Pin<Box<dyn Stream<Item = Result<crate::proto::IgcNetEvent, Status>> + Send + 'static>>;

    async fn subscribe_events(
        &self,
        _request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        Err(Status::unimplemented("SubscribeEvents is not yet implemented"))
    }

    async fn provision_private_access_key(
        &self,
        request: Request<ProvisionPrivateAccessKeyRequest>,
    ) -> Result<Response<ProvisionPrivateAccessKeyResponse>, Status> {
        let req = request.into_inner();

        if req.pilot_id.is_empty() {
            return Err(Status::invalid_argument("pilot_id is required"));
        }
        if req.private_access_secret_key.len() != 32 {
            return Err(Status::invalid_argument(
                "private_access_secret_key must be exactly 32 bytes (Ed25519 seed)",
            ));
        }
        if req.expected_private_access_public_key.len() != 64
            || !req
                .expected_private_access_public_key
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(Status::invalid_argument(
                "expected_private_access_public_key must be 64 lowercase hex chars",
            ));
        }

        // R-ACCESS-23: derive public key from supplied private key and verify it
        // matches the expected value.
        let seed: [u8; 32] = req.private_access_secret_key.as_slice().try_into().unwrap();
        let private_key = iroh::SecretKey::from_bytes(&seed);
        let derived_pubkey_hex = private_key.public().to_string();
        if derived_pubkey_hex != req.expected_private_access_public_key {
            return Err(Status::invalid_argument(
                "derived public key does not match expected_private_access_public_key (R-ACCESS-23)",
            ));
        }

        // R-ACCESS-24/25: validate against governance rotation record.
        // TODO: implement once a private-access-rotation-record governance store is wired.
        // For POC, skip governance check and proceed directly to storage.

        self.ctx
            .private_access_key_store
            .provision(&private_key, &self.ctx.node_secret_key)
            .map_err(|e| Status::internal(e.to_string()))?;

        tracing::info!(pilot_id = %req.pilot_id, "private access key provisioned");

        Ok(Response::new(ProvisionPrivateAccessKeyResponse {
            pilot_id: req.pilot_id,
            private_access_public_key: derived_pubkey_hex,
            full_governance_catchup_required: false,
            pilot_governance_sync_state: GovernanceSyncState::Ready as i32,
            restricted_serving_ready: true,
        }))
    }

    async fn revoke_private_access(
        &self,
        request: Request<RevokePrivateAccessRequest>,
    ) -> Result<Response<RevokePrivateAccessResponse>, Status> {
        let req = request.into_inner();

        if req.pilot_id.is_empty() {
            return Err(Status::invalid_argument("pilot_id is required"));
        }

        // Delete the private access key (R-ACCESS-16, R-ACCESS-17).
        self.ctx
            .private_access_key_store
            .delete()
            .map_err(|e| Status::internal(e.to_string()))?;

        tracing::info!(pilot_id = %req.pilot_id, "private access key revoked");

        Ok(Response::new(RevokePrivateAccessResponse {
            pilot_id: req.pilot_id,
            key_deleted: true,
            // Restricted plaintext deletion requires enumerating cached blobs —
            // not yet implemented. Callers should treat this as a compliance
            // obligation and purge their blob store separately (R-ACCESS-17).
            restricted_plaintext_deleted: false,
            tombstone_retained: false,
        }))
    }
}

// ── Restricted-fetch helper ───────────────────────────────────────────────────

impl IgcNetService {
    fn verify_restricted_fetch(
        &self,
        req: &FetchArtifactRequest,
        rust_class: &ArtifactClass,
    ) -> Result<(), Status> {
        // Load the pilot's authorized public key (derived from the stored private key).
        let private_key = self
            .ctx
            .private_access_key_store
            .load(&self.ctx.node_secret_key)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::unauthenticated(
                    "no private_access_keypair provisioned; call ProvisionPrivateAccessKey first",
                )
            })?;
        let authorized_public_key = private_key.public();

        // Proto `bytes signature` → hex string for FetchProof.
        let signature_hex = hex::encode(&req.signature);

        let proof = FetchProof {
            schema: "igc-net/fetch-request".to_string(),
            schema_version: 1,
            raw_igc_hash: req.raw_igc_hash.clone(),
            artifact_class: rust_class.clone(),
            requester_key: req.requester_key.clone(),
            seq_num: req.seq_num,
            signature: signature_hex,
        };

        let last_seen = self
            .ctx
            .seq_num_store
            .last_seen(&req.requester_key)
            .map_err(|e| Status::internal(e.to_string()))?;

        verify_fetch_proof(&proof, &authorized_public_key, rust_class, last_seen)
            .map_err(fetch_proof_error_to_status)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns the Rust `ArtifactClass` for restricted proto values, or `None` for
/// public/unspecified classes that need no authorization.
fn restricted_artifact_class(proto_value: i32) -> Option<ArtifactClass> {
    match proto_value {
        PROTO_CLASS_PROTECTED_RAW_COMPANION => Some(ArtifactClass::ProtectedRawCompanion),
        PROTO_CLASS_PRIVATE_RAW_IGC => Some(ArtifactClass::PrivateRawIgc),
        _ => None,
    }
}

fn fetch_proof_error_to_status(e: igc_net::FetchProofError) -> Status {
    use igc_net::FetchProofError::*;
    match e {
        RequesterKeyMismatch | ArtifactClassMismatch | SignatureVerification => {
            Status::unauthenticated(e.to_string())
        }
        SeqNumNotMonotonic { .. } | SeqNumZero => Status::unauthenticated(e.to_string()),
        InvalidHash | InvalidRequesterKey | InvalidSignatureEncoding => {
            Status::invalid_argument(e.to_string())
        }
        Json(_) => Status::internal(e.to_string()),
    }
}
