//! `IgcNet` gRPC service implementation.
//!
//! POC scope: PublishFlight (public), FetchArtifact (restricted + public),
//! QueryIndex, ProvisionPrivateAccessKey, RevokePrivateAccess, GetNodeStatus.
//! Other RPCs return UNIMPLEMENTED.

use std::sync::Arc;

use std::pin::Pin;

use futures::Stream;
use tonic::{Request, Response, Status};

use igc_net::{
    ArtifactClass, ArtifactRegistryRecord, Blake3Hex, FetchProof, IgcIrohNode, PilotId,
    PrivateAccessKeyStore, PrivateAccessRotationRecord, PrivateAccessRotationStateStatus,
    PublicationMode as StorePublicationMode, SeqNumStore, publish, verify_fetch_proof,
};

use crate::proto::igc_net_server::IgcNet;
use crate::proto::{
    FetchArtifactRequest, FetchArtifactResponse, GetNodeStatusRequest, GetNodeStatusResponse,
    GovernanceSyncState, IndexEntry, ProvisionPrivateAccessKeyRequest,
    ProvisionPrivateAccessKeyResponse, PublicationMode as ProtoPublicationMode,
    PublishFlightRequest, PublishFlightResponse, QueryIndexRequest, QueryIndexResponse,
    RevokePrivateAccessRequest, RevokePrivateAccessResponse, SubscribeEventsRequest,
};

// ── Artifact-class constants (proto i32 values) ───────────────────────────────

const PROTO_CLASS_PUBLIC_RAW_IGC: i32 = 1;
const PROTO_CLASS_PROTECTED_SANITIZED_IGC: i32 = 2;
const PROTO_CLASS_PROTECTED_RAW_COMPANION: i32 = 3;
const PROTO_CLASS_PRIVATE_RAW_IGC: i32 = 4;

// ── NodeContext ───────────────────────────────────────────────────────────────

/// Shared, immutable context threaded through all RPC handlers.
pub struct NodeContext {
    pub node: IgcIrohNode,
    pub node_secret_key: iroh::SecretKey,
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
            node_id: self.ctx.node.node_id().to_string(),
            ready: true,
            governance_sync_state: GovernanceSyncState::Ready as i32,
            latest_event_seq: 0,
            governance_baseline_ready: true,
        }))
    }

    async fn publish_flight(
        &self,
        request: Request<PublishFlightRequest>,
    ) -> Result<Response<PublishFlightResponse>, Status> {
        let req = request.into_inner();
        if req.raw_igc.is_empty() {
            return Err(Status::invalid_argument("raw_igc is required"));
        }

        let mode = ProtoPublicationMode::try_from(req.publication_mode)
            .map_err(|_| Status::invalid_argument("publication_mode is invalid"))?;
        if mode == ProtoPublicationMode::Unspecified {
            return Err(Status::invalid_argument("publication_mode is required"));
        }
        if mode != ProtoPublicationMode::Public {
            return Err(Status::unimplemented(
                "PublishFlight currently supports public mode only; protected/private require mode-aware announcements and sanitization",
            ));
        }

        let filename = if req.filename.is_empty() {
            None
        } else {
            Some(req.filename)
        };
        let result = publish(&self.ctx.node, req.raw_igc, filename.as_deref())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.ctx
            .node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: result.igc_hash.clone(),
                pilot_id: None,
                publication_mode: StorePublicationMode::Public,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![self.ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PublishFlightResponse {
            raw_igc_hash: result.igc_hash.to_string(),
            protected_hash: String::new(),
            tickets: vec![result.igc_ticket],
            companion_tickets: Vec::new(),
        }))
    }

    async fn fetch_artifact(
        &self,
        request: Request<FetchArtifactRequest>,
    ) -> Result<Response<FetchArtifactResponse>, Status> {
        let req = request.into_inner();

        let raw_igc_hash = Blake3Hex::parse(req.raw_igc_hash.clone())
            .map_err(|_| Status::invalid_argument("raw_igc_hash must be 64 lowercase hex chars"))?;

        let record = self
            .ctx
            .node
            .store()
            .artifact_registry_record(&raw_igc_hash)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("artifact is not present in the artifact registry"))?;

        let artifact_hash = artifact_hash_for_request(&record, req.artifact_class)?;

        // For restricted artifact classes, verify the signed fetch proof.
        if let Some(rust_class) = restricted_artifact_class(req.artifact_class) {
            self.verify_restricted_fetch(&req, &record, &rust_class)?;
            // Advance seq_num durably BEFORE handing bytes to caller (R-ACCESS-13).
            self.ctx
                .seq_num_store
                .advance(&req.requester_key, req.seq_num)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        // Retrieve the artifact bytes from the local blob store.
        let blob = self
            .ctx
            .node
            .store()
            .get(&artifact_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("artifact not found in local store"))?;

        let actual_artifact_hash = Blake3Hex::from_hash(blake3::hash(&blob));
        if actual_artifact_hash != artifact_hash {
            return Err(Status::internal(
                "artifact bytes do not match registry hash",
            ));
        }

        Ok(Response::new(FetchArtifactResponse {
            artifact_bytes: blob,
            artifact_hash: artifact_hash.to_string(),
        }))
    }

    async fn query_index(
        &self,
        request: Request<QueryIndexRequest>,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        let req = request.into_inner();
        let page_size = match req.page_size {
            0 => 100,
            value => value.min(500),
        } as usize;
        let offset = if req.page_token.is_empty() {
            0
        } else {
            req.page_token
                .parse::<usize>()
                .map_err(|_| Status::invalid_argument("page_token must be an integer offset"))?
        };

        let records = self
            .ctx
            .node
            .store()
            .artifact_registry_records()
            .map_err(|e| Status::internal(e.to_string()))?;

        let entries = records
            .iter()
            .skip(offset)
            .take(page_size)
            .map(|record| IndexEntry {
                raw_igc_hash: record.raw_igc_hash.to_string(),
                publication_mode: proto_publication_mode(&record.publication_mode),
                protected_hash: record
                    .protected_hash
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                serving_node_ids: record
                    .serving_node_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            })
            .collect::<Vec<_>>();

        let next_offset = offset.saturating_add(entries.len());
        let next_page_token = if next_offset < records.len() {
            next_offset.to_string()
        } else {
            String::new()
        };

        Ok(Response::new(QueryIndexResponse {
            entries,
            next_page_token,
        }))
    }

    type SubscribeEventsStream =
        Pin<Box<dyn Stream<Item = Result<crate::proto::IgcNetEvent, Status>> + Send + 'static>>;

    async fn subscribe_events(
        &self,
        _request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        Err(Status::unimplemented(
            "SubscribeEvents is not yet implemented",
        ))
    }

    async fn provision_private_access_key(
        &self,
        request: Request<ProvisionPrivateAccessKeyRequest>,
    ) -> Result<Response<ProvisionPrivateAccessKeyResponse>, Status> {
        let req = request.into_inner();

        if req.pilot_id.is_empty() {
            return Err(Status::invalid_argument("pilot_id is required"));
        }
        let pilot_id = PilotId::parse(req.pilot_id.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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

        let private_access_governance = self
            .ctx
            .node
            .governance_store()
            .resolve_private_access_rotation_state(&pilot_id)
            .map_err(|e| Status::internal(e.to_string()))?;
        match private_access_governance.status() {
            PrivateAccessRotationStateStatus::Authoritative => {}
            PrivateAccessRotationStateStatus::Absent => {
                return Err(Status::failed_precondition(
                    "no active private-access rotation record is known for pilot",
                ));
            }
            PrivateAccessRotationStateStatus::Tentative => {
                return Err(Status::failed_precondition(
                    "private-access rotation governance is incomplete; catch-up required",
                ));
            }
        }
        let active_record = private_access_governance
            .authoritative
            .as_ref()
            .expect("authoritative private-access state has record");
        if active_record.private_access_public_key != derived_pubkey_hex {
            return Err(Status::failed_precondition(
                "supplied private-access key does not match current active rotation record",
            ));
        }

        self.ctx
            .private_access_key_store
            .provision_for_pilot(&pilot_id, &private_key, &self.ctx.node_secret_key)
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
        let pilot_id = PilotId::parse(req.pilot_id.clone())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Delete the private access key (R-ACCESS-16, R-ACCESS-17).
        self.ctx
            .private_access_key_store
            .delete_for_pilot(&pilot_id)
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
        record: &ArtifactRegistryRecord,
        rust_class: &ArtifactClass,
    ) -> Result<(), Status> {
        let pilot_id = record.pilot_id.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "restricted artifact has no pilot owner in artifact registry",
            )
        })?;

        let private_key = self
            .ctx
            .private_access_key_store
            .load_for_pilot(pilot_id, &self.ctx.node_secret_key)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::unauthenticated(
                    "no private_access_keypair provisioned for artifact owner; call ProvisionPrivateAccessKey first",
                )
            })?;
        let private_access_governance = self
            .ctx
            .node
            .governance_store()
            .resolve_private_access_rotation_state(pilot_id)
            .map_err(|e| Status::internal(e.to_string()))?;
        match private_access_governance.status() {
            PrivateAccessRotationStateStatus::Authoritative => {}
            PrivateAccessRotationStateStatus::Absent => {
                return Err(Status::failed_precondition(
                    "no active private-access rotation record is known for artifact owner",
                ));
            }
            PrivateAccessRotationStateStatus::Tentative => {
                return Err(Status::failed_precondition(
                    "private-access rotation governance is incomplete; catch-up required",
                ));
            }
        }
        let active_record = private_access_governance
            .authoritative
            .as_ref()
            .expect("authoritative private-access state has record");
        if active_record.private_access_public_key != req.requester_key {
            return Err(Status::unauthenticated(
                "requester_key does not match current private-access rotation record",
            ));
        }
        let authorized_public_key = active_record
            .private_access_public_key()
            .map_err(|e| Status::internal(e.to_string()))?;
        if private_key.public() != authorized_public_key {
            return Err(Status::failed_precondition(
                "stored private-access key is older than the active rotation record",
            ));
        }

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

fn artifact_hash_for_request(
    record: &ArtifactRegistryRecord,
    proto_value: i32,
) -> Result<Blake3Hex, Status> {
    match proto_value {
        PROTO_CLASS_PUBLIC_RAW_IGC => {
            if record.publication_mode != StorePublicationMode::Public {
                return Err(Status::permission_denied(
                    "public_raw_igc is not allowed by the current publication_mode",
                ));
            }
            if !record.has_raw_igc {
                return Err(Status::not_found("raw IGC is not available locally"));
            }
            Ok(record.raw_igc_hash.clone())
        }
        PROTO_CLASS_PROTECTED_SANITIZED_IGC => {
            if record.publication_mode != StorePublicationMode::Protected {
                return Err(Status::permission_denied(
                    "protected_sanitized_igc is not allowed by the current publication_mode",
                ));
            }
            if !record.has_protected_sanitized_igc {
                return Err(Status::not_found(
                    "protected sanitized IGC is not available locally",
                ));
            }
            record.protected_hash.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "protected artifact registry record is missing protected_hash",
                )
            })
        }
        PROTO_CLASS_PROTECTED_RAW_COMPANION => {
            if record.publication_mode != StorePublicationMode::Protected {
                return Err(Status::permission_denied(
                    "protected_raw_companion is not allowed by the current publication_mode",
                ));
            }
            if !record.has_protected_raw_companion || !record.has_raw_igc {
                return Err(Status::not_found(
                    "protected raw companion is not available locally",
                ));
            }
            Ok(record.raw_igc_hash.clone())
        }
        PROTO_CLASS_PRIVATE_RAW_IGC => {
            if record.publication_mode != StorePublicationMode::Private {
                return Err(Status::permission_denied(
                    "private_raw_igc is not allowed by the current publication_mode",
                ));
            }
            if !record.has_raw_igc {
                return Err(Status::not_found(
                    "private raw IGC is not available locally",
                ));
            }
            Ok(record.raw_igc_hash.clone())
        }
        _ => Err(Status::invalid_argument("artifact_class is invalid")),
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

fn proto_publication_mode(mode: &StorePublicationMode) -> i32 {
    match mode {
        StorePublicationMode::Public => crate::proto::PublicationMode::Public as i32,
        StorePublicationMode::Protected => crate::proto::PublicationMode::Protected as i32,
        StorePublicationMode::Private => crate::proto::PublicationMode::Private as i32,
    }
}

fn canonical_utc_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::proto::{ArtifactClass as ProtoArtifactClass, PublicationMode, QueryIndexRequest};

    fn secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    fn persist_private_access_rotation(
        ctx: &NodeContext,
        pilot_root_key: &iroh::SecretKey,
        private_access_key: &iroh::SecretKey,
    ) -> PrivateAccessRotationRecord {
        let record = PrivateAccessRotationRecord::issue(
            pilot_root_key,
            private_access_key.public(),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        ctx.node
            .governance_store()
            .persist_private_access_rotation_record(&record)
            .unwrap();
        record
    }

    async fn temp_service() -> (IgcNetService, Arc<NodeContext>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let node = IgcIrohNode::start(dir.path()).await.unwrap();
        let node_secret_key =
            iroh::SecretKey::from_bytes(&node.store().load_key_bytes().unwrap().unwrap());
        let ctx = Arc::new(NodeContext {
            node,
            node_secret_key,
            private_access_key_store: PrivateAccessKeyStore::for_data_dir(dir.path()),
            seq_num_store: SeqNumStore::for_data_dir(dir.path()),
        });
        (IgcNetService::new(ctx.clone()), ctx, dir)
    }

    #[tokio::test]
    async fn publish_flight_public_populates_registry_and_query_index() {
        let (service, ctx, _dir) = temp_service().await;

        let response = service
            .publish_flight(Request::new(PublishFlightRequest {
                raw_igc: b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n".to_vec(),
                filename: "flight.igc".to_string(),
                publication_mode: PublicationMode::Public as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.raw_igc_hash.len(), 64);
        assert!(response.protected_hash.is_empty());
        assert_eq!(response.tickets.len(), 1);
        assert!(response.companion_tickets.is_empty());

        let records = ctx.node.store().artifact_registry_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].raw_igc_hash.to_string(), response.raw_igc_hash);
        assert_eq!(records[0].publication_mode, StorePublicationMode::Public);
        assert!(records[0].has_raw_igc);

        let query = service
            .query_index(Request::new(QueryIndexRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(query.entries.len(), 1);
        assert_eq!(query.entries[0].raw_igc_hash, response.raw_igc_hash);
        assert_eq!(
            query.entries[0].publication_mode,
            PublicationMode::Public as i32
        );

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn publish_flight_rejects_non_public_modes_without_registry_mutation() {
        let (service, ctx, _dir) = temp_service().await;

        let err = service
            .publish_flight(Request::new(PublishFlightRequest {
                raw_igc: b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n".to_vec(),
                filename: "flight.igc".to_string(),
                publication_mode: PublicationMode::Private as i32,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(
            ctx.node
                .store()
                .artifact_registry_records()
                .unwrap()
                .is_empty()
        );

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn fetch_artifact_enforces_publication_mode_before_serving() {
        let (service, ctx, _dir) = temp_service().await;
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(PilotId::from_public_key(secret_key(41).public())),
                publication_mode: StorePublicationMode::Private,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();

        let err = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::PublicRawIgc as i32,
                requester_key: String::new(),
                seq_num: 0,
                signature: Vec::new(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn fetch_private_raw_uses_artifact_owner_private_access_key() {
        let (service, ctx, _dir) = temp_service().await;
        let owner_private_access_key = secret_key(51);
        let other_private_access_key = secret_key(52);
        let owner_pilot_root_key = secret_key(53);
        let owner_pilot_id = PilotId::from_public_key(owner_pilot_root_key.public());
        let other_pilot_id = PilotId::from_public_key(other_private_access_key.public());
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        persist_private_access_rotation(&ctx, &owner_pilot_root_key, &owner_private_access_key);
        ctx.private_access_key_store
            .provision_for_pilot(
                &owner_pilot_id,
                &owner_private_access_key,
                &ctx.node_secret_key,
            )
            .unwrap();
        ctx.private_access_key_store
            .provision_for_pilot(
                &other_pilot_id,
                &other_private_access_key,
                &ctx.node_secret_key,
            )
            .unwrap();
        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(owner_pilot_id.clone()),
                publication_mode: StorePublicationMode::Private,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();

        let other_proof = igc_net::sign_fetch_proof(
            raw_igc_hash.as_str(),
            ArtifactClass::PrivateRawIgc,
            1,
            &other_private_access_key,
        )
        .unwrap();
        let other_err = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
                requester_key: other_proof.requester_key,
                seq_num: other_proof.seq_num,
                signature: hex::decode(other_proof.signature).unwrap(),
            }))
            .await
            .unwrap_err();
        assert_eq!(other_err.code(), tonic::Code::Unauthenticated);

        let owner_proof = igc_net::sign_fetch_proof(
            raw_igc_hash.as_str(),
            ArtifactClass::PrivateRawIgc,
            1,
            &owner_private_access_key,
        )
        .unwrap();
        let response = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
                requester_key: owner_proof.requester_key,
                seq_num: owner_proof.seq_num,
                signature: hex::decode(owner_proof.signature).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.artifact_bytes, raw_igc);
        assert_eq!(response.artifact_hash, raw_igc_hash.to_string());

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn provision_private_access_key_requires_active_rotation_record() {
        let (service, ctx, _dir) = temp_service().await;
        let pilot_root_key = secret_key(61);
        let private_access_key = secret_key(62);
        let pilot_id = PilotId::from_public_key(pilot_root_key.public());

        let err = service
            .provision_private_access_key(Request::new(ProvisionPrivateAccessKeyRequest {
                pilot_id: pilot_id.to_string(),
                private_access_secret_key: private_access_key.to_bytes().to_vec(),
                expected_private_access_public_key: private_access_key.public().to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            ctx.private_access_key_store
                .load_for_pilot(&pilot_id, &ctx.node_secret_key)
                .unwrap()
                .is_none()
        );

        persist_private_access_rotation(&ctx, &pilot_root_key, &private_access_key);
        let response = service
            .provision_private_access_key(Request::new(ProvisionPrivateAccessKeyRequest {
                pilot_id: pilot_id.to_string(),
                private_access_secret_key: private_access_key.to_bytes().to_vec(),
                expected_private_access_public_key: private_access_key.public().to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.pilot_id, pilot_id.to_string());
        assert_eq!(
            response.private_access_public_key,
            private_access_key.public().to_string()
        );
        assert!(
            ctx.private_access_key_store
                .load_for_pilot(&pilot_id, &ctx.node_secret_key)
                .unwrap()
                .is_some()
        );

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn fetch_private_raw_rejects_rotated_private_access_key() {
        let (service, ctx, _dir) = temp_service().await;
        let old_private_access_key = secret_key(71);
        let new_private_access_key = secret_key(72);
        let pilot_root_key = secret_key(73);
        let pilot_id = PilotId::from_public_key(pilot_root_key.public());
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        let old_record =
            persist_private_access_rotation(&ctx, &pilot_root_key, &old_private_access_key);
        let new_record = PrivateAccessRotationRecord::issue(
            &pilot_root_key,
            new_private_access_key.public(),
            Some(old_record.record_id),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        ctx.node
            .governance_store()
            .persist_private_access_rotation_record(&new_record)
            .unwrap();
        ctx.private_access_key_store
            .provision_for_pilot(&pilot_id, &old_private_access_key, &ctx.node_secret_key)
            .unwrap();
        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(pilot_id),
                publication_mode: StorePublicationMode::Private,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();

        let old_proof = igc_net::sign_fetch_proof(
            raw_igc_hash.as_str(),
            ArtifactClass::PrivateRawIgc,
            1,
            &old_private_access_key,
        )
        .unwrap();
        let err = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
                requester_key: old_proof.requester_key,
                seq_num: old_proof.seq_num,
                signature: hex::decode(old_proof.signature).unwrap(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        ctx.node.close().await;
    }
}
