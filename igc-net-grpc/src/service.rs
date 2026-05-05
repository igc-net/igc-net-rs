//! `IgcNet` gRPC service implementation.
//!
//! POC scope: PublishFlight (public), FetchArtifact (restricted + public),
//! QueryIndex, ProvisionPrivateAccessKey, RevokePrivateAccess, GetNodeStatus.
//! Other RPCs return UNIMPLEMENTED.

use std::sync::Arc;

use std::pin::Pin;

use futures::{Stream, stream};
use tonic::{Request, Response, Status};

use igc_net::{
    ArtifactClass, ArtifactRegistryRecord, Blake3Hex, FetchProof, FlightGovernanceState,
    FlightGovernanceStatus, IgcIrohNode, OwnerClaimRecord, PilotId, PrivateAccessKeyStore,
    PrivateAccessRotationStateStatus, PublicationMode as StorePublicationMode,
    PublicationModeRecord, SeqNumStore, publish, publish_private, publish_protected,
    verify_fetch_proof,
};

use crate::proto::igc_net_server::IgcNet;
use crate::proto::{
    EventKind, FetchArtifactRequest, FetchArtifactResponse, GetNodeStatusRequest,
    GetNodeStatusResponse, GovernanceServingState, GovernanceSyncState, IgcNetEvent, IndexEntry,
    ProvisionPrivateAccessKeyRequest, ProvisionPrivateAccessKeyResponse,
    PublicationMode as ProtoPublicationMode, PublishFlightRequest, PublishFlightResponse,
    QueryIndexRequest, QueryIndexResponse, RevokePrivateAccessRequest, RevokePrivateAccessResponse,
    SubscribeEventsRequest,
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
        let store_ready = self.ctx.node.store().artifact_registry_records().is_ok();
        let governance_ready = self.ctx.node.governance_store().init().is_ok();
        let latest_event_seq = self
            .ctx
            .node
            .store()
            .latest_artifact_registry_event_seq()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GetNodeStatusResponse {
            protocol_version: "igc-net/v0.3".to_string(),
            api_version: "0".to_string(),
            node_id: self.ctx.node.node_id().to_string(),
            ready: store_ready && governance_ready,
            governance_sync_state: if governance_ready {
                GovernanceSyncState::Ready as i32
            } else {
                GovernanceSyncState::Failed as i32
            },
            latest_event_seq,
            governance_baseline_ready: governance_ready,
            blob_store_ready: store_ready,
            artifact_registry_ready: store_ready,
            event_cursor_ready: store_ready,
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
        let filename = if req.filename.is_empty() {
            None
        } else {
            Some(req.filename)
        };

        if mode == ProtoPublicationMode::Protected {
            let identity = self
                .ctx
                .node
                .load_or_generate_pilot_identity()
                .map_err(|e| Status::internal(e.to_string()))?;
            let result = publish_protected(&self.ctx.node, req.raw_igc)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let created_at = canonical_utc_now();
            let owner_claim = OwnerClaimRecord::issue(
                &identity.pilot_id_secret_key(),
                result.raw_igc_hash.clone(),
                created_at.clone(),
                Vec::new(),
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            let publication_mode = PublicationModeRecord::issue(
                &identity.pilot_id_secret_key(),
                result.raw_igc_hash.clone(),
                StorePublicationMode::Protected,
                Some(result.protected_hash.clone()),
                None,
                created_at,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .governance_store()
                .persist_owner_claim_record(&owner_claim)
                .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .governance_store()
                .persist_publication_mode_record(&publication_mode)
                .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .broadcast_governance_record(&owner_claim)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .broadcast_governance_record(&publication_mode)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            self.ctx
                .node
                .store()
                .append_artifact_registry_record(&ArtifactRegistryRecord {
                    raw_igc_hash: result.raw_igc_hash.clone(),
                    pilot_id: Some(identity.pilot_id()),
                    publication_mode: StorePublicationMode::Protected,
                    protected_hash: Some(result.protected_hash.clone()),
                    has_raw_igc: true,
                    has_protected_sanitized_igc: true,
                    has_protected_raw_companion: true,
                    serving_node_ids: vec![self.ctx.node.node_id().clone()],
                    recorded_at: canonical_utc_now(),
                })
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            return Ok(Response::new(PublishFlightResponse {
                raw_igc_hash: result.raw_igc_hash.to_string(),
                protected_hash: result.protected_hash.to_string(),
                tickets: vec![result.protected_ticket],
                companion_tickets: vec![result.raw_companion_ticket],
            }));
        }

        if mode == ProtoPublicationMode::Private {
            let identity = self
                .ctx
                .node
                .load_or_generate_pilot_identity()
                .map_err(|e| Status::internal(e.to_string()))?;
            self.require_publish_private_access_ready(&identity.pilot_id())?;
            let result = publish_private(&self.ctx.node, req.raw_igc)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let created_at = canonical_utc_now();
            let owner_claim = OwnerClaimRecord::issue(
                &identity.pilot_id_secret_key(),
                result.raw_igc_hash.clone(),
                created_at.clone(),
                Vec::new(),
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            let publication_mode = PublicationModeRecord::issue(
                &identity.pilot_id_secret_key(),
                result.raw_igc_hash.clone(),
                StorePublicationMode::Private,
                None,
                None,
                created_at,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .governance_store()
                .persist_owner_claim_record(&owner_claim)
                .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .governance_store()
                .persist_publication_mode_record(&publication_mode)
                .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .broadcast_governance_record(&owner_claim)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            self.ctx
                .node
                .broadcast_governance_record(&publication_mode)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            self.ctx
                .node
                .store()
                .append_artifact_registry_record(&ArtifactRegistryRecord {
                    raw_igc_hash: result.raw_igc_hash.clone(),
                    pilot_id: Some(identity.pilot_id()),
                    publication_mode: StorePublicationMode::Private,
                    protected_hash: None,
                    has_raw_igc: true,
                    has_protected_sanitized_igc: false,
                    has_protected_raw_companion: false,
                    serving_node_ids: vec![self.ctx.node.node_id().clone()],
                    recorded_at: canonical_utc_now(),
                })
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            return Ok(Response::new(PublishFlightResponse {
                raw_igc_hash: result.raw_igc_hash.to_string(),
                protected_hash: String::new(),
                tickets: vec![result.raw_igc_ticket],
                companion_tickets: Vec::new(),
            }));
        }

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

        let governance_state = self
            .ctx
            .node
            .governance_store()
            .resolve_flight_governance_state(&raw_igc_hash)
            .map_err(|e| Status::internal(e.to_string()))?;
        let publication_mode_record =
            self.authorized_publication_mode_record(&record, governance_state.as_ref())?;
        let effective = effective_artifact_state(&record, publication_mode_record.as_ref());

        let restricted_class = restricted_artifact_class(req.artifact_class);
        if governance_requires_restricted_plaintext_purge(governance_state.as_ref()) {
            self.purge_restricted_plaintext_for_record(&record, &effective)
                .await?;
        }
        let artifact_hash = artifact_hash_for_request(&record, &effective, req.artifact_class)?;
        self.enforce_flight_governance(
            &record,
            governance_state.as_ref(),
            restricted_class.is_some(),
        )?;

        // For restricted artifact classes, verify the signed fetch proof.
        if let Some(rust_class) = restricted_class {
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
            raw_igc_hash: raw_igc_hash.to_string(),
            artifact_class: req.artifact_class,
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
            .map(|record| self.index_entry_for_record(record))
            .collect::<Result<Vec<_>, _>>()?;

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
        request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let req = request.into_inner();
        let events = self
            .ctx
            .node
            .store()
            .artifact_registry_events_since(req.from_seq)
            .map_err(|e| Status::internal(e.to_string()))?;

        let response_events = events
            .into_iter()
            .map(|(seq, record)| {
                self.index_entry_for_record_with_seq(&record, seq)
                    .map(|entry| IgcNetEvent {
                        seq,
                        kind: EventKind::LocalPublish as i32,
                        entry: Some(entry),
                    })
            })
            .collect::<Vec<_>>();

        Ok(Response::new(Box::pin(stream::iter(response_events))))
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
        let purge = self.purge_restricted_plaintext_for_pilot(&pilot_id).await?;

        tracing::info!(pilot_id = %req.pilot_id, "private access key revoked");

        Ok(Response::new(RevokePrivateAccessResponse {
            pilot_id: req.pilot_id,
            key_deleted: true,
            restricted_plaintext_deleted: true,
            tombstone_retained: purge.tombstone_retained,
        }))
    }
}

// ── Restricted-fetch helper ───────────────────────────────────────────────────

impl IgcNetService {
    async fn purge_restricted_plaintext_for_pilot(
        &self,
        pilot_id: &PilotId,
    ) -> Result<RestrictedPlaintextPurge, Status> {
        let records = self
            .ctx
            .node
            .store()
            .artifact_registry_records()
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut result = RestrictedPlaintextPurge::default();

        for record in records
            .into_iter()
            .filter(|record| record.pilot_id.as_ref() == Some(pilot_id))
        {
            let governance_state = self
                .ctx
                .node
                .governance_store()
                .resolve_flight_governance_state(&record.raw_igc_hash)
                .map_err(|e| Status::internal(e.to_string()))?;
            let publication_mode_record =
                self.authorized_publication_mode_record(&record, governance_state.as_ref())?;
            let effective = effective_artifact_state(&record, publication_mode_record.as_ref());
            result.merge(
                self.purge_restricted_plaintext_for_record(&record, &effective)
                    .await?,
            );
        }

        Ok(result)
    }

    async fn purge_restricted_plaintext_for_record(
        &self,
        record: &ArtifactRegistryRecord,
        effective: &EffectiveArtifactState,
    ) -> Result<RestrictedPlaintextPurge, Status> {
        let mut tombstone = record.clone();
        let mut changed = false;
        let mut result = RestrictedPlaintextPurge::default();

        match effective.publication_mode {
            StorePublicationMode::Private => {
                if tombstone.has_raw_igc {
                    result.deleted_blob |= self
                        .ctx
                        .node
                        .store()
                        .delete_blob(&record.raw_igc_hash)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    tombstone.has_raw_igc = false;
                    tombstone
                        .serving_node_ids
                        .retain(|node_id| node_id != self.ctx.node.node_id());
                    changed = true;
                }
            }
            StorePublicationMode::Protected => {
                if tombstone.has_raw_igc {
                    result.deleted_blob |= self
                        .ctx
                        .node
                        .store()
                        .delete_blob(&record.raw_igc_hash)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    tombstone.has_raw_igc = false;
                    changed = true;
                }
                if tombstone.has_protected_raw_companion {
                    tombstone.has_protected_raw_companion = false;
                    changed = true;
                }
                if changed && !tombstone.has_protected_sanitized_igc {
                    tombstone
                        .serving_node_ids
                        .retain(|node_id| node_id != self.ctx.node.node_id());
                }
            }
            StorePublicationMode::Public => {}
        }

        if changed {
            tombstone.publication_mode = effective.publication_mode.clone();
            tombstone.protected_hash = effective.protected_hash.clone();
            tombstone.recorded_at = canonical_utc_now();
            self.ctx
                .node
                .store()
                .append_artifact_registry_record(&tombstone)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            result.tombstone_retained = true;
        }

        Ok(result)
    }

    fn index_entry_for_record(
        &self,
        record: &ArtifactRegistryRecord,
    ) -> Result<IndexEntry, Status> {
        let seq = self
            .ctx
            .node
            .store()
            .latest_artifact_registry_event_seq_for(&record.raw_igc_hash)
            .map_err(|e| Status::internal(e.to_string()))?
            .unwrap_or(0);
        self.index_entry_for_record_with_seq(record, seq)
    }

    fn index_entry_for_record_with_seq(
        &self,
        record: &ArtifactRegistryRecord,
        seq: u64,
    ) -> Result<IndexEntry, Status> {
        let governance_state = self
            .ctx
            .node
            .governance_store()
            .resolve_flight_governance_state(&record.raw_igc_hash)
            .map_err(|e| Status::internal(e.to_string()))?;
        let publication_mode_record =
            self.authorized_publication_mode_record(record, governance_state.as_ref())?;
        let effective = effective_artifact_state(record, publication_mode_record.as_ref());

        Ok(IndexEntry {
            raw_igc_hash: record.raw_igc_hash.to_string(),
            publication_mode: proto_publication_mode(&effective.publication_mode),
            protected_hash: effective
                .protected_hash
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            serving_node_ids: record
                .serving_node_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
            locally_available_artifact_classes: locally_available_artifact_classes(
                record, &effective,
            ),
            governance_serving_state: proto_governance_serving_state(governance_state.as_ref()),
            locally_fetchable: index_entry_locally_fetchable(
                record,
                &effective,
                governance_state.as_ref(),
            ),
            updated_event_seq: seq,
        })
    }

    fn enforce_flight_governance(
        &self,
        record: &ArtifactRegistryRecord,
        state: Option<&FlightGovernanceState>,
        restricted: bool,
    ) -> Result<(), Status> {
        let Some(state) = state else {
            if restricted {
                return Err(Status::failed_precondition(
                    "restricted artifact requires flight governance baseline",
                ));
            }
            return Ok(());
        };

        if state.serving_blocked() {
            return Err(Status::permission_denied(
                "flight governance state forbids serving this artifact",
            ));
        }

        if restricted {
            let pilot_id = record.pilot_id.as_ref().ok_or_else(|| {
                Status::failed_precondition(
                    "restricted artifact has no pilot owner in artifact registry",
                )
            })?;
            if !state.restricted_serving_ready_for(pilot_id) {
                return Err(Status::failed_precondition(
                    "flight governance state is not ready for restricted serving",
                ));
            }
        }

        Ok(())
    }

    fn authorized_publication_mode_record(
        &self,
        record: &ArtifactRegistryRecord,
        governance_state: Option<&FlightGovernanceState>,
    ) -> Result<Option<PublicationModeRecord>, Status> {
        let Some(mode_record) = self
            .ctx
            .node
            .governance_store()
            .resolve_publication_mode_record(&record.raw_igc_hash)
            .map_err(|e| Status::internal(e.to_string()))?
        else {
            return Ok(None);
        };

        let owner = governance_state
            .and_then(|state| {
                (state.baseline_ready && state.status == FlightGovernanceStatus::Approved)
                    .then_some(state.owner_pilot_id.as_ref())
                    .flatten()
            })
            .or(record.pilot_id.as_ref());

        Ok(owner
            .filter(|owner| *owner == &mode_record.pilot_id)
            .map(|_| mode_record))
    }

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

    fn require_publish_private_access_ready(&self, pilot_id: &PilotId) -> Result<(), Status> {
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
                    "private PublishFlight requires an active private-access rotation record",
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
        let active_public_key = active_record
            .private_access_public_key()
            .map_err(|e| Status::internal(e.to_string()))?;
        let private_key = self
            .ctx
            .private_access_key_store
            .load_for_pilot(pilot_id, &self.ctx.node_secret_key)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::failed_precondition(
                    "private PublishFlight requires a provisioned private-access key for the local pilot",
                )
            })?;
        if private_key.public() != active_public_key {
            return Err(Status::failed_precondition(
                "provisioned private-access key is older than the active rotation record",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RestrictedPlaintextPurge {
    deleted_blob: bool,
    tombstone_retained: bool,
}

impl RestrictedPlaintextPurge {
    fn merge(&mut self, other: Self) {
        self.deleted_blob |= other.deleted_blob;
        self.tombstone_retained |= other.tombstone_retained;
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
    effective: &EffectiveArtifactState,
    proto_value: i32,
) -> Result<Blake3Hex, Status> {
    match proto_value {
        PROTO_CLASS_PUBLIC_RAW_IGC => {
            if effective.publication_mode != StorePublicationMode::Public {
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
            if effective.publication_mode != StorePublicationMode::Protected {
                return Err(Status::permission_denied(
                    "protected_sanitized_igc is not allowed by the current publication_mode",
                ));
            }
            if !record.has_protected_sanitized_igc {
                return Err(Status::not_found(
                    "protected sanitized IGC is not available locally",
                ));
            }
            effective.protected_hash.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "protected artifact registry record is missing protected_hash",
                )
            })
        }
        PROTO_CLASS_PROTECTED_RAW_COMPANION => {
            if effective.publication_mode != StorePublicationMode::Protected {
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
            if effective.publication_mode != StorePublicationMode::Private {
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

struct EffectiveArtifactState {
    publication_mode: StorePublicationMode,
    protected_hash: Option<Blake3Hex>,
}

fn effective_artifact_state(
    record: &ArtifactRegistryRecord,
    publication_mode_record: Option<&PublicationModeRecord>,
) -> EffectiveArtifactState {
    match publication_mode_record {
        Some(record) => EffectiveArtifactState {
            publication_mode: record.publication_mode.clone(),
            protected_hash: record.protected_hash.clone(),
        },
        None => EffectiveArtifactState {
            publication_mode: record.publication_mode.clone(),
            protected_hash: record.protected_hash.clone(),
        },
    }
}

fn governance_requires_restricted_plaintext_purge(
    governance_state: Option<&FlightGovernanceState>,
) -> bool {
    governance_state.is_some_and(|state| {
        matches!(
            state.status,
            FlightGovernanceStatus::Deleted | FlightGovernanceStatus::Revoked
        )
    })
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

fn locally_available_artifact_classes(
    record: &ArtifactRegistryRecord,
    effective: &EffectiveArtifactState,
) -> Vec<i32> {
    let mut classes = Vec::new();
    if effective.publication_mode == StorePublicationMode::Public && record.has_raw_igc {
        classes.push(PROTO_CLASS_PUBLIC_RAW_IGC);
    }
    if effective.publication_mode == StorePublicationMode::Protected {
        if record.has_protected_sanitized_igc {
            classes.push(PROTO_CLASS_PROTECTED_SANITIZED_IGC);
        }
        if record.has_protected_raw_companion && record.has_raw_igc {
            classes.push(PROTO_CLASS_PROTECTED_RAW_COMPANION);
        }
    }
    if effective.publication_mode == StorePublicationMode::Private && record.has_raw_igc {
        classes.push(PROTO_CLASS_PRIVATE_RAW_IGC);
    }
    classes
}

fn proto_governance_serving_state(state: Option<&FlightGovernanceState>) -> i32 {
    match state {
        None => GovernanceServingState::Unknown as i32,
        Some(state) if !state.baseline_ready => GovernanceServingState::Stale as i32,
        Some(state) => match state.status {
            FlightGovernanceStatus::Pending => GovernanceServingState::Pending as i32,
            FlightGovernanceStatus::Approved => GovernanceServingState::Approved as i32,
            FlightGovernanceStatus::Contested => GovernanceServingState::Contested as i32,
            FlightGovernanceStatus::Rejected
            | FlightGovernanceStatus::Superseded
            | FlightGovernanceStatus::Revoked => GovernanceServingState::Rejected as i32,
            FlightGovernanceStatus::Deleted => GovernanceServingState::Deleted as i32,
        },
    }
}

fn index_entry_locally_fetchable(
    record: &ArtifactRegistryRecord,
    effective: &EffectiveArtifactState,
    governance_state: Option<&FlightGovernanceState>,
) -> bool {
    if locally_available_artifact_classes(record, effective).is_empty() {
        return false;
    }
    match governance_state {
        None => true,
        Some(state) => state.baseline_ready && !state.serving_blocked(),
    }
}

fn canonical_utc_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::OnceLock;

    use futures::StreamExt;

    use crate::proto::{
        ArtifactClass as ProtoArtifactClass, EventKind as ProtoEventKind, PublicationMode,
        QueryIndexRequest,
    };
    use igc_net::{
        ClaimApprovalRecord, DeletionRequestRecord, PrivateAccessRotationRecord,
        PublicationModeRecord,
    };

    fn secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    static TEST_NODE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

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

    fn persist_approved_flight_governance(
        ctx: &NodeContext,
        raw_igc_hash: Blake3Hex,
        owner_pilot_id: PilotId,
    ) {
        ctx.node
            .governance_store()
            .persist_flight_governance_state(&FlightGovernanceState::approved_owner(
                raw_igc_hash,
                owner_pilot_id,
                "2026-05-01T09:14:00Z",
            ))
            .unwrap();
    }

    async fn temp_service() -> (
        IgcNetService,
        Arc<NodeContext>,
        tempfile::TempDir,
        tokio::sync::MutexGuard<'static, ()>,
    ) {
        let guard = TEST_NODE_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
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
        (IgcNetService::new(ctx.clone()), ctx, dir, guard)
    }

    #[tokio::test]
    async fn publish_flight_public_populates_registry_and_query_index() {
        let (service, ctx, _dir, _guard) = temp_service().await;

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
        assert_eq!(query.entries[0].updated_event_seq, 0);
        assert_eq!(
            query.entries[0].locally_available_artifact_classes,
            vec![ProtoArtifactClass::PublicRawIgc as i32]
        );
        assert!(query.entries[0].locally_fetchable);

        let status = service
            .get_node_status(Request::new(GetNodeStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(status.ready);
        assert!(status.blob_store_ready);
        assert!(status.artifact_registry_ready);
        assert!(status.event_cursor_ready);
        assert_eq!(status.latest_event_seq, 0);

        let mut events = service
            .subscribe_events(Request::new(SubscribeEventsRequest { from_seq: 0 }))
            .await
            .unwrap()
            .into_inner();
        let first = events.next().await.unwrap().unwrap();
        assert_eq!(first.seq, 0);
        assert_eq!(first.kind, ProtoEventKind::LocalPublish as i32);
        assert_eq!(first.entry.unwrap().raw_igc_hash, response.raw_igc_hash);
        assert!(events.next().await.is_none());

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn publish_flight_rejects_private_mode_without_registry_mutation() {
        let (service, ctx, _dir, _guard) = temp_service().await;

        let err = service
            .publish_flight(Request::new(PublishFlightRequest {
                raw_igc: b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n".to_vec(),
                filename: "flight.igc".to_string(),
                publication_mode: PublicationMode::Private as i32,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
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
    async fn publish_flight_private_requires_and_uses_local_private_access_workflow() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let identity = ctx.node.load_or_generate_pilot_identity().unwrap();
        let private_access_key = secret_key(45);
        persist_private_access_rotation(&ctx, &identity.pilot_id_secret_key(), &private_access_key);
        ctx.private_access_key_store
            .provision_for_pilot(
                &identity.pilot_id(),
                &private_access_key,
                &ctx.node_secret_key,
            )
            .unwrap();
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let expected_raw_hash = Blake3Hex::from_hash(blake3::hash(raw_igc));

        let response = service
            .publish_flight(Request::new(PublishFlightRequest {
                raw_igc: raw_igc.to_vec(),
                filename: "private.igc".to_string(),
                publication_mode: PublicationMode::Private as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.raw_igc_hash, expected_raw_hash.to_string());
        assert!(response.protected_hash.is_empty());
        assert_eq!(response.tickets.len(), 1);
        assert!(response.companion_tickets.is_empty());

        let registry = ctx
            .node
            .store()
            .artifact_registry_record(&expected_raw_hash)
            .unwrap()
            .unwrap();
        assert_eq!(registry.pilot_id, Some(identity.pilot_id()));
        assert_eq!(registry.publication_mode, StorePublicationMode::Private);
        assert!(registry.has_raw_igc);
        assert!(!registry.has_protected_sanitized_igc);
        assert!(!registry.has_protected_raw_companion);

        let mode_record = ctx
            .node
            .governance_store()
            .resolve_publication_mode_record(&expected_raw_hash)
            .unwrap()
            .unwrap();
        assert_eq!(mode_record.publication_mode, StorePublicationMode::Private);
        assert_eq!(mode_record.pilot_id, identity.pilot_id());

        let query = service
            .query_index(Request::new(QueryIndexRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(query.entries.len(), 1);
        assert_eq!(
            query.entries[0].locally_available_artifact_classes,
            vec![ProtoArtifactClass::PrivateRawIgc as i32]
        );

        let proof = igc_net::sign_fetch_proof(
            expected_raw_hash.as_str(),
            ArtifactClass::PrivateRawIgc,
            1,
            &private_access_key,
        )
        .unwrap();
        let pending_governance_err = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: expected_raw_hash.to_string(),
                artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
                requester_key: proof.requester_key,
                seq_num: proof.seq_num,
                signature: hex::decode(proof.signature).unwrap(),
            }))
            .await
            .unwrap_err();
        assert_eq!(
            pending_governance_err.code(),
            tonic::Code::FailedPrecondition
        );

        let claim = ctx
            .node
            .governance_store()
            .load_owner_claim_records(&expected_raw_hash)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let resolver = secret_key(46);
        let approval = ClaimApprovalRecord::issue(
            &resolver,
            claim.record_id,
            expected_raw_hash.clone(),
            "2026-05-01T10:14:00Z",
        )
        .unwrap();
        ctx.node
            .governance_store()
            .trust_resolver(&approval.resolver_id)
            .unwrap();
        ctx.node
            .governance_store()
            .persist_claim_approval_record(&approval)
            .unwrap();
        let proof = igc_net::sign_fetch_proof(
            expected_raw_hash.as_str(),
            ArtifactClass::PrivateRawIgc,
            1,
            &private_access_key,
        )
        .unwrap();
        let fetched = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: expected_raw_hash.to_string(),
                artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
                requester_key: proof.requester_key,
                seq_num: proof.seq_num,
                signature: hex::decode(proof.signature).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched.artifact_bytes, raw_igc);

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn publish_flight_protected_populates_governance_registry_and_serves_sanitized() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let raw_igc =
            b"HFPLTPILOT:Alice\r\nHFCIDCOMPETITION:ABC\r\nB1300004730000N00837000EA0030003000\r\n";
        let sanitized =
            b"HFPLT:REDACTED\r\nHFCID:REDACTED\r\nB1300004730000N00837000EA0030003000\r\n";
        let expected_raw_hash = Blake3Hex::from_hash(blake3::hash(raw_igc));
        let expected_protected_hash = Blake3Hex::from_hash(blake3::hash(sanitized));

        let response = service
            .publish_flight(Request::new(PublishFlightRequest {
                raw_igc: raw_igc.to_vec(),
                filename: "protected.igc".to_string(),
                publication_mode: PublicationMode::Protected as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.raw_igc_hash, expected_raw_hash.to_string());
        assert_eq!(response.protected_hash, expected_protected_hash.to_string());
        assert_eq!(response.tickets.len(), 1);
        assert_eq!(response.companion_tickets.len(), 1);

        let registry = ctx
            .node
            .store()
            .artifact_registry_record(&expected_raw_hash)
            .unwrap()
            .unwrap();
        assert_eq!(registry.publication_mode, StorePublicationMode::Protected);
        assert_eq!(
            registry.protected_hash,
            Some(expected_protected_hash.clone())
        );
        assert!(registry.has_raw_igc);
        assert!(registry.has_protected_sanitized_igc);
        assert!(registry.has_protected_raw_companion);
        let owner = registry.pilot_id.clone().unwrap();

        let claims = ctx
            .node
            .governance_store()
            .load_owner_claim_records(&expected_raw_hash)
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].pilot_id, owner);

        let mode_record = ctx
            .node
            .governance_store()
            .resolve_publication_mode_record(&expected_raw_hash)
            .unwrap()
            .unwrap();
        assert_eq!(
            mode_record.publication_mode,
            StorePublicationMode::Protected
        );
        assert_eq!(
            mode_record.protected_hash,
            Some(expected_protected_hash.clone())
        );
        assert_eq!(mode_record.pilot_id, owner);

        let query = service
            .query_index(Request::new(QueryIndexRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(query.entries.len(), 1);
        assert_eq!(
            query.entries[0].publication_mode,
            PublicationMode::Protected as i32
        );
        assert_eq!(query.entries[0].protected_hash, response.protected_hash);
        assert_eq!(
            query.entries[0].locally_available_artifact_classes,
            vec![
                ProtoArtifactClass::ProtectedSanitizedIgc as i32,
                ProtoArtifactClass::ProtectedRawCompanion as i32
            ]
        );

        let fetched = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: expected_raw_hash.to_string(),
                artifact_class: ProtoArtifactClass::ProtectedSanitizedIgc as i32,
                requester_key: String::new(),
                seq_num: 0,
                signature: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched.artifact_bytes, sanitized);
        assert_eq!(fetched.artifact_hash, expected_protected_hash.to_string());

        let public_raw = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: expected_raw_hash.to_string(),
                artifact_class: ProtoArtifactClass::PublicRawIgc as i32,
                requester_key: String::new(),
                seq_num: 0,
                signature: Vec::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(public_raw.code(), tonic::Code::PermissionDenied);

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn fetch_artifact_enforces_publication_mode_before_serving() {
        let (service, ctx, _dir, _guard) = temp_service().await;
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
    async fn publication_mode_record_overrides_index_and_fetch_for_known_owner() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let pilot_root_key = secret_key(42);
        let pilot_id = PilotId::from_public_key(pilot_root_key.public());
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(pilot_id),
                publication_mode: StorePublicationMode::Public,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();
        let mode_record = PublicationModeRecord::issue(
            &pilot_root_key,
            raw_igc_hash.clone(),
            StorePublicationMode::Private,
            None,
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        ctx.node
            .governance_store()
            .persist_publication_mode_record(&mode_record)
            .unwrap();

        let query = service
            .query_index(Request::new(QueryIndexRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            query.entries[0].publication_mode,
            PublicationMode::Private as i32
        );
        assert_eq!(
            query.entries[0].locally_available_artifact_classes,
            vec![ProtoArtifactClass::PrivateRawIgc as i32]
        );

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
    async fn publication_mode_record_from_unknown_owner_is_ignored() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let owner_key = secret_key(43);
        let attacker_key = secret_key(44);
        let owner_id = PilotId::from_public_key(owner_key.public());
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(owner_id),
                publication_mode: StorePublicationMode::Public,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();
        let mode_record = PublicationModeRecord::issue(
            &attacker_key,
            raw_igc_hash.clone(),
            StorePublicationMode::Private,
            None,
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        ctx.node
            .governance_store()
            .persist_publication_mode_record(&mode_record)
            .unwrap();

        let query = service
            .query_index(Request::new(QueryIndexRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            query.entries[0].publication_mode,
            PublicationMode::Public as i32
        );

        let response = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::PublicRawIgc as i32,
                requester_key: String::new(),
                seq_num: 0,
                signature: Vec::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.artifact_bytes, raw_igc);

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn fetch_private_raw_uses_artifact_owner_private_access_key() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let owner_private_access_key = secret_key(51);
        let other_private_access_key = secret_key(52);
        let owner_pilot_root_key = secret_key(53);
        let owner_pilot_id = PilotId::from_public_key(owner_pilot_root_key.public());
        let other_pilot_id = PilotId::from_public_key(other_private_access_key.public());
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        persist_private_access_rotation(&ctx, &owner_pilot_root_key, &owner_private_access_key);
        persist_approved_flight_governance(&ctx, raw_igc_hash.clone(), owner_pilot_id.clone());
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
        let (service, ctx, _dir, _guard) = temp_service().await;
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
        let (service, ctx, _dir, _guard) = temp_service().await;
        let old_private_access_key = secret_key(71);
        let new_private_access_key = secret_key(72);
        let pilot_root_key = secret_key(73);
        let pilot_id = PilotId::from_public_key(pilot_root_key.public());
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        let old_record =
            persist_private_access_rotation(&ctx, &pilot_root_key, &old_private_access_key);
        persist_approved_flight_governance(&ctx, raw_igc_hash.clone(), pilot_id.clone());
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

    #[tokio::test]
    async fn fetch_private_raw_requires_flight_governance_baseline() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let private_access_key = secret_key(81);
        let pilot_root_key = secret_key(82);
        let pilot_id = PilotId::from_public_key(pilot_root_key.public());
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();

        persist_private_access_rotation(&ctx, &pilot_root_key, &private_access_key);
        ctx.private_access_key_store
            .provision_for_pilot(&pilot_id, &private_access_key, &ctx.node_secret_key)
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

        let proof = igc_net::sign_fetch_proof(
            raw_igc_hash.as_str(),
            ArtifactClass::PrivateRawIgc,
            1,
            &private_access_key,
        )
        .unwrap();
        let err = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
                requester_key: proof.requester_key,
                seq_num: proof.seq_num,
                signature: hex::decode(proof.signature).unwrap(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        ctx.node.close().await;
    }

    #[tokio::test]
    async fn fetch_artifact_refuses_blocked_flight_governance_state() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();
        let pilot_id = PilotId::from_public_key(secret_key(91).public());

        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(pilot_id.clone()),
                publication_mode: StorePublicationMode::Public,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();
        ctx.node
            .governance_store()
            .persist_flight_governance_state(&FlightGovernanceState {
                raw_igc_hash: raw_igc_hash.clone(),
                owner_pilot_id: Some(pilot_id),
                status: FlightGovernanceStatus::Contested,
                baseline_ready: true,
                recorded_at: "2026-05-01T09:14:00Z".to_string(),
            })
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
    async fn fetch_artifact_refuses_owner_deletion_request() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();
        let pilot_key = secret_key(92);
        let pilot_id = PilotId::from_public_key(pilot_key.public());

        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(pilot_id.clone()),
                publication_mode: StorePublicationMode::Public,
                protected_hash: None,
                has_raw_igc: true,
                has_protected_sanitized_igc: false,
                has_protected_raw_companion: false,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();
        persist_approved_flight_governance(&ctx, raw_igc_hash.clone(), pilot_id);
        let deletion =
            DeletionRequestRecord::issue(&pilot_key, raw_igc_hash.clone(), "2026-05-01T10:14:00Z")
                .unwrap();
        ctx.node
            .governance_store()
            .persist_deletion_request_record(&deletion)
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
    async fn deletion_request_purges_protected_raw_companion_but_keeps_sanitized_artifact() {
        let (service, ctx, _dir, _guard) = temp_service().await;
        let raw_igc =
            b"HFPLTPILOT:Alice\r\nHFCIDCOMPETITION:ABC\r\nB1300004730000N00837000EA0030003000\r\n";
        let sanitized = igc_net::sanitize_protected_igc(raw_igc);
        let raw_igc_hash = ctx.node.store().put(raw_igc).await.unwrap();
        let protected_hash = ctx.node.store().put(&sanitized).await.unwrap();
        let pilot_key = secret_key(93);
        let pilot_id = PilotId::from_public_key(pilot_key.public());

        ctx.node
            .store()
            .append_artifact_registry_record(&ArtifactRegistryRecord {
                raw_igc_hash: raw_igc_hash.clone(),
                pilot_id: Some(pilot_id.clone()),
                publication_mode: StorePublicationMode::Protected,
                protected_hash: Some(protected_hash.clone()),
                has_raw_igc: true,
                has_protected_sanitized_igc: true,
                has_protected_raw_companion: true,
                serving_node_ids: vec![ctx.node.node_id().clone()],
                recorded_at: canonical_utc_now(),
            })
            .await
            .unwrap();
        persist_approved_flight_governance(&ctx, raw_igc_hash.clone(), pilot_id);
        let deletion =
            DeletionRequestRecord::issue(&pilot_key, raw_igc_hash.clone(), "2026-05-01T10:14:00Z")
                .unwrap();
        ctx.node
            .governance_store()
            .persist_deletion_request_record(&deletion)
            .unwrap();

        let err = service
            .fetch_artifact(Request::new(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::ProtectedRawCompanion as i32,
                requester_key: String::new(),
                seq_num: 0,
                signature: Vec::new(),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(!ctx.node.store().contains(&raw_igc_hash).unwrap());
        assert!(ctx.node.store().contains(&protected_hash).unwrap());
        let tombstone = ctx
            .node
            .store()
            .artifact_registry_record(&raw_igc_hash)
            .unwrap()
            .unwrap();
        assert!(!tombstone.has_raw_igc);
        assert!(!tombstone.has_protected_raw_companion);
        assert!(tombstone.has_protected_sanitized_igc);
        assert_eq!(tombstone.protected_hash, Some(protected_hash));
        assert_eq!(tombstone.serving_node_ids, vec![ctx.node.node_id().clone()]);

        ctx.node.close().await;
    }
}
