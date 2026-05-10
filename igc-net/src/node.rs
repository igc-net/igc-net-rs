//! igc-net network node.
//!
//! `IgcIrohNode` manages the iroh endpoint, iroh-blobs store, gossip,
//! and the local flat-file store.  It is the runtime handle passed to
//! all publish and indexer operations.

use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use iroh::Endpoint;
use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh_blobs::store::fs::FsStore;
use iroh_gossip::api::{Event as GossipEvent, GossipSender};
use iroh_gossip::net::{GOSSIP_ALPN, Gossip};
use iroh_gossip::proto::TopicId;

use crate::governance::{
    GovernanceRecord, GovernanceStore, GovernanceStoreError, PilotAuthDidGossipAnnouncement,
    PilotAuthDidState, PilotAuthDidSyncRequest, PilotAuthDidSyncResponse,
    PilotAuthDidWorkflowError, issue_initial_pilot_auth_did_record, rotate_pilot_auth_did_record,
};
use crate::id::NodeIdHex;
use crate::id::PilotId;
use crate::keys::{
    MultiPilotKeyStore, PilotCredentialStore, PilotIdentity, PilotKeyStore, PilotKeyStoreError,
    PilotProfile, PilotPublicIdentityWithProfile,
};
use crate::store::{FlatFileStore, StoreError};
use crate::topic::{announce_topic_id, governance_topic_id, pilot_auth_did_governance_topic_id};

const GOVERNANCE_SYNC_ALPN: &[u8] = b"igc-net/governance-sync/v1";
const GOVERNANCE_SYNC_MAX_REQUEST_BYTES: usize = 64 * 1024;
const GOVERNANCE_SYNC_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("pilot keys: {0}")]
    PilotKeys(#[from] PilotKeyStoreError),
    #[error("governance: {0}")]
    Governance(#[from] GovernanceStoreError),
    #[error("pilot-auth-did workflow: {0}")]
    PilotAuthDidWorkflow(#[from] PilotAuthDidWorkflowError),
    #[error("pilot-auth-did sync: {0}")]
    PilotAuthDidSync(#[from] crate::governance::PilotAuthDidSyncError),
    #[error("pilot-auth-did rotation governance persist failed after key replacement: {0}")]
    PilotAuthDidRotationPersistFailed(GovernanceStoreError),
    #[error(
        "pilot-auth-did rotation governance persist failed after key replacement ({persist}); rollback also failed ({rollback})"
    )]
    PilotAuthDidRotationPersistRollback {
        persist: GovernanceStoreError,
        rollback: PilotKeyStoreError,
    },
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to bind iroh endpoint: {0}")]
    EndpointBind(String),
    #[error("failed to load iroh blob store: {0}")]
    BlobStoreLoad(String),
    #[error("failed to subscribe to announce topic: {0}")]
    GossipSubscribe(String),
    #[error("failed to subscribe to pilot-auth-did governance topic: {0}")]
    GovernanceGossipSubscribe(String),
    #[error("failed to join pilot-auth-did governance topic peers: {0}")]
    GovernanceGossipJoin(String),
    #[error("failed to broadcast pilot-auth-did governance update: {0}")]
    GovernanceGossipBroadcast(String),
    #[error("pilot-auth-did network sync transport failed: {0}")]
    GovernanceSyncTransport(String),
    #[error("pilot-auth-did network sync JSON: {0}")]
    GovernanceSyncJson(#[from] serde_json::Error),
    #[error("no IPv4 loopback socket is bound for this node")]
    NoLoopbackSocket,
}

// ── IgcIrohNode ───────────────────────────────────────────────────────────────

/// Runtime handle for an igc-net node.
///
/// Holds the iroh endpoint, iroh-blobs filesystem store, gossip handler,
/// and the local flat-file store.
pub struct IgcIrohNode {
    pub(crate) endpoint: Endpoint,
    pub(crate) fs_store: FsStore,
    pub(crate) gossip: Gossip,
    pub(crate) store: Arc<FlatFileStore>,
    memory_lookup: MemoryLookup,
    /// Holds the protocol router alive.  `Router` is `#[must_use]` — dropping
    /// it aborts the accept loop for all registered ALPNs.
    _router: Router,
    /// Persistent announce-topic subscription.
    ///
    /// iroh-gossip only tracks HyParView state for a topic once a local
    /// subscriber exists.  Without this subscription, incoming JOIN messages
    /// from remote peers are silently discarded because the per-topic state
    /// map entry is absent.  Keeping the sender alive ensures the topic state
    /// exists from node start-up onwards, so remote peers can join the swarm
    /// before the first `publish()` call.
    ///
    /// Also used by `publish()` to broadcast announcements without creating a
    /// new subscription per call.
    announce_sender: GossipSender,
    /// Persistent pilot-auth-did governance topic sender.
    ///
    /// Local issuance/rotation broadcasts lightweight governance update
    /// announcements on this topic. Receivers then use the pull-sync transport
    /// to fetch any missing records from the delivering peer.
    governance_sender: GossipSender,
    /// Persistent normative governance topic sender.
    governance_record_sender: GossipSender,
    node_id: NodeIdHex,
    node_key_bytes: [u8; 32],
    multi_pilot_keys: MultiPilotKeyStore,
    pilot_credentials: PilotCredentialStore,
    governance: GovernanceStore,
}

#[derive(Debug, Clone)]
struct GovernanceSyncProtocol {
    governance: GovernanceStore,
}

impl ProtocolHandler for GovernanceSyncProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let request_bytes = recv
            .read_to_end(GOVERNANCE_SYNC_MAX_REQUEST_BYTES)
            .await
            .map_err(AcceptError::from_err)?;
        let request: PilotAuthDidSyncRequest =
            serde_json::from_slice(&request_bytes).map_err(AcceptError::from_err)?;
        let response = self
            .governance
            .prepare_pilot_auth_did_sync(&request)
            .map_err(AcceptError::from_err)?;
        let response_bytes = serde_json::to_vec(&response).map_err(AcceptError::from_err)?;
        send.write_all(&response_bytes)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

async fn request_pilot_auth_did_sync_from_peer_via(
    endpoint: &Endpoint,
    peer: iroh::PublicKey,
    request: &PilotAuthDidSyncRequest,
) -> Result<PilotAuthDidSyncResponse, NodeError> {
    let conn = endpoint
        .connect(peer, GOVERNANCE_SYNC_ALPN)
        .await
        .map_err(|err| NodeError::GovernanceSyncTransport(err.to_string()))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|err| NodeError::GovernanceSyncTransport(err.to_string()))?;
    let request_bytes = serde_json::to_vec(request)?;
    send.write_all(&request_bytes)
        .await
        .map_err(|err| NodeError::GovernanceSyncTransport(err.to_string()))?;
    send.finish()
        .map_err(|err| NodeError::GovernanceSyncTransport(err.to_string()))?;
    let response_bytes = recv
        .read_to_end(GOVERNANCE_SYNC_MAX_RESPONSE_BYTES)
        .await
        .map_err(|err| NodeError::GovernanceSyncTransport(err.to_string()))?;
    let response: PilotAuthDidSyncResponse = serde_json::from_slice(&response_bytes)?;
    response.validate()?;
    conn.close(0u32.into(), b"pilot-auth-did-sync-complete");
    Ok(response)
}

async fn broadcast_pilot_auth_did_gossip_announcement(
    sender: &GossipSender,
    announcement: &PilotAuthDidGossipAnnouncement,
) -> Result<(), NodeError> {
    let payload = serde_json::to_vec(announcement)?;
    sender
        .broadcast(payload.into())
        .await
        .map_err(|err| NodeError::GovernanceGossipBroadcast(err.to_string()))
}

async fn broadcast_governance_record<T: serde::Serialize>(
    sender: &GossipSender,
    record: &T,
) -> Result<(), NodeError> {
    let payload = serde_json::to_vec(record)?;
    sender
        .broadcast(payload.into())
        .await
        .map_err(|err| NodeError::GovernanceGossipBroadcast(err.to_string()))
}

async fn sync_pilot_auth_did_from_gossip_announcement(
    endpoint: &Endpoint,
    governance: &GovernanceStore,
    governance_sender: &GossipSender,
    delivered_from: iroh::PublicKey,
    announcement: &PilotAuthDidGossipAnnouncement,
) -> Result<usize, NodeError> {
    let request = governance.build_pilot_auth_did_sync_request(&announcement.pilot_id)?;
    if request.knows(&announcement.record_id) {
        return Ok(0);
    }

    let response =
        request_pilot_auth_did_sync_from_peer_via(endpoint, delivered_from, &request).await?;
    let applied = governance.apply_pilot_auth_did_sync(&response)?;

    if applied == 0 {
        return Ok(0);
    }

    for record in response
        .records
        .iter()
        .filter(|record| !request.knows(&record.record_id))
    {
        let announcement = PilotAuthDidGossipAnnouncement::from_record(record);
        broadcast_pilot_auth_did_gossip_announcement(governance_sender, &announcement).await?;
    }

    Ok(applied)
}

impl IgcIrohNode {
    /// Build and start a node rooted at `data_dir`.
    ///
    /// - Loads or generates the Ed25519 key from `data_dir/node.key`.
    /// - Initializes the separate `pilot-keys/` directory for pilot identity
    ///   custody; pilot keys remain distinct from the node transport key.
    /// - Opens `FlatFileStore` at `data_dir`.
    /// - Binds an iroh `Endpoint`, starts `iroh-blobs` and `iroh-gossip`.
    /// - Subscribes to the announce gossip topic so remote peers can join
    ///   the swarm immediately (HyParView state must exist for this to work).
    pub async fn start(data_dir: impl Into<PathBuf>) -> Result<Self, NodeError> {
        let data_dir = data_dir.into();

        // ── Flat-file store ───────────────────────────────────────────────────
        let store = Arc::new(FlatFileStore::open(data_dir.clone()));
        store.init().await?;

        // ── Ed25519 key ───────────────────────────────────────────────────────
        let key_bytes = match store.load_key_bytes()? {
            Some(b) => b,
            None => {
                let mut rng = rand::rng();
                let secret_key = iroh::SecretKey::generate(&mut rng);
                let bytes = secret_key.to_bytes();
                store.save_key_bytes(&bytes)?;
                bytes
            }
        };
        let secret_key = iroh::SecretKey::from_bytes(&key_bytes);
        let multi_pilot_keys = MultiPilotKeyStore::for_data_dir(&data_dir);
        multi_pilot_keys.init()?;
        let pilot_credentials = PilotCredentialStore::for_data_dir(&data_dir);
        pilot_credentials.init()?;
        let governance = GovernanceStore::for_data_dir(&data_dir);
        governance.init()?;

        // ── iroh Endpoint ─────────────────────────────────────────────────────
        // `MemoryLookup` allows callers to pre-populate peer addresses before
        // gossip-bootstrapping, enabling direct loopback connections without
        // relay infrastructure (used by integration tests).
        let memory_lookup = MemoryLookup::new();
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .address_lookup(memory_lookup.clone())
            .bind()
            .await
            .map_err(|e| NodeError::EndpointBind(e.to_string()))?;

        let node_id = NodeIdHex::from_public_key(endpoint.id());

        // ── iroh-blobs filesystem store ───────────────────────────────────────
        let blob_dir = data_dir.join("iroh-blobs");
        tokio::fs::create_dir_all(&blob_dir).await?;
        let fs_store = FsStore::load(&blob_dir)
            .await
            .map_err(|e| NodeError::BlobStoreLoad(e.to_string()))?;

        // ── iroh-gossip ───────────────────────────────────────────────────────
        let gossip = Gossip::builder().spawn(endpoint.clone());

        // ── Router: register protocol handlers ────────────────────────────────
        // `Router` is `#[must_use]` — the accept loop runs as long as the
        // handle is alive.  It is stored in `IgcIrohNode` so it lives for the
        // full lifetime of the node.
        let governance_sync = GovernanceSyncProtocol {
            governance: governance.clone(),
        };
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&fs_store, None),
            )
            .accept(GOVERNANCE_SYNC_ALPN, governance_sync)
            .spawn();

        // ── Persistent announce-topic subscription ────────────────────────────
        // Subscribe to the announce topic with no bootstrap peers so the
        // per-topic HyParView state is created immediately.  Remote indexers
        // that bootstrap from this node via its PublicKey will then have their
        // JOIN messages accepted and be added to the active view.  Without this
        // subscription, incoming JOINs for an unknown topic are silently dropped
        // by the gossip actor, so the broadcaster would have no known neighbors
        // when it later calls `publish()`.
        let announce_topic = TopicId::from_bytes(announce_topic_id());
        let (announce_sender, mut announce_receiver) = gossip
            .subscribe(announce_topic, vec![])
            .await
            .map_err(|e| NodeError::GossipSubscribe(e.to_string()))?
            .split();

        // Drain the receiver in the background to prevent backpressure from
        // filling the event buffer and closing the subscription.
        tokio::spawn(async move { while announce_receiver.next().await.is_some() {} });

        // ── Persistent pilot-auth-did governance subscription ─────────────────
        let governance_topic = TopicId::from_bytes(pilot_auth_did_governance_topic_id());
        let (governance_sender, mut governance_receiver) = gossip
            .subscribe(governance_topic, vec![])
            .await
            .map_err(|e| NodeError::GovernanceGossipSubscribe(e.to_string()))?
            .split();
        let governance_sender_task = governance_sender.clone();
        let governance_store = governance.clone();
        let governance_endpoint = endpoint.clone();
        let local_endpoint_id = endpoint.id();
        tokio::spawn(async move {
            while let Some(event) = governance_receiver.next().await {
                match event {
                    Ok(GossipEvent::Received(message)) => {
                        if message.delivered_from == local_endpoint_id {
                            continue;
                        }
                        let announcement: PilotAuthDidGossipAnnouncement =
                            match serde_json::from_slice(&message.content) {
                                Ok(announcement) => announcement,
                                Err(err) => {
                                    tracing::warn!(
                                        peer = %message.delivered_from,
                                        error = %err,
                                        "ignoring invalid pilot-auth-did governance gossip payload"
                                    );
                                    continue;
                                }
                            };
                        match sync_pilot_auth_did_from_gossip_announcement(
                            &governance_endpoint,
                            &governance_store,
                            &governance_sender_task,
                            message.delivered_from,
                            &announcement,
                        )
                        .await
                        {
                            Ok(0) => {}
                            Ok(applied) => tracing::info!(
                                peer = %message.delivered_from,
                                pilot_id = %announcement.pilot_id,
                                record_id = %announcement.record_id,
                                applied,
                                "applied pilot-auth-did governance update from gossip"
                            ),
                            Err(err) => tracing::warn!(
                                peer = %message.delivered_from,
                                pilot_id = %announcement.pilot_id,
                                record_id = %announcement.record_id,
                                error = %err,
                                "failed to apply pilot-auth-did governance update from gossip"
                            ),
                        }
                    }
                    Ok(GossipEvent::NeighborUp(peer)) => {
                        tracing::debug!(%peer, "pilot-auth-did governance neighbor up");
                    }
                    Ok(GossipEvent::NeighborDown(peer)) => {
                        tracing::debug!(%peer, "pilot-auth-did governance neighbor down");
                    }
                    Ok(GossipEvent::Lagged) => {
                        tracing::warn!(
                            "pilot-auth-did governance gossip receiver lagged; some updates may require catch-up"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "pilot-auth-did governance gossip subscription closed"
                        );
                        break;
                    }
                }
            }
        });

        // ── Persistent normative governance subscription ─────────────────────
        let governance_record_topic = TopicId::from_bytes(governance_topic_id());
        let (governance_record_sender, mut governance_record_receiver) = gossip
            .subscribe(governance_record_topic, vec![])
            .await
            .map_err(|e| NodeError::GovernanceGossipSubscribe(e.to_string()))?
            .split();
        let governance_record_store = governance.clone();
        let governance_record_local_endpoint_id = endpoint.id();
        tokio::spawn(async move {
            while let Some(event) = governance_record_receiver.next().await {
                match event {
                    Ok(GossipEvent::Received(message)) => {
                        if message.delivered_from == governance_record_local_endpoint_id {
                            continue;
                        }
                        let record = match GovernanceRecord::from_slice(&message.content) {
                            Ok(record) => record,
                            Err(err) => {
                                tracing::warn!(
                                    peer = %message.delivered_from,
                                    error = %err,
                                    "ignoring invalid governance gossip payload"
                                );
                                continue;
                            }
                        };
                        match governance_record_store.apply_governance_record(&record) {
                            Ok(true) => tracing::info!(
                                peer = %message.delivered_from,
                                "applied governance record from gossip"
                            ),
                            Ok(false) => {}
                            Err(err) => tracing::warn!(
                                peer = %message.delivered_from,
                                error = %err,
                                "ignoring invalid governance gossip payload"
                            ),
                        }
                    }
                    Ok(GossipEvent::NeighborUp(peer)) => {
                        tracing::debug!(%peer, "governance neighbor up");
                    }
                    Ok(GossipEvent::NeighborDown(peer)) => {
                        tracing::debug!(%peer, "governance neighbor down");
                    }
                    Ok(GossipEvent::Lagged) => {
                        tracing::warn!(
                            "governance gossip receiver lagged; some updates may require catch-up"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "governance gossip subscription closed");
                        break;
                    }
                }
            }
        });

        tracing::info!(%node_id, data_dir = %data_dir.display(), "igc-net node started");

        Ok(Self {
            endpoint,
            fs_store,
            gossip,
            store,
            memory_lookup,
            _router: router,
            announce_sender,
            governance_sender,
            governance_record_sender,
            node_id,
            node_key_bytes: key_bytes,
            multi_pilot_keys,
            pilot_credentials,
            governance,
        })
    }

    /// Gracefully shut down the node (closes endpoint and router).
    pub async fn close(&self) {
        self.endpoint.close().await;
    }

    /// The node's stable network identity (hex-encoded Ed25519 public key).
    pub fn node_id(&self) -> &NodeIdHex {
        &self.node_id
    }

    /// The node's iroh `PublicKey` (EndpointId) — use this for gossip bootstrap
    /// when dialling the node directly via iroh.
    pub fn iroh_node_id(&self) -> iroh::PublicKey {
        self.endpoint.id()
    }

    /// The node's current `EndpointAddr` as reported by the iroh endpoint.
    ///
    /// Right after `start()` this typically contains wildcard bind addresses
    /// (`0.0.0.0:PORT`) which are not dialable by remote peers.  For loopback
    /// integration tests use [`loopback_endpoint_addr`] instead.
    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Build an `EndpointAddr` with a proper `127.0.0.1:PORT` direct address.
    ///
    /// Uses the actual bound UDP port from the endpoint and replaces the
    /// wildcard `0.0.0.0` bind address with the loopback interface.  Pass
    /// the result to a peer's [`add_peer_addr`] in integration tests so that
    /// gossip-bootstrap can dial over loopback without relay infrastructure.
    pub fn loopback_endpoint_addr(&self) -> Result<EndpointAddr, NodeError> {
        let id = self.endpoint.id();
        let port = self.loopback_port()?;
        Ok(EndpointAddr::new(id).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], port))))
    }

    /// Return the node's loopback endpoint as a `"node_id@127.0.0.1:port"` string.
    ///
    /// Use this to populate a remote peer's address book (via [`add_peer_addr`])
    /// for direct loopback connections in tests and private networks that don't
    /// rely on relay-based discovery.
    pub fn loopback_addr_str(&self) -> Result<String, NodeError> {
        let port = self.loopback_port()?;
        Ok(format!("{}@127.0.0.1:{}", self.node_id(), port))
    }

    /// Pre-populate this node's address book with a peer's `EndpointAddr`.
    ///
    /// After calling this, the node can dial the peer by its `EndpointId`
    /// alone (e.g., as a gossip bootstrap peer) using the known direct address
    /// instead of relay-based discovery.
    pub fn add_peer_addr(&self, addr: EndpointAddr) {
        self.memory_lookup.add_endpoint_info(addr);
    }

    /// Join known peers on the pilot-auth-did governance gossip topic.
    ///
    /// Call this after populating peer addresses for direct/private networks.
    /// Receiving a governance gossip announcement triggers a pull-sync against
    /// the delivering peer over the dedicated governance-sync transport.
    pub async fn join_pilot_auth_did_gossip_peers(
        &self,
        peers: Vec<iroh::PublicKey>,
    ) -> Result<(), NodeError> {
        let peers = peers
            .into_iter()
            .filter(|peer| *peer != self.iroh_node_id())
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return Ok(());
        }
        self.governance_sender
            .join_peers(peers)
            .await
            .map_err(|err| NodeError::GovernanceGossipJoin(err.to_string()))
    }

    /// Join known peers on the normative governance gossip topic.
    ///
    /// Call this after populating peer addresses for direct/private networks.
    /// Received records are validated, persisted idempotently, and then used by
    /// local governance state resolution.
    pub async fn join_governance_gossip_peers(
        &self,
        peers: Vec<iroh::PublicKey>,
    ) -> Result<(), NodeError> {
        let peers = peers
            .into_iter()
            .filter(|peer| *peer != self.iroh_node_id())
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return Ok(());
        }
        self.governance_record_sender
            .join_peers(peers)
            .await
            .map_err(|err| NodeError::GovernanceGossipJoin(err.to_string()))
    }

    /// The persistent announce-topic sender.
    ///
    /// Use this to broadcast on the announce topic without creating a new
    /// gossip subscription.
    pub(crate) fn announce_sender(&self) -> &GossipSender {
        &self.announce_sender
    }

    /// Broadcast a full governance record on the normative governance topic.
    ///
    /// Callers must persist the record locally before broadcasting so a local
    /// restart does not lose authority that was already advertised.
    pub async fn broadcast_governance_record<T: serde::Serialize>(
        &self,
        record: &T,
    ) -> Result<(), NodeError> {
        broadcast_governance_record(&self.governance_record_sender, record).await
    }

    async fn broadcast_pilot_auth_did_update(
        &self,
        record: &crate::PilotAuthDidRecord,
    ) -> Result<(), NodeError> {
        let announcement = PilotAuthDidGossipAnnouncement::from_record(record);
        broadcast_governance_record(&self.governance_record_sender, record).await?;
        broadcast_pilot_auth_did_gossip_announcement(&self.governance_sender, &announcement).await
    }

    /// Access the local flat-file store.
    pub fn store(&self) -> &FlatFileStore {
        self.store.as_ref()
    }

    /// Generate a new registered pilot identity in the multi-pilot key store.
    pub fn generate_pilot_identity(
        &self,
        display_name: impl Into<String>,
        country: Option<String>,
    ) -> Result<PilotIdentity, NodeError> {
        Ok(self
            .multi_pilot_keys
            .generate_pilot(display_name, country, &self.node_secret_key())?)
    }

    /// Register a pilot with a local portal credential and publish its initial auth DID.
    pub async fn register_pilot_identity(
        &self,
        display_name: impl Into<String>,
        country: Option<String>,
        access_pin: &str,
        created_at: impl Into<String>,
    ) -> Result<PilotIdentity, NodeError> {
        let identity = self.generate_pilot_identity(display_name, country)?;
        self.pilot_credentials
            .set_credential(&identity.pilot_id(), access_pin)?;
        let record = issue_initial_pilot_auth_did_record(&self.governance, &identity, created_at)?;
        self.governance.persist_pilot_auth_did_record(&record)?;
        self.broadcast_pilot_auth_did_update(&record).await?;
        Ok(identity)
    }

    /// Load a registered pilot identity by stable `pilot_id`.
    pub fn load_registered_pilot_identity(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Option<PilotIdentity>, NodeError> {
        Ok(self
            .multi_pilot_keys
            .load_pilot(pilot_id, &self.node_secret_key())?)
    }

    /// List registered pilots without exposing private key material.
    pub fn list_registered_pilots(&self) -> Result<Vec<PilotPublicIdentityWithProfile>, NodeError> {
        Ok(self.multi_pilot_keys.list_pilots(&self.node_secret_key())?)
    }

    /// Load registered pilot profile metadata.
    pub fn load_registered_pilot_profile(
        &self,
        pilot_id: &PilotId,
    ) -> Result<Option<PilotProfile>, NodeError> {
        Ok(self.multi_pilot_keys.load_profile(pilot_id)?)
    }

    /// Verify a registered pilot's local portal credential.
    pub fn verify_pilot_credential(
        &self,
        pilot_id: &PilotId,
        access_pin: &str,
    ) -> Result<bool, NodeError> {
        Ok(self
            .pilot_credentials
            .verify_credential(pilot_id, access_pin)?)
    }

    /// Return the per-pilot store used for pilot-auth-DID rotation.
    pub fn registered_pilot_store(&self, pilot_id: &PilotId) -> PilotKeyStore {
        self.multi_pilot_keys.pilot_store(pilot_id)
    }

    /// Create and persist the initial pilot-auth-did-record for a registered pilot.
    pub async fn issue_initial_registered_pilot_auth_did_record(
        &self,
        pilot_id: &PilotId,
        created_at: impl Into<String>,
    ) -> Result<crate::PilotAuthDidRecord, NodeError> {
        let identity = self
            .load_registered_pilot_identity(pilot_id)?
            .ok_or(PilotKeyStoreError::MissingPilotIdentity)?;
        let record = issue_initial_pilot_auth_did_record(&self.governance, &identity, created_at)?;
        self.governance.persist_pilot_auth_did_record(&record)?;
        self.broadcast_pilot_auth_did_update(&record).await?;
        Ok(record)
    }

    /// Rotate the active pilot_auth_did key for a registered pilot.
    pub async fn rotate_registered_pilot_auth_did(
        &self,
        pilot_id: &PilotId,
        created_at: impl Into<String>,
    ) -> Result<crate::PilotAuthDidRecord, NodeError> {
        let current_identity = self
            .load_registered_pilot_identity(pilot_id)?
            .ok_or(PilotKeyStoreError::MissingPilotIdentity)?;
        let pilot_store = self.registered_pilot_store(pilot_id);
        let next_active_pilot_auth_secret_key =
            pilot_store.generate_next_active_pilot_auth_secret_key(&self.node_secret_key())?;
        let record = rotate_pilot_auth_did_record(
            &self.governance,
            &current_identity,
            &next_active_pilot_auth_secret_key,
            created_at,
        )?;
        pilot_store.replace_active_pilot_auth(
            &self.node_secret_key(),
            &next_active_pilot_auth_secret_key,
        )?;
        if let Err(persist_err) = self.governance.persist_pilot_auth_did_record(&record) {
            match pilot_store.replace_active_pilot_auth(
                &self.node_secret_key(),
                &current_identity.active_pilot_auth_secret_key(),
            ) {
                Ok(_) => return Err(NodeError::PilotAuthDidRotationPersistFailed(persist_err)),
                Err(rollback_err) => {
                    return Err(NodeError::PilotAuthDidRotationPersistRollback {
                        persist: persist_err,
                        rollback: rollback_err,
                    });
                }
            }
        }
        self.broadcast_pilot_auth_did_update(&record).await?;
        Ok(record)
    }

    /// Access the governance store that persists identity governance records.
    pub fn governance_store(&self) -> &GovernanceStore {
        &self.governance
    }

    /// Resolve the current pilot-auth-DID state using local governance history.
    pub fn resolve_pilot_auth_did_state(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PilotAuthDidState, NodeError> {
        Ok(self.governance.resolve_pilot_auth_did_state(pilot_id)?)
    }

    /// Build a pull-style catch-up response for a peer's known pilot-auth-DID history.
    pub fn prepare_pilot_auth_did_sync(
        &self,
        request: &PilotAuthDidSyncRequest,
    ) -> Result<PilotAuthDidSyncResponse, NodeError> {
        Ok(self.governance.prepare_pilot_auth_did_sync(request)?)
    }

    /// Build a pull-style catch-up request from the node's full local pilot-auth-DID history.
    pub fn build_pilot_auth_did_sync_request(
        &self,
        pilot_id: &PilotId,
    ) -> Result<PilotAuthDidSyncRequest, NodeError> {
        Ok(self
            .governance
            .build_pilot_auth_did_sync_request(pilot_id)?)
    }

    /// Apply a pulled batch of pilot-auth-DID governance records to local storage.
    pub fn apply_pilot_auth_did_sync(
        &self,
        response: &PilotAuthDidSyncResponse,
    ) -> Result<usize, NodeError> {
        Ok(self.governance.apply_pilot_auth_did_sync(response)?)
    }

    /// Request a peer's catch-up response over the governance-sync transport.
    pub async fn request_pilot_auth_did_sync_from_peer(
        &self,
        peer: iroh::PublicKey,
        request: &PilotAuthDidSyncRequest,
    ) -> Result<PilotAuthDidSyncResponse, NodeError> {
        request_pilot_auth_did_sync_from_peer_via(&self.endpoint, peer, request).await
    }

    /// Pull pilot-auth-DID governance records for `pilot_id` from a peer and apply them locally.
    pub async fn sync_pilot_auth_did_from_peer(
        &self,
        peer: iroh::PublicKey,
        pilot_id: &PilotId,
    ) -> Result<usize, NodeError> {
        let request = self.build_pilot_auth_did_sync_request(pilot_id)?;
        let response = self
            .request_pilot_auth_did_sync_from_peer(peer, &request)
            .await?;
        self.apply_pilot_auth_did_sync(&response)
    }

    /// Resolve a local read-only filesystem path for a BLAKE3-keyed blob.
    ///
    /// Returns `Some(path)` when the blob is present in the flat-file store.
    /// The caller may read the file directly in read-only mode; mutation must
    /// go through `publish()` or the store's `put()` method.
    pub fn resolve_path(&self, igc_hash: &str) -> Result<Option<std::path::PathBuf>, StoreError> {
        self.store.resolve_path(igc_hash)
    }

    fn loopback_port(&self) -> Result<u16, NodeError> {
        self.endpoint
            .bound_sockets()
            .into_iter()
            .find_map(|addr| {
                if addr.is_ipv4() {
                    Some(addr.port())
                } else {
                    None
                }
            })
            .ok_or(NodeError::NoLoopbackSocket)
    }

    pub(crate) fn node_secret_key(&self) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&self.node_key_bytes)
    }
}
