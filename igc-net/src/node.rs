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
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh_blobs::store::fs::FsStore;
use iroh_gossip::api::GossipSender;
use iroh_gossip::net::{GOSSIP_ALPN, Gossip};
use iroh_gossip::proto::TopicId;

use crate::id::NodeIdHex;
use crate::store::{FlatFileStore, StoreError};
use crate::topic::announce_topic_id;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to bind iroh endpoint: {0}")]
    EndpointBind(String),
    #[error("failed to load iroh blob store: {0}")]
    BlobStoreLoad(String),
    #[error("failed to subscribe to announce topic: {0}")]
    GossipSubscribe(String),
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
    node_id: NodeIdHex,
}

impl IgcIrohNode {
    /// Build and start a node rooted at `data_dir`.
    ///
    /// - Loads or generates the Ed25519 key from `data_dir/node.key`.
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
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&fs_store, None),
            )
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

        tracing::info!(%node_id, data_dir = %data_dir.display(), "igc-net node started");

        Ok(Self {
            endpoint,
            fs_store,
            gossip,
            store,
            memory_lookup,
            _router: router,
            announce_sender,
            node_id,
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
        Ok(EndpointAddr::new(id).with_ip_addr(std::net::SocketAddr::from((
            [127, 0, 0, 1],
            port,
        ))))
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

    /// The persistent announce-topic sender.
    ///
    /// Use this to broadcast on the announce topic without creating a new
    /// gossip subscription.
    pub(crate) fn announce_sender(&self) -> &GossipSender {
        &self.announce_sender
    }

    /// Access the local flat-file store.
    pub fn store(&self) -> &FlatFileStore {
        self.store.as_ref()
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
            .find_map(|addr| if addr.is_ipv4() { Some(addr.port()) } else { None })
            .ok_or(NodeError::NoLoopbackSocket)
    }
}
