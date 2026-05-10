use std::net::TcpListener;
use std::sync::OnceLock;
use std::time::Duration;

use igc_net_grpc::proto::igc_net_client::IgcNetClient;
use igc_net_grpc::proto::{
    ArtifactClass as ProtoArtifactClass, EventKind, FetchArtifactRequest, GetNodeStatusRequest,
    IssuePortalAuthTokenRequest, ListPilotsRequest, ProvisionPrivateAccessKeyRequest,
    PublicationMode, PublishFlightRequest, PublishFlightResponse, PublishedArtifact,
    QueryIndexRequest, RegisterPilotRequest, RevokePrivateAccessRequest, SubscribeEventsRequest,
};
use std::path::Path;
use std::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;

fn secret_key(byte: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[byte; 32])
}

fn published_response_artifact(
    response: &PublishFlightResponse,
    artifact_class: ProtoArtifactClass,
) -> &PublishedArtifact {
    response
        .artifacts
        .iter()
        .find(|artifact| artifact.artifact_class == artifact_class as i32)
        .unwrap()
}

fn free_loopback_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn process_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct GrpcProcess {
    child: Child,
    addr: String,
    stopped: bool,
}

impl GrpcProcess {
    fn start(data_dir: &Path) -> Self {
        let addr = free_loopback_addr();
        let bin = env!("CARGO_BIN_EXE_igc-net-grpc");
        let child = Command::new(bin)
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--grpc-addr")
            .arg(&addr)
            .spawn()
            .unwrap();
        Self {
            child,
            addr,
            stopped: false,
        }
    }

    async fn client(&self) -> IgcNetClient<tonic::transport::Channel> {
        connect_with_retry(&self.addr).await
    }

    fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::thread::sleep(Duration::from_millis(250));
        self.stopped = true;
    }
}

impl Drop for GrpcProcess {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn connect_with_retry(addr: &str) -> IgcNetClient<tonic::transport::Channel> {
    let endpoint = format!("http://{addr}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match IgcNetClient::connect(endpoint.clone()).await {
            Ok(client) => return client,
            Err(err) if tokio::time::Instant::now() < deadline => {
                let _ = err;
                sleep(Duration::from_millis(100)).await;
            }
            Err(err) => panic!("failed to connect to igc-net-grpc at {endpoint}: {err}"),
        }
    }
}

async fn seed_private_artifact(
    node: &igc_net::IgcIrohNode,
    raw_igc: &[u8],
    pilot_id: &igc_net::PilotId,
    status: igc_net::FlightGovernanceStatus,
) -> igc_net::Blake3Hex {
    seed_private_artifact_with_baseline(node, raw_igc, pilot_id, status, true).await
}

async fn seed_private_artifact_with_baseline(
    node: &igc_net::IgcIrohNode,
    raw_igc: &[u8],
    pilot_id: &igc_net::PilotId,
    status: igc_net::FlightGovernanceStatus,
    baseline_ready: bool,
) -> igc_net::Blake3Hex {
    let raw_igc_hash = node.store().put(raw_igc).await.unwrap();
    node.store()
        .append_artifact_registry_record(&igc_net::ArtifactRegistryRecord {
            raw_igc_hash: raw_igc_hash.clone(),
            pilot_id: Some(pilot_id.clone()),
            publication_mode: igc_net::PublicationMode::Private,
            protected_hash: None,
            has_raw_igc: true,
            has_protected_sanitized_igc: false,
            has_protected_raw_companion: false,
            serving_node_ids: vec![node.node_id().clone()],
            g_record_present: Some(false),
            recorded_at: "2026-05-01T09:14:00Z".to_string(),
        })
        .await
        .unwrap();
    node.governance_store()
        .persist_flight_governance_state(&igc_net::FlightGovernanceState {
            raw_igc_hash: raw_igc_hash.clone(),
            owner_pilot_id: Some(pilot_id.clone()),
            status,
            baseline_ready,
            recorded_at: "2026-05-01T09:14:00Z".to_string(),
        })
        .unwrap();
    raw_igc_hash
}

#[tokio::test]
async fn process_grpc_public_publish_index_and_events() {
    let _guard = process_test_lock().lock().await;
    let data_dir = tempfile::tempdir().unwrap();
    let process = GrpcProcess::start(data_dir.path());
    let mut client = process.client().await;

    let status = client
        .get_node_status(GetNodeStatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(status.ready);
    assert!(status.blob_store_ready);
    assert!(status.artifact_registry_ready);
    assert!(status.event_cursor_ready);

    let published = client
        .publish_flight(PublishFlightRequest {
            raw_igc: b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n".to_vec(),
            filename: "flight.igc".to_string(),
            publication_mode: PublicationMode::Public as i32,
            pilot_id: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(published.raw_igc_hash.len(), 64);

    let index = client
        .query_index(QueryIndexRequest {
            page_size: 10,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(index.entries.len(), 1);
    assert_eq!(index.entries[0].raw_igc_hash, published.raw_igc_hash);
    assert_eq!(
        index.entries[0].publication_mode,
        PublicationMode::Public as i32
    );
    assert!(index.entries[0].locally_fetchable);

    let mut events = client
        .subscribe_events(SubscribeEventsRequest { from_seq: 0 })
        .await
        .unwrap()
        .into_inner();
    let first = events.message().await.unwrap().unwrap();
    assert_eq!(first.seq, 0);
    assert_eq!(first.kind, EventKind::LocalPublish as i32);
    assert_eq!(first.entry.unwrap().raw_igc_hash, published.raw_igc_hash);
    assert!(events.message().await.unwrap().is_none());

    process.stop();
}

#[tokio::test]
async fn process_grpc_register_list_and_issue_portal_auth_token() {
    let _guard = process_test_lock().lock().await;
    let data_dir = tempfile::tempdir().unwrap();
    let process = GrpcProcess::start(data_dir.path());
    let mut client = process.client().await;

    let registered = client
        .register_pilot(RegisterPilotRequest {
            display_name: "Alice".to_string(),
            access_pin: "1234".to_string(),
            country: "NO".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(registered.pilot_id.starts_with("igcnet:id:"));
    assert!(registered.pilot_auth_did.starts_with("did:key:"));
    assert_eq!(registered.display_name, "Alice");

    let listed = client
        .list_pilots(ListPilotsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.pilots.len(), 1);
    assert_eq!(listed.pilots[0].pilot_id, registered.pilot_id);
    assert_eq!(listed.pilots[0].display_name, "Alice");
    assert_eq!(listed.pilots[0].country, "NO");

    let token = client
        .issue_portal_auth_token(IssuePortalAuthTokenRequest {
            pilot_id: registered.pilot_id.clone(),
            portal_id: "cs-archive-local".to_string(),
            jti: "process-test-jti".to_string(),
            access_pin: "1234".to_string(),
            expires_in_seconds: 60,
        })
        .await
        .unwrap()
        .into_inner();
    let jwt = igc_net::PilotProfileCredentialJwt::parse(&token.pilot_profile_vc_jwt).unwrap();
    jwt.verify_signature().unwrap();
    assert_eq!(jwt.claims().sub.to_string(), registered.pilot_id);
    assert!(matches!(
        jwt.claims().aud.as_ref(),
        Some(igc_net::JwtAudience::One(audience)) if audience == "cs-archive-local"
    ));
    assert_eq!(jwt.claims().jti, "process-test-jti");
    assert_eq!(
        jwt.claims().vc.credential_subject.name.as_deref(),
        Some("Alice")
    );
    assert_eq!(
        jwt.claims().vc.credential_subject.country.as_deref(),
        Some("NO")
    );

    let bad_pin = client
        .issue_portal_auth_token(IssuePortalAuthTokenRequest {
            pilot_id: registered.pilot_id,
            portal_id: "cs-archive-local".to_string(),
            jti: "process-test-bad-pin".to_string(),
            access_pin: "9999".to_string(),
            expires_in_seconds: 60,
        })
        .await
        .unwrap_err();
    assert_eq!(bad_pin.code(), tonic::Code::Unauthenticated);

    process.stop();
}

#[tokio::test]
async fn process_grpc_public_fetch_pagination_events_and_restart_persist() {
    let _guard = process_test_lock().lock().await;
    let data_dir = tempfile::tempdir().unwrap();
    let raw_first = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLONE\r\n";
    let raw_second = b"HFDTE030714\r\nB1400004731000N00838000EA0030003000\r\nLTWO\r\n";

    let first_hash;
    let second_hash;
    let first_node_id;
    {
        let process = GrpcProcess::start(data_dir.path());
        let mut client = process.client().await;
        first_node_id = client
            .get_node_status(GetNodeStatusRequest {})
            .await
            .unwrap()
            .into_inner()
            .node_id;

        let first = client
            .publish_flight(PublishFlightRequest {
                raw_igc: raw_first.to_vec(),
                filename: "first.igc".to_string(),
                publication_mode: PublicationMode::Public as i32,
                pilot_id: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        let second = client
            .publish_flight(PublishFlightRequest {
                raw_igc: raw_second.to_vec(),
                filename: "second.igc".to_string(),
                publication_mode: PublicationMode::Public as i32,
                pilot_id: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        first_hash = first.raw_igc_hash.clone();
        second_hash = second.raw_igc_hash.clone();

        let fetched = client
            .fetch_artifact(FetchArtifactRequest {
                raw_igc_hash: first_hash.clone(),
                artifact_class: ProtoArtifactClass::PublicRawIgc as i32,
                requester_key: String::new(),
                seq_num: 0,
                signature: Vec::new(),
                group_fetch_proof: None,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(fetched.artifact_bytes, raw_first);
        assert_eq!(fetched.artifact_hash, first_hash);
        assert_eq!(fetched.raw_igc_hash, first_hash);
        assert_eq!(
            fetched.artifact_class,
            ProtoArtifactClass::PublicRawIgc as i32
        );

        let first_page = client
            .query_index(QueryIndexRequest {
                page_size: 1,
                page_token: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first_page.entries.len(), 1);
        assert!(!first_page.next_page_token.is_empty());

        let second_page = client
            .query_index(QueryIndexRequest {
                page_size: 1,
                page_token: first_page.next_page_token,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(second_page.entries.len(), 1);
        assert!(second_page.next_page_token.is_empty());
        let paged_entries = [&first_page.entries[0], &second_page.entries[0]];
        assert!(
            paged_entries
                .iter()
                .any(|entry| entry.raw_igc_hash == first_hash && entry.updated_event_seq == 0)
        );
        assert!(
            paged_entries
                .iter()
                .any(|entry| entry.raw_igc_hash == second_hash && entry.updated_event_seq == 1)
        );

        let mut events = client
            .subscribe_events(SubscribeEventsRequest { from_seq: 1 })
            .await
            .unwrap()
            .into_inner();
        let resumed = events.message().await.unwrap().unwrap();
        assert_eq!(resumed.seq, 1);
        assert_eq!(resumed.entry.unwrap().raw_igc_hash, second_hash);
        assert!(events.message().await.unwrap().is_none());

        process.stop();
    }

    let restarted = GrpcProcess::start(data_dir.path());
    let mut client = restarted.client().await;
    let restarted_status = client
        .get_node_status(GetNodeStatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(restarted_status.node_id, first_node_id);
    assert_eq!(restarted_status.latest_event_seq, 1);

    let index = client
        .query_index(QueryIndexRequest {
            page_size: 10,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(index.entries.len(), 2);
    assert!(
        index
            .entries
            .iter()
            .any(|entry| entry.raw_igc_hash == first_hash)
    );
    assert!(
        index
            .entries
            .iter()
            .any(|entry| entry.raw_igc_hash == second_hash)
    );

    let fetched = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: second_hash.clone(),
            artifact_class: ProtoArtifactClass::PublicRawIgc as i32,
            requester_key: String::new(),
            seq_num: 0,
            signature: Vec::new(),
            group_fetch_proof: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.artifact_bytes, raw_second);
    assert_eq!(fetched.raw_igc_hash, second_hash);

    restarted.stop();
}

#[tokio::test]
async fn process_grpc_protected_publish_serves_only_sanitized_without_auth() {
    let _guard = process_test_lock().lock().await;
    let data_dir = tempfile::tempdir().unwrap();
    let raw_igc =
        b"HFPLTPILOT:Alice\r\nHFCIDCOMPETITION:ABC\r\nB1300004730000N00837000EA0030003000\r\n";
    let sanitized = b"HFPLT:REDACTED\r\nHFCID:REDACTED\r\nB1300004730000N00837000EA0030003000\r\n";

    let seed_node = igc_net::IgcIrohNode::start(data_dir.path()).await.unwrap();
    let pilot_id = seed_node
        .generate_pilot_identity("Test Pilot", None)
        .unwrap()
        .pilot_id();
    seed_node.close().await;
    drop(seed_node);
    sleep(Duration::from_millis(250)).await;

    let process = GrpcProcess::start(data_dir.path());
    let mut client = process.client().await;

    let published = client
        .publish_flight(PublishFlightRequest {
            raw_igc: raw_igc.to_vec(),
            filename: "protected.igc".to_string(),
            publication_mode: PublicationMode::Protected as i32,
            pilot_id: pilot_id.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(published.raw_igc_hash.len(), 64);
    assert_eq!(published.artifacts.len(), 2);
    let protected_artifact =
        published_response_artifact(&published, ProtoArtifactClass::ProtectedSanitizedIgc);
    let companion_artifact =
        published_response_artifact(&published, ProtoArtifactClass::ProtectedRawCompanion);
    assert_eq!(protected_artifact.artifact_hash.len(), 64);
    assert_eq!(companion_artifact.artifact_hash, published.raw_igc_hash);
    assert!(!protected_artifact.ticket.is_empty());
    assert!(!companion_artifact.ticket.is_empty());

    let fetched = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: published.raw_igc_hash.clone(),
            artifact_class: ProtoArtifactClass::ProtectedSanitizedIgc as i32,
            requester_key: String::new(),
            seq_num: 0,
            signature: Vec::new(),
            group_fetch_proof: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.artifact_bytes, sanitized);
    assert_eq!(fetched.artifact_hash, protected_artifact.artifact_hash);

    let public_raw = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: published.raw_igc_hash,
            artifact_class: ProtoArtifactClass::PublicRawIgc as i32,
            requester_key: String::new(),
            seq_num: 0,
            signature: Vec::new(),
            group_fetch_proof: None,
        })
        .await
        .unwrap_err();
    assert_eq!(public_raw.code(), tonic::Code::PermissionDenied);

    process.stop();
}

#[tokio::test]
async fn process_grpc_private_fetch_refuses_stale_governance_and_stale_rotation_key() {
    let _guard = process_test_lock().lock().await;
    let stale_governance_dir = tempfile::tempdir().unwrap();
    let stale_governance_key = secret_key(131);
    let stale_rotation_old_key = secret_key(132);
    let stale_rotation_new_key = secret_key(133);

    let seed_node = igc_net::IgcIrohNode::start(stale_governance_dir.path())
        .await
        .unwrap();
    let node_secret_key =
        iroh::SecretKey::from_bytes(&seed_node.store().load_key_bytes().unwrap().unwrap());
    let identity = seed_node
        .generate_pilot_identity("Test Pilot", None)
        .unwrap();
    let pilot_id = identity.pilot_id();

    let stale_governance_rotation = igc_net::PrivateAccessRotationRecord::issue(
        &identity.pilot_id_secret_key(),
        stale_governance_key.public(),
        None,
        "2026-05-01T09:14:00Z",
    )
    .unwrap();
    seed_node
        .governance_store()
        .persist_private_access_rotation_record(&stale_governance_rotation)
        .unwrap();
    let stale_governance_hash = seed_private_artifact_with_baseline(
        &seed_node,
        b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLSTALEGOV\r\n",
        &pilot_id,
        igc_net::FlightGovernanceStatus::Approved,
        false,
    )
    .await;
    igc_net::PrivateAccessKeyStore::for_data_dir(stale_governance_dir.path())
        .provision_for_pilot(&pilot_id, &stale_governance_key, &node_secret_key)
        .unwrap();
    seed_node.close().await;
    drop(seed_node);
    sleep(Duration::from_millis(250)).await;

    let process = GrpcProcess::start(stale_governance_dir.path());
    let mut client = process.client().await;
    let proof = igc_net::sign_fetch_proof(
        stale_governance_hash.as_str(),
        igc_net::ArtifactClass::PrivateRawIgc,
        1,
        &stale_governance_key,
    )
    .unwrap();
    let stale_governance = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: stale_governance_hash.to_string(),
            artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
            requester_key: proof.requester_key,
            seq_num: proof.seq_num,
            signature: hex::decode(proof.signature).unwrap(),
            group_fetch_proof: None,
        })
        .await
        .unwrap_err();
    assert_eq!(stale_governance.code(), tonic::Code::FailedPrecondition);
    process.stop();

    let stale_rotation_dir = tempfile::tempdir().unwrap();
    let seed_node = igc_net::IgcIrohNode::start(stale_rotation_dir.path())
        .await
        .unwrap();
    let node_secret_key =
        iroh::SecretKey::from_bytes(&seed_node.store().load_key_bytes().unwrap().unwrap());
    let identity = seed_node
        .generate_pilot_identity("Test Pilot", None)
        .unwrap();
    let pilot_id = identity.pilot_id();
    let old_rotation = igc_net::PrivateAccessRotationRecord::issue(
        &identity.pilot_id_secret_key(),
        stale_rotation_old_key.public(),
        None,
        "2026-05-01T10:14:00Z",
    )
    .unwrap();
    let new_rotation = igc_net::PrivateAccessRotationRecord::issue(
        &identity.pilot_id_secret_key(),
        stale_rotation_new_key.public(),
        Some(old_rotation.record_id.clone()),
        "2026-05-01T11:14:00Z",
    )
    .unwrap();
    seed_node
        .governance_store()
        .persist_private_access_rotation_record(&old_rotation)
        .unwrap();
    seed_node
        .governance_store()
        .persist_private_access_rotation_record(&new_rotation)
        .unwrap();
    let stale_rotation_hash = seed_private_artifact(
        &seed_node,
        b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLSTALEROT\r\n",
        &pilot_id,
        igc_net::FlightGovernanceStatus::Approved,
    )
    .await;
    igc_net::PrivateAccessKeyStore::for_data_dir(stale_rotation_dir.path())
        .provision_for_pilot(&pilot_id, &stale_rotation_old_key, &node_secret_key)
        .unwrap();
    seed_node.close().await;
    drop(seed_node);
    sleep(Duration::from_millis(250)).await;

    let process = GrpcProcess::start(stale_rotation_dir.path());
    let mut client = process.client().await;
    let proof = igc_net::sign_fetch_proof(
        stale_rotation_hash.as_str(),
        igc_net::ArtifactClass::PrivateRawIgc,
        1,
        &stale_rotation_new_key,
    )
    .unwrap();
    let stale_rotation = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: stale_rotation_hash.to_string(),
            artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
            requester_key: proof.requester_key,
            seq_num: proof.seq_num,
            signature: hex::decode(proof.signature).unwrap(),
            group_fetch_proof: None,
        })
        .await
        .unwrap_err();
    assert_eq!(stale_rotation.code(), tonic::Code::FailedPrecondition);

    process.stop();
}

#[tokio::test]
async fn process_grpc_private_fetch_uses_seeded_governance_and_provisioned_key() {
    let _guard = process_test_lock().lock().await;
    let data_dir = tempfile::tempdir().unwrap();
    let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
    let pilot_root = secret_key(101);
    let private_access_key = secret_key(102);
    let pilot_id = igc_net::PilotId::from_public_key(pilot_root.public());

    let seed_node = igc_net::IgcIrohNode::start(data_dir.path()).await.unwrap();
    let raw_igc_hash = seed_node.store().put(raw_igc).await.unwrap();
    seed_node
        .store()
        .append_artifact_registry_record(&igc_net::ArtifactRegistryRecord {
            raw_igc_hash: raw_igc_hash.clone(),
            pilot_id: Some(pilot_id.clone()),
            publication_mode: igc_net::PublicationMode::Private,
            protected_hash: None,
            has_raw_igc: true,
            has_protected_sanitized_igc: false,
            has_protected_raw_companion: false,
            serving_node_ids: vec![seed_node.node_id().clone()],
            g_record_present: Some(false),
            recorded_at: "2026-05-01T09:14:00Z".to_string(),
        })
        .await
        .unwrap();
    seed_node
        .governance_store()
        .persist_flight_governance_state(&igc_net::FlightGovernanceState::approved_owner(
            raw_igc_hash.clone(),
            pilot_id.clone(),
            "2026-05-01T09:14:00Z",
        ))
        .unwrap();
    let rotation = igc_net::PrivateAccessRotationRecord::issue(
        &pilot_root,
        private_access_key.public(),
        None,
        "2026-05-01T09:14:00Z",
    )
    .unwrap();
    seed_node
        .governance_store()
        .persist_private_access_rotation_record(&rotation)
        .unwrap();
    seed_node.close().await;
    drop(seed_node);
    sleep(Duration::from_millis(250)).await;

    let process = GrpcProcess::start(data_dir.path());
    let mut client = process.client().await;

    let provisioned = client
        .provision_private_access_key(ProvisionPrivateAccessKeyRequest {
            pilot_id: pilot_id.to_string(),
            private_access_secret_key: private_access_key.to_bytes().to_vec(),
            expected_private_access_public_key: private_access_key.public().to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        provisioned.private_access_public_key,
        private_access_key.public().to_string()
    );

    let proof = igc_net::sign_fetch_proof(
        raw_igc_hash.as_str(),
        igc_net::ArtifactClass::PrivateRawIgc,
        1,
        &private_access_key,
    )
    .unwrap();
    let fetched = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: raw_igc_hash.to_string(),
            artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
            requester_key: proof.requester_key,
            seq_num: proof.seq_num,
            signature: hex::decode(proof.signature).unwrap(),
            group_fetch_proof: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.artifact_bytes, raw_igc);
    assert_eq!(fetched.raw_igc_hash, raw_igc_hash.to_string());
    assert_eq!(
        fetched.artifact_class,
        ProtoArtifactClass::PrivateRawIgc as i32
    );

    let revoked = client
        .revoke_private_access(RevokePrivateAccessRequest {
            pilot_id: pilot_id.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(revoked.pilot_id, pilot_id.to_string());
    assert!(revoked.key_deleted);
    assert!(revoked.restricted_plaintext_deleted);
    assert!(revoked.tombstone_retained);

    let index = client
        .query_index(QueryIndexRequest {
            page_size: 10,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let entry = index
        .entries
        .iter()
        .find(|entry| entry.raw_igc_hash == raw_igc_hash.to_string())
        .unwrap();
    assert!(entry.locally_available_artifact_classes.is_empty());
    assert!(!entry.locally_fetchable);

    let proof = igc_net::sign_fetch_proof(
        raw_igc_hash.as_str(),
        igc_net::ArtifactClass::PrivateRawIgc,
        2,
        &private_access_key,
    )
    .unwrap();
    let after_revoke = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: raw_igc_hash.to_string(),
            artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
            requester_key: proof.requester_key,
            seq_num: proof.seq_num,
            signature: hex::decode(proof.signature).unwrap(),
            group_fetch_proof: None,
        })
        .await
        .unwrap_err();
    assert_eq!(after_revoke.code(), tonic::Code::NotFound);

    process.stop();
}

#[tokio::test]
async fn process_grpc_private_publish_requires_key_and_serves_after_approval() {
    let _guard = process_test_lock().lock().await;
    let data_dir = tempfile::tempdir().unwrap();
    let raw_igc = b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n";
    let private_access_key = secret_key(111);

    let seed_node = igc_net::IgcIrohNode::start(data_dir.path()).await.unwrap();
    let identity = seed_node
        .generate_pilot_identity("Test Pilot", None)
        .unwrap();
    let pilot_id = identity.pilot_id();
    let rotation = igc_net::PrivateAccessRotationRecord::issue(
        &identity.pilot_id_secret_key(),
        private_access_key.public(),
        None,
        "2026-05-01T09:14:00Z",
    )
    .unwrap();
    seed_node
        .governance_store()
        .persist_private_access_rotation_record(&rotation)
        .unwrap();
    seed_node.close().await;
    drop(seed_node);
    sleep(Duration::from_millis(250)).await;

    let process = GrpcProcess::start(data_dir.path());
    let mut client = process.client().await;

    client
        .provision_private_access_key(ProvisionPrivateAccessKeyRequest {
            pilot_id: pilot_id.to_string(),
            private_access_secret_key: private_access_key.to_bytes().to_vec(),
            expected_private_access_public_key: private_access_key.public().to_string(),
        })
        .await
        .unwrap();

    let published = client
        .publish_flight(PublishFlightRequest {
            raw_igc: raw_igc.to_vec(),
            filename: "private.igc".to_string(),
            publication_mode: PublicationMode::Private as i32,
            pilot_id: pilot_id.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(published.raw_igc_hash.len(), 64);
    assert_eq!(published.artifacts.len(), 1);
    let private_artifact =
        published_response_artifact(&published, ProtoArtifactClass::PrivateRawIgc);
    assert_eq!(private_artifact.artifact_hash, published.raw_igc_hash);
    assert!(!private_artifact.ticket.is_empty());

    let proof = igc_net::sign_fetch_proof(
        &published.raw_igc_hash,
        igc_net::ArtifactClass::PrivateRawIgc,
        1,
        &private_access_key,
    )
    .unwrap();
    let pending = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: published.raw_igc_hash.clone(),
            artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
            requester_key: proof.requester_key,
            seq_num: proof.seq_num,
            signature: hex::decode(proof.signature).unwrap(),
            group_fetch_proof: None,
        })
        .await
        .unwrap_err();
    assert_eq!(pending.code(), tonic::Code::FailedPrecondition);

    let raw_hash = igc_net::Blake3Hex::parse(published.raw_igc_hash.clone()).unwrap();
    let governance = igc_net::GovernanceStore::for_data_dir(data_dir.path());
    let claim = governance
        .load_owner_claim_records(&raw_hash)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let resolver = secret_key(112);
    let approval = igc_net::ClaimApprovalRecord::issue(
        &resolver,
        claim.record_id,
        raw_hash.clone(),
        "2026-05-01T10:14:00Z",
    )
    .unwrap();
    governance.trust_resolver(&approval.resolver_id).unwrap();
    governance.persist_claim_approval_record(&approval).unwrap();

    let proof = igc_net::sign_fetch_proof(
        &published.raw_igc_hash,
        igc_net::ArtifactClass::PrivateRawIgc,
        1,
        &private_access_key,
    )
    .unwrap();
    let fetched = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: published.raw_igc_hash,
            artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
            requester_key: proof.requester_key,
            seq_num: proof.seq_num,
            signature: hex::decode(proof.signature).unwrap(),
            group_fetch_proof: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.artifact_bytes, raw_igc);

    process.stop();
}

#[tokio::test]
async fn process_grpc_private_fetch_refuses_missing_key_and_blocked_governance() {
    let _guard = process_test_lock().lock().await;
    let data_dir = tempfile::tempdir().unwrap();
    let private_access_key = secret_key(121);

    let seed_node = igc_net::IgcIrohNode::start(data_dir.path()).await.unwrap();
    let identity = seed_node
        .generate_pilot_identity("Test Pilot", None)
        .unwrap();
    let pilot_id = identity.pilot_id();
    let rotation = igc_net::PrivateAccessRotationRecord::issue(
        &identity.pilot_id_secret_key(),
        private_access_key.public(),
        None,
        "2026-05-01T09:14:00Z",
    )
    .unwrap();
    seed_node
        .governance_store()
        .persist_private_access_rotation_record(&rotation)
        .unwrap();

    let missing_key_hash = seed_private_artifact(
        &seed_node,
        b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLMISSINGKEY\r\n",
        &pilot_id,
        igc_net::FlightGovernanceStatus::Approved,
    )
    .await;
    let blocked_hashes = vec![
        seed_private_artifact(
            &seed_node,
            b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLCONTESTED\r\n",
            &pilot_id,
            igc_net::FlightGovernanceStatus::Contested,
        )
        .await,
        seed_private_artifact(
            &seed_node,
            b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLREJECTED\r\n",
            &pilot_id,
            igc_net::FlightGovernanceStatus::Rejected,
        )
        .await,
        seed_private_artifact(
            &seed_node,
            b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLREVOKED\r\n",
            &pilot_id,
            igc_net::FlightGovernanceStatus::Revoked,
        )
        .await,
        seed_private_artifact(
            &seed_node,
            b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLDELETED\r\n",
            &pilot_id,
            igc_net::FlightGovernanceStatus::Deleted,
        )
        .await,
    ];
    seed_node.close().await;
    drop(seed_node);
    sleep(Duration::from_millis(250)).await;

    let process = GrpcProcess::start(data_dir.path());
    let mut client = process.client().await;

    let proof = igc_net::sign_fetch_proof(
        missing_key_hash.as_str(),
        igc_net::ArtifactClass::PrivateRawIgc,
        1,
        &private_access_key,
    )
    .unwrap();
    let missing_key = client
        .fetch_artifact(FetchArtifactRequest {
            raw_igc_hash: missing_key_hash.to_string(),
            artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
            requester_key: proof.requester_key,
            seq_num: proof.seq_num,
            signature: hex::decode(proof.signature).unwrap(),
            group_fetch_proof: None,
        })
        .await
        .unwrap_err();
    assert_eq!(missing_key.code(), tonic::Code::Unauthenticated);

    client
        .provision_private_access_key(ProvisionPrivateAccessKeyRequest {
            pilot_id: pilot_id.to_string(),
            private_access_secret_key: private_access_key.to_bytes().to_vec(),
            expected_private_access_public_key: private_access_key.public().to_string(),
        })
        .await
        .unwrap();

    for raw_igc_hash in blocked_hashes {
        let proof = igc_net::sign_fetch_proof(
            raw_igc_hash.as_str(),
            igc_net::ArtifactClass::PrivateRawIgc,
            1,
            &private_access_key,
        )
        .unwrap();
        let blocked = client
            .fetch_artifact(FetchArtifactRequest {
                raw_igc_hash: raw_igc_hash.to_string(),
                artifact_class: ProtoArtifactClass::PrivateRawIgc as i32,
                requester_key: proof.requester_key,
                seq_num: proof.seq_num,
                signature: hex::decode(proof.signature).unwrap(),
                group_fetch_proof: None,
            })
            .await
            .unwrap_err();
        assert_eq!(blocked.code(), tonic::Code::PermissionDenied);
    }

    process.stop();
}
