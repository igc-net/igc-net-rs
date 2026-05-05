mod common;

use igc_net::{
    ArtifactClass, ArtifactRegistryRecord, Blake3Hex, ClaimApprovalRecord, ClaimChallengeRecord,
    ClaimResolutionOutcome, ClaimResolutionRecord, DeletionRequestRecord, FetchProofError,
    FlatFileStore, FlightGovernanceState, FlightGovernanceStatus, GovernanceStore, NodeIdHex,
    OwnerClaimRecord, PilotId, PrivateAccessKeyStore, PublicationMode, PublicationModeRecord,
    ResolverProfile, RosterUpdateAction, RosterUpdateRecord, SeqNumStore, sanitize_protected_igc,
    sign_fetch_proof, verify_fetch_proof,
};

fn secret_key(byte: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[byte; 32])
}

fn hash(byte: u8) -> Blake3Hex {
    Blake3Hex::parse(format!("{byte:02x}").repeat(32)).unwrap()
}

fn node_id(byte: u8) -> NodeIdHex {
    NodeIdHex::from_public_key(secret_key(byte).public())
}

fn resolver_profile() -> ResolverProfile {
    ResolverProfile {
        display_name: "fixture resolver".to_string(),
        service_url: "https://resolver.example.org".to_string(),
        privacy_policy_url: "https://resolver.example.org/privacy".to_string(),
        public_key_url: "https://resolver.example.org/.well-known/igc-net-resolver-key".to_string(),
    }
}

#[test]
fn access_fixture_rejects_wrong_artifact_class_and_seq_replay() {
    // Covers R-ACCESS-08, R-ACCESS-09, R-ACCESS-10, R-ACCESS-11, R-ACCESS-28.
    let private_access_key = secret_key(1);
    let proof = sign_fetch_proof(
        hash(0xaa).as_str(),
        ArtifactClass::PrivateRawIgc,
        7,
        &private_access_key,
    )
    .unwrap();

    let wrong_class = verify_fetch_proof(
        &proof,
        &private_access_key.public(),
        &ArtifactClass::ProtectedRawCompanion,
        0,
    )
    .unwrap_err();
    assert!(matches!(
        wrong_class,
        FetchProofError::ArtifactClassMismatch
    ));

    let replay = verify_fetch_proof(
        &proof,
        &private_access_key.public(),
        &ArtifactClass::PrivateRawIgc,
        7,
    )
    .unwrap_err();
    assert!(matches!(replay, FetchProofError::SeqNumNotMonotonic { .. }));
}

#[test]
fn seq_num_fixture_persists_monotonic_replay_boundary() {
    // Covers R-ACCESS-11 and R-ACCESS-13.
    let dir = tempfile::tempdir().unwrap();
    let store = SeqNumStore::for_data_dir(dir.path());
    let requester_key = secret_key(2).public().to_string();

    assert_eq!(store.last_seen(&requester_key).unwrap(), 0);
    store.advance(&requester_key, 3).unwrap();
    assert_eq!(store.last_seen(&requester_key).unwrap(), 3);
    assert!(store.advance(&requester_key, 3).is_err());

    let reopened = SeqNumStore::for_data_dir(dir.path());
    assert_eq!(reopened.last_seen(&requester_key).unwrap(), 3);
}

#[test]
fn governance_fixture_deletion_overrides_approved_owner_state() {
    // Covers R-GOV-07, R-GOV-08, R-TRANS-26, R-DUR-01, R-DUR-02.
    let dir = tempfile::tempdir().unwrap();
    let store = GovernanceStore::for_data_dir(dir.path());
    let pilot_root = secret_key(3);
    let owner = PilotId::from_public_key(pilot_root.public());
    let raw_igc_hash = hash(0xbb);

    store
        .persist_flight_governance_state(&FlightGovernanceState::approved_owner(
            raw_igc_hash.clone(),
            owner.clone(),
            "2026-05-01T09:14:00Z",
        ))
        .unwrap();

    let deletion =
        DeletionRequestRecord::issue(&pilot_root, raw_igc_hash.clone(), "2026-05-01T10:14:00Z")
            .unwrap();
    store.persist_deletion_request_record(&deletion).unwrap();

    let resolved = store
        .resolve_flight_governance_state(&raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.owner_pilot_id, Some(owner));
    assert_eq!(resolved.status, FlightGovernanceStatus::Deleted);
    assert!(resolved.serving_blocked());
}

#[test]
fn governance_fixture_restricted_serving_requires_approved_owner_baseline() {
    // Covers R-GSYNC-08, R-GSYNC-09, R-GSYNC-18, R-ACCESS-07.
    let owner = PilotId::from_public_key(secret_key(4).public());
    let raw_igc_hash = hash(0xcc);

    let approved = FlightGovernanceState::approved_owner(
        raw_igc_hash.clone(),
        owner.clone(),
        "2026-05-01T09:14:00Z",
    );
    assert!(approved.restricted_serving_ready_for(&owner));
    assert!(!approved.serving_blocked());

    let contested = FlightGovernanceState {
        raw_igc_hash,
        owner_pilot_id: Some(owner.clone()),
        status: FlightGovernanceStatus::Contested,
        baseline_ready: true,
        recorded_at: "2026-05-01T10:14:00Z".to_string(),
    };
    assert!(contested.serving_blocked());
    assert!(!contested.restricted_serving_ready_for(&owner));

    let stale = FlightGovernanceState {
        baseline_ready: false,
        ..approved
    };
    assert!(!stale.restricted_serving_ready_for(&owner));
}

#[test]
fn governance_fixture_trusted_resolver_records_drive_serving_state() {
    // Covers R-GOV-01, R-GOV-02, R-GOV-04, R-GOV-18, R-GOV-21, R-GOV-22.
    let dir = tempfile::tempdir().unwrap();
    let store = GovernanceStore::for_data_dir(dir.path());
    let pilot_root = secret_key(10);
    let bootstrap_resolver = secret_key(11);
    let resolver = secret_key(12);
    let raw_igc_hash = hash(0xdd);
    let claim = OwnerClaimRecord::issue(
        &pilot_root,
        raw_igc_hash.clone(),
        "2026-05-01T09:14:00Z",
        Vec::new(),
    )
    .unwrap();
    let approval = ClaimApprovalRecord::issue(
        &resolver,
        claim.record_id.clone(),
        raw_igc_hash.clone(),
        "2026-05-01T10:14:00Z",
    )
    .unwrap();
    let roster_add = RosterUpdateRecord::issue(
        &bootstrap_resolver,
        RosterUpdateAction::Add,
        resolver.public().to_string(),
        Some(resolver_profile()),
        "2026-05-01T09:30:00Z",
    )
    .unwrap();

    store.persist_owner_claim_record(&claim).unwrap();
    store.persist_claim_approval_record(&approval).unwrap();

    let untrusted = store
        .resolve_flight_governance_state(&raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(untrusted.status, FlightGovernanceStatus::Pending);

    store
        .trust_resolver(&bootstrap_resolver.public().to_string())
        .unwrap();
    store.persist_roster_update_record(&roster_add).unwrap();
    let approved = store
        .resolve_flight_governance_state(&raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(approved.status, FlightGovernanceStatus::Approved);

    let challenge = ClaimChallengeRecord::issue(
        &resolver,
        claim.record_id.clone(),
        raw_igc_hash.clone(),
        "ownership_dispute",
        "2026-05-01T11:14:00Z",
    )
    .unwrap();
    store.persist_claim_challenge_record(&challenge).unwrap();
    let contested = store
        .resolve_flight_governance_state(&raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(contested.status, FlightGovernanceStatus::Contested);
    assert!(contested.serving_blocked());

    let resolution = ClaimResolutionRecord::issue(
        &resolver,
        raw_igc_hash.clone(),
        claim.record_id.clone(),
        ClaimResolutionOutcome::Rejected,
        vec!["manual_review".to_string()],
        Vec::new(),
        "2026-05-01T12:14:00Z",
    )
    .unwrap();
    store.persist_claim_resolution_record(&resolution).unwrap();
    let rejected = store
        .resolve_flight_governance_state(&raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(rejected.status, FlightGovernanceStatus::Rejected);
    assert!(rejected.serving_blocked());
}

#[test]
fn publication_mode_fixture_requires_hash_and_complete_chain() {
    // Covers R-ART-01, R-GOV-06, R-GOV-12, R-GOV-13.
    let dir = tempfile::tempdir().unwrap();
    let store = GovernanceStore::for_data_dir(dir.path());
    let pilot_root = secret_key(13);
    let raw_igc_hash = hash(0xee);
    let protected_hash = hash(0xef);

    let missing_protected_hash = PublicationModeRecord::issue(
        &pilot_root,
        raw_igc_hash.clone(),
        PublicationMode::Protected,
        None,
        None,
        "2026-05-01T09:14:00Z",
    )
    .unwrap_err();
    assert!(matches!(
        missing_protected_hash,
        igc_net::FlightGovernanceRecordError::ProtectedHashPresence
    ));

    let public = PublicationModeRecord::issue(
        &pilot_root,
        raw_igc_hash.clone(),
        PublicationMode::Public,
        None,
        None,
        "2026-05-01T09:14:00Z",
    )
    .unwrap();
    let protected = PublicationModeRecord::issue(
        &pilot_root,
        raw_igc_hash.clone(),
        PublicationMode::Protected,
        Some(protected_hash.clone()),
        Some(public.record_id.clone()),
        "2026-05-01T10:14:00Z",
    )
    .unwrap();
    let stale_incomplete = PublicationModeRecord::issue(
        &pilot_root,
        raw_igc_hash.clone(),
        PublicationMode::Private,
        None,
        Some(hash(0xf0)),
        "2026-05-01T11:14:00Z",
    )
    .unwrap();

    store.persist_publication_mode_record(&public).unwrap();
    store.persist_publication_mode_record(&protected).unwrap();
    store
        .persist_publication_mode_record(&stale_incomplete)
        .unwrap();

    let resolved = store
        .resolve_publication_mode_record(&raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.publication_mode, PublicationMode::Protected);
    assert_eq!(resolved.protected_hash, Some(protected_hash));
    assert_eq!(resolved.supersedes, Some(public.record_id));
}

#[test]
fn protected_sanitization_fixture_is_byte_stable() {
    // Covers R-ART-01, R-ART-02, R-ART-20, R-ART-21.
    let raw = b"HFPLTPILOT:Alice\r\nHFCIDCOMPETITION:ABC\nHFGIDGLIDER:XYZ\r\nHFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLXXXHFPLTKEEP\r\n";
    let expected = b"HFPLT:REDACTED\r\nHFCID:REDACTED\nHFGID:REDACTED\r\nHFDTE020714\r\nB1300004730000N00837000EA0030003000\r\nLXXXHFPLTKEEP\r\n";

    let sanitized = sanitize_protected_igc(raw);

    assert_eq!(sanitized, expected);
    assert_eq!(
        raw.iter().filter(|byte| **byte == b'\n').count(),
        sanitized.iter().filter(|byte| **byte == b'\n').count()
    );
    assert_eq!(
        Blake3Hex::from_hash(blake3::hash(&sanitized)),
        Blake3Hex::from_hash(blake3::hash(expected))
    );
}

#[test]
fn key_custody_fixture_is_partitioned_by_pilot_id() {
    // Covers R-ACCESS-16, R-ACCESS-17 and the v0.3 multi-pilot sidecar rule.
    let dir = tempfile::tempdir().unwrap();
    let store = PrivateAccessKeyStore::for_data_dir(dir.path());
    let node_key = secret_key(5);
    let pilot_a = PilotId::from_public_key(secret_key(6).public());
    let pilot_b = PilotId::from_public_key(secret_key(7).public());
    let key_a = secret_key(8);
    let key_b = secret_key(9);

    store
        .provision_for_pilot(&pilot_a, &key_a, &node_key)
        .unwrap();
    store
        .provision_for_pilot(&pilot_b, &key_b, &node_key)
        .unwrap();
    store.delete_for_pilot(&pilot_a).unwrap();

    assert!(store.load_for_pilot(&pilot_a, &node_key).unwrap().is_none());
    let loaded_b = store
        .load_for_pilot(&pilot_b, &node_key)
        .unwrap()
        .expect("pilot B key must survive pilot A revocation");
    assert_eq!(loaded_b.to_bytes(), key_b.to_bytes());
}

#[tokio::test]
async fn durability_fixture_restricted_plaintext_delete_is_idempotent() {
    // Covers R-ACCESS-17 and R-DUR-12.
    let dir = tempfile::tempdir().unwrap();
    let store = FlatFileStore::open(dir.path());
    store.init().await.unwrap();
    let raw_hash = store
        .put(b"HFDTE020714\r\nB1300004730000N00837000EA0030003000\r\n")
        .await
        .unwrap();

    assert!(store.contains(&raw_hash).unwrap());
    assert!(store.delete_blob(&raw_hash).await.unwrap());
    assert!(!store.contains(&raw_hash).unwrap());
    assert!(store.get(&raw_hash).await.unwrap().is_none());
    assert!(!store.delete_blob(&raw_hash).await.unwrap());
}

#[tokio::test]
async fn transport_fixture_tombstone_updates_event_cursor_and_local_availability() {
    // Covers R-TRANS-29, R-TRANS-30, R-TRANS-31, R-TRANS-32, R-TRANS-33,
    // R-ACCESS-22, and R-DUR-13.
    let dir = tempfile::tempdir().unwrap();
    let store = FlatFileStore::open(dir.path());
    store.init().await.unwrap();
    let raw_igc_hash = hash(0x31);
    let serving_node = node_id(0x32);
    let initial = ArtifactRegistryRecord {
        raw_igc_hash: raw_igc_hash.clone(),
        pilot_id: Some(PilotId::from_public_key(secret_key(0x33).public())),
        publication_mode: PublicationMode::Private,
        protected_hash: None,
        has_raw_igc: true,
        has_protected_sanitized_igc: false,
        has_protected_raw_companion: false,
        serving_node_ids: vec![serving_node],
        recorded_at: "2026-05-01T09:14:00Z".to_string(),
    };
    let tombstone = ArtifactRegistryRecord {
        has_raw_igc: false,
        serving_node_ids: Vec::new(),
        recorded_at: "2026-05-01T10:14:00Z".to_string(),
        ..initial.clone()
    };

    store
        .append_artifact_registry_record(&initial)
        .await
        .unwrap();
    store
        .append_artifact_registry_record(&tombstone)
        .await
        .unwrap();

    assert_eq!(store.latest_artifact_registry_event_seq().unwrap(), 1);
    assert_eq!(
        store
            .latest_artifact_registry_event_seq_for(&raw_igc_hash)
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        store.artifact_registry_events_since(1).unwrap(),
        vec![(1, tombstone.clone())]
    );
    let latest = store
        .artifact_registry_record(&raw_igc_hash)
        .unwrap()
        .unwrap();
    assert!(!latest.has_raw_igc);
    assert!(latest.serving_node_ids.is_empty());
}
