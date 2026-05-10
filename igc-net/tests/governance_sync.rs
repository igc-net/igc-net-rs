mod common;

use std::time::Duration;

use common::{init_tracing, wait_for_pilot_auth_record};
use igc_net::IgcIrohNode;

#[tokio::test]
async fn pilot_auth_did_sync_round_trips_over_network() {
    init_tracing();
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let node_a = IgcIrohNode::start(dir_a.path()).await.unwrap();
    let node_b = IgcIrohNode::start(dir_b.path()).await.unwrap();

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());

    let pilot_id = node_a
        .generate_pilot_identity("Test Pilot", None)
        .unwrap()
        .pilot_id();
    let initial = node_a
        .issue_initial_registered_pilot_auth_did_record(&pilot_id, "2026-05-01T09:14:00Z")
        .await
        .unwrap();

    let applied_initial = node_b
        .sync_pilot_auth_did_from_peer(node_a.iroh_node_id(), &pilot_id)
        .await
        .unwrap();
    assert_eq!(applied_initial, 1);
    let initial_state = node_b.resolve_pilot_auth_did_state(&pilot_id).unwrap();
    assert_eq!(
        initial_state.authoritative.as_ref().unwrap().record_id,
        initial.record_id
    );

    let rotated = node_a
        .rotate_registered_pilot_auth_did(&pilot_id, "2026-05-01T10:14:00Z")
        .await
        .unwrap();

    let applied_rotation = node_b
        .sync_pilot_auth_did_from_peer(node_a.iroh_node_id(), &pilot_id)
        .await
        .unwrap();
    assert_eq!(applied_rotation, 1);

    let final_state = node_b.resolve_pilot_auth_did_state(&pilot_id).unwrap();
    assert_eq!(
        final_state.authoritative.as_ref().unwrap().record_id,
        rotated.record_id
    );

    let applied_again = node_b
        .sync_pilot_auth_did_from_peer(node_a.iroh_node_id(), &pilot_id)
        .await
        .unwrap();
    assert_eq!(applied_again, 0);

    node_a.close().await;
    node_b.close().await;
}

#[tokio::test]
async fn pilot_auth_did_gossip_propagates_updates_across_peers() {
    init_tracing();
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let dir_c = tempfile::tempdir().unwrap();

    let node_a = IgcIrohNode::start(dir_a.path()).await.unwrap();
    let node_b = IgcIrohNode::start(dir_b.path()).await.unwrap();
    let node_c = IgcIrohNode::start(dir_c.path()).await.unwrap();

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_c.add_peer_addr(node_b.loopback_endpoint_addr().unwrap());
    node_c.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());

    node_b
        .join_pilot_auth_did_gossip_peers(vec![node_a.iroh_node_id()])
        .await
        .unwrap();
    node_c
        .join_pilot_auth_did_gossip_peers(vec![node_b.iroh_node_id(), node_a.iroh_node_id()])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let pilot_id = node_a
        .generate_pilot_identity("Test Pilot", None)
        .unwrap()
        .pilot_id();
    let initial = node_a
        .issue_initial_registered_pilot_auth_did_record(&pilot_id, "2026-05-01T09:14:00Z")
        .await
        .unwrap();

    assert!(
        wait_for_pilot_auth_record(
            &node_b,
            &pilot_id,
            &initial.record_id,
            Duration::from_secs(10)
        )
        .await
    );
    assert!(
        wait_for_pilot_auth_record(
            &node_c,
            &pilot_id,
            &initial.record_id,
            Duration::from_secs(10)
        )
        .await
    );

    let rotated = node_a
        .rotate_registered_pilot_auth_did(&pilot_id, "2026-05-01T10:14:00Z")
        .await
        .unwrap();

    assert!(
        wait_for_pilot_auth_record(
            &node_b,
            &pilot_id,
            &rotated.record_id,
            Duration::from_secs(10)
        )
        .await
    );
    assert!(
        wait_for_pilot_auth_record(
            &node_c,
            &pilot_id,
            &rotated.record_id,
            Duration::from_secs(10)
        )
        .await
    );

    node_a.close().await;
    node_b.close().await;
    node_c.close().await;
}

#[tokio::test]
async fn governance_topic_gossip_applies_full_pilot_auth_records() {
    init_tracing();
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let node_a = IgcIrohNode::start(dir_a.path()).await.unwrap();
    let node_b = IgcIrohNode::start(dir_b.path()).await.unwrap();

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_b
        .join_governance_gossip_peers(vec![node_a.iroh_node_id()])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let pilot_id = node_a
        .generate_pilot_identity("Test Pilot", None)
        .unwrap()
        .pilot_id();
    let initial = node_a
        .issue_initial_registered_pilot_auth_did_record(&pilot_id, "2026-05-01T09:14:00Z")
        .await
        .unwrap();

    assert!(
        wait_for_pilot_auth_record(
            &node_b,
            &pilot_id,
            &initial.record_id,
            Duration::from_secs(10)
        )
        .await
    );

    node_a.close().await;
    node_b.close().await;
}
