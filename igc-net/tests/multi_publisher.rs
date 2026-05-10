mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use common::{SAMPLE_IGC, wait_for_artifact_registry_record};
use igc_net::{FetchPolicy, IgcIrohNode, IndexerConfig, publish, run_indexer};

/// Two independent nodes (A and B) each publish the same raw IGC bytes.
/// Indexer C must retain both serving nodes in the artifact registry.
#[tokio::test]
async fn two_publishers_of_same_igc_create_separate_index_records() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let dir_c = tempfile::tempdir().unwrap();

    let node_a = Arc::new(IgcIrohNode::start(dir_a.path()).await.unwrap());
    let node_b = Arc::new(IgcIrohNode::start(dir_b.path()).await.unwrap());
    let node_c = Arc::new(IgcIrohNode::start(dir_c.path()).await.unwrap());

    node_c.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_c.add_peer_addr(node_b.loopback_endpoint_addr().unwrap());
    node_a.add_peer_addr(node_c.loopback_endpoint_addr().unwrap());
    node_b.add_peer_addr(node_c.loopback_endpoint_addr().unwrap());

    let node_c_task = Arc::clone(&node_c);
    let bootstrap = vec![node_a.iroh_node_id(), node_b.iroh_node_id()];
    let indexer = tokio::spawn(async move {
        run_indexer(
            &node_c_task,
            IndexerConfig::simple(FetchPolicy::IndexOnly, bootstrap),
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result_a = publish(&node_a, SAMPLE_IGC.to_vec(), Some("flight_a.igc"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result_b = publish(&node_b, SAMPLE_IGC.to_vec(), Some("flight_b.igc"))
        .await
        .unwrap();

    assert_eq!(
        result_a.igc_hash, result_b.igc_hash,
        "both nodes published identical bytes — igc_hash must match"
    );
    assert!(
        wait_for_artifact_registry_record(
            node_c.store(),
            &result_a.igc_hash,
            Duration::from_secs(30)
        )
        .await,
        "Node C should have the artifact registry record within the timeout"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let record = loop {
        let record = node_c
            .store()
            .artifact_registry_record(&result_a.igc_hash)
            .unwrap()
            .unwrap();
        if record.serving_node_ids.len() == 2 || tokio::time::Instant::now() >= deadline {
            break record;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let node_ids: HashSet<&str> = record
        .serving_node_ids
        .iter()
        .map(|node_id| node_id.as_str())
        .collect();
    assert_eq!(
        node_ids.len(),
        2,
        "artifact registry must reference two distinct serving node_ids"
    );

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
    node_c.close().await;
}
