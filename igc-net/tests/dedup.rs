mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SAMPLE_IGC, init_tracing, wait_for_index_record};
use igc_net::{FetchPolicy, IgcIrohNode, IndexerConfig, publish, run_indexer};

/// Publishing the same IGC file twice from the same node should result in
/// only one index record on the indexer.
#[tokio::test]
async fn duplicate_publish_is_deduplicated_on_indexer() {
    init_tracing();

    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let node_a = Arc::new(IgcIrohNode::start(dir_a.path()).await.unwrap());
    let node_b = Arc::new(IgcIrohNode::start(dir_b.path()).await.unwrap());

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());

    let node_b_task = Arc::clone(&node_b);
    let bootstrap = vec![node_a.iroh_node_id()];
    let indexer = tokio::spawn(async move {
        run_indexer(
            &node_b_task,
            IndexerConfig::simple(FetchPolicy::MetadataOnly, bootstrap),
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result1 = publish(&node_a, SAMPLE_IGC.to_vec(), Some("dup.igc"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let result2 = publish(&node_a, SAMPLE_IGC.to_vec(), Some("dup.igc"))
        .await
        .unwrap();

    assert_eq!(result1.igc_hash, result2.igc_hash);
    assert_eq!(result1.meta_hash, result2.meta_hash);

    assert!(
        wait_for_index_record(node_b.store(), &result1.igc_hash, Duration::from_secs(30)).await,
        "Node B should receive at least one announcement"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    let count = node_b
        .store()
        .iter_index()
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|r| r.igc_hash == result1.igc_hash)
        .count();
    assert_eq!(
        count, 1,
        "duplicate announcements from the same (meta_hash, node_id) must be deduplicated"
    );

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
}
