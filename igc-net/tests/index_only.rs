mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SAMPLE_IGC, init_tracing, wait_for_artifact_registry_record};
use igc_net::{FetchPolicy, IgcIrohNode, IndexerConfig, PublicationMode, publish, run_indexer};

/// Node A publishes an IGC file; Node B records the artifact announcement
/// without fetching raw IGC bytes.
#[tokio::test]
async fn publisher_and_indexer_exchange_artifact_announcement_on_loopback() {
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
            IndexerConfig::simple(FetchPolicy::IndexOnly, bootstrap),
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result = publish(&node_a, SAMPLE_IGC.to_vec(), Some("sample.igc"))
        .await
        .unwrap();

    assert!(
        wait_for_artifact_registry_record(
            node_b.store(),
            &result.igc_hash,
            Duration::from_secs(30)
        )
        .await,
        "Node B did not receive the announcement within the timeout"
    );

    assert!(
        !node_b.store().contains(&result.igc_hash).unwrap(),
        "raw IGC blob must not be fetched under IndexOnly policy"
    );

    let record = node_b
        .store()
        .artifact_registry_record(&result.igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(record.publication_mode, PublicationMode::Public);
    assert_eq!(record.raw_igc_hash, result.igc_hash);
    assert!(!record.has_raw_igc);

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
}
