mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SAMPLE_IGC, init_tracing, wait_for_index_record};
use igc_net::{FetchPolicy, IgcIrohNode, IndexRecordSource, IndexerConfig, publish, run_indexer};

/// Node A publishes an IGC file; Node B (MetadataOnly indexer) receives the
/// announcement, fetches and verifies the metadata blob, and does NOT fetch
/// the raw IGC.
#[tokio::test]
async fn publisher_and_indexer_exchange_metadata_on_loopback() {
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

    let result = publish(&node_a, SAMPLE_IGC.to_vec(), Some("sample.igc"))
        .await
        .unwrap();

    assert!(
        wait_for_index_record(node_b.store(), &result.igc_hash, Duration::from_secs(30)).await,
        "Node B did not receive the announcement within the timeout"
    );

    let meta_bytes = node_b.store().get(&result.meta_hash).await.unwrap();
    assert!(
        meta_bytes.is_some(),
        "metadata blob should be in Node B's store after indexing"
    );

    assert!(
        !node_b.store().contains(&result.igc_hash).unwrap(),
        "raw IGC blob must not be fetched under MetadataOnly policy"
    );

    let records: Vec<_> = node_b
        .store()
        .iter_index()
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert_eq!(records.len(), 1, "expected exactly one index record on B");
    assert_eq!(
        records[0].source,
        IndexRecordSource::RemoteAnnouncement,
        "source must be RemoteAnnouncement"
    );
    assert_eq!(records[0].igc_hash, result.igc_hash);
    assert_eq!(records[0].meta_hash, result.meta_hash);

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
}
