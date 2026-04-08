mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SAMPLE_IGC, init_tracing, wait_for_index_record};
use igc_net::{FetchPolicy, IgcIrohNode, IndexerConfig, publish, run_indexer};

/// Node A publishes a flight in Switzerland (~47.5N, 8.6E). Node B with a
/// Europe filter fetches both blobs; Node C with a South America filter
/// fetches only metadata.
#[tokio::test]
async fn geo_filtered_indexer_fetches_only_matching_region() {
    init_tracing();

    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let dir_c = tempfile::tempdir().unwrap();

    let node_a = Arc::new(IgcIrohNode::start(dir_a.path()).await.unwrap());
    let node_b = Arc::new(IgcIrohNode::start(dir_b.path()).await.unwrap());
    let node_c = Arc::new(IgcIrohNode::start(dir_c.path()).await.unwrap());

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_a.add_peer_addr(node_b.loopback_endpoint_addr().unwrap());
    node_c.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_a.add_peer_addr(node_c.loopback_endpoint_addr().unwrap());

    let node_b_task = Arc::clone(&node_b);
    let bootstrap_b = vec![node_a.iroh_node_id()];
    let indexer_b = tokio::spawn(async move {
        run_indexer(
            &node_b_task,
            IndexerConfig::simple(
                FetchPolicy::GeoFiltered {
                    min_lat: 40.0,
                    max_lat: 55.0,
                    min_lon: 0.0,
                    max_lon: 20.0,
                },
                bootstrap_b,
            ),
        )
        .await
        .ok();
    });

    let node_c_task = Arc::clone(&node_c);
    let bootstrap_c = vec![node_a.iroh_node_id()];
    let indexer_c = tokio::spawn(async move {
        run_indexer(
            &node_c_task,
            IndexerConfig::simple(
                FetchPolicy::GeoFiltered {
                    min_lat: -55.0,
                    max_lat: -10.0,
                    min_lon: -80.0,
                    max_lon: -35.0,
                },
                bootstrap_c,
            ),
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result = publish(&node_a, SAMPLE_IGC.to_vec(), Some("geo.igc"))
        .await
        .unwrap();

    assert!(
        wait_for_index_record(node_b.store(), &result.igc_hash, Duration::from_secs(30)).await,
        "Node B (Europe filter) should receive the announcement"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        node_b.store().contains(&result.igc_hash).unwrap(),
        "Node B (Europe filter) should fetch the raw IGC blob"
    );

    assert!(
        wait_for_index_record(node_c.store(), &result.igc_hash, Duration::from_secs(30)).await,
        "Node C (South America filter) should receive the announcement"
    );
    assert!(
        node_c.store().contains(&result.meta_hash).unwrap(),
        "Node C should have the metadata blob"
    );
    assert!(
        !node_c.store().contains(&result.igc_hash).unwrap(),
        "Node C (South America filter) should NOT fetch the raw IGC blob"
    );

    indexer_b.abort();
    indexer_c.abort();
    node_a.close().await;
    node_b.close().await;
    node_c.close().await;
}
