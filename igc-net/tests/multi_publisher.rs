mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use common::{SAMPLE_IGC, wait_for_index_count};
use igc_net::{FetchPolicy, IgcIrohNode, IndexRecordSource, IndexerConfig, publish, run_indexer};

/// Two independent nodes (A and B) each publish the same raw IGC bytes.
/// Indexer C must retain one index record per publisher.
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
            IndexerConfig::simple(FetchPolicy::MetadataOnly, bootstrap),
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
    assert_ne!(
        result_a.meta_hash, result_b.meta_hash,
        "distinct publisher_node_ids produce distinct meta blobs"
    );

    assert!(
        wait_for_index_count(
            node_c.store(),
            &result_a.igc_hash,
            2,
            Duration::from_secs(30)
        )
        .await,
        "Node C should have two index records (one per publisher) within the timeout"
    );

    let records: Vec<_> = node_c
        .store()
        .iter_index()
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|r| r.igc_hash == result_a.igc_hash)
        .collect();

    assert_eq!(records.len(), 2, "expected two records for the shared igc_hash");
    assert!(
        records
            .iter()
            .all(|r| r.source == IndexRecordSource::RemoteAnnouncement),
        "all records from remote publishers must be tagged RemoteAnnouncement"
    );

    let node_ids: HashSet<&str> = records.iter().map(|r| r.node_id.as_str()).collect();
    assert_eq!(node_ids.len(), 2, "records must reference two distinct node_ids");

    let meta_hashes: HashSet<&str> = records.iter().map(|r| r.meta_hash.as_str()).collect();
    assert_eq!(
        meta_hashes.len(),
        2,
        "records must reference two distinct meta_hashes"
    );

    assert!(
        node_c.store().contains(&result_a.meta_hash).unwrap(),
        "C must store A's metadata blob"
    );
    assert!(
        node_c.store().contains(&result_b.meta_hash).unwrap(),
        "C must store B's metadata blob"
    );

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
    node_c.close().await;
}
