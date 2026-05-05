mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SAMPLE_IGC, wait_for_artifact_registry_record, wait_for_index_record};
use igc_net::{
    FetchPolicy, IgcIrohNode, IndexerConfig, PublicationMode, publish, publish_private,
    publish_protected, run_indexer, sanitize_protected_igc,
};

/// Node A publishes; Node B (Eager indexer) fetches both the metadata and the
/// raw IGC blob and verifies content integrity.
#[tokio::test]
async fn eager_indexer_fetches_raw_igc_blob() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let node_a = Arc::new(IgcIrohNode::start(dir_a.path()).await.unwrap());
    let node_b = Arc::new(IgcIrohNode::start(dir_b.path()).await.unwrap());

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_a.add_peer_addr(node_b.loopback_endpoint_addr().unwrap());

    let node_b_task = Arc::clone(&node_b);
    let bootstrap = vec![node_a.iroh_node_id()];
    let indexer = tokio::spawn(async move {
        run_indexer(
            &node_b_task,
            IndexerConfig::simple(FetchPolicy::Eager, bootstrap),
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result = publish(&node_a, SAMPLE_IGC.to_vec(), Some("eager.igc"))
        .await
        .unwrap();

    assert!(
        wait_for_index_record(node_b.store(), &result.igc_hash, Duration::from_secs(30)).await,
        "Node B did not receive the announcement within the timeout"
    );

    assert!(
        node_b.store().contains(&result.meta_hash).unwrap(),
        "metadata blob must be present after Eager indexing"
    );
    assert!(
        node_b.store().contains(&result.igc_hash).unwrap(),
        "raw IGC blob must be present after Eager indexing"
    );

    let fetched = node_b
        .store()
        .get(&result.igc_hash)
        .await
        .unwrap()
        .expect("raw IGC missing from store");
    assert_eq!(
        fetched, SAMPLE_IGC,
        "fetched IGC bytes must match the original"
    );

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
}

#[tokio::test]
async fn eager_indexer_accepts_v03_protected_announcement_without_raw_leakage() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let node_a = Arc::new(IgcIrohNode::start(dir_a.path()).await.unwrap());
    let node_b = Arc::new(IgcIrohNode::start(dir_b.path()).await.unwrap());

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_a.add_peer_addr(node_b.loopback_endpoint_addr().unwrap());

    let node_b_task = Arc::clone(&node_b);
    let bootstrap = vec![node_a.iroh_node_id()];
    let indexer = tokio::spawn(async move {
        run_indexer(
            &node_b_task,
            IndexerConfig::simple(FetchPolicy::Eager, bootstrap),
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result = publish_protected(&node_a, SAMPLE_IGC.to_vec())
        .await
        .unwrap();

    assert!(
        wait_for_artifact_registry_record(
            node_b.store(),
            &result.raw_igc_hash,
            Duration::from_secs(30)
        )
        .await,
        "Node B did not receive the protected artifact announcement within the timeout"
    );

    let record = node_b
        .store()
        .artifact_registry_record(&result.raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(record.publication_mode, PublicationMode::Protected);
    assert_eq!(record.protected_hash, Some(result.protected_hash.clone()));
    assert!(record.has_protected_sanitized_igc);
    assert!(!record.has_raw_igc);
    assert!(!record.has_protected_raw_companion);

    assert!(
        node_b.store().contains(&result.protected_hash).unwrap(),
        "protected sanitized artifact must be present after eager v0.3 indexing"
    );
    assert!(
        !node_b.store().contains(&result.raw_igc_hash).unwrap(),
        "raw companion must not be fetched from a protected announcement"
    );

    let sanitized = node_b
        .store()
        .get(&result.protected_hash)
        .await
        .unwrap()
        .expect("sanitized artifact missing from store");
    assert_eq!(sanitized, sanitize_protected_igc(SAMPLE_IGC));

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
}

#[tokio::test]
async fn eager_indexer_accepts_v03_private_announcement_without_fetching_raw() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let node_a = Arc::new(IgcIrohNode::start(dir_a.path()).await.unwrap());
    let node_b = Arc::new(IgcIrohNode::start(dir_b.path()).await.unwrap());

    node_b.add_peer_addr(node_a.loopback_endpoint_addr().unwrap());
    node_a.add_peer_addr(node_b.loopback_endpoint_addr().unwrap());

    let node_b_task = Arc::clone(&node_b);
    let bootstrap = vec![node_a.iroh_node_id()];
    let indexer = tokio::spawn(async move {
        run_indexer(
            &node_b_task,
            IndexerConfig::simple(FetchPolicy::Eager, bootstrap),
        )
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result = publish_private(&node_a, SAMPLE_IGC.to_vec()).await.unwrap();

    assert!(
        wait_for_artifact_registry_record(
            node_b.store(),
            &result.raw_igc_hash,
            Duration::from_secs(30)
        )
        .await,
        "Node B did not receive the private artifact announcement within the timeout"
    );

    let record = node_b
        .store()
        .artifact_registry_record(&result.raw_igc_hash)
        .unwrap()
        .unwrap();
    assert_eq!(record.publication_mode, PublicationMode::Private);
    assert!(!record.has_raw_igc);
    assert!(!record.has_protected_sanitized_igc);
    assert!(!record.has_protected_raw_companion);
    assert!(
        !node_b.store().contains(&result.raw_igc_hash).unwrap(),
        "private raw IGC must not be fetched from an announcement"
    );

    indexer.abort();
    node_a.close().await;
    node_b.close().await;
}
