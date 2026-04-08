#![allow(dead_code)]

use std::time::{Duration, Instant};

use tracing_subscriber::EnvFilter;

/// Minimal valid IGC file with three B-records; used across all tests.
pub const SAMPLE_IGC: &[u8] = b"AXXX001 Test Device\r\n\
    HFDTE020714\r\n\
    HFPLTPILOTINCHARGE:Integration Test Pilot\r\n\
    HFGTYGLIDERTYPE:Test Wing EN-A\r\n\
    HFGIDGLIDERID:TEST-001\r\n\
    B1300004730000N00837000EA0030003000\r\n\
    B1305004732000N00838000EA0050005000\r\n\
    B1310004731000N00839000EA0040004000\r\n";

/// Initialize tracing once for the test binary.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("igc_net=debug,iroh_gossip=debug,iroh=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Poll `store`'s index until at least one record for `igc_hash` appears,
/// or `timeout` elapses. Returns `true` if found.
pub async fn wait_for_index_record(
    store: &igc_net::FlatFileStore,
    igc_hash: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let found = store
            .iter_index()
            .unwrap()
            .filter_map(|r| r.ok())
            .any(|r| r.igc_hash.as_str() == igc_hash);
        if found {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `store`'s index until at least `expected` records for `igc_hash`
/// appear, or `timeout` elapses. Returns `true` if the count was reached.
pub async fn wait_for_index_count(
    store: &igc_net::FlatFileStore,
    igc_hash: &str,
    expected: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let count = store
            .iter_index()
            .unwrap()
            .filter_map(|r| r.ok())
            .filter(|r| r.igc_hash.as_str() == igc_hash)
            .count();
        if count >= expected {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
