//! Replay only on a disposable database, never on the running routing store.
use prefrontal_routing_core::{RouteStateStore, UsageObservation};
use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};
use rusqlite::Connection;

#[cfg(target_os = "macos")]
fn disk_bytes() -> (u64, u64) {
    unsafe extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, info: *mut u64) -> i32;
    }
    let mut info = [0_u64; 20]; // rusage_info_v2, including its 16-byte UUID.
    assert_eq!(
        unsafe { proc_pid_rusage(std::process::id() as i32, 2, info.as_mut_ptr()) },
        0
    );
    (info[18], info[19])
}

#[cfg(not(target_os = "macos"))]
fn disk_bytes() -> (u64, u64) {
    (0, 0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .expect("disposable database under target/");
    assert!(
        path.starts_with("target/"),
        "replay must use a disposable target/ database"
    );
    let store = SqliteRouteStateStore::open(&dev_descriptor("routing-io-replay", &path))?;
    let conn = Connection::open(&path)?;
    let now = 1_800_000_000_000_f64;
    let observations: Vec<_> = (0..1000)
        .map(|i| UsageObservation {
            id: format!("io-replay-{i}"),
            route_key: "replay/model".into(),
            capacity_key: "replay/model".into(),
            provider_id: "replay".into(),
            input_tokens: Some(1000.0),
            output_tokens: 500.0,
            duration_ms: 1000.0,
            observed_at: now,
            reasoning_tokens: Some(100.0),
            cached_read_tokens: Some(100.0),
            cache_write_tokens: Some(100.0),
            workload_class: Some("ephemeral".into()),
            account_key: Some("replay/account".into()),
        })
        .collect();
    for obs in &observations {
        store.record_usage_delivery(obs)?;
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let before = disk_bytes();
    let started = std::time::Instant::now();
    for _ in 0..10 {
        for obs in &observations {
            let result = store.record_usage_delivery(obs)?;
            assert!(!result.recorded && !result.usage_recorded);
        }
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let after = disk_bytes();
    println!(
        "usage duplicate replay: 10000 deliveries, {:.3}s, read={} B written={} B",
        started.elapsed().as_secs_f64(),
        after.0 - before.0,
        after.1 - before.1
    );
    Ok(())
}
