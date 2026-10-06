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
    assert!(
        std::path::Path::new(&path)
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_))),
        "replay path must not escape target/"
    );
    let history = std::env::args().nth(2).as_deref() == Some("history");
    let started = std::time::Instant::now();
    let store = SqliteRouteStateStore::open(&dev_descriptor("routing-io-replay", &path))?;
    let conn = Connection::open(&path)?;
    if history {
        println!(
            "store migration/open: {:.3}s",
            started.elapsed().as_secs_f64()
        );
        let now: i64 =
            conn.query_row("SELECT max(observed_at) FROM model_route_usage", [], |r| {
                r.get(0)
            })?;
        let policy = || -> Result<String, Box<dyn std::error::Error>> {
            Ok(serde_json::to_string(
                &store.get_facts_with_observed_speed(now as f64)?,
            )?)
        };
        let before_policy = policy()?;
        let started = std::time::Instant::now();
        let mut total = (0, 0);
        let mut sweeps = 0;
        loop {
            let removed = store.prune_history(now)?;
            total.0 += removed.0;
            total.1 += removed.1;
            sweeps += 1;
            if removed == (0, 0) {
                break;
            }
        }
        assert_eq!(policy()?, before_policy, "retention changed policy inputs");
        println!("history replay: {sweeps} bounded sweeps, retired={} usage, pruned={} shadows, {:.3}s, policy inputs identical", total.0, total.1, started.elapsed().as_secs_f64());
        // Offline copies only. Production retention reuses freed pages and does
        // not VACUUM; this reports the compacted end-state size, not runtime I/O.
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); VACUUM;")?;
        println!(
            "offline compacted database: {} B",
            std::fs::metadata(&path)?.len()
        );
        return Ok(());
    }
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
