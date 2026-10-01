#![forbid(unsafe_code)]

//! Real-daemon supervision proof: the standalone `subc-core` binary reads a
//! `subc.jsonc`, spawns + supervises `prefrontal-routing-module` as a child process
//! it owns, and routes `route.select` end-to-end.
//!
//! This is the "is it actually a subc module" gate: a real `subc-core` process,
//! launched exactly as a user would, spawns the module from config, injects
//! `SUBC_MODULE_ID`, and supervises the child. We only drive the consumer.
//!
//! Requires both binaries built (the harness builds them): `prefrontal-routing-module`
//! (this workspace) and `subc-core` (the sibling `../subconscious`). `#[ignore]` by
//! default because it shells out to `cargo build` in a sibling repo and binds real
//! loopback ports: run explicitly:
//!   cargo test -p prefrontal-routing-module --test it real_daemon_e2e:: -- --ignored --nocapture

#[path = "../common/mod.rs"]
mod common;

use std::{path::PathBuf, process::Stdio, time::Duration};

use tokio::process::{Child, Command};

use common::{
    connect_consumer, control_rpc, raw_route_frame, route_close, route_open, route_select,
    unique_temp_dir, wait_for_catalog, MODULE_ID, SETUP_TIMEOUT,
};

/// A real `subc-core` daemon process plus its isolated rig dir; killed on drop.
struct RealDaemon {
    #[allow(dead_code)]
    child: Child,
    rig: PathBuf,
    connection_file: PathBuf,
    /// The central storage data_home; the module's store opens under
    /// `<data_home>/cortexkit/<module_id>/store.db`.
    data_home: PathBuf,
}

impl RealDaemon {
    /// The path the module's route-state store should be created at, per the
    /// central storage config + the database-per-module path convention.
    fn expected_store_db(&self, module_id: &str) -> PathBuf {
        self.data_home
            .join("cortexkit")
            .join(module_id)
            .join("store.db")
    }
}

impl Drop for RealDaemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.rig);
    }
}

async fn start_real_daemon() -> RealDaemon {
    let subc_core = prefrontal_test_support::SubcDaemonBinary::build();
    let module_bin = PathBuf::from(env!("CARGO_BIN_EXE_ck-prefrontal-routing"));
    prefrontal_test_support::sign_test_binary(&module_bin);
    // Warm-exec the freshly built module binary too (same assessment stall).
    prefrontal_test_support::warm_exec_fenced(&module_bin);
    assert!(module_bin.exists());

    let rig = unique_temp_dir("prefrontal-routing-real-daemon");
    let config_dir = rig.join("config/cortexkit");
    let runtime_dir = rig.join("runtime");
    let data_home = rig.join("data");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::create_dir_all(&runtime_dir).unwrap();
    std::fs::create_dir_all(&data_home).unwrap();

    // Central storage config: subc resolves a per-module sqlite descriptor under
    // data_home and delivers it in HELLO_ACK.storage. The module opens its
    // route-state store at <data_home>/cortexkit/<module_id>/store.db.
    let subc_jsonc = serde_json::json!({
        "version": 1,
        "storage": { "backend": "sqlite", "data_home": data_home },
        "modules": {
            MODULE_ID: { "program": module_bin, "args": [], "env": {} }
        }
    });
    std::fs::write(
        config_dir.join("subc.jsonc"),
        serde_json::to_vec_pretty(&subc_jsonc).unwrap(),
    )
    .unwrap();

    let child = Command::from(subc_core.command(&rig))
        .env("SUBC_PORT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real subc-core daemon");

    let connection_file = runtime_dir.join("subc-connection.json");
    let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
    while !connection_file.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon did not publish a connection file within {SETUP_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    RealDaemon {
        child,
        rig,
        connection_file,
        data_home,
    }
}

/// The full supervision proof: a real subc-core binary spawns the module from
/// subc.jsonc, it HELLO-registers as a routable ManagementSurface exposing
/// `route.select`, and a real selection routes end-to-end against the seed.
#[tokio::test]
#[ignore = "builds subc-core in ../subconscious and binds loopback ports"]
async fn real_subc_core_supervises_routing_module_and_routes_select() {
    let daemon = start_real_daemon().await;
    let mut consumer = connect_consumer(&daemon.connection_file).await;

    // Registration is asynchronous (the daemon spawns the module); poll the catalog.
    let entry = wait_for_catalog(&mut consumer, MODULE_ID, SETUP_TIMEOUT).await;
    // It must register as a routable provider exposing route.select.
    eprintln!(
        "[real-daemon] catalog entry: {}",
        serde_json::to_string(&entry).unwrap()
    );
    let exposes_select = entry["roles"].as_array().is_some_and(|roles| {
        roles.iter().any(|role| {
            role["operations"]
                .as_array()
                .is_some_and(|ops| ops.iter().any(|op| op["name"] == "route.select"))
        })
    });
    assert!(
        exposes_select,
        "module must expose route.select in its catalog roles: {entry}"
    );

    let health = control_rpc(
        &mut consumer,
        6000,
        serde_json::json!({ "op": "supervisor.health_probe", "module_id": MODULE_ID }),
    )
    .await;
    assert_eq!(health.header.ty, subc_protocol::FrameType::Response);
    let health_body: serde_json::Value = serde_json::from_slice(&health.body).unwrap();
    assert_eq!(health_body["status"], "ok");

    let project_root = unique_temp_dir("prefrontal-routing-real-daemon-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let route_channel = route_open(&mut consumer, &project_root, 1).await;

    // A maximum-demand request may still choose a cheaper near-frontier route,
    // but its request-relative fit must clear the normal adequacy floor.
    let available: Vec<&str> = vec![
        "anthropic/claude-opus-4-8",
        "openai/gpt-5.5",
        "xai/grok-4.3",
        "ollama-cloud/minimax-m2.5",
        "ollama-cloud/deepseek-v4-flash:0731",
    ];
    let params = serde_json::json!({
        "targetAgent": "mason",
        "requirements": { "iq": 100, "eq": 20 },
        "availableModels": available,
    });
    let response = route_select(&mut consumer, route_channel, 2, params).await;
    // route.select is now a mutate that returns { selected, decisionID }.
    let selected = &response["result"]["selected"];
    let model_id = selected["model"]["modelID"]
        .as_str()
        .expect("result.selected.model.modelID");
    eprintln!("[real-daemon] route.select(iq:100) -> {model_id}");
    let adequacy = selected["score"]["adequacy"]
        .as_f64()
        .expect("result.selected.score.adequacy");
    assert!(
        adequacy >= 0.9,
        "iq:100 must clear the adequacy floor, got {model_id} at {adequacy}"
    );
    assert!(
        response["result"]["decisionID"]
            .as_str()
            .is_some_and(|s| s.starts_with("dec_")),
        "route.select must return a recorded decisionID, got {response}"
    );

    // A trivial task must NOT pick the most expensive model (cost-primary).
    let params_low = serde_json::json!({
        "targetAgent": "mason",
        "requirements": { "iq": 20, "eq": 0 },
        "availableModels": available,
    });
    let low = route_select(&mut consumer, route_channel, 3, params_low).await;
    let low_id = low["result"]["selected"]["model"]["modelID"]
        .as_str()
        .unwrap();
    eprintln!("[real-daemon] route.select(iq:20) -> {low_id}");
    assert_ne!(
        low_id, "claude-opus-4-8",
        "trivial task must not pick the priciest model"
    );

    // STORAGE MILESTONE: central-config -> delivered-descriptor -> real store open.
    // The two selections above ALREADY prove the store served its seeded facts (an
    // unopened/empty store would fail route.select with no_available_model). This
    // asserts the db was physically created at the daemon-DELIVERED path (from the
    // subc.jsonc storage section, via HELLO_ACK.storage), not the dev fallback:
    // the first end-to-end proof of the storage contract through a real consumer.
    let store_db = daemon.expected_store_db(MODULE_ID);
    eprintln!(
        "[real-daemon] route-state store db at delivered path: {}",
        store_db.display()
    );
    assert!(
        store_db.exists(),
        "route-state store must open at the daemon-delivered path {} (central-config -> HELLO_ACK.storage -> open)",
        store_db.display()
    );

    let _ = std::fs::remove_dir_all(&project_root);
}

/// Closing and reopening a route causes a fresh bind, and the hand-rolled provider
/// captures the new binding epoch before serving data on the replacement route.
#[tokio::test]
#[ignore = "builds subc-core in ../subconscious and binds loopback ports"]
async fn real_daemon_rebinds_after_route_close_with_fresh_epoch() {
    let daemon = start_real_daemon().await;
    let mut consumer = connect_consumer(&daemon.connection_file).await;
    wait_for_catalog(&mut consumer, MODULE_ID, SETUP_TIMEOUT).await;

    let project_root = unique_temp_dir("prefrontal-routing-real-daemon-rebind");
    std::fs::create_dir_all(&project_root).unwrap();
    let first = route_open(&mut consumer, &project_root, 1).await;
    let params = serde_json::json!({
        "targetAgent": "mason",
        "requirements": { "iq": 20, "eq": 0 },
        "availableModels": ["ollama-cloud/minimax-m2.5"],
    });
    let before = route_select(&mut consumer, first, 2, params.clone()).await;
    assert!(before["result"]["selected"]["model"]["modelID"]
        .as_str()
        .is_some());

    route_close(&mut consumer, first).await;
    let rebound = route_open(&mut consumer, &project_root, 3).await;
    assert_ne!(
        (first.channel, first.epoch),
        (rebound.channel, rebound.epoch),
        "a replacement binding must receive a fresh wire identity"
    );
    let after = route_select(&mut consumer, rebound, 4, params).await;
    assert!(after["result"]["selected"]["model"]["modelID"]
        .as_str()
        .is_some());

    let _ = std::fs::remove_dir_all(&project_root);
}

/// The module-level error contract holds through a real daemon: an unknown method
/// on the route channel returns a canonical Error frame, not a Response.
#[tokio::test]
#[ignore = "builds subc-core in ../subconscious and binds loopback ports"]
async fn real_daemon_unknown_method_returns_error_frame() {
    let daemon = start_real_daemon().await;
    let mut consumer = connect_consumer(&daemon.connection_file).await;
    wait_for_catalog(&mut consumer, MODULE_ID, SETUP_TIMEOUT).await;

    let project_root = unique_temp_dir("prefrontal-routing-real-daemon-err");
    std::fs::create_dir_all(&project_root).unwrap();
    let route_channel = route_open(&mut consumer, &project_root, 1).await;

    let frame = raw_route_frame(
        &mut consumer,
        route_channel,
        2,
        serde_json::json!({ "method": "does.not.exist", "params": {} }),
    )
    .await;
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Error,
        "unknown method must return an Error frame"
    );
    let body: subc_protocol::ErrorBody = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(body.code, "unknown_method");

    let _ = std::fs::remove_dir_all(&project_root);
}

/// A typed routing failure (no available model) crosses the wire as an Error frame
/// carrying the typed reason code, so a consumer can branch on it (break-glass).
#[tokio::test]
#[ignore = "builds subc-core in ../subconscious and binds loopback ports"]
async fn real_daemon_no_available_model_returns_typed_error() {
    let daemon = start_real_daemon().await;
    let mut consumer = connect_consumer(&daemon.connection_file).await;
    wait_for_catalog(&mut consumer, MODULE_ID, SETUP_TIMEOUT).await;

    let project_root = unique_temp_dir("prefrontal-routing-real-daemon-noavail");
    std::fs::create_dir_all(&project_root).unwrap();
    let route_channel = route_open(&mut consumer, &project_root, 1).await;

    // Empty availableModels → nothing admissible → typed no_available_model.
    let frame = raw_route_frame(
        &mut consumer,
        route_channel,
        2,
        serde_json::json!({
            "method": "route.select",
            "params": { "requirements": { "iq": 50, "eq": 10 }, "availableModels": [] }
        }),
    )
    .await;
    assert_eq!(frame.header.ty, subc_protocol::FrameType::Error);
    let body: subc_protocol::ErrorBody = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(
        body.code, "no_available_model",
        "typed reason must cross the wire"
    );

    let _ = std::fs::remove_dir_all(&project_root);
}
