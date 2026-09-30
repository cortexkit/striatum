#![forbid(unsafe_code)]

//! The module-to-module quota-consumption proof: a real subc-core daemon
//! supervises BOTH insula AND prefrontal-routing, and prefrontal-routing
//! holds TWO independent authenticated connections to the daemon at once: its
//! INBOUND provider connection (serving route.select) and its OUTBOUND consumer
//! connection (calling insula's usage.get): without the daemon
//! conflating or rejecting either.
//!
//! This exercises the three concerns flagged before the wire path was built:
//!   (a) a second outbound consumer connection from a process that is also a
//!       provider: independent, not a self-reference the daemon rejects;
//!   (b) module->module route.open (prefrontal-routing -> insula) works
//!       identically to an external consumer;
//!   (c) no deadlock when the serve loop's selection path consults quota.
//!
//! `#[ignore]` by default: builds subc-core AND insula (the quota module) from sibling
//! repos and binds real loopback ports. Run:
//!   cargo test -p prefrontal-routing-module --test both_modules_e2e -- --ignored --nocapture

mod common;

use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use tokio::process::{Child, Command};

use common::{
    catalog_list, connect_consumer, route_open, route_quota_status, route_select, unique_temp_dir,
    wait_for_catalog, MODULE_ID, SETUP_TIMEOUT,
};

const QUOTA_REPO_REL: &str = "../../../insula";
const QUOTA_MODULE_ID: &str = "insula";

struct RealDaemon {
    #[allow(dead_code)]
    child: Child,
    rig: PathBuf,
    connection_file: PathBuf,
}

impl Drop for RealDaemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.rig);
    }
}

fn sibling(rel: &str) -> PathBuf {
    let manifest_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let direct = manifest_root.join(rel);
    let configured = (rel == QUOTA_REPO_REL)
        .then(|| std::env::var_os("AI_PROVIDER_QUOTA_REPO"))
        .flatten()
        .map(PathBuf::from);
    let primary_checkout_sibling = || {
        let worktree_root = manifest_root.parent()?.parent()?;
        let marker = std::fs::read_to_string(worktree_root.join(".git")).ok()?;
        let git_dir = PathBuf::from(marker.trim().strip_prefix("gitdir: ")?);
        let primary_checkout = git_dir
            .ancestors()
            .find(|path| path.file_name().is_some_and(|name| name == ".git"))?
            .parent()?;
        Some(primary_checkout.parent()?.join(Path::new(rel).file_name()?))
    };

    configured
        .into_iter()
        .chain([direct])
        .chain(primary_checkout_sibling())
        .find(|path| path.exists())
        .and_then(|path| path.canonicalize().ok())
        .unwrap_or_else(|| panic!("sibling repo {rel} must exist for the both-modules test"))
}

fn build_bin(repo: &Path, bin: &str) -> PathBuf {
    prefrontal_test_support::build_sibling_binary(repo, "insula", bin)
}

/// Stand up a real subc-core daemon supervising BOTH modules from one subc.jsonc.
async fn start_both_modules_daemon() -> RealDaemon {
    let subc_core = prefrontal_test_support::SubcDaemonBinary::build();
    let quota_module = build_bin(&sibling(QUOTA_REPO_REL), "ck-insula");
    let routing_module = PathBuf::from(env!("CARGO_BIN_EXE_ck-prefrontal-routing"));
    assert!(routing_module.exists());
    prefrontal_test_support::sign_test_binary(&routing_module);

    let rig = unique_temp_dir("both-modules-real-daemon");
    let config_dir = rig.join("config/cortexkit");
    let runtime_dir = rig.join("runtime");
    let data_home = rig.join("data");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::create_dir_all(&runtime_dir).unwrap();
    std::fs::create_dir_all(&data_home).unwrap();

    // Central storage config so routing's store opens at a rig-isolated delivered
    // path (not the global dev fallback): keeps parallel daemons from contending
    // on one lease.
    let subc_jsonc = serde_json::json!({
        "version": 1,
        "storage": { "backend": "sqlite", "data_home": data_home },
        "modules": {
            QUOTA_MODULE_ID: { "program": quota_module, "args": [], "env": {} },
            MODULE_ID: { "program": routing_module, "args": [], "env": {} }
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
            "daemon did not publish a connection file"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    RealDaemon {
        child,
        rig,
        connection_file,
    }
}

/// Both modules register; prefrontal-routing serves route.select on its INBOUND
/// provider connection WHILE its OUTBOUND consumer connection calls quota: the
/// two coexist on one process. We drive an external consumer to confirm both
/// modules are routable, and assert routing's selection still works (which it can
/// only do if its inbound provider connection is healthy concurrently with its
/// outbound quota connection running in the background refresh task).
#[tokio::test]
#[ignore = "builds subc-core + insula in sibling repos and binds loopback ports"]
async fn routing_consumes_quota_module_to_module_while_serving() {
    let daemon = start_both_modules_daemon().await;
    let mut consumer = connect_consumer(&daemon.connection_file).await;

    // Both modules land in the catalog (each is a routable management_surface).
    wait_for_catalog(&mut consumer, QUOTA_MODULE_ID, SETUP_TIMEOUT).await;
    wait_for_catalog(&mut consumer, MODULE_ID, SETUP_TIMEOUT).await;
    let modules = catalog_list(&mut consumer, 5000).await;
    let ids: Vec<&str> = modules
        .iter()
        .filter_map(|m| m["module_id"].as_str())
        .collect();
    eprintln!("[both-modules] catalog: {ids:?}");
    assert!(ids.contains(&QUOTA_MODULE_ID) && ids.contains(&MODULE_ID));

    let project_root = unique_temp_dir("both-modules-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let route_channel = route_open(&mut consumer, &project_root, 1).await;

    // THE DISTINGUISHING ASSERTION (a)+(b): poll route.quota_status until routing's
    // OUTBOUND consumer connection has actually round-tripped a usage.get against
    // insula. `available: true` means: the second outbound connection
    // authenticated, route.open(insula) succeeded, and usage.get
    // returned a ProviderUsage[]: module->module consumption proven on the wire,
    // not merely "didn't hang". (In a credless shell every provider silent-degrades,
    // so healthyCount may be 0, but availability + providerCount>0 prove the round
    // trip: the quota module always returns its full provider array.)
    let mut quota_live = false;
    let mut last_status = serde_json::Value::Null;
    for i in 0..40 {
        let status = route_quota_status(&mut consumer, route_channel, 1000 + i).await;
        let r = &status["result"];
        last_status = r.clone();
        if r["available"].as_bool() == Some(true) && r["providerCount"].as_u64().unwrap_or(0) > 0 {
            eprintln!(
                "[both-modules] quota_status: {}",
                serde_json::to_string(r).unwrap()
            );
            quota_live = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        quota_live,
        "routing's outbound quota-consumer connection must round-trip usage.get against insula; last status: {}",
        serde_json::to_string(&last_status).unwrap()
    );

    // Drive route.select on routing's INBOUND provider connection. This succeeding
    // (while the outbound quota connection is concurrently live, just proven above)
    // is (c): two connections, one process, no conflation, no deadlock.

    let available = vec![
        "anthropic/claude-opus-4-8",
        "openai/gpt-5.5",
        "xai/grok-4.3",
        "ollama-cloud/minimax-m2.5",
    ];
    let params = serde_json::json!({
        "targetAgent": "mason",
        "requirements": { "iq": 80, "eq": 20 },
        "availableModels": available,
    });
    let response = route_select(&mut consumer, route_channel, 2, params).await;
    let model_id = response["result"]["selected"]["model"]["modelID"]
        .as_str()
        .expect("a selection");
    eprintln!("[both-modules] route.select (quota live in background) -> {model_id}");
    assert!(
        !model_id.is_empty(),
        "selection must succeed while quota-consumer runs concurrently"
    );

    // Repeat several selections back-to-back: if the outbound quota call could
    // deadlock the serve path, a selection here would hang (the test would time out
    // on read). Completing them proves (c) no-deadlock by observation.
    for i in 0..5 {
        let p = serde_json::json!({
            "targetAgent": "mason",
            "requirements": { "iq": 60, "eq": 10 },
            "availableModels": available,
        });
        let r = route_select(&mut consumer, route_channel, 10 + i, p).await;
        assert!(
            r["result"]["selected"]["model"]["modelID"]
                .as_str()
                .is_some(),
            "selection {i} must not hang"
        );
    }
    eprintln!("[both-modules] 6 selections completed with no deadlock while quota-consumer ran");

    let _ = std::fs::remove_dir_all(&project_root);
}
