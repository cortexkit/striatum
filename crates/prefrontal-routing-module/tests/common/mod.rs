#![allow(dead_code)]
//! Shared consumer-side wire driver for the prefrontal-routing-module e2e tests.
//!
//! Mirrors insula (formerly ai-provider-quota)'s `common/mod.rs` (the proven harness): authenticate
//! as a client, `catalog.list`, `route.open` the ManagementSurface, then
//! `route.select` on the route channel. Adapted only in the module id and the
//! data-plane op (`route.select` vs `usage.get`).

use std::{
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde_json::Value;
// Wire types come from the protocol and transport crates directly: subc-core is
// the daemon binary package and stopped re-exporting them when the library split
// into subc-daemon (subconscious 74370aee).
use subc_protocol::Frame;
use subc_protocol::{BindIdentity, Flags, FrameType, Priority, RouteTarget};
use subc_transport::{authenticate_client, connection_file};
use subc_transport::{read_frame, write_frame};
use tokio::{
    net::TcpStream,
    time::{timeout, Instant},
};

pub const MODULE_ID: &str = "prefrontal-routing";
pub const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
pub const READ_TIMEOUT: Duration = Duration::from_secs(10);

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
pub struct TestRoute {
    pub channel: u16,
    pub epoch: u32,
}

/// A process-unique path under the test scratch root (no collision across tests).
pub fn unique_temp_dir(label: &str) -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    prefrontal_test_support::scratch_root().join(format!("{label}-{}-{n}", process::id()))
}

/// Connect to a daemon from its connection file and complete the client HMAC handshake.
pub async fn connect_consumer(connection_file_path: &Path) -> TcpStream {
    let conn = connection_file::read(connection_file_path).unwrap();
    let endpoint = conn.endpoints.first().unwrap();
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .unwrap();
    authenticate_client(&mut stream, &conn, Duration::from_secs(2))
        .await
        .unwrap();
    stream
}

/// Send a channel-0 control request and read until its channel-0 reply for `corr`.
pub async fn control_rpc(stream: &mut TcpStream, corr: u64, body: Value) -> Frame {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(stream, &frame).await.unwrap();
    read_until_channel0(stream, corr).await
}

async fn read_until_channel0(stream: &mut TcpStream, corr: u64) -> Frame {
    loop {
        let frame = read_frame_timeout(stream).await;
        if frame.header.channel == 0
            && matches!(frame.header.ty, FrameType::Response | FrameType::Error)
            && frame.header.corr == corr
        {
            return frame;
        }
    }
}

pub async fn read_frame_timeout(stream: &mut TcpStream) -> Frame {
    timeout(READ_TIMEOUT, async {
        read_frame(stream)
            .await
            .unwrap()
            .expect("connection should stay open")
    })
    .await
    .expect("timed out waiting for a frame")
}

/// `catalog.list` → the `modules` array.
pub async fn catalog_list(stream: &mut TcpStream, corr: u64) -> Vec<Value> {
    let frame = control_rpc(stream, corr, serde_json::json!({ "op": "catalog.list" })).await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let value: Value = serde_json::from_slice(&frame.body).unwrap();
    value["modules"].as_array().cloned().unwrap_or_default()
}

/// Poll `catalog.list` until the module appears (real daemon spawns asynchronously).
pub async fn wait_for_catalog(stream: &mut TcpStream, module_id: &str, wait: Duration) -> Value {
    let deadline = Instant::now() + wait;
    let mut corr = 1000;
    loop {
        let modules = catalog_list(stream, corr).await;
        if let Some(m) = modules.into_iter().find(|m| m["module_id"] == module_id) {
            return m;
        }
        assert!(
            Instant::now() < deadline,
            "module {module_id} did not appear in catalog within {wait:?}"
        );
        corr += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn json_route_open(project_root: &Path) -> Value {
    let target = RouteTarget::ManagementSurface {
        module_id: MODULE_ID.to_string(),
    };
    let identity = BindIdentity::new(
        project_root.to_path_buf(),
        "prefrontal-routing-e2e".to_string(),
        "session-1".to_string(),
    );
    // NOTE: subc-protocol 0.4 (commit e0e7ce77) removed the route.open `config`
    // relay + ConfigTier; the current shape is { op, target, identity } only.
    serde_json::json!({ "op": "route.open", "target": target, "identity": identity })
}

/// `route.open` the management surface; returns its wire identity.
pub async fn route_open(stream: &mut TcpStream, project_root: &Path, corr: u64) -> TestRoute {
    let frame = control_rpc(stream, corr, json_route_open(project_root)).await;
    assert_eq!(
        frame.header.ty,
        FrameType::Response,
        "route.open should succeed: {}",
        String::from_utf8_lossy(&frame.body)
    );
    let value: Value = serde_json::from_slice(&frame.body).unwrap();
    TestRoute {
        channel: value["route_channel"].as_u64().unwrap() as u16,
        epoch: value["route_epoch"].as_u64().unwrap() as u32,
    }
}

/// Close one route binding. The daemon relays the goodbye before processing the next
/// frame from this consumer connection.
pub async fn route_close(stream: &mut TcpStream, route: TestRoute) {
    let frame = Frame::build(
        FrameType::Goodbye,
        Flags::new(false, Priority::Interactive, false),
        route.channel,
        route.epoch,
        0,
        Vec::new(),
    )
    .unwrap();
    write_frame(stream, &frame).await.unwrap();
}

/// Send an executor selection using the fixture's availability list, unless the
/// fixture explicitly supplies another executor value.
pub async fn route_select(
    stream: &mut TcpStream,
    route: TestRoute,
    corr: u64,
    mut params: Value,
) -> Value {
    params
        .as_object_mut()
        .unwrap()
        .entry("executor")
        .or_insert(serde_json::json!("broca"));
    raw_route_request(
        stream,
        route,
        corr,
        serde_json::json!({ "method": "route.select", "params": params }),
    )
    .await
}

/// Send a `route.quota_status` request; returns the decoded body (observability:
/// whether the outbound quota-consumer connection is live + how fresh).
pub async fn route_quota_status(stream: &mut TcpStream, route: TestRoute, corr: u64) -> Value {
    raw_route_request(
        stream,
        route,
        corr,
        serde_json::json!({ "method": "route.quota_status", "params": {} }),
    )
    .await
}

/// Send a raw data-plane request on the route channel; returns the decoded body of
/// the terminal Response (panics on Error).
pub async fn raw_route_request(
    stream: &mut TcpStream,
    route: TestRoute,
    corr: u64,
    body: Value,
) -> Value {
    let frame = raw_route_frame(stream, route, corr, body).await;
    match frame.header.ty {
        FrameType::Response => serde_json::from_slice(&frame.body).unwrap(),
        FrameType::Error => {
            panic!(
                "route request returned error: {}",
                String::from_utf8_lossy(&frame.body)
            )
        }
        ty => panic!("unexpected route frame {ty:?}"),
    }
}

/// Like [`raw_route_request`] but returns the raw terminal frame (Response OR Error).
pub async fn raw_route_frame(
    stream: &mut TcpStream,
    route: TestRoute,
    corr: u64,
    body: Value,
) -> Frame {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        route.channel,
        route.epoch,
        corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(stream, &frame).await.unwrap();
    loop {
        let frame = read_frame_timeout(stream).await;
        if frame.header.corr == corr
            && matches!(frame.header.ty, FrameType::Response | FrameType::Error)
        {
            return frame;
        }
    }
}
