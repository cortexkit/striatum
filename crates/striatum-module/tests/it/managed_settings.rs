//! Drive the routing binary with the daemon's channel-0 settings exchanges.

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use cortexkit_test_support::{fenced_command, stage_test_binary, ScratchDir};
use serde_json::{json, Value};
use subc_client_rs::config::{ConfigLayer, ConfigLayerSnapshot, CONFIG_GATE_CAPABILITY};
use subc_protocol::{Flags, Frame, FrameType, ModuleHelloAckBody, Priority};
use subc_transport::{authenticate_server, read_frame, write_frame};
use tokio::{
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(10);
const CHANNEL: u16 = 317;
const EPOCH: u32 = 42;

struct Rig {
    peer: TcpStream,
    child: Child,
    hello_control_ops: Value,
    ver: u8,
    config: PathBuf,
    _scratch: ScratchDir,
}

impl Rig {
    async fn start(cap: bool, file: &str) -> Self {
        let scratch = ScratchDir::new("routing-settings-wire");
        let root = scratch.path();
        let xdg = root.join("conf");
        let config = xdg.join("cortexkit/striatum.jsonc");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        std::fs::write(&config, file).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = [7_u8; 32];
        let daemon = [0_u8; 16];
        let connection = root.join("connection.json");
        std::fs::write(
            &connection,
            json!({
                "schema": 1,
                "endpoints": [{"host": "127.0.0.1", "port": listener.local_addr().unwrap().port()}],
                "key": key,
                "daemon_id": daemon,
                "pid": std::process::id(),
                "daemon_ver": "0.50.17"
            })
            .to_string(),
        )
        .unwrap();
        std::fs::set_permissions(&connection, std::fs::Permissions::from_mode(0o600)).unwrap();
        let binary = stage_test_binary(Path::new(env!("CARGO_BIN_EXE_ck-striatum")));
        let mut command = Command::from(fenced_command(&binary, root));
        command
            .current_dir(root)
            .arg("--subc")
            .arg(&connection)
            .env("XDG_CONFIG_HOME", &xdg)
            .env_remove("SUBC_MODULE_ID")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let (mut peer, _) = tokio::select! {
            accepted = timeout(DEADLINE, listener.accept()) => accepted.expect("routing did not connect").unwrap(),
            stopped = child.wait() => panic!("routing stopped before connecting: {stopped:?}"),
        };
        authenticate_server(&mut peer, &key, &daemon, "0.50.17", Duration::from_secs(2))
            .await
            .unwrap();
        let hello = recv(&mut peer).await;
        assert_eq!(hello.header.ty, FrameType::Hello);
        let ver = hello.header.ver;
        let ack = ModuleHelloAckBody {
            negotiated_ver: ver,
            subc_ops: vec![],
            subc_capabilities: if cap {
                vec![CONFIG_GATE_CAPABILITY.into()]
            } else {
                vec![]
            },
            storage: Some(
                serde_json::to_value(striatum_store::dev_descriptor(
                    "striatum",
                    root.join("store.db").to_str().unwrap(),
                ))
                .unwrap(),
            ),
            machine_id: None,
        };
        let mut rig = Self {
            peer,
            child,
            hello_control_ops: body(&hello)["control_ops"].clone(),
            ver,
            config,
            _scratch: scratch,
        };
        rig.send(
            FrameType::HelloAck,
            hello.header.corr,
            serde_json::to_value(ack).unwrap(),
        )
        .await;
        rig
    }

    async fn send(&mut self, kind: FrameType, corr: u64, body: Value) {
        self.send_on(kind, 0, 0, corr, body).await;
    }

    async fn send_on(&mut self, kind: FrameType, channel: u16, epoch: u32, corr: u64, body: Value) {
        let bytes = if body.is_null() {
            vec![]
        } else {
            serde_json::to_vec(&body).unwrap()
        };
        let frame = Frame::build_with_version(
            self.ver,
            kind,
            Flags::new(false, Priority::Passive, false),
            channel,
            epoch,
            corr,
            bytes,
        )
        .unwrap();
        write_frame(&mut self.peer, &frame).await.unwrap();
    }

    async fn ping(&mut self) {
        self.send(FrameType::Ping, 800, Value::Null).await;
        let frame = recv(&mut self.peer).await;
        assert_eq!((frame.header.ty, frame.header.corr), (FrameType::Pong, 800));
    }

    async fn request(&mut self, op: &str) -> Frame {
        let frame = recv(&mut self.peer).await;
        assert_eq!(
            (frame.header.ty, frame.header.channel, frame.header.epoch),
            (FrameType::Request, 0, 0)
        );
        assert_eq!(body(&frame)["op"], op);
        frame
    }

    async fn declare(&mut self) -> Frame {
        let declare = self.request("config.declare").await;
        self.assert_review_advertised();
        let declaration = body(&declare);
        let schema: Value =
            serde_json::from_str(include_str!("../../routing-config.schema.json")).unwrap();
        assert_eq!(declaration["user_file"], self.config.to_str().unwrap());
        assert_eq!(declaration["schema"], schema);
        assert_eq!(declaration["project_dirs"], json!([]));
        declare
    }

    fn assert_review_advertised(&self) {
        assert_eq!(
            self.hello_control_ops,
            json!(["health.check", "config.review"]),
            "HELLO advertised the wrong control ops"
        );
    }

    async fn bind(&mut self) {
        self.send(FrameType::Request, 81, json!({
            "op": "route.bind", "route_channel": CHANNEL, "epoch": EPOCH,
            "target": {"kind": "management_surface", "module_id": "striatum"},
            "identity": {"project_root": self._scratch.path(), "harness": "test", "session": "settings"}
        })).await;
        let frame = recv(&mut self.peer).await;
        assert_eq!(
            (frame.header.ty, frame.header.corr),
            (FrameType::Response, 81)
        );
    }

    async fn explain_missing(&mut self) -> Value {
        self.send_on(
            FrameType::Request,
            CHANNEL,
            EPOCH,
            82,
            json!({
                "method": "route.explain", "params": {"decisionID": "absent"}
            }),
        )
        .await;
        let frame = recv(&mut self.peer).await;
        assert_eq!((frame.header.ty, frame.header.corr), (FrameType::Error, 82));
        body(&frame)
    }

    async fn finish(mut self) -> String {
        self.send(FrameType::Goodbye, 0, Value::Null).await;
        let output = timeout(DEADLINE, self.child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(output.status.success(), "{stderr}");
        stderr
    }
}

async fn recv(peer: &mut TcpStream) -> Frame {
    timeout(DEADLINE, read_frame(peer))
        .await
        .expect("module did not reply")
        .unwrap()
        .expect("module closed connection")
}

fn body(frame: &Frame) -> Value {
    serde_json::from_slice(&frame.body).unwrap()
}

fn assert_settings_source(logs: &str, expected: &str) {
    let sources: Vec<_> = logs
        .lines()
        .filter_map(|line| line.strip_prefix("[striatum] settings source: "))
        .collect();
    assert_eq!(sources, [expected], "{logs}");
}

macro_rules! vector {
    ($name:literal) => {
        serde_json::from_str::<Value>(include_str!(concat!(
            "../fixtures/config_gate/",
            $name,
            ".json"
        )))
        .unwrap()
    };
}

#[tokio::test]
async fn capable_daemon_gets_a_declaration_without_a_startup_flag() {
    let mut rig = Rig::start(true, "{}").await;
    let declare = rig.declare().await;
    rig.send(
        FrameType::Response,
        declare.header.corr,
        vector!("config_declare_reply"),
    )
    .await;
    rig.request("config.get").await;
    rig.ping().await;
    assert_settings_source(
        &rig.finish().await,
        "managed (daemon advertised config.gate)",
    );
}

#[tokio::test]
async fn old_daemon_uses_the_file_without_sending_a_declaration() {
    let mut rig = Rig::start(false, r#"{"retention":{"model_route_decisions_days":31}}"#).await;
    rig.ping().await;
    rig.bind().await;
    let missing = rig.explain_missing().await;
    assert_eq!(missing["code"], "not_found");
    assert!(missing["message"]
        .as_str()
        .unwrap()
        .contains("retention is 31 days"));
    std::fs::write(
        &rig.config,
        r#"{"retention":{"model_route_decisions_days":37}}"#,
    )
    .unwrap();
    timeout(DEADLINE, async {
        loop {
            let missing = rig.explain_missing().await;
            assert_eq!(missing["code"], "not_found");
            if missing["message"]
                .as_str()
                .unwrap()
                .contains("retention is 37 days")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("updated routing settings file was not reloaded");
    rig.assert_review_advertised();
    assert_settings_source(
        &rig.finish().await,
        "file (daemon did not advertise config.gate)",
    );
}

#[tokio::test]
async fn refused_declaration_loads_the_file_and_keeps_serving() {
    let mut rig = Rig::start(true, r#"{"retention":{"model_route_decisions_days":31}}"#).await;
    let declare = rig.declare().await;
    rig.send(
        FrameType::Error,
        declare.header.corr,
        vector!("config_get_error_gated"),
    )
    .await;
    timeout(DEADLINE, async {
        loop {
            rig.send(FrameType::Request, 80, json!({"op":"health.check"}))
                .await;
            let health = body(&recv(&mut rig.peer).await);
            assert_eq!(health["op"], "health.check");
            if !health["detail"]
                .as_str()
                .unwrap_or("")
                .contains("waiting for the daemon's settings view")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("file fallback did not become ready");
    rig.bind().await;
    let missing = rig.explain_missing().await;
    assert_eq!(missing["code"], "not_found");
    assert!(missing["message"]
        .as_str()
        .unwrap()
        .contains("retention is 31 days"));
    let logs = rig.finish().await;
    assert_settings_source(&logs, "file (daemon refused config.declare)");
    assert_eq!(logs.matches("config.declare refused").count(), 1, "{logs}");
    assert_eq!(
        logs.matches("waiting for the daemon's settings view")
            .count(),
        1,
        "{logs}"
    );
    assert_eq!(
        logs.matches("routing settings are ready").count(),
        1,
        "{logs}"
    );
}

#[tokio::test]
async fn module_delivery_vectors_drive_review_wait_ack_and_nack() {
    let mut rig = Rig::start(true, "not a valid routing file").await;
    let declare = rig.declare().await;
    rig.send(
        FrameType::Response,
        declare.header.corr,
        vector!("config_declare_reply"),
    )
    .await;
    let get = rig.request("config.get").await;
    rig.send(
        FrameType::Error,
        get.header.corr,
        vector!("config_get_error_gated"),
    )
    .await;

    rig.ping().await;
    rig.send(FrameType::Request, 80, json!({"op":"health.check"}))
        .await;
    let health = body(&recv(&mut rig.peer).await);
    assert_eq!(health["status"], "degraded");
    assert!(health["detail"]
        .as_str()
        .unwrap()
        .contains("waiting for the daemon's settings view"));
    rig.bind().await;
    let waiting = rig.explain_missing().await;
    assert_eq!(waiting["code"], "routing_not_ready");
    assert_eq!(waiting["detail"]["retryable"], true);

    let mut review_request = vector!("config_review_request");
    review_request["base"] = Value::Null;
    review_request["layers"] = json!([ConfigLayerSnapshot::file(
        "striatum",
        ConfigLayer::User,
        rig.config.to_str().unwrap(),
        Some(br#"{"retention":{"model_route_decisions_days":37}}"#.to_vec()),
    )]);
    rig.send(FrameType::Request, 83, review_request).await;
    let review = recv(&mut rig.peer).await;
    assert_eq!(
        (
            review.header.ty,
            review.header.channel,
            review.header.epoch,
            review.header.corr
        ),
        (FrameType::Response, 0, 0, 83)
    );
    let review = body(&review);
    assert_eq!(review["op"], "config.review");
    assert_eq!(review["refusals"], json!([]));
    let user_changes: Vec<_> = review["fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|field| field["new_source"] == "user")
        .collect();
    assert_eq!(user_changes.len(), 1, "{review}");
    let change = user_changes[0];
    assert_eq!(change["path"], "/retention/model_route_decisions_days");
    assert_eq!(change["new"], json!({"kind":"value","value":37}));
    assert_eq!(change["apply"], "live");

    let valid = json!({"retention":{"model_route_decisions_days":37}});
    let pending = deliver(&mut rig, 4, 8, valid).await;
    assert_eq!(body(&pending), vector!("config_applied_request_ack"));
    rig.send(
        FrameType::Response,
        pending.header.corr,
        vector!("config_applied_reply"),
    )
    .await;
    let missing = rig.explain_missing().await;
    assert_eq!(missing["code"], "not_found");
    assert!(missing["message"]
        .as_str()
        .unwrap()
        .contains("retention is 37 days"));

    let rejected = deliver(&mut rig, 5, 9, json!({"catalog":{"poll_ms":0}})).await;
    let nack = body(&rejected);
    assert_eq!(nack["result"], "nack");
    assert_eq!(
        (nack["generation"].as_u64(), nack["attempt"].as_u64()),
        (Some(5), Some(9))
    );
    assert!(nack["reasons"][0]
        .as_str()
        .unwrap()
        .contains("catalog.poll_ms"));
    rig.send(
        FrameType::Response,
        rejected.header.corr,
        vector!("config_applied_reply"),
    )
    .await;
    assert_eq!(rig.explain_missing().await, missing);
    let logs = rig.finish().await;
    assert!(!logs.contains("config.declare refused"), "{logs}");
    assert_eq!(
        logs.matches("routing settings are ready").count(),
        1,
        "{logs}"
    );
}

async fn deliver(rig: &mut Rig, generation: u64, attempt: u64, effective: Value) -> Frame {
    let mut hint = vector!("config_changed");
    hint.as_object_mut().unwrap().remove("project_id");
    hint["generation"] = json!(generation);
    hint["fields"] = json!(["/retention/model_route_decisions_days"]);
    rig.send(FrameType::Push, 0, hint).await;
    let request = rig.request("config.get").await;
    // Use the published first-delivery envelope with routing's single user
    // snapshot instead of the sample module's settings and project layer.
    let mut reply = vector!("config_get_reply_first_delivery");
    let pending = &mut reply["pending"];
    pending["generation"] = json!(generation);
    pending["attempt"] = json!(attempt);
    let snapshot = ConfigLayerSnapshot::file(
        "striatum",
        ConfigLayer::User,
        rig.config.to_str().unwrap(),
        Some(serde_json::to_vec(&effective).unwrap()),
    );
    pending["effective"] = effective;
    pending["layers"] = json!([snapshot]);
    pending["sources"] = json!({});
    rig.send(FrameType::Response, request.header.corr, reply)
        .await;
    rig.request("config.applied").await
}
