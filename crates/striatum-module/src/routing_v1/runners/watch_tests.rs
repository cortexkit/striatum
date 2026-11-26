//! Catalog pushes use the same publication path as periodic declaration refreshes.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{authenticate_server, read_frame, write_frame};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use super::super::conformance_tests::{install_catalog, reserved, Harness};

const SLOW_RUNNER: &str = "slow-describe-runner";

#[derive(Clone, Debug)]
enum Command {
    Push(Vec<u64>),
    Fail,
    Disconnect,
    Release,
}

#[derive(Default)]
struct DaemonState {
    declared: AtomicBool,
    generation: AtomicU64,
    watch_up: AtomicBool,
    block_describe: AtomicBool,
    slow_runner: AtomicBool,
    classifier: AtomicBool,
    classifier_describes: AtomicUsize,
    catalog_fails: AtomicBool,
    lists: AtomicUsize,
    watches: AtomicUsize,
    describes: AtomicUsize,
    slow_describes: AtomicUsize,
    pushes: AtomicUsize,
    connections: AtomicUsize,
}

struct Rig {
    harness: Arc<Harness>,
    state: Arc<DaemonState>,
    commands: broadcast::Sender<Command>,
    daemon: tokio::task::JoinHandle<()>,
    poller: tokio::task::JoinHandle<()>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.poller.abort();
        self.daemon.abort();
    }
}

async fn until(mut predicate: impl FnMut() -> bool, timeout: Duration, message: &str) {
    tokio::time::timeout(timeout, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect(message);
}

async fn reply(stream: &mut TcpStream, request: &Frame, ty: FrameType, body: Value) {
    let frame = Frame::build(
        ty,
        Flags::new(false, Priority::Interactive, false),
        request.header.channel,
        request.header.epoch,
        request.header.corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(stream, &frame).await.unwrap();
}

fn modules(declared: bool) -> Value {
    if !declared {
        return json!([]);
    }
    json!([{
        "module_id":fixture::RUNNER_ONE, "nonce_verified":true, "first_party":true,
        "roles":[], "control_ops":[], "incarnation":"daemon-a", "spawn":7,
        "role_declarations":{"routing/v1":{"runner":{
            "harnesses":["fixture"]
        }}}
    }])
}

fn runner_describe() -> Value {
    json!({"result":{"capabilities":["serves"],"serves":{
        "generation":"watch-fixture","harnesses":{"fixture":[
            {"provider":fixture::PRICED_PROVIDER,"models":"all"},
            {"provider":fixture::SUBSCRIPTION_PROVIDER,"models":"all"}
        ]}
    }}})
}

async fn serve(
    mut stream: TcpStream,
    state: Arc<DaemonState>,
    mut commands: broadcast::Receiver<Command>,
) {
    authenticate_server(
        &mut stream,
        &[7; 32],
        &[0; 16],
        "0.40.0",
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    state.connections.fetch_add(1, Ordering::SeqCst);
    let mut watch = None;
    let mut held_describe = None;
    loop {
        tokio::select! {
            frame = read_frame(&mut stream) => {
                let Ok(Some(request)) = frame else { return; };
                if request.header.ty != FrameType::Request { continue; }
                let body: Value = serde_json::from_slice(&request.body).unwrap();
                match body["op"].as_str() {
                    Some("server.describe") => reply(&mut stream, &request, FrameType::Response,
                        json!({"op":"server.describe","protocol_ver":1,"subc_ops":[],
                            "capabilities":["route-close-reasons/v1",subc_protocol::scope::CAP_ROUTE_ROLE_VERSIONS_V1],"connected_clients":1})).await,
                    Some("catalog.list") => {
                        let mut modules = modules(state.declared.load(Ordering::SeqCst));
                        if state.slow_runner.load(Ordering::SeqCst) {
                            let mut slow = modules[0].clone();
                            slow["module_id"] = json!(SLOW_RUNNER);
                            slow["spawn"] = json!(8);
                            modules.as_array_mut().unwrap().push(slow);
                        }
                        if state.classifier.load(Ordering::SeqCst) {
                            modules.as_array_mut().unwrap().push(json!({
                                "module_id":"classifier", "nonce_verified":true, "first_party":true,
                                "incarnation":"daemon-a", "spawn":9,
                                "roles":[], "control_ops":[],
                                "capabilities":{"provides":["classifier/v1"]}
                            }));
                        }
                        if state.catalog_fails.load(Ordering::SeqCst) {
                            reply(&mut stream, &request, FrameType::Error,
                                json!({"code":"unavailable","message":"catalog read failed"})).await;
                        } else {
                            reply(&mut stream, &request, FrameType::Response,
                                json!({"op":"catalog.list","generation":state.generation.load(Ordering::SeqCst),
                                    "modules":modules,"subc_ops":[]})).await;
                        }
                        state.lists.fetch_add(1, Ordering::SeqCst);
                    }
                    Some("catalog.watch") => {
                        state.watches.fetch_add(1, Ordering::SeqCst);
                        if state.watch_up.load(Ordering::SeqCst) {
                            reply(&mut stream, &request, FrameType::StreamData,
                                json!({"op":"catalog.watch","generation":state.generation.load(Ordering::SeqCst)})).await;
                            watch = Some(request);
                        } else {
                            reply(&mut stream, &request, FrameType::Error,
                                json!({"code":"watch_unavailable","message":"watch down"})).await;
                        }
                    }
                    Some("route.open") => {
                        let channel = match body["target"]["module_id"].as_str() {
                            Some(SLOW_RUNNER) => 8,
                            Some("classifier") => 9,
                            _ => 7,
                        };
                        reply(&mut stream, &request, FrameType::Response,
                            json!({"op":"route.open","route_channel":channel,"route_epoch":3,
                                "target":{"first_party":true,"kind":"spawn","incarnation":"daemon-a",
                                    "spawn":channel,"authority_epoch":2,"approved_declarations_digest":"sha256:approved"}})).await;
                    }
                    Some("route.close") => reply(&mut stream, &request, FrameType::Response,
                        json!({"op":"route.close"})).await,
                    _ if body["method"] == "role.describe" && request.header.channel == 9 => {
                        let count = state.classifier_describes.fetch_add(1, Ordering::SeqCst);
                        let value = if count == 0 {
                            json!({"result":{"role":"classifier/v1","models":17}})
                        } else {
                            serde_json::from_slice(include_bytes!("../../../tests/fixtures/classifier-describe.reply.json")).unwrap()
                        };
                        reply(&mut stream, &request, FrameType::Response, value).await;
                    }
                    _ if body["method"] == "role.describe" && request.header.channel == 8 => {
                        state.slow_describes.fetch_add(1, Ordering::SeqCst);
                    }
                    _ if body["method"] == "role.describe" => {
                        assert_eq!(body["params"], json!({"serves":true}));
                        state.describes.fetch_add(1, Ordering::SeqCst);
                        if state.block_describe.load(Ordering::SeqCst) {
                            held_describe = Some(request);
                        } else {
                            reply(&mut stream, &request, FrameType::Response,
                                runner_describe()).await;
                        }
                    }
                    _ => panic!("unexpected fake daemon request: {body}"),
                }
            }
            command = commands.recv() => match command.unwrap() {
                Command::Push(generations) => if let Some(request) = &watch {
                    for generation in generations {
                        reply(&mut stream, request, FrameType::StreamData,
                            json!({"op":"catalog.watch","generation":generation})).await;
                        state.pushes.fetch_add(1, Ordering::SeqCst);
                    }
                },
                Command::Fail => if let Some(request) = watch.take() {
                    reply(&mut stream, &request, FrameType::Error,
                        json!({"code":"watch_unavailable","message":"watch failed"})).await;
                },
                Command::Disconnect => return,
                Command::Release => if let Some(request) = held_describe.take() {
                    reply(&mut stream, &request, FrameType::Response,
                        runner_describe()).await;
                },
            }
        }
    }
}

impl Rig {
    async fn new(label: &str) -> Self {
        let harness = Harness::new(label);
        install_catalog(&harness, &fixture::catalog());
        harness.edit_config(|config| {
            config["runners"] = json!([]);
            config["consumers"] = json!({"watch-test":{"owner":"test-owner"}});
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let path = harness
            .config_path_for_test()
            .parent()
            .unwrap()
            .join("connection.json");
        std::fs::write(&path, serde_json::to_vec(&json!({
            "schema":1,"endpoints":[{"host":"127.0.0.1","port":listener.local_addr().unwrap().port()}],
            "key":vec![7_u8;32],"daemon_id":vec![0_u8;16],"pid":std::process::id(),"daemon_ver":"0.40.0"
        })).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let state = Arc::new(DaemonState::default());
        state.watch_up.store(true, Ordering::SeqCst);
        state.generation.store(1, Ordering::SeqCst);
        let (commands, _) = broadcast::channel(64);
        let daemon = {
            let state = state.clone();
            let commands = commands.clone();
            tokio::spawn(async move {
                let mut peers = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (stream, _) = accepted.unwrap();
                            peers.spawn(serve(stream, state.clone(), commands.subscribe()));
                        }
                        done = peers.join_next(), if !peers.is_empty() => {
                            done.unwrap().unwrap();
                        }
                    }
                }
            })
        };
        let poller = tokio::spawn(super::refresh_loop(
            path,
            harness.credentials(),
            harness.config_path_for_test(),
        ));
        let rig = Self {
            harness,
            state,
            commands,
            daemon,
            poller,
        };
        until(
            || rig.state.watches.load(Ordering::SeqCst) > 0,
            Duration::from_secs(3),
            "routing never subscribed to catalog.watch",
        )
        .await;
        // Let the initial generation's forced pass finish before counting later passes.
        rig.settled().await;
        rig
    }

    async fn settled(&self) {
        let mut previous = self.state.lists.load(Ordering::SeqCst);
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let current = self.state.lists.load(Ordering::SeqCst);
            if current == previous {
                break;
            }
            previous = current;
        }
    }

    fn push(&self, generations: Vec<u64>) {
        self.state
            .generation
            .store(*generations.last().unwrap(), Ordering::SeqCst);
        self.commands.send(Command::Push(generations)).unwrap();
    }

    async fn select(&self) -> (FrameType, Value) {
        self.harness
            .bind(reserved("test-owner"), None)
            .await
            .call(
                "route.select",
                &json!({"consumer":"watch-test","requirements":{"iq":0}}),
            )
            .await
    }

    async fn declared(&self) {
        until(
            || {
                self.harness
                    .credentials()
                    .lock()
                    .unwrap()
                    .runner_declarations
                    .contains_key(fixture::RUNNER_ONE)
            },
            Duration::from_secs(3),
            "watch change did not publish runner declarations",
        )
        .await;
    }
}

#[tokio::test]
async fn catalog_watch_change_is_visible_to_route_select_without_interval() {
    let rig = Rig::new("watch-select").await;
    assert_eq!(rig.select().await.0, FrameType::Error);
    rig.state.declared.store(true, Ordering::SeqCst);
    rig.push(vec![2]);
    rig.declared().await;
    let selected = rig.select().await;
    assert_eq!(selected.0, FrameType::Response, "{selected:?}");
    assert_eq!(
        selected.1["result"]["runner"]["module"],
        fixture::RUNNER_ONE
    );
    until(
        || rig.state.describes.load(Ordering::SeqCst) > 0,
        Duration::from_secs(2),
        "watch pass did not re-read role.describe",
    )
    .await;
    rig.state.declared.store(false, Ordering::SeqCst);
    rig.push(vec![3]);
    until(
        || {
            rig.harness
                .credentials()
                .lock()
                .unwrap()
                .runner_declarations
                .is_empty()
        },
        Duration::from_secs(2),
        "watch withdrawal was not published",
    )
    .await;
    assert_eq!(rig.select().await.0, FrameType::Error);
}

#[tokio::test]
async fn catalog_watch_failure_retries_and_falls_back_to_thirty_seconds() {
    let rig = Rig::new("watch-fallback").await;
    rig.state.watch_up.store(false, Ordering::SeqCst);
    rig.commands.send(Command::Fail).unwrap();
    until(
        || rig.state.watches.load(Ordering::SeqCst) >= 3,
        Duration::from_secs(5),
        "failed watch was not resubscribed",
    )
    .await;
    rig.settled().await;
    let passes = rig.state.lists.load(Ordering::SeqCst);
    let attempts = rig.state.watches.load(Ordering::SeqCst);
    rig.state.declared.store(true, Ordering::SeqCst);
    until(
        || rig.state.lists.load(Ordering::SeqCst) > passes,
        Duration::from_secs(31),
        "watch failure did not restore the 30 second pass",
    )
    .await;
    rig.declared().await;
    assert_eq!(rig.select().await.0, FrameType::Response);
    assert!(rig.state.watches.load(Ordering::SeqCst) > attempts);
    assert!(
        rig.state.watches.load(Ordering::SeqCst) <= attempts + 6,
        "watch retries must back off instead of spinning"
    );
    rig.state.watch_up.store(true, Ordering::SeqCst);
    until(
        || rig.state.watches.load(Ordering::SeqCst) > attempts + 3,
        Duration::from_secs(32),
        "watch did not recover after its bounded backoff",
    )
    .await;
    rig.settled().await;
    rig.state.declared.store(false, Ordering::SeqCst);
    rig.push(vec![3]);
    until(
        || {
            rig.harness
                .credentials()
                .lock()
                .unwrap()
                .runner_declarations
                .is_empty()
        },
        Duration::from_secs(2),
        "recovered watch did not resume immediate refreshes",
    )
    .await;
}

#[tokio::test]
async fn catalog_watch_bursts_during_a_pass_coalesce_into_one_followup() {
    let rig = Rig::new("watch-burst").await;
    rig.state.declared.store(true, Ordering::SeqCst);
    rig.state.block_describe.store(true, Ordering::SeqCst);
    rig.push(vec![2]);
    until(
        || rig.state.describes.load(Ordering::SeqCst) == 1,
        Duration::from_secs(2),
        "watch did not start the blocked pass",
    )
    .await;
    let passes = rig.state.lists.load(Ordering::SeqCst);
    rig.push((3..=22).collect());
    until(
        || rig.state.pushes.load(Ordering::SeqCst) == 21,
        Duration::from_millis(300),
        "fake daemon did not send burst",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    rig.state.block_describe.store(false, Ordering::SeqCst);
    rig.commands.send(Command::Release).unwrap();
    until(
        || rig.state.describes.load(Ordering::SeqCst) >= 2,
        Duration::from_secs(2),
        "burst during publication was lost",
    )
    .await;
    rig.settled().await;
    assert_eq!(
        rig.state.lists.load(Ordering::SeqCst),
        passes + 1,
        "a burst must schedule exactly one follow-up pass"
    );
    let passes = rig.state.lists.load(Ordering::SeqCst);
    rig.push(vec![22, 22, 22]);
    rig.settled().await;
    assert_eq!(
        rig.state.lists.load(Ordering::SeqCst),
        passes,
        "duplicate generations must not trigger passes"
    );
}

#[tokio::test]
async fn catalog_watch_daemon_restart_rebuilds_consumer_and_refreshes() {
    let rig = Rig::new("watch-restart").await;
    let connections = rig.state.connections.load(Ordering::SeqCst);
    let watches = rig.state.watches.load(Ordering::SeqCst);
    rig.state.declared.store(true, Ordering::SeqCst);
    rig.commands.send(Command::Disconnect).unwrap();
    rig.declared().await;
    assert!(rig.state.connections.load(Ordering::SeqCst) > connections);
    until(
        || rig.state.watches.load(Ordering::SeqCst) > watches,
        Duration::from_secs(2),
        "restart did not replace the catalog feed",
    )
    .await;
    assert_eq!(rig.select().await.0, FrameType::Response);
}

#[tokio::test]
async fn runner_describe_misses_do_not_reconnect_or_reprobe_healthy_runners() {
    let rig = Rig::new("watch-runner-miss").await;
    let connections = rig.state.connections.load(Ordering::SeqCst);
    let watches = rig.state.watches.load(Ordering::SeqCst);
    let lists = rig.state.lists.load(Ordering::SeqCst);
    rig.state.declared.store(true, Ordering::SeqCst);
    rig.state.slow_runner.store(true, Ordering::SeqCst);
    rig.push(vec![2]);
    until(
        || rig.state.slow_describes.load(Ordering::SeqCst) >= 2,
        Duration::from_secs(10),
        "the slow runner was not retried",
    )
    .await;
    assert_eq!(
        rig.state.connections.load(Ordering::SeqCst),
        connections,
        "one runner's miss replaced the healthy catalog connection"
    );
    assert_eq!(rig.state.watches.load(Ordering::SeqCst), watches);
    assert_eq!(rig.state.lists.load(Ordering::SeqCst), lists + 1);
    assert_eq!(rig.state.describes.load(Ordering::SeqCst), 1);

    rig.state.catalog_fails.store(true, Ordering::SeqCst);
    rig.push(vec![3]);
    until(
        || rig.state.connections.load(Ordering::SeqCst) > connections,
        Duration::from_secs(10),
        "a failed catalog exchange did not reconnect",
    )
    .await;
}

#[tokio::test]
async fn malformed_classifier_describe_retries_without_a_catalog_push() {
    let rig = Rig::new("watch-classifier-retry").await;
    rig.state.classifier.store(true, Ordering::SeqCst);
    rig.push(vec![2]);
    until(
        || rig.state.classifier_describes.load(Ordering::SeqCst) == 1,
        Duration::from_secs(3),
        "the attested classifier was not queried",
    )
    .await;
    let lists = rig.state.lists.load(Ordering::SeqCst);
    let credentials = rig.harness.credentials();
    until(
        || {
            credentials
                .lock()
                .unwrap()
                .classifier_models
                .get("classifier")
                .is_some_and(|models| !models.is_empty())
        },
        Duration::from_secs(5),
        "the invalid classifier model list did not get a per-module retry",
    )
    .await;
    assert_eq!(rig.state.classifier_describes.load(Ordering::SeqCst), 2);
    assert_eq!(rig.state.lists.load(Ordering::SeqCst), lists);
}
