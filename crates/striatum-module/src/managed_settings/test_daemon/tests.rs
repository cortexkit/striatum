use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};
use subc_protocol::config::{
    ConfigAppliedReply, ConfigAppliedRequest, ConfigChanged, ConfigDeclareReply, ConfigGetReply,
    ConfigGetRequest, ConfigLayer, ConfigLayerSnapshot, ConfigStatus, ConfigView,
    CONFIG_APPLIED_OP, CONFIG_DECLARE_OP, CONFIG_GET_OP,
};
use subc_transport::{authenticate_server, read_frame, write_frame};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use super::{Session, PROJECTS_CAPABILITY};
use crate::{Frame, FrameType, DEFAULT_MODULE_ID, PROTOCOL_VERSION};

pub(crate) const ID: &str = "pj-active";
const KEY: [u8; 32] = [7; 32];
const DAEMON_ID: [u8; 16] = [0; 16];
const DAEMON_VERSION: &str = "0.50.32";

#[derive(Default)]
struct State {
    projects: BTreeMap<String, Reply>,
    gets: BTreeMap<String, usize>,
    reports: Vec<ConfigAppliedRequest>,
    report_bytes: Vec<Vec<u8>>,
    global_gets: usize,
    describes: usize,
    module_describes: usize,
    consumer_connections: usize,
    advertise: Advertise,
}

enum Reply {
    Layer(Value),
    Global,
    Delivery(ConfigGetReply),
    Delayed,
    Refused,
    Captured(FrameType, &'static [u8]),
}

#[derive(Clone)]
pub(crate) struct Daemon {
    state: Arc<Mutex<State>>,
    hints: mpsc::UnboundedSender<Frame>,
    replied: watch::Receiver<usize>,
}

pub(crate) struct Connection(JoinHandle<()>, JoinHandle<()>);

impl Drop for Connection {
    fn drop(&mut self) {
        self.0.abort();
        self.1.abort();
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) enum Advertise {
    Hello,
    Describe,
    #[default]
    Neither,
    Refused,
    Failed,
    Slow,
}

impl Daemon {
    pub(crate) async fn connect(
        path: &Path,
        user: Value,
        advertise: Advertise,
    ) -> (Self, Connection) {
        Self::connect_as(DEFAULT_MODULE_ID, path, user, advertise).await
    }

    pub(crate) async fn connect_as(
        module_id: &str,
        path: &Path,
        user: Value,
        advertise: Advertise,
    ) -> (Self, Connection) {
        let (tx, mut rx) = mpsc::channel(16);
        let (hints, mut pushes) = mpsc::unbounded_channel();
        let state = Arc::new(Mutex::new(State {
            advertise,
            ..State::default()
        }));
        let (connection_file, consumers) = serve_consumers(path, Arc::clone(&state)).await;
        let (replied_tx, replied) = watch::channel(0);
        let mut session = Session::start(
            module_id,
            path,
            &connection_file,
            PROTOCOL_VERSION,
            &tx,
            matches!(advertise, Advertise::Hello),
        )
        .unwrap();
        let (ready_tx, ready) = tokio::sync::oneshot::channel();
        let daemon = Self {
            state: state.clone(),
            hints,
            replied,
        };
        let connection = tokio::spawn(async move {
            let mut ready_tx = Some(ready_tx);
            loop {
                let frame = tokio::select! {
                    req = rx.recv() => {
                        let request = req.unwrap();
                        let kind = FrameType::Response;
                        let value: Value = serde_json::from_slice(&request.body).unwrap();
                        let mut state = state.lock().unwrap();
                        match value["op"].as_str().unwrap() {
                            CONFIG_DECLARE_OP => response(&request, kind, ConfigDeclareReply::new()),
                            "server.describe" => {
                                assert_eq!((request.header.ty, request.header.channel), (FrameType::Request, 0));
                                state.module_describes += 1;
                                response(&request, FrameType::Error, crate::ErrorBody::new(
                                    "unsupported_control_frame",
                                    "module-originated channel-0 REQUEST is not supported",
                                ))
                            }
                            CONFIG_GET_OP => {
                                let get: ConfigGetRequest = serde_json::from_value(value).unwrap();
                                let Some(project) = get.project_id else {
                                    state.global_gets += 1;
                                    let reply = response(&request, kind, ConfigGetReply::new(Some(active(user.clone(), None)), None));
                                    assert!(session.dispatch(&reply));
                                    replied_tx.send_modify(|count| *count += 1);
                                    continue;
                                };
                                *state.gets.entry(project.clone()).or_default() += 1;
                                let frame = match state.projects.get(&project) {
                                    Some(Reply::Layer(layer)) => {
                                        response(&request, kind, ConfigGetReply::new(Some(project_view(&user, &project, layer)), None))
                                    }
                                    Some(Reply::Global) => {
                                        let mut view = active(user.clone(), Some(&project));
                                        view.status = ConfigStatus::Active;
                                        view.generation = 1;
                                        response(&request, kind, ConfigGetReply::new(Some(view), None))
                                    }
                                    Some(Reply::Delivery(reply)) => response(&request, kind, reply),
                                    Some(Reply::Delayed) => continue,
                                    Some(Reply::Refused) => response(&request, FrameType::Error, crate::ErrorBody::new("layer_forbidden", "invalid scoped settings")),
                                    Some(Reply::Captured(kind, bytes)) => raw_frame(*kind, request.header.corr, bytes.to_vec()),
                                    None => response(&request, FrameType::Error, crate::ErrorBody::new("unknown_project", "no such project")),
                                };
                                assert!(session.dispatch(&frame));
                                replied_tx.send_modify(|count| *count += 1);
                                continue;
                            }
                            CONFIG_APPLIED_OP => {
                                state.reports.push(serde_json::from_value(value).unwrap());
                                state.report_bytes.push(request.body.to_vec());
                                response(&request, kind, ConfigAppliedReply::new())
                            }
                            op => panic!("unexpected settings request: {op}"),
                        }
                    }
                    Some(hint) = pushes.recv() => hint,
                    result = session.ready(), if ready_tx.is_some() => {
                        ready_tx.take().unwrap().send(result).unwrap();
                        continue;
                    }
                };
                assert!(session.dispatch(&frame));
            }
        });
        tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        (daemon, Connection(connection, consumers))
    }

    pub(crate) fn set(&self, project: &str, layer: Value) {
        self.state
            .lock()
            .unwrap()
            .projects
            .insert(project.into(), Reply::Layer(layer));
    }

    pub(crate) fn deliver(&self, project: &str, reply: ConfigGetReply) {
        self.state
            .lock()
            .unwrap()
            .projects
            .insert(project.into(), Reply::Delivery(reply));
    }

    pub(crate) fn delay(&self, project: &str) {
        self.state
            .lock()
            .unwrap()
            .projects
            .insert(project.into(), Reply::Delayed);
    }

    pub(crate) fn refuse(&self, project: &str) {
        self.state
            .lock()
            .unwrap()
            .projects
            .insert(project.into(), Reply::Refused);
    }

    pub(crate) fn captured_reply(&self, project: &str, kind: FrameType, body: &'static [u8]) {
        self.state
            .lock()
            .unwrap()
            .projects
            .insert(project.into(), Reply::Captured(kind, body));
    }

    pub(crate) fn no_overrides(&self, project: &str) {
        self.state
            .lock()
            .unwrap()
            .projects
            .insert(project.into(), Reply::Global);
    }

    pub(crate) fn gets(&self, project: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .gets
            .get(project)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn describes(&self) -> usize {
        self.state.lock().unwrap().describes
    }

    pub(crate) fn module_describes(&self) -> usize {
        self.state.lock().unwrap().module_describes
    }

    pub(crate) fn consumer_connections(&self) -> usize {
        self.state.lock().unwrap().consumer_connections
    }

    pub(crate) fn advertise_on_consumer(&self) {
        self.state.lock().unwrap().advertise = Advertise::Describe;
    }

    pub(crate) fn global_gets(&self) -> usize {
        self.state.lock().unwrap().global_gets
    }

    pub(crate) fn reports(&self) -> Vec<ConfigAppliedRequest> {
        self.state.lock().unwrap().reports.clone()
    }

    pub(crate) fn report_bytes(&self) -> Vec<Vec<u8>> {
        self.state.lock().unwrap().report_bytes.clone()
    }

    pub(crate) async fn changed(&self, project: &str) {
        self.change(Some(project)).await;
    }

    pub(crate) async fn global_changed(&self) {
        self.change(None).await;
    }

    async fn change(&self, project: Option<&str>) {
        let mut notice = ConfigChanged::new(0, vec![]);
        notice.project_id = project.map(str::to_owned);
        self.push(frame(FrameType::Push, 0, notice)).await;
    }

    pub(crate) async fn captured_change(&self, body: &[u8]) {
        self.push(raw_frame(FrameType::Push, 0, body.to_vec()))
            .await;
    }

    async fn push(&self, hint: Frame) {
        let mut replied = self.replied.clone();
        replied.borrow_and_update();
        self.hints.send(hint).unwrap();
        tokio::time::timeout(Duration::from_secs(5), replied.changed())
            .await
            .unwrap()
            .unwrap();
    }
}

async fn serve_consumers(
    path: &Path,
    state: Arc<Mutex<State>>,
) -> (std::path::PathBuf, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connection = path.with_extension("connection.json");
    std::fs::write(&connection, serde_json::to_vec(&json!({
        "schema":1,"endpoints":[{"host":"127.0.0.1","port":listener.local_addr().unwrap().port()}],
        "key":KEY,"daemon_id":DAEMON_ID,"pid":std::process::id(),"daemon_ver":DAEMON_VERSION,
    })).unwrap()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&connection, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let task = tokio::spawn(async move {
        let mut peers = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    state.lock().unwrap().consumer_connections += 1;
                    peers.spawn(serve_consumer(stream, Arc::clone(&state)));
                }
                done = peers.join_next(), if !peers.is_empty() => done.unwrap().unwrap(),
            }
        }
    });
    (connection, task)
}

async fn serve_consumer(mut stream: TcpStream, state: Arc<Mutex<State>>) {
    authenticate_server(
        &mut stream,
        &KEY,
        &DAEMON_ID,
        DAEMON_VERSION,
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let request = read_frame(&mut stream).await.unwrap().unwrap();
    assert_eq!(
        (
            request.header.ty,
            request.header.channel,
            request.header.epoch
        ),
        (FrameType::Request, 0, 0)
    );
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["op"], "server.describe");
    let advertise = {
        let mut state = state.lock().unwrap();
        state.describes += 1;
        state.advertise
    };
    let capabilities = match advertise {
        Advertise::Describe => json!([PROJECTS_CAPABILITY]),
        Advertise::Failed => json!("not an array"),
        Advertise::Slow => {
            assert!(read_frame(&mut stream).await.unwrap().is_none());
            return;
        }
        Advertise::Refused => {
            let reply = response(
                &request,
                FrameType::Error,
                crate::ErrorBody::new("unavailable", "describe unavailable"),
            );
            write_frame(&mut stream, &reply).await.unwrap();
            return;
        }
        _ => json!([]),
    };
    let reply = response(
        &request,
        FrameType::Response,
        json!({
            "op":"server.describe", "protocol_ver":PROTOCOL_VERSION, "subc_ops":[],
            "capabilities":capabilities, "connected_clients":1,
        }),
    );
    write_frame(&mut stream, &reply).await.unwrap();
}

pub(crate) fn active(effective: Value, project: Option<&str>) -> ConfigView {
    let mut view = ConfigView::new(
        0,
        effective,
        BTreeMap::new(),
        vec![],
        ConfigStatus::Unstamped,
    );
    view.project_id = project.map(str::to_owned);
    view
}

pub(crate) fn project_view(user: &Value, project: &str, layer: &Value) -> ConfigView {
    fn merge(merged: &mut Value, overlay: &Value) {
        if let (Some(dst), Some(src)) = (merged.as_object_mut(), overlay.as_object()) {
            for (key, value) in src {
                merge(dst.entry(key).or_insert(Value::Null), value);
            }
        } else {
            *merged = overlay.clone();
        }
    }
    let mut view = active(user.clone(), Some(project));
    merge(&mut view.effective, layer);
    let mut snapshot = ConfigLayerSnapshot::file(
        DEFAULT_MODULE_ID,
        ConfigLayer::Project,
        "/ignored/project/striatum.jsonc",
        Some(serde_json::to_vec(layer).unwrap()),
    );
    snapshot.project_id = Some(project.into());
    view.layers.push(snapshot);
    view
}

fn response(request: &Frame, kind: FrameType, body: impl Serialize) -> Frame {
    frame(kind, request.header.corr, body)
}

fn frame(kind: FrameType, corr: u64, body: impl Serialize) -> Frame {
    raw_frame(kind, corr, serde_json::to_vec(&body).unwrap())
}

fn raw_frame(kind: FrameType, corr: u64, body: Vec<u8>) -> Frame {
    Frame::build(kind, crate::control_flags(), 0, 0, corr, body).unwrap()
}
