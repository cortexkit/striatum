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
use tokio::sync::{mpsc, watch};

use super::{Session, PROJECTS_CAPABILITY};
use crate::{Frame, FrameType, DEFAULT_MODULE_ID, PROTOCOL_VERSION};

pub(crate) const ID: &str = "pj-active";

#[derive(Default)]
struct State {
    projects: BTreeMap<String, Reply>,
    gets: BTreeMap<String, usize>,
    reports: Vec<ConfigAppliedRequest>,
    report_bytes: Vec<Vec<u8>>,
    global_gets: usize,
    describes: usize,
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

pub(crate) struct Connection(tokio::task::JoinHandle<()>);

impl Drop for Connection {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) enum Advertise {
    Hello,
    Describe,
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
        let state = Arc::new(Mutex::new(State::default()));
        let (replied_tx, replied) = watch::channel(0);
        let mut session = Session::start(
            module_id,
            path,
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
                                state.describes += 1;
                                let capabilities = match advertise {
                                    Advertise::Describe => vec![PROJECTS_CAPABILITY],
                                    Advertise::Refused => {
                                        assert!(session.dispatch(&response(&request, FrameType::Error, crate::ErrorBody::new("unknown_op", "no describe operation"))));
                                        continue;
                                    }
                                    Advertise::Failed => {
                                        assert!(session.dispatch(&response(&request, kind, json!({"op":"unexpected"}))));
                                        continue;
                                    }
                                    Advertise::Slow => continue,
                                    _ => vec![],
                                };
                                response(&request, kind, json!({"op":"server.describe", "capabilities":capabilities}))
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
        (daemon, Connection(connection))
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
