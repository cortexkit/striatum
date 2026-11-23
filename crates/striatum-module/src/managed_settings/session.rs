use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use subc_client_rs::config::{
    ConfigAppliedRequest, ConfigApplyResult, ConfigChanged, ConfigError, ConfigGetReply,
    ConfigGetRequest, ConfigReviewRequest, ConfigStatus, ConfigView, CONFIG_APPLIED_OP,
    CONFIG_CHANGED_OP, CONFIG_DECLARE_OP, CONFIG_GET_OP, CONFIG_REVIEW_OP,
};
use subc_client_rs::{ConsumerOptions, SubcConsumer};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;

use super::{projects::Projects, Settings};
use crate::{control_flags, send, ErrorBody, Frame, FrameType, ModuleError};

pub(crate) const WAITING: &str = "waiting for the daemon's settings view";
// The locked SDK predates the exported name for this daemon capability.
pub(crate) const PROJECTS_CAPABILITY: &str = "config.settings.projects/v1";
const PROJECT_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_INITIAL: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(60);

struct RetryDelay(Duration);

impl RetryDelay {
    fn new() -> Self {
        Self(RETRY_INITIAL)
    }

    async fn wait(&mut self) {
        tokio::time::sleep(self.0).await;
        self.0 = self.0.saturating_mul(2).min(RETRY_MAX);
    }
}

pub(crate) struct Session {
    wire: Arc<Wire>,
    ready: oneshot::Receiver<Result<(), ModuleError>>,
    task: JoinHandle<()>,
}

impl Session {
    pub(crate) fn start(
        module_id: &str,
        path: &Path,
        connection_file: &Path,
        negotiated_ver: u8,
        writer: &mpsc::Sender<Frame>,
        projects_advertised: bool,
    ) -> Result<Self, ModuleError> {
        let settings = Settings {
            module_id: module_id.into(),
            path: path.into(),
        };
        let declaration = settings.config_declaration()?;
        let wire = Arc::new(Wire {
            settings: Arc::new(settings),
            ver: negotiated_ver,
            writer: writer.clone(),
            pending: Mutex::new(HashMap::new()),
            next_corr: AtomicU64::new(crate::HELLO_CORR + 1),
            projects: Projects::default(),
            changed: Notify::new(),
        });
        let (ready_tx, ready) = oneshot::channel();
        let mut driver = Driver {
            wire: Arc::clone(&wire),
            ready: Some(ready_tx),
            has_view: false,
            report: None,
        };
        crate::info_operator(format!("[striatum] {WAITING}"));
        let connection_file = connection_file.to_owned();
        let task = tokio::spawn(async move {
            enum InitialGet {
                Fetch,
                Probed(Result<ConfigGetReply, ConfigError>),
                WaitForChange,
            }

            let mut retry = RetryDelay::new();
            let initial_get = loop {
                match driver.call(CONFIG_DECLARE_OP, &declaration).await {
                    Ok(_) => break InitialGet::Fetch,
                    Err(error @ ConfigError::Timeout) => {
                        log_error(CONFIG_DECLARE_OP, &error);
                        retry.wait().await;
                        // A lost reply does not cancel the declaration. The daemon
                        // accepts one per connection and checks for it before
                        // config.get can return a view or report gated delivery.
                        match driver.get_with_retry(&mut retry).await {
                            Err(ConfigError::Refused(refusal))
                                if refusal.code == "declaration_order" =>
                            {
                                retry.wait().await;
                            }
                            reply => break InitialGet::Probed(reply),
                        }
                    }
                    Err(ConfigError::Refused(refusal)) => {
                        // Older deployments can advertise the gate before accepting routing's schema.
                        crate::warn_operator(format!(
                            "[striatum] config.declare refused ({}: {}); keeping the routing settings file reader",
                            refusal.code, refusal.message,
                        ));
                        crate::info_operator(
                            "[striatum] settings source: file (daemon refused config.declare)"
                                .into(),
                        );
                        crate::info_operator("[striatum] project settings disabled: managed settings are unavailable".into());
                        let result = load_files(&driver.wire.settings.path).await;
                        driver.finish_wait(result);
                        return;
                    }
                    Err(error) => {
                        log_error(CONFIG_DECLARE_OP, &error);
                        break InitialGet::WaitForChange;
                    }
                }
            };
            crate::info_operator(
                "[striatum] settings source: managed (daemon advertised config.gate)".into(),
            );
            let wire = Arc::clone(&driver.wire);
            // Older daemons advertise the project protocol in server.describe
            // only. That probe must not delay serving the global settings view.
            tokio::join!(biased;
                wire.discover_projects(projects_advertised, &connection_file),
                async {
                    match initial_get {
                        InitialGet::Fetch => driver.refresh().await,
                        InitialGet::Probed(reply) => driver.apply_reply(reply).await,
                        InitialGet::WaitForChange => {}
                    }
                    loop {
                        driver.wire.changed.notified().await;
                        driver.refresh().await;
                    }
                },
            );
        });
        Ok(Self { wire, ready, task })
    }

    /// Route only channel-0 settings messages to the driver. Review and apply
    /// use the blocking pool so other control messages keep getting replies.
    pub(crate) fn dispatch(&self, frame: &Frame) -> bool {
        if frame.header.channel != 0 || frame.header.epoch != 0 || self.task.is_finished() {
            return false;
        }
        match frame.header.ty {
            FrameType::Response | FrameType::Error => {
                if let Some(response) = self.wire.pending.lock().unwrap().remove(&frame.header.corr)
                {
                    let _ = response.send(frame.clone());
                    return true;
                }
            }
            FrameType::Push if has_op(frame, CONFIG_CHANGED_OP) => {
                match serde_json::from_slice::<ConfigChanged>(&frame.body) {
                    Ok(hint) => match hint.project_id {
                        Some(project) => self.wire.projects.refresh_cached(&self.wire, &project),
                        None => self.wire.changed.notify_one(),
                    },
                    Err(error) => tracing::warn!(%error, "ignoring malformed config.changed"),
                }
                return true;
            }
            FrameType::Request if has_op(frame, CONFIG_REVIEW_OP) => {
                let frame = frame.clone();
                let wire = Arc::clone(&self.wire);
                tokio::spawn(async move {
                    if let Err(error) = wire.review(frame).await {
                        tracing::warn!(%error, "could not reply to config.review");
                    }
                });
                return true;
            }
            _ => {}
        }
        false
    }

    pub(crate) async fn ready(&mut self) -> Result<(), ModuleError> {
        (&mut self.ready).await.map_err(|_| {
            ModuleError::Message("managed settings task ended before routing was ready".into())
        })?
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) struct Wire {
    settings: Arc<Settings>,
    ver: u8,
    writer: mpsc::Sender<Frame>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Frame>>>,
    next_corr: AtomicU64,
    pub(super) projects: Projects,
    changed: Notify,
}

impl Wire {
    async fn discover_projects(self: &Arc<Self>, advertised: bool, connection_file: &Path) {
        if advertised {
            self.projects.enable(self, &self.settings.path);
            return;
        }
        let mut retry = RetryDelay::new();
        let mut logged_disabled = false;
        loop {
            match probe_projects(connection_file).await {
                Ok(true) => {
                    self.projects.enable(self, &self.settings.path);
                    return;
                }
                Ok(false) => {
                    if !logged_disabled {
                        crate::info_operator("[striatum] project settings disabled: daemon did not advertise project settings".into());
                    }
                    return;
                }
                Err(error) => {
                    if !logged_disabled {
                        crate::info_operator(format!("[striatum] project settings disabled: consumer capability probe failed ({error}); will retry"));
                        logged_disabled = true;
                    }
                    retry.wait().await;
                }
            }
        }
    }

    async fn review(&self, frame: Frame) -> Result<(), ModuleError> {
        let (kind, body) = match serde_json::from_slice::<ConfigReviewRequest>(&frame.body) {
            Ok(request) => {
                let settings = Arc::clone(&self.settings);
                let reply = tokio::task::spawn_blocking(move || settings.config_review(request))
                    .await
                    .map_err(|error| ModuleError::Message(format!("config.review: {error}")))?;
                (FrameType::Response, serde_json::to_vec(&reply))
            }
            Err(_) => (
                FrameType::Error,
                serde_json::to_vec(&ErrorBody::new(
                    "invalid_request",
                    "invalid config.review snapshots",
                )),
            ),
        };
        let reply = control_frame(
            frame.header.ver,
            kind,
            frame.header.corr,
            body.map_err(ModuleError::Json)?,
        )?;
        send(&self.writer, reply).await
    }
}

async fn probe_projects(connection_file: &Path) -> Result<bool, String> {
    // The module connection rejects server.describe, and the runner refresh
    // consumer only starts after global settings are ready.
    tokio::time::timeout(PROJECT_PROBE_TIMEOUT, async {
        let consumer = SubcConsumer::connect(connection_file, ConsumerOptions::default())
            .await
            .map_err(|error| error.to_string())?;
        let describe = consumer
            .server_describe()
            .await
            .map_err(|error| error.to_string())?;
        Ok(describe
            .capabilities
            .iter()
            .any(|cap| cap == PROJECTS_CAPABILITY))
    })
    .await
    .map_err(|_| "server.describe timed out".to_owned())?
}

struct Driver {
    wire: Arc<Wire>,
    ready: Option<oneshot::Sender<Result<(), ModuleError>>>,
    has_view: bool,
    report: Option<ConfigAppliedRequest>,
}

impl Driver {
    fn finish_wait(&mut self, result: Result<(), ModuleError>) {
        if let Some(ready) = self.ready.take() {
            if result.is_ok() {
                crate::info_operator("[striatum] routing settings are ready".to_owned());
            }
            let _ = ready.send(result);
        }
    }

    async fn refresh(&mut self) {
        if !self.send_report().await {
            return;
        }
        let reply = self.get_with_retry(&mut RetryDelay::new()).await;
        self.apply_reply(reply).await;
    }

    async fn get_with_retry(
        &mut self,
        retry: &mut RetryDelay,
    ) -> Result<ConfigGetReply, ConfigError> {
        let request = ConfigGetRequest::new();
        loop {
            match self.call(CONFIG_GET_OP, &request).await {
                Err(error @ ConfigError::Timeout) => {
                    log_error(CONFIG_GET_OP, &error);
                    retry.wait().await;
                }
                result => {
                    return result.and_then(|body| {
                        serde_json::from_value(body)
                            .map_err(|error| ConfigError::Protocol(error.to_string()))
                    });
                }
            }
        }
    }

    async fn apply_reply(&mut self, reply: Result<ConfigGetReply, ConfigError>) {
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                log_error(CONFIG_GET_OP, &error);
                return;
            }
        };
        if let Some(pending) = reply.pending {
            let Some(attempt) = pending.attempt.filter(|_| {
                pending.status == ConfigStatus::Pending && pending.project_id.is_none()
            }) else {
                tracing::warn!("config.get did not return a global pending delivery attempt");
                return;
            };
            if !self.has_view {
                // Recover the approved view first so a rejected pending generation
                // cannot leave routing without settings it could already run on.
                if let Some(active) = reply.active {
                    if active.status != ConfigStatus::Active || self.apply(active).await.is_err() {
                        tracing::warn!("could not recover the active routing settings view");
                        return;
                    }
                }
            }
            self.apply_and_report(pending, attempt).await;
        } else if let Some(active) = reply.active {
            if active.project_id.is_some()
                || !matches!(
                    active.status,
                    ConfigStatus::Active | ConfigStatus::Unstamped
                )
            {
                tracing::warn!(
                    "config.get returned neither an active nor an unstamped routing view"
                );
            } else if let Some(attempt) = active.attempt.filter(|_| active.status.is_active()) {
                // A changed declaration can receive the old approved view with
                // a connection-local delivery attempt of its own.
                self.apply_and_report(active, attempt).await;
            } else if let Err(reasons) = self.apply(active).await {
                tracing::warn!(?reasons, "routing settings view was rejected");
            }
        } else {
            tracing::warn!("config.get returned no routing settings view");
        }
    }

    async fn apply_and_report(&mut self, view: ConfigView, attempt: u64) {
        let result = self.apply(view.clone()).await;
        self.report = Some(applied_request(&view, attempt, &result));
        self.send_report().await;
    }

    async fn apply(&mut self, view: ConfigView) -> Result<(), Vec<String>> {
        let settings = Arc::clone(&self.wire.settings);
        let view = Arc::new(view);
        let delivered = Arc::clone(&view);
        tokio::task::spawn_blocking(move || settings.config_apply(&delivered))
            .await
            .map_err(|error| vec![format!("config.apply: {error}")])??;
        self.wire
            .projects
            .set_global(&self.wire, &self.wire.settings.path, view);
        self.has_view = true;
        self.finish_wait(Ok(()));
        Ok(())
    }

    async fn send_report(&mut self) -> bool {
        let Some(report) = self.report.take() else {
            return true;
        };
        match self.call(CONFIG_APPLIED_OP, &report).await {
            Ok(_) => true,
            Err(error) => {
                if matches!(&error, ConfigError::Refused(refusal) if refusal.code == "anchor_pending")
                {
                    self.report = Some(report);
                }
                log_error(CONFIG_APPLIED_OP, &error);
                false
            }
        }
    }

    async fn call(&mut self, op: &str, body: &impl Serialize) -> Result<Value, ConfigError> {
        self.wire.call(op, body, CALL_TIMEOUT).await
    }
}

pub(super) fn applied_request<T>(
    view: &ConfigView,
    attempt: u64,
    outcome: &Result<T, Vec<String>>,
) -> ConfigAppliedRequest {
    let (result, reasons) = match outcome {
        Ok(_) => (ConfigApplyResult::Ack, vec![]),
        Err(reasons) => (ConfigApplyResult::Nack, reasons.clone()),
    };
    let mut report = ConfigAppliedRequest::new(view.generation, attempt, result, reasons);
    report.project_id.clone_from(&view.project_id);
    report
}

impl Wire {
    pub(super) fn module_id(&self) -> &str {
        &self.settings.module_id
    }

    pub(super) async fn call(
        &self,
        op: &str,
        body: &impl Serialize,
        timeout: Duration,
    ) -> Result<Value, ConfigError> {
        let corr = self
            .next_corr
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |corr| {
                corr.checked_add(1)
            })
            .map_err(|_| ConfigError::ConnectionClosed)?;
        let body =
            serde_json::to_vec(body).map_err(|error| ConfigError::Protocol(error.to_string()))?;
        let request = control_frame(self.ver, FrameType::Request, corr, body)
            .map_err(|error| ConfigError::Protocol(error.to_string()))?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(corr, tx);
        // An abandoned request must release its correlation slot too.
        struct Pending<'a>(&'a Wire, u64);
        impl Drop for Pending<'_> {
            fn drop(&mut self) {
                self.0.pending.lock().unwrap().remove(&self.1);
            }
        }
        let _pending = Pending(self, corr);
        let reply = tokio::time::timeout(timeout, async {
            send(&self.writer, request)
                .await
                .map_err(|_| ConfigError::ConnectionClosed)?;
            rx.await.map_err(|_| ConfigError::ConnectionClosed)
        })
        .await;
        let reply = reply.map_err(|_| ConfigError::Timeout)??;
        if reply.header.ty == FrameType::Error {
            let error = serde_json::from_slice(&reply.body)
                .map_err(|err| ConfigError::Protocol(err.to_string()))?;
            return Err(ConfigError::Refused(error));
        }
        let body: Value = serde_json::from_slice(&reply.body)
            .map_err(|error| ConfigError::Protocol(error.to_string()))?;
        if body.get("op").and_then(Value::as_str) != Some(op) {
            return Err(ConfigError::Protocol(format!("unexpected reply to {op}")));
        }
        Ok(body)
    }
}

async fn load_files(path: &Path) -> Result<(), ModuleError> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || Settings::config_files(&path))
        .await
        .map_err(|error| ModuleError::Message(format!("reading routing settings: {error}")))?
}

fn control_frame(ver: u8, kind: FrameType, corr: u64, body: Vec<u8>) -> Result<Frame, ModuleError> {
    Frame::build_with_version(ver, kind, control_flags(), 0, 0, corr, body)
        .map_err(|error| ModuleError::Message(error.to_string()))
}

fn has_op(frame: &Frame, op: &str) -> bool {
    serde_json::from_slice::<Value>(&frame.body)
        .is_ok_and(|body| body.get("op").and_then(Value::as_str) == Some(op))
}

fn log_error(op: &str, error: &ConfigError) {
    tracing::warn!(op, %error, "managed routing settings exchange failed");
}
