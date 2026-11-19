use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use subc_client_rs::config::{
    ConfigAppliedRequest, ConfigApplyResult, ConfigChanged, ConfigError, ConfigGetReply,
    ConfigGetRequest, ConfigReviewRequest, ConfigStatus, ConfigView, CONFIG_APPLIED_OP,
    CONFIG_CHANGED_OP, CONFIG_DECLARE_OP, CONFIG_GATE_CAPABILITY, CONFIG_GET_OP, CONFIG_REVIEW_OP,
};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;

use super::Settings;
use crate::{control_flags, send, ErrorBody, Frame, FrameType, ModuleError, ModuleHelloAckBody};

pub(crate) const WAITING: &str = "waiting for the daemon's settings view";
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct Session {
    wire: Arc<Wire>,
    ready: oneshot::Receiver<Result<(), ModuleError>>,
    task: JoinHandle<()>,
}

impl Session {
    pub(crate) async fn start(
        enabled: bool,
        module_id: &str,
        path: &Path,
        ack: &ModuleHelloAckBody,
        writer: &mpsc::Sender<Frame>,
    ) -> Result<Option<Self>, ModuleError> {
        if !enabled {
            return Ok(None);
        }
        if !ack
            .subc_capabilities
            .iter()
            .any(|capability| capability == CONFIG_GATE_CAPABILITY)
        {
            load_files(path).await?;
            return Ok(None);
        }
        let settings = Settings {
            module_id: module_id.into(),
            path: path.into(),
        };
        let declaration = settings.config_declaration()?;
        let wire = Arc::new(Wire {
            settings: Arc::new(settings),
            ver: ack.negotiated_ver,
            writer: writer.clone(),
            pending: Mutex::new(None),
            changed: Notify::new(),
        });
        let (ready_tx, ready) = oneshot::channel();
        let mut driver = Driver {
            wire: Arc::clone(&wire),
            next_corr: crate::HELLO_CORR + 1,
            ready: Some(ready_tx),
            has_view: false,
            report: None,
        };
        crate::info_operator(format!("[striatum] {WAITING}"));
        let task = tokio::spawn(async move {
            match driver.call(CONFIG_DECLARE_OP, &declaration).await {
                Ok(_) => driver.refresh().await,
                Err(ConfigError::Refused(refusal)) => {
                    // Older deployments can advertise the gate before accepting routing's schema.
                    crate::warn_operator(format!(
                        "[striatum] config.declare refused ({}: {}); keeping the routing settings file reader",
                        refusal.code, refusal.message,
                    ));
                    let result = load_files(&driver.wire.settings.path).await;
                    driver.finish_wait(result);
                    return;
                }
                Err(error) => log_error(CONFIG_DECLARE_OP, &error),
            }
            loop {
                driver.wire.changed.notified().await;
                driver.refresh().await;
            }
        });
        Ok(Some(Self { wire, ready, task }))
    }

    /// Route only channel-0 settings messages to the driver. Review and apply
    /// use the blocking pool so other control messages keep getting replies.
    pub(crate) fn dispatch(&self, frame: &Frame) -> bool {
        if frame.header.channel != 0 || frame.header.epoch != 0 || self.task.is_finished() {
            return false;
        }
        match frame.header.ty {
            FrameType::Response | FrameType::Error => {
                let mut pending = self.wire.pending.lock().unwrap();
                if pending
                    .as_ref()
                    .is_some_and(|(corr, _)| *corr == frame.header.corr)
                {
                    let (_, response) = pending.take().unwrap();
                    let _ = response.send(frame.clone());
                    return true;
                }
            }
            FrameType::Push if has_op(frame, CONFIG_CHANGED_OP) => {
                match serde_json::from_slice::<ConfigChanged>(&frame.body) {
                    Ok(hint) if hint.project_id.is_none() => self.wire.changed.notify_one(),
                    Ok(_) => {}
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

struct Wire {
    settings: Arc<Settings>,
    ver: u8,
    writer: mpsc::Sender<Frame>,
    pending: Mutex<Option<(u64, oneshot::Sender<Frame>)>>,
    changed: Notify,
}

impl Wire {
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

struct Driver {
    wire: Arc<Wire>,
    next_corr: u64,
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
        let reply = self
            .call(CONFIG_GET_OP, &ConfigGetRequest::new())
            .await
            .and_then(|body| {
                serde_json::from_value::<ConfigGetReply>(body)
                    .map_err(|error| ConfigError::Protocol(error.to_string()))
            });
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
        let generation = view.generation;
        let (result, reasons) = match self.apply(view).await {
            Ok(()) => (ConfigApplyResult::Ack, vec![]),
            Err(reasons) => (ConfigApplyResult::Nack, reasons),
        };
        self.report = Some(ConfigAppliedRequest::new(
            generation, attempt, result, reasons,
        ));
        self.send_report().await;
    }

    async fn apply(&mut self, view: ConfigView) -> Result<(), Vec<String>> {
        let settings = Arc::clone(&self.wire.settings);
        tokio::task::spawn_blocking(move || settings.config_apply(&view))
            .await
            .map_err(|error| vec![format!("config.apply: {error}")])??;
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
        let corr = self.next_corr;
        self.next_corr = corr.checked_add(1).ok_or(ConfigError::ConnectionClosed)?;
        let body =
            serde_json::to_vec(body).map_err(|error| ConfigError::Protocol(error.to_string()))?;
        let request = control_frame(self.wire.ver, FrameType::Request, corr, body)
            .map_err(|error| ConfigError::Protocol(error.to_string()))?;
        let (tx, rx) = oneshot::channel();
        *self.wire.pending.lock().unwrap() = Some((corr, tx));
        let reply = tokio::time::timeout(CALL_TIMEOUT, async {
            send(&self.wire.writer, request)
                .await
                .map_err(|_| ConfigError::ConnectionClosed)?;
            rx.await.map_err(|_| ConfigError::ConnectionClosed)
        })
        .await;
        self.wire.pending.lock().unwrap().take();
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
