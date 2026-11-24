use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use subc_protocol::config::{
    ConfigGetReply, ConfigGetRequest, ConfigStatus, ConfigView, Layer, CONFIG_APPLIED_OP,
    CONFIG_GET_OP,
};
use tokio::time::Instant;

use super::session::{applied_request, Wire};
use crate::project_settings::Overrides;
use subc_client_rs::config::ConfigError;

const CALL_TIMEOUT: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const RETRY_DELAY: Duration = Duration::from_secs(1);

type RequestView = tokio::sync::OnceCell<Option<Arc<Overrides>>>;

/// A panel uses the same scoped view, or the same fallback, for every seat.
#[derive(Clone, Default)]
pub(crate) struct RequestViews(Arc<Mutex<HashMap<String, Arc<RequestView>>>>);

impl RequestViews {
    pub(crate) async fn get(&self, path: &Path, project: &str) -> Option<Arc<Overrides>> {
        let view = self
            .0
            .lock()
            .unwrap()
            .entry(project.into())
            .or_default()
            .clone();
        view.get_or_init(|| project_view(path, project))
            .await
            .clone()
    }
}

#[derive(Default)]
pub(super) struct Projects {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    enabled: bool,
    global: Option<Arc<ConfigView>>,
    projects: HashMap<String, Arc<Slot>>,
}

#[derive(Default)]
struct Slot {
    // A miss and a change notice for the same project must not race to publish
    // different views. Requests for other projects never wait for this lock.
    view: tokio::sync::Mutex<Cached>,
    // A copy of `Cached::last_good` that a timed-out request can read without
    // waiting for the refresh holding `view`.
    last_good: Mutex<Option<Arc<Overrides>>>,
    warnings: Mutex<Warnings>,
}

#[derive(Default)]
struct Cached {
    last_good: Option<Arc<Overrides>>,
    retry: Option<Retry>,
}

struct Retry {
    after: Instant,
    use_user_view: bool,
}

#[derive(Default)]
struct Warnings {
    unavailable: bool,
    unknown_project: bool,
    ignored_policy: bool,
    unknown: BTreeSet<String>,
}

fn sessions() -> &'static Mutex<HashMap<PathBuf, Weak<Wire>>> {
    static SESSIONS: OnceLock<Mutex<HashMap<PathBuf, Weak<Wire>>>> = OnceLock::new();
    SESSIONS.get_or_init(Default::default)
}

pub(crate) async fn project_view(path: &Path, project: &str) -> Option<Arc<Overrides>> {
    let wire = sessions().lock().unwrap().get(path)?.upgrade()?;
    let slot = wire
        .projects
        .state
        .lock()
        .unwrap()
        .projects
        .entry(project.into())
        .or_default()
        .clone();
    match tokio::time::timeout(REQUEST_TIMEOUT, slot.load(&wire, project, false)).await {
        Ok(view) => view,
        Err(_) => {
            slot.warn_unavailable(project, "project settings lookup timed out");
            // Refreshes queued ahead of this request can outlast its budget;
            // the last delivered view still applies, as it would after a
            // failed refresh.
            slot.last_good.lock().unwrap().clone()
        }
    }
}

impl Projects {
    pub(super) fn enable(&self, wire: &Arc<Wire>, path: &Path) {
        let mut state = self.state.lock().unwrap();
        state.enabled = true;
        if state.global.is_some() {
            register(path, wire);
        }
    }

    pub(super) fn set_global(&self, wire: &Arc<Wire>, path: &Path, view: Arc<ConfigView>) {
        let mut state = self.state.lock().unwrap();
        if state.global.replace(view).is_none() && state.enabled {
            register(path, wire);
        }
    }

    pub(super) fn refresh_cached(&self, wire: &Arc<Wire>, project: &str) {
        let slot = self.state.lock().unwrap().projects.get(project).cloned();
        if let Some(slot) = slot {
            let id = project.to_owned();
            let wire = Arc::clone(wire);
            tokio::spawn(async move {
                slot.load(&wire, &id, true).await;
            });
        }
    }
}

fn register(path: &Path, wire: &Arc<Wire>) {
    let mut sessions = sessions().lock().unwrap();
    sessions.retain(|_, wire| wire.strong_count() != 0);
    sessions.insert(path.into(), Arc::downgrade(wire));
}

impl Slot {
    async fn load(&self, wire: &Wire, project: &str, refresh: bool) -> Option<Arc<Overrides>> {
        let mut cache = self.view.lock().await;
        let view = self.load_locked(&mut cache, wire, project, refresh).await;
        self.last_good.lock().unwrap().clone_from(&cache.last_good);
        view
    }

    async fn load_locked(
        &self,
        cache: &mut Cached,
        wire: &Wire,
        project: &str,
        refresh: bool,
    ) -> Option<Arc<Overrides>> {
        if !refresh {
            match &cache.retry {
                None if cache.last_good.is_some() => return cache.last_good.clone(),
                Some(retry) if retry.after > Instant::now() => {
                    return if retry.use_user_view {
                        Some(Arc::default())
                    } else {
                        cache.last_good.clone()
                    };
                }
                _ => {}
            }
        }
        match fetch(wire, project).await {
            Ok(Delivery::Applied(view)) => {
                cache.last_good = Some(Arc::new(view));
                cache.retry = None;
            }
            Ok(Delivery::Rejected { active, reasons }) => {
                if cache.last_good.is_none() {
                    cache.last_good = active.map(Arc::new);
                }
                cache.retry = cache.last_good.is_none().then(|| Retry::later(false));
                self.warn_unavailable(project, &reasons.join("; "));
            }
            Ok(Delivery::UnknownProject) => {
                cache.last_good = None;
                cache.retry = Some(Retry::later(true));
                let mut warnings = self.warnings.lock().unwrap();
                if !std::mem::replace(&mut warnings.unknown_project, true) {
                    tracing::warn!(
                        project_id = project,
                        "unknown_project: using user routing settings"
                    );
                }
                return Some(Arc::default());
            }
            Err(reason) => {
                // A failed fetch does not withdraw the last delivered view.
                cache.retry = Some(Retry::later(false));
                self.warn_unavailable(project, &reason);
                return cache.last_good.clone();
            }
        }
        if let Some(view) = &cache.last_good {
            self.warn_about_view(project, view);
        }
        cache.last_good.clone()
    }

    fn warn_unavailable(&self, project_id: &str, reason: &str) {
        let mut warnings = self.warnings.lock().unwrap();
        if !std::mem::replace(&mut warnings.unavailable, true) {
            tracing::warn!(project_id, reason, "project settings unresolved");
        }
    }

    fn warn_about_view(&self, project_id: &str, view: &Overrides) {
        let mut warnings = self.warnings.lock().unwrap();
        if view.ignored_policy && !std::mem::replace(&mut warnings.ignored_policy, true) {
            tracing::warn!(
                project_id,
                "ignored user-only fields in project routing settings"
            );
        }
        for name in &view.names {
            if !crate::routing_v1::consumers::is_declared_or_builtin(name)
                && warnings.unknown.insert(name.clone())
            {
                tracing::warn!(
                    project_id,
                    consumer = name,
                    "unknown consumer in project routing settings"
                );
            }
        }
    }
}

impl Retry {
    fn later(use_user_view: bool) -> Self {
        Self {
            after: Instant::now() + RETRY_DELAY,
            use_user_view,
        }
    }
}

enum Delivery {
    Applied(Overrides),
    UnknownProject,
    Rejected {
        active: Option<Overrides>,
        reasons: Vec<String>,
    },
}

async fn fetch(wire: &Wire, project: &str) -> Result<Delivery, String> {
    let mut request = ConfigGetRequest::new();
    request.project_id = Some(project.into());
    let body = match wire.call(CONFIG_GET_OP, &request, CALL_TIMEOUT).await {
        Ok(body) => body,
        Err(ConfigError::Refused(error)) if error.code == "unknown_project" => {
            return Ok(Delivery::UnknownProject)
        }
        Err(error) => return Err(error.to_string()),
    };
    let reply: ConfigGetReply = serde_json::from_value(body).map_err(|error| error.to_string())?;
    let global = wire
        .projects
        .state
        .lock()
        .unwrap()
        .global
        .clone()
        .ok_or("no global settings")?;
    let decode = |view: &ConfigView| {
        if view.layers.iter().any(|layer| {
            let scope_ok = match layer.layer.known_layer() {
                Some(Layer::User) => {
                    layer.project_id.as_deref().is_none_or(|id| id == project)
                        && layer.workspace_id.is_none()
                }
                Some(Layer::Project) => layer.project_id.as_deref() == Some(project),
                Some(Layer::Workspace) => {
                    layer.project_id.as_deref().is_none_or(|id| id == project)
                }
                _ => false,
            };
            !scope_ok || layer.module_id != wire.module_id()
        }) {
            return Err(vec![
                "config.get returned invalid project settings layer identities".into(),
            ]);
        }
        let overrides = Overrides::read(&global.effective, view)?;
        // Without approved project settings the daemon returns the active
        // user view, tagged with the requested project ID.
        if view.status.is_active() && view.attempt.is_none() && !Overrides::has_scoped_layer(view) {
            return Ok(Overrides {
                ignored_policy: overrides.ignored_policy,
                ..Overrides::default()
            });
        }
        Ok(overrides)
    };
    let (view, recover) = if let Some(pending) = reply.pending {
        if pending.status != ConfigStatus::Pending || pending.attempt.is_none() {
            return Err("config.get returned no pending project delivery attempt".into());
        }
        (pending, reply.active)
    } else {
        let active = reply
            .active
            .ok_or("config.get returned no project settings view")?;
        if !active.status.is_active()
            && !(active.status == ConfigStatus::Unstamped
                && active.attempt.is_none()
                && active.generation == 0)
        {
            return Err(
                "config.get returned neither an active nor an unstamped project settings view"
                    .into(),
            );
        }
        (active, None)
    };
    if view.project_id.as_deref() != Some(project) {
        return Err("config.get returned a different project id".into());
    }
    let outcome = decode(&view);
    if let Some(attempt) = view.attempt {
        let report = applied_request(&view, attempt, &outcome);
        wire.call(CONFIG_APPLIED_OP, &report, CALL_TIMEOUT)
            .await
            .map_err(|error| error.to_string())?;
    }
    match outcome {
        Ok(project) => Ok(Delivery::Applied(project)),
        Err(reasons) if view.attempt.is_some() => Ok(Delivery::Rejected {
            active: recover
                .filter(|view| {
                    view.status.is_active() && view.project_id.as_deref() == Some(project)
                })
                .and_then(|view| decode(&view).ok()),
            reasons,
        }),
        Err(reasons) => Err(reasons.join("; ")),
    }
}
