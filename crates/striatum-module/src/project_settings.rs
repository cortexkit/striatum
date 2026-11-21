//! Project routing settings resolved from Entorhinal's machine-local census.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cortexkit_project_identity::{
    EnumerateParams, EnumerateReply, Outcome, Request, ResolveProjectIdReply,
};
use cortexkit_role_routing::select::ProjectSettings;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::time::{timeout, timeout_at, Instant};

use crate::{config_policy::RunnerPolicy, tier_settings::Layers};

const REFRESH_MS: u64 = 60_000;
const MAX_STALE_MS: u64 = 10 * 60_000;
const SETTINGS_DEADLINE: Duration = Duration::from_secs(2);

enum ReadError {
    Timeout,
    Unavailable(String),
}

impl From<String> for ReadError {
    fn from(error: String) -> Self {
        Self::Unavailable(error)
    }
}

impl From<&str> for ReadError {
    fn from(error: &str) -> Self {
        Self::Unavailable(error.into())
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => out.write_str("Entorhinal settings deadline exceeded"),
            Self::Unavailable(error) => out.write_str(error),
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait Source: Send + Sync {
    async fn request(&self, body: Value) -> Result<Value, String>;
}

struct Entorhinal {
    connection: PathBuf,
    bind_root: PathBuf,
}

#[async_trait::async_trait]
impl Source for Entorhinal {
    async fn request(&self, body: Value) -> Result<Value, String> {
        let mut client = crate::quota_consumer::QuotaConsumer::connect_module(
            &self.connection,
            &self.bind_root,
            "entorhinal",
            "management_surface",
        )
        .await
        .map_err(|error| error.to_string())?;
        let bytes = client
            .operation_body(body)
            .await
            .map_err(|error| error.to_string())?;
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())
    }
}

struct Census {
    reply: EnumerateReply,
    raw: Value,
}

impl Census {
    fn decode(value: Value) -> Result<Self, String> {
        let outcome: Outcome<EnumerateReply> =
            serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
        let Outcome::Success(success) = outcome else {
            return Err("Entorhinal enumerate was refused".into());
        };
        let reply = success.result;
        if reply.generation < 0 {
            return Err("negative census generation".into());
        }
        Ok(Self {
            reply,
            raw: value["result"].clone(),
        })
    }

    fn identity(&self) -> (Value, Value) {
        (
            self.raw["incarnation"].clone(),
            self.raw["generation"].clone(),
        )
    }

    fn roots(&self, id: &str) -> Option<(Vec<PathBuf>, Option<PathBuf>)> {
        let project = self
            .reply
            .projects
            .iter()
            .find(|project| project.project_id == id)?;
        let projects = project
            .root_records
            .as_ref()
            .into_iter()
            .flatten()
            .filter(|record| {
                matches!(
                    record.identity,
                    cortexkit_project_identity::RootIdentity::Bound
                ) && String::from(record.approval.state.clone()) == "approved"
                    && project.roots.contains(&record.root)
            })
            .map(|record| PathBuf::from(&record.root))
            .collect::<Vec<_>>();
        let workspace = project.workspace_id.as_ref().and_then(|id| {
            self.reply
                .workspaces
                .iter()
                .find(|workspace| &workspace.workspace_id == id)
                .and_then(|workspace| workspace.root.as_ref())
                .map(PathBuf::from)
        });
        Some((projects, workspace))
    }
}

#[derive(Default)]
struct Cache {
    census: Option<Census>,
    last_attempt: Option<u64>,
    last_success: Option<u64>,
    resolved: HashMap<String, Option<String>>,
    missed: HashSet<String>,
    last_timeout: Option<Instant>,
    registry_unavailable: bool,
}

pub(crate) struct Resolver {
    source: Arc<dyn Source>,
    // The lock covers registry reads and publication. Concurrent requests share
    // one refresh rather than racing separate census/alias snapshots.
    cache: Mutex<Cache>,
    warned_projects: std::sync::Mutex<HashSet<String>>,
}

impl Resolver {
    pub(crate) fn new(source: Arc<dyn Source>) -> Self {
        Self {
            source,
            cache: Mutex::new(Cache::default()),
            warned_projects: Default::default(),
        }
    }

    async fn request(&self, body: Value) -> Result<Value, ReadError> {
        // Source::request includes connection setup, so its timeout also covers
        // a stall before the Entorhinal operation can be sent.
        timeout(SETTINGS_DEADLINE, self.source.request(body))
            .await
            .map_err(|_| ReadError::Timeout)?
            .map_err(ReadError::Unavailable)
    }

    fn warn_once(&self, project_id: &str, error: &ReadError) {
        let first_warning = self
            .warned_projects
            .lock()
            .unwrap()
            .insert(project_id.into());
        if first_warning {
            tracing::warn!(project_id, %error, "project settings unresolved");
        }
    }

    fn unresolved(&self, project_id: &str, error: &ReadError) -> (Layers, ProjectSettings) {
        self.warn_once(project_id, error);
        (Layers::new(None, None), ProjectSettings::Unresolved)
    }

    async fn refresh(&self, cache: &mut Cache, now: u64) -> Result<(), ReadError> {
        cache.last_attempt = Some(now);
        let body = serde_json::to_value(Request::new("enumerate", EnumerateParams::new(None)))
            .map_err(|error| error.to_string())?;
        let result =
            async { Census::decode(self.request(body).await?).map_err(ReadError::from) }.await;
        let census = match result {
            Ok(census) => census,
            Err(error) => {
                cache.registry_unavailable = true;
                return Err(error);
            }
        };
        if cache
            .census
            .as_ref()
            .is_none_or(|old| old.identity() != census.identity())
        {
            cache.resolved.clear();
            cache.missed.clear();
        }
        // Replace even at the same generation: roots and workspace folders are
        // machine-local facts and can change without a generation bump.
        cache.census = Some(census);
        cache.last_success = Some(now);
        cache.registry_unavailable = false;
        Ok(())
    }

    async fn canonical_id(
        &self,
        cache: &mut Cache,
        id: &str,
        now: u64,
    ) -> Result<Option<String>, ReadError> {
        if let Some(id) = cache.resolved.get(id) {
            return Ok(id.clone());
        }
        let value = match self
            .request(json!({"method":"resolve_project_id", "params":{"projectId":id}}))
            .await
        {
            Ok(value) => value,
            Err(error) => {
                cache.registry_unavailable = true;
                return Err(error);
            }
        };
        let outcome: Outcome<ResolveProjectIdReply> =
            serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
        let Outcome::Success(success) = outcome else {
            return Err("Entorhinal project id resolution was refused".into());
        };
        let identity = (
            value["result"]["incarnation"].clone(),
            value["result"]["generation"].clone(),
        );
        if cache
            .census
            .as_ref()
            .is_none_or(|census| census.identity() != identity)
        {
            self.refresh(cache, now).await?;
        }
        if cache
            .census
            .as_ref()
            .is_none_or(|census| census.identity() != identity)
        {
            return Err("Entorhinal census changed during id resolution".into());
        }
        let canonical = (!success.result.gone).then_some(success.result.project_id);
        if canonical.is_none() {
            tracing::warn!(project_id = id, "project settings id is gone");
        }
        cache.resolved.insert(id.to_owned(), canonical.clone());
        Ok(canonical)
    }

    #[cfg(test)]
    async fn layers(&self, project_id: &str, now: u64) -> (Layers, ProjectSettings) {
        self.enrolled_layers(project_id, now, &[]).await
    }

    async fn enrolled_layers(
        &self,
        project_id: &str,
        now: u64,
        enrolled: &[RunnerPolicy],
    ) -> (Layers, ProjectSettings) {
        let started = Instant::now();
        let deadline = started + SETTINGS_DEADLINE;
        let mut cache = match timeout_at(deadline, self.cache.lock()).await {
            Ok(cache) => cache,
            Err(_) => return self.unresolved(project_id, &ReadError::Timeout),
        };
        // Waiters share the timed-out flight's result instead of starting a
        // fresh two-second request when its owner releases the cache lock.
        if cache.last_timeout.is_some_and(|at| at >= started) {
            return self.unresolved(project_id, &ReadError::Timeout);
        }
        match timeout_at(
            deadline,
            self.layers_cached(&mut cache, project_id, now, enrolled),
        )
        .await
        {
            Ok(Ok((layers, applied))) => {
                if !cache.registry_unavailable {
                    self.warned_projects.lock().unwrap().remove(project_id);
                }
                (
                    layers,
                    if applied {
                        ProjectSettings::Applied
                    } else {
                        ProjectSettings::None
                    },
                )
            }
            Ok(Err(ReadError::Unavailable(error))) => {
                if error != "project id is gone" {
                    self.warn_once(project_id, &ReadError::Unavailable(error));
                }
                (Layers::new(None, None), ProjectSettings::Unresolved)
            }
            Ok(Err(ReadError::Timeout)) | Err(_) => {
                // Publish failure before unlocking, including when cancelling
                // layers_cached at its overall (rather than per-request) limit.
                cache.last_timeout = Some(Instant::now());
                cache.registry_unavailable = true;
                self.unresolved(project_id, &ReadError::Timeout)
            }
        }
    }

    async fn layers_cached(
        &self,
        cache: &mut Cache,
        project_id: &str,
        now: u64,
        enrolled: &[RunnerPolicy],
    ) -> Result<(Layers, bool), ReadError> {
        if cache.census.is_some()
            && cache
                .last_attempt
                .is_none_or(|at| now.saturating_sub(at) >= REFRESH_MS)
        {
            if let Err(error) = self.refresh(cache, now).await {
                if matches!(error, ReadError::Timeout) {
                    return Err(error);
                }
                self.warn_once(project_id, &error);
            }
        }
        let result = async {
            let Some(canonical) = self.canonical_id(cache, project_id, now).await? else {
                return Err("project id is gone".into());
            };
            if !cache
                .last_success
                .is_some_and(|at| now.saturating_sub(at) <= MAX_STALE_MS)
            {
                return Err("project settings census expired".into());
            }
            if cache
                .census
                .as_ref()
                .and_then(|census| census.roots(&canonical))
                .is_none()
                && cache.missed.insert(canonical.clone())
            {
                self.refresh(cache, now).await?;
                cache.missed.insert(canonical.clone());
            }
            let (projects, workspace) = cache
                .census
                .as_ref()
                .and_then(|census| census.roots(&canonical))
                .ok_or_else(|| "project id is absent from census".to_owned())?;
            Layers::for_enrolled_runners(&projects, workspace.as_deref(), enrolled)
                .map_err(ReadError::from)
        }
        .await;
        result
    }
}

static LIVE: OnceLock<Resolver> = OnceLock::new();

pub(crate) fn initialize(connection: &Path, bind_root: &Path) {
    let _ = LIVE.set(Resolver::new(Arc::new(Entorhinal {
        connection: connection.to_owned(),
        bind_root: bind_root.to_owned(),
    })));
}

/// Resolve a settings project id to file layers and `applied`, `none` or
/// `unresolved`. Only a daemon-stamped project id may supply runner overrides;
/// unscoped tier lookups discard them. The config path
/// identifies the caller's routing configuration in isolated test harnesses.
pub(crate) async fn layers(
    project: &str,
    _config: &Path,
    enrolled: &[RunnerPolicy],
) -> (Layers, ProjectSettings) {
    #[cfg(test)]
    {
        let resolver = test_sources().lock().unwrap().get(_config).cloned();
        if let Some(resolver) = resolver {
            return resolver
                .enrolled_layers(project, crate::now_ms(), enrolled)
                .await;
        }
    }
    match LIVE.get() {
        Some(resolver) => {
            resolver
                .enrolled_layers(project, crate::now_ms(), enrolled)
                .await
        }
        None => (Layers::new(None, None), ProjectSettings::Unresolved),
    }
}

#[cfg(test)]
fn test_sources() -> &'static std::sync::Mutex<HashMap<PathBuf, Arc<Resolver>>> {
    static SOURCES: OnceLock<std::sync::Mutex<HashMap<PathBuf, Arc<Resolver>>>> = OnceLock::new();
    SOURCES.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) fn set_test_source(config: &Path, source: Arc<dyn Source>) {
    test_sources()
        .lock()
        .unwrap()
        .insert(config.to_owned(), Arc::new(Resolver::new(source)));
}

#[cfg(test)]
pub(crate) mod tests;
