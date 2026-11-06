//! Project tier settings resolved from Entorhinal's machine-local census.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use cortexkit_project_identity::{
    EnumerateParams, EnumerateReply, Outcome, Request, ResolveProjectIdReply,
};
use cortexkit_role_routing::select::ProjectSettings;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::tier_settings::Layers;

const REFRESH_MS: u64 = 60_000;
const MAX_STALE_MS: u64 = 10 * 60_000;

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
}

pub(crate) struct Resolver {
    source: Arc<dyn Source>,
    // The lock covers registry reads and publication. Concurrent requests share
    // one refresh rather than racing separate census/alias snapshots.
    cache: Mutex<Cache>,
}

impl Resolver {
    pub(crate) fn new(source: Arc<dyn Source>) -> Self {
        Self {
            source,
            cache: Mutex::new(Cache::default()),
        }
    }

    async fn refresh(&self, cache: &mut Cache, now: u64) -> Result<(), String> {
        cache.last_attempt = Some(now);
        let body = serde_json::to_value(Request::new("enumerate", EnumerateParams::new(None)))
            .map_err(|error| error.to_string())?;
        let census = Census::decode(self.source.request(body).await?)?;
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
        Ok(())
    }

    async fn canonical_id(
        &self,
        cache: &mut Cache,
        id: &str,
        now: u64,
    ) -> Result<Option<String>, String> {
        if let Some(id) = cache.resolved.get(id) {
            return Ok(id.clone());
        }
        let value = self
            .source
            .request(json!({"method":"resolve_project_id", "params":{"projectId":id}}))
            .await?;
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

    pub(crate) async fn layers(&self, project_id: &str, now: u64) -> (Layers, ProjectSettings) {
        let mut cache = self.cache.lock().await;
        if cache.census.is_some()
            && cache
                .last_attempt
                .is_none_or(|at| now.saturating_sub(at) >= REFRESH_MS)
        {
            if let Err(error) = self.refresh(&mut cache, now).await {
                tracing::warn!(project_id, %error, "project settings census unavailable");
            }
        }
        let result = async {
            let Some(canonical) = self.canonical_id(&mut cache, project_id, now).await? else {
                return Err("project id is gone".into());
            };
            if !cache
                .last_success
                .is_some_and(|at| now.saturating_sub(at) <= MAX_STALE_MS)
            {
                return Err("project settings census expired".to_owned());
            }
            if cache
                .census
                .as_ref()
                .and_then(|census| census.roots(&canonical))
                .is_none()
                && cache.missed.insert(canonical.clone())
            {
                self.refresh(&mut cache, now).await?;
                cache.missed.insert(canonical.clone());
            }
            let (projects, workspace) = cache
                .census
                .as_ref()
                .and_then(|census| census.roots(&canonical))
                .ok_or_else(|| "project id is absent from census".to_owned())?;
            Layers::from_roots(&projects, workspace.as_deref())
        }
        .await;
        match result {
            Ok((layers, applied)) => (
                layers,
                if applied {
                    ProjectSettings::Applied
                } else {
                    ProjectSettings::None
                },
            ),
            Err(error) => {
                if error != "project id is gone" {
                    tracing::warn!(project_id, %error, "project settings unresolved");
                }
                (Layers::new(None, None), ProjectSettings::Unresolved)
            }
        }
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
/// `unresolved`. Select and panel callers validate the trusted project stamp
/// with `SelectSettings::check_project_stamp` before calling. The config path
/// identifies the caller's routing configuration in isolated test harnesses.
pub(crate) async fn layers(project: &str, _config: &Path) -> (Layers, ProjectSettings) {
    #[cfg(test)]
    {
        let resolver = test_sources().lock().unwrap().get(_config).cloned();
        if let Some(resolver) = resolver {
            return resolver.layers(project, crate::now_ms()).await;
        }
    }
    match LIVE.get() {
        Some(resolver) => resolver.layers(project, crate::now_ms()).await,
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
