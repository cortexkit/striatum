use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use cortexkit_role_routing::service_tier::ServiceTier;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config_policy::RunnerPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Standard,
    Fast,
    Ultrafast,
}

impl Tier {
    pub fn value(self) -> ServiceTier {
        match self {
            Self::Standard => ServiceTier::Standard,
            Self::Fast => ServiceTier::Fast,
            Self::Ultrafast => ServiceTier::Ultrafast,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Consumer {
    service_tier: Option<Tier>,
    service_tiers: Option<BTreeMap<String, Tier>>,
    runners: Option<Vec<RunnerChoice>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunnerChoice {
    module: String,
    harness: Option<String>,
    routes: Option<Vec<String>>,
}

impl RunnerChoice {
    fn narrow(&self, enrolled: &[RunnerPolicy]) -> Option<RunnerPolicy> {
        let mut policy = enrolled
            .iter()
            .find(|entry| {
                entry.module == self.module
                    && (self.harness.is_none()
                        || entry.harness.is_none()
                        || entry.harness == self.harness)
            })?
            .clone();
        if self.harness.is_some() {
            policy.harness.clone_from(&self.harness);
        }
        if let Some(routes) = &self.routes {
            policy.routes = Some(policy.routes.as_ref().map_or_else(
                || routes.clone(),
                |allowed| intersect_routes(allowed, routes),
            ));
        }
        Some(policy)
    }

    fn as_unrestricted_choice(&self) -> RunnerPolicy {
        RunnerPolicy {
            module: self.module.clone(),
            harness: self.harness.clone(),
            routes: self.routes.clone(),
            tool_capabilities: Vec::new(),
            via_vault: false,
        }
    }
}

fn intersect_routes(enrolled: &[String], project: &[String]) -> Vec<String> {
    // Each positive route spec admits its slash-separated descendants. Two
    // specs overlap only when one names a descendant of the other.
    fn includes(prefix: &str, key: &str) -> bool {
        key == prefix
            || key
                .strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with('/'))
    }
    let mut intersection = Vec::new();
    for allowed in enrolled {
        for chosen in project {
            if includes(allowed, chosen) {
                intersection.push(chosen.clone());
            } else if includes(chosen, allowed) {
                intersection.push(allowed.clone());
            }
        }
    }
    intersection.sort_unstable();
    intersection.dedup();
    intersection
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Entity {
    service_tier: Option<Tier>,
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Document {
    consumers: BTreeMap<String, Consumer>,
    models: BTreeMap<String, Entity>,
    providers: BTreeMap<String, Entity>,
}

impl Document {
    fn validate_enrollment(&self, path: &Path, enrolled: &[RunnerPolicy]) -> Result<(), String> {
        for (consumer, entry) in &self.consumers {
            for (index, runner) in entry.runners.iter().flatten().enumerate() {
                if !enrolled.iter().any(|entry| entry.module == runner.module) {
                    return Err(ConfigError {
                        code: "routing_tier_config_invalid",
                        path: path.to_owned(),
                        key: format!("consumers.{consumer}.runners[{index}].module"),
                        reason: format!("runner {:?} is not enrolled", runner.module),
                    }
                    .to_string());
                }
            }
        }
        Ok(())
    }

    fn same_tiers(&self, other: &Self) -> bool {
        fn tiers(
            document: &Document,
        ) -> impl Iterator<Item = (&String, Option<Tier>, &Option<BTreeMap<String, Tier>>)>
        {
            document.consumers.iter().filter_map(|(id, entry)| {
                (entry.service_tier.is_some() || entry.service_tiers.is_some()).then_some((
                    id,
                    entry.service_tier,
                    &entry.service_tiers,
                ))
            })
        }
        self.models == other.models
            && self.providers == other.providers
            && tiers(self).eq(tiers(other))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigError {
    pub code: &'static str,
    pub path: PathBuf,
    pub key: String,
    pub reason: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            out,
            "[striatum] {}: ignoring routing settings file {} ({}{}{}); the next level's settings apply",
            self.code,
            self.path.display(),
            self.key,
            if self.key.is_empty() { "" } else { ": " },
            self.reason
        )
    }
}

fn parse(path: &Path, raw: &str) -> Result<Document, ConfigError> {
    let error = |key: String, reason: String| ConfigError {
        code: "routing_tier_config_invalid",
        path: path.to_owned(),
        key,
        reason,
    };
    let json =
        subc_jsonc::jsonc_to_json(raw).map_err(|err| error(String::new(), err.to_string()))?;
    let value: serde_json::Value =
        serde_json::from_str(&json).map_err(|err| error(String::new(), err.to_string()))?;
    if let Some(root) = value.as_object() {
        for (section, entries) in root {
            if !matches!(section.as_str(), "consumers" | "models" | "providers") {
                return Err(error(
                    section.clone(),
                    "a workspace or project routing file may hold only consumers, models and providers settings".into(),
                ));
            }
            if let Some(entries) = entries.as_object() {
                for (id, entry) in entries {
                    if let Some(entry) = entry.as_object() {
                        for key in entry.keys() {
                            if key != "service_tier"
                                && !(section == "consumers"
                                    && matches!(key.as_str(), "service_tiers" | "runners"))
                            {
                                return Err(error(
                                    format!("{section}.{id}.{key}"),
                                    "a workspace or project routing file may hold only service_tier (and service_tiers or runners under consumers)".into(),
                                ));
                            }
                            let field = format!("{section}.{id}.{key}");
                            if key == "runners" && !entry[key].is_null() {
                                let runners =
                                    serde_json::from_value::<Vec<RunnerChoice>>(entry[key].clone())
                                        .map_err(|err| error(field.clone(), err.to_string()))?;
                                let policies: Vec<_> = runners
                                    .iter()
                                    .map(RunnerChoice::as_unrestricted_choice)
                                    .collect();
                                crate::config_overrides::validate_runner_list(&policies, &field)
                                    .map_err(|reason| error(String::new(), reason))?;
                            }
                            if key == "service_tier"
                                && !entry[key].is_null()
                                && serde_json::from_value::<Tier>(entry[key].clone()).is_err()
                            {
                                return Err(error(
                                    field,
                                    "a service tier must be standard, fast or ultrafast".into(),
                                ));
                            }
                            if key == "service_tiers" && !entry[key].is_null() {
                                let tiers = entry[key].as_object().ok_or_else(|| {
                                    error(
                                        field.clone(),
                                        "service_tiers must map model or provider ids to tiers"
                                            .into(),
                                    )
                                })?;
                                for (route, tier) in tiers {
                                    if serde_json::from_value::<Tier>(tier.clone()).is_err() {
                                        return Err(error(
                                            format!("{field}.{route}"),
                                            "a service tier must be standard, fast or ultrafast"
                                                .into(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    serde_json::from_value(value).map_err(|err| error(String::new(), err.to_string()))
}

#[derive(Default)]
struct DocumentCache {
    parsed: Option<((SystemTime, u64), Result<Arc<Document>, String>)>,
    last_good: Option<Arc<Document>>,
    missing_reported: bool,
}

struct LoadedDocument {
    document: Arc<Document>,
    present: bool,
}

#[cfg(test)]
fn read_document(path: &Path) -> Result<Option<Arc<Document>>, String> {
    Ok(load(path, None)?.map(|loaded| loaded.document))
}

#[cfg(test)]
fn read_document_with_reporter(
    path: &Path,
    report_rejection: impl FnMut(&str),
) -> Result<Option<Arc<Document>>, String> {
    Ok(load_with_reporter(path, None, report_rejection)?.map(|loaded| loaded.document))
}

fn load(path: &Path, enrolled: Option<&[RunnerPolicy]>) -> Result<Option<LoadedDocument>, String> {
    load_with_reporter(path, enrolled, |error| {
        eprintln!(
            "[striatum] routing-config-reload-rejected: {}: {error}; keeping last good tier settings",
            path.display()
        );
    })
}

fn load_with_reporter(
    path: &Path,
    enrolled: Option<&[RunnerPolicy]>,
    mut report_rejection: impl FnMut(&str),
) -> Result<Option<LoadedDocument>, String> {
    static SCOPED: OnceLock<Mutex<HashMap<PathBuf, DocumentCache>>> = OnceLock::new();
    static LEGACY: OnceLock<Mutex<HashMap<PathBuf, DocumentCache>>> = OnceLock::new();
    // A legacy tier lookup has no enrollment snapshot and cannot publish a
    // last-good document for daemon-stamped requests.
    let cache = if enrolled.is_some() { &SCOPED } else { &LEGACY };
    let mut files = cache.get_or_init(Default::default).lock().unwrap();
    let stamp = std::fs::metadata(path)
        .ok()
        .and_then(|meta| Some((meta.modified().ok()?, meta.len())));
    let found = |cache: &mut DocumentCache, document: Arc<Document>| {
        if let Some(enrolled) = enrolled {
            document.validate_enrollment(path, enrolled)?;
        }
        cache.last_good = Some(document.clone());
        cache.missing_reported = false;
        Ok(Some(LoadedDocument {
            document,
            present: true,
        }))
    };
    if let Some(cache) = files.get_mut(path) {
        if let Some((previous, parsed)) = &cache.parsed {
            if Some(*previous) == stamp {
                return found(cache, parsed.clone()?);
            }
        }
    }
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let parsed = parse(path, &raw)
                .map(Arc::new)
                .map_err(|err| err.to_string());
            let cache = files.entry(path.to_owned()).or_default();
            cache.parsed = stamp.map(|stamp| (stamp, parsed.clone()));
            found(cache, parsed?)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let Some(cache) = files.get_mut(path) else {
                return Ok(None);
            };
            cache.parsed = None;
            let Some(document) = &cache.last_good else {
                return Ok(None);
            };
            if !cache.missing_reported {
                report_rejection("missing");
                cache.missing_reported = true;
            }
            // Keep the last good tier settings, but withdraw the runner override:
            // a stale override would send work to the wrong runner.
            Ok(Some(LoadedDocument {
                document: document.clone(),
                present: false,
            }))
        }
        Err(err) => Err(ConfigError {
            code: "routing_tier_config_invalid",
            path: path.to_owned(),
            key: String::new(),
            reason: err.to_string(),
        }
        .to_string()),
    }
}

pub(crate) fn without_context(params: &serde_json::Value) -> serde_json::Value {
    let mut params = params.clone();
    if let Some(object) = params.as_object_mut() {
        for key in ["project_root", "workspace_root", "tier_consumer"] {
            object.remove(key);
        }
    }
    params
}

pub(crate) fn at_level(
    consumer: &str,
    provider: &str,
    model: &str,
    consumers: impl Iterator<Item = (String, Option<Tier>, BTreeMap<String, Tier>)>,
    model_tier: Option<Tier>,
    provider_tier: Option<Tier>,
) -> Option<ServiceTier> {
    let mut matched: Vec<_> = consumers
        .filter(|(name, _, _)| crate::config_policy::consumer_matches(name, consumer))
        .collect();
    matched.sort_by_key(|(name, _, _)| {
        std::cmp::Reverse(crate::config_policy::consumer_specificity(name))
    });
    let route = format!("{provider}/{model}");
    matched
        .iter()
        .find_map(|(_, _, tiers)| tiers.get(&route).copied())
        .or_else(|| {
            matched
                .iter()
                .find_map(|(_, _, tiers)| tiers.get(provider).copied())
        })
        .or_else(|| matched.iter().find_map(|(_, tier, _)| *tier))
        .or(model_tier)
        .or(provider_tier)
        .map(Tier::value)
}

pub(crate) struct Layers {
    documents: Vec<LoadedDocument>,
    enrolled: Option<Vec<RunnerPolicy>>,
}

impl Layers {
    /// Roots belonging to the same project must agree on its settings document;
    /// folder order is not authority to choose between conflicting documents.
    #[cfg(test)]
    fn from_roots(projects: &[PathBuf], workspace: Option<&Path>) -> Result<(Self, bool), String> {
        Self::for_enrolled_runners(projects, workspace, &[])
    }

    pub(crate) fn for_enrolled_runners(
        projects: &[PathBuf],
        workspace: Option<&Path>,
        enrolled: &[RunnerPolicy],
    ) -> Result<(Self, bool), String> {
        let read = |root: &Path| {
            let path = crate::config_overrides::config_path_in(&root.join(".cortexkit"));
            load(&path, Some(enrolled))
        };
        let mut project: Option<LoadedDocument> = None;
        for root in projects {
            if let Some(loaded) = read(root)? {
                if project.as_ref().is_some_and(|previous| {
                    if previous.present && loaded.present {
                        previous.document != loaded.document
                    } else {
                        !previous.document.same_tiers(&loaded.document)
                    }
                }) {
                    return Err("project routing documents differ".into());
                }
                if loaded.present || project.is_none() {
                    project = Some(loaded);
                }
            }
        }
        let workspace = workspace.map(read).transpose()?.flatten();
        let documents: Vec<_> = [project, workspace].into_iter().flatten().collect();
        let applied = !documents.is_empty();
        Ok((
            Self {
                documents,
                enrolled: Some(enrolled.to_vec()),
            },
            applied,
        ))
    }

    pub(crate) fn new(project: Option<&str>, workspace: Option<&str>) -> Self {
        Self {
            documents: [project, workspace]
                .into_iter()
                .flatten()
                .filter_map(|root| {
                    let path = crate::config_overrides::config_path_in(
                        &Path::new(root).join(".cortexkit"),
                    );
                    match load(&path, None) {
                        Ok(loaded) => loaded,
                        Err(err) => {
                            eprintln!("{err}");
                            None
                        }
                    }
                })
                .collect(),
            enrolled: None,
        }
    }

    pub(crate) fn retain_tiers_only(&mut self) {
        self.enrolled = None;
    }

    pub(crate) fn runners(&self, consumer: &str) -> Option<Vec<RunnerPolicy>> {
        let enrolled = self.enrolled.as_deref()?;
        self.documents
            .iter()
            .filter(|layer| layer.present)
            .find_map(|layer| {
                let choices = layer
                    .document
                    .consumers
                    .iter()
                    .filter(|(name, _)| crate::config_policy::consumer_matches(name, consumer))
                    .filter_map(|(name, entry)| {
                        Some((
                            crate::config_policy::consumer_specificity(name),
                            entry.runners.as_ref()?,
                        ))
                    })
                    .max_by_key(|(specificity, _)| *specificity)?
                    .1;
                Some(
                    choices
                        .iter()
                        .filter_map(|choice| choice.narrow(enrolled))
                        .collect(),
                )
            })
    }

    pub(crate) fn resolve(
        &self,
        global: &crate::config_overrides::LiveConfig,
        consumer: &str,
        provider: &str,
        model: &str,
        pin: Option<ServiceTier>,
    ) -> Option<ServiceTier> {
        if let Some(pin) = pin {
            return Some(pin);
        }
        for layer in &self.documents {
            let document = &layer.document;
            if let Some(tier) = at_level(
                consumer,
                provider,
                model,
                document.consumers.iter().map(|(id, entry)| {
                    (
                        id.clone(),
                        entry.service_tier,
                        entry.service_tiers.clone().unwrap_or_default(),
                    )
                }),
                document
                    .models
                    .get(&format!("{provider}/{model}"))
                    .and_then(|entry| entry.service_tier),
                document
                    .providers
                    .get(provider)
                    .and_then(|entry| entry.service_tier),
            ) {
                return Some(tier);
            }
        }
        global.service_tier(consumer, provider, model)
    }
}

#[cfg(test)]
fn resolve(
    global: &crate::config_overrides::LiveConfig,
    project: Option<&str>,
    workspace: Option<&str>,
    consumer: &str,
    provider: &str,
    model: &str,
    pin: Option<ServiceTier>,
) -> Option<ServiceTier> {
    Layers::new(project, workspace).resolve(global, consumer, provider, model, pin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn root() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = cortexkit_test_support::scratch_root().join(format!(
            "tier-settings-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn write(root: &Path, raw: &str) -> PathBuf {
        static REVISION: AtomicU64 = AtomicU64::new(0);
        let path = root.join(".cortexkit/striatum.jsonc");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, raw).unwrap();
        std::fs::File::open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(
                SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs(
                        1_700_000_000 + REVISION.fetch_add(1, Ordering::Relaxed),
                    ),
            ))
            .unwrap();
        path
    }

    #[test]
    fn missing_tier_file_reports_once_after_each_successful_load() {
        let dir = cortexkit_test_support::ScratchDir::new("missing-tier-reports");
        let root = dir.join("root");
        let path = root.join(".cortexkit/striatum.jsonc");
        let mut reports = Vec::new();
        assert!(
            read_document_with_reporter(&path, |error| reports.push(error.to_owned()))
                .unwrap()
                .is_none()
        );
        assert!(
            reports.is_empty(),
            "an initially absent tier file is optional"
        );

        for expected_reports in 1..=2 {
            write(&root, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
            let held = read_document(&path).unwrap().unwrap();
            std::fs::remove_file(&path).unwrap();
            for _ in 0..3 {
                let next =
                    read_document_with_reporter(&path, |error| reports.push(error.to_owned()))
                        .unwrap()
                        .unwrap();
                assert!(Arc::ptr_eq(&held, &next));
            }
            assert_eq!(reports, vec!["missing"; expected_reports]);
        }
    }

    #[test]
    fn vanished_tier_file_keeps_last_good_project_and_workspace_settings() {
        let global = crate::config_overrides::LiveConfig::default();
        for workspace in [false, true] {
            for cached in [false, true] {
                let dir = cortexkit_test_support::ScratchDir::new("vanished-tier-settings");
                let root = dir.join("root");
                let path = write(&root, r#"{"providers":{"p":{"service_tier":"fast"}}}"#);
                let read = || {
                    if cached {
                        Layers::new(
                            (!workspace).then(|| root.to_str().unwrap()),
                            workspace.then(|| root.to_str().unwrap()),
                        )
                    } else {
                        let projects = if workspace {
                            Vec::new()
                        } else {
                            vec![root.clone()]
                        };
                        let (layers, applied) =
                            Layers::from_roots(&projects, workspace.then_some(root.as_path()))
                                .unwrap();
                        assert!(applied);
                        layers
                    }
                };
                assert_eq!(
                    read().resolve(&global, "worker", "p", "m", None),
                    Some(ServiceTier::Fast)
                );
                std::fs::remove_file(&path).unwrap();
                for _ in 0..3 {
                    assert_eq!(
                        read().resolve(&global, "worker", "p", "m", None),
                        Some(ServiceTier::Fast)
                    );
                }
                write(&root, r#"{"providers":{"p":{"service_tier":"ultrafast"}}}"#);
                assert_eq!(
                    read().resolve(&global, "worker", "p", "m", None),
                    Some(ServiceTier::Ultrafast)
                );
            }
        }
    }

    #[test]
    fn identity_roots_layer_equal_documents_and_reject_conflicts() {
        let a = root();
        let b = root();
        let workspace = root();
        write(&a, r#"{"models":{"p/m":{"service_tier":"fast"}}}"#);
        write(
            &b,
            r#"{ /* same document */ "models":{"p/m":{"service_tier":"fast"}}}"#,
        );
        write(
            &workspace,
            r#"{"providers":{"p":{"service_tier":"ultrafast"}}}"#,
        );
        let global = crate::config_overrides::LiveConfig::default();
        let (layers, applied) =
            Layers::from_roots(&[a.clone(), b.clone()], Some(&workspace)).unwrap();
        assert!(applied);
        assert_eq!(
            layers.resolve(&global, "worker", "p", "m", None),
            Some(ServiceTier::Fast)
        );
        assert_eq!(
            layers.resolve(&global, "worker", "p", "other", None),
            Some(ServiceTier::Ultrafast)
        );
        write(&b, r#"{"models":{"p/m":{"service_tier":"standard"}}}"#);
        assert!(Layers::from_roots(&[a, b], Some(&workspace)).is_err());
    }

    #[test]
    fn identity_roots_distinguish_missing_malformed_and_unreadable_files() {
        let missing = root();
        assert!(!Layers::from_roots(&[missing.clone()], None).unwrap().1);
        let path = write(&missing, "not json");
        assert!(Layers::from_roots(&[missing.clone()], None).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(Layers::from_roots(&[missing], None).is_err());
    }

    #[test]
    fn tier_settings_precedence_and_specificity_and_pin() {
        let root = root();
        let global_path = root.join("global.jsonc");
        std::fs::write(&global_path, r#"{"consumers":{"sidekick":{"service_tier":"ultrafast"},"head":{"service_tier":"fast"},"synthesizer":{"service_tier":"fast"},"lab_planner":{"service_tiers":{"openai":"fast"}},"spec_designer":{"service_tier":"ultrafast"}}}"#).unwrap();
        let global = crate::config_overrides::settle_live_config(&global_path);
        let workspace = root.join("workspace");
        let project = workspace.join("project");
        write(
            &workspace,
            r#"{"consumers":{"sidekick":{"service_tier":"fast"}}}"#,
        );
        write(
            &project,
            r#"{"consumers":{"sidekick":{"service_tier":"standard","service_tiers":{"openai":"fast","openai/gpt-6.1-sol":"ultrafast"}}},"models":{"openai/gpt-6.1-sol":{"service_tier":"standard"}},"providers":{"openai":{"service_tier":"standard"}}}"#,
        );
        let resolve = |project: Option<&Path>, workspace: Option<&Path>, model: &str, pin| {
            super::resolve(
                &global,
                project.and_then(Path::to_str),
                workspace.and_then(Path::to_str),
                "sidekick",
                "openai",
                model,
                pin,
            )
        };
        assert_eq!(
            resolve(Some(&project), Some(&workspace), "gpt-6.1-sol", None),
            Some(ServiceTier::Ultrafast)
        );
        assert_eq!(
            resolve(Some(&project), Some(&workspace), "other", None),
            Some(ServiceTier::Fast)
        );
        assert_eq!(
            resolve(None, Some(&workspace), "gpt-6.1-sol", None),
            Some(ServiceTier::Fast)
        );
        assert_eq!(
            resolve(None, None, "gpt-6.1-sol", None),
            Some(ServiceTier::Ultrafast)
        );
        assert_eq!(
            resolve(
                Some(&project),
                Some(&workspace),
                "gpt-6.1-sol",
                Some(ServiceTier::Standard)
            ),
            Some(ServiceTier::Standard)
        );
        assert_eq!(
            global.service_tier("head", "openai", "other"),
            Some(ServiceTier::Fast)
        );
        assert_eq!(
            global.service_tier("synthesizer", "openai", "other"),
            Some(ServiceTier::Fast)
        );
        assert_eq!(
            global.service_tier("lab_planner", "openai", "other"),
            Some(ServiceTier::Fast)
        );
        assert_eq!(
            global.service_tier("spec_designer", "openai", "other"),
            Some(ServiceTier::Ultrafast)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tier_settings_reject_non_tier_keys_by_file_and_key_and_isolate_projects() {
        let root = root();
        let global = crate::config_overrides::LiveConfig::default();
        let workspace = root.join("workspace");
        let project = root.join("bad-project");
        let other = root.join("other-project");
        write(
            &workspace,
            r#"{"consumers":{"sidekick":{"service_tier":"fast"}}}"#,
        );
        for (document, key) in [
            (r#"{"runners":[]}"#, "runners"),
            (
                r#"{"consumers":{"sidekick":{"workload":"lookup"}}}"#,
                "consumers.sidekick.workload",
            ),
            (
                r#"{"models":{"openai/test":{"iq":50}}}"#,
                "models.openai/test.iq",
            ),
            (
                r#"{"providers":{"openai":{"accounts":{}}}}"#,
                "providers.openai.accounts",
            ),
        ] {
            let path = write(&project, document);
            let error = parse(&path, document).unwrap_err();
            assert_eq!(error.key, key);
            assert_eq!(error.path, path);
            assert_eq!(error.code, "routing_tier_config_invalid");
            assert_eq!(
                resolve(
                    &global,
                    project.to_str(),
                    workspace.to_str(),
                    "sidekick",
                    "openai",
                    "test",
                    None
                ),
                Some(ServiceTier::Fast)
            );
            assert_eq!(
                resolve(
                    &global,
                    other.to_str(),
                    None,
                    "sidekick",
                    "openai",
                    "test",
                    None
                ),
                None
            );
        }
        write(
            &workspace,
            r#"{"consumers":{"sidekick":{"service_tier":"ultrafast","mode":"demand"}}}"#,
        );
        assert_eq!(
            resolve(
                &global,
                other.to_str(),
                workspace.to_str(),
                "sidekick",
                "openai",
                "test",
                None
            ),
            None
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tier_settings_project_live_reload_and_malformed_edit_drops_tiers() {
        let root = root();
        let global = crate::config_overrides::LiveConfig::default();
        let decision = || {
            resolve(
                &global,
                root.to_str(),
                None,
                "sidekick",
                "openai",
                "test",
                None,
            )
        };
        let path = write(
            &root,
            r#"{"consumers":{"sidekick":{"service_tier":"fast"}}}"#,
        );
        assert_eq!(decision(), Some(ServiceTier::Fast));
        write(
            &root,
            r#"{"consumers":{"sidekick":{"service_tier":"ultrafast"}}}"#,
        );
        assert_eq!(decision(), Some(ServiceTier::Ultrafast));
        write(&root, r#"{"providers":{"openai":{"service_tier":"fast"}}}"#);
        assert_eq!(decision(), Some(ServiceTier::Fast));
        write(&root, r#"{"providers":{"google":{"service_tier":"fast"}}}"#);
        assert_eq!(decision(), None);
        std::fs::write(path, "{malformed}").unwrap();
        assert_eq!(decision(), None);
        std::fs::remove_dir_all(root).unwrap();
    }
}
