use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use cortexkit_role_routing::service_tier::ServiceTier;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
            "[striatum] {}: ignoring tier settings file {} ({}{}{}); the next level's settings apply",
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
                    "a workspace or project routing file may hold only consumers, models and providers tier settings".into(),
                ));
            }
            if let Some(entries) = entries.as_object() {
                for (id, entry) in entries {
                    if let Some(entry) = entry.as_object() {
                        for key in entry.keys() {
                            if key != "service_tier"
                                && !(section == "consumers" && key == "service_tiers")
                            {
                                return Err(error(
                                    format!("{section}.{id}.{key}"),
                                    "a workspace or project routing file may hold only service_tier (and service_tiers under consumers)".into(),
                                ));
                            }
                            let field = format!("{section}.{id}.{key}");
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

type Stamp = Option<(SystemTime, u64)>;
type Cached = (Stamp, Arc<Document>);

struct LastGoodDocument {
    document: Arc<Document>,
    missing_reported: bool,
}

fn read_document(path: &Path) -> Result<Option<Arc<Document>>, String> {
    read_document_with_reporter(path, |error| {
        eprintln!(
            "[striatum] routing-config-reload-rejected: {}: {error}; keeping last good config",
            path.display()
        );
    })
}

fn read_document_with_reporter(
    path: &Path,
    mut report_rejection: impl FnMut(&str),
) -> Result<Option<Arc<Document>>, String> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, LastGoodDocument>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let document = Arc::new(parse(path, &raw).map_err(|err| err.to_string())?);
            cache.insert(
                path.to_owned(),
                LastGoodDocument {
                    document: Arc::clone(&document),
                    missing_reported: false,
                },
            );
            Ok(Some(document))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // An absent optional tier file is harmless only before it has
            // supplied settings; removal must not withdraw an active policy.
            let Some(previous) = cache.get_mut(path) else {
                return Ok(None);
            };
            if !previous.missing_reported {
                report_rejection("missing");
                previous.missing_reported = true;
            }
            Ok(Some(Arc::clone(&previous.document)))
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

pub(crate) fn record_requested(document: &mut String, tier: Option<ServiceTier>) {
    if let Some(tier) = tier {
        let mut value: serde_json::Value =
            serde_json::from_str(document).expect("decision requirements are JSON");
        value["service_tier_requested"] = serde_json::json!(tier);
        *document = value.to_string();
    }
}

fn load(path: &Path) -> Arc<Document> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, Cached>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    let stamp = std::fs::metadata(path)
        .ok()
        .and_then(|meta| Some((meta.modified().ok()?, meta.len())));
    if let Some((previous, document)) = cache.get(path) {
        if stamp.is_some() && previous == &stamp {
            return document.clone();
        }
    }
    let document = match read_document(path) {
        Ok(document) => document.unwrap_or_default(),
        Err(err) => {
            eprintln!("{err}");
            Arc::default()
        }
    };
    cache.insert(path.to_owned(), (stamp, document.clone()));
    document
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
    documents: Vec<Arc<Document>>,
}

impl Layers {
    /// Roots belonging to the same project must agree on its settings document;
    /// folder order is not authority to choose between conflicting documents.
    pub(crate) fn from_roots(
        projects: &[PathBuf],
        workspace: Option<&Path>,
    ) -> Result<(Self, bool), String> {
        fn read(root: &Path) -> Result<Option<Arc<Document>>, String> {
            let path = crate::config_overrides::config_path_in(&root.join(".cortexkit"));
            read_document(&path)
        }
        let mut project = None;
        for root in projects {
            if let Some(document) = read(root)? {
                if project
                    .as_ref()
                    .is_some_and(|previous| previous != &document)
                {
                    return Err("project routing documents differ".into());
                }
                project = Some(document);
            }
        }
        let workspace = workspace.map(read).transpose()?.flatten();
        let documents: Vec<_> = [project, workspace].into_iter().flatten().collect();
        let applied = !documents.is_empty();
        Ok((Self { documents }, applied))
    }

    pub(crate) fn new(project: Option<&str>, workspace: Option<&str>) -> Self {
        Self {
            documents: [project, workspace]
                .into_iter()
                .flatten()
                .map(|root| {
                    load(&crate::config_overrides::config_path_in(
                        &Path::new(root).join(".cortexkit"),
                    ))
                })
                .collect(),
        }
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
        for document in &self.documents {
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
                r#"{"consumers":{"sidekick":{"variants":{}}}}"#,
                "consumers.sidekick.variants",
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
