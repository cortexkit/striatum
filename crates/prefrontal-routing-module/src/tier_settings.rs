use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use cortexkit_role_routing::service_tier::ServiceTier;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
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

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Consumer {
    service_tier: Option<Tier>,
    service_tiers: Option<BTreeMap<String, Tier>>,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Entity {
    service_tier: Option<Tier>,
}

#[derive(Debug, Default, Clone, Deserialize)]
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
            "[TODO(user): {} {} {} {}]",
            self.code,
            self.path.display(),
            self.key,
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
                    "[TODO(user): tier keys only]".into(),
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
                                    "[TODO(user): tier keys only]".into(),
                                ));
                            }
                            let field = format!("{section}.{id}.{key}");
                            if key == "service_tier"
                                && !entry[key].is_null()
                                && serde_json::from_value::<Tier>(entry[key].clone()).is_err()
                            {
                                return Err(error(
                                    field,
                                    "[TODO(user): invalid service tier]".into(),
                                ));
                            }
                            if key == "service_tiers" && !entry[key].is_null() {
                                let tiers = entry[key].as_object().ok_or_else(|| {
                                    error(
                                        field.clone(),
                                        "[TODO(user): service tier map required]".into(),
                                    )
                                })?;
                                for (route, tier) in tiers {
                                    if serde_json::from_value::<Tier>(tier.clone()).is_err() {
                                        return Err(error(
                                            format!("{field}.{route}"),
                                            "[TODO(user): invalid service tier]".into(),
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
        if previous == &stamp {
            return document.clone();
        }
    }
    let document = match std::fs::read_to_string(path) {
        Ok(raw) => parse(path, &raw).unwrap_or_else(|err| {
            eprintln!("{err}");
            Document::default()
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Document::default(),
        Err(err) => {
            eprintln!(
                "{}",
                ConfigError {
                    code: "routing_tier_config_invalid",
                    path: path.to_owned(),
                    key: String::new(),
                    reason: err.to_string()
                }
            );
            Document::default()
        }
    };
    let document = Arc::new(document);
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
    matched.sort_by_key(|(name, _, _)| std::cmp::Reverse(name.len()));
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
    pub(crate) fn new(project: Option<&str>, workspace: Option<&str>) -> Self {
        Self {
            documents: [project, workspace]
                .into_iter()
                .flatten()
                .map(|root| load(&Path::new(root).join(".cortexkit/prefrontal-routing.jsonc")))
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
        let path = prefrontal_test_support::scratch_root().join(format!(
            "tier-settings-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn write(root: &Path, raw: &str) -> PathBuf {
        static REVISION: AtomicU64 = AtomicU64::new(0);
        let path = root.join(".cortexkit/prefrontal-routing.jsonc");
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
    fn tier_settings_precedence_and_specificity_and_pin() {
        let root = root();
        let global_path = root.join("global.jsonc");
        std::fs::write(&global_path, r#"{"consumers":{"sidekick":{"service_tier":"ultrafast"},"head":{"service_tier":"fast"}}}"#).unwrap();
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
