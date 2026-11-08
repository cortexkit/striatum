//! Verified consumer claims and route-bound ownership admission.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{OnceLock, RwLock};

use cortexkit_role_routing::declarations::{parse_consumer_declarations, DemandDefaults};
use cortexkit_role_routing::errors::Refusal;
use serde_json::Value;

use crate::config_overrides::LiveConfig;
use crate::config_policy::ConsumerPolicy;

use super::Caller;

/// Claims stay keyed by their declaring module until the effective view is
/// built. No module can overwrite another module's claim to the same name.
#[derive(Clone, Default)]
pub(crate) struct Catalog {
    modules: BTreeMap<String, BTreeMap<String, DemandDefaults>>,
    engines: BTreeMap<String, cortexkit_role_routing::EngineDeclarations>,
    agent_hosts: BTreeSet<String>,
    issues: Vec<String>,
}

static CATALOG: OnceLock<RwLock<Catalog>> = OnceLock::new();

pub(crate) fn replace_catalog(modules: &Value) {
    let snapshot = Catalog::from_catalog(modules);
    *CATALOG.get_or_init(Default::default).write().unwrap() = snapshot;
}

pub(crate) fn engine_declarations() -> BTreeMap<String, cortexkit_role_routing::EngineDeclarations>
{
    CATALOG
        .get_or_init(Default::default)
        .read()
        .unwrap()
        .engines
        .clone()
}

pub(crate) fn is_media_provider(module: &str) -> bool {
    CATALOG
        .get_or_init(Default::default)
        .read()
        .unwrap()
        .engines
        .contains_key(module)
}

pub(crate) fn media_pair(module: &str, provider: &str, model: &str) -> bool {
    engine_declarations().get(module).is_some_and(|slot| {
        slot.engines
            .iter()
            .any(|engine| !engine.local && engine.provider == provider && engine.model == model)
    })
}

pub(crate) struct Registry {
    owners: BTreeMap<String, BTreeSet<String>>,
    pub policies: BTreeMap<String, ConsumerPolicy>,
    defaults: BTreeMap<String, DemandDefaults>,
}

impl Catalog {
    pub(crate) fn from_catalog(modules: &Value) -> Self {
        let mut catalog = Self::default();
        for entry in modules.as_array().into_iter().flatten() {
            if entry.get("nonce_verified").and_then(Value::as_bool) != Some(true) {
                if entry
                    .get("role_declarations")
                    .and_then(|roles| {
                        roles
                            .get(cortexkit_role_routing::ROLE_DECLARATIONS_KEY)
                            .or_else(|| roles.get("media-provider/v1"))
                    })
                    .is_some()
                {
                    catalog.issues.push(format!(
                        "ignored unverified catalog entry: {}",
                        entry.get("module_id").unwrap_or(&Value::Null)
                    ));
                }
                continue;
            }
            let Some(module) = entry.get("module_id").and_then(Value::as_str) else {
                continue;
            };
            if let Some(slot) = entry
                .get("role_declarations")
                .and_then(|roles| roles.get("media-provider/v1"))
            {
                match cortexkit_role_routing::parse_engine_declarations(slot) {
                    Ok(declarations) => {
                        catalog.engines.insert(module.to_owned(), declarations);
                    }
                    Err(error) => catalog.issues.push(format!(
                        "Module {module} declares media engines routing can't read, so routing ignores them: {error}"
                    )),
                }
            }
            if entry
                .get("provides")
                .and_then(Value::as_array)
                .is_some_and(|provides| {
                    provides
                        .iter()
                        .any(|capability| capability == "agent-host/v1")
                })
            {
                catalog.agent_hosts.insert(module.to_owned());
            }
            let Some(slot) = entry
                .get("role_declarations")
                .and_then(|roles| roles.get(cortexkit_role_routing::ROLE_DECLARATIONS_KEY))
            else {
                continue;
            };
            match parse_consumer_declarations(slot) {
                Ok(declarations) => {
                    catalog.modules.insert(
                        module.to_owned(),
                        declarations
                            .consumers
                            .into_iter()
                            .filter(|declaration| !crate::config_policy::is_slot(&declaration.name))
                            .map(|declaration| (declaration.name, declaration.defaults))
                            .collect(),
                    );
                }
                Err(error) => {
                    let issue =
                        format!("ignored invalid consumer declaration from {module}: {error}");
                    crate::warn_operator(format!("[prefrontal-routing] {issue}"));
                    catalog.issues.push(issue);
                }
            }
        }
        catalog
    }

    pub(crate) fn effective(&self, config: &LiveConfig) -> Registry {
        let mut registry = Registry {
            owners: BTreeMap::new(),
            policies: BTreeMap::new(),
            defaults: BTreeMap::new(),
        };
        for name in config.v1_consumers() {
            registry.owners.entry(name).or_default();
        }
        for (module, declarations) in &self.modules {
            for (name, defaults) in declarations {
                registry
                    .owners
                    .entry(name.clone())
                    .or_default()
                    .insert(module.clone());
                registry.policies.insert(
                    name.clone(),
                    ConsumerPolicy {
                        eq: defaults.eq.map(f64::from),
                        context_tokens: defaults.context_window,
                        max_output_tokens: defaults.max_output_tokens,
                        tool_capabilities: defaults.tool_capabilities.clone(),
                        ..Default::default()
                    },
                );
                registry.defaults.insert(name.clone(), defaults.clone());
            }
        }
        for (name, owner) in config.consumer_owners() {
            registry.owners.entry(name).or_default().insert(owner);
        }
        registry
            .policies
            .retain(|name, _| registry.owners[name].len() == 1);
        registry
            .defaults
            .retain(|name, _| registry.owners[name].len() == 1);
        registry
    }
}

pub(crate) fn effective(config: &LiveConfig) -> Registry {
    CATALOG
        .get_or_init(Default::default)
        .read()
        .unwrap()
        .effective(config)
}

/// Capabilities come from verified daemon catalog entries, independently of
/// whether the module declares routing consumers.
pub(crate) fn provides_agent_host(module: &str) -> bool {
    CATALOG
        .get_or_init(Default::default)
        .read()
        .unwrap()
        .agent_hosts
        .contains(module)
}

impl Registry {
    pub(crate) fn knows(&self, consumer: &str) -> bool {
        self.owners.contains_key(consumer)
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.owners
            .iter()
            .filter(|(_, owners)| owners.len() <= 1)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Defaults inherit only within the owning module's namespace. Request
    /// members and then operator policy override the inherited demand fields.
    pub(crate) fn with_defaults(
        &self,
        consumer: &str,
        params: &Value,
        config: &LiveConfig,
    ) -> Value {
        let mut requirements = serde_json::Map::new();
        let mut chain: Vec<_> = self
            .defaults
            .iter()
            .filter(|(name, _)| {
                crate::config_policy::consumer_matches(name, consumer)
                    && self.owners.get(*name) == self.owners.get(consumer)
            })
            .collect();
        chain.sort_by_key(|(name, _)| name.len());
        for (_, defaults) in chain {
            requirements.extend(
                serde_json::to_value(defaults)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }
        if let Some(request) = params.get("requirements").and_then(Value::as_object) {
            requirements.extend(
                request
                    .iter()
                    .filter(|(_, value)| !value.is_null())
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        let operator =
            config.consumer_policy(consumer, &ConsumerPolicy::default(), &BTreeMap::new());
        for (name, value) in [
            ("context_window", operator.context_tokens.map(Value::from)),
            (
                "max_output_tokens",
                operator.max_output_tokens.map(Value::from),
            ),
            (
                "tool_capabilities",
                operator
                    .tool_capabilities
                    .map(|tags| serde_json::to_value(tags).unwrap()),
            ),
        ] {
            if let Some(value) = value {
                requirements.insert(name.into(), value);
            }
        }
        let mut result = params.clone();
        result["requirements"] = Value::Object(requirements);
        result
    }

    pub(crate) fn admit(&self, consumer: &str, caller: &Caller) -> Result<(), Refusal> {
        let Some(owners) = self.owners.get(consumer) else {
            return Err(Refusal::unknown_consumer(consumer));
        };
        if !caller.binding_is_current() {
            return Err(Refusal::consumer_not_owned(consumer));
        }
        // A config-only name without an owner adds no ownership restriction.
        if owners.is_empty() {
            return Ok(());
        }
        if owners.len() == 1 {
            let owner = owners.first().unwrap();
            if caller.attested_module() == Some(owner.as_str())
                || caller.scope_authority.as_ref().is_some_and(|scope| {
                    scope.owner_authorized && scope.owner == format!("reserved:{owner}")
                })
            {
                return Ok(());
            }
        }
        Err(Refusal::consumer_not_owned(consumer))
    }
}

/// Core's legacy request classes remain a documented compatibility exception.
/// An explicit consumer wins over targetAgent for admission, never vice versa.
pub(crate) fn legacy_consumer(params: &Value) -> &str {
    params
        .get("consumer")
        .and_then(Value::as_str)
        .or_else(|| params.get("targetAgent").and_then(Value::as_str))
        .unwrap_or("mason")
}

pub(crate) fn legacy_core_consumer(name: &str) -> bool {
    // These core-owned targetAgent labels exist only in the old route.select
    // shape. Delete the data file when the old route.select adapter is removed;
    // the labels are not consumer groups.
    let target_labels: Vec<&str> =
        serde_json::from_str(include_str!("legacy_consumer_aliases.json"))
            .expect("legacy core target labels are a JSON array");
    matches!(
        name,
        "mason" | "sidekick" | "athena" | "lab" | "core" | "codemode" | "flow"
    ) || target_labels.contains(&name)
        || crate::config_policy::consumer_matches("sidekick", name)
        || crate::config_policy::consumer_matches("athena", name)
}

pub(crate) fn health() -> Value {
    let catalog = CATALOG.get_or_init(Default::default).read().unwrap();
    let mut claims: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (module, declarations) in &catalog.modules {
        for name in declarations.keys() {
            claims.entry(name).or_default().push(module);
        }
    }
    serde_json::json!({"issues": catalog.issues, "conflicts": claims.into_iter().filter(|(_, owners)| owners.len() > 1).collect::<BTreeMap<_, _>>()})
}
