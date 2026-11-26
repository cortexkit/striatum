//! Verified consumer claims and route-bound ownership admission.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use cortexkit_role_routing::declarations::DemandDefaults;
use cortexkit_role_routing::errors::Refusal;
use serde_json::Value;

use crate::config_overrides::LiveConfig;
use crate::config_policy::ConsumerPolicy;

use super::Caller;

pub(super) mod declared;
pub(super) mod kind;

/// Claims stay keyed by their declaring module until the effective view is
/// built. No module can overwrite another module's claim to the same name.
#[derive(Clone, Default)]
pub(crate) struct Catalog {
    modules: BTreeMap<String, BTreeMap<String, DemandDefaults>>,
    engines: BTreeMap<String, cortexkit_role_routing::EngineDeclarations>,
    search: BTreeMap<String, cortexkit_role_routing::SearchDeclarations>,
    agent_hosts: BTreeSet<String>,
    issues: Vec<String>,
    media: Arc<Mutex<Option<MediaJoin>>>,
    // The registry for the last settings snapshot this catalog served. A
    // catalog refresh replaces the whole catalog, and with it this entry.
    registry: Arc<Mutex<Option<(crate::config_overrides::SnapshotKey, Arc<Registry>)>>>,
}

#[cfg(test)]
static REGISTRY_BUILDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn registry_builds_for_test() -> usize {
    REGISTRY_BUILDS.load(std::sync::atomic::Ordering::SeqCst)
}

pub(super) struct MediaRoute {
    pub module: String,
    pub engine: cortexkit_role_routing::select_engine::Engine,
}

#[derive(Default)]
pub(super) struct MediaKind {
    pub routes: Vec<MediaRoute>,
    pub grades: BTreeMap<String, f64>,
}

#[derive(Default)]
pub(super) struct MediaRoutes {
    pub by_kind: BTreeMap<String, MediaKind>,
    pub issues: Vec<String>,
    catalog_targets: BTreeMap<String, String>,
}

impl MediaRoutes {
    pub(super) fn catalog_provider<'a>(&'a self, provider: &'a str) -> &'a str {
        striatum_core::score_alias::alias_target(&self.catalog_targets, provider)
            .unwrap_or(provider)
    }
}

struct MediaJoin {
    source: Option<Arc<crate::fusiform_catalog::CatalogSnapshot>>,
    credential_view: Option<String>,
    routes: Arc<MediaRoutes>,
}

static CATALOG: OnceLock<RwLock<Catalog>> = OnceLock::new();

pub(crate) fn replace_catalog(modules: &Value) {
    let mut snapshot = Catalog::from_catalog(modules);
    static REPORTED: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
    if let Ok(mut reported) = REPORTED.lock() {
        for issue in &snapshot.issues {
            if reported.insert(issue.clone()) {
                crate::warn_operator(format!("[striatum] {issue}"));
            }
        }
    }
    let mut current = CATALOG.get_or_init(Default::default).write().unwrap();
    // Catalog refreshes also carry unrelated module metadata. Keep the join
    // when the engine declarations have not changed.
    if current.engines == snapshot.engines && current.search == snapshot.search {
        snapshot.media = current.media.clone();
    }
    *current = snapshot;
}

pub(super) fn media_routes(
    credentials: &crate::broca_credentials::CredentialData,
) -> Arc<MediaRoutes> {
    let source = credentials.catalog_snapshot.as_ref();
    let credential_view = credentials.listing_version().map(|(view, _)| view);
    let catalog = CATALOG.get_or_init(Default::default).read().unwrap();
    let mut cached = catalog.media.lock().unwrap();
    if let Some(join) = cached.as_ref() {
        let unchanged = match (join.source.as_ref(), source) {
            (Some(old), Some(new)) => Arc::ptr_eq(old, new),
            (None, None) => true,
            _ => false,
        };
        if unchanged && join.credential_view.as_deref() == credential_view {
            return join.routes.clone();
        }
    }
    // Only module declarations are joined here. Caller-local offers arrive per
    // request and read cached grades under local:<provider>/<model> keys rather
    // than catalog row identities.
    // The served snapshot can have media or search rows that are not stored for
    // chat. Use its provider index with the same vault resolver as chat.
    let mut routes = MediaRoutes {
        catalog_targets: source.map_or_else(BTreeMap::new, |snapshot| {
            let (_, providers) = snapshot.provider_index();
            credentials
                .catalog_resolution_for_providers(&providers)
                .targets
        }),
        ..Default::default()
    };
    for (module, slot) in &catalog.engines {
        for engine in &slot.engines {
            let kind = engine.kind.as_str();
            let route = format!("{}/{}", engine.provider, engine.model);
            let catalog_key =
                striatum_core::score_alias::catalog_key(&routes.catalog_targets, &route);
            let joined = routes.by_kind.entry(kind.into()).or_default();
            if let Some(grade) =
                source.and_then(|snapshot| snapshot.media_grade(kind, &catalog_key))
            {
                joined.grades.insert(route, grade);
            }
            joined.routes.push(MediaRoute {
                module: module.clone(),
                engine: engine.clone(),
            });
        }
    }
    for (module, slot) in &catalog.search {
        for route in &slot.routes {
            let key = format!("{}/{}", route.provider, route.model);
            let catalog_key =
                striatum_core::score_alias::catalog_key(&routes.catalog_targets, &key);
            if !source.is_some_and(|snapshot| {
                snapshot
                    .model(&catalog_key)
                    .is_some_and(|row| row.present && row.model_type.as_deref() == Some("search"))
            }) {
                routes.issues.push(format!(
                    "Module {module} search route {key} excluded: missing catalog search row"
                ));
                continue;
            }
            let joined = routes.by_kind.entry("search".into()).or_default();
            if let Some(grade) =
                source.and_then(|snapshot| snapshot.media_grade("search", &catalog_key))
            {
                joined.grades.insert(key, grade);
            }
            joined.routes.push(MediaRoute {
                module: module.clone(),
                engine: cortexkit_role_routing::select_engine::Engine::new(
                    cortexkit_role_routing::select_engine::Kind::new("search"),
                    route.provider.clone(),
                    route.model.clone(),
                    false,
                    vec![],
                    cortexkit_role_routing::select_engine::LatencyClass::Interactive,
                ),
            });
        }
    }
    let routes = Arc::new(routes);
    *cached = Some(MediaJoin {
        source: source.cloned(),
        credential_view: credential_view.map(str::to_owned),
        routes: routes.clone(),
    });
    routes
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

#[cfg(test)]
pub(super) fn search_declarations() -> BTreeMap<String, cortexkit_role_routing::SearchDeclarations>
{
    CATALOG
        .get_or_init(Default::default)
        .read()
        .unwrap()
        .search
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
    priorities: BTreeMap<String, cortexkit_role_routing::declarations::ConsumerPriority>,
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
                            .or_else(|| roles.get("search-provider/v1"))
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
                .get("role_declarations")
                .and_then(|roles| roles.get("search-provider/v1"))
                .is_some()
            {
                let slot = &entry["role_declarations"]["search-provider/v1"];
                match cortexkit_role_routing::parse_search_declarations(slot) {
                    Ok(declarations) => {
                        catalog.search.insert(module.to_owned(), declarations);
                    }
                    Err(error) => catalog.issues.push(format!(
                        "Module {module} declares search routes routing can't read, so routing ignores them: {error}"
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
            match declared::parse(slot) {
                Ok(declarations) => {
                    catalog.modules.insert(
                        module.to_owned(),
                        declarations
                            .consumers
                            .into_iter()
                            .filter(|declaration| {
                                declared::honor(module, declaration, &mut catalog.issues)
                            })
                            .filter(|declaration| !crate::config_policy::is_slot(&declaration.name))
                            .map(|declaration| (declaration.name, declaration.defaults))
                            .collect(),
                    );
                }
                Err(error) => {
                    let issue =
                        format!("ignored invalid consumer declaration from {module}: {error}");
                    catalog.issues.push(issue);
                }
            }
        }
        catalog.issues.sort();
        catalog.issues.dedup();
        catalog
    }

    /// The registry for `config`, built at most once per settings snapshot
    /// for this catalog.
    fn shared_effective(&self, config: &LiveConfig) -> Arc<Registry> {
        let key = config.snapshot_key();
        let mut cached = self.registry.lock().unwrap();
        if let Some((cached_key, registry)) = cached.as_ref() {
            if *cached_key == key {
                return Arc::clone(registry);
            }
        }
        let registry = Arc::new(self.effective(config));
        *cached = Some((key, Arc::clone(&registry)));
        registry
    }

    pub(crate) fn effective(&self, config: &LiveConfig) -> Registry {
        #[cfg(test)]
        REGISTRY_BUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut registry = Registry {
            owners: BTreeMap::new(),
            policies: BTreeMap::new(),
            defaults: BTreeMap::new(),
            priorities: config.consumer_priorities(),
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
        // A settings child that inherits extended declaration policy also
        // inherits the declaration's admission restriction. Otherwise an
        // ownerless child remains open to any currently bound caller.
        let inherited_owners: Vec<_> = registry
            .owners
            .iter()
            .filter(|(_, owners)| owners.is_empty())
            .filter_map(|(name, _)| {
                let ancestor = registry
                    .defaults
                    .keys()
                    .filter(|ancestor| crate::config_policy::consumer_matches(ancestor, name))
                    .max_by_key(|ancestor| crate::config_policy::consumer_specificity(ancestor))?;
                let owners = &registry.owners[ancestor];
                registry
                    .defaults
                    .iter()
                    .any(|(parent, defaults)| {
                        crate::config_policy::consumer_matches(parent, ancestor)
                            && &registry.owners[parent] == owners
                            && declared::extended(defaults)
                    })
                    .then(|| (name.clone(), owners.clone()))
            })
            .collect();
        registry.owners.extend(inherited_owners);
        registry
            .policies
            .retain(|name, _| registry.owners[name].len() == 1);
        registry
            .defaults
            .retain(|name, _| registry.owners[name].len() == 1);
        registry
    }
}

/// The consumer registry for the current catalog and `config`. Requests on
/// an unchanged settings snapshot share one registry instead of rebuilding
/// ownership and inheritance for every call.
pub(crate) fn effective(config: &LiveConfig) -> Arc<Registry> {
    CATALOG
        .get_or_init(Default::default)
        .read()
        .unwrap()
        .shared_effective(config)
}

pub(crate) fn is_declared_or_builtin(name: &str) -> bool {
    crate::select_policy::Consumer::from_id(name).is_some()
        || crate::config_policy::is_slot(name)
        || CATALOG
            .get_or_init(Default::default)
            .read()
            .unwrap()
            .modules
            .values()
            .any(|declared| {
                declared
                    .keys()
                    .any(|declared| crate::config_policy::consumer_matches(declared, name))
            })
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
    /// Resolve whole dotted segments so an unlisted child uses the nearest
    /// declared or configured name, including its ownership restrictions.
    fn owners_for(&self, consumer: &str) -> Option<&BTreeSet<String>> {
        self.owners
            .iter()
            .filter(|(name, _)| crate::config_policy::consumer_matches(name, consumer))
            .max_by_key(|(name, _)| crate::config_policy::consumer_specificity(name))
            .map(|(_, owners)| owners)
    }

    pub(crate) fn knows(&self, consumer: &str) -> bool {
        self.owners_for(consumer).is_some()
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.owners
            .iter()
            .filter(|(_, owners)| owners.len() <= 1)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Resolve each member independently, without crossing an ownership boundary.
    pub(crate) fn defaults_for(&self, consumer: &str) -> DemandDefaults {
        let mut result = DemandDefaults::default();
        let owners = self.owners_for(consumer);
        let mut chain: Vec<_> = self
            .defaults
            .iter()
            .filter(|(name, _)| {
                crate::config_policy::consumer_matches(name, consumer)
                    && self.owners.get(*name) == owners
            })
            .collect();
        chain.sort_by_key(|(name, _)| crate::config_policy::consumer_specificity(name));
        for (_, defaults) in chain {
            macro_rules! inherit {
                ($($field:ident),+ $(,)?) => {
                    $(if defaults.$field.is_some() {
                        result.$field = defaults.$field.clone();
                    })+
                };
            }
            inherit!(
                priority,
                iq,
                ladder,
                placement,
                selectors,
                kind,
                eq,
                context_window,
                max_output_tokens,
                tools,
                tool_capabilities,
                modalities,
                structured_output,
                workload,
            );
        }
        // Operator overrides are resolved after inheritance: an ancestor
        // override must beat even a child's explicit declaration.
        result.priority = Some(
            self.priorities
                .iter()
                .filter(|(name, _)| crate::config_policy::consumer_matches(name, consumer))
                .max_by_key(|(name, _)| crate::config_policy::consumer_specificity(name))
                .map(|(_, priority)| *priority)
                .or(result.priority)
                .unwrap_or_default(),
        );
        result
    }

    /// Request members and then operator policy override inherited demand fields.
    pub(crate) fn with_defaults(
        &self,
        consumer: &str,
        params: &Value,
        config: &LiveConfig,
    ) -> Value {
        let mut demand = serde_json::to_value(self.defaults_for(consumer)).unwrap();
        let mut requirements = demand.as_object_mut().unwrap().clone();
        // parse_select_request_with_defaults resolves IQ after admission. The
        // remaining declaration policy and authority members aren't requirements.
        // Workload stays separate so selection can record which layer supplied it.
        for member in [
            "priority",
            "iq",
            "ladder",
            "placement",
            "selectors",
            "kind",
            "workload",
        ] {
            requirements.remove(member);
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
        let Some(owners) = self.owners_for(consumer) else {
            return Err(Refusal::unknown_consumer(consumer));
        };
        if !caller.module_identity_is_current() {
            return Err(Refusal::consumer_not_owned(consumer));
        }
        // A config-only name without an owner adds no ownership restriction.
        if owners.is_empty() {
            return Ok(());
        }
        if owners.len() == 1 {
            let owner = owners.first().unwrap();
            let selectors = self
                .defaults
                .iter()
                .filter(|(name, defaults)| {
                    crate::config_policy::consumer_matches(name, consumer)
                        && self.owners.get(*name) == Some(owners)
                        && defaults.selectors.is_some()
                })
                .max_by_key(|(name, _)| crate::config_policy::consumer_specificity(name))
                .and_then(|(_, defaults)| defaults.selectors.as_ref());
            if caller
                .attested_module()
                .is_some_and(|module| module == owner)
                || caller.attested_module().is_some_and(|module| {
                    selectors.is_some_and(|selectors| selectors.iter().any(|id| id == module))
                })
                || caller.scope_authority.as_ref().is_some_and(|scope| {
                    scope.owner_authorized
                        && scope.owner.strip_prefix("reserved:") == Some(owner.as_str())
                })
            {
                return Ok(());
            }
        }
        Err(Refusal::consumer_not_owned(consumer))
    }
}

/// Count requests at the API boundary, not selection attempts: internal
/// retries must not turn one API call into several log lines. `pinned` and
/// `pin_origin` are logged as the caller sent them, before routing checks
/// whether that caller may mark a user pin, so an operator can see what
/// actually arrived on the wire.
pub(crate) fn record_select(surface: &str, params: &Value, config: &LiveConfig) {
    let consumer = params.get("consumer").and_then(Value::as_str);
    let class = consumer
        .and_then(|name| effective(config).defaults_for(name).priority)
        .unwrap_or_default();
    crate::info_operator(format!(
        "routing select call {}",
        serde_json::json!({
            "surface":surface,"consumer":consumer,"class":class,
            "head_request":class == cortexkit_role_routing::declarations::ConsumerPriority::Interactive,
            "pinned":params.get("pin").is_some_and(|pin| !pin.is_null()),
            "pin_origin":params.get("pin_origin").and_then(Value::as_str)
        })
    ));
}

pub(crate) fn health() -> Value {
    let catalog = CATALOG.get_or_init(Default::default).read().unwrap();
    let mut claims: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (module, declarations) in &catalog.modules {
        for name in declarations.keys() {
            claims.entry(name).or_default().push(module);
        }
    }
    let mut issues = catalog.issues.clone();
    if let Some(join) = catalog.media.lock().unwrap().as_ref() {
        issues.extend(join.routes.issues.iter().cloned());
    }
    serde_json::json!({"issues": issues, "conflicts": claims.into_iter().filter(|(_, owners)| owners.len() > 1).collect::<BTreeMap<_, _>>()})
}
