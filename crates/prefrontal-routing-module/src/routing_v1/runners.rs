//! One effective view of operator runner policy and verified runner facts.

use std::collections::BTreeSet;

pub(crate) use cortexkit_role_routing::declarations::RunnerDeclaration;
use cortexkit_role_routing::declarations::{RunnerPair, StandingLimits};
use cortexkit_role_routing::spellings::{AUTH_METHODS, WIRE_APIS};
use prefrontal_routing_core::RouteStateStore;
use prefrontal_routing_core::{
    make_route_key,
    provider_model_spec::{parse_spec_list, resolve_spec},
    ModelFact,
};
use serde::Serialize;

use crate::{
    broca_credentials::{credential_serves_fact, BrocaCredentials},
    config_policy::RunnerPolicy,
};

mod admission;
mod describe_cache;
pub(crate) use admission::{fetch_describe, refresh_admission_tiers, tier_view_unknown};
pub(crate) use describe_cache::DescribeCache;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EffectiveRunner {
    #[serde(flatten)]
    pub policy: RunnerPolicy,
    pub follows_account_choice: bool,
    #[serde(skip)]
    pub routed_accounts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub standing_limits: Option<StandingLimits>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairs: Option<Vec<RunnerPair>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub harnesses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_harness: Option<String>,
    #[serde(skip_serializing_if = "is_true")]
    pub starts_sessions: bool,
    #[serde(skip)]
    route_policy: Option<Vec<String>>,
    #[serde(skip)]
    reachable_models: BTreeSet<String>,
    #[serde(skip)]
    statically_reachable_models: BTreeSet<String>,
    #[serde(skip)]
    unverified_wire_models: BTreeSet<String>,
}

fn is_true(value: &bool) -> bool {
    *value
}

impl EffectiveRunner {
    pub(crate) fn selection_harness(&self, requested: Option<&str>) -> Option<String> {
        if let Some(requested) = requested {
            return self
                .harnesses
                .iter()
                .find(|harness| harness.as_str() == requested)
                .cloned();
        }
        self.default_harness
            .clone()
            .or_else(|| match self.harnesses.as_slice() {
                [only] => Some(only.clone()),
                _ => None,
            })
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Registry {
    pub entries: Vec<EffectiveRunner>,
}

impl Registry {
    pub(crate) fn classifier(
        module: &str,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
    ) -> Self {
        if !credentials.classifier_models.contains_key(module) {
            return Self::default();
        }
        super::classifier::report_missing_rows(credentials, module, now_ms as i64);
        let models: BTreeSet<String> = facts
            .iter()
            .filter(|fact| {
                super::classifier::eligible(
                    credentials,
                    module,
                    fact,
                    &cortexkit_role_routing::select::Requirements::iq(0),
                    now_ms as i64,
                )
            })
            .map(|fact| format!("{}/{}", fact.provider_id, fact.model_id))
            .collect();
        let reachable = if credentials.listing_is_fresh(now_ms) {
            models.clone()
        } else {
            BTreeSet::new()
        };
        Self {
            entries: vec![EffectiveRunner {
                policy: RunnerPolicy {
                    module: module.into(),
                    routes: Some(reachable.iter().cloned().collect()),
                    tool_capabilities: Vec::new(),
                    via_vault: true,
                },
                follows_account_choice: true,
                routed_accounts: vec!["apikey".into()],
                standing_limits: None,
                pairs: None,
                harnesses: Vec::new(),
                default_harness: None,
                starts_sessions: true,
                route_policy: None,
                reachable_models: reachable,
                statically_reachable_models: models,
                unverified_wire_models: BTreeSet::new(),
            }],
        }
    }

    pub(crate) fn build(
        policies: Vec<RunnerPolicy>,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
    ) -> Self {
        let catalog = facts
            .iter()
            .map(|fact| fact.provider_id.clone())
            .collect::<BTreeSet<_>>();
        let entries = policies
            .into_iter()
            .map(|mut policy| {
                let declaration = credentials.runner_declarations.get(&policy.module);
                if let Some(declaration) = declaration {
                    report_unknown_pair_tokens_once(&policy.module, declaration);
                }
                let needs_vault = declaration.is_some() || policy.via_vault;
                let mut unverified_wire_models = BTreeSet::new();
                let statically_reachable_models: BTreeSet<_> = facts
                    .iter()
                    .filter(|fact| {
                        !needs_vault
                            || credentials.listed_credentials().iter().any(|row| {
                                credential_serves_fact(row, fact, &catalog)
                                    && declaration.is_none_or(|declaration| {
                                        declaration
                                            .standing_limits
                                            .as_ref()
                                            .and_then(|limits| limits.explicit_providers.as_ref())
                                            .is_none_or(|providers| {
                                                providers.contains(&fact.provider_id)
                                            })
                                            && {
                                                let method =
                                                    row.auth_method.as_deref().unwrap_or("");
                                                if fact.billing_plane.as_ref().is_some_and(
                                                    |plane| plane.auth_method != method,
                                                ) {
                                                    return false;
                                                }
                                                let Some(api) = credentials.wire_api(fact, method)
                                                else {
                                                    unverified_wire_models.insert(format!(
                                                        "{}/{}",
                                                        fact.provider_id, fact.model_id
                                                    ));
                                                    return false;
                                                };
                                                declaration.pairs.iter().any(|pair| {
                                                    known_pair(pair)
                                                        && api == pair.wire_api
                                                        && row.auth_method.as_ref()
                                                            == Some(&pair.auth_method)
                                                })
                                            }
                                    })
                            })
                    })
                    .map(|fact| format!("{}/{}", fact.provider_id, fact.model_id))
                    .collect();
                // Listing age can stop a send, but cannot change which model
                // scores define IQ when the last listing still names its credential.
                let reachable_models = if !needs_vault || credentials.listing_is_fresh(now_ms) {
                    statically_reachable_models.clone()
                } else {
                    unverified_wire_models.clear();
                    BTreeSet::new()
                };
                unverified_wire_models.retain(|model| !reachable_models.contains(model));
                let route_policy = policy.routes.clone();
                let routes = facts
                    .iter()
                    .filter(|fact| {
                        let key = make_route_key(
                            &fact.provider_id,
                            &fact.model_id,
                            fact.variant.as_deref(),
                        );
                        reachable_models
                            .contains(&format!("{}/{}", fact.provider_id, fact.model_id))
                            && route_policy.as_ref().is_none_or(|specs| {
                                resolve_spec(&parse_spec_list(specs), &key, false)
                            })
                    })
                    .map(|fact| {
                        make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    })
                    .collect();
                policy.routes = Some(routes);
                let follows_account_choice =
                    declaration.map_or(policy.via_vault, |d| d.follows_account_choice);
                let standing_limits = declaration.and_then(|d| d.standing_limits.clone());
                let routed_accounts = credentials
                    .runner_routed_accounts
                    .get(&policy.module)
                    .filter(|_| declaration.is_some_and(|d| d.follows_account_choice))
                    .cloned()
                    .unwrap_or_default();
                if let Some(declaration) = declaration {
                    policy.tool_capabilities = declaration.tool_capabilities.clone();
                }
                EffectiveRunner {
                    policy,
                    follows_account_choice,
                    routed_accounts,
                    standing_limits,
                    pairs: declaration.map(|declaration| declaration.pairs.clone()),
                    harnesses: declaration
                        .and_then(|d| d.harnesses.clone())
                        .unwrap_or_default(),
                    default_harness: declaration.and_then(|d| d.default_harness.clone()),
                    starts_sessions: declaration.is_none_or(|d| d.starts_sessions),
                    route_policy,
                    reachable_models,
                    statically_reachable_models,
                    unverified_wire_models,
                }
            })
            .collect();
        Self { entries }
    }

    pub(crate) fn policies(&self) -> Vec<RunnerPolicy> {
        self.entries
            .iter()
            // Non-starting runners remain available through get() for
            // route.account_for, but cannot be selected to start sessions.
            .filter(|runner| runner.starts_sessions)
            .map(|runner| runner.policy.clone())
            .collect()
    }

    pub(crate) fn get(&self, module: &str) -> Option<&EffectiveRunner> {
        self.entries
            .iter()
            .find(|runner| runner.policy.module == module)
    }

    pub(crate) fn runs(&self, module: &str, route: &str, capacity: &str) -> bool {
        self.get(module).is_some_and(|runner| {
            // Variants share their model's transport and credential reach, but
            // operator route rules still judge the exact requested variant.
            runner.reachable_models.contains(capacity)
                && runner
                    .route_policy
                    .as_ref()
                    .is_none_or(|specs| resolve_spec(&parse_spec_list(specs), route, false))
        })
    }

    pub(crate) fn can_run(&self, module: &str, route: &str, capacity: &str) -> bool {
        self.get(module).is_some_and(|runner| {
            runner.statically_reachable_models.contains(capacity)
                && runner
                    .route_policy
                    .as_ref()
                    .is_none_or(|specs| resolve_spec(&parse_spec_list(specs), route, false))
        })
    }

    pub(crate) fn wire_family_unverified(&self, module: &str, route: &str, capacity: &str) -> bool {
        self.get(module).is_some_and(|runner| {
            runner.unverified_wire_models.contains(capacity)
                && runner
                    .route_policy
                    .as_ref()
                    .is_none_or(|specs| resolve_spec(&parse_spec_list(specs), route, false))
        })
    }
}

pub(crate) fn effective(deps: &super::Deps<'_>) -> Registry {
    let mut facts = deps.store.get_facts().unwrap_or_default();
    deps.broca_credentials
        .lock()
        .map(|credentials| {
            let targets = credentials.catalog_targets(&facts);
            prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &targets);
            Registry::build(
                super::operator_config(deps).v1_runners(),
                &credentials,
                &facts,
                crate::now_ms(),
            )
        })
        .unwrap_or_default()
}

/// Read runner facts with the shared routing role declaration validator.
pub(crate) fn parse_declaration(
    slot: &serde_json::Value,
) -> Result<Option<RunnerDeclaration>, String> {
    cortexkit_role_routing::declarations::parse_consumer_declarations(slot)
        .map(|declaration| declaration.runner)
        .map_err(|error| error.to_string())
}

fn known_pair(pair: &RunnerPair) -> bool {
    WIRE_APIS.contains(&pair.wire_api.as_str()) && AUTH_METHODS.contains(&pair.auth_method.as_str())
}

fn report_unknown_pair_tokens_once(module: &str, declaration: &RunnerDeclaration) {
    use std::sync::Mutex;
    static REPORTED: Mutex<BTreeSet<(String, &'static str, String)>> = Mutex::new(BTreeSet::new());
    let Ok(mut reported) = REPORTED.lock() else {
        return;
    };
    for pair in &declaration.pairs {
        for (field, token, known) in [
            ("wire_api", pair.wire_api.as_str(), WIRE_APIS),
            ("auth_method", pair.auth_method.as_str(), AUTH_METHODS),
        ] {
            if !known.contains(&token) && reported.insert((module.into(), field, token.into())) {
                crate::warn_operator(format!(
                    "[prefrontal-routing] runner {module}: unknown {field} token {token:?}; pair cannot match"
                ));
            }
        }
    }
}

/// Each successful catalog read replaces the declaration snapshot, so removal
/// or loss of nonce verification also removes a module's previously trusted facts.
pub(crate) fn declarations_from_catalog(
    modules: &serde_json::Value,
) -> std::collections::BTreeMap<String, RunnerDeclaration> {
    modules.as_array().into_iter().flatten().filter_map(|entry| {
        if entry.get("nonce_verified").and_then(serde_json::Value::as_bool) != Some(true) {
            return None;
        }
        let module = entry.get("module_id")?.as_str()?;
        let slot = entry.get("role_declarations")?.get(cortexkit_role_routing::ROLE_DECLARATIONS_KEY)?;
        match parse_declaration(slot) {
            Ok(Some(declaration)) => {
                report_unknown_pair_tokens_once(module, &declaration);
                Some((module.to_owned(), declaration))
            }
            Ok(None) => None,
            Err(error) => {
                crate::warn_operator(format!("[prefrontal-routing] ignored invalid runner declaration from {module}: {error}"));
                None
            }
        }
    }).collect()
}

pub(crate) fn service_tiers_from_describe(
    value: &serde_json::Value,
) -> Option<cortexkit_role_llm_runner::service_tier::ServiceTiers> {
    let value = value.get("result").unwrap_or(value);
    if !value["capabilities"]
        .as_array()?
        .iter()
        .any(|capability| capability == "service_tier")
    {
        return None;
    }
    let tiers = serde_json::from_value(value.get("service_tiers")?.clone()).ok()?;
    cortexkit_role_llm_runner::service_tier::check_service_tiers(&tiers).ok()?;
    Some(tiers)
}

pub(crate) fn routed_accounts_from_describe(value: &serde_json::Value) -> Option<Vec<String>> {
    let value = value.get("result").unwrap_or(value);
    if !value["capabilities"]
        .as_array()?
        .iter()
        .any(|capability| capability == "routed_accounts")
    {
        return None;
    }
    let methods: Vec<String> =
        serde_json::from_value(value.get("routed_accounts")?.clone()).ok()?;
    cortexkit_role_llm_runner::describe::check_routed_accounts(&methods).ok()?;
    Some(methods)
}

pub(crate) fn tier_discovery_modules(
    modules: &serde_json::Value,
    declarations: &std::collections::BTreeMap<String, RunnerDeclaration>,
    policies: &[RunnerPolicy],
) -> BTreeSet<String> {
    modules
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            if entry
                .get("nonce_verified")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            {
                return None;
            }
            let module = entry.get("module_id")?.as_str()?;
            // A configured vault runner can serve accounts without a routing
            // declaration. Its own role.describe must still advertise the tier.
            (declarations.contains_key(module)
                || policies
                    .iter()
                    .any(|policy| policy.module == module && policy.via_vault))
            .then(|| module.to_owned())
        })
        .collect()
}

pub(crate) async fn refresh_loop(
    connection_file: std::path::PathBuf,
    credentials: crate::SharedBrocaCredentials,
    config_path: std::path::PathBuf,
) {
    use subc_client_rs::{ConsumerOptions, SubcConsumer};
    let mut cache = DescribeCache::default();
    let mut classifiers = super::classifier::DescribeCache::default();
    loop {
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let consumer = SubcConsumer::connect(&connection_file, ConsumerOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            let catalog = consumer
                .catalog_list()
                .await
                .map_err(|error| error.to_string())?;
            let modules =
                serde_json::to_value(catalog.modules).map_err(|error| error.to_string())?;
            Ok::<_, String>((consumer, modules))
        })
        .await;
        if let Ok(Ok((consumer, modules))) = result {
            super::consumers::replace_catalog(&modules);
            let policies = crate::config_overrides::live_config(&config_path).v1_runners();
            let classifier_modules = credentials
                .lock()
                .map(|mut credentials| {
                    classifiers.catalog(&modules, &mut credentials, crate::now_ms() as i64)
                })
                .unwrap_or_default();
            for module in classifier_modules {
                let result =
                    super::classifier::fetch_describe(&consumer, &connection_file, &module).await;
                if let Ok(mut credentials) = credentials.lock() {
                    if matches!(result, Ok(None)) {
                        classifiers.revoke(&module, &mut credentials);
                    }
                    classifiers.reply(
                        &module,
                        result.as_ref().ok().and_then(Option::as_ref),
                        &mut credentials,
                        crate::now_ms() as i64,
                    );
                }
            }
            let tier_modules = if let Ok(mut credentials) = credentials.lock() {
                cache.catalog(
                    &modules,
                    &policies,
                    &mut credentials,
                    crate::now_ms() as i64,
                )
            } else {
                BTreeSet::new()
            };
            // The routing declaration describes reach, not tier application.
            // Only the runner's own role.describe can establish tier support.
            for module in tier_modules {
                let describe = fetch_describe(&consumer, &connection_file, &module).await;
                if let Ok(mut credentials) = credentials.lock() {
                    cache.reply(
                        &module,
                        describe.as_ref(),
                        &mut credentials,
                        crate::now_ms() as i64,
                    );
                }
            }
        } else if let Ok(mut credentials) = credentials.lock() {
            cache.missed_all(&mut credentials, crate::now_ms() as i64);
            classifiers.expire(&mut credentials, crate::now_ms() as i64);
        }
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}
