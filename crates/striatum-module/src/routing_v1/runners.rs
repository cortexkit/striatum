//! One effective view of operator runner policy and verified runner facts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use cortexkit_role_routing::declarations::{RunnerPair, StandingLimits};
use cortexkit_role_routing::spellings::{AUTH_METHODS, WIRE_APIS};
use serde::Serialize;
use striatum_core::RouteStateStore;
use striatum_core::{make_route_key, provider_model_spec::CompiledSpecs, ModelFact};

use crate::{
    broca_credentials::{credential_serves_fact, BrocaCredentials},
    config_policy::RunnerPolicy,
};

mod admission;
mod describe_cache;
pub(crate) mod enrollment;
mod harness_choice;
pub(crate) use harness_choice::TaskRunnerDiagnostics;
mod harness_facts;
#[cfg(test)]
mod harness_facts_tests;
pub(crate) mod placements;
#[cfg(test)]
mod placements_tests;
#[cfg(test)]
#[path = "runners/watch_tests.rs"]
mod watch_tests;
#[cfg(test)]
pub(crate) use admission::fetch_describe;
pub(crate) use admission::{refresh_admission_tiers, tier_view_unknown, AdmissionDescribes};
pub(crate) use describe_cache::DescribeCache;
pub(crate) use harness_facts::RunnerDeclaration;

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
    #[serde(skip_serializing_if = "is_false")]
    pub requires_wire_api: bool,
    #[serde(skip)]
    wire_declaration: Option<RunnerDeclaration>,
    #[serde(skip)]
    route_policy: Option<std::sync::Arc<CompiledSpecs>>,
    #[serde(skip)]
    reachable_models: BTreeSet<String>,
    #[serde(skip)]
    statically_reachable_models: BTreeSet<String>,
    #[serde(skip)]
    unverified_wire_models: BTreeSet<String>,
    #[serde(skip)]
    missing_wire_api_models: BTreeSet<String>,
    #[serde(skip)]
    no_account_models: BTreeSet<String>,
    #[serde(skip)]
    provider_limit_models: BTreeSet<String>,
    #[serde(skip)]
    harness_choices: Option<Vec<harness_choice::Candidate>>,
    #[serde(skip)]
    placement_baseline: Option<Box<EffectiveRunner>>,
    #[serde(skip)]
    shared_placement_key: bool,
}

pub(crate) fn can_offer_account(
    module: &str,
    follows_account_choice: bool,
    row: &cortexkit_claustrum_client::ListedCredential,
) -> bool {
    // Broca's subscription candidates require a vault provider account id;
    // API-key and company-gateway candidates instead fall back to the
    // credential id, because Broca offers each key as its own account. Insula's
    // credentialRef join cannot supply metadata to Broca's separate listing.
    // Replace the module-specific rule with a runner declaration capability
    // such as candidates_require_provider_account when the contract exposes it.
    // This temporary compatibility bridge uses the legacy wire's runner name
    // until the declaration can advertise that candidate requirement.
    module != crate::legacy_wire::BROCA
        || !follows_account_choice
        || matches!(row.auth_method.as_deref(), Some("apikey" | "gateway"))
        || row.account_id.is_some()
}

fn is_true(value: &bool) -> bool {
    *value
}

fn is_false(value: &bool) -> bool {
    !value
}

impl EffectiveRunner {
    pub(super) fn wire_declaration(&self) -> Option<&RunnerDeclaration> {
        self.wire_declaration.as_ref()
    }
    pub(crate) fn requires_wire_api_for_pair(&self, pair: &RunnerPair) -> bool {
        self.wire_declaration
            .as_ref()
            .is_some_and(|declaration| declaration.requires_wire_api_for_pair(pair))
    }

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

/// Session selection keeps placements distinct; module-scoped operations keep
/// reading the first configured entry, as they did before harness placements.
pub(crate) trait RunnerKey {
    fn matches(&self, runner: &EffectiveRunner) -> bool;
}

impl RunnerKey for str {
    fn matches(&self, runner: &EffectiveRunner) -> bool {
        self == runner.policy.module
    }
}

impl RunnerKey for String {
    fn matches(&self, runner: &EffectiveRunner) -> bool {
        self.as_str() == runner.policy.module
    }
}

impl RunnerKey for RunnerPolicy {
    fn matches(&self, runner: &EffectiveRunner) -> bool {
        self.module == runner.policy.module
            && self.harness == runner.policy.harness
            && (!runner.shared_placement_key || self.routes == runner.policy.routes)
    }
}

fn placement_routes(
    config: Option<&crate::config_overrides::LiveConfig>,
    policy: &RunnerPolicy,
) -> Option<Arc<CompiledSpecs>> {
    config
        .and_then(|config| config.runner_routes(policy).cloned())
        .or_else(|| {
            // Project intersections are not part of the published user config.
            policy
                .routes
                .as_deref()
                .map(|routes| Arc::new(placements::compile(routes)))
        })
}

pub(crate) struct RunnerPolicies<'a> {
    policies: Vec<RunnerPolicy>,
    config: Option<&'a crate::config_overrides::LiveConfig>,
    consumer: Option<&'a crate::config_policy::ConsumerPolicy>,
    auto_enroll: bool,
}

impl<'a> RunnerPolicies<'a> {
    pub(crate) fn for_consumer(
        config: &'a crate::config_overrides::LiveConfig,
        consumer: &'a crate::config_policy::ConsumerPolicy,
    ) -> Self {
        Self {
            policies: config.v1_runners(),
            config: Some(config),
            consumer: Some(consumer),
            auto_enroll: true,
        }
    }

    fn enrolled(&self, credentials: &BrocaCredentials, now_ms: u64) -> Vec<RunnerPolicy> {
        let global = if self.auto_enroll {
            enrollment::policies_at(&self.policies, credentials, now_ms as i64)
        } else {
            self.policies.clone()
        };
        let policies = self
            .consumer
            .and_then(|consumer| consumer.runners.as_ref())
            .map_or(global.clone(), |entries| {
                entries
                    .iter()
                    .filter(|entry| global.iter().any(|allowed| allowed.module == entry.module))
                    .cloned()
                    .collect()
            });
        policies
            .into_iter()
            .filter(|policy| {
                enrollment::declaration_available_at(&policy.module, credentials, now_ms as i64)
            })
            .collect()
    }
}

impl<'a> From<&'a crate::config_overrides::LiveConfig> for RunnerPolicies<'a> {
    fn from(config: &'a crate::config_overrides::LiveConfig) -> Self {
        Self {
            policies: config.v1_runners(),
            config: Some(config),
            consumer: None,
            auto_enroll: true,
        }
    }
}

impl From<Vec<RunnerPolicy>> for RunnerPolicies<'_> {
    fn from(policies: Vec<RunnerPolicy>) -> Self {
        // Compatibility source compiles per call; request paths must pass their LiveConfig.
        Self {
            policies,
            config: None,
            consumer: None,
            auto_enroll: false,
        }
    }
}

impl Registry {
    pub(crate) fn classifier_for_consumer(
        module: &str,
        config: &crate::config_overrides::LiveConfig,
        consumer: &crate::config_policy::ConsumerPolicy,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
    ) -> Self {
        let mut registry = Self::classifier(module, credentials, facts, now_ms);
        // Classifier selects may run only on the module the daemon verified as
        // the caller. Settings may narrow that runner, never replace it.
        // The old scalar spelling keeps its existing caller-bound behavior.
        if consumer.runners.is_none() || consumer.runner.is_some() {
            return registry;
        }
        let Some(original) = registry.entries.pop() else {
            return registry;
        };
        let policies: Vec<_> = RunnerPolicies::for_consumer(config, consumer)
            .enrolled(credentials, now_ms)
            .into_iter()
            .filter(|policy| policy.module == module)
            .collect();
        registry.entries = policies
            .iter()
            .map(|policy| {
                let mut runner = original.clone();
                runner.route_policy = placement_routes(Some(config), policy);
                runner.policy.harness.clone_from(&policy.harness);
                runner.policy.routes.as_mut().unwrap().retain(|route| {
                    runner
                        .route_policy
                        .as_ref()
                        .is_none_or(|specs| specs.resolve(route, false))
                });
                runner
            })
            .collect();
        registry.keep_distinct_placement_keys(&policies);
        registry
    }

    pub(crate) fn classifier(
        module: &str,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
    ) -> Self {
        if !credentials.classifier_models.contains_key(module)
            || !enrollment::declaration_available_at(module, credentials, now_ms as i64)
        {
            return Self::default();
        }
        super::classifier::report_missing_rows(credentials, module, now_ms as i64);
        let models: BTreeSet<String> = facts
            .iter()
            .filter(|fact| {
                super::classifier::meets_capabilities(
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
                    harness: None,
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
                requires_wire_api: false,
                wire_declaration: None,
                route_policy: None,
                reachable_models: reachable,
                statically_reachable_models: models,
                unverified_wire_models: BTreeSet::new(),
                missing_wire_api_models: BTreeSet::new(),
                no_account_models: BTreeSet::new(),
                provider_limit_models: BTreeSet::new(),
                harness_choices: None,
                placement_baseline: None,
                shared_placement_key: false,
            }],
        }
    }

    pub(crate) fn build<'a>(
        policies: impl Into<RunnerPolicies<'a>>,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
    ) -> Self {
        Self::build_resolved(policies.into(), credentials, facts, now_ms, None, false)
    }

    pub(crate) fn build_for_harness<'a>(
        policies: impl Into<RunnerPolicies<'a>>,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
        harness: Option<&str>,
    ) -> Self {
        Self::build_resolved(policies.into(), credentials, facts, now_ms, harness, false)
    }

    /// Session selection resolves defaults; a picker without a harness instead
    /// uses build() or build_for_harness(None) to keep every model allowed by
    /// the runner's top-level pairs and provider limits.
    pub(crate) fn build_for_selection<'a>(
        policies: impl Into<RunnerPolicies<'a>>,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
        harness: Option<&str>,
    ) -> Self {
        Self::build_resolved(policies.into(), credentials, facts, now_ms, harness, true)
    }

    fn build_resolved(
        source: RunnerPolicies<'_>,
        credentials: &BrocaCredentials,
        facts: &[ModelFact],
        now_ms: u64,
        harness: Option<&str>,
        resolve_default: bool,
    ) -> Self {
        let catalog = facts
            .iter()
            .collect::<crate::broca_credentials::AuthCatalog>();
        let entries = source
            .enrolled(credentials, now_ms)
            .into_iter()
            .map(|mut policy| {
                let wire_declaration = credentials.runner_declarations.get(&policy.module);
                let resolved_declaration =
                    credentials
                        .runner_declarations
                        .get(&policy.module)
                        .map(|declaration| {
                            let selected = harness.or_else(|| {
                                resolve_default
                                    .then(|| {
                                        declaration.default_harness.as_deref().or_else(|| {
                                            match declaration.harnesses.as_deref() {
                                                Some([only]) => Some(only.as_str()),
                                                _ => None,
                                            }
                                        })
                                    })
                                    .flatten()
                            });
                            let mut resolved = declaration.as_contract().clone();
                            match declaration.limits_for_harness(selected) {
                                Ok(limits) => {
                                    resolved.pairs = limits.pairs.unwrap_or_default();
                                    if let Some(providers) = limits.explicit_providers {
                                        resolved
                                            .standing_limits
                                            .get_or_insert_with(Default::default)
                                            .explicit_providers = Some(providers);
                                    }
                                }
                                Err(error) => report_harness_limits_once(&policy.module, &error),
                            }
                            resolved
                        });
                let declaration = resolved_declaration.as_ref();
                if let Some(declaration) = declaration {
                    report_unknown_pair_tokens_once(&policy.module, declaration);
                }
                let needs_vault = declaration.is_some() || policy.via_vault;
                let follows_account_choice =
                    declaration.map_or(policy.via_vault, |d| d.follows_account_choice);
                let mut unverified_wire_models = BTreeSet::new();
                let mut missing_wire_api_models = BTreeSet::new();
                let mut no_account_models = BTreeSet::new();
                let mut provider_limit_models = BTreeSet::new();
                let statically_reachable_models: BTreeSet<_> = facts
                    .iter()
                    .filter(|fact| {
                        let indexed = source.config.map(|config| {
                            crate::serving_index::ServingIndex::runner_fact(
                                config,
                                credentials,
                                fact,
                                &catalog,
                            )
                        });
                        let serving_rows: Vec<_> = indexed.as_ref().map_or_else(
                            || {
                                credentials
                                    .listed_credentials()
                                    .iter()
                                    .filter(|row| {
                                        credential_serves_fact(row, fact, &catalog, credentials)
                                    })
                                    .map(|row| (row, None))
                                    .collect()
                            },
                            |entry| {
                                entry
                                    .runner
                                    .iter()
                                    .map(|row| {
                                        (&credentials.listed_credentials()[row.index], Some(row))
                                    })
                                    .collect()
                            },
                        );
                        !needs_vault
                            || serving_rows.into_iter().any(|(row, indexed_row)| {
                                let provider_allowed = declaration
                                    .and_then(|declaration| declaration.standing_limits.as_ref())
                                    .and_then(|limits| limits.explicit_providers.as_ref())
                                    .is_none_or(|providers| {
                                        credentials.runner_allows_provider(fact, row, providers)
                                    });
                                // A provider limit is the cause only when the
                                // runner could otherwise use this account and pair.
                                let otherwise_allowed = declaration.is_none_or(|declaration| {
                                    let method = row.auth_method.as_deref().unwrap_or("");
                                    if fact
                                        .billing_plane
                                        .as_ref()
                                        .is_some_and(|plane| plane.auth_method != method)
                                    {
                                        return false;
                                    }
                                    let Some(api) = indexed_row.map_or_else(
                                        || credentials.wire_api(fact, method),
                                        |row| row.wire_api.as_deref(),
                                    ) else {
                                        if provider_allowed {
                                            unverified_wire_models.insert(format!(
                                                "{}/{}",
                                                fact.provider_id, fact.model_id
                                            ));
                                        }
                                        return false;
                                    };
                                    declaration.pairs.iter().any(|pair| {
                                        if !known_pair(pair)
                                            || api != pair.wire_api
                                            || row.auth_method.as_ref() != Some(&pair.auth_method)
                                        {
                                            return false;
                                        }
                                        // The narrowed declaration restricts pair
                                        // membership; the original pair still determines
                                        // whether an endpoint is required.
                                        let admitted = wire_declaration
                                            .map(RunnerDeclaration::as_contract)
                                            .unwrap_or(declaration)
                                            .admits_pair_row(
                                                pair,
                                                indexed_row.map_or_else(
                                                    || {
                                                        credentials
                                                            .row_serves_wire_api(fact, method)
                                                    },
                                                    |row| row.has_endpoint,
                                                ),
                                            );
                                        if !admitted && provider_allowed {
                                            missing_wire_api_models.insert(format!(
                                                "{}/{}",
                                                fact.provider_id, fact.model_id
                                            ));
                                        }
                                        admitted
                                    })
                                }) && {
                                    let offers = can_offer_account(
                                        &policy.module,
                                        follows_account_choice,
                                        row,
                                    );
                                    if !offers && provider_allowed {
                                        no_account_models.insert(format!(
                                            "{}/{}",
                                            fact.provider_id, fact.model_id
                                        ));
                                    }
                                    offers
                                };
                                if otherwise_allowed && !provider_allowed {
                                    provider_limit_models
                                        .insert(format!("{}/{}", fact.provider_id, fact.model_id));
                                }
                                otherwise_allowed && provider_allowed
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
                missing_wire_api_models
                    .retain(|model| !statically_reachable_models.contains(model));
                no_account_models.retain(|model| !statically_reachable_models.contains(model));
                provider_limit_models.retain(|model| !statically_reachable_models.contains(model));
                let route_policy = placement_routes(source.config, &policy);
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
                            && route_policy
                                .as_ref()
                                .is_none_or(|specs| specs.resolve(&key, false))
                    })
                    .map(|fact| {
                        make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    })
                    .collect();
                policy.routes = Some(routes);
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
                    requires_wire_api: declaration.is_some_and(|d| d.requires_wire_api()),
                    wire_declaration: wire_declaration.cloned(),
                    route_policy,
                    reachable_models,
                    statically_reachable_models,
                    unverified_wire_models,
                    missing_wire_api_models,
                    no_account_models,
                    provider_limit_models,
                    harness_choices: None,
                    placement_baseline: None,
                    shared_placement_key: false,
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

    pub(crate) fn get(&self, key: &(impl RunnerKey + ?Sized)) -> Option<&EffectiveRunner> {
        self.entries.iter().find(|runner| key.matches(runner))
    }

    fn keep_distinct_placement_keys(&mut self, source: &[RunnerPolicy]) {
        // Inheriting a global harness can resolve a bare project entry and an
        // explicit entry to the same pair. Keep their configured route lists as
        // keys even if they currently reach the same models (or none).
        for (i, (runner, policy)) in self.entries.iter_mut().zip(source).enumerate() {
            if source.iter().enumerate().any(|(j, other)| {
                i != j && other.module == policy.module && other.harness == policy.harness
            }) {
                runner.shared_placement_key = true;
                runner.policy.routes.clone_from(&policy.routes);
            }
        }
    }

    pub(crate) fn runs(
        &self,
        key: &(impl RunnerKey + ?Sized),
        route: &str,
        capacity: &str,
    ) -> bool {
        self.get(key).is_some_and(|runner| {
            // Variants share their model's transport and credential reach, but
            // operator route rules still judge the exact requested variant.
            runner.matches_route(route, capacity, |runner| &runner.reachable_models)
        })
    }

    pub(crate) fn can_run(
        &self,
        key: &(impl RunnerKey + ?Sized),
        route: &str,
        capacity: &str,
    ) -> bool {
        self.get(key).is_some_and(|runner| {
            runner.matches_route(route, capacity, |runner| {
                &runner.statically_reachable_models
            })
        })
    }

    pub(crate) fn no_runner_account(
        &self,
        key: &(impl RunnerKey + ?Sized),
        route: &str,
        capacity: &str,
    ) -> bool {
        self.get(key).is_some_and(|runner| {
            runner.matches_route(route, capacity, |runner| &runner.no_account_models)
        })
    }

    pub(crate) fn provider_limit(
        &self,
        key: &(impl RunnerKey + ?Sized),
        route: &str,
        capacity: &str,
    ) -> bool {
        self.get(key).is_some_and(|runner| {
            runner.matches_route(route, capacity, |runner| &runner.provider_limit_models)
        })
    }

    pub(crate) fn wire_family_unverified(
        &self,
        key: &(impl RunnerKey + ?Sized),
        route: &str,
        capacity: &str,
    ) -> bool {
        self.get(key).is_some_and(|runner| {
            runner.matches_route(route, capacity, |runner| &runner.unverified_wire_models)
        })
    }

    pub(crate) fn requires_wire_api(
        &self,
        key: &(impl RunnerKey + ?Sized),
        route: &str,
        capacity: &str,
    ) -> bool {
        self.get(key).is_some_and(|runner| {
            runner.matches_route(route, capacity, |runner| &runner.missing_wire_api_models)
        })
    }
}

pub(crate) fn effective(deps: &super::Deps<'_>) -> Registry {
    let mut facts = deps.store.get_facts().unwrap_or_default();
    deps.broca_credentials
        .lock()
        .map(|credentials| {
            let targets = credentials.catalog_targets(&facts);
            striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
            Registry::build(
                super::operator_config(deps).as_ref(),
                &credentials,
                &facts,
                crate::now_ms(),
            )
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod compiled_registry_tests {
    use super::*;
    use crate::routing_v1::request_cpu_bench::{self as bench, FixtureCounts};

    #[test]
    fn snapshot_runner_tables_equal_vec_source_on_live_fixture_and_harnesses() {
        let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
        let fixture = bench::representative_fixture(FixtureCounts {
            subscription_rules: 10,
            exclusion_rules: 103,
            runner_route_rules: 0,
            catalog_facts: 836,
            listed_credentials: 39,
        });
        for with_rules in [false, true] {
            if with_rules {
                fixture.harness.edit_config(|config| {
                    config["runners"][0]["routes"] = serde_json::json!([
                        "cpu-provider-0/model-0",
                        "cpu-provider-0/model-0/variant",
                        "cpu-provider-0",
                        "cpu-provider-1",
                        "cpu-provider-1/model-1",
                    ]);
                });
            }
            let config = fixture.operator_config();
            let shared = fixture.credentials();
            let credentials = shared.lock().unwrap();
            let facts = fixture.store().get_facts().unwrap();
            let harnesses: BTreeSet<_> = credentials
                .runner_declarations
                .values()
                .flat_map(|runner| runner.harnesses.iter().flatten().cloned())
                .chain(["default".into(), "alternate".into()])
                .collect();
            for harness in
                std::iter::once(None).chain(harnesses.iter().map(|harness| Some(harness.as_str())))
            {
                for selection in [false, true] {
                    let from_config = if selection {
                        Registry::build_for_selection(
                            config.as_ref(),
                            &credentials,
                            &facts,
                            bench::FIXTURE_NOW,
                            harness,
                        )
                    } else {
                        Registry::build_for_harness(
                            config.as_ref(),
                            &credentials,
                            &facts,
                            bench::FIXTURE_NOW,
                            harness,
                        )
                    };
                    let from_vec = if selection {
                        Registry::build_for_selection(
                            config.v1_runners(),
                            &credentials,
                            &facts,
                            bench::FIXTURE_NOW,
                            harness,
                        )
                    } else {
                        Registry::build_for_harness(
                            config.v1_runners(),
                            &credentials,
                            &facts,
                            bench::FIXTURE_NOW,
                            harness,
                        )
                    };
                    assert_eq!(
                        serde_json::to_value(&from_config.entries).unwrap(),
                        serde_json::to_value(&from_vec.entries).unwrap()
                    );
                    for policy in config.v1_runners() {
                        for fact in &facts {
                            let capacity = format!("{}/{}", fact.provider_id, fact.model_id);
                            for route in [capacity.clone(), format!("{capacity}/variant/extra")] {
                                assert_eq!(
                                    from_config.runs(&policy.module, &route, &capacity),
                                    from_vec.runs(&policy.module, &route, &capacity)
                                );
                                assert_eq!(
                                    from_config.can_run(&policy.module, &route, &capacity),
                                    from_vec.can_run(&policy.module, &route, &capacity)
                                );
                                assert_eq!(
                                    from_config.no_runner_account(
                                        &policy.module,
                                        &route,
                                        &capacity
                                    ),
                                    from_vec.no_runner_account(&policy.module, &route, &capacity)
                                );
                                assert_eq!(
                                    from_config.wire_family_unverified(
                                        &policy.module,
                                        &route,
                                        &capacity
                                    ),
                                    from_vec.wire_family_unverified(
                                        &policy.module,
                                        &route,
                                        &capacity
                                    )
                                );
                                assert_eq!(
                                    from_config.requires_wire_api(
                                        &policy.module,
                                        &route,
                                        &capacity
                                    ),
                                    from_vec.requires_wire_api(&policy.module, &route, &capacity)
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Read runner facts with the shared routing role declaration validator.
pub(crate) fn parse_declaration(
    slot: &serde_json::Value,
) -> Result<Option<RunnerDeclaration>, String> {
    super::consumers::declared::parse(slot)
        .map(|declaration| declaration.runner.map(RunnerDeclaration::read))
        .map_err(|error| error.to_string())
}

fn known_pair(pair: &RunnerPair) -> bool {
    WIRE_APIS.contains(&pair.wire_api.as_str()) && AUTH_METHODS.contains(&pair.auth_method.as_str())
}

fn report_harness_limits_once(
    module: &str,
    error: &cortexkit_role_routing::declarations::HarnessLimitsError,
) {
    use std::sync::Mutex;
    static REPORTED: Mutex<BTreeSet<(String, String)>> = Mutex::new(BTreeSet::new());
    if let Ok(mut reported) = REPORTED.lock() {
        if reported.insert((module.into(), error.to_string())) {
            crate::warn_operator(format!(
                "[striatum] runner {module}: {error}; using runner-level limits for all harnesses"
            ));
        }
    }
}

fn report_unknown_pair_tokens_once(
    module: &str,
    declaration: &cortexkit_role_routing::declarations::RunnerDeclaration,
) {
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
                    "[striatum] runner {module}: unknown {field} token {token:?}; pair cannot match"
                ));
            }
        }
    }
}

/// Read the current trusted declarations. DescribeCache retains absent configured
/// runners briefly; a present row that loses trust or withdraws facts is not retained.
pub(crate) fn declarations_from_catalog(
    modules: &serde_json::Value,
) -> std::collections::BTreeMap<String, RunnerDeclaration> {
    modules
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            if entry
                .get("nonce_verified")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
                && entry
                    .get("first_party")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
            {
                return None;
            }
            let module = entry.get("module_id")?.as_str()?;
            let slot = entry
                .get("role_declarations")?
                .get(cortexkit_role_routing::ROLE_DECLARATIONS_KEY)?;
            match parse_declaration(slot) {
                Ok(Some(declaration)) => {
                    if let Some(error) = declaration.harness_limits_error() {
                        report_harness_limits_once(module, error);
                    }
                    report_unknown_pair_tokens_once(module, &declaration);
                    Some((module.to_owned(), declaration))
                }
                Ok(None) => None,
                Err(error) => {
                    crate::warn_operator(format!(
                        "[striatum] ignored invalid runner declaration from {module}: {error}"
                    ));
                    None
                }
            }
        })
        .collect()
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
                && entry
                    .get("first_party")
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
    let wake = credentials
        .lock()
        .expect("credentials lock")
        .runner_catalog_wake
        .clone();
    let mut backoff = WATCH_RETRY_MIN;
    loop {
        let connected = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            SubcConsumer::connect(&connection_file, ConsumerOptions::default()),
        )
        .await;
        if let Ok(Ok(consumer)) = connected {
            let (updates, mut changes) = tokio::sync::watch::channel(CatalogWatchState::default());
            let watch = watch_catalog(&consumer, &updates);
            let refresh = async {
                let mut discovery = Discovery {
                    consumer: &consumer,
                    connection_file: &connection_file,
                    credentials: &credentials,
                    config_path: &config_path,
                    cache: &mut cache,
                    classifiers: &mut classifiers,
                    retries: BTreeMap::new(),
                };
                loop {
                    let revision = changes.borrow_and_update().revision;
                    if !discovery.refresh_pass().await {
                        break;
                    }
                    let mut deadline = tokio::time::Instant::now() + WATCH_BACKSTOP;
                    loop {
                        match wait_for_catalog_refresh(
                            &mut changes,
                            revision,
                            &wake,
                            &mut deadline,
                            discovery.next_retry(),
                        )
                        .await
                        {
                            Refresh::Catalog => break,
                            Refresh::Describe => {
                                if !discovery.retry_describes().await {
                                    return;
                                }
                            }
                            Refresh::Reconnect => return,
                        }
                    }
                }
            };
            tokio::pin!(watch, refresh);
            tokio::select! {
                // A failed pass rebuilds the consumer, including its watch.
                _ = &mut refresh => {},
                // Finish publication before replacing a closed watch's consumer.
                _ = &mut watch => refresh.await,
            }
            if updates.borrow().revision != 0 {
                backoff = WATCH_RETRY_MIN;
            }
        } else if let Ok(mut credentials) = credentials.lock() {
            cache.missed_all(&mut credentials, crate::now_ms() as i64);
            classifiers.expire(&mut credentials, crate::now_ms() as i64);
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {},
            _ = wake.notified() => {},
        }
        backoff = (backoff * 2).min(WATCH_RETRY_MAX);
    }
}

// Leave time for the catalog read and serialized describes before views expire.
const WATCH_BACKSTOP: std::time::Duration =
    std::time::Duration::from_millis(describe_cache::LAST_GOOD_MS as u64 / 2);
const WATCH_FALLBACK: std::time::Duration = std::time::Duration::from_secs(30);
const WATCH_RETRY_MIN: std::time::Duration = std::time::Duration::from_secs(1);
const WATCH_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Copy, Default)]
struct CatalogWatchState {
    revision: u64,
    healthy: bool,
    rebuild: bool,
}

async fn watch_catalog(
    consumer: &subc_client_rs::SubcConsumer,
    updates: &tokio::sync::watch::Sender<CatalogWatchState>,
) {
    let mut backoff = WATCH_RETRY_MIN;
    loop {
        if let Ok(mut subscription) = consumer.catalog_watch().await {
            let mut generation = None;
            while let Ok(Some(event)) = subscription.next().await {
                if generation != Some(event.generation) {
                    generation = Some(event.generation);
                    backoff = WATCH_RETRY_MIN;
                    updates.send_modify(|state| {
                        state.healthy = true;
                        state.revision = state.revision.wrapping_add(1);
                    });
                }
            }
            // Once a feed has worked, its termination may mean the daemon
            // restarted. Replace the connection and re-read before trusting it.
            if generation.is_some() {
                updates.send_modify(|state| {
                    state.healthy = false;
                    state.rebuild = true;
                });
                return;
            }
        }
        updates.send_modify(|state| state.healthy = false);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(WATCH_RETRY_MAX);
    }
}

enum Refresh {
    Catalog,
    Describe,
    Reconnect,
}

async fn wait_for_catalog_refresh(
    changes: &mut tokio::sync::watch::Receiver<CatalogWatchState>,
    revision: u64,
    wake: &tokio::sync::Notify,
    deadline: &mut tokio::time::Instant,
    retry: Option<tokio::time::Instant>,
) -> Refresh {
    loop {
        let state = *changes.borrow_and_update();
        if state.rebuild {
            return Refresh::Reconnect;
        }
        // Only one latest revision is retained while a pass runs. Mark it seen
        // at the start of the next pass, never at the end of the current one.
        if state.revision != revision {
            return Refresh::Catalog;
        }
        if !state.healthy {
            *deadline = (*deadline).min(tokio::time::Instant::now() + WATCH_FALLBACK);
        }
        tokio::select! {
            _ = tokio::time::sleep_until(*deadline) => return Refresh::Catalog,
            _ = wake.notified() => return Refresh::Catalog,
            _ = async { tokio::time::sleep_until(retry.unwrap()).await }, if retry.is_some() => return Refresh::Describe,
            result = changes.changed() => if result.is_err() { return Refresh::Reconnect; },
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DescribeTarget {
    Classifier(String),
    Tiers(String),
}

struct DescribeRetry {
    due: tokio::time::Instant,
    delay: std::time::Duration,
}

struct Discovery<'a> {
    consumer: &'a subc_client_rs::SubcConsumer,
    connection_file: &'a std::path::Path,
    credentials: &'a crate::SharedBrocaCredentials,
    config_path: &'a std::path::Path,
    cache: &'a mut DescribeCache,
    classifiers: &'a mut super::classifier::DescribeCache,
    retries: BTreeMap<DescribeTarget, DescribeRetry>,
}

impl Discovery<'_> {
    async fn refresh_pass(&mut self) -> bool {
        striatum_store::job_timing::run("runner_declarations", self.read_catalog()).await
    }

    async fn read_catalog(&mut self) -> bool {
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let catalog = self
                .consumer
                .catalog_list()
                .await
                .map_err(|error| error.to_string())?;
            let modules =
                serde_json::to_value(catalog.modules).map_err(|error| error.to_string())?;
            Ok::<_, String>(modules)
        })
        .await;
        let Ok(Ok(modules)) = result else {
            if let Ok(mut credentials) = self.credentials.lock() {
                self.cache
                    .missed_all(&mut credentials, crate::now_ms() as i64);
                self.classifiers
                    .expire(&mut credentials, crate::now_ms() as i64);
            }
            return false;
        };
        super::consumers::replace_catalog(&modules);
        let policies = crate::config_overrides::live_config(self.config_path).v1_runners();
        let targets: BTreeSet<_> = self
            .credentials
            .lock()
            .map(|mut credentials| {
                let now = crate::now_ms() as i64;
                let tiers = self
                    .cache
                    .catalog(&modules, &policies, &mut credentials, now);
                placements::declarations_changed(&credentials.runner_declarations);
                let classifiers = self.classifiers.catalog(&modules, &mut credentials, now);
                classifiers
                    .into_iter()
                    .map(DescribeTarget::Classifier)
                    .chain(tiers.into_iter().map(DescribeTarget::Tiers))
                    .collect()
            })
            .unwrap_or_default();
        self.retries.retain(|target, _| targets.contains(target));
        for target in targets {
            if self
                .retries
                .get(&target)
                .is_none_or(|retry| retry.due <= tokio::time::Instant::now())
            {
                if !self.describe(target, &policies).await {
                    return false;
                }
            }
        }
        true
    }

    fn next_retry(&self) -> Option<tokio::time::Instant> {
        self.retries.values().map(|retry| retry.due).min()
    }

    async fn retry_describes(&mut self) -> bool {
        let now = tokio::time::Instant::now();
        let targets: Vec<_> = self
            .retries
            .iter()
            .filter(|(_, retry)| retry.due <= now)
            .map(|(target, _)| target.clone())
            .collect();
        let policies = crate::config_overrides::live_config(self.config_path).v1_runners();
        for target in targets {
            if !self.describe(target, &policies).await {
                return false;
            }
        }
        true
    }

    async fn describe(&mut self, target: DescribeTarget, policies: &[RunnerPolicy]) -> bool {
        let success = match &target {
            DescribeTarget::Classifier(module) => {
                let result = super::classifier::fetch_describe(
                    self.consumer,
                    self.connection_file,
                    module,
                    policies,
                    self.credentials,
                )
                .await;
                if result
                    .as_ref()
                    .is_err_and(|error| error.needs_catalog_reconnect())
                {
                    return false;
                }
                self.credentials.lock().is_ok_and(|mut credentials| {
                    let revoked = matches!(result, Ok(None));
                    if revoked {
                        self.classifiers.revoke(module, &mut credentials);
                    }
                    let valid = self.classifiers.reply(
                        module,
                        result.as_ref().ok().and_then(Option::as_ref),
                        &mut credentials,
                        crate::now_ms() as i64,
                    );
                    revoked || valid
                })
            }
            DescribeTarget::Tiers(module) => {
                let describe = admission::fetch_describe_result(
                    self.consumer,
                    module,
                    policies,
                    self.credentials,
                )
                .await;
                if matches!(
                    describe,
                    Err(enrollment::DescribeError::TargetChangedNeverSent)
                ) {
                    // The route opened to a different target than the catalog
                    // advertised. Re-read the catalog before sending a describe.
                    return false;
                }
                self.credentials.lock().is_ok_and(|mut credentials| {
                    self.cache.reply(
                        module,
                        describe.as_ref().ok().and_then(Option::as_ref),
                        &mut credentials,
                        crate::now_ms() as i64,
                    )
                })
            }
        };
        if success {
            self.retries.remove(&target);
        } else {
            // Only a catalog exchange failure replaces the shared consumer. A
            // describe miss retries just that endpoint without extending its TTL.
            let delay = self.retries.get(&target).map_or(WATCH_RETRY_MIN, |retry| {
                (retry.delay * 2).min(WATCH_RETRY_MAX)
            });
            self.retries.insert(
                target,
                DescribeRetry {
                    due: tokio::time::Instant::now() + delay,
                    delay,
                },
            );
        }
        true
    }
}
