//! Apply consumer exclusions and select pinned models without quality ranking.

use crate::config_policy::SynthesizerPin;
use striatum_core::model_facts::ModelRequirements;
use striatum_core::model_router::{SelectModelRouteInput, SelectedModelRoute};
use striatum_core::provider_model_spec::ExclusionPolicy;
use striatum_core::{ConcurrencySlots, RoutingConfig};
use serde::Deserialize;
use std::collections::HashMap;

use striatum_core::model_facts::{fact_to_route, ModelFact};

/// Normalized selection inputs. Wire adapters supply reachability and hard-pin
/// admission, but share requirements, entity rules, ordering and scoring here.
#[derive(Clone, Copy)]
pub(crate) struct SelectionPass<'a> {
    pub consumer: &'a str,
    pub target_agent: &'a str,
    pub requirements: ModelRequirements,
    pub required_capabilities: Option<striatum_core::model_router::RequiredCapabilities>,
    pub facts: &'a [ModelFact],
    pub snapshot: &'a striatum_core::ModelRouteStateSnapshot,
    pub exclude: &'a [String],
    pub operator_exclude: &'a [String],
    pub consumer_exclude: &'a [String],
    pub subscription_keys: &'a std::collections::HashSet<String>,
    pub priced_per_token: bool,
    pub rejections: &'a HashMap<String, String>,
    pub config: &'a RoutingConfig,
    pub slots: Option<&'a ConcurrencySlots>,
    pub min_band: Option<f64>,
    pub prefer_prompt_cache: bool,
    pub providers_without_prompt_cache: &'a [String],
    pub operator_config: &'a crate::config_overrides::LiveConfig,
}

impl<'a> SelectionPass<'a> {
    pub fn routing_config(&self) -> RoutingConfig {
        let policy = self.operator_config.consumer_policy(
            self.consumer,
            &crate::config_policy::ConsumerPolicy::default(),
            &std::collections::BTreeMap::new(),
        );
        RoutingConfig {
            allow_best_effort: policy.below_bar.unwrap_or_default()
                == crate::config_policy::BelowBarPolicy::BestEffort,
            ..*self.config
        }
    }

    pub fn input(
        &self,
        available: &std::collections::HashSet<String>,
        snapshot: striatum_core::ModelRouteStateSnapshot,
    ) -> SelectModelRouteInput<'a> {
        let subscription_keys = self.subscription_keys.clone();
        let priced_per_token = self.priced_per_token;
        SelectModelRouteInput {
            target_agent: self.target_agent.to_owned(),
            target_is_head: self.operator_config.is_head_agent(self.target_agent),
            requirements: self.requirements,
            available_models: available.clone(),
            facts: self.facts.to_vec(),
            runtime_snapshot: Some(snapshot),
            exclude_route_keys: self.exclude.to_vec(),
            operator_exclude_keys: self.operator_exclude.to_vec(),
            required_capabilities: self.required_capabilities,
            is_subscription_route: Some(Box::new(move |route| {
                subscription_covered(priced_per_token, &subscription_keys, route)
            })),
            min_best_effort_adequacy: self.min_band,
            prefer_prompt_cache: self.prefer_prompt_cache,
            providers_without_prompt_cache: self
                .providers_without_prompt_cache
                .iter()
                .cloned()
                .collect(),
            providers_without_cache_write_reports: Default::default(),
        }
    }

    pub fn score(
        &self,
        available: &std::collections::HashSet<String>,
    ) -> Result<SelectedModelRoute, striatum_core::model_router::NoRouteError> {
        let allowed = available
            .iter()
            .filter(|route| {
                self.operator_config
                    .route_allowed(self.consumer, route, false)
            })
            .cloned()
            .collect();
        let result = select_candidates(
            self.input(&allowed, self.snapshot.clone()),
            &self.routing_config(),
            self.slots,
            self.consumer_exclude,
            self.rejections,
        );
        explain_dotted_exclusions(result, self.operator_config, self.consumer)
    }

    pub fn select<E>(
        &self,
        available: &std::collections::HashSet<String>,
        request_pin: Option<&str>,
        project_pin: Option<&str>,
        continuing: Option<&str>,
        admit_pin: impl FnMut(&str) -> Result<SelectedModelRoute, E>,
    ) -> Result<
        (SelectedModelRoute, OrderBasis),
        OrderFailure<E, striatum_core::model_router::NoRouteError>,
    > {
        let policy = self.operator_config.consumer_policy(
            self.consumer,
            &crate::config_policy::ConsumerPolicy::default(),
            &std::collections::BTreeMap::new(),
        );
        select_ordered(
            available,
            request_pin,
            project_pin,
            continuing,
            admit_pin,
            |only| self.score(only),
            || select_in_order(available, &policy, |only| self.score(only)),
        )
    }
}

pub(crate) enum CandidateSource<'a> {
    Host {
        auth_types: &'a HashMap<String, String>,
    },
    Vault {
        credentials: &'a crate::broca_credentials::SharedBrocaCredentials,
        /// The fences selection enforces: live fences except the stale ones a
        /// probe may admit.
        fences: &'a [striatum_core::route_state_store::CredentialEntitlementFence],
        /// The stale fences a probe may admit. Key-pool providers (those with
        /// no catalog rows of their own) are still judged against every live
        /// fence including these: the pool leases no probed key, so a stale
        /// fence there holds until an operator lifts it.
        stale: &'a [striatum_core::route_state_store::CredentialEntitlementFence],
        /// Legacy pool pins report per-key lease failures through the pool's
        /// existing refusal adapter. Automatic and v1 selection reject here.
        defer_pool_pin: bool,
    },
}

pub(crate) struct Candidates {
    pub available: std::collections::HashSet<String>,
    pub subscription_keys: std::collections::HashSet<String>,
    pub rejected: HashMap<String, String>,
    pub filters: HashMap<String, String>,
}

pub(crate) fn select_candidates(
    input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    consumer_exclude: &[String],
    rejected: &HashMap<String, String>,
) -> Result<SelectedModelRoute, striatum_core::model_router::NoRouteError> {
    striatum_core::model_router::select_model_route_with_availability(
        input,
        config,
        slots,
        consumer_exclude,
        rejected,
    )
    .map_err(describe_filter_refusal)
}

/// A ladder tries its configured entries in order. Credential refusals stay in
/// the explanation even when a later entry succeeds; they are never a hold.
pub(crate) fn select_in_order(
    available: &std::collections::HashSet<String>,
    policy: &crate::config_policy::ConsumerPolicy,
    attempt: impl Fn(
        &std::collections::HashSet<String>,
    )
        -> Result<SelectedModelRoute, striatum_core::model_router::NoRouteError>,
) -> Result<SelectedModelRoute, striatum_core::model_router::NoRouteError> {
    use crate::config_policy::ConsumerMode;
    if policy.mode != Some(ConsumerMode::Ladder) {
        return attempt(available);
    }
    let mut rejected = Vec::new();
    let mut last = None;
    for pin in policy.ladder.iter().flatten() {
        let only = available
            .iter()
            .filter(|key| *key == pin || key.starts_with(&format!("{pin}/")))
            .cloned()
            .collect();
        match attempt(&only) {
            Ok(mut selected) => {
                selected.rejected.retain(|entry| {
                    !rejected.iter().any(
                        |prior: &striatum_core::model_router::RejectedRoute| {
                            prior.route_key == entry.route_key
                        },
                    )
                });
                selected.rejected.extend(rejected);
                return Ok(selected);
            }
            Err(error) => {
                rejected.extend(
                    error
                        .rejected
                        .iter()
                        .filter(|entry| {
                            entry.route_key == *pin
                                || entry.route_key.starts_with(&format!("{pin}/"))
                        })
                        .cloned(),
                );
                last = Some(error);
            }
        }
    }
    let mut error = last.unwrap_or_else(|| attempt(&std::collections::HashSet::new()).unwrap_err());
    error.rejected = rejected;
    Err(error)
}

/// The candidate/credential pass shared by both wire shapes. Reachability is
/// supplied by the runner adapter; host reachability is the host's own list.
/// Permanent credential refusals are not quota walls and cannot produce a hold.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_candidates(
    facts: &[ModelFact],
    snapshot: &mut striatum_core::ModelRouteStateSnapshot,
    available: std::collections::HashSet<String>,
    mut rejected: HashMap<String, String>,
    source: CandidateSource<'_>,
    classifier: bool,
    consumer: Option<Consumer>,
    priced_per_token: bool,
    allowed_providers: Option<&[String]>,
    subscription_plan_providers: Option<&std::collections::HashSet<String>>,
    policy: &striatum_core::launch_policy::ModelRoutingPolicyInput,
    catalog_targets: &std::collections::BTreeMap<String, String>,
    classes: Option<&crate::account_selection::ClassContext>,
) -> Candidates {
    use striatum_core::model_router::CredentialRefusalMark;
    use striatum_core::provider_model_spec::{parse_spec_list, resolve_spec};

    let specs = parse_spec_list(&policy.subscription);
    let mut subscription_keys = std::collections::HashSet::new();
    let filters;
    match source {
        CandidateSource::Host { auth_types } => {
            // Return before touching any vault or account API. A host can route
            // even when routing has no credentials or account sets of its own.
            for fact in facts {
                let route = fact_to_route(fact);
                if resolve_spec(
                    &specs,
                    &route.route_key,
                    auth_types
                        .get(&fact.provider_id)
                        .is_some_and(|kind| kind == "oauth"),
                ) {
                    subscription_keys.insert(route.route_key);
                }
            }
            filters = candidate_rejections_priced(
                facts,
                consumer,
                priced_per_token,
                allowed_providers,
                subscription_plan_providers,
                None,
            );
        }
        CandidateSource::Vault {
            credentials,
            fences,
            stale,
            defer_pool_pin,
        } => {
            let full: Vec<striatum_core::route_state_store::CredentialEntitlementFence> =
                fences.iter().chain(stale.iter()).cloned().collect();
            let guard = credentials.lock().ok();
            let rows = guard
                .as_ref()
                .map_or(&[][..], |guard| guard.listed_credentials());
            let catalog_providers: std::collections::BTreeSet<String> = facts
                .iter()
                .filter(|fact| !catalog_targets.contains_key(&fact.provider_id))
                .map(|fact| fact.provider_id.clone())
                .collect();
            let mut metered_accounts = std::collections::HashSet::new();
            for fact in facts {
                let route = fact_to_route(fact);
                let provider_rows: Vec<_> = rows
                    .iter()
                    .filter(|row| {
                        row.state == "active"
                            && row
                                .categories
                                .iter()
                                .any(|category| category == "llm-provider")
                            && crate::broca_credentials::credential_providers(
                                row,
                                &catalog_providers,
                            )
                            .contains(&fact.provider_id)
                    })
                    .collect();
                let serving: Vec<_> = provider_rows
                    .iter()
                    .copied()
                    .filter(|row| {
                        fact.auth_methods.as_ref().is_none_or(|methods| {
                            row.auth_method
                                .as_ref()
                                .is_some_and(|method| methods.contains(method))
                        })
                    })
                    .collect();
                if serving.is_empty() {
                    snapshot
                        .by_route
                        .entry(route.route_key.clone())
                        .or_default()
                        .statically_routable = Some(false);
                }
                let resolved = classes.map(|classes| classes.class(fact, priced_per_token));
                let fallback = crate::account_selection::resolve_class(
                    rows,
                    fact,
                    &catalog_providers,
                    &specs,
                    catalog_targets.contains_key(&fact.provider_id),
                    priced_per_token,
                    false,
                    |_| true,
                );
                let resolved = resolved.as_ref().unwrap_or(&fallback);
                if priced_per_token && !resolved.rows.is_empty() && !resolved.subscription {
                    metered_accounts.insert(route.route_key.clone());
                }
                if resolved.subscription {
                    subscription_keys.insert(route.route_key.clone());
                }
                let class: Vec<_> = serving
                    .iter()
                    .copied()
                    .filter(|row| resolved.rows.iter().any(|allowed| allowed.id == row.id))
                    .collect();
                let identities: Vec<_> = class
                    .iter()
                    .map(|row| (row.id.clone(), row.account_id.clone()))
                    .collect();
                let pool_fact = catalog_targets.contains_key(&fact.provider_id);
                // Pool facts are judged against every live fence: the pool
                // leases no probed key, so exempting a stale fence here would
                // only record a selection the launch then refuses.
                let judged = if pool_fact { &full } else { fences };
                let refused =
                    crate::credential_fences::unentitled(judged, &fact.provider_id, &identities);
                if class.is_empty() || (!refused.is_empty() && !(defer_pool_pin && pool_fact)) {
                    snapshot
                        .by_capacity
                        .entry(route.capacity_key.clone())
                        .or_default()
                        .credential_refusal = Some(CredentialRefusalMark {
                        reason: if class.is_empty() {
                            "no_eligible_account"
                        } else {
                            crate::credential_fences::rejection_reason(
                                judged,
                                &fact.provider_id,
                                &refused,
                            )
                        },
                        credential_ids: if class.is_empty() {
                            provider_rows.iter().map(|row| row.id.clone()).collect()
                        } else {
                            refused
                        },
                    });
                }
            }
            let mut admission_facts = guard.as_ref().map_or_else(
                || facts.to_vec(),
                |guard| guard.admission_facts(facts, consumer, snapshot.now as u64),
            );
            // A consumer priced per token by its flag, rather than by the
            // historical request class, still needs the vault's payment
            // methods on the admission copy. Merging again is harmless when
            // the class already injected them.
            if priced_per_token {
                admission_facts = with_vault_methods(&admission_facts, rows);
            }
            filters = candidate_rejections_priced(
                &admission_facts,
                consumer,
                priced_per_token,
                allowed_providers,
                subscription_plan_providers,
                Some(&metered_accounts),
            );
        }
    }
    rejected.extend(filters.clone());
    let structural: HashMap<_, _> = facts.iter().flat_map(|fact| {
        striatum_core::model_facts::llm_classification_rejection(fact, classifier)
            .into_iter().flat_map(|reason| {
                let route = fact_to_route(fact);
                [(route.route_key, reason.clone()), (route.capacity_key, reason)]
            })
    }).collect();
    let mut filters = filters;
    filters.extend(structural.clone());
    rejected.extend(structural);
    Candidates {
        available,
        subscription_keys,
        rejected,
        filters,
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Consumer {
    Mason,
    Sidekick,
    Athena,
    Lab,
    Core,
    Codemode,
    Flow,
}

impl Consumer {
    /// Every request class, in declaration order.
    pub(crate) const ALL: [Consumer; 7] = [
        Consumer::Mason,
        Consumer::Sidekick,
        Consumer::Athena,
        Consumer::Lab,
        Consumer::Core,
        Consumer::Codemode,
        Consumer::Flow,
    ];

    /// The request class named by a consumer's root, if it names one.
    pub(crate) fn from_id(id: &str) -> Option<Consumer> {
        let root = id.split('.').next().unwrap_or(id);
        Self::ALL
            .into_iter()
            .find(|consumer| crate::variant_policy::consumer_group(Some(*consumer)) == Some(root))
    }
}

pub(crate) fn explain_dotted_exclusions(
    mut result: Result<SelectedModelRoute, striatum_core::model_router::NoRouteError>,
    config: &crate::config_overrides::LiveConfig,
    consumer: &str,
) -> Result<SelectedModelRoute, striatum_core::model_router::NoRouteError> {
    let quality_refusal = matches!(
        &result,
        Err(error) if error.reason == striatum_core::model_router::RouteFailureReason::NoAdequateRoute
    );
    let rejected = match &mut result {
        Ok(selected) => &mut selected.rejected,
        Err(error) => &mut error.rejected,
    };
    for rejection in rejected {
        if rejection.reason == "excluded_by_operator" || rejection.reason == "not_available" {
            if let Some(detail) = config.dotted_rule_refusal(consumer, &rejection.route_key) {
                rejection.detail = Some(detail);
            } else if quality_refusal {
                if let Some(detail) = config.route_policy_refusal(consumer, &rejection.route_key) {
                    rejection.detail = Some(detail);
                }
            }
        }
    }
    result
}

pub(crate) fn requires_metered(consumer: Option<Consumer>) -> bool {
    matches!(consumer, Some(Consumer::Codemode | Consumer::Flow))
}

/// Whether this selection runs priced per token. An explicit
/// `pay_per_token` on the consumer is the authority; the consumer's name does
/// not imply a payment class. Conversion writes flags for the old classes.
pub(crate) fn priced_per_token(
    policy: &crate::config_policy::ConsumerPolicy,
    _consumer: Option<Consumer>,
) -> bool {
    policy.pay_per_token == Some(true)
}

/// Copy vault payment methods onto an admission-only fact list. Used when a
/// consumer is priced per token by its flag, so the metered filter can see
/// methods the catalog row itself does not list.
fn with_vault_methods(
    facts: &[ModelFact],
    rows: &[cortexkit_claustrum_client::ListedCredential],
) -> Vec<ModelFact> {
    let mut methods_by_provider: HashMap<String, std::collections::BTreeSet<String>> =
        HashMap::new();
    for row in rows {
        if row.state != "active"
            || !row
                .categories
                .iter()
                .any(|category| category == "llm-provider")
        {
            continue;
        }
        let Some(method) = row.auth_method.clone().filter(|method| !method.is_empty()) else {
            continue;
        };
        for provider in &row.provider_ids {
            methods_by_provider
                .entry(provider.clone())
                .or_default()
                .insert(method.clone());
        }
    }
    facts
        .iter()
        .cloned()
        .map(|mut fact| {
            if let Some(methods) = methods_by_provider.get(&fact.provider_id) {
                let auth = fact.auth_methods.get_or_insert_with(Vec::new);
                for method in methods {
                    if !auth.contains(method) {
                        auth.push(method.clone());
                    }
                }
            }
            fact
        })
        .collect()
}

/// Why the ordered pass chose the route it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderBasis {
    RequestPin,
    ProjectPin,
    Continuing,
    Mode,
}

/// A pin refusal, or the scorer's refusal after pins and a continuing model
/// have been tried.
#[derive(Debug)]
pub(crate) enum OrderFailure<PinE, ScoreE> {
    Pin(PinE),
    Score(ScoreE),
}

/// Routes in `available` that are the named pin or one of its variants.
pub(crate) fn routes_matching(
    available: &std::collections::HashSet<String>,
    pin: &str,
) -> std::collections::HashSet<String> {
    available
        .iter()
        .filter(|key| *key == pin || key.starts_with(&format!("{pin}/")))
        .cloned()
        .collect()
}

/// Request pin, then project pin, then a continuing model, then `mode`.
///
/// A present pin is a one-entry ladder: it does not fall through. A continuing
/// model is kept only while `score` still accepts it; otherwise `mode` runs.
/// `mode` is the consumer's ladder or demand, supplied by the caller so a
/// ladder can keep the refusals `select_in_order` records.
pub(crate) fn select_ordered<T, PinE, ScoreE>(
    available: &std::collections::HashSet<String>,
    request_pin: Option<&str>,
    project_pin: Option<&str>,
    continuing: Option<&str>,
    mut admit_pin: impl FnMut(&str) -> Result<T, PinE>,
    mut score: impl FnMut(&std::collections::HashSet<String>) -> Result<T, ScoreE>,
    mode: impl FnOnce() -> Result<T, ScoreE>,
) -> Result<(T, OrderBasis), OrderFailure<PinE, ScoreE>> {
    if let Some(pin) = request_pin {
        return admit_pin(pin)
            .map(|selected| (selected, OrderBasis::RequestPin))
            .map_err(OrderFailure::Pin);
    }
    if let Some(pin) = project_pin {
        return admit_pin(pin)
            .map(|selected| (selected, OrderBasis::ProjectPin))
            .map_err(OrderFailure::Pin);
    }
    if let Some(previous) = continuing {
        let only = routes_matching(available, previous);
        if !only.is_empty() {
            if let Ok(selected) = score(&only) {
                return Ok((selected, OrderBasis::Continuing));
            }
        }
    }
    mode()
        .map(|selected| (selected, OrderBasis::Mode))
        .map_err(OrderFailure::Score)
}

/// The runner for a chosen route: the consumer's `runner` when it names one,
/// otherwise the first entry in array order that can run the route.
///
/// Module ids are not sorted, and session data does not choose the runner.
pub(crate) fn choose_runner<'a>(
    runners: &'a [crate::config_policy::RunnerPolicy],
    consumer_runner: Option<&str>,
    route_key: &str,
    capacity_key: &str,
    runs: impl Fn(&crate::config_policy::RunnerPolicy, &str, &str) -> bool,
) -> Option<&'a crate::config_policy::RunnerPolicy> {
    if let Some(module) = consumer_runner.filter(|module| !module.is_empty()) {
        return runners
            .iter()
            .find(|runner| runner.module == *module && runs(runner, route_key, capacity_key));
    }
    runners
        .iter()
        .find(|runner| runs(runner, route_key, capacity_key))
}

/// Whether selection prices `route` as paid from subscription quota: it is
/// on the subscription list, and this selection is not priced per token.
pub(crate) fn subscription_covered(
    priced_per_token: bool,
    subscription_keys: &std::collections::HashSet<String>,
    route: &striatum_core::ModelRoute,
) -> bool {
    !priced_per_token
        && (subscription_keys.contains(&route.route_key)
            || subscription_keys.contains(&route.capacity_key))
}

/// Admission uses these route-local refusals after exclusions and before scoring,
/// including for pins. Neither an explicit model nor a quota retry lifts them.
pub(crate) fn candidate_rejections(
    facts: &[ModelFact],
    consumer: Option<Consumer>,
    allowed_providers: Option<&[String]>,
    subscription_plan_providers: Option<&std::collections::HashSet<String>>,
) -> HashMap<String, String> {
    candidate_rejections_priced(
        facts,
        consumer,
        requires_metered(consumer),
        allowed_providers,
        subscription_plan_providers,
        None,
    )
}

fn candidate_rejections_priced(
    facts: &[ModelFact],
    consumer: Option<Consumer>,
    priced_per_token: bool,
    allowed_providers: Option<&[String]>,
    subscription_plan_providers: Option<&std::collections::HashSet<String>>,
    metered_accounts: Option<&std::collections::HashSet<String>>,
) -> HashMap<String, String> {
    facts
        .iter()
        .filter_map(|fact| {
            let detail = if allowed_providers
                .is_some_and(|allowed| !allowed.contains(&fact.provider_id))
            {
                Some(format!("allowed_providers filter denies provider {}", fact.provider_id))
            } else if priced_per_token
                && !(fact.auth_methods.as_ref().is_some_and(|methods| methods.iter().any(|method| method == "apikey"))
                    && fact.billing_plane.as_ref().is_some_and(|plane| plane.is_api_metered())
                    && metered_accounts.map_or_else(
                        || subscription_plan_providers.is_some_and(|providers| !providers.contains(&fact.provider_id)),
                        |routes| routes.contains(&fact_to_route(fact).route_key)))
            {
                Some(format!(
                    "consumer {} requires a priced-per-token account, known apikey auth methods and a metered billing plane with a known output price",
                    crate::variant_policy::consumer_group(consumer).unwrap_or("pay_per_token")
                ))
            } else {
                None
            };
            detail.map(|detail| (fact_to_route(fact).route_key, detail))
        })
        .collect()
}

pub(crate) fn describe_filter_refusal(
    mut error: striatum_core::model_router::NoRouteError,
) -> striatum_core::model_router::NoRouteError {
    use striatum_core::model_router::RouteFailureReason;
    if error.reason == RouteFailureReason::NoAvailableModel {
        let details: std::collections::BTreeSet<_> = error
            .rejected
            .iter()
            .filter_map(|route| route.detail.as_deref())
            .filter(|detail| {
                detail.starts_with("allowed_providers filter") || detail.starts_with("consumer ")
            })
            .collect();
        if !details.is_empty() {
            error.message = format!(
                "{}; no candidates remain after selection filters: {}",
                error.message,
                details.into_iter().collect::<Vec<_>>().join("; ")
            );
        }
    }
    error
}

pub(crate) fn deserialize_allowed_providers<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let ids = Vec::<String>::deserialize(deserializer)
        .map_err(|error| D::Error::custom(format!("allowed_providers: {error}")))?;
    let distinct: std::collections::HashSet<_> = ids.iter().collect();
    if ids.is_empty()
        || distinct.len() != ids.len()
        || ids.iter().any(|id| {
            id.trim().is_empty() || id.contains('/') || id.chars().any(char::is_whitespace)
        })
    {
        return Err(D::Error::custom(
            "allowed_providers must be a non-empty array of distinct non-empty provider ids",
        ));
    }
    Ok(Some(ids))
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Pin {
    Slot(PinSlotRequest),
    Model(SynthesizerPin),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinSlotRequest {
    slot: PinSlot,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PinSlot {
    Synthesizer,
    LabPlanner,
    SpecDesigner,
}

impl PinSlot {
    fn name(self) -> &'static str {
        match self {
            Self::Synthesizer => "synthesizer",
            Self::LabPlanner => "lab_planner",
            Self::SpecDesigner => "spec_designer",
        }
    }
}

impl Pin {
    /// The pin a request names, or `None` when it names a slot the operator
    /// left unset; the request is then selected like an unpinned one.
    ///
    /// A slot is read from `consumers.<slot>.pin`. The model and variant are
    /// kept as stored.
    pub(crate) fn resolve(
        &self,
        slot_pin: impl Fn(&str) -> Option<SynthesizerPin>,
    ) -> Option<SynthesizerPin> {
        match self {
            Self::Slot(PinSlotRequest { slot }) => slot_pin(slot.name()),
            Self::Model(pin) => Some(pin.clone()),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PinFallback {
    Route,
    #[default]
    None,
}

pub(crate) fn select_pin_with_availability(
    pin: &SynthesizerPin,
    mut input: SelectModelRouteInput<'_>,
    config: &RoutingConfig,
    slots: Option<&ConcurrencySlots>,
    availability_rejections: &std::collections::HashMap<String, String>,
) -> Result<SelectedModelRoute, String> {
    let (provider, model) = pin.model.split_once('/').ok_or("invalid_model_pin")?;
    if provider.is_empty()
        || model.is_empty()
        || pin.model.chars().any(char::is_whitespace)
        || pin
            .variant
            .as_ref()
            .is_some_and(|variant| variant.trim().is_empty())
    {
        return Err("invalid_model_pin".into());
    }
    let mut fact = input
        .facts
        .iter()
        .filter(|fact| fact.provider_id == provider && fact.model_id == model)
        .min_by_key(|fact| {
            if fact.variant == pin.variant {
                0
            } else if fact.variant.is_none() {
                1
            } else {
                2
            }
        })
        .cloned()
        .ok_or("unknown_model")?;
    if let Some(variant) = &pin.variant {
        fact.variant = Some(variant.clone());
        let defaults = fact.prompt_defaults.get_or_insert_with(Default::default);
        defaults.variant = Some(variant.clone());
        defaults.reasoning_effort = None;
    }
    input.facts = vec![fact];
    input.operator_exclude_keys.clear();
    let admission = striatum_core::model_router::admit_routes_with_availability(
        &input.facts,
        input.runtime_snapshot.as_ref(),
        ExclusionPolicy::caller_only(&input.exclude_route_keys),
        &[],
        input.required_capabilities,
        &input.available_models,
        &input.requirements,
        config,
        availability_rejections,
    );
    let adequacy = admission
        .admitted
        .first()
        .ok_or_else(|| pin_rejection(&admission.rejected[0].reason))?
        .adequacy
        .clone();
    // A pin chooses a model rather than ranking it by quality. The normal router
    // still checks availability and quota reserved for head sessions, but a low
    // quality score must not reject or substitute the caller's explicit choice.
    input.requirements = ModelRequirements { iq: 0.0, eq: 0.0 };
    input
        .required_capabilities
        .get_or_insert_with(Default::default)
        .min_context
        .get_or_insert(0.0);
    let mut selected = striatum_core::model_router::select_model_route_with_availability(
        input,
        config,
        slots,
        &[],
        availability_rejections,
    )
    .map_err(|error| {
        error.rejected.first().map_or_else(
            || error.reason.as_str().into(),
            |route| pin_rejection(&route.reason),
        )
    })?;
    selected.score.adequacy = adequacy.adequacy;
    selected.score.raw_adequacy = adequacy.raw_adequacy;
    selected.score.metrics = adequacy.metrics;
    selected.below_floor = None;
    selected.explanation = vec![format!("admission-only pin: {}", pin.model)];
    Ok(selected)
}

fn pin_rejection(reason: &str) -> String {
    match reason {
        "excluded" => "caller_excluded".into(),
        reason => reason.into(),
    }
}

#[cfg(test)]
mod candidate_tests {
    use super::*;
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
    use striatum_core::route_state_store::CredentialEntitlementFence;
    use striatum_core::{ModelRouteStateSnapshot, ModelRoutingPolicyInput};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::{Arc, Mutex};

    fn row(id: &str, kind: &str, method: &str) -> ListedCredential {
        ListedCredential {
            id: id.into(),
            credential_type: kind.into(),
            auth_method: Some(method.into()),
            provider_ids: vec!["example".into()],
            categories: vec!["llm-provider".into()],
            state: "active".into(),
            account_id: Some(id.into()),
            ..Default::default()
        }
    }

    fn prepare(
        rows: Vec<ListedCredential>,
        fences: &[CredentialEntitlementFence],
        policy: ModelRoutingPolicyInput,
    ) -> (Candidates, ModelRouteStateSnapshot) {
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "candidate-facts".into(),
                credentials: rows,
                ..Default::default()
            },
            1000,
        );
        let credentials = Arc::new(Mutex::new(credentials));
        let mut snapshot = ModelRouteStateSnapshot {
            now: 1000.0,
            ..Default::default()
        };
        let result = prepare_candidates(
            &[
                crate::tests::fact("example", "model"),
                crate::tests::fact("other", "model"),
            ],
            &mut snapshot,
            HashSet::from(["example/model".into()]),
            HashMap::new(),
            CandidateSource::Vault {
                credentials: &credentials,
                fences,
                stale: &[],
                defer_pool_pin: false,
            },
            false,
            Some(Consumer::Mason),
            false,
            None,
            None,
            &policy,
            &BTreeMap::new(),
            None,
        );
        (result, snapshot)
    }

    #[test]
    fn subscription_uses_the_vault_type_per_account_without_an_auth_file() {
        let (candidates, _) = prepare(
            vec![row("opaque-a", "oauth", "oauth")],
            &[],
            Default::default(),
        );
        assert!(candidates.subscription_keys.contains("example/model"));
        let (candidates, _) = prepare(
            vec![row("opaque-b", "apikey", "oauth")],
            &[],
            Default::default(),
        );
        assert!(
            candidates.subscription_keys.is_empty(),
            "auth method is not the vault credential type"
        );
        let (candidates, _) = prepare(
            vec![row("opaque-b", "apikey", "apikey")],
            &[],
            ModelRoutingPolicyInput {
                subscription: vec!["example".into()],
                ..Default::default()
            },
        );
        assert!(candidates.subscription_keys.contains("example/model"));
    }

    #[test]
    fn losing_subscription_accounts_never_authorizes_a_metered_key() {
        let fence = CredentialEntitlementFence {
            provider_id: "example".into(),
            credential_id: "login".into(),
            code: "ENTITLEMENT_ERROR".into(),
            listing_fingerprint: None,
            observed_at_ms: 999,
        };
        let (candidates, snapshot) = prepare(
            vec![
                row("login", "oauth", "oauth"),
                row("key", "apikey", "apikey"),
            ],
            &[fence],
            Default::default(),
        );
        assert!(candidates.subscription_keys.contains("example/model"));
        let mark = snapshot.by_capacity["example/model"]
            .credential_refusal
            .as_ref()
            .unwrap();
        assert_eq!(mark.reason, "credential_unentitled");
        assert_eq!(mark.credential_ids, ["login"]);
    }

    #[test]
    fn a_provider_without_an_account_set_is_not_eligible() {
        let (_, snapshot) = prepare(Vec::new(), &[], Default::default());
        assert_eq!(
            snapshot.by_capacity["example/model"]
                .credential_refusal
                .as_ref()
                .unwrap()
                .reason,
            "no_eligible_account"
        );
    }

    #[test]
    fn empty_provider_ids_resolve_only_one_catalog_vendor_from_serves() {
        let mut credential = row("chatgpt:openai", "oauth", "oauth");
        credential.provider_ids.clear();
        credential.serves = vec!["example".into()];
        let (candidates, snapshot) = prepare(vec![credential.clone()], &[], Default::default());
        assert!(candidates.subscription_keys.contains("example/model"));
        assert!(snapshot
            .by_capacity
            .get("example/model")
            .and_then(|state| state.credential_refusal.as_ref())
            .is_none());
        credential.serves.push("other".into());
        let (_, snapshot) = prepare(vec![credential.clone()], &[], Default::default());
        assert_eq!(
            snapshot.by_capacity["example/model"]
                .credential_refusal
                .as_ref()
                .unwrap()
                .reason,
            "no_eligible_account"
        );
        credential.id = "example".into();
        credential.serves = vec!["absent-vendor".into()];
        let (_, snapshot) = prepare(vec![credential], &[], Default::default());
        assert_eq!(
            snapshot.by_capacity["example/model"]
                .credential_refusal
                .as_ref()
                .unwrap()
                .reason,
            "no_eligible_account"
        );
    }
}

#[cfg(test)]
mod order_unit_tests {
    use super::{choose_runner, select_ordered, OrderBasis, OrderFailure};
    use crate::config_policy::RunnerPolicy;
    use std::collections::HashSet;

    fn runners(modules: &[&str]) -> Vec<RunnerPolicy> {
        modules
            .iter()
            .map(|module| RunnerPolicy {
                module: (*module).to_owned(),
                routes: None,
                tool_capabilities: Vec::new(),
                via_vault: false,
            })
            .collect()
    }

    #[test]
    fn order_is_request_pin_then_project_pin_then_continuing_then_mode() {
        let available = HashSet::from(["a/m".to_owned(), "b/m".to_owned(), "c/m".to_owned()]);
        let admit = |pin: &str| Ok::<_, &str>(format!("pin:{pin}"));
        let score = |set: &HashSet<String>| Ok::<_, &str>(format!("score:{}", set.len()));
        let mode = || Ok::<_, &str>("mode".to_owned());

        let (chosen, basis) = select_ordered(
            &available,
            Some("a/m"),
            Some("b/m"),
            Some("c/m"),
            admit,
            score,
            mode,
        )
        .unwrap();
        assert_eq!(
            (chosen, basis),
            ("pin:a/m".to_owned(), OrderBasis::RequestPin)
        );

        let admit = |pin: &str| Ok::<_, &str>(format!("pin:{pin}"));
        let score = |set: &HashSet<String>| Ok::<_, &str>(format!("score:{}", set.len()));
        let mode = || Ok::<_, &str>("mode".to_owned());
        let (chosen, basis) = select_ordered(
            &available,
            None,
            Some("b/m"),
            Some("c/m"),
            admit,
            score,
            mode,
        )
        .unwrap();
        assert_eq!(
            (chosen, basis),
            ("pin:b/m".to_owned(), OrderBasis::ProjectPin)
        );

        let admit = |pin: &str| Ok::<_, &str>(format!("pin:{pin}"));
        let score = |set: &HashSet<String>| Ok::<_, &str>(format!("score:{}", set.len()));
        let mode = || Ok::<_, &str>("mode".to_owned());
        let (chosen, basis) =
            select_ordered(&available, None, None, Some("c/m"), admit, score, mode).unwrap();
        assert_eq!(basis, OrderBasis::Continuing);
        assert_eq!(chosen, "score:1");

        let admit = |pin: &str| Ok::<_, &str>(format!("pin:{pin}"));
        let score = |set: &HashSet<String>| Ok::<_, &str>(format!("score:{}", set.len()));
        let mode = || Ok::<_, &str>("mode".to_owned());
        let (chosen, basis) =
            select_ordered(&available, None, None, None, admit, score, mode).unwrap();
        assert_eq!((chosen, basis), ("mode".to_owned(), OrderBasis::Mode));
    }

    #[test]
    fn a_continuing_model_that_is_no_longer_allowed_falls_through_to_mode() {
        let available = HashSet::from(["a/m".to_owned()]);
        let (chosen, basis) = select_ordered(
            &available,
            None,
            None,
            Some("gone/model"),
            |_: &str| Ok::<_, &str>("pin"),
            |_: &HashSet<String>| Err("not allowed"),
            || Ok::<_, &str>("mode"),
        )
        .unwrap();
        assert_eq!((chosen, basis), ("mode", OrderBasis::Mode));
    }

    #[test]
    fn a_request_pin_does_not_fall_through() {
        let available = HashSet::from(["a/m".to_owned()]);
        let error = select_ordered(
            &available,
            Some("a/m"),
            Some("b/m"),
            None,
            |_: &str| Err("refused"),
            |_: &HashSet<String>| Ok::<_, &str>("score"),
            || Ok::<_, &str>("mode"),
        )
        .unwrap_err();
        assert!(matches!(error, OrderFailure::Pin("refused")));
    }

    #[test]
    fn runner_choice_uses_the_consumer_then_array_order_not_module_id_order() {
        let listed = runners(&["zeta-runner", "alpha-runner"]);
        let runs = |_: &RunnerPolicy, _: &str, _: &str| true;
        assert_eq!(
            choose_runner(&listed, None, "p/m", "p/m", runs)
                .unwrap()
                .module,
            "zeta-runner"
        );
        assert_eq!(
            choose_runner(&listed, Some("alpha-runner"), "p/m", "p/m", runs)
                .unwrap()
                .module,
            "alpha-runner"
        );
        // A runner the route cannot use is skipped when the consumer named none.
        let runs = |runner: &RunnerPolicy, _: &str, _: &str| runner.module == "alpha-runner";
        assert_eq!(
            choose_runner(&listed, None, "p/m", "p/m", runs)
                .unwrap()
                .module,
            "alpha-runner"
        );
    }
}
