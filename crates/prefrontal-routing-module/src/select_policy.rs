//! Apply consumer exclusions and select pinned models without quality ranking.

use crate::config_policy::{ExcludeGroups, Pins, SynthesizerPin};
use prefrontal_routing_core::model_facts::ModelRequirements;
use prefrontal_routing_core::model_router::{SelectModelRouteInput, SelectedModelRoute};
use prefrontal_routing_core::provider_model_spec::ExclusionPolicy;
use prefrontal_routing_core::{ConcurrencySlots, RoutingConfig};
use serde::Deserialize;
use std::collections::HashMap;

use prefrontal_routing_core::model_facts::{fact_to_route, ModelFact};

pub(crate) enum CandidateSource<'a> {
    Host {
        auth_types: &'a HashMap<String, String>,
    },
    Vault {
        credentials: &'a crate::broca_credentials::SharedBrocaCredentials,
        fences: &'a [prefrontal_routing_core::route_state_store::CredentialEntitlementFence],
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
) -> Result<SelectedModelRoute, prefrontal_routing_core::model_router::NoRouteError> {
    prefrontal_routing_core::model_router::select_model_route_with_availability(
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
        -> Result<SelectedModelRoute, prefrontal_routing_core::model_router::NoRouteError>,
) -> Result<SelectedModelRoute, prefrontal_routing_core::model_router::NoRouteError> {
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
                        |prior: &prefrontal_routing_core::model_router::RejectedRoute| {
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
    snapshot: &mut prefrontal_routing_core::ModelRouteStateSnapshot,
    available: std::collections::HashSet<String>,
    mut rejected: HashMap<String, String>,
    source: CandidateSource<'_>,
    consumer: Option<Consumer>,
    allowed_providers: Option<&[String]>,
    subscription_plan_providers: Option<&std::collections::HashSet<String>>,
    policy: &prefrontal_routing_core::launch_policy::ModelRoutingPolicyInput,
    catalog_targets: &std::collections::BTreeMap<String, String>,
) -> Candidates {
    use prefrontal_routing_core::model_router::CredentialRefusalMark;
    use prefrontal_routing_core::provider_model_spec::{parse_spec_list, resolve_spec};

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
            filters = candidate_rejections(
                facts,
                consumer,
                allowed_providers,
                subscription_plan_providers,
            );
        }
        CandidateSource::Vault {
            credentials,
            fences,
            defer_pool_pin,
        } => {
            let guard = credentials.lock().ok();
            let rows = guard
                .as_ref()
                .map_or(&[][..], |guard| guard.listed_credentials());
            let catalog_providers: std::collections::HashSet<&str> = facts
                .iter()
                .filter(|fact| !catalog_targets.contains_key(&fact.provider_id))
                .map(|fact| fact.provider_id.as_str())
                .collect();
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
                            && (row.provider_ids.contains(&fact.provider_id)
                                || (row.provider_ids.is_empty() && {
                                    let vendors: std::collections::HashSet<_> = row
                                        .serves
                                        .iter()
                                        .filter(|vendor| {
                                            catalog_providers.contains(vendor.as_str())
                                        })
                                        .collect();
                                    vendors.len() == 1
                                        && vendors.iter().any(|vendor| {
                                            **vendor == fact.provider_id
                                                || catalog_targets.get(&fact.provider_id)
                                                    == Some(*vendor)
                                        })
                                }))
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
                let covered = |row: &&cortexkit_claustrum_client::ListedCredential| {
                    !catalog_targets.contains_key(&fact.provider_id)
                        && resolve_spec(&specs, &route.route_key, row.credential_type == "oauth")
                };
                // Account class is decided before fences: losing every oauth
                // account must not silently authorize a priced-per-token key.
                let subscription = serving.iter().any(covered);
                if subscription && !requires_metered(consumer) {
                    subscription_keys.insert(route.route_key.clone());
                }
                let class: Vec<_> = serving
                    .iter()
                    .copied()
                    .filter(|row| {
                        if requires_metered(consumer) {
                            !covered(row)
                        } else {
                            !subscription || covered(row)
                        }
                    })
                    .collect();
                let identities: Vec<_> = class
                    .iter()
                    .map(|row| (row.id.clone(), row.account_id.clone()))
                    .collect();
                let refused =
                    crate::credential_fences::unentitled(fences, &fact.provider_id, &identities);
                if class.is_empty()
                    || (!refused.is_empty()
                        && !(defer_pool_pin && catalog_targets.contains_key(&fact.provider_id)))
                {
                    snapshot
                        .by_capacity
                        .entry(route.capacity_key.clone())
                        .or_default()
                        .credential_refusal = Some(CredentialRefusalMark {
                        reason: if class.is_empty() {
                            "no_eligible_account"
                        } else {
                            crate::credential_fences::rejection_reason(
                                fences,
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
            let admission_facts = guard.as_ref().map_or_else(
                || facts.to_vec(),
                |guard| guard.admission_facts(facts, consumer, snapshot.now as u64),
            );
            filters = candidate_rejections(
                &admission_facts,
                consumer,
                allowed_providers,
                subscription_plan_providers,
            );
        }
    }
    rejected.extend(filters.clone());
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

pub(crate) fn consumer_exclusions(groups: &ExcludeGroups, consumer: Option<Consumer>) -> &[String] {
    match consumer {
        Some(Consumer::Mason) => &groups.mason,
        Some(Consumer::Sidekick) => &groups.sidekick,
        Some(Consumer::Athena) => &groups.athena,
        Some(Consumer::Lab) => &groups.lab,
        Some(Consumer::Core) => &groups.core,
        Some(Consumer::Codemode) => &groups.codemode,
        Some(Consumer::Flow) => &groups.flow,
        None => &[],
    }
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

    /// The request class a consumer id names, if it names one.
    pub(crate) fn from_id(id: &str) -> Option<Consumer> {
        Self::ALL
            .into_iter()
            .find(|consumer| crate::variant_policy::consumer_group(Some(*consumer)) == Some(id))
    }
}

pub(crate) fn requires_metered(consumer: Option<Consumer>) -> bool {
    matches!(consumer, Some(Consumer::Codemode | Consumer::Flow))
}

/// Whether selection prices `route` as paid from subscription quota: it is
/// on the subscription list, and the consumer is not one that must run
/// priced per token ([`requires_metered`]).
pub(crate) fn subscription_covered(
    consumer: Option<Consumer>,
    subscription_keys: &std::collections::HashSet<String>,
    route: &prefrontal_routing_core::ModelRoute,
) -> bool {
    !requires_metered(consumer)
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
    facts
        .iter()
        .filter_map(|fact| {
            let detail = if allowed_providers
                .is_some_and(|allowed| !allowed.contains(&fact.provider_id))
            {
                Some(format!("allowed_providers filter denies provider {}", fact.provider_id))
            } else if requires_metered(consumer)
                && !(fact.auth_methods.as_ref().is_some_and(|methods| methods.iter().any(|method| method == "apikey"))
                    && fact.billing_plane.as_ref().is_some_and(|plane| plane.is_api_metered())
                    && subscription_plan_providers.is_some_and(|providers| !providers.contains(&fact.provider_id)))
            {
                Some(format!(
                    "consumer {} requires known apikey auth methods, a metered billing plane with a known output price, and known absence of a subscription plan",
                    crate::variant_policy::consumer_group(consumer).unwrap()
                ))
            } else {
                None
            };
            detail.map(|detail| (fact_to_route(fact).route_key, detail))
        })
        .collect()
}

pub(crate) fn describe_filter_refusal(
    mut error: prefrontal_routing_core::model_router::NoRouteError,
) -> prefrontal_routing_core::model_router::NoRouteError {
    use prefrontal_routing_core::model_router::RouteFailureReason;
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PinSlot {
    LabPlanner,
    SpecDesigner,
}

impl Pin {
    /// The pin a request names, or `None` when it names a slot the operator
    /// left unset; the request is then selected like an unpinned one.
    pub(crate) fn resolve(&self, pins: &Pins) -> Option<SynthesizerPin> {
        match self {
            Self::Slot(PinSlotRequest {
                slot: PinSlot::LabPlanner,
            }) => pins.lab_planner.as_ref().map(|pin| SynthesizerPin {
                model: pin.model.clone(),
                variant: None,
            }),
            // Only the designer's own pin: a configured synthesizer pin is
            // Athena's choice and never routes the designer.
            Self::Slot(PinSlotRequest {
                slot: PinSlot::SpecDesigner,
            }) => pins.spec_designer.clone(),
            Self::Model(pin) => Some(pin.clone()),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PinFallback {
    #[default]
    Route,
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
    let admission = prefrontal_routing_core::model_router::admit_routes_with_availability(
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
    let mut selected = prefrontal_routing_core::model_router::select_model_route_with_availability(
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
    use prefrontal_routing_core::route_state_store::CredentialEntitlementFence;
    use prefrontal_routing_core::{ModelRouteStateSnapshot, ModelRoutingPolicyInput};
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
                defer_pool_pin: false,
            },
            Some(Consumer::Mason),
            None,
            None,
            &policy,
            &BTreeMap::new(),
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
