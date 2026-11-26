use super::*;
use crate::config_policy::{
    consumer_matches, is_slot, resolve_consumer_policy, slot_consumer, ConsumerPolicy, EntityPolicy,
};

#[derive(Clone, Copy)]
enum PinOnlyScope {
    ProviderAndModel,
    Model,
}

/// Exact slash-delimited model prefixes, owned by one validated settings snapshot.
#[derive(Debug)]
pub(super) struct EntityPolicyIndex(BTreeMap<String, EntityPolicy>);

impl EntityPolicyIndex {
    pub(super) fn compile(config: &RoutingConfigFile) -> Self {
        Self(
            config
                .model_routing
                .models
                .iter()
                .map(|(key, model)| (key.clone(), model.rules.clone()))
                .collect(),
        )
    }

    fn matching<'a>(&'a self, route: &'a str) -> impl Iterator<Item = (&'a str, &'a EntityPolicy)> {
        // Every matching key is either the whole route or ends at a slash.
        // Visit shortest first so each explicitly set field replaces its parent.
        route
            .match_indices('/')
            .map(|(end, _)| &route[..end])
            .chain(std::iter::once(route))
            .filter_map(|key| {
                self.0
                    .get_key_value(key)
                    .map(|(key, rules)| (key.as_str(), rules))
            })
    }
}

#[derive(Default)]
pub(crate) struct SelectionPolicyMemo(std::sync::Mutex<Vec<ExclusionMemoEntry>>);

struct ExclusionMemoEntry {
    settings: Arc<CompiledPolicy>,
    consumer: String,
    pinned: bool,
    // Only entity-policy inputs belong here, never account or availability data.
    routes: Vec<(String, bool)>,
    exclusions: Vec<String>,
}

impl SelectionPolicyMemo {
    pub(crate) fn exclusions(
        &self,
        config: &LiveConfig,
        consumer: &str,
        facts: &[ModelFact],
        pinned: bool,
    ) -> Vec<String> {
        let routes: Vec<_> = facts
            .iter()
            .map(|fact| {
                (
                    striatum_core::fact_to_route(fact).route_key,
                    default_pin_only(fact.billing_plane.as_ref()),
                )
            })
            .collect();
        let mut entries = self.0.lock().unwrap();
        if let Some(entry) = entries.iter().find(|entry| {
            Arc::ptr_eq(&entry.settings, &config.compiled)
                && entry.consumer == consumer
                && entry.pinned == pinned
                && entry.routes == routes
        }) {
            return entry.exclusions.clone();
        }
        let exclusions = config.selection_exclusions(consumer, facts, pinned);
        entries.push(ExclusionMemoEntry {
            settings: config.compiled.clone(),
            consumer: consumer.into(),
            pinned,
            routes,
            exclusions: exclusions.clone(),
        });
        exclusions
    }
}

fn default_pin_only(plane: Option<&striatum_core::billing::BillingPlane>) -> bool {
    plane.is_some_and(|plane| {
        striatum_core::billing::is_api_metered_auth_method(&plane.auth_method)
            && plane.kind != "quota_proxy"
    })
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[tokio::test]
async fn panel_policy_memo_matches_uncached_verdicts() {
    use crate::routing_v1::request_cpu_bench::{production_fixture, FIXTURE_NOW};
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = production_fixture();
    let original = fixture.operator_config();
    let mut facts = fixture.store().get_facts().unwrap();
    assert_eq!(facts.len(), 800);
    let mut consumers: Vec<_> = crate::select_policy::Consumer::ALL
        .iter()
        .map(|consumer| consumer.exclusion_group())
        .map(str::to_owned)
        .collect();
    consumers.extend(original.config.consumers.keys().cloned());
    consumers.extend(crate::routing_v1::consumers::effective(&original).names());
    consumers.extend(
        [
            "mc.dream",
            "mc.dream.curate",
            "mcx",
            "mason.review",
            "unknown.child",
        ]
        .map(str::to_owned),
    );
    consumers.sort();
    consumers.dedup();
    let memo = SelectionPolicyMemo::default();
    let check = |config: &LiveConfig, facts: &[ModelFact]| {
        for consumer in &consumers {
            for pinned in [false, true] {
                let expected: Vec<_> = facts
                    .iter()
                    .filter_map(|fact| {
                        let route = striatum_core::fact_to_route(fact).route_key;
                        config
                            .config
                            .uncached_refusal(
                                consumer,
                                &route,
                                pinned,
                                default_pin_only(fact.billing_plane.as_ref()),
                                PinOnlyScope::ProviderAndModel,
                            )
                            .is_some()
                            .then_some(route)
                    })
                    .collect();
                assert_eq!(
                    memo.exclusions(config, consumer, facts, pinned),
                    expected,
                    "{consumer} pinned={pinned}"
                );
                let entries = memo.0.lock().unwrap().len();
                assert_eq!(memo.exclusions(config, consumer, facts, pinned), expected);
                assert_eq!(
                    memo.0.lock().unwrap().len(),
                    entries,
                    "identical inputs must hit the memo"
                );
                for fact in facts {
                    let route = striatum_core::fact_to_route(fact).route_key;
                    for metered in [false, true] {
                        for scope in [PinOnlyScope::ProviderAndModel, PinOnlyScope::Model] {
                            assert_eq!(
                                config.entity_refusal(consumer, &route, pinned, metered, scope),
                                config
                                    .config
                                    .uncached_refusal(consumer, &route, pinned, metered, scope),
                                "{consumer} {route} pinned={pinned} metered={metered}"
                            );
                        }
                    }
                }
            }
        }
    };
    check(&original, &facts);
    fixture.edit_config(|config| {
        config["providers"]["cpu-provider-0"] = serde_json::json!({
            "only_for":["mc", "mason"], "not_for":["mc"], "pin_only":true,
            "models":["model-0", "model-1"]
        });
        config["models"]["cpu-provider-0/model-0"] = serde_json::json!({
            "only_for":["mc.dream"], "not_for":[], "pin_only":false,
            "subscription":false, "pin_only_for":["mc.dream"]
        });
        config["models"]["cpu-provider-0/model-0/nested"] = serde_json::json!({
            "only_for":["mason"], "not_for":["mason.review"], "pin_only_for":[], "subscription":true
        });
    });
    let reloaded = fixture.operator_config();
    assert!(!Arc::ptr_eq(&original.compiled, &reloaded.compiled));
    // Reuse one memo across snapshots, routes and billing-plane changes to
    // prove that none of those inputs can inherit an earlier exclusion vector.
    check(&reloaded, &facts);
    let mut nested = facts[0].clone();
    nested.provider_id = "cpu-provider-0".into();
    nested.model_id = "model-0/nested/deep".into();
    facts = vec![nested];
    check(&reloaded, &facts);
    for (auth_method, kind) in [
        ("apikey", "api"),
        ("apikey", "quota_proxy"),
        ("oauth", "subscription"),
    ] {
        facts[0].billing_plane = Some(striatum_core::billing::BillingPlane {
            provider_id: "cpu-provider-0".into(),
            auth_method: auth_method.into(),
            kind: kind.into(),
            selection_reason: "policy differential".into(),
            rates: BTreeMap::new(),
        });
        check(&reloaded, &facts);
    }
    // A route without an explicit subscription or pin-only mark must follow
    // its billing plane, even when the same request memo saw another plane.
    facts[0].provider_id = "cpu-provider-9".into();
    facts[0].model_id = "unconfigured".into();
    for (auth_method, kind) in [
        ("apikey", "api"),
        ("apikey", "quota_proxy"),
        ("oauth", "subscription"),
    ] {
        facts[0].billing_plane.as_mut().unwrap().auth_method = auth_method.into();
        facts[0].billing_plane.as_mut().unwrap().kind = kind.into();
        check(&reloaded, &facts);
    }
    facts[0].provider_id = "cpu-provider-0".into();
    facts[0].model_id = "model-0/nested/deep".into();
    let patched = reloaded
        .with_model_policy_patch(&BTreeMap::from([(
            "cpu-provider-0/model-0/nested".into(),
            super::ModelPolicyPatch {
                not_for: Some(vec!["mc".into()]),
                ..Default::default()
            },
        )]))
        .unwrap();
    check(&patched, &facts);
    check(&original, &facts);
}

impl RoutingConfigFile {
    /// Derive the pin-only list, the consumer exclusion groups and the
    /// subscription list from the provider and model rules. A policy patch
    /// that edits those rules must run this again, or the derived lists keep
    /// the unpatched rules.
    pub(super) fn derive_entity_lists(&mut self) -> Result<(), String> {
        let mut exclusion_groups = serde_json::Map::new();
        let mut global = Vec::new();
        let mut subscription = Vec::new();
        let entities: Vec<(&str, &EntityPolicy)> = self
            .providers
            .iter()
            .map(|(key, provider)| (key.as_str(), &provider.rules))
            .chain(
                self.model_routing
                    .models
                    .iter()
                    .map(|(key, model)| (key.as_str(), &model.rules)),
            )
            .collect();
        for (key, rules) in &entities {
            for name in rules.only_for.iter().chain(rules.not_for.iter()).flatten() {
                if name.split('.').any(str::is_empty) || name.chars().any(char::is_whitespace) {
                    return Err(format!("{key}: invalid consumer pattern {name:?}"));
                }
            }
            if let Some(value) = rules.pin_only {
                global.push(if value {
                    key.to_string()
                } else {
                    format!("-{key}")
                });
            }
            if let Some(value) = rules.subscription {
                subscription.push(if value {
                    key.to_string()
                } else {
                    format!("-{key}")
                });
            }
        }
        global.sort_by_key(|key| (key.trim_start_matches('-').split('/').count(), key.clone()));
        subscription
            .sort_by_key(|key| (key.trim_start_matches('-').split('/').count(), key.clone()));
        exclusion_groups.insert("all".into(), serde_json::json!(global));
        for consumer in crate::select_policy::Consumer::ALL {
            let id = consumer.exclusion_group();
            let exclusions = self.consumer_exclusions(consumer.id());
            exclusion_groups.insert(id.into(), serde_json::json!(exclusions));
        }
        self.model_routing.exclude =
            serde_json::from_value(serde_json::Value::Object(exclusion_groups))
                .map_err(|error| error.to_string())?;
        self.model_routing.subscription = subscription;
        Ok(())
    }

    /// Materialize the temporary interfaces used by the existing selection pass.
    /// Only the entity-shaped fields are read from disk; the grouped views are
    /// rebuilt atomically with each validated snapshot.
    pub(super) fn prepare(&mut self) -> Result<(), String> {
        self.derive_entity_lists()?;
        let mut pins = serde_json::Map::new();
        for (name, policy) in &self.consumers {
            if name.is_empty() || name.chars().any(char::is_whitespace) {
                return Err(format!("consumer id {name:?} must be one non-empty word"));
            }
            for pin in policy.ladder.iter().flatten() {
                if cortexkit_role_routing::select::RouteRef::parse_pin(pin).is_none() {
                    return Err(format!("consumers.{name}.ladder: invalid pin {pin:?}"));
                }
            }
            for value in [&policy.runner, &policy.owner].into_iter().flatten() {
                if value.is_empty() || value.chars().any(char::is_whitespace) {
                    return Err(format!(
                        "consumers.{name}: runner and owner must be one non-empty word"
                    ));
                }
            }
            // Scoring refuses demand outside 0..=100, so a value that loaded
            // would fail every automatic request of this consumer.
            for (field, value) in [("iq", policy.iq), ("eq", policy.eq)] {
                if value.is_some_and(|value| !(0.0..=100.0).contains(&value)) {
                    return Err(format!(
                        "consumers.{name}.{field} must be a number from 0 to 100"
                    ));
                }
            }
            if let Some(pin) = &policy.pin {
                Pins {
                    synthesizer: Some(pin.clone()),
                    ..Pins::default()
                }
                .validate()
                .map_err(|error| {
                    error.replace(
                        "model_routing.pins.synthesizer",
                        &format!("consumers.{name}.pin"),
                    )
                })?;
            }
            if is_slot(name) {
                let mut rest = serde_json::to_value(policy).unwrap();
                rest.as_object_mut().unwrap().remove("pin");
                rest.as_object_mut().unwrap().remove("service_tier");
                rest.as_object_mut().unwrap().remove("service_tiers");
                if rest
                    .as_object()
                    .unwrap()
                    .values()
                    .any(|value| !value.is_null() && value != &serde_json::json!({}))
                {
                    return Err(format!("consumers.{name} is a pinned slot and accepts only pin, service_tier and service_tiers"));
                }
                if let Some(pin) = &policy.pin {
                    let mut value = serde_json::to_value(pin).unwrap();
                    if name == "lab_planner" {
                        value.as_object_mut().unwrap().remove("variant");
                    }
                    pins.insert(name.clone(), value);
                }
            }
            for (project, policy) in &policy.projects {
                self.model_routing
                    .project_pins
                    .entry(project.clone())
                    .or_default()
                    .insert(name.clone(), policy.pin.clone());
            }
        }
        self.model_routing.pins = serde_json::from_value(serde_json::Value::Object(pins))
            .map_err(|error| error.to_string())?;
        self.model_routing.pins.synthesizer = crate::config_policy::consumer_entry(
            &self.consumers,
            crate::config_policy::ATHENA_SYNTHESIZER_CONSUMER,
        )
        .and_then(|policy| policy.pin.clone());
        self.model_routing.consumers = Some(
            self.consumers
                .keys()
                .filter(|id| !is_slot(id) || id.as_str() == "synthesizer")
                .cloned()
                .collect(),
        );
        self.model_routing.runners = Some(self.runners.clone());
        for (id, provider) in &self.providers {
            if id.is_empty() || id.chars().any(char::is_whitespace) || id.contains('/') {
                return Err(format!("invalid provider id {id:?}"));
            }
            if provider.prompt_cache == Some(false) {
                self.model_routing
                    .providers_without_prompt_cache
                    .push(id.clone());
            }
            if let Some(value) = provider.subscription_multiplier {
                self.model_routing
                    .subscription_multipliers
                    .insert(id.clone(), value);
            }
            if let Some(value) = provider.keep_free_percent {
                self.model_routing
                    .keep_free_percent
                    .insert(id.clone(), value);
            }
        }
        Ok(())
    }
}

impl LiveConfig {
    /// Clone settings for one evaluation. A pricing experiment never publishes
    /// the clone into the live cache or writes the operator's settings file.
    pub(crate) fn with_pricing_patch(&self, patch: &super::PricingPatch) -> Result<Self, String> {
        let mut patched = self.clone();
        if let Some(mode) = patch.overqualification {
            let section = &mut patched.config.model_routing.overqualification;
            section.mode = mode;
            // A mode-only preview chooses the fixed cost-scaled curve without
            // reinterpreting any legacy additive tuning in the live settings.
            if mode == super::OverqualificationMode::CostScaled {
                section.threshold = None;
                section.doubling = None;
                section.base = None;
            }
            section.resolve()?;
        }
        for (id, mode) in patch.banked_resets.iter().flatten() {
            if id.is_empty() || id.chars().any(char::is_whitespace) || id.contains('/') {
                return Err(format!("invalid provider id {id:?}"));
            }
            patched
                .config
                .providers
                .entry(id.clone())
                .or_default()
                .banked_resets = *mode;
        }
        Ok(Self::new(patched.config, patched.document))
    }

    pub(crate) fn with_model_policy_patch(
        &self,
        models: &std::collections::BTreeMap<String, super::ModelPolicyPatch>,
    ) -> Result<Self, String> {
        let mut patched = self.clone();
        for (route, patch) in models {
            if super::parse_model_key(route, None).is_none() {
                return Err(format!("invalid provider/model key {route:?}"));
            }
            let rules = &mut patched
                .config
                .model_routing
                .models
                .entry(route.clone())
                .or_default()
                .rules;
            if let Some(value) = &patch.pin_only_for {
                rules.pin_only_for = Some(value.clone());
            }
            if let Some(value) = patch.pin_only {
                rules.pin_only = Some(value);
            }
            if let Some(value) = &patch.not_for {
                rules.not_for = Some(value.clone());
            }
            if let Some(value) = &patch.only_for {
                rules.only_for = Some(value.clone());
            }
        }
        patched.config.derive_entity_lists()?;
        Ok(Self::new(patched.config, patched.document))
    }

    pub(crate) fn route_policy_refusal(&self, consumer: &str, route: &str) -> Option<String> {
        self.route_policy_refusal_with_pin(consumer, route, false)
    }

    pub(crate) fn route_policy_refusal_with_pin(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
    ) -> Option<String> {
        self.entity_refusal(
            consumer,
            route,
            pinned,
            false,
            PinOnlyScope::ProviderAndModel,
        )
        .map(|(detail, _)| detail)
    }

    /// Resolve entity restrictions from this snapshot for the full request name.
    /// Unknown roots have no matching consumer group, just as undotted unknown names do.
    pub fn consumer_exclusions(&self, consumer: &str) -> Vec<String> {
        if crate::select_policy::Consumer::from_id(consumer).is_none() {
            return Vec::new();
        }
        self.config.consumer_exclusions(consumer)
    }

    pub fn dotted_rule_refusal(&self, consumer: &str, route: &str) -> Option<String> {
        self.entity_refusal(
            consumer,
            route,
            false,
            false,
            PinOnlyScope::ProviderAndModel,
        )
        .and_then(|(detail, dotted)| dotted.then_some(detail))
    }

    /// Contract slot names look up an operator-configured pin, not a caller's
    /// consumer defaults. The older slot-pin reads still use `slot_pin` and
    /// `config_policy::slot_consumer`'s reserved slot list.
    pub(crate) fn select_slot_pin(&self, slot: &str) -> Option<String> {
        let pin = crate::config_policy::consumer_entry(&self.config.consumers, slot)?
            .pin
            .as_ref()?;
        Some(pin.variant.as_ref().map_or_else(
            || pin.model.clone(),
            |variant| format!("{}:{variant}", pin.model),
        ))
    }

    pub fn slot_pin(&self, slot: &str) -> Option<&crate::config_policy::SynthesizerPin> {
        crate::config_policy::consumer_entry(&self.config.consumers, slot)?
            .pin
            .as_ref()
    }
    pub fn consumer_policy(
        &self,
        consumer: &str,
        request: &ConsumerPolicy,
        declared: &BTreeMap<String, ConsumerPolicy>,
    ) -> ConsumerPolicy {
        resolve_consumer_policy(consumer, &self.config.consumers, request, declared)
    }

    pub(crate) fn consumer_workload(&self, consumer: &str) -> Option<&str> {
        self.config
            .consumers
            .iter()
            .filter(|(name, _)| crate::config_policy::consumer_matches(name, consumer))
            .filter_map(|(name, policy)| Some((name, policy.workload.as_deref()?)))
            .max_by_key(|(name, _)| crate::config_policy::consumer_specificity(name))
            .map(|(_, workload)| workload)
    }

    /// Explicit pins bypass `pin_only` and `pin_only_for`, but never a
    /// provider/model `only_for` or `not_for` restriction.
    pub fn route_allowed(&self, consumer: &str, route: &str, pinned: bool) -> bool {
        self.entity_refusal(
            consumer,
            route,
            pinned,
            false,
            PinOnlyScope::ProviderAndModel,
        )
        .is_none()
    }

    /// Media engines have no subscription alternative, so chat provider pin-only
    /// rules must not disable automatic media choice. Model pin-only rules still
    /// bind; the built-in rule requiring metered chat routes to be pinned does not.
    pub(crate) fn media_route_allowed(&self, consumer: &str, route: &str, pinned: bool) -> bool {
        self.entity_refusal(consumer, route, pinned, false, PinOnlyScope::Model)
            .is_none()
    }

    /// Metered billing needs an explicit pin unless the operator opts the
    /// provider or route into automatic selection. Quota-backed API plans are
    /// subscriptions; an unknown API price is not permission to spend.
    pub(crate) fn route_allowed_on_plane(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
        plane: Option<&striatum_core::billing::BillingPlane>,
    ) -> bool {
        let metered = default_pin_only(plane);
        self.route_allowed_with_default(consumer, route, pinned, metered)
    }

    pub(crate) fn route_allowed_with_default(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
        metered: bool,
    ) -> bool {
        self.entity_refusal(
            consumer,
            route,
            pinned,
            metered,
            PinOnlyScope::ProviderAndModel,
        )
        .is_none()
    }

    fn entity_refusal(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
        default_pin_only: bool,
        scope: PinOnlyScope,
    ) -> Option<(String, bool)> {
        self.config.refusal_with_models(
            consumer,
            route,
            pinned,
            default_pin_only,
            scope,
            self.compiled.entities.matching(route),
        )
    }

    pub(crate) fn selection_exclusions(
        &self,
        consumer: &str,
        facts: &[ModelFact],
        pinned: bool,
    ) -> Vec<String> {
        facts
            .iter()
            .filter_map(|fact| {
                let route = striatum_core::fact_to_route(fact).route_key;
                (!self.route_allowed_on_plane(
                    consumer,
                    &route,
                    pinned,
                    fact.billing_plane.as_ref(),
                ))
                .then_some(route)
            })
            .collect()
    }
}

impl RoutingConfigFile {
    fn consumer_exclusions(&self, consumer: &str) -> Vec<String> {
        let rules_at = |key: &str| {
            if key.contains('/') {
                self.model_routing.models.get(key).map(|model| &model.rules)
            } else {
                self.providers.get(key).map(|provider| &provider.rules)
            }
        };
        // Resolve `only_for` and `not_for` field by field, provider first and
        // then every configured model prefix shortest first, the same
        // inheritance the per-route refusal uses. `inclusive` decides whether
        // the key's own entry takes part, so the parent state excludes it.
        let excluded = |key: &str, inclusive: bool| {
            let (mut only_for, mut not_for) = (None, None);
            let ends = key
                .match_indices('/')
                .map(|(end, _)| end)
                .chain(inclusive.then_some(key.len()));
            for rules in ends.filter_map(|end| rules_at(&key[..end])) {
                only_for = rules.only_for.as_ref().or(only_for);
                not_for = rules.not_for.as_ref().or(not_for);
            }
            only_for.is_some_and(|names: &Vec<String>| {
                !names.iter().any(|name| consumer_matches(name, consumer))
            }) || not_for.is_some_and(|names: &Vec<String>| {
                names.iter().any(|name| consumer_matches(name, consumer))
            })
        };
        let entities = self
            .providers
            .iter()
            .map(|(key, provider)| (key, &provider.rules))
            .chain(
                self.model_routing
                    .models
                    .iter()
                    .map(|(key, model)| (key, &model.rules)),
            );
        let mut exclusions = Vec::new();
        for (key, rules) in entities {
            if rules.only_for.is_some() || rules.not_for.is_some() {
                if excluded(key, true) {
                    exclusions.push(key.clone());
                } else if excluded(key, false) {
                    exclusions.push(format!("-{key}"));
                }
            }
        }
        exclusions.sort_by_key(|key| (key.trim_start_matches('-').split('/').count(), key.clone()));
        exclusions
    }

    /// Why this config refuses `route` for `consumer`, naming the config rule
    /// that refuses it, or `None` when the route is allowed. Explicit pins
    /// bypass `pin_only` and `pin_only_for`, but never a provider/model
    /// `only_for` or `not_for` restriction, nor a provider's `models` list. The most specific
    /// `models` entry that sets a rule replaces the provider's rule.
    pub(super) fn route_refusal(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
    ) -> Option<String> {
        self.route_refusal_detail(consumer, route, pinned)
            .map(|(detail, _)| detail)
    }

    /// The flag marks a refusal caused by a dotted consumer pattern. Root-only
    /// rules retain the existing rejection shape without an added detail.
    fn route_refusal_detail(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
    ) -> Option<(String, bool)> {
        self.route_refusal_detail_with_default(consumer, route, pinned, false)
    }

    fn route_refusal_detail_with_default(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
        default_pin_only: bool,
    ) -> Option<(String, bool)> {
        self.route_refusal_detail_with_pin_scope(
            consumer,
            route,
            pinned,
            default_pin_only,
            PinOnlyScope::ProviderAndModel,
        )
    }

    fn route_refusal_detail_with_pin_scope(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
        default_pin_only: bool,
        pin_only_scope: PinOnlyScope,
    ) -> Option<(String, bool)> {
        let index = EntityPolicyIndex::compile(self);
        self.refusal_with_models(
            consumer,
            route,
            pinned,
            default_pin_only,
            pin_only_scope,
            index.matching(route),
        )
    }

    #[cfg(test)]
    fn uncached_refusal(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
        default_pin_only: bool,
        pin_only_scope: PinOnlyScope,
    ) -> Option<(String, bool)> {
        // Preserve the original full-map scan as an independent matching oracle.
        let mut models: Vec<_> = self
            .model_routing
            .models
            .iter()
            .filter(|(key, _)| route == key.as_str() || route.starts_with(&format!("{key}/")))
            .collect();
        models.sort_by_key(|(key, _)| key.len());
        self.refusal_with_models(
            consumer,
            route,
            pinned,
            default_pin_only,
            pin_only_scope,
            models
                .into_iter()
                .map(|(key, model)| (key.as_str(), &model.rules)),
        )
    }

    fn refusal_with_models<'a>(
        &'a self,
        consumer: &str,
        route: &str,
        pinned: bool,
        default_pin_only: bool,
        pin_only_scope: PinOnlyScope,
        models: impl Iterator<Item = (&'a str, &'a EntityPolicy)>,
    ) -> Option<(String, bool)> {
        let provider = route.split('/').next().unwrap_or(route);
        let provider_rules = self.providers.get(provider).map(|policy| &policy.rules);
        let provider_pin_rules =
            provider_rules.filter(|_| matches!(pin_only_scope, PinOnlyScope::ProviderAndModel));
        let provider_source = |field: &str| format!("providers.{provider}.{field}");
        let mut only_for = provider_rules
            .and_then(|rules| rules.only_for.as_ref())
            .map(|names| (provider_source("only_for"), names));
        let mut not_for = provider_rules
            .and_then(|rules| rules.not_for.as_ref())
            .map(|names| (provider_source("not_for"), names));
        let mut pin_only = provider_pin_rules
            .and_then(|rules| rules.pin_only)
            .map(|value| (provider_source("pin_only"), value));
        let mut subscription = provider_rules.and_then(|rules| rules.subscription);
        let mut pin_only_for = provider_pin_rules
            .and_then(|rules| rules.pin_only_for.as_ref())
            .map(|names| (provider_source("pin_only_for"), names));
        for (key, rules) in models {
            let model_source = |field: &str| format!("models[{key:?}].{field}");
            if let Some(names) = &rules.only_for {
                only_for = Some((model_source("only_for"), names));
            }
            if let Some(names) = &rules.not_for {
                not_for = Some((model_source("not_for"), names));
            }
            if let Some(value) = rules.pin_only {
                pin_only = Some((model_source("pin_only"), value));
            }
            if let Some(value) = rules.subscription {
                subscription = Some(value);
            }
            if let Some(names) = &rules.pin_only_for {
                pin_only_for = Some((model_source("pin_only_for"), names));
            }
        }
        // Flat-rate plans can use API-key authentication. An explicit
        // subscription mark overrides the billing-derived default, but never
        // an operator's separate pin-only rule.
        if pin_only.is_none() && default_pin_only && subscription != Some(true) {
            pin_only = Some(("built-in metered pin_only".into(), true));
        }
        if let Some((source, names)) = only_for {
            if !names.iter().any(|name| consumer_matches(name, consumer)) {
                return Some((
                    format!("{source} {names:?} does not name {consumer:?}"),
                    names.iter().any(|name| name.contains('.')),
                ));
            }
        }
        if let Some((source, names)) = not_for {
            if let Some(name) = names.iter().find(|name| consumer_matches(name, consumer)) {
                return Some((format!("{source} lists {name:?}"), name.contains('.')));
            }
        }
        if let Some((source, true)) = pin_only {
            if !pinned {
                return Some((format!("{source} admits the route only when pinned"), false));
            }
        }
        if let Some((source, names)) = pin_only_for {
            if !pinned {
                if let Some(name) = names.iter().find(|name| consumer_matches(name, consumer)) {
                    return Some((
                        format!("{source} lists {name:?}, so it admits the route only when pinned"),
                        name.contains('.'),
                    ));
                }
            }
        }
        let listed = self.providers.get(provider)?.models.as_ref()?;
        let served = listed.iter().any(|model| {
            route
                .strip_prefix(&format!("{provider}/"))
                .is_some_and(|tail| tail == model || tail.starts_with(&format!("{model}/")))
        });
        (!served).then(|| {
            (
                format!("providers.{provider}.models {listed:?} does not list the model"),
                false,
            )
        })
    }

    /// Refuse a config that pins a consumer to a route its own rules refuse
    /// for that consumer. Routing applies a model or provider `only_for` /
    /// `not_for` and a provider's `models` list even to explicit pins, so such
    /// a pin could never be served: every request through it would fail at
    /// runtime with "pinned route unavailable" while the config loaded
    /// cleanly. Checked: each `consumers.<name>.pin` (a slot pin under the
    /// consumer its callers request it with, see [`slot_consumer`]) and each
    /// `consumers.<name>.projects.<project>.pin`.
    pub(super) fn validate_consumer_pins(&self) -> Result<(), String> {
        let refuse = |path: &str, consumer: &str, routes: &[String]| -> Result<(), String> {
            for route in routes {
                if let Some(rule) = self.route_refusal(consumer, route, true) {
                    return Err(format!(
                        "{path} pins {} but this config refuses route {route} for consumer {consumer:?}: {rule}",
                        routes[0]
                    ));
                }
            }
            Ok(())
        };
        for (name, policy) in &self.consumers {
            let consumer = slot_consumer(name).unwrap_or(name);
            if let Some(pin) = &policy.pin {
                let mut routes = vec![pin.model.clone()];
                if let (Some((provider, model)), Some(variant)) =
                    (pin.model.split_once('/'), pin.variant.as_deref())
                {
                    routes.push(make_route_key(provider, model, Some(variant)));
                }
                refuse(&format!("consumers.{name}.pin"), consumer, &routes)?;
            }
            for (project, project_policy) in &policy.projects {
                let Some(route) =
                    cortexkit_role_routing::select::RouteRef::parse_pin(&project_policy.pin)
                else {
                    continue;
                };
                let mut routes = vec![route.route_key()];
                if let Some(variant) = route.variant.as_deref() {
                    routes.push(make_route_key(&route.provider, &route.model, Some(variant)));
                }
                refuse(
                    &format!("consumers.{name}.projects.{project}.pin"),
                    name,
                    &routes,
                )?;
            }
        }
        Ok(())
    }
}
