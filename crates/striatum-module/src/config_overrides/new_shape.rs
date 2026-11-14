use super::*;
use crate::config_policy::{
    consumer_matches, is_slot, resolve_consumer_policy, slot_consumer, ConsumerPolicy, EntityPolicy,
};

#[derive(Clone, Copy)]
enum PinOnlyScope {
    ProviderAndModel,
    Model,
}

impl RoutingConfigFile {
    /// Materialize the temporary interfaces used by the existing selection pass.
    /// Only the entity-shaped fields are read from disk; the grouped views are
    /// rebuilt atomically with each validated snapshot.
    pub(super) fn prepare(&mut self) -> Result<(), String> {
        // Two spellings of one policy cannot be ordered without silently
        // discarding an operator's intent, even when their values happen to agree.
        let mut exclusion_groups = serde_json::Map::new();
        let mut variants = serde_json::Map::new();
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
        let names: Vec<&str> = self
            .consumers
            .keys()
            .map(String::as_str)
            .chain(entities.iter().flat_map(|(_, rules)| {
                rules
                    .only_for
                    .iter()
                    .chain(rules.not_for.iter())
                    .chain(rules.pin_only_for.iter())
                    .flatten()
                    .map(String::as_str)
            }))
            .collect();
        for (old, new) in crate::config_policy::CONSUMER_ALIAS_PAIRS {
            for name in &names {
                if let Some(tail) = name.strip_prefix(old) {
                    let alias = format!("{new}{tail}");
                    if (tail.is_empty() || tail.starts_with('.')) && names.contains(&alias.as_str())
                    {
                        return Err(format!(
                            "consumer alias pair {name:?} and {alias:?} cannot both be configured"
                        ));
                    }
                }
            }
        }
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
            let Some(id) = crate::variant_policy::consumer_group(Some(consumer)) else {
                continue;
            };
            let exclusions = self.consumer_exclusions(id);
            exclusion_groups.insert(id.into(), serde_json::json!(exclusions));
        }
        for id in self.consumers.keys().filter(|id| !is_slot(id)) {
            let policy = resolve_consumer_policy(
                id,
                &self.consumers,
                &ConsumerPolicy::default(),
                &BTreeMap::new(),
            );
            variants.insert(
                id.clone(),
                serde_json::json!(policy.variants.unwrap_or_default()),
            );
        }
        self.model_routing.exclude =
            serde_json::from_value(serde_json::Value::Object(exclusion_groups))
                .map_err(|error| error.to_string())?;
        let mut groups = serde_json::Map::new();
        for consumer in crate::select_policy::Consumer::ALL {
            if let Some(id) = crate::variant_policy::consumer_group(Some(consumer)) {
                let policy = resolve_consumer_policy(
                    id,
                    &self.consumers,
                    &ConsumerPolicy::default(),
                    &BTreeMap::new(),
                );
                groups.insert(
                    id.into(),
                    serde_json::json!(policy.variants.unwrap_or_default()),
                );
            }
        }
        groups.insert("all".into(), serde_json::json!(self.default_variants));
        groups.insert("consumers".into(), serde_json::Value::Object(variants));
        self.model_routing.variants = serde_json::from_value(serde_json::Value::Object(groups))
            .map_err(|error| error.to_string())?;
        let mut pins = serde_json::Map::new();
        for (name, policy) in &self.consumers {
            if name.is_empty() || name.chars().any(char::is_whitespace) {
                return Err(format!("consumer id {name:?} must be one non-empty word"));
            }
            for (key, variant) in policy.variants.iter().flatten() {
                if striatum_core::provider_model_spec::parse_spec(key)
                    .is_none_or(|spec| spec.negated)
                    || key.trim() != key
                {
                    return Err(format!("consumers.{name}.variants: key {key:?} must be provider, provider/model or provider/model/variant"));
                }
                if variant.is_empty() || variant.chars().any(char::is_whitespace) {
                    return Err(format!(
                        "consumers.{name}.variants: variant must be one non-empty word"
                    ));
                }
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
        self.model_routing.pins.synthesizer =
            crate::config_policy::consumer_entry(&self.consumers, "synthesizer")
                .and_then(|policy| policy.pin.clone());
        self.model_routing.consumers = Some(
            self.consumers
                .keys()
                .filter(|id| !is_slot(id) || id.as_str() == "synthesizer")
                .cloned()
                .collect(),
        );
        self.model_routing.runners = Some(self.runners.clone());
        self.model_routing.subscription = subscription;
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
            if super::parse_model_key(route).is_none() {
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
        Ok(patched)
    }

    pub(crate) fn route_policy_refusal(&self, consumer: &str, route: &str) -> Option<String> {
        self.config.route_refusal(consumer, route, false)
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
        self.config
            .route_refusal_detail(consumer, route, false)
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
        is_slot(slot)
            .then(|| {
                crate::config_policy::consumer_entry(&self.config.consumers, slot)?
                    .pin
                    .as_ref()
            })
            .flatten()
    }
    pub fn consumer_policy(
        &self,
        consumer: &str,
        request: &ConsumerPolicy,
        declared: &BTreeMap<String, ConsumerPolicy>,
    ) -> ConsumerPolicy {
        resolve_consumer_policy(consumer, &self.config.consumers, request, declared)
    }

    /// Explicit pins bypass `pin_only` and `pin_only_for`, but never a
    /// provider/model `only_for` or `not_for` restriction.
    pub fn route_allowed(&self, consumer: &str, route: &str, pinned: bool) -> bool {
        self.config.route_refusal(consumer, route, pinned).is_none()
    }

    /// Media engines have no subscription alternative, so chat provider pin-only
    /// rules must not disable automatic media choice. Model pin-only rules still
    /// bind; the built-in rule requiring metered chat routes to be pinned does not.
    pub(crate) fn media_route_allowed(&self, consumer: &str, route: &str, pinned: bool) -> bool {
        self.config
            .route_refusal_detail_with_pin_scope(
                consumer,
                route,
                pinned,
                false,
                PinOnlyScope::Model,
            )
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
        let metered = plane.is_some_and(|plane| {
            striatum_core::billing::is_api_metered_auth_method(&plane.auth_method)
                && plane.kind != "quota_proxy"
        });
        self.route_allowed_with_default(consumer, route, pinned, metered)
    }

    pub(crate) fn route_allowed_with_default(
        &self,
        consumer: &str,
        route: &str,
        pinned: bool,
        metered: bool,
    ) -> bool {
        self.config
            .route_refusal_detail_with_default(consumer, route, pinned, metered)
            .is_none()
    }

    pub(crate) fn automatic_exclusions(&self, consumer: &str, facts: &[ModelFact]) -> Vec<String> {
        facts
            .iter()
            .filter_map(|fact| {
                let route = striatum_core::fact_to_route(fact).route_key;
                (!self.route_allowed_on_plane(consumer, &route, false, fact.billing_plane.as_ref()))
                    .then_some(route)
            })
            .collect()
    }
}

impl RoutingConfigFile {
    fn consumer_exclusions(&self, consumer: &str) -> Vec<String> {
        let excluded = |rules: &EntityPolicy| {
            rules
                .only_for
                .as_ref()
                .is_some_and(|names| !names.iter().any(|name| consumer_matches(name, consumer)))
                || rules
                    .not_for
                    .iter()
                    .flatten()
                    .any(|name| consumer_matches(name, consumer))
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
                let entity_excluded = excluded(rules);
                let parent_excluded = key
                    .split_once('/')
                    .and_then(|(provider, _)| self.providers.get(provider))
                    .is_some_and(|provider| excluded(&provider.rules));
                if entity_excluded {
                    exclusions.push(key.clone());
                } else if parent_excluded {
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
        let mut models: Vec<_> = self
            .model_routing
            .models
            .iter()
            .filter(|(key, _)| route == key.as_str() || route.starts_with(&format!("{key}/")))
            .collect();
        models.sort_by_key(|(key, _)| key.len());
        for (key, model) in models {
            let model_source = |field: &str| format!("models[{key:?}].{field}");
            if let Some(names) = &model.rules.only_for {
                only_for = Some((model_source("only_for"), names));
            }
            if let Some(names) = &model.rules.not_for {
                not_for = Some((model_source("not_for"), names));
            }
            if let Some(value) = model.rules.pin_only {
                pin_only = Some((model_source("pin_only"), value));
            }
            if let Some(value) = model.rules.subscription {
                subscription = Some(value);
            }
            if let Some(names) = &model.rules.pin_only_for {
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
