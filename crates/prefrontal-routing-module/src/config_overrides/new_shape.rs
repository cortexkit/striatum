use super::*;
use crate::config_policy::{
    consumer_matches, is_slot, resolve_consumer_policy, slot_consumer, ConsumerPolicy, EntityPolicy,
};

impl RoutingConfigFile {
    /// Materialize the temporary interfaces used by the existing selection pass.
    /// Only the entity-shaped fields are read from disk; the grouped views are
    /// rebuilt atomically with each validated snapshot.
    pub(super) fn prepare(&mut self) -> Result<(), String> {
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
            let mut exclusions = Vec::new();
            for (key, rules) in &entities {
                if rules.only_for.is_some() || rules.not_for.is_some() {
                    let excluded =
                        rules.only_for.as_ref().is_some_and(|names| {
                            !names.iter().any(|name| consumer_matches(name, id))
                        }) || rules
                            .not_for
                            .iter()
                            .flatten()
                            .any(|name| consumer_matches(name, id));
                    let parent_excluded = key
                        .split_once('/')
                        .and_then(|(provider, _)| self.providers.get(provider))
                        .is_some_and(|provider| {
                            provider.rules.only_for.as_ref().is_some_and(|names| {
                                !names.iter().any(|name| consumer_matches(name, id))
                            }) || provider
                                .rules
                                .not_for
                                .iter()
                                .flatten()
                                .any(|name| consumer_matches(name, id))
                        });
                    if !excluded && !parent_excluded {
                        continue;
                    }
                    exclusions.push(if excluded {
                        key.to_string()
                    } else {
                        format!("-{key}")
                    });
                }
            }
            exclusions
                .sort_by_key(|key| (key.trim_start_matches('-').split('/').count(), key.clone()));
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
                if let Some(entries) = variants.get(id) {
                    groups.insert(id.into(), entries.clone());
                }
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
                if prefrontal_routing_core::provider_model_spec::parse_spec(key)
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
                if rest
                    .as_object()
                    .unwrap()
                    .values()
                    .any(|value| !value.is_null() && value != &serde_json::json!({}))
                {
                    return Err(format!("consumers.{name} is a slot and carries only pin"));
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
        self.model_routing.consumers = Some(
            self.consumers
                .keys()
                .filter(|id| !is_slot(id))
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
    pub fn slot_pin(&self, slot: &str) -> Option<&crate::config_policy::SynthesizerPin> {
        is_slot(slot)
            .then(|| self.config.consumers.get(slot)?.pin.as_ref())
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
}

impl RoutingConfigFile {
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
        let provider = route.split('/').next().unwrap_or(route);
        let provider_rules = self.providers.get(provider).map(|policy| &policy.rules);
        let provider_source = |field: &str| format!("providers.{provider}.{field}");
        let mut only_for = provider_rules
            .and_then(|rules| rules.only_for.as_ref())
            .map(|names| (provider_source("only_for"), names));
        let mut not_for = provider_rules
            .and_then(|rules| rules.not_for.as_ref())
            .map(|names| (provider_source("not_for"), names));
        let mut pin_only = provider_rules
            .and_then(|rules| rules.pin_only)
            .map(|value| (provider_source("pin_only"), value));
        let mut pin_only_for = provider_rules
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
            if let Some(names) = &model.rules.pin_only_for {
                pin_only_for = Some((model_source("pin_only_for"), names));
            }
        }
        if let Some((source, names)) = only_for {
            if !names.iter().any(|name| consumer_matches(name, consumer)) {
                return Some(format!("{source} {names:?} does not name {consumer:?}"));
            }
        }
        if let Some((source, names)) = not_for {
            if let Some(name) = names.iter().find(|name| consumer_matches(name, consumer)) {
                return Some(format!("{source} lists {name:?}"));
            }
        }
        if let Some((source, true)) = pin_only {
            if !pinned {
                return Some(format!("{source} admits the route only when pinned"));
            }
        }
        if let Some((source, names)) = pin_only_for {
            if !pinned {
                if let Some(name) = names.iter().find(|name| consumer_matches(name, consumer)) {
                    return Some(format!(
                        "{source} lists {name:?}, so it admits the route only when pinned"
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
        (!served).then(|| format!("providers.{provider}.models {listed:?} does not list the model"))
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
