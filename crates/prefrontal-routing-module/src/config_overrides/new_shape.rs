use super::*;
use crate::config_policy::{
    consumer_matches, is_slot, resolve_consumer_policy, ConsumerPolicy, EntityPolicy,
};

impl ModelFactOverride {
    pub(super) fn has_fact_override(&self) -> bool {
        self.family.is_some()
            || self.elo.is_some()
            || self.eq.is_some()
            || self.speed.is_some()
            || self.cost_per_mtok_out.is_some()
            || self.context_window.is_some()
            || self.vision.is_some()
            || self.speed_multiplier.is_some()
            || self.subscription_multiplier.is_some()
    }
}

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
            let policy = resolve_consumer_policy(
                id,
                &self.consumers,
                &ConsumerPolicy::default(),
                &BTreeMap::new(),
            );
            variants.insert(
                id.into(),
                serde_json::json!(policy.variants.unwrap_or_default()),
            );
        }
        self.model_routing.exclude =
            serde_json::from_value(serde_json::Value::Object(exclusion_groups))
                .map_err(|error| error.to_string())?;
        variants.insert("all".into(), serde_json::json!(self.default_variants));
        self.model_routing.variants = serde_json::from_value(serde_json::Value::Object(variants))
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

    /// Explicit pins bypass `pin_only`, but never a provider/model `only_for`
    /// or `not_for` restriction.
    pub fn route_allowed(&self, consumer: &str, route: &str, pinned: bool) -> bool {
        let provider = route.split('/').next().unwrap_or(route);
        let mut rules = self
            .config
            .providers
            .get(provider)
            .map(|provider| provider.rules.clone())
            .unwrap_or_default();
        let mut models: Vec<_> = self
            .config
            .model_routing
            .models
            .iter()
            .filter(|(key, _)| route == key.as_str() || route.starts_with(&format!("{key}/")))
            .collect();
        models.sort_by_key(|(key, _)| key.len());
        for (_, model) in models {
            if model.rules.only_for.is_some() {
                rules.only_for = model.rules.only_for.clone();
            }
            if model.rules.not_for.is_some() {
                rules.not_for = model.rules.not_for.clone();
            }
            if model.rules.pin_only.is_some() {
                rules.pin_only = model.rules.pin_only;
            }
        }
        rules
            .only_for
            .as_ref()
            .is_none_or(|names| names.iter().any(|name| consumer_matches(name, consumer)))
            && !rules
                .not_for
                .iter()
                .flatten()
                .any(|name| consumer_matches(name, consumer))
            && (pinned || rules.pin_only != Some(true))
            && self
                .config
                .providers
                .get(provider)
                .and_then(|provider| provider.models.as_ref())
                .is_none_or(|models| {
                    models.iter().any(|model| {
                        route
                            .strip_prefix(&format!("{provider}/"))
                            .is_some_and(|tail| {
                                tail == model || tail.starts_with(&format!("{model}/"))
                            })
                    })
                })
    }
}
