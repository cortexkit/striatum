use std::collections::BTreeMap;
use std::sync::Arc;

use cortexkit_role_routing::service_tier::ServiceTier;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config_policy::RunnerPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Standard,
    Fast,
    Ultrafast,
}

impl Tier {
    pub fn value(self) -> ServiceTier {
        match self {
            Self::Standard => ServiceTier::Standard,
            Self::Fast => ServiceTier::Fast,
            Self::Ultrafast => ServiceTier::Ultrafast,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Consumer {
    pub(crate) service_tier: Option<Tier>,
    pub(crate) service_tiers: Option<BTreeMap<String, Tier>>,
    pub(crate) runners: Option<Vec<RunnerChoice>>,
}

impl Consumer {
    /// A scoped runner list follows the user list's rules for names, harness
    /// words, duplicates and route specs, so a malformed route is refused
    /// instead of silently matching nothing.
    pub(crate) fn validate_runners(&self, name: &str) -> Result<(), String> {
        let Some(runners) = &self.runners else {
            return Ok(());
        };
        let policies: Vec<_> = runners.iter().map(RunnerChoice::policy).collect();
        crate::config_overrides::validate_runner_list(
            &policies,
            &format!("consumers.{name}.runners"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunnerChoice {
    module: String,
    harness: Option<String>,
    routes: Option<Vec<String>>,
}

impl RunnerChoice {
    fn policy(&self) -> RunnerPolicy {
        RunnerPolicy {
            module: self.module.clone(),
            harness: self.harness.clone(),
            routes: self.routes.clone(),
            tool_capabilities: Vec::new(),
            via_vault: false,
        }
    }

    fn narrow(&self, enrolled: &[RunnerPolicy], user: &[RunnerPolicy]) -> Option<RunnerPolicy> {
        let mut policy = enrolled
            .iter()
            .find(|entry| {
                entry.module == self.module
                    && (self.harness.is_none()
                        || entry.harness.is_none()
                        || entry.harness == self.harness)
            })?
            .clone();
        if self.harness.is_some() {
            policy.harness.clone_from(&self.harness);
        }
        if let Some(routes) = &self.routes {
            policy.routes = Some(policy.routes.as_ref().map_or_else(
                || routes.clone(),
                |allowed| intersect_routes(allowed, routes),
            ));
        }
        // A scoped list replaces the placement order, not user-only metadata
        // for the same runner. The user's bare module entry covers its harnesses.
        for entry in user.iter().filter(|entry| {
            entry.module == policy.module
                && (entry.harness.is_none() || entry.harness == policy.harness)
        }) {
            policy.via_vault |= entry.via_vault;
            for tag in &entry.tool_capabilities {
                if !policy.tool_capabilities.contains(tag) {
                    policy.tool_capabilities.push(tag.clone());
                }
            }
        }
        Some(policy)
    }
}

fn intersect_routes(enrolled: &[String], project: &[String]) -> Vec<String> {
    // Each positive route spec admits its slash-separated descendants. Two
    // specs overlap only when one names a descendant of the other.
    fn includes(prefix: &str, key: &str) -> bool {
        key == prefix
            || key
                .strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with('/'))
    }
    let mut intersection = Vec::new();
    for allowed in enrolled {
        for chosen in project {
            if includes(allowed, chosen) {
                intersection.push(chosen.clone());
            } else if includes(chosen, allowed) {
                intersection.push(allowed.clone());
            }
        }
    }
    intersection.sort_unstable();
    intersection.dedup();
    intersection
}

pub(crate) fn without_context(params: &serde_json::Value) -> serde_json::Value {
    let mut params = params.clone();
    if let Some(object) = params.as_object_mut() {
        for key in ["project_root", "workspace_root", "tier_consumer"] {
            object.remove(key);
        }
    }
    params
}

/// One consumer's configured service tiers, gathered once per request so each
/// candidate model is a few map lookups rather than a scan and copy of every
/// configured consumer.
pub(crate) struct TierResolver<'a> {
    global: &'a crate::config_overrides::LiveConfig,
    // Entries whose name matches the consumer, most specific first.
    matched: Vec<MatchedTiers<'a>>,
}

pub(crate) struct MatchedTiers<'a> {
    pub(crate) name: &'a str,
    pub(crate) tier: Option<Tier>,
    /// Routes a workspace or project layer set; they win over `user`.
    pub(crate) scoped: Option<&'a BTreeMap<String, Tier>>,
    pub(crate) user: Option<&'a BTreeMap<String, Tier>>,
}

impl<'a> TierResolver<'a> {
    pub(crate) fn new(
        global: &'a crate::config_overrides::LiveConfig,
        mut matched: Vec<MatchedTiers<'a>>,
    ) -> Self {
        // A stable sort keeps settings order between equally specific names.
        matched.sort_by_key(|entry| {
            std::cmp::Reverse(crate::config_policy::consumer_specificity(entry.name))
        });
        Self { global, matched }
    }

    /// The most specific consumer entry naming the model's route, then its
    /// provider, then the entry's own tier, then the model and provider rules.
    pub(crate) fn resolve(&self, provider: &str, model: &str) -> Option<ServiceTier> {
        let route = format!("{provider}/{model}");
        let by_key = |key: &str| {
            self.matched.iter().find_map(|entry| {
                entry
                    .scoped
                    .and_then(|tiers| tiers.get(key))
                    .or_else(|| entry.user.and_then(|tiers| tiers.get(key)))
                    .copied()
            })
        };
        by_key(&route)
            .or_else(|| by_key(provider))
            .or_else(|| self.matched.iter().find_map(|entry| entry.tier))
            .or_else(|| self.global.rule_service_tier(provider, &route))
            .map(Tier::value)
    }
}

#[derive(Default)]
pub(crate) struct Layers {
    consumers: Option<Arc<BTreeMap<String, Consumer>>>,
    enrolled: Option<Vec<RunnerPolicy>>,
}

impl Layers {
    pub(crate) fn new(
        consumers: Arc<BTreeMap<String, Consumer>>,
        enrolled: Option<Vec<RunnerPolicy>>,
    ) -> Self {
        Self {
            consumers: Some(consumers),
            enrolled,
        }
    }

    pub(crate) fn runners(
        &self,
        consumer: &str,
        global: &crate::config_overrides::LiveConfig,
        user_runners: Option<&[RunnerPolicy]>,
    ) -> Option<Vec<RunnerPolicy>> {
        self.runner_candidates(consumer, global, user_runners)
            .map(|candidates| {
                candidates
                    .into_iter()
                    .filter_map(|(policy, allowed)| allowed.then_some(policy))
                    .collect()
            })
    }

    pub(crate) fn runner_candidates(
        &self,
        consumer: &str,
        global: &crate::config_overrides::LiveConfig,
        user_runners: Option<&[RunnerPolicy]>,
    ) -> Option<Vec<(RunnerPolicy, bool)>> {
        use crate::config_policy::{consumer_matches, consumer_specificity};
        let enrolled = self.enrolled.as_deref()?;
        let (name, choices) = self
            .consumers
            .as_ref()?
            .iter()
            .filter(|(name, _)| consumer_matches(name, consumer))
            .filter_map(|(name, entry)| Some((name, entry.runners.as_ref()?)))
            .max_by_key(|(name, _)| consumer_specificity(name))?;
        if global.has_more_specific_runner_list(consumer, name) {
            return None;
        }
        Some(
            choices
                .iter()
                .map(|choice| {
                    choice
                        .narrow(enrolled, user_runners.unwrap_or_default())
                        .map_or_else(|| (choice.policy(), false), |policy| (policy, true))
                })
                .collect(),
        )
    }

    pub(crate) fn resolve(
        &self,
        global: &crate::config_overrides::LiveConfig,
        consumer: &str,
        provider: &str,
        model: &str,
        pin: Option<ServiceTier>,
    ) -> Option<ServiceTier> {
        pin.or_else(|| {
            global.service_tier_with(consumer, provider, model, self.consumers.as_deref())
        })
    }

    /// Gather `consumer`'s tier settings once for a pass over many models.
    pub(crate) fn tier_resolver<'a>(
        &'a self,
        global: &'a crate::config_overrides::LiveConfig,
        consumer: &str,
    ) -> TierResolver<'a> {
        global.tier_resolver(consumer, self.consumers.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn project_runner_keeps_matching_user_metadata_without_borrowing_it_for_other_runners() {
        let enrolled: Vec<RunnerPolicy> = serde_json::from_value(json!([
            {"module":"chosen", "tool_capabilities":["enrolled/v1"]},
            {"module":"unmatched", "tool_capabilities":["unmatched/v1"]},
        ]))
        .unwrap();
        let operator = serde_json::from_value(json!({
            "prefrontal.sidekick": {"runners":[
                {"module":"chosen", "via_vault":true, "tool_capabilities":["user/v1"]},
                {"module":"unmatched", "harness":"other", "via_vault":true, "tool_capabilities":["other/v1"]}
            ]},
            "prefrontal.sidekick.review": {"service_tier":"fast"}
        })).unwrap();
        let user = crate::config_policy::resolve_consumer_policy(
            "prefrontal.sidekick.review",
            &operator,
            &Default::default(),
            &Default::default(),
        );
        let layers = Layers::new(
            Arc::new(
                serde_json::from_value(json!({
                    "prefrontal.sidekick.review": {"runners":[
                        {"module":"chosen", "harness":"requested"},
                        {"module":"unmatched", "harness":"requested"},
                    ]},
                }))
                .unwrap(),
            ),
            Some(enrolled),
        );
        let runners = layers
            .runners(
                "prefrontal.sidekick.review",
                &Default::default(),
                user.runners.as_deref(),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(runners).unwrap(),
            json!([
                {"module":"chosen", "harness":"requested", "via_vault":true, "tool_capabilities":["enrolled/v1", "user/v1"]},
                {"module":"unmatched", "harness":"requested", "via_vault":false, "tool_capabilities":["unmatched/v1"]},
            ])
        );
    }
}
