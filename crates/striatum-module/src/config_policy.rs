//! Closed operator policy shapes. Consumer admission and panel selection read
//! these values from the same immutable snapshot as the model overrides.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

pub(crate) const ATHENA_SYNTHESIZER_CONSUMER: &str = "prefrontal-core.athena.synthesizer";

/// Operator spelling of the contract's consumer scheduling classes.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PriorityClass {
    Interactive,
    Blocking,
    Worker,
    Background,
}

impl From<PriorityClass> for cortexkit_role_routing::declarations::ConsumerPriority {
    fn from(value: PriorityClass) -> Self {
        match value {
            PriorityClass::Interactive => Self::Interactive,
            PriorityClass::Blocking => Self::Blocking,
            PriorityClass::Worker => Self::Worker,
            PriorityClass::Background => Self::Background,
        }
    }
}

fn present_value<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ExcludeGroups {
    pub all: Vec<String>,
    pub mason: Vec<String>,
    pub sidekick: Vec<String>,
    pub athena: Vec<String>,
    pub lab: Vec<String>,
    pub core: Vec<String>,
    pub codemode: Vec<String>,
    pub flow: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Ranking {
    Iq,
    Eq,
    Cost,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtraRanking {
    Random,
    Iq,
    Eq,
    Cost,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PanelPolicy {
    /// Number of leading models to select, from 1 to 8.
    #[schemars(range(min = 1, max = 8))]
    pub top_k: u8,
    /// Rank the leading models by reasoning quality, collaboration quality, or cost.
    pub by: Ranking,
    /// Number of additional models to select, from 0 to 8 minus the leading count.
    #[serde(default)]
    #[schemars(range(min = 0, max = 7))]
    pub extra: u8,
    /// How to choose the additional models. Required when the additional count is above 0.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub extra_by: Option<ExtraRanking>,
    /// Minimum quality score for a panel seat, from 0 to 100. Omit to use the request's quality rules.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(range(min = 0, max = 100))]
    pub floor: Option<u8>,
}

impl PanelPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.top_k == 0 || u16::from(self.top_k) + u16::from(self.extra) > 8 {
            return Err("top_k must be >= 1 and top_k + extra must be <= 8".into());
        }
        if self.extra > 0 && self.extra_by.is_none() {
            return Err("extra_by is required when extra > 0".into());
        }
        if self.floor.is_some_and(|floor| floor > 100) {
            return Err("floor must be an integer from 0 to 100".into());
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PanelPolicies {
    /// Model selection for audit panels. Omit to use the built-in audit policy.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub audit: Option<PanelPolicy>,
    /// Model selection for design review panels. Omit to use the built-in design review policy.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub design_review: Option<PanelPolicy>,
    /// Model selection for diagnosis panels. Omit to use the built-in diagnosis policy.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub diagnose: Option<PanelPolicy>,
    /// Model selection for evaluation panels. Omit to use the built-in evaluation policy.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub evaluate: Option<PanelPolicy>,
    /// Model selection for explanation panels. Omit to use the built-in explanation policy.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub explain: Option<PanelPolicy>,
    /// Model selection for specification panels. Omit to use the built-in specification policy.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub spec: Option<PanelPolicy>,
    /// Model selection for lab panels. Omit to use the built-in lab policy.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub lab: Option<PanelPolicy>,
}

impl PanelPolicies {
    pub fn validate(&self) -> Result<(), String> {
        for (class, policy) in [
            ("audit", &self.audit),
            ("design_review", &self.design_review),
            ("diagnose", &self.diagnose),
            ("evaluate", &self.evaluate),
            ("explain", &self.explain),
            ("spec", &self.spec),
            ("lab", &self.lab),
        ] {
            if let Some(policy) = policy {
                policy
                    .validate()
                    .map_err(|error| format!("panels.{class}: {error}"))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SynthesizerPin {
    /// Route to pin, written as provider/model.
    #[schemars(regex(pattern = r"^[^/\s]+/[^\s]+$"))]
    pub model: String,
    /// Optional named model variant. Omit to use the model's usual variant.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(regex(pattern = r"\S"))]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LabPlannerPin {
    #[schemars(regex(pattern = r"^[^/\s]+/[^\s]+$"))]
    pub model: String,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Pins {
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "SynthesizerPin", !default)]
    pub synthesizer: Option<SynthesizerPin>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "LabPlannerPin", !default)]
    pub lab_planner: Option<LabPlannerPin>,
    // The Spec Campaigns V2 designer's model, requested on `route.select` as
    // the `spec_designer` pin slot. It has the synthesizer pin's shape: a
    // `provider/model` and an optional variant the designer then runs at.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "SynthesizerPin", !default)]
    pub spec_designer: Option<SynthesizerPin>,
}

impl Pins {
    pub fn validate(&self) -> Result<(), String> {
        for (slot, model) in [
            (
                "synthesizer",
                self.synthesizer.as_ref().map(|pin| pin.model.as_str()),
            ),
            (
                "lab_planner",
                self.lab_planner.as_ref().map(|pin| pin.model.as_str()),
            ),
            (
                "spec_designer",
                self.spec_designer.as_ref().map(|pin| pin.model.as_str()),
            ),
        ] {
            if let Some(model) = model {
                if !model
                    .split_once('/')
                    .is_some_and(|(provider, model)| !provider.is_empty() && !model.is_empty())
                    || model.chars().any(char::is_whitespace)
                {
                    return Err(format!(
                        "model_routing.pins.{slot}.model must be provider/model"
                    ));
                }
            }
        }
        for (slot, pin) in [
            ("synthesizer", &self.synthesizer),
            ("spec_designer", &self.spec_designer),
        ] {
            if pin
                .as_ref()
                .and_then(|pin| pin.variant.as_ref())
                .is_some_and(|variant| variant.trim().is_empty())
            {
                return Err(format!(
                    "model_routing.pins.{slot}.variant must be non-empty"
                ));
            }
        }
        Ok(())
    }
}
impl ExcludeGroups {
    /// The exclusion list a consumer group names, or nothing when no group of
    /// that name exists. Looked up through the serialized field names so a
    /// group added to this struct is found without a second list to update.
    #[cfg(test)]
    pub fn named_group(&self, group: &str) -> Vec<String> {
        if group == "all" {
            return Vec::new();
        }
        serde_json::to_value(self)
            .ok()
            .and_then(|groups| groups.get(group).cloned())
            .and_then(|entries| serde_json::from_value(entries).ok())
            .unwrap_or_default()
    }
}
/// A runner module `routing/v1` answers with (`runners[]`).
/// Listed in preference order: the first listed runner that can serve the
/// answered route is named.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunnerPolicy {
    /// Name of the module that runs the selected model.
    #[schemars(regex(pattern = r"^\S+$"))]
    pub module: String,
    /// Restrict this entry to one of the runner's named execution environments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    /// Routes this runner may use, as provider, provider/model, or provider/model/variant. Omit to allow every route the runner supports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routes: Option<Vec<String>>,
    /// Tools this runner can provide, named by capability tags such as `code.read/v1`.
    #[serde(default)]
    pub tool_capabilities: Vec<String>,
    /// Require an available account credential before choosing this runner.
    #[serde(default)]
    pub via_vault: bool,
}

fn dotted_prefix(prefix: &str, consumer: &str) -> bool {
    consumer == prefix
        || consumer
            .strip_prefix(prefix)
            .is_some_and(|tail| tail.starts_with('.'))
}

pub fn consumer_specificity(name: &str) -> usize {
    striatum_core::module_identity::canonical(name).len()
}

pub fn consumer_entry<'a, T>(entries: &'a BTreeMap<String, T>, name: &str) -> Option<&'a T> {
    striatum_core::module_identity::spellings(name)
        .iter()
        .find_map(|key| entries.get(key.as_ref()))
}

/// Match whole dotted segments, so children inherit their namespace's policy.
pub fn consumer_matches(prefix: &str, consumer: &str) -> bool {
    dotted_prefix(
        &striatum_core::module_identity::canonical(prefix),
        &striatum_core::module_identity::canonical(consumer),
    )
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EntityPolicy {
    /// Service speed to request for this provider or model. Omit to follow the broader routing policy.
    pub service_tier: Option<crate::tier_settings::Tier>,
    /// Allow only these consumers and their dotted subgroups. Omit to inherit the broader allow list.
    pub only_for: Option<Vec<String>>,
    /// Block these consumers and their dotted subgroups. Omit to inherit the broader block list.
    pub not_for: Option<Vec<String>>,
    /// Require an explicitly named model instead of choosing it automatically. Omit to follow the provider or billing policy.
    pub pin_only: Option<bool>,
    /// Require an explicitly named model only for these consumers and their dotted subgroups.
    pub pin_only_for: Option<Vec<String>>,
    /// Treat this provider or model as a flat-rate subscription. Omit to use the account's billing information.
    pub subscription: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderPolicy {
    #[serde(flatten)]
    pub rules: EntityPolicy,
    /// Allow this provider to use cached prompts when available.
    pub prompt_cache: Option<bool>,
    /// Divide the estimated cost of using a subscription by this number. Use a number greater than 0.
    pub subscription_multiplier: Option<f64>,
    /// Reserve this percentage of weekly quota for operator-facing sessions, from 0 to 100.
    pub keep_free_percent: Option<f64>,
    /// Count saved quota resets only near exhaustion (`emergency`) or count all unexpired resets (`capacity`).
    pub banked_resets: BankedResets,
    /// Models this provider may serve. Omit to allow every model offered by the provider.
    pub models: Option<Vec<String>>,
    /// Account selection and spending rules that override the global account settings for this provider.
    pub accounts: ProviderAccountsPolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BankedResets {
    #[default]
    Emergency,
    Capacity,
}

/// Per-provider account controls. Metered fallback requires explicit opt-in.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderAccountsPolicy {
    /// Allow this provider to use a pay-per-token account when its subscription accounts are unavailable.
    pub metered_fallback: bool,
    /// How to choose among available accounts: `sticky`, `round_robin`, `hybrid`, or `ordered`.
    pub strategy: Option<String>,
    /// Preferred account order, listed by account ID.
    pub order: Option<Vec<String>>,
    /// For ordered selection, try main accounts first (`main-first`) or fallback accounts first (`fallback-first`).
    pub ordered: Option<String>,
    /// Prefer accounts with at least this percentage of quota remaining, from 0 to 100.
    pub soft_quota_remaining_percent: Option<f64>,
    /// Minimum remaining quota for individual quota pools.
    pub floors: Option<Vec<AccountPoolFloor>>,
    /// Longest time a request may wait for an account. Use seconds (`s`), minutes (`m`), or hours (`h`).
    pub max_hold: Option<AccountDuration>,
    /// Wait for an account only if its quota resets within this time. Use seconds (`s`), minutes (`m`), or hours (`h`). Omit to use the request's wait limit.
    pub hold_when_reset_within: Option<AccountDuration>,
    /// Wait this long before retrying an account whose reset time is unknown. Use seconds (`s`), minutes (`m`), or hours (`h`).
    pub unknown_reset_retry: Option<AccountDuration>,
    /// Maximum account spending in US dollars. Use a positive amount or omit for no spending limit.
    pub spend_budget_usd: Option<f64>,
    /// Scheduled spending-limit reset as milliseconds since the Unix epoch. Omit for no scheduled reset.
    pub spend_budget_reset_at_ms: Option<i64>,
    /// Allow calls to draw on the account's paid credits.
    pub paid_credits: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct AccountDuration(String);

impl AccountDuration {
    pub fn seconds(&self) -> Result<u64, String> {
        let text = &self.0;
        let (number, factor) = if let Some(number) = text.strip_suffix('s') {
            (number, 1)
        } else if let Some(number) = text.strip_suffix('m') {
            (number, 60)
        } else if let Some(number) = text.strip_suffix('h') {
            (number, 3600)
        } else {
            return Err("duration must use s, m or h".into());
        };
        number
            .parse::<u64>()
            .ok()
            .and_then(|number| number.checked_mul(factor))
            .ok_or_else(|| "duration must be a non-negative whole number of seconds".into())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountPoolFloor {
    /// Name of the quota pool. Provide a pool name, a window length, or both.
    pub pool: Option<String>,
    /// Length of the quota window in minutes. Provide a window length, a pool name, or both.
    pub window_minutes: Option<u32>,
    /// Model this floor applies to. Omit to apply it to every model using the pool.
    pub model: Option<String>,
    /// Minimum quota that must remain in the pool, from 0 to 100 percent.
    pub remaining_percent: f64,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerMode {
    Demand,
    Ladder,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BelowBarPolicy {
    #[default]
    BestEffort,
    Refuse,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectPolicy {
    /// Prefer this model in the project, as provider/model or provider/model:variant. Leave empty for ordinary selection.
    pub pin: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ConsumerPolicy {
    /// Name of the workload this consumer serves. Omit when it does not declare a workload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload: Option<String>,
    /// Service speed to request for this consumer. Omit to follow the broader routing policy.
    pub service_tier: Option<crate::tier_settings::Tier>,
    /// Service speed to request for particular model or provider IDs.
    pub service_tiers: Option<BTreeMap<String, crate::tier_settings::Tier>>,
    /// Choose a model for each request (`demand`) or follow an ordered list of models (`ladder`).
    pub mode: Option<ConsumerMode>,
    /// When no model meets the quality target, decline the request (`refuse`) or try the best available model (`best_effort`).
    pub below_bar: Option<BelowBarPolicy>,
    /// Ordered model preferences, written as provider/model or provider/model:variant.
    pub ladder: Option<Vec<String>>,
    /// Internal provenance: only an operator-written ladder admits pin-only routes.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) ladder_source: Option<ConsumerLadderSource>,
    /// Name of the preferred module that runs this consumer's models.
    pub runner: Option<String>,
    /// Replace the global runner list for this consumer and its dotted subgroups. An empty list disables all runners.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runners: Option<Vec<RunnerPolicy>>,
    /// Module responsible for this consumer. Omit to use the consumer's declared owner.
    pub owner: Option<String>,
    /// Limit this consumer to routes priced per token.
    pub pay_per_token: Option<bool>,
    /// Prefer a specific model and optional variant for this consumer.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub pin: Option<SynthesizerPin>,
    /// Project-specific model preferences for this consumer.
    pub projects: BTreeMap<String, ProjectPolicy>,
    /// Minimum reasoning quality requested, from 0 to 100.
    pub iq: Option<f64>,
    /// Minimum collaboration quality requested, from 0 to 100.
    pub eq: Option<f64>,
    /// Minimum context capacity in tokens.
    pub context_tokens: Option<u64>,
    /// Minimum output capacity in tokens.
    pub max_output_tokens: Option<u64>,
    /// Tools the selected runner must provide, named by capability tags.
    pub tool_capabilities: Option<Vec<String>>,
}

pub fn is_slot(name: &str) -> bool {
    slot_consumer(name).is_some()
}

/// The `route.select` consumer a pin slot is requested under, so the slot's
/// pin is judged by that consumer's `only_for` / `not_for` rules. Core asks for
/// the synthesizer pin and the Spec Campaigns designer's `spec_designer` slot as
/// prefrontal's Athena consumer, and `lab_planner` as its Lab consumer. The old
/// spelling remains the comparison representative during the rename window.
pub fn slot_consumer(slot: &str) -> Option<&'static str> {
    match slot {
        "synthesizer" | "spec_designer" => Some("prefrontal-core.athena"),
        "lab_planner" => Some("prefrontal-core.lab"),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumerLadderSource {
    OperatorSettings,
    Declaration,
    Request,
}

impl ConsumerPolicy {
    pub(crate) fn operator_ladder(&self) -> bool {
        self.mode == Some(ConsumerMode::Ladder)
            && self.ladder_source == Some(ConsumerLadderSource::OperatorSettings)
    }
}

/// Resolve each key independently, exhausting operator ancestors before looking
/// at the request or declaration. A more specific lower-priority source cannot
/// override an operator's parent entry.
pub fn resolve_consumer_policy(
    consumer: &str,
    operator: &BTreeMap<String, ConsumerPolicy>,
    request: &ConsumerPolicy,
    declared: &BTreeMap<String, ConsumerPolicy>,
) -> ConsumerPolicy {
    let mut result = serde_json::Map::new();
    if consumer_matches("prefrontal-core.mason", consumer) {
        result.insert("below_bar".into(), serde_json::json!("refuse"));
    }
    let mut ladder_source = None;
    let mut overlay = |entry: &ConsumerPolicy, source: ConsumerLadderSource| {
        if entry.ladder.is_some() {
            ladder_source = Some(source);
        }
        // Loaded legacy entries carry their expanded list and original module.
        // A child's own list must clear that module as well as replace the list.
        if entry.runners.is_some() {
            result.remove("runner");
        }
        for (key, value) in serde_json::to_value(entry).unwrap().as_object().unwrap() {
            if !value.is_null() && value != &serde_json::json!({}) {
                result.insert(key.clone(), value.clone());
            }
        }
    };
    for (source, is_operator) in [(declared, false), (operator, true)] {
        if is_operator {
            overlay(
                request,
                request
                    .ladder_source
                    .unwrap_or(ConsumerLadderSource::Request),
            );
        }
        let mut chain: Vec<_> = source
            .iter()
            .filter(|(name, _)| {
                (!is_slot(name) || consumer_matches(consumer, name))
                    && consumer_matches(name, consumer)
            })
            .collect();
        chain.sort_by_key(|(name, _)| consumer_specificity(name));
        for (_, entry) in chain {
            overlay(
                entry,
                if is_operator {
                    ConsumerLadderSource::OperatorSettings
                } else {
                    ConsumerLadderSource::Declaration
                },
            );
        }
    }
    let mut result: ConsumerPolicy =
        serde_json::from_value(serde_json::Value::Object(result)).unwrap();
    result.ladder_source = ladder_source;
    if let Some(pin) = &result.pin {
        result.mode = Some(ConsumerMode::Ladder);
        result.ladder = Some(vec![pin.variant.as_ref().map_or_else(
            || pin.model.clone(),
            |variant| format!("{}:{variant}", pin.model),
        )]);
    }
    result
}
