//! Closed operator policy shapes. Consumer admission and panel selection read
//! these values from the same immutable snapshot as the model overrides.

use schemars::{JsonSchema, Schema};
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
#[schemars(transform = panel_constraints)]
pub struct PanelPolicy {
    #[serde(rename = "topK")]
    #[schemars(range(min = 1, max = 8))]
    pub top_k: u8,
    pub by: Ranking,
    #[serde(default)]
    #[schemars(range(min = 0, max = 7))]
    pub extra: u8,
    #[serde(
        rename = "extraBy",
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "ExtraRanking", !default)]
    pub extra_by: Option<ExtraRanking>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "u8", !default)]
    #[schemars(range(min = 0, max = 100))]
    pub floor: Option<u8>,
}

impl PanelPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.top_k == 0 || u16::from(self.top_k) + u16::from(self.extra) > 8 {
            return Err("topK must be >= 1 and topK + extra must be <= 8".into());
        }
        if self.extra > 0 && self.extra_by.is_none() {
            return Err("extraBy is required when extra > 0".into());
        }
        if self.floor.is_some_and(|floor| floor > 100) {
            return Err("floor must be an integer from 0 to 100".into());
        }
        Ok(())
    }
}

// JSON Schema cannot add two properties, so enumerate the eight possible
// topK values to express the same width cap used by runtime validation.
fn panel_constraints(schema: &mut Schema) {
    let mut constraints = vec![serde_json::json!({
        "if": {"required": ["extra"], "properties": {"extra": {"minimum": 1}}},
        "then": {"required": ["extraBy"], "properties": {"extraBy": {"type": "string"}}}
    })];
    for top_k in 1..=8 {
        constraints.push(serde_json::json!({
            "if": {"properties": {"topK": {"const": top_k}}},
            "then": {"properties": {"extra": {"maximum": 8 - top_k}}}
        }));
    }
    schema.insert("allOf".into(), constraints.into());
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PanelPolicies {
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PanelPolicy", !default)]
    pub audit: Option<PanelPolicy>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PanelPolicy", !default)]
    pub design_review: Option<PanelPolicy>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PanelPolicy", !default)]
    pub diagnose: Option<PanelPolicy>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PanelPolicy", !default)]
    pub evaluate: Option<PanelPolicy>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PanelPolicy", !default)]
    pub explain: Option<PanelPolicy>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PanelPolicy", !default)]
    pub spec: Option<PanelPolicy>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PanelPolicy", !default)]
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
                    .map_err(|error| format!("model_routing.panels.{class}: {error}"))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SynthesizerPin {
    #[schemars(regex(pattern = r"^[^/\s]+/[^\s]+$"))]
    pub model: String,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "String", !default, regex(pattern = r"\S"))]
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
    /// The runner's module id. Only a route principal the daemon verified as
    /// this module may call the runner operations.
    #[schemars(regex(pattern = r"^\S+$"))]
    pub module: String,
    /// Restrict this placement to one of the runner's declared harnesses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    /// The routes it runs, in the exclusion grammar (`provider`,
    /// `provider/model`, `provider/model/variant`). Absent: every route it
    /// serves. With `via_vault`, reach also requires a serving vault credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routes: Option<Vec<String>>,
    /// Tool capability tags (such as `code.read/v1`) the runner can reach a
    /// tool provider for. A select that lists tool capabilities is answered
    /// only on a runner reaching every one of them.
    #[serde(default)]
    pub tool_capabilities: Vec<String>,
    /// When true, reaching this runner also requires a serving vault credential.
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
    pub service_tier: Option<crate::tier_settings::Tier>,
    pub only_for: Option<Vec<String>>,
    pub not_for: Option<Vec<String>>,
    /// Require a pin for this entity. Metered billing planes default to true;
    /// false opts into automatic selection, with model values overriding providers.
    pub pin_only: Option<bool>,
    /// Like `pin_only`, but only for these consumers and their dotted
    /// descendants: other admitted consumers still route here automatically.
    /// Lets a scarce model serve one consumer by choice and others by pin.
    pub pin_only_for: Option<Vec<String>>,
    /// Mark a flat-rate subscription, including API-key plans. True also exempts
    /// this entity from built-in metered pin-only defaults, not explicit pin-only rules.
    pub subscription: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderPolicy {
    #[serde(flatten)]
    pub rules: EntityPolicy,
    pub prompt_cache: Option<bool>,
    pub subscription_multiplier: Option<f64>,
    pub keep_free_percent: Option<f64>,
    /// Saved-reset pricing: emergency credits one near the wall; capacity counts
    /// every unexpired reset as a full quota window. Neither applies a reset.
    pub banked_resets: BankedResets,
    pub models: Option<Vec<String>>,
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
    pub metered_fallback: bool,
    pub strategy: Option<String>,
    pub order: Option<Vec<String>>,
    pub ordered: Option<String>,
    pub soft_quota_remaining_percent: Option<f64>,
    pub floors: Option<Vec<AccountPoolFloor>>,
    pub max_hold: Option<AccountDuration>,
    pub hold_when_reset_within: Option<AccountDuration>,
    pub unknown_reset_retry: Option<AccountDuration>,
    pub spend_budget: Option<f64>,
    pub spend_budget_reset_at_ms: Option<i64>,
    pub paid_credits: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum AccountDuration {
    Seconds(u64),
    Text(String),
}

impl AccountDuration {
    pub fn seconds(&self) -> Result<u64, String> {
        match self {
            Self::Seconds(seconds) => Ok(*seconds),
            Self::Text(text) => {
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
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AccountPoolFloor {
    pub pool: Option<String>,
    pub window_minutes: Option<u32>,
    pub model: Option<String>,
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
    pub pin: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ConsumerPolicy {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload: Option<String>,
    pub service_tier: Option<crate::tier_settings::Tier>,
    pub service_tiers: Option<BTreeMap<String, crate::tier_settings::Tier>>,
    pub mode: Option<ConsumerMode>,
    /// Automatic selection when no route clears the adequacy floor. Defaults to
    /// refuse for mason and its dotted descendants, best_effort for others;
    /// explicit model pins are admitted without quality ranking.
    pub below_bar: Option<BelowBarPolicy>,
    pub ladder: Option<Vec<String>>,
    /// Internal provenance: only an operator-written ladder admits pin-only routes.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) ladder_source: Option<ConsumerLadderSource>,
    pub runner: Option<String>,
    /// Replaces the global runner list for this consumer and its dotted children, including when empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runners: Option<Vec<RunnerPolicy>>,
    pub owner: Option<String>,
    pub pay_per_token: Option<bool>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub pin: Option<SynthesizerPin>,
    pub projects: BTreeMap<String, ProjectPolicy>,
    pub iq: Option<f64>,
    pub eq: Option<f64>,
    pub context_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
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
