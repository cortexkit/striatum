//! Closed operator policy shapes. Consumer admission and panel selection read
//! these values from the same immutable snapshot as the model overrides.

use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

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
}

/// Per-consumer reasoning-variant overrides, grouped like `model_routing.exclude`.
/// Each key is a route spec in the exclusion grammar without negation
/// (`provider`, `provider/model`, `provider/model/variant`); each value is a
/// variant name the catalog publishes for the matched models.
#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct VariantGroups {
    pub all: BTreeMap<String, String>,
    pub mason: BTreeMap<String, String>,
    pub sidekick: BTreeMap<String, String>,
    pub athena: BTreeMap<String, String>,
    pub lab: BTreeMap<String, String>,
    pub core: BTreeMap<String, String>,
}

impl VariantGroups {
    /// Every group with its config name, `all` first.
    pub fn named(&self) -> [(&'static str, &BTreeMap<String, String>); 6] {
        [
            ("all", &self.all),
            ("mason", &self.mason),
            ("sidekick", &self.sidekick),
            ("athena", &self.athena),
            ("lab", &self.lab),
            ("core", &self.core),
        ]
    }

    pub fn is_empty(&self) -> bool {
        self.named().iter().all(|(_, group)| group.is_empty())
    }

    /// Shape checks that need no catalog: every key is a non-negated route spec
    /// and every value is a single non-empty word.
    pub fn validate(&self) -> Result<(), String> {
        for (group, entries) in self.named() {
            for (key, variant) in entries {
                let spec = prefrontal_routing_core::provider_model_spec::parse_spec(key);
                if key.trim() != key || spec.is_none() || spec.is_some_and(|spec| spec.negated) {
                    return Err(format!(
                        "model_routing.variants.{group}: key {key:?} must be provider, provider/model or provider/model/variant"
                    ));
                }
                if variant.is_empty() || variant.chars().any(char::is_whitespace) {
                    return Err(format!(
                        "model_routing.variants.{group}.{key}: variant {variant:?} must be one non-empty word"
                    ));
                }
            }
        }
        Ok(())
    }
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

/// One `routing/v1` consumer's policy (`model_routing.consumers.<id>`).
#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ConsumerPolicy {
    /// Answer this consumer only with routes priced per token, never with a
    /// route paid from subscription quota.
    pub priced_only: bool,
}

/// The `routing/v1` consumers served when the routing file names none:
/// `(id, priced_only)`. A consumer's exclusions are `model_routing.exclude.all`
/// plus the `exclude` group of the same name when one exists.
pub const DEFAULT_CONSUMERS: &[(&str, bool)] = &[
    ("mason", false),
    ("sidekick", false),
    ("athena", false),
    ("lab", false),
    ("core", false),
    // Codemode runs and flows pay per token: they are never answered with a
    // route paid from subscription quota.
    ("codemode", true),
    ("flow", true),
    // Magic Context owns these two consumers (its summary and dream work).
    // They take the default policy: no exclusions beyond `exclude.all`.
    ("mc_summary", false),
    ("mc_dream", false),
];

/// A runner module `routing/v1` answers with (`model_routing.runners[]`).
/// Listed in preference order: the first listed runner that can serve the
/// answered route is named.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunnerPolicy {
    /// The runner's module id. Only a route principal the daemon verified as
    /// this module may call the runner operations.
    #[schemars(regex(pattern = r"^\S+$"))]
    pub module: String,
    /// The routes it runs, in the exclusion grammar (`provider`,
    /// `provider/model`, `provider/model/variant`). Absent: every route it
    /// serves; for `broca`, the routes Broca's credentials serve, and for any
    /// other module every catalog route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routes: Option<Vec<String>>,
    /// Tool capability tags (such as `code.read/v1`) the runner can reach a
    /// tool provider for. A select that lists tool capabilities is answered
    /// only on a runner reaching every one of them.
    #[serde(default)]
    pub tool_capabilities: Vec<String>,
}

/// The runners served when the routing file names none: Broca, on the routes
/// its credentials serve.
pub fn default_runners() -> Vec<RunnerPolicy> {
    vec![RunnerPolicy {
        module: "broca".into(),
        routes: None,
        tool_capabilities: Vec::new(),
    }]
}
