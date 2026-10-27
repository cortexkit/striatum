//! Pure model resolution and capability compatibility policy.
//!
//! The vectors in this module are direct ports of the TypeScript model pipeline,
//! pin, fallback, alias, capability, compatibility, and speed-observation tests.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{make_capacity_key, make_route_key, UsageObservation};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FallbackEntry {
    pub providers: Vec<String>,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelResolutionIntent {
    #[serde(default)]
    pub ui_selected_model: Option<String>,
    #[serde(default)]
    pub user_model: Option<String>,
    #[serde(default)]
    pub user_fallback_models: Vec<String>,
    #[serde(default)]
    pub category_default_model: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelResolutionConstraints {
    #[serde(default)]
    pub available_models: Vec<String>,
    /// `None` means the host has not observed a provider catalog yet. An empty
    /// vector means the host observed that no providers are connected.
    #[serde(default)]
    pub connected_providers: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelResolutionPolicy {
    #[serde(default)]
    pub fallback_chain: Vec<FallbackEntry>,
    #[serde(default)]
    pub system_default_model: Option<String>,
    /// Preserves the old first-boot registration path without leaving a host-side
    /// fallback: the module chooses the first configured provider/model.
    #[serde(default)]
    pub use_first_fallback_when_unresolved: bool,
    #[serde(default)]
    pub require_any_fallback_model: bool,
    #[serde(default)]
    pub has_explicit_agent_config: bool,
    #[serde(default)]
    pub first_run_no_cache: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelResolutionRequest {
    #[serde(default)]
    pub intent: ModelResolutionIntent,
    pub constraints: ModelResolutionConstraints,
    #[serde(default)]
    pub policy: ModelResolutionPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelResolutionResult {
    pub model: String,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempted: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserModelPinRequest {
    #[serde(default)]
    pub user_model: Option<String>,
    #[serde(default)]
    pub user_variant: Option<String>,
    #[serde(default)]
    pub user_fallback_models: Vec<String>,
    #[serde(default)]
    pub available_models: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelegatedModelConfig {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelAliasResolution {
    pub requested_model_id: String,
    pub canonical_model_id: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotModalities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotLimit {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilitiesSnapshotEntry {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modalities: Option<SnapshotModalities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<SnapshotLimit>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilitiesSnapshot {
    #[serde(default)]
    pub models: HashMap<String, ModelCapabilitiesSnapshotEntry>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilitiesInput {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(default)]
    pub runtime_model: Option<Value>,
    #[serde(default)]
    pub runtime_snapshot: Option<ModelCapabilitiesSnapshot>,
    #[serde(default)]
    pub bundled_snapshot: Option<ModelCapabilitiesSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilities {
    pub requested_model_id: String,
    pub canonical_model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variants: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_efforts: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_thinking: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_temperature: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_top_p: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modalities: Option<SnapshotModalities>,
    pub diagnostics: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DesiredModelSettings {
    #[serde(default)]
    pub variant: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<f64>,
    #[serde(default)]
    pub thinking: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelSettingsCompatibilityChange {
    pub field: String,
    pub from: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelSettingsCompatibilityResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<Value>,
    pub changes: Vec<ModelSettingsCompatibilityChange>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUsageFacts {
    #[serde(rename = "messageID")]
    pub message_id: String,
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(rename = "modelID")]
    pub model_id: String,
    #[serde(default)]
    pub variant: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub input_tokens: Option<f64>,
    #[serde(default)]
    pub output_tokens: f64,
    #[serde(default)]
    pub reasoning_tokens: Option<f64>,
    #[serde(default)]
    pub cached_read_tokens: Option<f64>,
    #[serde(default)]
    pub cache_write_tokens: Option<f64>,
    #[serde(default)]
    pub workload_class: Option<String>,
    #[serde(default)]
    pub account_key: Option<String>,
    pub created_ms: f64,
    pub completed_ms: f64,
    /// Wall time the message spent inside tools: the UNION of its call
    /// intervals, which the producer computes because only it holds them.
    ///
    /// Deliberately NOT the older `toolDurationMs`, which carried the SUM of
    /// per-call durations under a name that read like elapsed time. Subtracting
    /// a sum overstated speed whenever calls overlapped and discarded the sample
    /// outright once the sum passed the message's own wall clock (Loop R143).
    /// The old spelling is not accepted: a pre-rename host sends no exclusion at
    /// all, so its samples understate speed for one restart window rather than
    /// overstating it indefinitely.
    #[serde(default)]
    pub tool_elapsed_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariantFact {
    pub provider_id: String,
    pub model_id: String,
    pub variant: Option<String>,
}

/// How providers spell the model ids they serve, and which model families
/// the capability heuristic recognizes by name. Operator config carries this
/// (`model_naming`, written by the config converter); routing core names no
/// provider and no family vocabulary of its own.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelNaming {
    /// For a gateway serving other vendors' models as `vendor/model`: the
    /// vendor of a bare model id, by the id's prefix. The first match wins.
    #[serde(default)]
    pub vendor_prefixes: Vec<VendorPrefix>,
    /// The rewrites each provider applies to a model id, in order. A provider
    /// with no entry serves ids as given.
    #[serde(default)]
    pub provider_rewrites: BTreeMap<String, Vec<ModelRewrite>>,
    /// Model families recognized by a substring of the normalized model id,
    /// each with the basic variants and no reasoning efforts. The first match
    /// wins, after the built-in Claude-style and GPT-style families.
    #[serde(default)]
    pub basic_families: Vec<BasicFamily>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VendorPrefix {
    pub prefix: String,
    pub vendor: String,
}

/// One step of a provider's model-id spelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelRewrite {
    /// Serve the id as `vendor/model`. An id that names its vendor keeps it; a
    /// bare id takes the vendor its prefix names (`vendor_prefixes`) or, when
    /// none does, is served exactly as given with no further rewrite.
    VendorPrefix,
    /// Spell a dashed two-part Claude-style version with a dot.
    DotVersion,
    /// Expand this bare alias to its `-preview` id.
    PreviewAlias(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicFamily {
    pub family: String,
    pub needles: Vec<String>,
}

impl ModelNaming {
    fn vendor_of(&self, model: &str) -> Option<&str> {
        self.vendor_prefixes
            .iter()
            .find(|entry| model.starts_with(&entry.prefix))
            .map(|entry| entry.vendor.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct FamilyDefinition {
    family: String,
    variants: &'static [&'static str],
    reasoning_efforts: &'static [&'static str],
    supports_thinking: Option<bool>,
}

const BASIC_VARIANTS: &[&str] = &["low", "medium", "high"];
const OPUS_VARIANTS: &[&str] = &["low", "medium", "high", "max"];
const GPT5_VARIANTS: &[&str] = &["low", "medium", "high", "xhigh"];
const OPENAI_REASONING: &[&str] = &["none", "minimal", "low", "medium", "high"];
const GPT5_REASONING: &[&str] = &["none", "minimal", "low", "medium", "high", "xhigh"];
const EMPTY: &[&str] = &[];

fn normalized_nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn normalize_model_name(value: &str) -> String {
    let lower = value.to_lowercase();
    for family in ["opus", "sonnet", "haiku"] {
        let prefix = format!("claude-{family}-");
        if let Some(index) = lower.find(&prefix) {
            let suffix = &lower[index + prefix.len()..];
            let mut pieces = suffix.splitn(3, ['-', '.']);
            if let (Some(major), Some(minor)) = (pieces.next(), pieces.next()) {
                if major.chars().all(|c| c.is_ascii_digit())
                    && minor.chars().all(|c| c.is_ascii_digit())
                {
                    let consumed = prefix.len() + major.len() + 1 + minor.len();
                    let tail = &lower[index + consumed..];
                    return format!("{}{}{}.{}{}", &lower[..index], prefix, major, minor, tail);
                }
            }
        }
    }
    lower
}

pub fn fuzzy_match_model(
    target: &str,
    available: &[String],
    providers: &[String],
) -> Option<String> {
    if available.is_empty() {
        return None;
    }
    let provider_set = providers.iter().collect::<HashSet<_>>();
    let target_normalized = normalize_model_name(target);
    let matches = available
        .iter()
        .filter(|model| {
            if provider_set.is_empty() {
                return true;
            }
            model.split_once('/').is_some_and(|(provider, _)| {
                provider_set.iter().any(|value| value.as_str() == provider)
            })
        })
        .filter(|model| normalize_model_name(model).contains(&target_normalized))
        .collect::<Vec<_>>();
    if let Some(exact) = matches
        .iter()
        .find(|model| normalize_model_name(model) == target_normalized)
    {
        return Some((**exact).clone());
    }
    let exact_model_ids = matches
        .iter()
        .copied()
        .filter(|model| {
            let model_id = model
                .split_once('/')
                .map_or(model.as_str(), |(_, model_id)| model_id);
            normalize_model_name(model_id) == target_normalized
        })
        .collect::<Vec<_>>();
    let candidates = if exact_model_ids.is_empty() {
        matches
    } else {
        exact_model_ids
    };
    candidates
        .into_iter()
        .min_by_key(|model| model.len())
        .map(|model| model.to_string())
}

fn claude_version_dot(model: &str) -> String {
    normalize_model_name(model)
}

/// Expand a bare Gemini alias to its preview id. An alias already followed by
/// `-` (its own `-preview`, or a tier suffix) is a canonical id and stays as it
/// is: the unconditional replace produced `gemini-3.1-pro-preview-preview` for
/// a caller supplying the canonical id (Loop R183), a name no provider serves.
/// The TypeScript twin keeps the same rule with `(?!-)`.
fn expand_gemini_alias(model: &str, alias: &str) -> String {
    let mut out = String::with_capacity(model.len() + 8);
    let mut rest = model;
    while let Some(at) = rest.find(alias) {
        let after = &rest[at + alias.len()..];
        out.push_str(&rest[..at + alias.len()]);
        if !after.starts_with('-') {
            out.push_str("-preview");
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

/// A model id as `provider` serves it, by the provider's configured rewrites.
pub fn transform_model_for_provider(naming: &ModelNaming, provider: &str, model: &str) -> String {
    let Some(rewrites) = naming.provider_rewrites.get(provider) else {
        return model.to_string();
    };
    let mut vendor = None;
    let mut spelled = model.to_string();
    if rewrites.contains(&ModelRewrite::VendorPrefix) {
        if let Some((named, rest)) = model.split_once('/') {
            vendor = Some(named.to_string());
            spelled = rest.to_string();
        } else if let Some(inferred) = naming.vendor_of(model) {
            vendor = Some(inferred.to_string());
        } else {
            return model.to_string();
        }
    }
    for rewrite in rewrites {
        spelled = match rewrite {
            ModelRewrite::VendorPrefix => spelled,
            ModelRewrite::DotVersion => claude_version_dot(&spelled),
            ModelRewrite::PreviewAlias(alias) => expand_gemini_alias(&spelled, alias),
        };
    }
    match vendor {
        Some(vendor) => format!("{vendor}/{spelled}"),
        None => spelled,
    }
}

fn first_fallback(
    naming: &ModelNaming,
    chain: &[FallbackEntry],
    attempted: Vec<String>,
) -> Option<ModelResolutionResult> {
    let entry = chain.first()?;
    let provider = entry.providers.first()?;
    Some(ModelResolutionResult {
        model: format!(
            "{provider}/{}",
            transform_model_for_provider(naming, provider, &entry.model)
        ),
        provenance: "provider-fallback".to_string(),
        variant: entry.variant.clone(),
        attempted,
        reason: None,
    })
}

pub fn resolve_model_pipeline(
    request: &ModelResolutionRequest,
    naming: &ModelNaming,
) -> Option<ModelResolutionResult> {
    let mut attempted = Vec::new();
    if let Some(model) = normalized_nonempty(request.intent.ui_selected_model.as_deref()) {
        return Some(ModelResolutionResult {
            model,
            provenance: "override".into(),
            variant: None,
            attempted,
            reason: None,
        });
    }
    if let Some(model) = normalized_nonempty(request.intent.user_model.as_deref()) {
        return Some(ModelResolutionResult {
            model,
            provenance: "override".into(),
            variant: None,
            attempted,
            reason: None,
        });
    }
    let available = &request.constraints.available_models;
    if request.policy.require_any_fallback_model
        && !request.policy.has_explicit_agent_config
        && !request.policy.first_run_no_cache
    {
        let has_available = request.policy.fallback_chain.iter().any(|entry| {
            entry.providers.iter().any(|provider| {
                fuzzy_match_model(&entry.model, available, std::slice::from_ref(provider)).is_some()
                    || request
                        .constraints
                        .connected_providers
                        .as_ref()
                        .is_some_and(|providers| providers.iter().any(|value| value == provider))
            })
        });
        if !has_available {
            return None;
        }
    }
    if let Some(category) = normalized_nonempty(request.intent.category_default_model.as_deref()) {
        attempted.push(category.clone());
        if !available.is_empty() {
            let providers = category
                .split_once('/')
                .map(|(provider, _)| vec![provider.to_string()])
                .unwrap_or_default();
            if let Some(model) = fuzzy_match_model(&category, available, &providers) {
                return Some(ModelResolutionResult {
                    model,
                    provenance: "category-default".into(),
                    variant: None,
                    attempted,
                    reason: None,
                });
            }
        } else if let Some(connected) = request.constraints.connected_providers.as_ref() {
            if let Some((provider, model)) = category.split_once('/') {
                if connected.iter().any(|candidate| candidate == provider) {
                    return Some(ModelResolutionResult {
                        model: format!(
                            "{provider}/{}",
                            transform_model_for_provider(naming, provider, model)
                        ),
                        provenance: "category-default".into(),
                        variant: None,
                        attempted,
                        reason: None,
                    });
                }
            }
        } else {
            return Some(ModelResolutionResult {
                model: category,
                provenance: "category-default".into(),
                variant: None,
                attempted,
                reason: None,
            });
        }
    }
    if !request.intent.user_fallback_models.is_empty() {
        if available.is_empty() {
            if let Some(connected) = request.constraints.connected_providers.as_ref() {
                for model in &request.intent.user_fallback_models {
                    attempted.push(model.clone());
                    if let Some((provider, model_id)) = model.split_once('/') {
                        if connected.iter().any(|candidate| candidate == provider) {
                            return Some(ModelResolutionResult {
                                model: format!(
                                    "{provider}/{}",
                                    transform_model_for_provider(naming, provider, model_id)
                                ),
                                provenance: "provider-fallback".into(),
                                variant: None,
                                attempted,
                                reason: None,
                            });
                        }
                    }
                }
            }
        } else {
            for model in &request.intent.user_fallback_models {
                attempted.push(model.clone());
                let providers = model
                    .split_once('/')
                    .map(|(provider, _)| vec![provider.to_string()])
                    .unwrap_or_default();
                if let Some(model) = fuzzy_match_model(model, available, &providers) {
                    return Some(ModelResolutionResult {
                        model,
                        provenance: "provider-fallback".into(),
                        variant: None,
                        attempted,
                        reason: None,
                    });
                }
            }
        }
    }
    if !request.policy.fallback_chain.is_empty() {
        if available.is_empty() {
            if let Some(connected) = request.constraints.connected_providers.as_ref() {
                for entry in &request.policy.fallback_chain {
                    for provider in &entry.providers {
                        if connected.iter().any(|candidate| candidate == provider) {
                            return Some(ModelResolutionResult {
                                model: format!(
                                    "{provider}/{}",
                                    transform_model_for_provider(naming, provider, &entry.model)
                                ),
                                provenance: "provider-fallback".into(),
                                variant: entry.variant.clone(),
                                attempted,
                                reason: None,
                            });
                        }
                    }
                }
            }
        } else {
            for entry in &request.policy.fallback_chain {
                for provider in &entry.providers {
                    let full_model = format!("{provider}/{}", entry.model);
                    if let Some(model) =
                        fuzzy_match_model(&full_model, available, std::slice::from_ref(provider))
                    {
                        return Some(ModelResolutionResult {
                            model,
                            provenance: "provider-fallback".into(),
                            variant: entry.variant.clone(),
                            attempted,
                            reason: None,
                        });
                    }
                }
                if let Some(model) = fuzzy_match_model(&entry.model, available, &[]) {
                    return Some(ModelResolutionResult {
                        model,
                        provenance: "provider-fallback".into(),
                        variant: entry.variant.clone(),
                        attempted,
                        reason: None,
                    });
                }
            }
        }
    }
    if request.policy.use_first_fallback_when_unresolved {
        if let Some(result) =
            first_fallback(naming, &request.policy.fallback_chain, attempted.clone())
        {
            return Some(result);
        }
    }
    request
        .policy
        .system_default_model
        .as_ref()
        .map(|model| ModelResolutionResult {
            model: model.clone(),
            provenance: "system-default".into(),
            variant: None,
            attempted,
            reason: None,
        })
}

fn parse_inline_variant(model: &str) -> (String, Option<String>) {
    let trimmed = model.trim();
    if trimmed.ends_with(')') {
        if let Some(open) = trimmed.rfind('(') {
            let base = trimmed[..open].trim();
            let variant = trimmed[open + 1..trimmed.len() - 1].trim();
            if !base.is_empty() && !variant.is_empty() {
                return (base.to_string(), Some(variant.to_string()));
            }
        }
    }
    if let Some((base, suffix)) = trimmed.rsplit_once(char::is_whitespace) {
        let normalized = suffix.to_lowercase();
        if [
            "low", "medium", "high", "xhigh", "max", "minimal", "none", "auto", "thinking",
        ]
        .contains(&normalized.as_str())
        {
            return (base.trim().to_string(), Some(normalized));
        }
    }
    (trimmed.to_string(), None)
}

fn parsed_model_candidate(model: &str) -> Option<(String, Vec<String>, Option<String>)> {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (base, variant) = parse_inline_variant(trimmed);
    if let Some((provider, model_id)) = base.split_once('/') {
        if provider.trim().is_empty() || model_id.trim().is_empty() {
            return None;
        }
        return Some((
            format!("{}/{}", provider.trim(), model_id.trim()),
            vec![provider.trim().to_string()],
            variant,
        ));
    }
    Some((base, Vec::new(), variant))
}

fn to_model_config(model: &str, variant: Option<String>) -> Option<DelegatedModelConfig> {
    let (provider, model_id) = model.split_once('/')?;
    Some(DelegatedModelConfig {
        provider_id: provider.to_string(),
        model_id: model_id.to_string(),
        variant,
    })
}

pub fn resolve_user_model_pin(input: &UserModelPinRequest) -> Option<DelegatedModelConfig> {
    if let Some(user_model) = normalized_nonempty(input.user_model.as_deref()) {
        let (base_model, providers, inline_variant) = parsed_model_candidate(&user_model)?;
        if !providers.is_empty() {
            let variant = input.user_variant.clone().or(inline_variant);
            // An empty catalog is the cold-start path: the explicit pin is trusted
            // as spelled because nothing can be checked against. A warm catalog
            // answers with the id the matcher found, the way the fallback loop
            // below does; answering with the pin as spelled while only a sibling
            // matched (gpt-5.4 against gpt-5.4-mini) forwarded an id the host
            // does not serve and skipped the configured fallback.
            if input.available_models.is_empty() {
                return to_model_config(&base_model, variant);
            }
            if let Some(model) = fuzzy_match_model(&base_model, &input.available_models, &providers)
            {
                return to_model_config(&model, variant);
            }
        }
    }
    if input.available_models.is_empty() {
        return None;
    }
    for fallback in &input.user_fallback_models {
        let Some((base_model, providers, inline_variant)) = parsed_model_candidate(fallback) else {
            continue;
        };
        let Some(model) = fuzzy_match_model(&base_model, &input.available_models, &providers)
        else {
            continue;
        };
        return to_model_config(&model, input.user_variant.clone().or(inline_variant));
    }
    None
}

pub fn find_most_specific_fallback_entry(
    provider_id: &str,
    model_id: &str,
    chain: &[FallbackEntry],
) -> Option<FallbackEntry> {
    let resolved = format!("{provider_id}/{model_id}").to_lowercase();
    chain
        .iter()
        .filter_map(|entry| {
            entry
                .providers
                .iter()
                .map(|provider| format!("{provider}/{}", entry.model).to_lowercase())
                .filter(|candidate| resolved.starts_with(candidate))
                .map(|candidate| candidate.len())
                .max()
                .map(|len| (len, entry))
        })
        .max_by_key(|(len, _)| *len)
        .map(|(_, entry)| entry.clone())
}

pub fn resolve_model_id_alias(model_id: &str) -> ModelAliasResolution {
    let requested = model_id.trim().to_lowercase();
    let lookup = requested
        .split_once('/')
        .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
        .map_or(requested.as_str(), |(_, model)| model);
    let (canonical, source, rule) = match lookup {
        "gemini-3-pro-high" | "gemini-3-pro-low" => (
            "gemini-3-pro-preview",
            "exact-alias",
            Some("gemini-3-pro-tier-alias"),
        ),
        "claude-opus-4-6-thinking" | "claude-opus-4-7-thinking" => (
            "claude-opus-4-7",
            "pattern-alias",
            Some("claude-thinking-legacy-alias"),
        ),
        "gemini-3.1-pro-high" | "gemini-3.1-pro-low" => (
            "gemini-3.1-pro",
            "pattern-alias",
            Some("gemini-3.1-pro-tier-alias"),
        ),
        _ => (lookup, "canonical", None),
    };
    let canonical_model_id = canonical.to_string();
    ModelAliasResolution {
        requested_model_id: requested,
        canonical_model_id,
        source: source.to_string(),
        rule_id: rule.map(str::to_string),
    }
}

fn heuristic_family(naming: &ModelNaming, model_id: &str) -> Option<FamilyDefinition> {
    let normalized = normalize_model_name(model_id);
    let definition = if normalized.contains("claude") && normalized.contains("opus") {
        FamilyDefinition {
            family: "claude-opus".into(),
            variants: OPUS_VARIANTS,
            reasoning_efforts: EMPTY,
            supports_thinking: Some(true),
        }
    } else if normalized.contains("claude") {
        FamilyDefinition {
            family: "claude-non-opus".into(),
            variants: BASIC_VARIANTS,
            reasoning_efforts: EMPTY,
            supports_thinking: Some(true),
        }
    } else if normalized
        .split_once('/')
        .map_or(normalized.as_str(), |(_, model)| model)
        .starts_with('o')
        && normalized
            .split_once('/')
            .map_or(normalized.as_str(), |(_, model)| model)
            .chars()
            .nth(1)
            .is_some_and(|value| value.is_ascii_digit())
    {
        FamilyDefinition {
            family: "openai-reasoning".into(),
            variants: BASIC_VARIANTS,
            reasoning_efforts: OPENAI_REASONING,
            supports_thinking: None,
        }
    } else if normalized.contains("gpt-5") {
        FamilyDefinition {
            family: "gpt-5".into(),
            variants: GPT5_VARIANTS,
            reasoning_efforts: GPT5_REASONING,
            supports_thinking: None,
        }
    } else if normalized.contains("gpt") {
        FamilyDefinition {
            family: "gpt-legacy".into(),
            variants: BASIC_VARIANTS,
            reasoning_efforts: EMPTY,
            supports_thinking: None,
        }
    } else {
        naming
            .basic_families
            .iter()
            .find(|family| {
                family
                    .needles
                    .iter()
                    .any(|needle| normalized.contains(needle.as_str()))
            })
            .map(|family| FamilyDefinition {
                family: family.family.clone(),
                variants: BASIC_VARIANTS,
                reasoning_efforts: EMPTY,
                supports_thinking: None,
            })?
    };
    Some(definition)
}

fn object(value: Option<&Value>) -> Option<&Map<String, Value>> {
    value?.as_object()
}

fn bool_at(root: Option<&Map<String, Value>>, keys: &[&str]) -> Option<bool> {
    let capabilities = root.and_then(|value| object(value.get("capabilities")));
    keys.iter().find_map(|key| {
        root.and_then(|value| value.get(*key))
            .and_then(Value::as_bool)
            .or_else(|| {
                capabilities
                    .and_then(|value| value.get(*key))
                    .and_then(Value::as_bool)
            })
    })
}

fn string_array_or_keys(value: Option<&Value>) -> Option<Vec<String>> {
    let value = value?;
    let entries = if let Some(array) = value.as_array() {
        array
            .iter()
            .filter_map(Value::as_str)
            .map(|entry| entry.to_lowercase())
            .collect::<Vec<_>>()
    } else if let Some(object) = value.as_object() {
        object.keys().map(|entry| entry.to_lowercase()).collect()
    } else {
        Vec::new()
    };
    (!entries.is_empty()).then_some(entries)
}

fn runtime_variants(root: Option<&Map<String, Value>>) -> Option<Vec<String>> {
    string_array_or_keys(root.and_then(|value| value.get("variants"))).or_else(|| {
        let capabilities = root.and_then(|value| object(value.get("capabilities")));
        string_array_or_keys(capabilities.and_then(|value| value.get("variants")))
    })
}

fn modality_keys(value: Option<&Value>) -> Option<Vec<String>> {
    let value = value?;
    let entries = if let Some(array) = value.as_array() {
        array
            .iter()
            .filter_map(Value::as_str)
            .map(|entry| entry.to_lowercase())
            .collect::<Vec<_>>()
    } else if let Some(object) = value.as_object() {
        object
            .iter()
            .filter(|(_, enabled)| enabled.as_bool() == Some(true))
            .map(|(key, _)| key.to_lowercase())
            .collect()
    } else {
        Vec::new()
    };
    (!entries.is_empty()).then_some(entries)
}

fn modalities_at(value: Option<&Value>) -> Option<SnapshotModalities> {
    let value = object(value)?;
    let input = modality_keys(value.get("input"));
    let output = modality_keys(value.get("output"));
    (input.is_some() || output.is_some()).then_some(SnapshotModalities { input, output })
}

fn runtime_modalities(root: Option<&Map<String, Value>>) -> Option<SnapshotModalities> {
    modalities_at(root.and_then(|value| value.get("modalities"))).or_else(|| {
        let capabilities = root.and_then(|value| object(value.get("capabilities")));
        modalities_at(capabilities.and_then(|value| value.get("modalities"))).or_else(|| {
            capabilities
                .map(|value| Value::Object(value.clone()))
                .and_then(|value| modalities_at(Some(&value)))
        })
    })
}

fn number_at(root: Option<&Map<String, Value>>, location: &str, key: &str) -> Option<f64> {
    let direct = root.and_then(|value| object(value.get(location)));
    let capabilities = root.and_then(|value| object(value.get("capabilities")));
    let nested = capabilities.and_then(|value| object(value.get(location)));
    direct
        .and_then(|value| value.get(key))
        .and_then(Value::as_f64)
        .or_else(|| {
            nested
                .and_then(|value| value.get(key))
                .and_then(Value::as_f64)
        })
}

pub fn resolve_model_capabilities(
    input: &ModelCapabilitiesInput,
    naming: &ModelNaming,
) -> ModelCapabilities {
    let alias = resolve_model_id_alias(&input.model_id);
    let runtime_root = object(input.runtime_model.as_ref());
    let runtime_entry = input
        .runtime_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.models.get(&alias.canonical_model_id));
    let bundled_entry = input
        .bundled_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.models.get(&alias.canonical_model_id));
    let snapshot_entry = runtime_entry.or(bundled_entry);
    let snapshot_source = if runtime_entry.is_some() {
        "runtime-snapshot"
    } else if bundled_entry.is_some() {
        "bundled-snapshot"
    } else {
        "none"
    };
    let heuristic = heuristic_family(naming, &alias.canonical_model_id);
    let runtime_variants = runtime_variants(runtime_root);
    let runtime_reasoning = bool_at(runtime_root, &["reasoning"]);
    let runtime_thinking =
        runtime_reasoning.or_else(|| bool_at(runtime_root, &["thinking", "supportsThinking"]));
    let runtime_temperature = bool_at(runtime_root, &["temperature"]);
    let runtime_top_p = bool_at(runtime_root, &["topP", "top_p"]);
    let runtime_output = number_at(runtime_root, "limit", "output");
    let runtime_context = number_at(runtime_root, "limit", "context").or_else(|| {
        runtime_root
            .and_then(|value| value.get("context"))
            .and_then(Value::as_f64)
    });
    let runtime_tool_call = bool_at(runtime_root, &["toolCall", "tool_call", "toolcall"]);
    let runtime_modalities = runtime_modalities(runtime_root);
    let family = snapshot_entry
        .and_then(|entry| entry.family.clone())
        .or_else(|| {
            heuristic
                .as_ref()
                .map(|definition| definition.family.to_string())
        });
    let variants = runtime_variants.clone().or_else(|| {
        heuristic.as_ref().map(|definition| {
            definition
                .variants
                .iter()
                .map(|value| (*value).to_string())
                .collect()
        })
    });
    let reasoning_efforts = heuristic.as_ref().and_then(|definition| {
        (!definition.reasoning_efforts.is_empty()).then(|| {
            definition
                .reasoning_efforts
                .iter()
                .map(|value| (*value).to_string())
                .collect()
        })
    });
    let reasoning = runtime_reasoning.or_else(|| snapshot_entry.and_then(|entry| entry.reasoning));
    let supports_thinking = heuristic
        .as_ref()
        .and_then(|definition| definition.supports_thinking)
        .or(runtime_thinking)
        .or_else(|| snapshot_entry.and_then(|entry| entry.reasoning));
    let supports_temperature =
        runtime_temperature.or_else(|| snapshot_entry.and_then(|entry| entry.temperature));
    let supports_top_p = runtime_top_p;
    let max_output_tokens =
        runtime_output.or_else(|| snapshot_entry.and_then(|entry| entry.limit.as_ref()?.output));
    let context_window =
        runtime_context.or_else(|| snapshot_entry.and_then(|entry| entry.limit.as_ref()?.context));
    let tool_call = runtime_tool_call.or_else(|| snapshot_entry.and_then(|entry| entry.tool_call));
    let modalities = runtime_modalities
        .clone()
        .or_else(|| snapshot_entry.and_then(|entry| entry.modalities.clone()));
    let resolution_mode = if snapshot_source != "none" && alias.source == "canonical" {
        "snapshot-backed"
    } else if snapshot_source != "none" {
        "alias-backed"
    } else if heuristic.is_some() {
        "heuristic-backed"
    } else {
        "unknown"
    };
    let has_reasoning_efforts = reasoning_efforts.is_some();
    ModelCapabilities {
        requested_model_id: alias.requested_model_id,
        canonical_model_id: alias.canonical_model_id,
        family,
        variants,
        reasoning_efforts,
        reasoning,
        supports_thinking,
        supports_temperature,
        supports_top_p,
        max_output_tokens,
        context_window,
        tool_call,
        modalities,
        diagnostics: json!({
            "resolutionMode": resolution_mode,
            "canonicalization": {
                "source": alias.source,
                "ruleID": alias.rule_id,
            },
            "snapshot": { "source": snapshot_source },
            "family": { "source": if snapshot_entry.and_then(|entry| entry.family.as_ref()).is_some() { "snapshot" } else if heuristic.is_some() { "heuristic" } else { "none" } },
            "variants": { "source": if runtime_variants.is_some() { "runtime" } else if heuristic.as_ref().is_some_and(|definition| !definition.variants.is_empty()) { "heuristic" } else { "none" } },
            "reasoningEfforts": { "source": if has_reasoning_efforts { "heuristic" } else { "none" } },
            "reasoning": { "source": if runtime_reasoning.is_some() { "runtime" } else if snapshot_entry.and_then(|entry| entry.reasoning).is_some() { snapshot_source } else { "none" } },
            "supportsThinking": { "source": if heuristic.as_ref().and_then(|definition| definition.supports_thinking).is_some() { "heuristic" } else if runtime_thinking.is_some() { "runtime" } else if snapshot_entry.and_then(|entry| entry.reasoning).is_some() { snapshot_source } else { "none" } },
            "supportsTemperature": { "source": if runtime_temperature.is_some() { "runtime" } else if snapshot_entry.and_then(|entry| entry.temperature).is_some() { snapshot_source } else { "none" } },
            "supportsTopP": { "source": if runtime_top_p.is_some() { "runtime" } else { "none" } },
            "maxOutputTokens": { "source": if runtime_output.is_some() { "runtime" } else if snapshot_entry.and_then(|entry| entry.limit.as_ref()?.output).is_some() { snapshot_source } else { "none" } },
            "contextWindow": { "source": if runtime_context.is_some() { "runtime" } else if snapshot_entry.and_then(|entry| entry.limit.as_ref()?.context).is_some() { snapshot_source } else { "none" } },
            "toolCall": { "source": if runtime_tool_call.is_some() { "runtime" } else if snapshot_entry.and_then(|entry| entry.tool_call).is_some() { snapshot_source } else { "none" } },
            "modalities": { "source": if runtime_modalities.is_some() { "runtime" } else if snapshot_entry.and_then(|entry| entry.modalities.as_ref()).is_some() { snapshot_source } else { "none" } },
        }),
    }
}

fn downgrade(value: &str, allowed: &[String], ladder: &[&str]) -> Option<String> {
    let requested = ladder.iter().position(|candidate| *candidate == value)?;
    (0..=requested).rev().find_map(|index| {
        allowed
            .iter()
            .any(|item| item == ladder[index])
            .then(|| ladder[index].to_string())
    })
}

fn resolve_compat_field(
    value: &str,
    family_caps: Option<&[String]>,
    ladder: &[&str],
    family_known: bool,
    metadata: Option<&[String]>,
) -> (Option<String>, Option<&'static str>) {
    if let Some(metadata) = metadata {
        return if metadata.iter().any(|candidate| candidate == value) {
            (Some(value.to_string()), None)
        } else {
            (
                downgrade(value, metadata, ladder),
                Some("unsupported-by-model-metadata"),
            )
        };
    }
    if let Some(family_caps) = family_caps {
        return if family_caps.iter().any(|candidate| candidate == value) {
            (Some(value.to_string()), None)
        } else {
            (
                downgrade(value, family_caps, ladder),
                Some("unsupported-by-model-family"),
            )
        };
    }
    if family_known {
        (None, Some("unsupported-by-model-family"))
    } else {
        (None, Some("unknown-model-family"))
    }
}

pub fn resolve_compatible_model_settings(
    model_id: &str,
    desired: &DesiredModelSettings,
    capabilities: Option<&ModelCapabilities>,
    naming: &ModelNaming,
) -> ModelSettingsCompatibilityResult {
    let heuristic = heuristic_family(naming, model_id);
    let family_known = heuristic.is_some()
        || capabilities
            .and_then(|value| value.family.as_ref())
            .is_some();
    let family_variants = heuristic.as_ref().map(|definition| {
        definition
            .variants
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>()
    });
    let family_reasoning = heuristic.as_ref().and_then(|definition| {
        (!definition.reasoning_efforts.is_empty()).then(|| {
            definition
                .reasoning_efforts
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>()
        })
    });
    let mut changes = Vec::new();
    let variant = desired.variant.as_ref().and_then(|original| {
        let normalized = original.to_lowercase();
        let (resolved, reason) = resolve_compat_field(
            &normalized,
            family_variants.as_deref(),
            &["low", "medium", "high", "xhigh", "max"],
            family_known,
            capabilities.and_then(|value| value.variants.as_deref()),
        );
        if resolved.as_deref() != Some(normalized.as_str()) {
            if let Some(reason) = reason {
                changes.push(ModelSettingsCompatibilityChange {
                    field: "variant".into(),
                    from: original.clone(),
                    to: resolved.clone(),
                    reason: reason.into(),
                });
            }
        }
        resolved
    });
    let reasoning_effort = desired.reasoning_effort.as_ref().and_then(|original| {
        let normalized = original.to_lowercase();
        let metadata = capabilities.and_then(|value| value.reasoning_efforts.as_deref());
        let (resolved, reason) = resolve_compat_field(
            &normalized,
            family_reasoning.as_deref(),
            &["none", "minimal", "low", "medium", "high", "xhigh"],
            family_known,
            metadata,
        );
        if resolved.as_deref() != Some(normalized.as_str()) {
            if let Some(reason) = reason {
                changes.push(ModelSettingsCompatibilityChange {
                    field: "reasoningEffort".into(),
                    from: original.clone(),
                    to: resolved.clone(),
                    reason: reason.into(),
                });
            }
        }
        resolved
    });
    let mut temperature = desired.temperature;
    if temperature.is_some()
        && capabilities.and_then(|value| value.supports_temperature) == Some(false)
    {
        changes.push(ModelSettingsCompatibilityChange {
            field: "temperature".into(),
            from: js_number(temperature.unwrap()),
            to: None,
            reason: "unsupported-by-model-metadata".into(),
        });
        temperature = None;
    }
    let mut top_p = desired.top_p;
    if top_p.is_some() && capabilities.and_then(|value| value.supports_top_p) == Some(false) {
        changes.push(ModelSettingsCompatibilityChange {
            field: "topP".into(),
            from: js_number(top_p.unwrap()),
            to: None,
            reason: "unsupported-by-model-metadata".into(),
        });
        top_p = None;
    }
    let mut max_tokens = desired.max_tokens;
    if let (Some(requested), Some(limit)) = (
        max_tokens,
        capabilities.and_then(|value| value.max_output_tokens),
    ) {
        if limit > 0.0 && requested > limit {
            changes.push(ModelSettingsCompatibilityChange {
                field: "maxTokens".into(),
                from: js_number(requested),
                to: Some(js_number(limit)),
                reason: "max-output-limit".into(),
            });
            max_tokens = Some(limit);
        }
    }
    let mut thinking = desired.thinking.clone();
    if thinking.is_some() && capabilities.and_then(|value| value.supports_thinking) == Some(false) {
        changes.push(ModelSettingsCompatibilityChange {
            field: "thinking".into(),
            from: serde_json::to_string(thinking.as_ref().expect("checked")).unwrap_or_default(),
            to: None,
            reason: "unsupported-by-model-metadata".into(),
        });
        thinking = None;
    }
    ModelSettingsCompatibilityResult {
        variant,
        reasoning_effort,
        temperature,
        top_p,
        max_tokens,
        thinking,
        changes,
    }
}

fn js_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn resolved_usage_route(
    facts: &NativeUsageFacts,
    variants: &[VariantFact],
) -> Option<(String, String, String)> {
    let message_id = facts.message_id.trim();
    let provider_id = facts.provider_id.trim();
    let model_id = facts.model_id.trim();
    if message_id.is_empty() || provider_id.is_empty() || model_id.is_empty() {
        return None;
    }

    let capacity_key = make_capacity_key(provider_id, model_id);
    let mut variant = normalized_nonempty(facts.variant.as_deref());
    if variant.is_none() {
        let observed = variants
            .iter()
            .filter(|fact| make_capacity_key(&fact.provider_id, &fact.model_id) == capacity_key)
            .map(|fact| fact.variant.clone().unwrap_or_default())
            .collect::<HashSet<_>>();
        if observed.len() > 1 {
            return None;
        }
        variant = observed
            .into_iter()
            .next()
            .filter(|value| !value.is_empty());
    }

    Some((
        message_id.to_string(),
        make_route_key(provider_id, model_id, variant.as_deref()),
        capacity_key,
    ))
}

fn usage_duration_ms(facts: &NativeUsageFacts) -> f64 {
    let wall_duration = facts.completed_ms - facts.created_ms;
    let tool_elapsed = facts.tool_elapsed_ms.unwrap_or(0.0);
    let duration_ms = wall_duration - tool_elapsed;
    if !wall_duration.is_finite()
        || !tool_elapsed.is_finite()
        || tool_elapsed < 0.0
        || !duration_ms.is_finite()
    {
        f64::NAN
    } else {
        duration_ms
    }
}

fn combined_output_tokens(facts: &NativeUsageFacts) -> f64 {
    let output_tokens = facts.output_tokens.floor();
    if !facts.output_tokens.is_finite() || facts.output_tokens <= 0.0 || output_tokens <= 0.0 {
        return facts.output_tokens;
    }
    output_tokens + facts.reasoning_tokens.unwrap_or(0.0).floor().max(0.0)
}

/// Build the durable row for every storable usage delivery. Speed eligibility is
/// deliberately separate: invalid measurements remain NULL-able evidence instead
/// of preventing the rest of the usage profile from being recorded.
pub const VALID_WORKLOAD_CLASSES: &[&str] = &["ephemeral", "head", "gather", "hire"];

pub fn parse_workload_class(value: &str) -> Option<&'static str> {
    let trimmed = value.trim();
    VALID_WORKLOAD_CLASSES
        .iter()
        .copied()
        .find(|&class| class.eq_ignore_ascii_case(trimmed))
}

pub const COUNTERS_PRODUCER: bool = true;
pub const CLASS_PRODUCER: bool = true;
pub const ACCOUNT_PRODUCER: bool = false;

pub fn map_agent_to_workload_class(agent: &str) -> Option<&'static str> {
    let trimmed = agent.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_lowercase();
    if lower == "mason" {
        return Some("ephemeral");
    }
    if lower == "alfonso" {
        return Some("head");
    }
    if lower == "explore" || lower == "librarian" || lower.starts_with("sidekick") {
        return Some("gather");
    }
    if lower.starts_with("hire_") || lower.starts_with("hire-") || lower == "hire" {
        return Some("hire");
    }
    None
}

/// use_basket may be true only when counters_producer and class_producer are both true.
/// account_producer never enters that predicate.
pub fn use_basket_permitted(counters_producer: bool, class_producer: bool) -> bool {
    counters_producer && class_producer
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BrocaUsageFacts {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    #[serde(default)]
    pub retries_used: u64,
}

impl NativeUsageFacts {
    /// Copies BrocaUsage into NativeUsageFacts:
    /// - cached_read = cached_input_tokens
    /// - cache_write = cache_write_tokens
    /// - reasoning = reasoning_tokens
    /// - fresh_input = input_tokens (Broca already reports fresh, non-cached input)
    /// - output_tokens = output_tokens - reasoning_tokens
    /// - absence of Broca usage is whole-object: None clears all five counters
    ///
    /// The output conversion is the load-bearing line. Broca's canonical
    /// `output_tokens` INCLUDES reasoning (broca-protocol `Usage::reasoning_tokens`:
    /// "ALWAYS A SUBSET of `output_tokens`"), while `NativeUsageFacts` follows the
    /// OpenCode message shape, where `tokens.output` is visible output and
    /// `tokens.reasoning` sits beside it. [`build_usage_observation`] adds the two
    /// back together and the class vector subtracts reasoning again, so copying
    /// Broca's total whole counted every reasoning token twice in the basket
    /// (once inside output, once as reasoning).
    pub fn apply_broca_usage(&mut self, usage: Option<&BrocaUsageFacts>) {
        if let Some(u) = usage {
            self.cached_read_tokens = Some(u.cached_input_tokens as f64);
            self.cache_write_tokens = Some(u.cache_write_tokens as f64);
            self.reasoning_tokens = Some(u.reasoning_tokens as f64);
            self.input_tokens = Some(u.input_tokens as f64);
            self.output_tokens = u.output_tokens.saturating_sub(u.reasoning_tokens) as f64;
        } else {
            self.cached_read_tokens = None;
            self.cache_write_tokens = None;
            self.reasoning_tokens = None;
            self.input_tokens = None;
            self.output_tokens = 0.0;
        }
    }
}

pub fn build_usage_observation(
    facts: &NativeUsageFacts,
    variants: &[VariantFact],
) -> Option<UsageObservation> {
    let (message_id, route_key, capacity_key) = resolved_usage_route(facts, variants)?;
    Some(UsageObservation {
        id: message_id,
        route_key,
        capacity_key,
        provider_id: facts.provider_id.trim().to_string(),
        input_tokens: facts.input_tokens,
        output_tokens: combined_output_tokens(facts),
        reasoning_tokens: facts.reasoning_tokens,
        cached_read_tokens: facts.cached_read_tokens,
        cache_write_tokens: facts.cache_write_tokens,
        duration_ms: usage_duration_ms(facts),
        workload_class: facts
            .workload_class
            .as_deref()
            .and_then(parse_workload_class)
            .or_else(|| facts.agent.as_deref().and_then(map_agent_to_workload_class))
            .map(str::to_string),
        account_key: facts.account_key.clone(),
        observed_at: facts.completed_ms,
    })
}

pub fn build_model_speed_observation(
    facts: &NativeUsageFacts,
    variants: &[VariantFact],
) -> Option<UsageObservation> {
    let observation = build_usage_observation(facts, variants)?;
    if !facts.output_tokens.is_finite()
        || facts.output_tokens <= 0.0
        || facts.output_tokens.floor() <= 0.0
        || !observation.duration_ms.is_finite()
        || observation.duration_ms <= 0.0
    {
        return None;
    }
    Some(observation)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The model naming the config converter writes into operator config.
    /// The TypeScript twin's vectors below hold for exactly these values.
    fn naming() -> ModelNaming {
        let defaults: Value = serde_json::from_str(include_str!(
            "../../prefrontal-routing-module/data/config_convert_defaults.json"
        ))
        .unwrap();
        serde_json::from_value(defaults["model_naming"].clone()).unwrap()
    }

    /// A canonical preview id keeps its single suffix through every provider
    /// arm; a bare alias still expands; a tier suffix is left alone (Loop R183).
    #[test]
    fn gemini_alias_expansion_is_suffix_aware() {
        for provider in ["google", "github-copilot"] {
            assert_eq!(
                transform_model_for_provider(&naming(), provider, "gemini-3.1-pro-preview"),
                "gemini-3.1-pro-preview",
                "{provider}"
            );
            assert_eq!(
                transform_model_for_provider(&naming(), provider, "gemini-3.1-pro"),
                "gemini-3.1-pro-preview",
                "{provider}"
            );
            assert_eq!(
                transform_model_for_provider(&naming(), provider, "gemini-3-flash-preview"),
                "gemini-3-flash-preview",
                "{provider}"
            );
        }
        assert_eq!(
            transform_model_for_provider(&naming(), "google", "gemini-3-flash"),
            "gemini-3-flash-preview"
        );
        assert_eq!(
            transform_model_for_provider(&naming(), "google", "gemini-3.1-pro-high"),
            "gemini-3.1-pro-high"
        );
        assert_eq!(
            transform_model_for_provider(&naming(), "vercel", "google/gemini-3.1-pro-preview"),
            "google/gemini-3.1-pro-preview"
        );
        assert_eq!(
            transform_model_for_provider(&naming(), "vercel", "google/gemini-3.1-pro"),
            "google/gemini-3.1-pro-preview"
        );
        // Applying the transform to its own output is a fixed point.
        let once = transform_model_for_provider(&naming(), "google", "gemini-3.1-pro");
        assert_eq!(
            transform_model_for_provider(&naming(), "google", &once),
            once
        );
    }

    fn fallback(provider: &str, model: &str, variant: Option<&str>) -> FallbackEntry {
        FallbackEntry {
            providers: vec![provider.into()],
            model: model.into(),
            variant: variant.map(str::to_string),
            ..FallbackEntry::default()
        }
    }

    fn usage_facts(id: &str) -> NativeUsageFacts {
        NativeUsageFacts {
            message_id: id.into(),
            provider_id: "provider".into(),
            model_id: "model".into(),
            variant: Some("high".into()),
            output_tokens: 100.0,
            reasoning_tokens: Some(25.0),
            created_ms: 1_780_000_000_000.0,
            completed_ms: 1_780_000_002_000.0,
            ..Default::default()
        }
    }

    fn ambiguous_variants() -> Vec<VariantFact> {
        vec![
            VariantFact {
                provider_id: "provider".into(),
                model_id: "model".into(),
                variant: Some("low".into()),
            },
            VariantFact {
                provider_id: "provider".into(),
                model_id: "model".into(),
                variant: Some("high".into()),
            },
        ]
    }

    #[test]
    fn model_resolution_pipeline_keeps_override_provenance_vector() {
        let result = resolve_model_pipeline(
            &ModelResolutionRequest {
                intent: ModelResolutionIntent {
                    user_model: Some("openai/gpt-5.3-codex".into()),
                    ..Default::default()
                },
                constraints: ModelResolutionConstraints::default(),
                policy: ModelResolutionPolicy::default(),
            },
            &naming(),
        )
        .unwrap();
        assert_eq!(result.model, "openai/gpt-5.3-codex");
        assert_eq!(result.provenance, "override");
        assert!(result.attempted.is_empty());
    }

    /// A pin whose only catalog match is a sibling must resolve to that sibling
    /// or fall through, never to the pin as spelled: the host refuses an id it
    /// does not serve, and the configured fallback was skipped on the way.
    #[test]
    fn warm_pin_resolves_to_the_catalog_id_not_the_spelling() {
        let result = resolve_user_model_pin(&UserModelPinRequest {
            user_model: Some("openai/gpt-5.4".into()),
            user_fallback_models: vec!["openai/gpt-5.3-codex".into()],
            available_models: ["openai/gpt-5.4-mini", "openai/gpt-5.3-codex"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(result.provider_id, "openai");
        assert!(
            ["gpt-5.4-mini", "gpt-5.3-codex"].contains(&result.model_id.as_str()),
            "resolved to an id the catalog does not serve: {}",
            result.model_id
        );
    }

    #[test]
    fn model_resolution_user_pin_vectors_are_unchanged() {
        let cases = [
            ("openai/gpt-5.4", None, false, "openai", "gpt-5.4", None),
            (
                "openai/gpt-5.4(max)",
                None,
                true,
                "openai",
                "gpt-5.4",
                Some("max"),
            ),
            (
                "openai/gpt-5.4 high",
                None,
                true,
                "openai",
                "gpt-5.4",
                Some("high"),
            ),
            (
                "openai/gpt-5.4(high)",
                Some("medium"),
                true,
                "openai",
                "gpt-5.4",
                Some("medium"),
            ),
            (
                "new-api-openai/gpt-5.4-high",
                None,
                false,
                "new-api-openai",
                "gpt-5.4-high",
                None,
            ),
        ];
        for (model, explicit_variant, warm, provider, model_id, expected_variant) in cases {
            let result = resolve_user_model_pin(&UserModelPinRequest {
                user_model: Some(model.into()),
                user_variant: explicit_variant.map(str::to_string),
                available_models: warm.then(|| "openai/gpt-5.4".into()).into_iter().collect(),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(result.provider_id, provider);
            assert_eq!(result.model_id, model_id);
            assert_eq!(result.variant.as_deref(), expected_variant);
        }
        let fallback_result = resolve_user_model_pin(&UserModelPinRequest {
            user_model: Some("anthropic/claude-sonnet-4.6".into()),
            user_fallback_models: vec![
                "google/gemini-3.1-pro(high)".into(),
                "openai/gpt-5.4(medium)".into(),
            ],
            available_models: vec!["openai/gpt-5.4".into()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(fallback_result.model_id, "gpt-5.4");
        assert_eq!(fallback_result.variant.as_deref(), Some("medium"));
    }

    #[test]
    fn model_resolution_fallback_and_fuzzy_vectors_are_unchanged() {
        let chain = vec![
            fallback("openai", "gpt-5", None),
            fallback("openai", "gpt-5.4-preview", Some("low")),
        ];
        assert_eq!(
            find_most_specific_fallback_entry("openai", "gpt-5.4-preview-2026", &chain)
                .unwrap()
                .model,
            "gpt-5.4-preview"
        );
        assert_eq!(
            fuzzy_match_model(
                "gpt-5.4",
                &["provider-long/gpt-5.4".into(), "p/gpt-5.4".into()],
                &[]
            )
            .as_deref(),
            Some("p/gpt-5.4")
        );
    }

    #[test]
    fn model_resolution_alias_vectors_are_unchanged() {
        let cases = [
            ("gpt-5.4", "gpt-5.4", "canonical", None),
            (
                "anthropic/claude-sonnet-4-6",
                "claude-sonnet-4-6",
                "canonical",
                None,
            ),
            (
                "gemini-3.1-pro-high",
                "gemini-3.1-pro",
                "pattern-alias",
                Some("gemini-3.1-pro-tier-alias"),
            ),
            (
                "gemini-3-pro-high",
                "gemini-3-pro-preview",
                "exact-alias",
                Some("gemini-3-pro-tier-alias"),
            ),
            (
                "anthropic/claude-opus-4-7-thinking",
                "claude-opus-4-7",
                "pattern-alias",
                Some("claude-thinking-legacy-alias"),
            ),
            ("constructor", "constructor", "canonical", None),
        ];
        for (requested, canonical, source, rule) in cases {
            let result = resolve_model_id_alias(requested);
            assert_eq!(result.canonical_model_id, canonical);
            assert_eq!(result.source, source);
            assert_eq!(result.rule_id.as_deref(), rule);
        }
    }

    #[test]
    fn model_resolution_capability_precedence_and_compatibility_vectors_are_unchanged() {
        let mut bundled = ModelCapabilitiesSnapshot::default();
        bundled.models.insert(
            "claude-opus-4-7".into(),
            ModelCapabilitiesSnapshotEntry {
                id: "claude-opus-4-7".into(),
                family: Some("claude-opus".into()),
                reasoning: Some(true),
                temperature: Some(true),
                tool_call: Some(true),
                limit: Some(SnapshotLimit {
                    context: Some(1_000_000.0),
                    output: Some(128_000.0),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let capabilities = resolve_model_capabilities(
            &ModelCapabilitiesInput {
                provider_id: "anthropic".into(),
                model_id: "claude-opus-4-7-thinking".into(),
                runtime_model: Some(json!({ "variants": { "low": {}, "medium": {}, "high": {} } })),
                bundled_snapshot: Some(bundled),
                ..Default::default()
            },
            &naming(),
        );
        assert_eq!(capabilities.canonical_model_id, "claude-opus-4-7");
        assert_eq!(
            capabilities.variants,
            Some(vec!["high".into(), "low".into(), "medium".into()])
        );
        assert_eq!(capabilities.max_output_tokens, Some(128_000.0));
        assert_eq!(capabilities.diagnostics["resolutionMode"], "alias-backed");

        let result = resolve_compatible_model_settings(
            "claude-sonnet-4-6",
            &DesiredModelSettings {
                variant: Some("max".into()),
                reasoning_effort: Some("high".into()),
                ..Default::default()
            },
            None,
            &naming(),
        );
        assert_eq!(result.variant.as_deref(), Some("high"));
        assert!(result.reasoning_effort.is_none());
        assert_eq!(result.changes.len(), 2);
    }

    #[test]
    fn model_resolution_model_family_compatibility_vectors_are_unchanged() {
        for model in [
            "gemini-3.1-pro",
            "kimi-k2.5",
            "k2-v2",
            "glm-5",
            "minimax-m2.5",
            "deepseek-r2",
            "mistral-large-next",
            "codestral-2506",
            "llama-4-maverick",
        ] {
            let result = resolve_compatible_model_settings(
                model,
                &DesiredModelSettings {
                    variant: Some("max".into()),
                    reasoning_effort: Some("high".into()),
                    ..Default::default()
                },
                None,
                &naming(),
            );
            assert_eq!(result.variant.as_deref(), Some("high"), "{model}");
            assert!(result.reasoning_effort.is_none(), "{model}");
        }
        let gpt = resolve_compatible_model_settings(
            "gpt-5.4",
            &DesiredModelSettings {
                variant: Some("max".into()),
                reasoning_effort: Some("xhigh".into()),
                ..Default::default()
            },
            None,
            &naming(),
        );
        assert_eq!(gpt.variant.as_deref(), Some("xhigh"));
        assert_eq!(gpt.reasoning_effort.as_deref(), Some("xhigh"));
    }

    #[test]
    fn model_resolution_speed_observation_vectors_are_unchanged() {
        let now = 1_780_000_000_000.0;
        let observation = build_model_speed_observation(
            &NativeUsageFacts {
                message_id: "msg_parse".into(),
                provider_id: "provider".into(),
                model_id: "model".into(),
                variant: Some("high".into()),
                output_tokens: 120.0,
                reasoning_tokens: Some(30.0),
                created_ms: now,
                completed_ms: now + 3_000.0,
                tool_elapsed_ms: None,
                ..Default::default()
            },
            &[],
        )
        .unwrap();
        assert_eq!(observation.route_key, "provider/model/high");
        assert_eq!(observation.output_tokens, 150.0);
        assert_eq!(observation.duration_ms, 3_000.0);

        let ambiguous = build_model_speed_observation(
            &NativeUsageFacts {
                message_id: "msg_ambiguous".into(),
                provider_id: "p".into(),
                model_id: "m".into(),
                variant: None,
                output_tokens: 120.0,
                reasoning_tokens: None,
                created_ms: now,
                completed_ms: now + 2_000.0,
                tool_elapsed_ms: None,
                ..Default::default()
            },
            &[
                VariantFact {
                    provider_id: "p".into(),
                    model_id: "m".into(),
                    variant: Some("low".into()),
                },
                VariantFact {
                    provider_id: "p".into(),
                    model_id: "m".into(),
                    variant: Some("high".into()),
                },
            ],
        );
        assert!(ambiguous.is_none());
    }

    #[test]
    fn usage_builder_carries_every_available_profile_field() {
        let mut facts = usage_facts("profile");
        facts.input_tokens = Some(300.0);
        facts.cached_read_tokens = Some(200.0);
        facts.cache_write_tokens = Some(10.0);
        facts.workload_class = Some("ephemeral".into());
        facts.account_key = Some("provider/primary".into());
        let observation = build_usage_observation(&facts, &[]).unwrap();

        assert_eq!(observation.input_tokens, Some(300.0));
        assert_eq!(observation.output_tokens, 125.0);
        assert_eq!(observation.reasoning_tokens, Some(25.0));
        assert_eq!(observation.cached_read_tokens, Some(200.0));
        assert_eq!(observation.cache_write_tokens, Some(10.0));
        assert_eq!(observation.duration_ms, 2_000.0);
        assert_eq!(observation.workload_class.as_deref(), Some("ephemeral"));
        assert_eq!(observation.account_key.as_deref(), Some("provider/primary"));
    }

    #[test]
    fn invalid_output_branches_build_usage_but_no_speed_observation() {
        for output in [f64::NAN, f64::INFINITY, 0.0, -1.0, 0.99] {
            let mut facts = usage_facts("invalid-output");
            facts.output_tokens = output;
            assert!(build_usage_observation(&facts, &[]).is_some());
            assert!(build_model_speed_observation(&facts, &[]).is_none());
        }
    }

    #[test]
    fn unusable_duration_branches_build_usage_but_no_speed_observation() {
        for (created_ms, completed_ms, tool_elapsed_ms) in [
            (0.0, 1_000.0, Some(f64::NAN)),
            (0.0, 1_000.0, Some(f64::INFINITY)),
            (2_000.0, 1_000.0, None),
            (0.0, 1_000.0, Some(-1.0)),
        ] {
            let mut facts = usage_facts("invalid-duration");
            facts.created_ms = created_ms;
            facts.completed_ms = completed_ms;
            facts.tool_elapsed_ms = tool_elapsed_ms;
            assert!(build_usage_observation(&facts, &[]).is_some());
            assert!(build_model_speed_observation(&facts, &[]).is_none());
        }
    }

    #[test]
    fn ambiguous_variant_builds_no_usage_row() {
        let mut facts = usage_facts("ambiguous");
        facts.variant = None;
        assert!(build_usage_observation(&facts, &ambiguous_variants()).is_none());
        assert!(build_model_speed_observation(&facts, &ambiguous_variants()).is_none());
    }

    #[test]
    fn empty_provider_or_model_builds_no_usage_row() {
        for (provider_id, model_id) in [("", "model"), ("provider", "")] {
            let mut facts = usage_facts("missing-route-part");
            facts.provider_id = provider_id.into();
            facts.model_id = model_id.into();
            assert!(build_usage_observation(&facts, &[]).is_none());
        }
    }

    #[test]
    fn empty_message_id_with_constructible_route_builds_no_usage_row() {
        let facts = usage_facts("");
        assert!(build_usage_observation(&facts, &[]).is_none());
    }

    #[test]
    fn reasoning_heavy_usage_keeps_the_pre_migration_output_token_meaning() {
        let mut facts = usage_facts("reasoning-heavy");
        facts.output_tokens = 100.0;
        facts.reasoning_tokens = Some(80.0);
        let observation = build_usage_observation(&facts, &[]).unwrap();

        assert_eq!(observation.output_tokens.to_bits(), 180.0_f64.to_bits());
        assert_eq!(observation.reasoning_tokens, Some(80.0));
    }
    #[test]
    fn class_producer_true_agent_mapping_to_workload_class() {
        // Gated on class_producer = true
        assert_eq!(map_agent_to_workload_class("mason"), Some("ephemeral"));
        assert_eq!(map_agent_to_workload_class("alfonso"), Some("head"));
        assert_eq!(map_agent_to_workload_class("explore"), Some("gather"));
        assert_eq!(map_agent_to_workload_class("librarian"), Some("gather"));
        assert_eq!(map_agent_to_workload_class("sidekick"), Some("gather"));
        assert_eq!(
            map_agent_to_workload_class("sidekick.classifier"),
            Some("gather")
        );
        assert_eq!(map_agent_to_workload_class("hire_agent"), Some("hire"));
        assert_eq!(map_agent_to_workload_class("hire-worker"), Some("hire"));
        assert_eq!(map_agent_to_workload_class("unknown_agent"), None);
    }

    #[test]
    fn counters_producer_true_broca_usage_copy_mapping() {
        // Gated on counters_producer = true
        let mut facts = usage_facts("broca-copy-live");
        let live_usage = BrocaUsageFacts {
            input_tokens: 500,
            cached_input_tokens: 250,
            cache_write_tokens: 15,
            output_tokens: 120,
            reasoning_tokens: 45,
            retries_used: 0,
        };
        facts.apply_broca_usage(Some(&live_usage));
        assert_eq!(facts.cached_read_tokens, Some(250.0));
        assert_eq!(facts.input_tokens, Some(500.0));

        // Fixture payload: output_tokens = 100 and reasoning_tokens = 80
        let mut fixture_facts = usage_facts("broca-fixture");
        let fixture_usage = BrocaUsageFacts {
            input_tokens: 300,
            cached_input_tokens: 40,
            cache_write_tokens: 20,
            output_tokens: 100,
            reasoning_tokens: 80,
            retries_used: 0,
        };
        fixture_facts.apply_broca_usage(Some(&fixture_usage));
        let obs = build_usage_observation(&fixture_facts, &[]).unwrap();

        assert_eq!(obs.cached_read_tokens, Some(40.0));
        assert_eq!(obs.cache_write_tokens, Some(20.0));
        assert_eq!(obs.reasoning_tokens, Some(80.0));
        assert_eq!(obs.input_tokens, Some(300.0));
        // Broca's 100 output tokens already include the 80 reasoning tokens, so
        // the stored total is 100, and the class vector's output-excluding-
        // reasoning (stored output minus reasoning) is the 20 visible tokens.
        assert_eq!(obs.output_tokens, 100.0);
        assert_eq!(obs.output_tokens - obs.reasoning_tokens.unwrap(), 20.0);
    }
    // Source of truth: Broca's normalized usage. broca-protocol
    // crates/broca-protocol/src/usage.rs:22 (Broca checkout 6c45a375): reasoning
    // tokens are "ALWAYS A SUBSET of `output_tokens`, never additional to it ...
    // so a consumer summing the two double-counts"; the OpenAI Responses
    // normalizer stores the provider's billing total as output_tokens
    // (crates/broca-provider/src/families/openai_responses.rs:955) and the
    // reasoning subset beside it without subtracting (:958). The usage block
    // below is a real Codex mason run's Broca usage as Broca reported it.
    // Routing's NativeUsageFacts follows OpenCode's shape instead (tokens.output
    // is visible output, tokens.reasoning sits beside it), so the copy must
    // subtract reasoning once; build_usage_observation adds it back, and the
    // class vector's output_excluding_reasoning (output - reasoning, see
    // compute_class_vector in prefrontal-routing-store) is then the 28,338
    // visible tokens, not the 39,620 total.
    #[test]
    fn broca_output_includes_reasoning_so_the_copy_subtracts_it_once() {
        let usage: BrocaUsageFacts = serde_json::from_str(
            r#"{"input_tokens":339380,"cached_input_tokens":20647936,"cache_write_tokens":0,"output_tokens":39620,"reasoning_tokens":11282}"#,
        )
        .unwrap();
        let mut facts = usage_facts("codex-mason-run");
        facts.apply_broca_usage(Some(&usage));
        assert_eq!(facts.output_tokens, 28_338.0);
        assert_eq!(facts.reasoning_tokens, Some(11_282.0));
        assert_eq!(facts.input_tokens, Some(339_380.0));
        assert_eq!(facts.cached_read_tokens, Some(20_647_936.0));
        assert_eq!(facts.cache_write_tokens, Some(0.0));

        let obs = build_usage_observation(&facts, &[]).unwrap();
        // The stored output is Broca's billing total, counted once.
        assert_eq!(obs.output_tokens, 39_620.0);
        let output_excluding_reasoning = obs.output_tokens - obs.reasoning_tokens.unwrap();
        assert_eq!(output_excluding_reasoning, 28_338.0);
        assert_eq!(
            output_excluding_reasoning + obs.reasoning_tokens.unwrap(),
            39_620.0,
            "visible + reasoning must equal Broca's output total, not exceed it"
        );
    }
    #[test]
    fn class_producer_true_workload_class_closed_enum() {
        // Gated on class_producer = true:
        // workload_class accepts exactly ephemeral, head, gather and hire.
        for class in ["ephemeral", "head", "gather", "hire"] {
            assert_eq!(parse_workload_class(class), Some(class));
            let mut facts = usage_facts("class-valid");
            facts.workload_class = Some(class.into());
            let obs = build_usage_observation(&facts, &[]).unwrap();
            assert_eq!(obs.workload_class.as_deref(), Some(class));
        }

        for invalid in ["mason", "task", "unknown", "ephemeral_extra", "", " "] {
            assert_eq!(parse_workload_class(invalid), None);
            let mut facts = usage_facts("class-invalid");
            facts.workload_class = Some(invalid.into());
            let obs = build_usage_observation(&facts, &[]).unwrap();
            assert_eq!(obs.workload_class, None);
        }
    }

    #[test]
    fn absence_is_whole_object_at_provider_boundary_and_per_counter_at_param_layer() {
        // Gated on absence whole-object vs per-counter:
        // Whole-object absence at provider boundary clears all 5 counter columns
        let mut facts = usage_facts("whole-object-absence");
        facts.apply_broca_usage(None);
        let obs = build_usage_observation(&facts, &[]).unwrap();
        assert_eq!(obs.input_tokens, None);
        assert_eq!(obs.output_tokens, 0.0); // 0.0 outputs write NULL to stored output_tokens
        assert_eq!(obs.reasoning_tokens, None);
        assert_eq!(obs.cached_read_tokens, None);
        assert_eq!(obs.cache_write_tokens, None);

        // Authoritative zero cached-read counter writes 0
        let zero_cached = BrocaUsageFacts {
            input_tokens: 100,
            cached_input_tokens: 0,
            cache_write_tokens: 5,
            output_tokens: 50,
            reasoning_tokens: 10,
            retries_used: 0,
        };
        let mut facts_zero = usage_facts("zero-cached");
        facts_zero.apply_broca_usage(Some(&zero_cached));
        let obs_zero = build_usage_observation(&facts_zero, &[]).unwrap();
        assert_eq!(obs_zero.cached_read_tokens, Some(0.0));
    }
}
