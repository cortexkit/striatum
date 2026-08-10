//! Launch-time request and pricing policy resolution shared by every route-selection path.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::model_facts::ModelRequirements;
use crate::model_router::RequiredCapabilities;
use crate::provider_model_spec::{filter_excluded_keys, parse_spec_list, resolve_spec};

pub const CURATED_SUBSCRIPTION_PROVIDERS: [&str; 2] = ["opencode-go", "ollama-cloud"];

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedCapabilities {
    #[serde(default)]
    pub vision: Option<bool>,
    #[serde(default)]
    pub min_context: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedModelRequirements {
    #[serde(default)]
    pub iq: Option<f64>,
    #[serde(default)]
    pub eq: Option<f64>,
    #[serde(default)]
    pub caps: Option<RequestedCapabilities>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedLaunchRequirements {
    pub requirements: ModelRequirements,
    pub required_capabilities: Option<RequiredCapabilities>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchRequirementsError {
    pub message: String,
}

fn clamp_demand(value: Option<f64>) -> f64 {
    match value {
        Some(value) if value.is_finite() => value.clamp(0.0, 100.0),
        _ => 0.0,
    }
}

fn default_requirements(target_agent: &str) -> Option<ModelRequirements> {
    match target_agent {
        "explore" => Some(ModelRequirements { iq: 30.0, eq: 0.0 }),
        "librarian" => Some(ModelRequirements { iq: 50.0, eq: 20.0 }),
        _ => None,
    }
}

/// Resolve the caller's raw optional demand vector before entering the shared scorer.
/// Mason has no meaningful role-level default, while specialist workers retain their
/// established fallback vectors when no non-zero demand was supplied.
pub fn resolve_launch_requirements(
    target_agent: &str,
    requested: Option<&RequestedModelRequirements>,
) -> Result<ResolvedLaunchRequirements, LaunchRequirementsError> {
    let mut requirements = ModelRequirements {
        iq: clamp_demand(requested.and_then(|value| value.iq)),
        eq: clamp_demand(requested.and_then(|value| value.eq)),
    };
    let has_demand = requirements.iq > 0.0 || requirements.eq > 0.0;

    if target_agent == "mason" && (requested.is_none() || !has_demand) {
        return Err(LaunchRequirementsError {
            message: format!(
                "{target_agent} requires explicit model_requirements (iq/eq, 0-100). Pick what THIS task needs, e.g. {{ iq: 90 }} to solve a hard problem, or {{ iq: 30, eq: 10 }} for a straightforward task."
            ),
        });
    }
    if requested.is_none() || !has_demand {
        if let Some(fallback) = default_requirements(target_agent) {
            requirements = fallback;
        }
    }

    let required_capabilities = requested
        .and_then(|value| value.caps.as_ref())
        .filter(|caps| {
            caps.vision == Some(true)
                || caps
                    .min_context
                    .is_some_and(|min_context| min_context.is_finite() && min_context != 0.0)
        })
        .map(|caps| RequiredCapabilities {
            vision: caps.vision,
            min_context: caps.min_context,
        });

    Ok(ResolvedLaunchRequirements {
        requirements,
        required_capabilities,
    })
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRoutingPolicyInput {
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub subscription: Vec<String>,
    #[serde(default)]
    pub prefer_fast_when_relaxed: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedRoutingPolicy {
    pub available_models: Vec<String>,
    pub subscription_keys: Vec<String>,
    pub prefer_fast_specs: Vec<String>,
    pub connected_model_count: usize,
}

/// Apply exclusion and subscription policy to raw connected-model and auth facts.
/// The output is deterministic so it can be returned as selection provenance.
pub fn resolve_routing_policy(
    available_models: impl IntoIterator<Item = String>,
    policy: &ModelRoutingPolicyInput,
    provider_auth_types: &HashMap<String, String>,
    suppress_fast_variant: bool,
) -> ResolvedRoutingPolicy {
    let connected: HashSet<String> = available_models.into_iter().collect();
    let connected_model_count = connected.len();
    let filtered = filter_excluded_keys(&connected, &policy.exclude);

    let mut subscription_specs = CURATED_SUBSCRIPTION_PROVIDERS
        .iter()
        .map(|provider| (*provider).to_string())
        .collect::<Vec<_>>();
    subscription_specs.extend(policy.subscription.iter().cloned());
    let subscription_specs = parse_spec_list(&subscription_specs);

    let mut available_models = filtered.into_iter().collect::<Vec<_>>();
    available_models.sort();
    let subscription_keys = available_models
        .iter()
        .filter(|key| {
            let provider = key.split('/').next().unwrap_or(key);
            let oauth = provider_auth_types
                .get(provider)
                .is_some_and(|kind| kind == "oauth");
            resolve_spec(&subscription_specs, key, oauth)
        })
        .cloned()
        .collect();

    ResolvedRoutingPolicy {
        available_models,
        subscription_keys,
        prefer_fast_specs: if suppress_fast_variant {
            Vec::new()
        } else {
            policy.prefer_fast_when_relaxed.clone()
        },
        connected_model_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth_types() -> HashMap<String, String> {
        [
            ("anthropic", "oauth"),
            ("openai", "oauth"),
            ("xai", "oauth"),
            ("google", "oauth"),
            ("opencode-go", "api"),
            ("fireworks-ai", "api"),
            ("deepseek", "api"),
        ]
        .into_iter()
        .map(|(provider, kind)| (provider.to_string(), kind.to_string()))
        .collect()
    }

    fn resolve(keys: &[&str], policy: ModelRoutingPolicyInput) -> ResolvedRoutingPolicy {
        resolve_routing_policy(
            keys.iter().map(|key| (*key).to_string()),
            &policy,
            &auth_types(),
            false,
        )
    }

    #[test]
    fn oauth_and_curated_providers_are_subscriptions_by_default() {
        let resolved = resolve(
            &[
                "anthropic/claude-opus-4-8",
                "xai/grok-4.3",
                "google/antigravity-gemini-3.5-flash",
                "opencode-go/qwen3.7-max",
                "deepseek/deepseek-v4-flash",
            ],
            ModelRoutingPolicyInput::default(),
        );
        assert_eq!(
            resolved.subscription_keys,
            vec![
                "anthropic/claude-opus-4-8",
                "google/antigravity-gemini-3.5-flash",
                "opencode-go/qwen3.7-max",
                "xai/grok-4.3",
            ]
        );
    }

    #[test]
    fn missing_auth_data_leaves_only_curated_or_configured_subscriptions() {
        let resolved = resolve_routing_policy(
            [
                "anthropic/claude-opus-4-8".to_string(),
                "opencode-go/qwen3.7-max".to_string(),
            ],
            &ModelRoutingPolicyInput::default(),
            &HashMap::new(),
            false,
        );
        assert_eq!(resolved.subscription_keys, vec!["opencode-go/qwen3.7-max"]);
    }

    #[test]
    fn user_subscription_specs_override_curated_and_oauth_defaults() {
        let resolved = resolve(
            &[
                "anthropic/claude-opus-4-8",
                "fireworks-ai/llama",
                "opencode-go/qwen3.7-max",
                "opencode-go/some-metered-model",
            ],
            ModelRoutingPolicyInput {
                subscription: vec![
                    "-anthropic".into(),
                    "fireworks-ai".into(),
                    "-opencode-go/some-metered-model".into(),
                ],
                ..ModelRoutingPolicyInput::default()
            },
        );
        assert_eq!(
            resolved.subscription_keys,
            vec!["fireworks-ai/llama", "opencode-go/qwen3.7-max"]
        );
    }

    #[test]
    fn exclusion_policy_filters_connected_models_before_classification() {
        let resolved = resolve(
            &[
                "openai/gpt-5.5",
                "opencode-go/deepseek-v4-flash",
                "opencode-go/minimax-m2.5",
            ],
            ModelRoutingPolicyInput {
                exclude: vec!["openai/gpt-5.5".into()],
                subscription: vec![
                    "opencode-go".into(),
                    "-opencode-go/deepseek-v4-flash".into(),
                ],
                prefer_fast_when_relaxed: vec!["openai".into()],
            },
        );
        assert_eq!(
            resolved.available_models,
            vec!["opencode-go/deepseek-v4-flash", "opencode-go/minimax-m2.5"]
        );
        assert_eq!(resolved.subscription_keys, vec!["opencode-go/minimax-m2.5"]);
        assert_eq!(resolved.prefer_fast_specs, vec!["openai"]);
        assert_eq!(resolved.connected_model_count, 3);

        let suppressed = resolve_routing_policy(
            ["openai/gpt-5.5".to_string()],
            &ModelRoutingPolicyInput {
                prefer_fast_when_relaxed: vec!["openai".into()],
                ..ModelRoutingPolicyInput::default()
            },
            &HashMap::new(),
            true,
        );
        assert!(suppressed.prefer_fast_specs.is_empty());
    }

    #[test]
    fn launch_requirements_preserve_specialist_defaults_and_mason_explicit_rule() {
        assert_eq!(
            resolve_launch_requirements("explore", None)
                .unwrap()
                .requirements,
            ModelRequirements { iq: 30.0, eq: 0.0 }
        );
        assert_eq!(
            resolve_launch_requirements("librarian", None)
                .unwrap()
                .requirements,
            ModelRequirements { iq: 50.0, eq: 20.0 }
        );
        assert!(resolve_launch_requirements("mason", None)
            .unwrap_err()
            .message
            .contains("requires explicit model_requirements"));
        assert!(
            resolve_launch_requirements("mason", Some(&RequestedModelRequirements::default()))
                .is_err()
        );
    }

    #[test]
    fn launch_requirements_clamp_demands_and_shape_capabilities() {
        let requested = RequestedModelRequirements {
            iq: Some(120.0),
            eq: Some(-10.0),
            caps: Some(RequestedCapabilities {
                vision: Some(true),
                min_context: Some(200_000.0),
            }),
        };
        let resolved = resolve_launch_requirements("mason", Some(&requested)).unwrap();
        assert_eq!(
            resolved.requirements,
            ModelRequirements { iq: 100.0, eq: 0.0 }
        );
        assert_eq!(
            resolved.required_capabilities,
            Some(RequiredCapabilities {
                vision: Some(true),
                min_context: Some(200_000.0),
            })
        );
    }
}
