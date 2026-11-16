//! Usage accounting facts, speed observations, and producer readiness gates.
//!
//! Usage and independent timing samples share route and variant attribution,
//! while keeping accounting completeness separate from speed eligibility.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{make_capacity_key, make_route_key, UsageObservation};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUsageFacts {
    /// A timing-only delivery: its counters never enter usage accounting.
    #[serde(
        default,
        rename = "speed_sample",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub speed_sample: bool,
    #[serde(
        default,
        rename = "service_tier",
        skip_serializing_if = "Option::is_none"
    )]
    pub service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<String>,
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
    /// outright once the sum passed the message's own wall clock.
    /// The old spelling is not accepted: a pre-rename host sends no exclusion at
    /// all, so its samples understate speed for one restart window rather than
    /// overstating it indefinitely.
    #[serde(default)]
    pub tool_elapsed_ms: Option<f64>,
    /// The producer measured no model timing for these counts, for example a
    /// whole run reported once, whose wall clock is mostly tool time. The
    /// usage row then records no duration and is never a speed sample,
    /// whatever `created_ms` and `tool_elapsed_ms` say; `completed_ms` is still
    /// when the usage was observed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub untimed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariantFact {
    pub provider_id: String,
    pub model_id: String,
    pub variant: Option<String>,
}

fn normalized_nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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
    // An unknown duration is not finite, which the store records as NULL and
    // the speed checks refuse (see `UsageObservation::duration_ms`).
    if facts.untimed {
        return f64::NAN;
    }
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
    if facts.speed_sample && facts.output_tokens == 0.0 {
        return facts.reasoning_tokens.unwrap_or(0.0).floor().max(0.0);
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
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
    /// - absent reasoning stays unknown; absent usage clears all five counters
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
            self.reasoning_tokens = u.reasoning_tokens.map(|tokens| tokens as f64);
            self.input_tokens = Some(u.input_tokens as f64);
            // With no breakdown, retain the combined output total without claiming
            // that the provider measured zero reasoning tokens.
            self.output_tokens =
                u.output_tokens
                    .saturating_sub(u.reasoning_tokens.unwrap_or(0)) as f64;
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

/// Independent generation timing, stored outside the accounting history.
#[derive(Debug, Clone, PartialEq)]
pub struct SpeedSampleObservation {
    pub id: String,
    pub route_key: String,
    pub provider_id: String,
    pub model_id: String,
    pub variant: Option<String>,
    pub service_tier: Option<String>,
    pub speed: Option<String>,
    /// Combined visible and reasoning output, matching timed OpenCode assistant usage rows.
    pub output_tokens: f64,
    pub reasoning_tokens: Option<f64>,
    pub duration_ms: f64,
    pub observed_at: f64,
}

pub fn build_speed_sample_observation(
    facts: &NativeUsageFacts,
    variants: &[VariantFact],
) -> Option<SpeedSampleObservation> {
    if !facts.speed_sample {
        return None;
    }
    let observation = build_usage_observation(facts, variants)?;
    if !observation.output_tokens.is_finite()
        || observation.output_tokens <= 0.0
        || !observation.duration_ms.is_finite()
        || observation.duration_ms <= 0.0
    {
        return None;
    }
    let variant = observation
        .route_key
        .strip_prefix(&format!("{}/", observation.capacity_key))
        .map(str::to_owned);
    Some(SpeedSampleObservation {
        id: observation.id,
        route_key: observation.route_key,
        provider_id: observation.provider_id,
        model_id: facts.model_id.trim().to_owned(),
        variant,
        service_tier: facts.service_tier.clone(),
        speed: facts.speed.clone(),
        output_tokens: observation.output_tokens,
        reasoning_tokens: observation.reasoning_tokens,
        duration_ms: observation.duration_ms,
        observed_at: observation.observed_at,
    })
}

/// Provider words stay raw in storage. Only the ordinary served modes share
/// the standard/standard population used by OpenCode assistant messages; every other
/// pair remains distinct, including absence.
pub fn speed_sample_population<'a>(
    service_tier: Option<&'a str>,
    speed: Option<&'a str>,
) -> (Option<&'a str>, Option<&'a str>) {
    if matches!(service_tier, None | Some("default" | "standard"))
        && matches!(speed, None | Some("standard"))
    {
        (Some("standard"), Some("standard"))
    } else {
        (service_tier, speed)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn speed_sample_population_keeps_provider_words_and_standard_aliases() {
        use super::speed_sample_population;
        for tier in [None, Some("default"), Some("standard")] {
            for speed in [None, Some("standard")] {
                assert_eq!(
                    speed_sample_population(tier, speed),
                    (Some("standard"), Some("standard")),
                    "{tier:?}/{speed:?}"
                );
            }
        }
        for (tier, speed) in [
            (Some("priority"), None),
            (Some("flex"), None),
            (Some("batch"), None),
            (Some("standard"), Some("fast")),
            (Some("default"), Some("fast")),
            (None, Some("fast")),
            (Some(""), None),
            (Some("Default"), None),
            (Some("standard"), Some("")),
            (Some(" standard "), None),
        ] {
            assert_eq!(speed_sample_population(tier, speed), (tier, speed));
        }
    }
    use super::*;

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
    fn usage_speed_observation_vectors_are_unchanged() {
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
    fn untimed_usage_records_no_duration_and_is_never_a_speed_sample() {
        // The same facts give a speed sample with timing, so only the flag
        // can be what removes it.
        let timed = usage_facts("untimed");
        assert!(build_model_speed_observation(&timed, &[]).is_some());
        let untimed = NativeUsageFacts {
            untimed: true,
            ..timed
        };
        let observation = build_usage_observation(&untimed, &[]).unwrap();
        assert!(!observation.duration_ms.is_finite(), "{observation:?}");
        assert_eq!(observation.output_tokens, 125.0);
        assert_eq!(observation.observed_at, untimed.completed_ms);
        assert!(build_model_speed_observation(&untimed, &[]).is_none());
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
            reasoning_tokens: Some(45),
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
            reasoning_tokens: Some(80),
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
    // compute_class_vector in striatum-store) is then the 28,338
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
            reasoning_tokens: Some(10),
            retries_used: 0,
        };
        let mut facts_zero = usage_facts("zero-cached");
        facts_zero.apply_broca_usage(Some(&zero_cached));
        let obs_zero = build_usage_observation(&facts_zero, &[]).unwrap();
        assert_eq!(obs_zero.cached_read_tokens, Some(0.0));
    }
}
