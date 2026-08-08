//! Tests for the quota overlay: mapping `insula (formerly ai-provider-quota)`'s `ProviderUsage[]`
//! through per-provider extraction + pace projection + hard exhaustion into a
//! routing runtime snapshot. Exercises the FULL parity pipeline (the flat
//! worst-utilization proxy was replaced).

use std::collections::HashMap;

use prefrontal_routing_core::model_facts::{Confidence, ModelFact, ModelScores};
use prefrontal_routing_core::model_router::{
    select_model_route, ModelRouteStateSnapshot, RouteRuntimeState, SelectModelRouteInput,
};
use prefrontal_routing_core::quota::{
    merge_quota_overlay_with, ProviderUsage, DEFAULT_HARD_BLOCK_THRESHOLD,
    NO_RESET_REPROBE_HORIZON_MS,
};
use prefrontal_routing_core::quota_pace::DEFAULT_PACE_CONFIG;
use prefrontal_routing_core::{make_capacity_key, make_route_key, ModelRequirements};

const NOW: f64 = 1_000_000.0;

fn fact(provider: &str, model: &str, cost: f64) -> ModelFact {
    ModelFact {
        provider_id: provider.into(),
        model_id: model.into(),
        variant: None,
        family: None,
        scores: ModelScores {
            iq: 60.0,
            eq: 1800.0,
            speed: 60.0,
        },
        context_window: None,
        max_output_tokens: None,
        cost_tier: None,
        cost_per_m_tok_out: Some(cost),
        supports_vision: None,
        supports_tool_use: None,
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
    }
}

fn usage_from_json(v: serde_json::Value) -> ProviderUsage {
    serde_json::from_value(v).unwrap()
}

fn name_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(w, p)| (w.to_string(), p.to_string()))
        .collect()
}

fn overlay(
    windows: &[ProviderUsage],
    facts: &[ModelFact],
    map: &HashMap<String, String>,
) -> ModelRouteStateSnapshot {
    let mut snap = ModelRouteStateSnapshot {
        now: NOW,
        by_route: HashMap::new(),
        by_capacity: HashMap::new(),
    };
    merge_quota_overlay_with(
        &mut snap,
        windows,
        facts,
        map,
        NOW,
        &DEFAULT_PACE_CONFIG,
        DEFAULT_HARD_BLOCK_THRESHOLD,
    );
    snap
}

#[test]
fn degraded_entry_is_skipped() {
    let entry = usage_from_json(serde_json::json!({
        "provider": "grok",
        "error": "no session: no XAI_API_KEY"
    }));
    assert!(!entry.is_healthy());
    let snap = overlay(
        &[entry],
        &[fact("xai", "grok-4.3", 5.0)],
        &name_map(&[("grok", "xai")]),
    );
    assert!(
        snap.by_capacity.is_empty(),
        "a degraded entry contributes nothing"
    );
}

#[test]
fn high_utilization_no_window_pressures_mapped_capacity() {
    // No windowMinutes → utilization-only ramp. 0.9 is past util_fallback_start so
    // it produces nonzero soft pressure on the openai capacity.
    let facts = vec![
        fact("openai", "gpt-5.5", 30.0),
        fact("anthropic", "claude-opus-4-8", 25.0),
    ];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 90.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let snap = overlay(&windows, &facts, &name_map(&[("codex", "openai")]));

    let openai_cap = make_capacity_key("openai", "gpt-5.5");
    let p = snap
        .by_capacity
        .get(&openai_cap)
        .and_then(|s| s.quota_pressure)
        .unwrap();
    assert!(p > 0.0, "90% util should yield soft pressure, got {p}");
    // anthropic had no window → no entry.
    assert!(!snap
        .by_capacity
        .contains_key(&make_capacity_key("anthropic", "claude-opus-4-8")));
}

#[test]
fn at_or_above_hard_block_sets_exhaustion_until_reset() {
    // 100% util crosses the hard-block threshold → quota_exhausted_until = resets_at.
    let facts = vec![fact("openai", "gpt-5.5", 30.0)];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let snap = overlay(&windows, &facts, &name_map(&[("codex", "openai")]));
    let state = snap
        .by_capacity
        .get(&make_capacity_key("openai", "gpt-5.5"))
        .unwrap();
    assert!(
        state.quota_exhausted_until.is_some_and(|e| e > NOW),
        "exhausted until a future reset"
    );
}

#[test]
fn exhausted_no_reset_uses_local_reprobe_horizon_and_low_utilization_recovers() {
    let facts = vec![fact("openai", "gpt-5.5", 30.0)];
    let exhausted_windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 100.0 } }
    }))];
    let map = name_map(&[("codex", "openai")]);
    let cap = make_capacity_key("openai", "gpt-5.5");

    let mut first = overlay(&exhausted_windows, &facts, &map);
    assert_eq!(
        first.by_capacity[&cap].quota_exhausted_until,
        Some(NOW + NO_RESET_REPROBE_HORIZON_MS),
        "an exhausted no-reset window gets a local re-probe horizon"
    );

    // The horizon is recomputed on every refresh while the wire still reports
    // exhaustion; it is not a fabricated provider reset timestamp.
    let refreshed_now = NOW + 60.0 * 60.0 * 1000.0;
    first.now = refreshed_now;
    merge_quota_overlay_with(
        &mut first,
        &exhausted_windows,
        &facts,
        &map,
        refreshed_now,
        &DEFAULT_PACE_CONFIG,
        DEFAULT_HARD_BLOCK_THRESHOLD,
    );
    assert_eq!(
        first.by_capacity[&cap].quota_exhausted_until,
        Some(refreshed_now + NO_RESET_REPROBE_HORIZON_MS)
    );

    let low_windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 40.0 } }
    }))];
    let recovered = overlay(&low_windows, &facts, &map);
    let state = &recovered.by_capacity[&cap];
    assert_eq!(state.quota_exhausted_until, None);
    assert_eq!(
        state.quota_pressure,
        Some(0.0),
        "low utilization still contributes a fresh pressure reading"
    );
}

#[test]
fn past_reset_window_is_skipped() {
    // resets_at before now → window already reset → no signal.
    let facts = vec![fact("openai", "gpt-5.5", 30.0)];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "1970-01-01T00:00:01Z" } }
    }))];
    let snap = overlay(&windows, &facts, &name_map(&[("codex", "openai")]));
    assert!(
        snap.by_capacity.is_empty(),
        "a past-reset window is not in force"
    );
}

#[test]
fn unmapped_provider_contributes_no_pressure() {
    let facts = vec![fact("openai", "gpt-5.5", 30.0)];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "gemini",
        "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    // name_map has no "gemini" → degrade safe (empty overlay).
    let snap = overlay(&windows, &facts, &name_map(&[("codex", "openai")]));
    assert!(snap.by_capacity.is_empty());
}

#[test]
fn antigravity_per_model_window_pressures_only_the_matched_capacity() {
    // The aggregate primary=100% must be IGNORED; only the matched Gemini capacity
    // gets the per-model window's signal.
    let facts = vec![
        fact("google", "antigravity-gemini-3.5-flash", 0.0),
        fact("google", "antigravity-gemini-3.1-pro", 0.0),
    ];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "antigravity",
        "usage": {
            "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" },
            "extraRateWindows": [
                { "title": "Gemini 3.1 Pro (High)", "id": "pro", "window": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" } }
            ]
        }
    }))];
    let snap = overlay(&windows, &facts, &name_map(&[("antigravity", "google")]));
    // The Pro capacity is exhausted; Flash (no matching window) is untouched.
    let pro = snap
        .by_capacity
        .get(&make_capacity_key("google", "antigravity-gemini-3.1-pro"))
        .unwrap();
    assert!(pro.quota_exhausted_until.is_some_and(|e| e > NOW));
    assert!(
        !snap
            .by_capacity
            .contains_key(&make_capacity_key("google", "antigravity-gemini-3.5-flash")),
        "the misleading aggregate must NOT pressure Flash"
    );
}

#[test]
fn fresh_soft_pressure_lowers_stale_pressure() {
    let facts = vec![fact("openai", "gpt-5.5", 30.0)];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 70.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let cap = make_capacity_key("openai", "gpt-5.5");
    let map = name_map(&[("codex", "openai")]);
    let expected = overlay(&windows, &facts, &map)
        .by_capacity
        .get(&cap)
        .and_then(|s| s.quota_pressure)
        .expect("fresh pressure");

    let mut snap = ModelRouteStateSnapshot {
        now: NOW,
        by_route: HashMap::new(),
        by_capacity: HashMap::from([(
            cap.clone(),
            RouteRuntimeState {
                quota_pressure: Some(1.0),
                ..Default::default()
            },
        )]),
    };
    merge_quota_overlay_with(
        &mut snap,
        &windows,
        &facts,
        &map,
        NOW,
        &DEFAULT_PACE_CONFIG,
        DEFAULT_HARD_BLOCK_THRESHOLD,
    );

    assert!(
        expected < 1.0,
        "fresh reading should be lower than the stale stored pressure"
    );
    assert_eq!(
        snap.by_capacity.get(&cap).and_then(|s| s.quota_pressure),
        Some(expected),
        "a fresh quota reading should replace stored soft pressure, even when it is lower"
    );
}

#[test]
fn hard_exhaustion_retained_until_future_reset_passes() {
    let facts = vec![fact("openai", "gpt-5.5", 30.0)];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let cap = make_capacity_key("openai", "gpt-5.5");
    let map = name_map(&[("codex", "openai")]);
    let fresh_until = overlay(&windows, &facts, &map)
        .by_capacity
        .get(&cap)
        .and_then(|s| s.quota_exhausted_until)
        .expect("fresh exhaustion");
    let retained_until = fresh_until + 60_000.0;

    let mut snap = ModelRouteStateSnapshot {
        now: NOW,
        by_route: HashMap::new(),
        by_capacity: HashMap::from([(
            cap.clone(),
            RouteRuntimeState {
                quota_exhausted_until: Some(retained_until),
                ..Default::default()
            },
        )]),
    };
    merge_quota_overlay_with(
        &mut snap,
        &windows,
        &facts,
        &map,
        NOW,
        &DEFAULT_PACE_CONFIG,
        DEFAULT_HARD_BLOCK_THRESHOLD,
    );

    assert_eq!(
        snap.by_capacity
            .get(&cap)
            .and_then(|s| s.quota_exhausted_until),
        Some(retained_until),
        "a confirmed future exhaustion should stay in force until its own reset passes"
    );
}

#[test]
fn exhausted_provider_loses_selection_to_unpressured_peer_end_to_end() {
    let facts = vec![fact("openai", "gpt-5.5", 5.0), fact("xai", "grok-4.3", 5.0)];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let snap = overlay(&windows, &facts, &name_map(&[("codex", "openai")]));

    let mut available = std::collections::HashSet::new();
    for f in &facts {
        available.insert(make_route_key(&f.provider_id, &f.model_id, None));
        available.insert(make_capacity_key(&f.provider_id, &f.model_id));
    }
    let r = select_model_route(SelectModelRouteInput {
        target_agent: "mason".into(),
        requirements: ModelRequirements { iq: 80.0, eq: 20.0 },
        available_models: available,
        facts: facts.clone(),
        runtime_snapshot: Some(snap),
        exclude_route_keys: Vec::new(),
        required_capabilities: None,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: std::collections::HashSet::new(),
    })
    .unwrap();
    assert_eq!(
        r.model.provider_id, "xai",
        "exhausted openai should lose to unpressured xai"
    );
}

#[test]
fn published_api_provider_slug_reaches_routes_the_local_map_never_heard_of() {
    // The alibaba dead-signal incident (2026-08-05): QTA served the token plan
    // as provider="qwen-cloud" with apiProvider="alibaba-token-plan". The wire
    // name is absent from our local map, so before the slug was honored the
    // entry was dropped at the name gate and the provider ran for weeks at 72%
    // of its 5h window while every decision read "quota signal: none". The
    // published slug must key the entry WITHOUT a local-map entry, and the
    // local map (deliberately empty here) must not be consulted at all.
    let facts = vec![fact("alibaba-token-plan", "qwen3.8-max", 6.0)];
    let entry = usage_from_json(serde_json::json!({
        "provider": "qwen-cloud",
        "apiProvider": "alibaba-token-plan",
        "usage": { "primary": {
            "usedPercent": 73.3,
            "windowMinutes": 300,
            "resetsAt": "2100-01-01T00:00:00Z"
        }}
    }));
    let snap = overlay(&[entry], &facts, &name_map(&[]));
    let state = snap
        .by_capacity
        .get("alibaba-token-plan/qwen3.8-max")
        .expect("published slug must key the entry to its provider capacities");
    assert!(
        state.quota_pressure.is_some(),
        "a healthy window must contribute pressure through the slug path"
    );
}
