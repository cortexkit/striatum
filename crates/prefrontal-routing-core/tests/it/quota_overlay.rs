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
use prefrontal_routing_core::quota_pace::{
    project_quota_pressure, PaceError, PaceInput, DEFAULT_PACE_CONFIG,
};
use prefrontal_routing_core::{
    extract_windows, make_capacity_key, make_route_key, ModelRequirements,
};

const NOW: f64 = 1_000_000.0;

fn fact(provider: &str, model: &str, cost: f64) -> ModelFact {
    ModelFact {
        billing_plane: None,
        auth_methods: None,
        speed_is_fleet_median: false,
        provider_id: provider.into(),
        model_id: model.into(),
        variant: None,
        family: None,
        scores: ModelScores {
            elo: 60.0,
            eq: 1800.0,
            speed: 60.0,
        },
        context_window: None,
        max_output_tokens: None,
        cost_tier: None,
        cost_per_m_tok_out: Some(cost),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: None,
        supports_tool_use: None,
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
        score_provenance: prefrontal_routing_core::ScoreProvenance::Curated,
        aa_index: None,
        review_by_ms: None,
        eq_source: None,
        class_vector: None,
        class_vector_is_overlay: false,
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
fn rolling_window_discriminator_is_typed_as_an_unsupported_pacing_model() {
    let entry = usage_from_json(serde_json::json!({
        "provider": "codex",
        "usage": {
            "primary": {
                "usedPercent": 50.0,
                "resetsAt": "2030-01-01T00:00:00Z",
                "windowMinutes": 300,
                "windowModel": "rolling"
            }
        }
    }));
    let extracted = extract_windows(&entry, "openai", "codex", &[])
        .pop()
        .expect("rolling provider window is still decoded for typed refusal");

    let result = project_quota_pressure(
        PaceInput {
            utilization: extracted.utilization,
            resets_at: extracted.resets_at,
            window_ms: extracted.window_ms,
            observed_at: NOW,
            provider_window_model: extracted.window_model,
        },
        &DEFAULT_PACE_CONFIG,
    );

    assert_eq!(result, Err(PaceError::RollingWindow));
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
        operator_exclude_keys: Vec::new(),

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

#[test]
fn one_exhausted_account_beside_a_free_sibling_does_not_wall_the_provider() {
    // QTA's multi-account contract: entries are atomic per (provider, account),
    // serving picks the best account, and a wall is per-account. Live incident
    // 2026-08-10: one anthropic account at 99-100% weekly walled opus-5 for the
    // whole provider while three sibling accounts sat at 5-8%.
    let facts = vec![fact("anthropic", "claude-opus-5", 25.0)];
    let windows = vec![
        usage_from_json(serde_json::json!({
            "provider": "claude", "apiProvider": "anthropic", "account": "acct-walled",
            "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z", "windowMinutes": 10080 } }
        })),
        usage_from_json(serde_json::json!({
            "provider": "claude", "apiProvider": "anthropic", "account": "acct-free",
            "usage": { "primary": { "usedPercent": 5.0, "resetsAt": "2030-01-01T00:00:00Z", "windowMinutes": 10080 } }
        })),
    ];
    let snap = overlay(&windows, &facts, &HashMap::new());
    let state = snap
        .by_capacity
        .get(&make_capacity_key("anthropic", "claude-opus-5"))
        .expect("capacity signal present");
    assert!(
        state.quota_exhausted_until.is_none(),
        "a free sibling account must keep the capacity unwalled, got {:?}",
        state.quota_exhausted_until
    );
}

#[test]
fn all_accounts_exhausted_walls_at_the_earliest_reset() {
    // When EVERY account is walled the capacity is genuinely exhausted, and it
    // recovers at the EARLIEST reset: the first account to free unblocks the
    // provider.
    let facts = vec![fact("anthropic", "claude-opus-5", 25.0)];
    let windows = vec![
        usage_from_json(serde_json::json!({
            "provider": "claude", "apiProvider": "anthropic", "account": "acct-a",
            "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-06-01T00:00:00Z", "windowMinutes": 10080 } }
        })),
        usage_from_json(serde_json::json!({
            "provider": "claude", "apiProvider": "anthropic", "account": "acct-b",
            "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z", "windowMinutes": 10080 } }
        })),
    ];
    let snap = overlay(&windows, &facts, &HashMap::new());
    let state = snap
        .by_capacity
        .get(&make_capacity_key("anthropic", "claude-opus-5"))
        .expect("capacity signal present");
    let expected_earliest = wall_ms_for("2030-01-01T00:00:00Z");
    assert_eq!(
        state.quota_exhausted_until,
        Some(expected_earliest),
        "all accounts walled -> the earliest reset unblocks"
    );
}

fn wall_ms_for(rfc3339: &str) -> f64 {
    // Reuse the same parse path the overlay uses via a decoded window round-trip:
    // build an entry whose resets_at is the target stamp and read the extracted
    // value back through the public snapshot (avoids depending on a datetime
    // crate in the test).
    let facts = vec![fact("zzz-probe", "probe-model", 1.0)];
    let windows = vec![usage_from_json(serde_json::json!({
        "provider": "zzz", "apiProvider": "zzz-probe", "account": "only",
        "usage": { "primary": { "usedPercent": 100.0, "resetsAt": rfc3339, "windowMinutes": 60 } }
    }))];
    let snap = overlay(&windows, &facts, &HashMap::new());
    snap.by_capacity
        .get(&make_capacity_key("zzz-probe", "probe-model"))
        .and_then(|s| s.quota_exhausted_until)
        .expect("probe wall present")
}

#[test]
fn spend_pools_decode_additively_and_unknown_funding_cannot_spend() {
    let entry = usage_from_json(serde_json::json!({
        "provider": "balance-provider",
        "futureProviderField": { "kept": "opaque" },
        "usage": {},
        "spend": [
            {
                "id": "future-fund",
                "label": "Future fund",
                "funding": "issuer_credit_v2",
                "remaining": { "minor": 100, "exponent": 2, "unit": "USD" },
                "total": { "minor": 100, "exponent": 2, "unit": "USD" },
                "basis": "reported",
                "spendable": true,
                "futurePoolField": true
            },
            {
                "id": "banked-credits",
                "label": "Banked credits",
                // The literal "unknown" is what insula's PoolFunding::Unknown
                // actually serializes for codex banked credits: it is a distinct
                // shape from a novel string even though our arm-less String
                // decode treats both identically, so both are driven.
                "funding": "unknown",
                "remaining": { "minor": 500, "exponent": 2, "unit": "USD" },
                "basis": "reported",
                "spendable": true
            },
            {
                "id": "subscription",
                "label": "Subscription",
                "funding": "subscription",
                "remaining": { "minor": 100, "exponent": 2, "unit": "USD" },
                "total": { "minor": 100, "exponent": 2, "unit": "USD" },
                "basis": "reported",
                "spendable": true
            }
        ]
    }));

    assert!(
        entry.is_healthy(),
        "reported pools are a valid quota response"
    );
    assert_eq!(entry.spend[0].funding, "issuer_credit_v2");
    assert_eq!(entry.spend[0].basis, "reported");
    assert!(
        !entry.spend[0].has_supported_funding(),
        "unknown funding must remain not-granted-do-not-spend"
    );
    assert_eq!(entry.spend[1].funding, "unknown");
    assert!(
        !entry.spend[1].has_supported_funding(),
        "the literal \"unknown\" (codex banked credits) must be not-spendable through the same path"
    );
    assert!(entry.spend[2].has_supported_funding());
}

#[test]
fn windowless_usage_with_empty_balance_pool_walls_without_reset() {
    let facts = vec![fact("balance-provider", "budget-model", 1.0)];
    let entry = usage_from_json(serde_json::json!({
        "provider": "balance-wire",
        "apiProvider": "balance-provider",
        "usage": {},
        "spend": [{
            "id": "subscription",
            "label": "Subscription balance",
            "funding": "subscription",
            "remaining": { "minor": 0, "exponent": 2, "unit": "USD" },
            "total": { "minor": 100, "exponent": 2, "unit": "USD" },
            "basis": "reported",
            "spendable": true
        }]
    }));

    assert!(
        extract_windows(&entry, "balance-provider", "balance-wire", &[]).is_empty(),
        "usage={{}} stays no-signal for the rate-window arm even when spend is present"
    );
    let snap = overlay(&[entry], &facts, &HashMap::new());
    let state = snap
        .by_capacity
        .get(&make_capacity_key("balance-provider", "budget-model"))
        .expect("the spend-pool arm contributes a provider-scoped signal");
    assert_eq!(
        state.quota_exhausted_until,
        Some(NOW + NO_RESET_REPROBE_HORIZON_MS),
        "an actual empty balance has no provider reset and must wall through the no-reset path"
    );
    assert!(
        state.quota_pressure.is_some(),
        "the same pool contributes soft pacing alongside its wall"
    );
}

#[test]
fn pool_without_spendable_flag_decodes_and_serves() {
    // Producer contract (cortexkit-provider-usage 0.6): `spendable` is
    // skip_serializing_if - ABSENT means the provider does not say, and only a
    // literal false closes a pool. Requiring the field refused the whole
    // usage.get reply (caught live by both_modules_e2e against real insula:
    // deepseek omits the flag when balance is sufficient, openrouter always).
    let facts = vec![fact("absent-provider", "budget-model", 1.0)];
    let entry = usage_from_json(serde_json::json!({
        "provider": "absent-wire",
        "apiProvider": "absent-provider",
        "usage": {},
        "spend": [{
            "id": "grant",
            "label": "Grant balance",
            "funding": "granted",
            "remaining": { "minor": 10, "exponent": 2, "unit": "USD" },
            "total": { "minor": 100, "exponent": 2, "unit": "USD" },
            "basis": "reported"
        }]
    }));
    let snap = overlay(&[entry], &facts, &HashMap::new());
    let cap = make_capacity_key("absent-provider", "budget-model");
    assert!(
        snap.by_capacity[&cap].quota_exhausted_until.is_none(),
        "an absent spendable flag must not wall the pool: absent is 'does not say', not 'closed'"
    );
    assert!(
        snap.by_capacity[&cap].quota_pressure.is_some(),
        "an absent-flag pool still informs pacing"
    );
}

#[test]
fn balance_only_positive_pool_keeps_provider_servable() {
    let facts = vec![fact("balance-provider", "budget-model", 1.0)];
    let entry = usage_from_json(serde_json::json!({
        "provider": "balance-wire",
        "apiProvider": "balance-provider",
        "usage": {},
        "spend": [{
            "id": "grant",
            "label": "Grant balance",
            "funding": "granted",
            "remaining": { "minor": 10, "exponent": 2, "unit": "USD" },
            "total": { "minor": 100, "exponent": 2, "unit": "USD" },
            "basis": "reported",
            "spendable": true
        }]
    }));
    let snap = overlay(&[entry], &facts, &HashMap::new());
    let cap = make_capacity_key("balance-provider", "budget-model");
    assert!(
        snap.by_capacity[&cap].quota_exhausted_until.is_none(),
        "a positive balance must not be walled"
    );
    assert!(
        snap.by_capacity[&cap].quota_pressure.is_some(),
        "the remaining-to-total ratio supplies a best-effort pacing signal"
    );

    let available = std::collections::HashSet::from([
        make_route_key("balance-provider", "budget-model", None),
        cap,
    ]);
    let selected = select_model_route(SelectModelRouteInput {
        target_agent: "mason".into(),
        requirements: ModelRequirements { iq: 80.0, eq: 20.0 },
        available_models: available,
        facts,
        runtime_snapshot: Some(snap),
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),

        required_capabilities: None,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: std::collections::HashSet::new(),
    })
    .expect("a positive balance-only provider remains selectable");
    assert_eq!(selected.model.provider_id, "balance-provider");
}

#[test]
fn derived_pool_remainder_can_pace_but_never_walls() {
    let facts = vec![fact("balance-provider", "budget-model", 1.0)];
    let entry = usage_from_json(serde_json::json!({
        "provider": "balance-wire",
        "apiProvider": "balance-provider",
        "usage": {},
        "spend": [{
            "id": "derived-ceiling",
            "label": "Derived ceiling",
            "funding": "subscription",
            "remaining": { "minor": 0, "exponent": 2, "unit": "USD" },
            "total": { "minor": 100, "exponent": 2, "unit": "USD" },
            "basis": "derived",
            "spendable": true
        }]
    }));
    let snap = overlay(&[entry], &facts, &HashMap::new());
    let state = &snap.by_capacity[&make_capacity_key("balance-provider", "budget-model")];

    assert!(
        state.quota_pressure.is_some_and(|pressure| pressure > 0.0),
        "the derived ceiling may still inform pacing"
    );
    assert!(
        state.quota_exhausted_until.is_none(),
        "derived balance data must never create a hard exhaustion wall"
    );
}

/// The live `usage.get` result that stopped routing's quota refresh on
/// 2026-10-02 at 16:56Z: insula served one Codex account's prepaid-credits pool
/// with `basis: "unstated"` and no `remaining` balance (its sibling still
/// carried 62500 credits). Requiring `remaining` failed the decode of the WHOLE
/// array, so every refresh errored and selection kept reading the 16:55Z
/// snapshot for hours. The array must decode, and a pool with no stated balance
/// is unknown: it neither walls nor paces, even when its funding may spend.
#[test]
fn spend_pool_without_a_stated_balance_decodes_and_neither_walls_nor_paces() {
    let result = serde_json::json!([
        {
            "provider": "codex", "apiProvider": "openai",
            "account": "00000000-0000-4000-8000-0000000000a2",
            "usage": { "primary": { "usedPercent": 2.0, "windowMinutes": 10080 } },
            "spend": [{
                "basis": "reported", "funding": "unknown", "id": "credits",
                "label": "Prepaid credits",
                "remaining": { "exponent": 0, "minor": 62500, "unit": "credit" }
            }]
        },
        {
            "provider": "codex", "apiProvider": "openai",
            "account": "00000000-0000-4000-8000-0000000000a3",
            "usage": { "primary": { "usedPercent": 2.0, "windowMinutes": 10080 } },
            "spend": [{
                "basis": "unstated", "funding": "unknown", "id": "credits",
                "label": "Prepaid credits"
            }]
        }
    ]);
    let windows: Vec<ProviderUsage> = serde_json::from_value(result)
        .expect("a pool with no stated balance must not fail the whole usage.get array");
    assert_eq!(windows.len(), 2);
    assert!(windows.iter().all(ProviderUsage::is_healthy));
    assert_eq!(windows[1].spend[0].remaining, None);
    assert!(!windows[1].spend[0].is_known_empty());

    // The same unstated shape on a pool whose funding MAY spend: still unknown,
    // so it must not wall the provider or add pressure.
    let facts = vec![fact("balance-provider", "budget-model", 1.0)];
    let granted_unstated = usage_from_json(serde_json::json!({
        "provider": "balance-wire",
        "apiProvider": "balance-provider",
        "usage": {},
        "spend": [{
            "id": "grant", "label": "Grant balance", "funding": "granted",
            "basis": "unstated", "spendable": true
        }]
    }));
    let snap = overlay(&[granted_unstated], &facts, &HashMap::new());
    let state = snap
        .by_capacity
        .get(&make_capacity_key("balance-provider", "budget-model"));
    assert!(
        state.is_none_or(
            |state| state.quota_exhausted_until.is_none() && state.quota_pressure.is_none()
        ),
        "an unstated balance is unknown, never empty: {state:?}"
    );

    // The persisted snapshot round-trips the absent balance as absent.
    let encoded = serde_json::to_value(&windows[1]).unwrap();
    assert!(encoded["spend"][0].get("remaining").is_none());
    let decoded: ProviderUsage = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, windows[1]);
}

/// An idle account is a FREE account, and the across-account fold walls a
/// capacity only when every account holding a signal for it is walled. An
/// account that contributes no signal holds none, so an idle sibling used to
/// vanish and one exhausted account walled the whole provider - the selector
/// then returned a healthy but more expensive route, with no error anywhere
/// (Loop R138). The two controls run first: the same shape with a 5%-used
/// sibling, and with an exactly-idle sibling that reports a reset, both of
/// which always kept their account represented.
#[test]
fn an_idle_account_keeps_its_provider_off_the_all_accounts_wall() {
    let facts = vec![
        fact("anthropic", "claude-opus-5", 1.0),
        fact("xai", "grok-4.3", 100.0),
    ];
    let selected_provider = |free_window: serde_json::Value| -> (String, Option<f64>) {
        let windows = vec![
            usage_from_json(serde_json::json!({
                "provider": "claude", "apiProvider": "anthropic", "account": "walled",
                "usage": {"primary": {"usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z"}}
            })),
            usage_from_json(serde_json::json!({
                "provider": "claude", "apiProvider": "anthropic", "account": "free",
                "usage": {"primary": free_window}
            })),
        ];
        assert!(
            windows.iter().all(ProviderUsage::is_healthy),
            "both accounts must be healthy, or the specimen is about health rather than membership"
        );
        let snap = overlay(&windows, &facts, &HashMap::new());
        let wall = snap
            .by_capacity
            .get("anthropic/claude-opus-5")
            .and_then(|s| s.quota_exhausted_until);
        let available = facts
            .iter()
            .map(|f| make_capacity_key(&f.provider_id, &f.model_id))
            .collect();
        let result = select_model_route(SelectModelRouteInput {
            target_agent: "mason".into(),
            requirements: ModelRequirements { iq: 80.0, eq: 20.0 },
            available_models: available,
            facts: facts.clone(),
            runtime_snapshot: Some(snap),
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),

            required_capabilities: None,
            is_subscription_route: None,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: Default::default(),
        })
        .unwrap();
        (result.model.provider_id, wall)
    };

    let (busy_sibling, busy_wall) = selected_provider(serde_json::json!({"usedPercent": 5.0}));
    assert_eq!(
        busy_sibling, "anthropic",
        "a 5%-used sibling keeps the provider available"
    );
    assert_eq!(busy_wall, None, "and leaves no wall behind");

    let (reset_sibling, reset_wall) = selected_provider(
        serde_json::json!({"usedPercent": 0.0, "resetsAt": "2030-01-01T00:00:00Z"}),
    );
    assert_eq!(
        reset_sibling, "anthropic",
        "an idle sibling reporting a reset keeps it available"
    );
    assert_eq!(reset_wall, None);

    // The case that used to disappear: idle, with no reset to report.
    let (idle_sibling, idle_wall) = selected_provider(serde_json::json!({"usedPercent": 0.0}));
    assert_eq!(
        idle_wall, None,
        "an idle account is unwalled, so the capacity must not be walled by its exhausted sibling"
    );
    assert_eq!(
        idle_sibling, "anthropic",
        "an idle sibling is no less available than a 5%-used one"
    );
}

/// A provider can report a window fully used AND declare semantics this router
/// will not project. Those are different facts: refusing to guess the burn-rate
/// trajectory of a rolling pool must not discard the measurement that the pool is
/// spent. Skipping the window outright left no capacity state at all, and absent
/// state is read as available, so the selector chose the exhausted route with the
/// cheaper price (Loop R140).
#[test]
fn an_exhausted_rolling_window_still_walls_its_route() {
    let facts = vec![
        fact("openai", "gpt-5.4", 1.0),
        fact("anthropic", "claude-opus-4-7", 100.0),
    ];
    let chosen = |window_model: Option<&str>, used: f64| -> (String, Option<f64>) {
        let mut primary = serde_json::json!({ "usedPercent": used });
        if let Some(model) = window_model {
            primary["windowModel"] = model.into();
        }
        let windows = vec![usage_from_json(serde_json::json!({
            "provider": "codex", "apiProvider": "openai", "account": "only",
            "usage": { "primary": primary }
        }))];
        assert!(
            windows.iter().all(ProviderUsage::is_healthy),
            "the entry must be healthy, or the specimen is about health rather than pacing"
        );
        let snap = overlay(&windows, &facts, &HashMap::new());
        let wall = snap
            .by_capacity
            .get("openai/gpt-5.4")
            .and_then(|state| state.quota_exhausted_until);
        let available = facts
            .iter()
            .map(|f| make_capacity_key(&f.provider_id, &f.model_id))
            .collect();
        let result = select_model_route(SelectModelRouteInput {
            target_agent: "mason".into(),
            requirements: ModelRequirements { iq: 50.0, eq: 50.0 },
            available_models: available,
            facts: facts.clone(),
            runtime_snapshot: Some(snap),
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),

            required_capabilities: None,
            is_subscription_route: None,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: Default::default(),
        })
        .unwrap();
        (result.model.model_id, wall)
    };

    // Controls: the same exhaustion under semantics the router DOES project.
    let (absent_marker, absent_wall) = chosen(None, 100.0);
    assert_eq!(
        absent_marker, "claude-opus-4-7",
        "an unmarked exhausted window walls"
    );
    assert!(absent_wall.is_some());
    let (fixed_marker, fixed_wall) = chosen(Some("fixed"), 100.0);
    assert_eq!(
        fixed_marker, "claude-opus-4-7",
        "a fixed exhausted window walls"
    );
    assert!(fixed_wall.is_some());

    // An idle rolling window must still leave the cheap route selectable: the rule
    // is about the measurement, not about distrusting rolling pools.
    let (idle_rolling, idle_wall) = chosen(Some("rolling"), 0.0);
    assert_eq!(
        idle_rolling, "gpt-5.4",
        "an idle rolling window walls nothing"
    );
    assert_eq!(idle_wall, None);

    // The case that used to vanish.
    let (exhausted_rolling, rolling_wall) = chosen(Some("rolling"), 100.0);
    assert!(
        rolling_wall.is_some(),
        "a reported-spent rolling pool must record its wall even with no projection"
    );
    assert_eq!(
        exhausted_rolling, "claude-opus-4-7",
        "an exhausted rolling route must not be selected just because its pacing is unprojectable"
    );
}
