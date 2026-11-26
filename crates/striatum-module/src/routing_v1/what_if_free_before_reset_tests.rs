use super::*;
use crate::account_quota::{apply_usage_reply, AccountQuotaState};
use striatum_core::subscription::free_before_reset::HOUR_MS;

const GROK_PROVIDER: &str = "xai";
const GROK_ROUTE: &str = "xai/grok-4.7";

fn install_grok(harness: &std::sync::Arc<Harness>) {
    let mut fact = kimi_fact();
    fact.provider_id = GROK_PROVIDER.into();
    fact.model_id = "grok-4.7".into();
    fact.cost_per_m_tok_out = Some(20.0);
    fact.input_rate = Some(5.0);
    fact.cache_read_rate = Some(1.0);
    let mut facts = harness.store().get_facts().unwrap();
    facts.push(fact);
    harness.install_facts(&facts);
    harness.edit_config(|config| {
        config["providers"][GROK_PROVIDER] = json!({
            "pin_only":false, "subscription":true,
            "subscription_multiplier":1, "models":["grok-4.7"],
        });
    });
}

fn grok_reading(at: i64, reset: i64, used: f64, short: Option<(i64, f64)>) -> Value {
    let date = crate::account_selection::rfc3339;
    let mut reading = json!({
        "provider":GROK_PROVIDER,"apiProvider":GROK_PROVIDER,"account":"catalog-account-xai-apikey",
        "source":"vault","fetchedAt":date(at),
        "usage":{"primary":{"usedPercent":used,"windowMinutes":10080,"resetsAt":date(reset)}}
    });
    if let Some((reset, used)) = short {
        reading["usage"]["secondary"] =
            json!({"usedPercent":used,"windowMinutes":300,"resetsAt":date(reset)});
    }
    reading
}

fn push(state: &mut AccountQuotaState, at: i64, entry: Value) {
    apply_usage_reply(
        state,
        &serde_json::to_vec(&json!({"result":[entry]})).unwrap(),
        at,
    )
    .unwrap();
}

fn candidate<'a>(row: &'a Value, route: &str) -> &'a Value {
    row["adequate"]
        .as_array()
        .unwrap()
        .iter()
        .find(|candidate| candidate["route"] == route)
        .unwrap_or_else(|| panic!("{route} not adequate: {row}"))
}

async fn sweep(
    harness: &std::sync::Arc<Harness>,
    fixture: &Value,
    state: AccountQuotaState,
) -> Value {
    let _pools = crate::subscription_economics::test_pools_with_recent_pace(
        &state,
        scenario_pools(fixture, false),
        KIMI_NOW,
    );
    publish_kimi_state(state);
    let lookup = kimi_lookup("prefrontal.mason", fixture);
    let with_grok = move |workload: &str, iq: f64, facts: &[striatum_core::ModelFact]| {
        let mut prices = lookup(workload, iq, facts);
        if let Some(profile) = prices.profiles.get(KIMI_ROUTE).cloned() {
            prices.profiles.insert(GROK_ROUTE.into(), profile);
        }
        prices
    };
    operator(harness)
        .await
        .what_if_with_profiles(
            &json!({"consumer":"prefrontal.mason","iqs":[50],"free_before_reset_preview":true}),
            &with_grok,
        )
        .await
        .unwrap()["results"][0]
        .clone()
}

#[tokio::test]
async fn quota_events_with_reset_jitter_show_pace_in_what_if_preview() {
    let _clock = crate::InjectedClock::set(KIMI_NOW as u64);
    let (harness, scenario, fixture) = setup(false);
    install_grok(&harness);
    let listing = install_key_only_kimi(&harness);
    harness.edit_config(|config| {
        config["consumers"]["prefrontal.mason"]["mode"] = json!("demand");
    });
    drop(scenario);

    let accounts = std::sync::Arc::new(std::sync::Mutex::new(kimi_state(&listing, false)));
    let minute = 60_000;
    let reset = KIMI_NOW + HOUR_MS;
    let samples = [
        (KIMI_NOW - 75 * minute, reset, 15.0),
        (KIMI_NOW - 30 * minute, reset + 1_000, 18.0),
        (KIMI_NOW, reset + 2_000, 20.0),
    ];
    crate::quota_events::store_test_samples(
        harness.store().clone(),
        accounts.clone(),
        samples
            .into_iter()
            .enumerate()
            .map(|(index, (at, reset, used))| {
                let mut sample = grok_reading(at, reset, used, None);
                sample["epoch"] = json!("grok-epoch");
                sample["seq"] = json!(index + 1);
                (at, sample)
            }),
    );

    let state = accounts.lock().unwrap().clone();
    let row = sweep(&harness, &fixture, state).await;
    let price = &candidate(&row, GROK_ROUTE)["free_before_reset_preview"];
    assert_eq!(price["pace_points_per_hour"], 4.0, "{price}");
    assert_eq!(price["free_before_reset"], true);
    assert_eq!(price["final_cost_usd"], 0.0);
}

#[tokio::test]
async fn grok_overtakes_a_cheaper_route_and_free_ties_spend_the_soonest_reset() {
    let _clock = crate::InjectedClock::set(KIMI_NOW as u64);
    let (harness, scenario, fixture) = setup(false);
    install_grok(&harness);
    let listing = install_key_only_kimi(&harness);
    harness.edit_config(|config| {
        config["consumers"]["prefrontal.mason"]["mode"] = json!("demand");
    });
    drop(scenario);
    let grok_reset = KIMI_NOW + HOUR_MS;
    let old = KIMI_NOW - HOUR_MS;
    for kimi_reset in [None, Some(KIMI_NOW + 2 * HOUR_MS), Some(grok_reset)] {
        let mut state = kimi_state(&listing, false);
        for (at, grok_used, kimi_used) in [(old, 15.0, 5.0), (KIMI_NOW, 16.0, 6.0)] {
            push(
                &mut state,
                at,
                grok_reading(at, grok_reset, grok_used, None),
            );
            if at == KIMI_NOW || kimi_reset.is_some() {
                let mut reading = kimi_reading();
                reading["fetchedAt"] = json!(crate::account_selection::rfc3339(at));
                reading["usage"]["primary"]["usedPercent"] = json!(kimi_used);
                if let Some(reset) = kimi_reset {
                    reading["usage"]["primary"]["resetsAt"] =
                        json!(crate::account_selection::rfc3339(reset));
                }
                push(&mut state, at, reading);
            }
        }
        let row = sweep(&harness, &fixture, state).await;
        let grok = candidate(&row, GROK_ROUTE);
        let kimi = candidate(&row, KIMI_ROUTE);
        let price = &grok["free_before_reset_preview"];
        assert!(
            grok["task_pricing"]["listCostUsd"].as_f64().unwrap()
                > kimi["task_pricing"]["listCostUsd"].as_f64().unwrap()
        );
        assert_ne!(row["pick"]["route"], GROK_ROUTE);
        assert_eq!(price["window"], "primary");
        assert_eq!(price["pace_points_per_hour"], 1.0);
        assert_eq!(price["hours_to_reset"], 1.0);
        assert_eq!(price["projected_leftover_points"], 83.0);
        assert_eq!(price["free_before_reset"], true);
        assert_eq!(price["final_cost_usd"], 0.0);
        assert_eq!(
            kimi["free_before_reset_preview"]["free_before_reset"],
            kimi_reset.is_some()
        );
        assert_eq!(
            row["free_before_reset_pick"]["route"],
            if kimi_reset == Some(grok_reset) {
                KIMI_ROUTE
            } else {
                GROK_ROUTE
            },
            "Kimi reset: {kimi_reset:?}; pick: {}; Grok: {grok}; Kimi: {kimi}",
            row["free_before_reset_pick"]
        );
    }
}

#[tokio::test]
async fn preview_keeps_today_for_missing_short_corrected_or_non_surplus_pace_and_binding_short_window(
) {
    let _clock = crate::InjectedClock::set(KIMI_NOW as u64);
    let (harness, scenario, fixture) = setup(false);
    install_grok(&harness);
    let listing = install_key_only_kimi(&harness);
    harness.edit_config(|config| {
        config["consumers"]["prefrontal.mason"]["mode"] = json!("demand");
    });
    drop(scenario);
    for (label, earlier, latest, hours, short) in [
        ("no weekly reading", None, None, 1, false),
        ("one reading", None, Some(16.0), 1, false),
        (
            "half an hour",
            Some((HOUR_MS / 2, 15.0)),
            Some(16.0),
            1,
            false,
        ),
        (
            "usage corrected down",
            Some((HOUR_MS, 17.0)),
            Some(16.0),
            1,
            false,
        ),
        (
            "exactly 5 points left",
            Some((HOUR_MS, 45.0)),
            Some(70.0),
            1,
            false,
        ),
        (
            "on pace to use the full budget",
            Some((HOUR_MS, 40.0)),
            Some(70.0),
            1,
            false,
        ),
        (
            "more than 24 hours",
            Some((HOUR_MS, 15.0)),
            Some(16.0),
            25,
            false,
        ),
        (
            "binding short window",
            Some((HOUR_MS, 15.0)),
            Some(16.0),
            1,
            true,
        ),
    ] {
        let mut state = kimi_state(&listing, true);
        let reset = KIMI_NOW + hours * HOUR_MS;
        let short_reset = KIMI_NOW + HOUR_MS;
        if let Some((age, used)) = earlier {
            let at = KIMI_NOW - age;
            push(
                &mut state,
                at,
                grok_reading(at, reset, used, short.then_some((short_reset, 60.0))),
            );
        }
        if let Some(used) = latest {
            push(
                &mut state,
                KIMI_NOW,
                grok_reading(KIMI_NOW, reset, used, short.then_some((short_reset, 80.0))),
            );
        }
        let row = sweep(&harness, &fixture, state).await;
        let grok = candidate(&row, GROK_ROUTE);
        let preview = &grok["free_before_reset_preview"];
        assert_eq!(preview["free_before_reset"], short, "{label}: {grok}");
        assert_eq!(
            preview["final_cost_usd"], grok["task_pricing"]["finalCostUsd"],
            "{label}: {grok}"
        );
        assert_eq!(
            row["free_before_reset_pick"]["route"], row["pick"]["route"],
            "{label}: {row}"
        );
        if short {
            assert_eq!(preview["weekly_pressure"], 0.0);
            assert_eq!(
                preview["pressure"],
                grok["task_pricing"]["shortWindowPressure"]
            );
            assert_eq!(
                preview["binding_short_windows"][0]["projected_leftover_points"],
                0.0
            );
        }
    }
}
