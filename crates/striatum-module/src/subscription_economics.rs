//! Optional read-only economics adapters. Replies are refreshed with account quota,
//! never from route.select. Missing services and unshipped fields are normal.

use crate::{
    account_quota::{
        parse_rfc3339_ms, AccountEntry, AccountQuotaState, AgeBand, SharedAccountQuota,
    },
    quota_consumer::QuotaConsumer,
};
use striatum_core::{
    model_facts::{make_capacity_key, RoutePricing},
    model_router::ModelRouteStateSnapshot,
    subscription::{
        MeasuredValue, QuotaAccount, SubscriptionPlan, SubscriptionPool, DEFAULT_PLAN_FEE_USD,
    },
    ModelFact,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Clone, Default)]
struct EconomicsCache {
    pools: HashMap<String, SubscriptionPool>,
    unpriced_tiers: BTreeSet<String>,
    refreshed_at_ms: i64,
}
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum PlanGroup {
    Joined(String),
    // Without a report match, keep accounts grouped by roster tier and fee;
    // a tier identifies a quota budget but does not establish a measured plan.
    Unjoined(Option<String>, u64),
}
static CACHE: OnceLock<Mutex<EconomicsCache>> = OnceLock::new();
static WARNED_AMBIGUOUS_ACCOUNTS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
fn cache() -> &'static Mutex<EconomicsCache> {
    CACHE.get_or_init(|| Mutex::new(EconomicsCache::default()))
}

/// Join by account reference, not provider/tier spelling: Antigravity's quota
/// provider can differ from the prefix in its Astrocyte plan name.
fn account_plan<'a>(report: &'a Value, account_id: &str) -> Result<Option<&'a Value>, ()> {
    let Some(rows) = report["latest"].as_array() else {
        return Ok(None);
    };
    let mut matches = rows.iter().filter(|row| {
        row["plan"].is_string()
            && row["plan_prices"].as_array().is_some_and(|prices| {
                prices
                    .iter()
                    .any(|price| price["account_ref"] == account_id)
            })
    });
    let matched = matches.next();
    if matches.next().is_some() {
        Err(())
    } else {
        Ok(matched)
    }
}

fn warn_ambiguous_account(account_id: &str) {
    let mut warned = WARNED_AMBIGUOUS_ACCOUNTS
        .get_or_init(Default::default)
        .lock()
        .unwrap();
    if warned.insert(account_id.into()) {
        tracing::warn!(
            account_id,
            "Astrocyte lists an account in multiple plan rows; ignoring its plan mapping"
        );
    }
}

/// Read the joined plan's explicitly usable observation:
/// `multiplier` is API list value divided by the subscription cost of quota used.
/// `fee_multiplier` instead compares value with fees paid and is not quota value.
/// Missing plan or eligibility fields in older reports supply no measurement.
/// Age uses interval_end_ms,
/// not snapshot_at_ms: recomputing an old day cannot freshen its evidence.
fn measured(row: &Value) -> Option<MeasuredValue> {
    row["plan"].as_str()?;
    if row["routing_eligible"].as_bool() != Some(true) {
        return None;
    }
    let multiplier = row["multiplier"].as_f64()?;
    if multiplier <= 0.0 || !multiplier.is_finite() {
        return None;
    }
    Some(MeasuredValue {
        multiplier,
        interval_end_ms: row["interval_end_ms"].as_i64()?,
        row_date: row["observation_day"].as_str()?.to_string(),
    })
}

fn decimal(value: &Value, units_key: &str) -> Option<f64> {
    let units = value[units_key].as_i64()?;
    let exponent = value["exponent"].as_i64()?;
    if !(0..=18).contains(&exponent) {
        return None;
    }
    let result = units as f64 / 10_f64.powi(exponent as i32);
    (result.is_finite() && result > 0.0).then_some(result)
}

fn price_row<'a>(prices: &'a Value, provider: &str, tier: &str) -> Option<&'a Value> {
    prices["prices"]
        .as_array()?
        .iter()
        .find(|row| row["provider_id"] == provider && row["tier"] == tier)
}

fn astro_fee(row: &Value, account_id: &str, now: i64) -> Option<f64> {
    let plan = row["plan"].as_str()?;
    let plan = row["plan_prices"]
        .as_array()?
        .iter()
        .find(|price| price["account_ref"] == account_id && price["plan"] == plan)?;
    if plan["review_by_ms"]
        .as_i64()
        .is_some_and(|review| review < now)
    {
        return None;
    }
    let fee = plan["monthly_nanos"].as_f64()? / 1e9;
    (fee.is_finite() && fee > 0.0).then_some(fee)
}

/// A `credits` spend pool with a positive balance the producer has not closed
/// (`spendable: false`). The provider draws it once the plan's limit is
/// reached, so the account keeps serving past a full window.
fn carries_prepaid_credits(reading: &Value) -> bool {
    reading["spend"].as_array().is_some_and(|pools| {
        pools.iter().any(|pool| {
            pool["id"] == "credits"
                && pool["spendable"].as_bool() != Some(false)
                && pool["remaining"]["minor"]
                    .as_i64()
                    .is_some_and(|minor| minor > 0)
        })
    })
}

fn account(reading: &Value, now: i64) -> Option<QuotaAccount> {
    let usage = reading.get("usage")?;
    let mut windows: Vec<&Value> = ["primary", "secondary", "tertiary"]
        .iter()
        .filter_map(|k| usage.get(*k).filter(|w| w.is_object()))
        .collect();
    if let Some(extra) = usage["extraRateWindows"].as_array() {
        windows.extend(extra.iter().filter_map(|w| w.get("window")));
    }
    // A past reset does not make last-good free. Even a short window with expired
    // usage makes a stale account unknown until a new observation arrives.
    if reading.get("stale").is_some_and(|stale| !stale.is_null())
        && windows.iter().any(|window| {
            window["resetsAt"]
                .as_str()
                .and_then(parse_rfc3339_ms)
                .is_some_and(|reset| reset <= now)
        })
    {
        return None;
    }
    let weekly = windows.iter().find(|w| {
        w["windowKind"] == "weekly"
            || w["windowMinutes"]
                .as_i64()
                .is_some_and(|m| m >= 7 * 24 * 60)
    })?;
    let used = |w: &Value| {
        w.get("rawUsedPercent")
            .and_then(Value::as_f64)
            .or_else(|| w["usedPercent"].as_f64())
            .filter(|v| v.is_finite())
            .map(|v| v.clamp(0.0, 100.0))
    };
    let remaining = |w: &Value| -> Option<(f64, f64)> {
        let reset = parse_rfc3339_ms(w["resetsAt"].as_str()?)?;
        if reset <= now {
            return None;
        }
        let duration = w["windowMinutes"].as_i64()? as f64 * 60_000.0;
        if duration <= 0.0 {
            return None;
        }
        Some((
            ((reset - now) as f64 / duration).clamp(0.0, 1.0),
            (reset - now) as f64 / 3_600_000.0,
        ))
    };
    let (time, hours) = remaining(weekly)?;
    let mut short_walled = false;
    let mut short_windows = Vec::new();
    for w in windows.iter().filter(|w| !std::ptr::eq(**w, *weekly)) {
        if let (Some(use_percent), Some((time, _))) = (used(w), remaining(w)) {
            short_walled |= use_percent >= 100.0;
            short_windows.push(striatum_core::subscription::QuotaConstraint {
                window_minutes: w["windowMinutes"].as_i64().unwrap_or(0),
                remaining_points: 100.0 - use_percent,
                remaining_fraction: time,
            });
        }
    }
    Some(QuotaAccount {
        weekly_used: used(weekly)?,
        weekly_remaining_fraction: time,
        hours_to_reset: hours,
        short_walled,
        short_windows,
        banked_resets: reading["savedResets"]["availableCount"]
            .as_u64()
            .unwrap_or(0)
            .min(u32::MAX as u64) as u32,
        banked_expires_at_ms: reading["savedResets"]["soonestExpiresAt"]
            .as_str()
            .and_then(parse_rfc3339_ms)
            .or_else(|| {
                reading["savedResets"]["credits"]
                    .as_array()
                    .and_then(|credits| {
                        credits
                            .iter()
                            .filter_map(|credit| {
                                credit["expiresAt"].as_str().and_then(parse_rfc3339_ms)
                            })
                            .min()
                    })
            }),
        prepaid_credits: carries_prepaid_credits(reading),
    })
}

fn build(state: &AccountQuotaState, report: &Value, prices: &Value, now: i64) -> EconomicsCache {
    let mut out = EconomicsCache {
        refreshed_at_ms: now,
        ..Default::default()
    };
    let mut plans: BTreeMap<(String, PlanGroup), SubscriptionPlan> = BTreeMap::new();
    for reading in state.readings.readings.values() {
        if reading.tombstoned_at_ms.is_some() {
            continue;
        }
        let Ok(entry) = serde_json::from_value::<AccountEntry>(reading.entry.clone()) else {
            continue;
        };
        // Share accounts::eligibility::assess_reading's last-good age limit so
        // the subscription budget does not drop capacity that account placement can use.
        // Stale is usable below 30 minutes of producer age with unchanged windows.
        if entry.has_error()
            || !entry.understood_stale()
            || !matches!(entry.age(now).band, AgeBand::Full | AgeBand::Discounted)
            || (entry.stale.is_none()
                && entry
                    .age(now)
                    .age_ms
                    .is_none_or(|age| !(0..=15 * 60_000).contains(&age)))
        {
            continue;
        }
        let Some(provider) = reading.routing_provider() else {
            continue;
        };
        let roster = state
            .roster
            .as_ref()
            .and_then(|r| crate::account_quota::identity_for_reading(r, reading));
        if roster.is_some_and(|r| r.active_credential_ids.is_empty()) {
            continue;
        }
        let tier = roster.and_then(|r| r.plan_tier.clone());
        let joined = match account_plan(report, &reading.account_id) {
            Ok(row) => row,
            Err(()) => {
                warn_ambiguous_account(&reading.account_id);
                None
            }
        };
        let astro_plan = joined
            .and_then(|row| row["plan"].as_str())
            .map(str::to_string);
        let row = tier
            .as_deref()
            .and_then(|tier| price_row(prices, provider, tier));
        let fee = row
            .and_then(|r| {
                (r["price"]["currency"] == "USD")
                    .then(|| decimal(&r["price"], "minor_units"))
                    .flatten()
            })
            .or_else(|| joined.and_then(|row| astro_fee(row, &reading.account_id, now)))
            .unwrap_or(DEFAULT_PLAN_FEE_USD);
        out.pools
            .entry(provider.into())
            .or_insert_with(|| SubscriptionPool {
                provider: provider.into(),
                now_ms: now,
                ..Default::default()
            });
        let Some(account) = account(&reading.entry, now) else {
            continue;
        };
        let group = astro_plan.as_ref().map_or_else(
            || PlanGroup::Unjoined(tier.clone(), fee.to_bits()),
            |plan| PlanGroup::Joined(plan.clone()),
        );
        let plan = plans
            .entry((provider.into(), group))
            .or_insert_with(|| SubscriptionPlan {
                tier: tier.clone(),
                plan: astro_plan,
                measured: joined.and_then(measured),
                fee_usd: fee,
                accounts: Vec::new(),
            });
        // For report-joined accounts, fees are metadata rather than a grouping
        // key: capacity and value are pooled by report plan. Unjoined accounts
        // remain grouped by roster tier and fee.
        plan.fee_usd += (fee - plan.fee_usd) / (plan.accounts.len() + 1) as f64;
        if plan.tier != tier {
            plan.tier = None;
        }
        plan.accounts.push(account);
    }
    for ((provider, _), plan) in plans {
        out.pools.get_mut(&provider).unwrap().plans.push(plan);
    }
    out
}

fn fallback_plan() -> SubscriptionPlan {
    SubscriptionPlan {
        tier: None,
        plan: None,
        measured: None,
        fee_usd: DEFAULT_PLAN_FEE_USD,
        accounts: Vec::new(),
    }
}

fn credential_plan<'a>(report: &'a Value, credential_id: &str) -> Option<&'a Value> {
    let mut matches = report["latest"].as_array()?.iter().filter(|row| {
        row["plan"].is_string()
            && row["plan_prices"].as_array().is_some_and(|prices| {
                prices
                    .iter()
                    .any(|price| price["credential_ref"] == credential_id)
            })
    });
    let row = matches.next()?;
    matches.next().is_none().then_some(row)
}

fn build_with_credentials(
    state: &AccountQuotaState,
    report: &Value,
    prices: &Value,
    now: i64,
    credentials: &BTreeMap<String, BTreeSet<String>>,
) -> EconomicsCache {
    let mut out = build(state, report, prices, now);
    for (provider, ids) in credentials {
        // Leave pools created from account-quota readings unchanged, even if no
        // usable capacity was found. Credential-only providers use the existing
        // unknown-capacity fallback.
        if out.pools.contains_key(provider) {
            continue;
        }
        let mut rows = ids.iter().map(|id| credential_plan(report, id));
        let Some(Some(row)) = rows.next() else {
            continue;
        };
        // Without capacity observations there is no basis for weighting distinct
        // plans. Every mapped API-key credential must identify the same unique row.
        if !rows.all(|other| other.is_some_and(|other| std::ptr::eq(row, other))) {
            continue;
        }
        out.pools.insert(
            provider.clone(),
            SubscriptionPool {
                provider: provider.clone(),
                now_ms: now,
                plans: vec![SubscriptionPlan {
                    plan: row["plan"].as_str().map(str::to_string),
                    measured: measured(row),
                    ..fallback_plan()
                }],
                ..Default::default()
            },
        );
    }
    out
}

pub(crate) async fn fetch_plan_prices(connection: &Path, project: &Path) -> Value {
    if let Ok(mut fusi) =
        QuotaConsumer::connect_module(connection, project, "fusiform", "tool_provider").await
    {
        fusi.query(json!({"name":"plan.prices", "arguments":{}}))
            .await
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    }
}

pub(crate) async fn refresh(
    connection: &Path,
    project: &Path,
    state: &SharedAccountQuota,
    listing: Option<&cortexkit_claustrum_client::ScopedCredentialListing>,
    credentials: &crate::broca_credentials::SharedBrocaCredentials,
) {
    let now = crate::now_ms() as i64;
    let from = now - 24 * 3_600_000;
    let report = async {
        if let Ok(mut astro) =
            QuotaConsumer::connect_module(connection, project, "astrocyte", "management_surface")
                .await
        {
            astro
            .query(
                json!({"method":"spend.multiplier.report", "params":{"from_ms":from,"to_ms":now}}),
            )
            .await
            .unwrap_or(Value::Null)
        } else {
            Value::Null
        }
    };
    // Admission needs plan prices even when Astro's spend report is slow.
    // Publish them as soon as Fusiform answers, without waiting for Astro.
    let prices = async {
        let prices = fetch_plan_prices(connection, project).await;
        if let Ok(mut credentials) = credentials.lock() {
            credentials.publish_plan_prices(&prices);
        }
        prices
    };
    let (prices, report) = tokio::join!(prices, report);
    let credential_ids = if let Ok(credentials) = credentials.lock() {
        credentials.api_key_credentials_by_provider(now as u64)
    } else {
        BTreeMap::new()
    };
    let Some(snapshot) = state.lock().ok().map(|s| s.clone()) else {
        return;
    };
    let mut next = build_with_credentials(&snapshot, &report, &prices, now, &credential_ids);
    if let Some(listing) = listing {
        next.unpriced_tiers = unpriced_tiers(listing, &prices);
    }
    if let Ok(mut guard) = cache().lock() {
        *guard = next;
    }
}

fn unpriced_tiers(
    listing: &cortexkit_claustrum_client::ScopedCredentialListing,
    prices: &Value,
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for credential in &listing.credentials {
        if let Some(tier) = &credential.plan_tier {
            for provider in &credential.provider_ids {
                if price_row(prices, provider, tier)
                    .and_then(|r| {
                        (r["price"]["currency"] == "USD")
                            .then(|| decimal(&r["price"], "minor_units"))
                            .flatten()
                    })
                    .is_none()
                {
                    out.insert(format!("unpriced plan tier: {provider}/{tier}"));
                }
            }
        }
    }
    out
}

fn set_banked_allowance(pools: &mut HashMap<String, SubscriptionPool>, now: i64) {
    let free: BTreeSet<String> = pools
        .iter()
        .filter(|(_, p)| {
            p.plans
                .iter()
                .any(|plan| plan.has_free_headroom(p.keep_free_percent, false, now))
        })
        .map(|(provider, _)| provider.clone())
        .collect();
    for (provider, pool) in pools {
        pool.allow_banked = !free.iter().any(|p| p != provider);
    }
}

pub(crate) fn health_detail() -> Value {
    cache()
        .lock()
        .map(|c| json!({"refreshedAtMs":c.refreshed_at_ms,"unpricedPlanTiers":c.unpriced_tiers}))
        .unwrap_or(Value::Null)
}

pub(crate) fn overlay(
    snapshot: &mut ModelRouteStateSnapshot,
    facts: &[ModelFact],
    now: i64,
    pricing: &RoutePricing,
) {
    let pools = cache().lock().map(|c| c.pools.clone()).unwrap_or_default();
    overlay_pools(pools, snapshot, facts, now, pricing);
}

/// Attach the cached subscription pools to the selection's snapshot. Every
/// pool takes its keep-free reserve from the selection's `pricing`, including a
/// pool whose provider has no fact here: `set_banked_allowance` compares pools
/// with each other, so one pool on a reserve read at an earlier refresh would
/// change whether the others may spend banked resets.
fn overlay_pools(
    mut pools: HashMap<String, SubscriptionPool>,
    snapshot: &mut ModelRouteStateSnapshot,
    facts: &[ModelFact],
    now: i64,
    pricing: &RoutePricing,
) {
    for fact in facts {
        let pool = pools
            .entry(fact.provider_id.clone())
            .or_insert_with(|| SubscriptionPool {
                provider: fact.provider_id.clone(),
                ..Default::default()
            });
        if pool.plans.is_empty() {
            pool.plans.push(fallback_plan());
        }
    }
    for (provider, pool) in pools.iter_mut() {
        pool.keep_free_percent = pricing.keep_free_percent(provider);
        let elapsed = (now - pool.now_ms).max(0) as f64 / 3_600_000.0;
        for plan in &mut pool.plans {
            if elapsed > 0.25 {
                plan.accounts.clear();
            } else {
                for account in &mut plan.accounts {
                    if account.hours_to_reset <= elapsed {
                        account.short_walled = true;
                    }
                    if account.hours_to_reset > 0.0 {
                        account.weekly_remaining_fraction *=
                            (account.hours_to_reset - elapsed).max(0.0) / account.hours_to_reset;
                        account.hours_to_reset = (account.hours_to_reset - elapsed).max(0.0);
                    }
                }
            }
        }
        pool.now_ms = now;
    }
    // Prefer unreserved live quota on any other subscription plan before
    // crediting a banked reset as emergency capacity on the depleted plan.
    set_banked_allowance(&mut pools, now);
    let shared: HashMap<_, _> = pools
        .into_iter()
        .map(|(key, value)| (key, Arc::new(value)))
        .collect();
    for fact in facts {
        let state = snapshot
            .by_capacity
            .entry(make_capacity_key(&fact.provider_id, &fact.model_id))
            .or_default();
        state.subscription_pool = shared.get(&fact.provider_id).cloned();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential_fixture() -> (Value, BTreeMap<String, BTreeSet<String>>, i64) {
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        let report = serde_json::from_str::<Value>(include_str!(
            "../fixtures/astrocyte-multiplier-report-v2-2026-10-29.json"
        ))
        .unwrap()["result"]
            .clone();
        let now = report["latest"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["interval_end_ms"].as_i64().unwrap())
            .max()
            .unwrap()
            + 1;
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "credential-plans".into(),
                credentials: [
                    ("apikey:fixture-opencode-go", "opencode-go"),
                    ("apikey:fixture-kimi-for-coding", "kimi-for-coding"),
                ]
                .into_iter()
                .map(|(id, provider)| ListedCredential {
                    id: id.into(),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    credential_type: "apikey".into(),
                    auth_method: Some("apikey".into()),
                    provider_ids: vec![provider.into()],
                    ..Default::default()
                })
                .collect(),
                ..Default::default()
            },
            now as u64,
        );
        (
            report,
            credentials.api_key_credentials_by_provider(now as u64),
            now,
        )
    }

    fn credential_route_price(
        pools: HashMap<String, SubscriptionPool>,
        now: i64,
        provider: &str,
        operator: Option<f64>,
    ) -> striatum_core::subscription::SubscriptionPricing {
        use striatum_core::model_router::{select_model_route, SelectModelRouteInput};
        let mut snapshot = ModelRouteStateSnapshot::default();
        let mut pricing = RoutePricing::default();
        if let Some(value) = operator {
            pricing
                .subscription_multipliers
                .insert(provider.into(), value);
        }
        let mut fact: ModelFact = serde_json::from_value(json!({
            "providerID":provider, "modelID":"fixture", "confidence":"curated", "family":"fixture",
            "scores":{"elo":1960.0,"eq":2200.0,"speed":0.0}, "costPerMTokOut":20.0
        }))
        .unwrap();
        fact.subscription_multiplier = operator;
        overlay_pools(pools, &mut snapshot, &[fact.clone()], now, &pricing);
        select_model_route(SelectModelRouteInput {
            target_agent: "alfonso".into(),
            target_is_head: true,
            requirements: striatum_core::ModelRequirements { iq: 70.0, eq: 0.0 },
            facts: vec![fact],
            runtime_snapshot: Some(snapshot),
            available_models: [format!("{provider}/fixture")].into(),
            is_subscription_route: Some(Box::new(|_| true)),
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),
            required_capabilities: None,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: Default::default(),
            providers_without_cache_write_reports: Default::default(),
        })
        .unwrap()
        .score
        .objective
        .subscription_pricing
        .unwrap()
    }

    #[test]
    fn subscription_credential_eligible_rows_price_mapped_routes() {
        let (mut report, credentials, now) = credential_fixture();
        assert_eq!(
            credentials["opencode-go"],
            BTreeSet::from(["apikey:fixture-opencode-go".to_string()])
        );
        for row in report["latest"].as_array_mut().unwrap() {
            if ["opencodego", "kimi-for-coding"].contains(&row["quota_provider"].as_str().unwrap())
            {
                row["routing_eligible"] = json!(true);
                row["multiplier"] = json!(if row["quota_provider"] == "opencodego" {
                    8.0
                } else {
                    13.0
                });
            }
        }
        let pools = build_with_credentials(
            &AccountQuotaState::default(),
            &report,
            &Value::Null,
            now,
            &credentials,
        )
        .pools;
        for (provider, plan, multiplier) in [
            ("opencode-go", "opencode-go/go", 8.0),
            ("kimi-for-coding", "kimi-code-plan-global/allegro", 13.0),
        ] {
            assert!(pools[provider].plans[0].accounts.is_empty());
            for operator in [Some(6.0), None] {
                let price = credential_route_price(pools.clone(), now, provider, operator);
                assert_eq!(price.value_multiplier, multiplier);
                assert_eq!(price.plan_values[0].plan.as_deref(), Some(plan));
                assert_eq!(price.plan_values[0].source, "measured");
                assert_eq!(price.plan_values[0].weight, 1.0);
            }
        }
        report["latest"].as_array_mut().unwrap().reverse();
        assert_eq!(
            pools,
            build_with_credentials(
                &AccountQuotaState::default(),
                &report,
                &Value::Null,
                now,
                &credentials
            )
            .pools
        );
    }

    #[test]
    fn subscription_credential_ineligible_rows_use_operator_then_default() {
        let (report, credentials, now) = credential_fixture();
        for eligibility in [Some(false), None] {
            let mut report = report.clone();
            for row in report["latest"].as_array_mut().unwrap() {
                if row["plan_prices"][0]["credential_ref"].is_string() {
                    row["multiplier"] = json!(19.0);
                    if let Some(eligible) = eligibility {
                        row["routing_eligible"] = json!(eligible);
                    } else {
                        row.as_object_mut().unwrap().remove("routing_eligible");
                    }
                }
            }
            let pools = build_with_credentials(
                &AccountQuotaState::default(),
                &report,
                &Value::Null,
                now,
                &credentials,
            )
            .pools;
            for provider in ["opencode-go", "kimi-for-coding"] {
                assert!(pools[provider].plans[0].measured.is_none());
                for operator in [Some(6.0), None] {
                    let price = credential_route_price(pools.clone(), now, provider, operator);
                    assert_eq!(price.value_multiplier, operator.unwrap_or(1.0));
                    assert_eq!(
                        price.plan_values[0].source,
                        if operator.is_some() {
                            "operator"
                        } else {
                            "default"
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn subscription_credential_join_rejects_unmatched_and_ambiguous_ids() {
        let (mut report, credentials, now) = credential_fixture();
        let row = report["latest"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|row| row["quota_provider"] == "opencodego")
            .unwrap();
        row["routing_eligible"] = json!(true);
        row["multiplier"] = json!(8.0);
        let row = row.clone();
        let mut unmatched = credentials.clone();
        unmatched.insert("opencode-go".into(), ["apikey:not-listed".into()].into());
        let unknown = build_with_credentials(
            &AccountQuotaState::default(),
            &report,
            &Value::Null,
            now,
            &unmatched,
        )
        .pools;
        let price = credential_route_price(unknown, now, "opencode-go", None);
        assert_eq!(price.value_multiplier, 1.0);
        assert!(price.plan_values[0].plan.is_none());
        assert_eq!(price.plan_values[0].source, "default");

        let mut shared = report.clone();
        shared["latest"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|row| row["quota_provider"] == "opencodego")
            .unwrap()["plan_prices"]
            .as_array_mut()
            .unwrap()
            .push(json!({"credential_ref":"opaque-second-key"}));
        let mut shared_ids = credentials.clone();
        shared_ids
            .get_mut("opencode-go")
            .unwrap()
            .insert("opaque-second-key".into());
        let shared_pools = build_with_credentials(
            &AccountQuotaState::default(),
            &shared,
            &Value::Null,
            now,
            &shared_ids,
        )
        .pools;
        assert_eq!(shared_pools["opencode-go"].plans.len(), 1);
        assert_eq!(
            credential_route_price(shared_pools, now, "opencode-go", None).value_multiplier,
            8.0
        );

        for different_plan in [false, true] {
            let mut report = report.clone();
            let mut duplicate = row.clone();
            if different_plan {
                duplicate["plan"] = json!("opencode-go/other");
                duplicate["plan_prices"][0]["plan"] = json!("opencode-go/other");
            }
            report["latest"].as_array_mut().unwrap().push(duplicate);
            for reverse in [false, true] {
                if reverse {
                    report["latest"].as_array_mut().unwrap().reverse();
                }
                let pools = build_with_credentials(
                    &AccountQuotaState::default(),
                    &report,
                    &Value::Null,
                    now,
                    &credentials,
                )
                .pools;
                assert!(!pools.contains_key("opencode-go"));
                let price = credential_route_price(pools, now, "opencode-go", Some(6.0));
                assert_eq!(price.value_multiplier, 6.0);
                assert!(price.plan_values[0].plan.is_none());
                assert_eq!(price.plan_values[0].source, "operator");
            }
        }
        unmatched
            .get_mut("opencode-go")
            .unwrap()
            .insert("apikey:fixture-opencode-go".into());
        assert!(!build_with_credentials(
            &AccountQuotaState::default(),
            &report,
            &Value::Null,
            now,
            &unmatched
        )
        .pools
        .contains_key("opencode-go"));
    }

    #[test]
    fn subscription_credential_capture_does_not_change_account_pools_or_v1() {
        let (capture, mut credentials, now) = credential_fixture();
        let mut report = report_v2();
        report["latest"].as_array_mut().unwrap().extend(
            capture["latest"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|row| row["plan_prices"][0]["credential_ref"].is_string())
                .cloned(),
        );
        credentials.insert(
            "openai".into(),
            ["apikey:fixture-opencode-go".into()].into(),
        );
        let state = v2_state(now);
        let accounts = build(&state, &report, &Value::Null, now).pools;
        assert!(accounts["openai"]
            .plans
            .iter()
            .any(|plan| plan.measured.is_some()));
        let combined =
            build_with_credentials(&state, &report, &Value::Null, now, &credentials).pools;
        for (provider, pool) in accounts {
            assert_eq!(combined[&provider], pool);
        }
        assert!(build_with_credentials(
            &AccountQuotaState::default(),
            &self::report(),
            &Value::Null,
            now,
            &credentials
        )
        .pools
        .is_empty());
    }

    #[test]
    fn subscription_credential_live_fixture_is_digest_pinned() {
        let bytes = include_bytes!("../fixtures/astrocyte-multiplier-report-v2-2026-10-29.json");
        assert_eq!(bytes.len(), 991_100);
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "3c9d952bdc3fa9387e39d38cb71367443a3cdf07ba8dc52dc8002b30332c24d4"
        );
    }

    /// The head agents a converted config lists (`head_agents`).
    fn head_agents_of_converted_config() -> Vec<String> {
        serde_json::from_value(crate::config_convert::defaults()["head_agents"].clone())
            .expect("converter head agents")
    }
    use crate::account_quota::{RosterIdentity, StoredReading, VaultRoster};
    use sha2::{Digest, Sha256};
    const NOW: i64 = 1791507600000;

    // Captured from the read-only spend.multiplier.report on 2026-10-09, with
    // both providers' latest observations and daily series. This pins the actual
    // api_nanos/delta_percent fields used to compute USD per pooled quota point.
    // Schema source: astrocyte/crates/astrocyte-core/src/multiplier.rs and
    // crates/astrocyte-module/src/main.rs at
    // 4a89d881101c9a666d8acf9cc3aeae7b4532b0c8. Synthetic observations below
    // vary numeric values; the captured fixture preserves the served structure.
    fn report() -> Value {
        serde_json::from_str::<Value>(include_str!("../fixtures/astrocyte-multiplier-report.json"))
            .unwrap()["result"]
            .clone()
    }
    fn report_v2() -> Value {
        serde_json::from_str::<Value>(include_str!(
            "../fixtures/astrocyte-multiplier-report-v2.json"
        ))
        .unwrap()["result"]
            .clone()
    }
    fn prices() -> Value {
        // Adapter tests use the saved upstream plan response, including prices
        // and the quota_value fields that routing must ignore.
        // Source: crates/fusiform-module/fixtures/served-payloads.json at commit 2e689aab.
        // Fixture SHA-256: 95a576e57ab51182853ce52aff0240c4026f6fcf6d4104c997d9ee68cf410a45.
        serde_json::from_str(include_str!("../fixtures/fusiform-plan-prices.json")).unwrap()
    }
    fn state(raw: f64, tier: Option<&str>) -> AccountQuotaState {
        let mut state = AccountQuotaState::default();
        state.readings.readings.insert(("claude".into(), "a".into()), StoredReading { provider: "claude".into(), account_id: "a".into(), received_at_ms: NOW, tombstoned_at_ms: None,
            entry: json!({"provider":"claude", "apiProvider":"anthropic", "account":"a", "fetchedAt":"2026-10-09T01:00:00Z", "usage":{"secondary":{"usedPercent":0,"rawUsedPercent":raw,"windowKind":"weekly","windowMinutes":10080,"resetsAt":"2026-10-16T00:00:00Z"}}, "savedResets":{"availableCount":3}}) });
        state.roster = Some(VaultRoster {
            view: "scoped".into(),
            fetched_at_ms: NOW,
            grants: Vec::new(),
            identities: vec![RosterIdentity {
                refresh_adapter: "claude".into(),
                account_id: "a".into(),
                credential_ids: vec!["oauth:anthropic:a".into()],
                active_credential_ids: vec!["oauth:anthropic:a".into()],
                first_seen_ms: NOW,
                plan_tier: tier.map(str::to_string),
            }],
        });
        state
    }

    #[test]
    fn error_only_restart_keeps_last_good_in_subscription_budget() {
        use crate::account_quota::apply_usage_reply;
        let good = |id: &str, fetched| {
            json!({
                "provider": "claude", "apiProvider": "anthropic", "account": id,
                "fetchedAt": crate::account_selection::rfc3339(fetched),
                "usage": {
                    "primary": {"usedPercent": 20.0, "windowMinutes": 300, "resetsAt": "2026-10-09T06:00:00Z"},
                    "secondary": {"usedPercent": 30.0, "windowMinutes": 10080, "resetsAt": "2026-10-09T07:00:00Z"}
                }
            })
        };
        let mut quota = AccountQuotaState::default();
        let original = good("a", NOW);
        let body =
            json!({"result": [original.clone(), good("b", NOW)], "completeProviders": ["claude"]});
        apply_usage_reply(&mut quota, &serde_json::to_vec(&body).unwrap(), NOW).unwrap();
        assert!(build(&quota, &report(), &prices(), NOW).pools["anthropic"].available(20.0, false));

        let later = NOW + 27 * 60_000;
        let failed = json!({"provider": "claude", "apiProvider": "anthropic", "account": "a",
            "fetchedAt": crate::account_selection::rfc3339(later),
            "error": "no reading after restart", "errorClass": "upstream_failed"});
        let body = json!({"result": [failed, good("b", later)], "completeProviders": ["claude"]});
        apply_usage_reply(&mut quota, &serde_json::to_vec(&body).unwrap(), later).unwrap();
        let built = build(&quota, &report(), &prices(), later);
        let pool = &built.pools["anthropic"];
        assert!(
            pool.available(20.0, false),
            "last-good capacity must survive in the subscription budget: {}",
            pool.reserve_diagnostic(20.0, false)
        );
        assert_eq!(pool.plans[0].accounts.len(), 2);
        assert_eq!(
            quota.readings.readings[&("claude".into(), "a".into())].entry["usage"],
            original["usage"]
        );
        assert_eq!(pool.plans[0].accounts[0].weekly_used, 30.0);
        assert_eq!(
            pool.price(0.1, None, 20.0, false).unwrap().available_points,
            Some(100.0)
        );

        let expired = build(&quota, &report(), &prices(), NOW + 30 * 60_000);
        let pool = &expired.pools["anthropic"];
        assert_eq!(
            pool.plans[0].accounts.len(),
            1,
            "source age expires without another poll"
        );
        assert_eq!(
            pool.price(0.1, None, 20.0, false).unwrap().available_points,
            Some(50.0)
        );
        assert!(pool.available(20.0, true));
    }

    #[test]
    fn expired_short_window_in_last_good_is_unknown_to_subscription_budget() {
        let mut quota = state(30.0, None);
        let entry = &mut quota.readings.readings.values_mut().next().unwrap().entry;
        entry["usage"]["primary"] =
            json!({"usedPercent": 0.0, "windowMinutes": 300, "resetsAt": "2026-10-09T01:01:00Z"});
        entry["stale"] = json!({"since": "2026-10-09T01:00:30Z", "class": "upstream_failed"});
        let later = NOW + 2 * 60_000;
        assert!(account(entry, later).is_none());
        let built = build(&quota, &report(), &prices(), later);
        assert!(built.pools["anthropic"].plans.is_empty());
        assert!(!built.pools["anthropic"].available(20.0, false));
    }

    #[test]
    fn v1_reply_parses_but_supplies_no_plan_measurement() {
        assert!(account_plan(&json!({"latest":[],"series":[]}), "a")
            .unwrap()
            .is_none());
        let captured = report();
        assert!(captured["latest"].is_array());
        assert!(account_plan(&captured, "a").unwrap().is_none());
        assert!(captured["latest"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| measured(row).is_none()));
        let pool = build(&state(30.0, Some("max_20x")), &captured, &prices(), NOW)
            .pools
            .remove("anthropic")
            .unwrap();
        assert_eq!(
            pool.price(1.0, Some(6.0), 0.0, false)
                .unwrap()
                .value_multiplier,
            6.0
        );
        assert_eq!(
            pool.price(1.0, None, 0.0, false).unwrap().value_multiplier,
            1.0
        );
    }

    #[test]
    fn plan_observation_uses_quota_multiplier_and_interval_end() {
        let report = json!({"latest":[{"quota_provider":"claude","plan":"anthropic/max_20x","routing_eligible":true,"observation_day":"2026-10-09","snapshot_at_ms":NOW,"interval_end_ms":NOW-1,"api_nanos":284000000000i64,"delta_percent":10.0,"subscription_nanos":4666666667i64,"multiplier":60.8,"fee_multiplier":123.0}],"series":[]});
        let row = measured(&report["latest"][0]).unwrap();
        assert_eq!(row.multiplier, 60.8);
        assert_eq!(row.interval_end_ms, NOW - 1);
        assert_eq!(row.row_date, "2026-10-09");
    }

    #[test]
    fn missing_or_invalid_quota_multiplier_never_uses_fees_or_point_value() {
        for multiplier in [Value::Null, json!(0.0), json!(-1.0), json!("60.8")] {
            let report = json!({"latest":[{
                "quota_provider":"claude", "observation_day":"2026-10-09",
                "plan":"anthropic/max_20x", "routing_eligible":true,
                "interval_end_ms":NOW-1, "api_nanos":284000000000i64,
                "delta_percent":10.0, "fee_multiplier":123.0, "multiplier":multiplier
            }]});
            assert!(measured(&report["latest"][0]).is_none());
        }
        let report = json!({"latest":[{
            "quota_provider":"claude", "observation_day":"2026-10-09",
            "plan":"anthropic/max_20x", "routing_eligible":true,
            "interval_end_ms":NOW-1, "multiplier":60.8
        }]});
        assert_eq!(measured(&report["latest"][0]).unwrap().multiplier, 60.8);
    }

    #[test]
    fn subscription_requires_explicit_eligibility_and_non_null_plan() {
        for eligibility in [None, Some(json!(false)), Some(json!("true"))] {
            let mut row = json!({"quota_provider":"codex", "plan":"openai/pro_200",
                "multiplier":21.45, "interval_end_ms":NOW, "observation_day":"2026-10-09"});
            if let Some(flag) = eligibility {
                row["routing_eligible"] = flag;
            }
            assert!(measured(&row).is_none());
        }
        let row = json!({"quota_provider":"codex", "plan":null, "routing_eligible":true,
            "multiplier":99.0, "interval_end_ms":NOW, "observation_day":"2026-10-09"});
        let report = json!({"latest":[row]});
        assert!(measured(&report["latest"][0]).is_none());
        assert!(account_plan(&report, "acct-10").unwrap().is_none());
        assert!(astro_fee(&report["latest"][0], "acct-10", NOW).is_none());
    }

    fn v2_state(now: i64) -> AccountQuotaState {
        let mut state = AccountQuotaState::default();
        let mut identities = Vec::new();
        for (quota_provider, provider, tier, account_id) in [
            ("antigravity", "antigravity", "ultra_5x", "acct-1"),
            ("claude", "anthropic", "max_20x", "acct-2"),
            ("claude", "anthropic", "max_20x", "acct-3"),
            ("claude", "anthropic", "max_20x", "acct-4"),
            ("claude", "anthropic", "max_20x", "acct-5"),
            ("claude", "anthropic", "max_20x", "acct-6"),
            ("claude", "anthropic", "max_20x", "acct-7"),
            ("claude", "anthropic", "max_20x", "acct-8"),
            ("codex", "openai", "pro_100", "acct-9"),
            ("codex", "openai", "pro_200", "acct-10"),
            ("codex", "openai", "pro_200", "acct-11"),
            ("muse", "meta", "power_usage", "acct-12"),
        ] {
            identities.push(RosterIdentity {
                refresh_adapter: quota_provider.into(),
                account_id: account_id.into(),
                credential_ids: vec![format!("oauth:{provider}:{account_id}")],
                active_credential_ids: vec![format!("oauth:{provider}:{account_id}")],
                first_seen_ms: now,
                plan_tier: Some(tier.into()),
            });
            state.readings.readings.insert((quota_provider.into(), account_id.into()), StoredReading {
                provider: quota_provider.into(), account_id: account_id.into(), received_at_ms: now,
                tombstoned_at_ms: None,
                entry: json!({"provider":quota_provider, "apiProvider":provider, "account":account_id,
                    "fetchedAt":crate::account_selection::rfc3339(now), "usage":{"secondary":{
                        "usedPercent":0.0, "windowKind":"weekly", "windowMinutes":10080,
                        "resetsAt":crate::account_selection::rfc3339(now + 7 * 24 * 60 * 60_000)}}}),
            });
        }
        state.roster = Some(VaultRoster {
            view: "scoped".into(),
            fetched_at_ms: now,
            grants: Vec::new(),
            identities,
        });
        state
    }

    #[test]
    fn subscription_v2_capture_prices_each_plan_and_ignores_row_order() {
        use striatum_core::model_router::{select_model_route, SelectModelRouteInput};
        let mut report = report_v2();
        let now = report["latest"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["interval_end_ms"].as_i64().unwrap())
            .max()
            .unwrap()
            + 1;
        let state = v2_state(now);
        let original = build(&state, &report, &Value::Null, now).pools;
        assert_eq!(
            original
                .values()
                .flat_map(|pool| &pool.plans)
                .map(|p| p.accounts.len())
                .sum::<usize>(),
            12
        );
        assert_eq!(
            original["antigravity"].plans[0].plan.as_deref(),
            Some("google/ultra_5x")
        );
        assert_eq!(
            original["anthropic"].plans[0].plan.as_deref(),
            Some("anthropic/max_20x")
        );
        assert_eq!(
            original["meta"].plans[0].plan.as_deref(),
            Some("meta/power_usage")
        );
        assert_eq!(report["latest"].as_array().unwrap().len(), 5);
        assert_eq!(original["openai"].plans[0].tier.as_deref(), Some("pro_100"));
        assert!(original["openai"].plans[0].measured.is_none());
        assert_eq!(
            original["openai"].plans[1]
                .measured
                .as_ref()
                .unwrap()
                .multiplier,
            21.452711840768313
        );
        report["latest"].as_array_mut().unwrap().reverse();
        let reversed = build(&state, &report, &Value::Null, now).pools;
        assert_eq!(original, reversed);

        for operator in [Some(6.0), None] {
            for provider in ["openai", "anthropic", "antigravity", "meta"] {
                let mut snapshot = ModelRouteStateSnapshot::default();
                let mut pricing = RoutePricing::default();
                if let Some(m) = operator {
                    pricing.subscription_multipliers.insert(provider.into(), m);
                }
                let mut fact: ModelFact = serde_json::from_value(json!({
                    "providerID":provider, "modelID":"fixture", "confidence":"curated", "family":"fixture",
                    "scores":{"elo":1960.0,"eq":2200.0,"speed":0.0}, "costPerMTokOut":20.0
                })).unwrap();
                fact.subscription_multiplier = operator;
                overlay_pools(
                    reversed.clone(),
                    &mut snapshot,
                    &[fact.clone()],
                    now,
                    &pricing,
                );
                let selected = select_model_route(SelectModelRouteInput {
                    target_agent: "mason".into(),
                    target_is_head: false,
                    requirements: striatum_core::ModelRequirements { iq: 70.0, eq: 0.0 },
                    facts: vec![fact],
                    runtime_snapshot: Some(snapshot),
                    available_models: [format!("{provider}/fixture")].into(),
                    is_subscription_route: Some(Box::new(|_| true)),
                    exclude_route_keys: Vec::new(),
                    operator_exclude_keys: Vec::new(),
                    required_capabilities: None,
                    min_best_effort_adequacy: None,
                    prefer_prompt_cache: false,
                    providers_without_prompt_cache: Default::default(),
                    providers_without_cache_write_reports: Default::default(),
                })
                .unwrap();
                let price = selected.score.objective.subscription_pricing.unwrap();
                let fallback = operator.unwrap_or(1.0);
                let fallback_source = if operator.is_some() {
                    "operator"
                } else {
                    "default"
                };
                if provider == "openai" {
                    assert!(
                        (price.value_multiplier
                            - 1.0 / ((1.0 / 3.0) / fallback + (2.0 / 3.0) / 21.452711840768313))
                            .abs()
                            < 1e-12
                    );
                    assert_eq!(price.plan_values[0].source, fallback_source);
                    assert_eq!(price.plan_values[0].value_multiplier, fallback);
                    assert_eq!(price.plan_values[0].weight, 1.0 / 3.0);
                    assert_eq!(price.plan_values[0].plan.as_deref(), Some("openai/pro_100"));
                    assert_eq!(price.plan_values[1].source, "measured");
                    assert_eq!(price.plan_values[1].value_multiplier, 21.452711840768313);
                    assert_eq!(price.plan_values[1].weight, 2.0 / 3.0);
                    assert_eq!(price.plan_values[1].plan.as_deref(), Some("openai/pro_200"));
                } else {
                    assert_eq!(price.value_multiplier, fallback);
                    assert_eq!(price.plan_values[0].source, fallback_source);
                    assert_eq!(price.plan_values[0].weight, 1.0);
                }
            }
        }
    }

    #[test]
    fn subscription_astro_fee_is_matched_by_plan_and_account() {
        let mut report = report_v2();
        let now = report["latest"][0]["interval_end_ms"].as_i64().unwrap();
        let pro100 = account_plan(&report, "acct-9").unwrap().unwrap();
        let pro200 = account_plan(&report, "acct-10").unwrap().unwrap();
        assert_eq!(astro_fee(pro100, "acct-9", now), Some(100.0));
        assert_eq!(astro_fee(pro200, "acct-10", now), Some(200.0));
        assert_eq!(astro_fee(pro100, "acct-10", now), None);
        report["latest"].as_array_mut().unwrap().reverse();
        let pro200 = account_plan(&report, "acct-10").unwrap().unwrap();
        assert_eq!(astro_fee(pro200, "acct-10", now), Some(200.0));
        let built = build(&v2_state(now), &report, &Value::Null, now);
        assert_eq!(built.pools["openai"].plans[0].fee_usd, 100.0);
        assert_eq!(built.pools["openai"].plans[1].fee_usd, 200.0);
        assert_eq!(built.pools["antigravity"].plans[0].fee_usd, 99.99);
        assert_eq!(built.pools["meta"].plans[0].fee_usd, 50.0);
        let row = report["latest"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|r| r["plan"] == "openai/pro_200")
            .unwrap();
        row["plan_prices"][0]["review_by_ms"] = json!(now - 1);
        assert_eq!(astro_fee(row, "acct-10", now), None);
    }

    #[test]
    fn subscription_account_plan_join_rejects_ambiguous_and_unlisted_accounts() {
        let mut report = report_v2();
        let now = report["latest"][0]["interval_end_ms"].as_i64().unwrap() + 1;
        let pro100 = report["latest"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|row| row["plan"] == "openai/pro_100")
            .unwrap();
        pro100["plan_prices"].as_array_mut().unwrap().push(json!({
            "account_ref":"acct-10", "plan":"openai/pro_100", "monthly_nanos":100_000_000_000i64
        }));
        assert!(account_plan(&report, "acct-10").is_err());
        report["latest"].as_array_mut().unwrap().reverse();
        assert!(account_plan(&report, "acct-10").is_err());
        let mut state = v2_state(now);
        let mut unlisted = state.readings.readings[&("codex".into(), "acct-11".into())].clone();
        unlisted.account_id = "not-listed".into();
        unlisted.entry["account"] = json!("not-listed");
        state
            .readings
            .readings
            .insert(("codex".into(), "not-listed".into()), unlisted);
        let roster = state.roster.as_mut().unwrap();
        let mut identity = roster
            .identities
            .iter()
            .find(|id| id.account_id == "acct-11")
            .unwrap()
            .clone();
        identity.account_id = "not-listed".into();
        roster.identities.push(identity);
        assert!(account_plan(&report, "not-listed").unwrap().is_none());
        let built = build(&state, &report, &Value::Null, now);
        let pool = &built.pools["openai"];
        let unknown = pool.plans.iter().find(|p| p.plan.is_none()).unwrap();
        assert_eq!(unknown.accounts.len(), 2);
        assert!(unknown.measured.is_none());
        assert_eq!(unknown.tier.as_deref(), Some("pro_200"));
        for operator in [None, Some(6.0)] {
            let price = pool.price(1.0, operator, 0.0, false).unwrap();
            let unknown = price.plan_values.iter().find(|p| p.plan.is_none()).unwrap();
            assert_eq!(unknown.value_multiplier, operator.unwrap_or(1.0));
            assert_eq!(unknown.weight, 0.5);
            assert_eq!(
                unknown.source,
                if operator.is_some() {
                    "operator"
                } else {
                    "default"
                }
            );
        }
        assert!(WARNED_AMBIGUOUS_ACCOUNTS
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .contains("acct-10"));
    }

    #[test]
    fn subscription_v2_redacted_fixture_is_digest_pinned() {
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(include_bytes!(
                    "../fixtures/astrocyte-multiplier-report-v2.json"
                ))
            ),
            "240a76e305a9db4979bbf1d6bec7dd9e0e988b3576722f96cea0a27003455501"
        );
    }

    #[test]
    fn raw_used_percent_survives_banked_reset_zeroing() {
        let state = state(89.0, Some("max_20x"));
        let cache = build(&state, &report(), &prices(), NOW);
        let pool = &cache.pools["anthropic"];
        assert_eq!(pool.plans[0].accounts[0].weekly_used, 89.0);
        assert_eq!(pool.plans[0].accounts[0].banked_resets, 3);
        assert!(!pool.available(20.0, false));
        let mut entry = state
            .readings
            .readings
            .values()
            .next()
            .unwrap()
            .entry
            .clone();
        entry["usage"]["secondary"]
            .as_object_mut()
            .unwrap()
            .remove("rawUsedPercent");
        entry["usage"]["secondary"]["usedPercent"] = json!(73.0);
        assert_eq!(account(&entry, NOW).unwrap().weekly_used, 73.0);
    }

    #[test]
    fn fusiform_plan_prices_fixture_is_digest_pinned() {
        let digest = format!(
            "{:x}",
            Sha256::digest(include_bytes!("../fixtures/fusiform-plan-prices.json"))
        );
        assert_eq!(
            digest,
            "95a576e57ab51182853ce52aff0240c4026f6fcf6d4104c997d9ee68cf410a45"
        );
    }

    #[test]
    fn fusiform_quota_values_never_supply_subscription_multipliers() {
        let p = prices();
        let row = price_row(&p, "anthropic", "max_20x").unwrap();
        assert_eq!(row["quota_value"]["state"], "measured");
        for tier in [None, Some("max_20x")] {
            let built = build(&state(3.0, tier), &Value::Null, &p, NOW);
            let pool = &built.pools["anthropic"];
            let price = pool.price(1.0, None, 0.0, false).unwrap();
            assert_eq!(price.value_multiplier, 1.0);
            assert_eq!(price.value_source, "unmeasured");
            let price = pool.price(1.0, Some(6.0), 0.0, false).unwrap();
            assert_eq!(price.value_multiplier, 6.0);
            assert_eq!(price.value_source, "operator override");
            assert_eq!(pool.plans[0].fee_usd, 200.0);
        }
    }

    #[test]
    fn unpriced_tier_health_is_deduplicated_and_names_unknown_and_refused_rows() {
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        let unknown = ListedCredential {
            provider_ids: vec!["anthropic".into()],
            plan_tier: Some("pro_5x".into()),
            ..Default::default()
        };
        let refused = ListedCredential {
            provider_ids: vec!["openai".into()],
            plan_tier: Some("pro".into()),
            ..Default::default()
        };
        let listing = ScopedCredentialListing {
            credentials: vec![
                unknown.clone(),
                unknown,
                refused.clone(),
                refused,
                ListedCredential {
                    provider_ids: vec!["anthropic".into()],
                    plan_tier: Some("max_20x".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            unpriced_tiers(&listing, &prices()),
            BTreeSet::from([
                "unpriced plan tier: anthropic/pro_5x".into(),
                "unpriced plan tier: openai/pro".into()
            ])
        );
        let mut without_quota_values = prices();
        for row in without_quota_values["prices"].as_array_mut().unwrap() {
            row.as_object_mut().unwrap().remove("quota_value");
        }
        assert_eq!(
            unpriced_tiers(&listing, &without_quota_values),
            BTreeSet::from([
                "unpriced plan tier: anthropic/pro_5x".into(),
                "unpriced plan tier: openai/pro".into()
            ])
        );
    }

    #[test]
    fn free_other_provider_disables_banked_reserves() {
        let mut pools = build(&state(3.0, None), &report(), &prices(), NOW).pools;
        let mut codex = pools["anthropic"].clone();
        codex.provider = "openai".into();
        codex.plans[0].accounts[0].weekly_used = 95.0;
        pools.insert("openai".into(), codex);
        set_banked_allowance(&mut pools, NOW);
        assert!(!pools["openai"].allow_banked);
        let no_bank = pools["openai"].price(0.1, None, 0.0, false).unwrap();
        assert_eq!(no_bank.banked_resets_applied, 0);
        pools.get_mut("anthropic").unwrap().plans[0].accounts[0].weekly_used = 100.0;
        set_banked_allowance(&mut pools, NOW);
        assert!(pools["openai"].allow_banked);
        assert_eq!(
            pools["openai"]
                .price(0.1, None, 0.0, false)
                .unwrap()
                .banked_resets_applied,
            1
        );
    }

    #[test]
    fn every_pool_is_reserved_on_the_selection_pricing_including_providers_without_facts() {
        // The cached anthropic pool still carries the 0% reserve in force when
        // the cache was last refreshed; this selection's configuration keeps
        // all of anthropic's quota free. Only openai has catalog facts.
        let mut pools = build(&state(3.0, None), &report(), &prices(), NOW).pools;
        pools.get_mut("anthropic").unwrap().keep_free_percent = 0.0;
        let mut codex = pools["anthropic"].clone();
        codex.provider = "openai".into();
        codex.plans[0].accounts[0].weekly_used = 95.0;
        pools.insert("openai".into(), codex);
        let pricing = RoutePricing {
            keep_free_percent: HashMap::from([("anthropic".into(), 100.0)]),
            ..Default::default()
        };
        let facts: Vec<ModelFact> = vec![serde_json::from_value(json!({
            "providerID":"openai", "modelID":"gpt-6-luna", "confidence":"curated",
            "scores":{"elo":1641.0,"eq":1555.0,"speed":0.0}, "costPerMTokOut":0.5
        }))
        .unwrap()];
        let mut snapshot = ModelRouteStateSnapshot::default();

        overlay_pools(pools, &mut snapshot, &facts, NOW, &pricing);

        // Under the selection's reserve anthropic has no free headroom, so the
        // nearly spent openai plan may fall back on its banked resets. Judged on
        // the stale 0% reserve, anthropic would look free and forbid them.
        let openai = snapshot.by_capacity["openai/gpt-6-luna"]
            .subscription_pool
            .as_ref()
            .unwrap();
        assert_eq!(openai.keep_free_percent, 0.0);
        assert!(
            openai.allow_banked,
            "{}",
            openai.reserve_diagnostic(openai.keep_free_percent, false)
        );
    }

    #[test]
    fn banked_expiration_reads_insula_soonest_expires_at_and_credits() {
        let s = state(95.0, None);
        let mut entry = s.readings.readings.values().next().unwrap().entry.clone();
        entry["savedResets"] = json!({"availableCount":2,"soonestExpiresAt":"2026-10-22T21:05:54.769036000+00:00","credits":[{"expiresAt":"2026-10-29T19:37:32.795474000+00:00"},{"expiresAt":"2026-10-22T21:05:54.769036000+00:00"}]});
        let soonest = account(&entry, NOW).unwrap().banked_expires_at_ms.unwrap();
        entry["savedResets"]
            .as_object_mut()
            .unwrap()
            .remove("soonestExpiresAt");
        assert_eq!(
            account(&entry, NOW).unwrap().banked_expires_at_ms,
            Some(soonest)
        );
    }

    fn pi_incident_input(
        harness: Option<&str>,
        target_agent: &str,
        requirements: Value,
    ) -> (
        striatum_core::model_router::SelectModelRouteInput<'static>,
        Value,
    ) {
        let fixture: Value = serde_json::from_str(include_str!(
            "../fixtures/pi-mason-subscription-reserve.json"
        ))
        .unwrap();
        subscription_fixture_input(harness, target_agent, requirements, fixture)
    }

    fn subscription_fixture_input(
        harness: Option<&str>,
        target_agent: &str,
        requirements: Value,
        fixture: Value,
    ) -> (
        striatum_core::model_router::SelectModelRouteInput<'static>,
        Value,
    ) {
        use striatum_core::{
            billing::{BillingPlane, BillingRate},
            model_router::{RouteRuntimeState, SelectModelRouteInput},
        };
        let now = fixture["now_ms"].as_i64().unwrap();
        let mut state = AccountQuotaState::default();
        let mut identities = Vec::new();
        for (index, account) in fixture["accounts"].as_array().unwrap().iter().enumerate() {
            let account_id = format!("anonymous-{index}");
            let credential = format!("fixture:{index}");
            identities.push(RosterIdentity {
                refresh_adapter: "anthropic".into(),
                account_id: account_id.clone(),
                credential_ids: vec![credential.clone()],
                active_credential_ids: vec![credential],
                first_seen_ms: now - 86_400_000,
                plan_tier: Some("max_20x".into()),
            });
            state.readings.readings.insert(("claude".into(), account_id.clone()), StoredReading {
                provider: "claude".into(), account_id: account_id.clone(),
                entry: json!({"provider":"claude", "apiProvider":"anthropic", "account":account_id,
                    "savedResets":account["saved_resets"],
                    "fetchedAt":fixture["fetched_at"], "usage":{"secondary":{
                        "usedPercent":account["used_percent"], "windowKind":"weekly", "windowMinutes":10080,
                        "resetsAt":account["resets_at"]}}}),
                received_at_ms: now, tombstoned_at_ms: None,
            });
        }
        state.roster = Some(VaultRoster {
            view: "incident".into(),
            fetched_at_ms: now,
            grants: Vec::new(),
            identities,
        });
        let mut pool = build(&state, &Value::Null, &prices(), now)
            .pools
            .remove("anthropic")
            .unwrap();
        pool.keep_free_percent = fixture["keep_free_percent"].as_f64().unwrap();

        // The fixture stores account readings and demand vectors. This synthetic
        // workload exercises basket pricing on the roster-selected OAuth plane
        // without depending on live usage statistics.
        let mut fact: ModelFact = serde_json::from_value(json!({
            "providerID":"anthropic", "modelID":"claude-opus-5-5", "confidence":"curated", "family":"claude",
            "scores":{"elo":1960.0,"eq":2200.0,"speed":0.0}, "costPerMTokOut":20.0
        }))
        .unwrap();
        fact.class_vector = Some(striatum_core::ClassVector {
            workload_class: "ephemeral".into(),
            sample_count: 50,
            trailing_window_days: 7.0,
            statistic: "mean".into(),
            is_overlay: true,
            fresh_input: 0.0,
            output_excluding_reasoning: 1000.0,
            reasoning: 0.0,
            cached_read: 1000.0,
            cache_write: 0.0,
        });
        fact.class_vector_is_overlay = true;
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        credentials.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "incident".into(),
                credentials: state
                    .roster
                    .as_ref()
                    .unwrap()
                    .identities
                    .iter()
                    .flat_map(|identity| {
                        identity.credential_ids.iter().map(|id| {
                            cortexkit_claustrum_client::ListedCredential {
                                id: id.clone(),
                                account_id: Some(identity.account_id.clone()),
                                refresh_adapter: Some("anthropic".into()),
                                provider_ids: vec!["anthropic".into()],
                                credential_type: "oauth".into(),
                                auth_method: Some("oauth".into()),
                                categories: vec!["llm-provider".into()],
                                state: "active".into(),
                                ..Default::default()
                            }
                        })
                    })
                    .collect(),
                ..Default::default()
            },
            now as u64,
        );
        credentials.refresh_serving_methods(&state, &[fact.clone()], now, &[]);
        assert_eq!(
            credentials.plane_keys(),
            BTreeSet::from([("anthropic".into(), "oauth".into())])
        );
        fact.billing_plane = Some(BillingPlane {
            provider_id: "anthropic".into(),
            auth_method: "oauth".into(),
            kind: "quota_proxy".into(),
            selection_reason: "roster".into(),
            rates: BTreeMap::from([
                ("rate.output".into(), BillingRate::Priced { rate: 20.0 }),
                ("rate.cache_read".into(), BillingRate::Priced { rate: 0.25 }),
            ]),
        });
        let mut params = json!({"targetAgent":target_agent, "requirements":requirements,
            "substrate":"broca", "availableModels":["anthropic/claude-opus-5-5"]});
        // OpenCode discovers OAuth locally; Pi supplies no host auth facts for
        // the worker. Broca sends both workers using the same subscription
        // accounts; its billing plane describes those accounts' quota rates.
        params["providerAuthTypes"] = if harness == Some("pi") {
            json!({})
        } else {
            json!({"anthropic":"oauth"})
        };
        if let Some(harness) = harness {
            params["harness"] = json!(harness);
        }
        let params: crate::SelectParams = serde_json::from_value(params).unwrap();
        let resolved = crate::resolve_select_request(&params, &Default::default()).unwrap();
        let sub: std::collections::HashSet<String> =
            resolved.policy.subscription_keys.into_iter().collect();
        assert_eq!(sub.is_empty(), harness == Some("pi"));
        let snapshot = ModelRouteStateSnapshot {
            now: now as f64,
            by_capacity: HashMap::from([(
                "anthropic/claude-opus-5-5".into(),
                RouteRuntimeState {
                    subscription_pool: Some(Arc::new(pool)),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let target_is_head = head_agents_of_converted_config()
            .iter()
            .any(|head| head == &params.target_agent);
        (
            SelectModelRouteInput {
                target_agent: params.target_agent,
                target_is_head,
                requirements: resolved.requirements,
                available_models: resolved.policy.available_models.into_iter().collect(),
                facts: vec![fact],
                runtime_snapshot: Some(snapshot),
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: None,
                is_subscription_route: Some(Box::new(move |r| sub.contains(&r.route_key))),
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: Default::default(),
                providers_without_cache_write_reports: Default::default(),
            },
            fixture,
        )
    }

    #[test]
    fn pi_masons_share_corrected_incident_subscription_admission_with_opencode() {
        use striatum_core::model_router::select_model_route;
        let (_, fixture) = pi_incident_input(Some("pi"), "mason", json!({"iq":85,"eq":15}));
        // Only the fixed margin protects heads: 232 - 66 = 166 worker points.
        // Pi's missing host OAuth metadata must not change the pooled decision.
        for decision in fixture["decisions"].as_array().unwrap() {
            let mut selections = Vec::new();
            for harness in [Some("pi"), Some("opencode"), None] {
                let (input, _) =
                    pi_incident_input(harness, "mason", decision["requirements"].clone());
                let pool = input.runtime_snapshot.as_ref().unwrap().by_capacity
                    ["anthropic/claude-opus-5-5"]
                    .subscription_pool
                    .as_ref()
                    .unwrap();
                let detail: Value =
                    serde_json::from_str(&pool.reserve_diagnostic(20.0, false)).unwrap();
                assert_eq!(detail["pool"], "anthropic");
                assert_eq!(detail["unit"], "weekly_percentage_points_per_account");
                let plan = &detail["plans"][0];
                for (key, value) in fixture["expected_plan"].as_object().unwrap() {
                    assert_eq!(&plan[key], value, "{key}");
                }
                let selected = select_model_route(input).unwrap();
                assert_eq!(selected.route_key, "anthropic/claude-opus-5-5");
                assert!(
                    (selected
                        .score
                        .objective
                        .subscription_pricing
                        .as_ref()
                        .unwrap()
                        .available_points
                        .unwrap()
                        - 166.0)
                        .abs()
                        < 1e-8
                );
                selections.push((
                    selected.route_key,
                    selected.score.objective.subscription_pricing,
                ));
            }
            assert_eq!(selections[0], selections[1]);
            assert_eq!(selections[0], selections[2]);
        }
    }

    #[test]
    fn incident_subscription_heads_keep_access_and_subscription_pricing() {
        for harness in ["pi", "opencode", "claude-code"] {
            let (input, _) = pi_incident_input(Some(harness), harness, json!({"iq":85,"eq":15}));
            let selected =
                striatum_core::model_router::select_model_route(input).unwrap();
            assert_eq!(selected.route_key, "anthropic/claude-opus-5-5");
            let pricing = selected.score.objective.subscription_pricing.unwrap();
            assert_eq!(pricing.available_points, Some(232.0));
        }
    }

    #[test]
    fn incident_subscription_workers_get_fixed_margin_subscription_pricing() {
        for harness in ["pi", "opencode"] {
            let (input, _) = pi_incident_input(Some(harness), "mason", json!({"iq":85,"eq":15}));
            let selected =
                striatum_core::model_router::select_model_route(input).unwrap();
            let pricing = selected.score.objective.subscription_pricing.unwrap();
            assert_eq!(pricing.available_points, Some(166.0));
            assert_eq!(
                selected.score.pricing_mode,
                striatum_core::model_router::PricingMode::Basket
            );
        }
    }

    #[test]
    fn incident_api_plane_is_still_metered_not_subscription_reserved() {
        let (mut input, _) = pi_incident_input(Some("pi"), "mason", json!({"iq":85,"eq":15}));
        let plane = input.facts[0].billing_plane.as_mut().unwrap();
        plane.auth_method = "apikey".into();
        plane.kind = "upstream".into();
        let selected = striatum_core::model_router::select_model_route(input).unwrap();
        assert!(selected.score.objective.subscription_pricing.is_none());
        assert!(selected
            .explanation
            .iter()
            .any(|line| line.contains("metered")));
    }

    #[test]
    fn october_17_iq85_worker_can_use_all_live_quota_above_fixed_margin() {
        use striatum_core::model_router::select_model_route;
        let fixture: Value = serde_json::from_str(include_str!(
            "../fixtures/claude-head-reserve-2026-10-17.json"
        ))
        .unwrap();
        for (margin, expected) in [(0.0, 256.0), (10.0, 223.0), (20.0, 193.0)] {
            for harness in [Some("pi"), Some("opencode"), None] {
                let mut fixture = fixture.clone();
                fixture["keep_free_percent"] = json!(margin);
                let (input, _) =
                    subscription_fixture_input(harness, "mason", json!({"iq":85,"eq":0}), fixture);
                let pool = input.runtime_snapshot.as_ref().unwrap().by_capacity
                    ["anthropic/claude-opus-5-5"]
                    .subscription_pool
                    .as_ref()
                    .unwrap();
                assert!(!pool.allow_banked);
                assert_eq!(
                    pool.plans[0]
                        .accounts
                        .iter()
                        .map(|a| a.banked_resets)
                        .sum::<u32>(),
                    4
                );
                let selected = select_model_route(input).unwrap();
                assert_eq!(selected.route_key, "anthropic/claude-opus-5-5");
                let pricing = selected.score.objective.subscription_pricing.unwrap();
                assert_eq!(pricing.available_points, Some(expected));
                assert_eq!(pricing.keep_free_percent, margin);
                assert_eq!(pricing.banked_resets_applied, 0);
                let wire = serde_json::to_value(pricing).unwrap();
                for removed in ["headReservePoints", "headReserveSteps", "tightestPoint"] {
                    assert!(wire.get(removed).is_none());
                }
            }
        }
    }

    #[test]
    fn iq85_worker_is_refused_only_when_every_live_account_is_at_margin() {
        use striatum_core::model_router::select_model_route;
        let mut fixture: Value = serde_json::from_str(include_str!(
            "../fixtures/claude-head-reserve-2026-10-17.json"
        ))
        .unwrap();
        for (margin, used) in [(0.0, 100.0), (20.0, 80.0)] {
            fixture["keep_free_percent"] = json!(margin);
            for account in fixture["accounts"].as_array_mut().unwrap() {
                account["used_percent"] = json!(used);
            }
            let (input, _) = subscription_fixture_input(
                Some("pi"),
                "mason",
                json!({"iq":85,"eq":0}),
                fixture.clone(),
            );
            let refusal = select_model_route(input).unwrap_err();
            assert_eq!(refusal.rejected.len(), 1);
            assert_eq!(refusal.rejected[0].route_key, "anthropic/claude-opus-5-5");
            assert_eq!(refusal.rejected[0].reason, "subscription_reserve");
            let detail: Value =
                serde_json::from_str(refusal.rejected[0].detail.as_deref().unwrap()).unwrap();
            assert!(detail["rule"]
                .as_str()
                .unwrap()
                .contains("keep-free margin"));
            assert_eq!(detail["plans"][0]["availablePoints"], 0.0);
            assert!(detail.get("headPace").is_none());
            for removed in [
                "headPointsPerHour",
                "headReservePoints",
                "headReserveSteps",
                "tightestPoint",
                "resetHorizonHours",
            ] {
                assert!(detail["plans"][0].get(removed).is_none());
            }
        }
    }

    fn outage_fixture() -> (AccountQuotaState, Value) {
        // Numeric readings precede the October 10 worker-selection request with
        // an IQ requirement of 90, identified in the fixture's source_decision.
        // Account aliases omit personal identities; the producer fetchedAt is
        // preserved so receipt of a stale reading does not make it fresh.
        let fixture: Value = serde_json::from_str(include_str!(
            "../fixtures/routing-reserve-outage-2026-10-10.json"
        ))
        .unwrap();
        let now = fixture["now_ms"].as_i64().unwrap();
        let mut state = AccountQuotaState::default();
        let mut identities = Vec::new();
        for row in fixture["accounts"].as_array().unwrap() {
            let entry = row["entry"].clone();
            let provider = entry["provider"].as_str().unwrap().to_string();
            let account_id = entry["account"].as_str().unwrap().to_string();
            let credential = format!("fixture:{account_id}");
            identities.push(RosterIdentity {
                refresh_adapter: provider.clone(),
                account_id: account_id.clone(),
                credential_ids: vec![credential.clone()],
                active_credential_ids: vec![credential],
                first_seen_ms: now - 86_400_000,
                plan_tier: Some(row["tier"].as_str().unwrap().into()),
            });
            state.readings.readings.insert(
                (provider.clone(), account_id.clone()),
                StoredReading {
                    provider,
                    account_id,
                    entry,
                    received_at_ms: fixture["observed_at_ms"].as_i64().unwrap(),
                    tombstoned_at_ms: None,
                },
            );
        }
        state.roster = Some(VaultRoster {
            view: "outage-fixture".into(),
            fetched_at_ms: now,
            grants: Vec::new(),
            identities,
        });
        (state, fixture)
    }

    fn outage_selection() -> Result<
        striatum_core::model_router::SelectedModelRoute,
        striatum_core::model_router::NoRouteError,
    > {
        use striatum_core::model_router::{
            select_model_route, RouteRuntimeState, SelectModelRouteInput,
        };
        let (state, fixture) = outage_fixture();
        let now = fixture["now_ms"].as_i64().unwrap();
        let mut pools = build(&state, &Value::Null, &prices(), now).pools;
        pools.get_mut("anthropic").unwrap().keep_free_percent = 20.0;
        pools.get_mut("openai").unwrap().keep_free_percent = 0.0;
        set_banked_allowance(&mut pools, now);
        let mut snapshot = ModelRouteStateSnapshot {
            now: now as f64,
            ..Default::default()
        };
        // These scores and rates are from the copied routing catalog. The three
        // subscription routes share provider pools. The xAI route has a live
        // quota wall, so this test must find capacity on a subscription route.
        let facts: Vec<ModelFact> = [
            json!({"providerID":"anthropic", "modelID":"claude-opus-5-5", "scores":{"elo":1960.0,"eq":2200.0,"speed":0.0}, "confidence":"curated", "costPerMTokOut":20.0}),
            json!({"providerID":"openai", "modelID":"gpt-6.1-sol", "scores":{"elo":1884.0,"eq":1620.0,"speed":0.0}, "confidence":"curated", "costPerMTokOut":10.0}),
            json!({"providerID":"openai", "modelID":"gpt-6-luna", "scores":{"elo":1641.0,"eq":1555.0,"speed":0.0}, "confidence":"curated", "costPerMTokOut":0.5}),
            json!({"providerID":"xai", "modelID":"grok-4.7", "scores":{"elo":1808.0,"eq":1600.0,"speed":0.0}, "confidence":"curated", "costPerMTokOut":6.0}),
        ].into_iter().map(|value| serde_json::from_value(value).unwrap()).collect();
        for fact in &facts {
            let runtime = if fact.provider_id == "xai" {
                RouteRuntimeState {
                    quota_exhausted_until: fixture["xai_wall_until_ms"].as_f64(),
                    ..Default::default()
                }
            } else {
                RouteRuntimeState {
                    subscription_pool: Some(Arc::new(pools[&fact.provider_id].clone())),
                    ..Default::default()
                }
            };
            snapshot.by_capacity.insert(
                make_capacity_key(&fact.provider_id, &fact.model_id),
                runtime,
            );
        }
        select_model_route(SelectModelRouteInput {
            target_agent: "mason".into(),
            target_is_head: false,
            requirements: striatum_core::model_facts::ModelRequirements {
                iq: 70.0,
                eq: 0.0,
            },
            available_models: facts
                .iter()
                .map(|f| make_capacity_key(&f.provider_id, &f.model_id))
                .collect(),
            facts,
            runtime_snapshot: Some(snapshot),
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),
            required_capabilities: None,
            is_subscription_route: Some(Box::new(|r| r.provider_id != "xai")),
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: Default::default(),
            providers_without_cache_write_reports: Default::default(),
        })
    }

    #[test]
    fn realistic_outage_snapshot_leaves_iq70_worker_route_without_spending_saved_resets() {
        let selected = outage_selection().expect("pooled quota can serve a worker");
        assert_eq!(selected.route_key, "openai/gpt-6.1-sol");
        assert!(selected
            .rejected
            .iter()
            .any(|r| r.route_key == "xai/grok-4.7" && r.reason == "quota_exhausted"));
        let pricing = selected.score.objective.subscription_pricing.unwrap();
        // Neither provider has a measured or operator value, so both use M = 1.
        // Sol wins without the old Fusiform subsidy; saved resets remain unused.
        assert_eq!(pricing.value_multiplier, 1.0);
        assert_eq!(pricing.value_source, "unmeasured");
        assert_eq!(pricing.plan_tier.as_deref(), Some("pro_100"));
        assert_eq!(pricing.banked_resets_applied, 0);
        assert_eq!(pricing.available_points, Some(90.0));
        assert!(!selected
            .rejected
            .iter()
            .any(|r| r.route_key == "anthropic/claude-opus-5-5"));

        // Claude remains admitted with 285 live points minus the fixed 100-point
        // margin, even though it no longer wins the price comparison.
        let (state, fixture) = outage_fixture();
        let pools = build(
            &state,
            &Value::Null,
            &prices(),
            fixture["now_ms"].as_i64().unwrap(),
        )
        .pools;
        let claude = pools["anthropic"].price(20.0, None, 20.0, false).unwrap();
        assert_eq!(claude.available_points, Some(185.0));
        assert_eq!(claude.banked_resets_applied, 0);
    }
}
