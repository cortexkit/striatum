//! Optional read-only economics adapters. Replies are refreshed with account quota,
//! never from route.select. Missing services and unshipped fields are normal.

use crate::{
    account_quota::{parse_rfc3339_ms, AccountQuotaState, SharedAccountQuota},
    quota_consumer::{QuotaConsumer, QuotaConsumerError},
};
use prefrontal_routing_core::{
    model_facts::{get_keep_free_percent, make_capacity_key},
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
static CACHE: OnceLock<Mutex<EconomicsCache>> = OnceLock::new();
fn cache() -> &'static Mutex<EconomicsCache> {
    CACHE.get_or_init(|| Mutex::new(EconomicsCache::default()))
}

/// Read each provider's latest observation from the report's `latest` array:
/// API list-price nanos divided by pooled percentage-point use yields USD/point.
/// Schema source: astrocyte/crates/astrocyte-core/src/multiplier.rs
/// (observation_rows/multiplier_report), commit
/// 4a89d881101c9a666d8acf9cc3aeae7b4532b0c8. Age uses interval_end_ms,
/// not snapshot_at_ms: recomputing an old day cannot freshen its evidence.
fn measured(report: &Value, quota_provider: &str) -> Option<MeasuredValue> {
    let row = report
        .get("latest")?
        .as_array()?
        .iter()
        .find(|r| r["quota_provider"] == quota_provider)?;
    let delta = row["delta_percent"].as_f64()?;
    let api = row["api_nanos"].as_f64()?;
    if delta <= 0.0 || api <= 0.0 || !delta.is_finite() || !api.is_finite() {
        return None;
    }
    Some(MeasuredValue {
        usd_per_point: api / 1e9 / delta,
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

fn astro_fee(report: &Value, provider: &str, account_id: &str, now: i64) -> Option<f64> {
    let row = report["latest"]
        .as_array()?
        .iter()
        .find(|row| row["quota_provider"] == provider)?;
    let plan = row["plan_prices"]
        .as_array()?
        .iter()
        .find(|plan| plan["account_ref"] == account_id)?;
    if plan["review_by_ms"]
        .as_i64()
        .is_some_and(|review| review < now)
    {
        return None;
    }
    let fee = plan["monthly_nanos"].as_f64()? / 1e9;
    (fee.is_finite() && fee > 0.0).then_some(fee)
}

/// Read the conservative measured quota value from the subscription-plan price
/// rows served by Fusiform, the service that supplies these prices. Missing or
/// refused measurements, and observations outside their date limits, stay unknown.
fn fusiform_value(row: &Value, now: i64) -> Option<f64> {
    if row.get("retired").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let value = row.get("quota_value")?.as_object()?;
    if value.get("review_by_ms")?.as_i64()? < now {
        return None;
    }
    match value.get("state")?.as_str()? {
        "measured" => {
            if value.get("as_of_ms")?.as_i64()? > now {
                return None;
            }
            for field in ["basis", "token_mix", "established_by", "source_ref"] {
                if value.get(field)?.as_str()?.trim().is_empty() {
                    return None;
                }
            }
            let low = decimal(value.get("low")?, "units")?;
            let high = decimal(value.get("high")?, "units")?;
            (low <= high).then_some(low)
        }
        _ => None,
    }
}

const PACE_HOUR_MS: i64 = 3_600_000;
const PACE_WINDOW_HOURS: i64 = 24;

fn pace_window(now_ms: i64) -> (i64, i64) {
    let to = now_ms - now_ms.rem_euclid(PACE_HOUR_MS);
    (to - PACE_WINDOW_HOURS * PACE_HOUR_MS, to)
}

fn pace_request(provider: &str, from: i64, to: i64) -> Value {
    json!({
        "method": "spend.multiplier.pace",
        "params": {"quota_provider": provider, "from_ms": from, "to_ms": to}
    })
}

fn producer_points(producer: &Value) -> Option<f64> {
    producer["events"].as_u64()?;
    producer["list_nanos"].as_i64()?;
    let points = producer["estimated_points"].as_f64()?;
    if points < 0.0 || !points.is_finite() {
        return None;
    }
    if producer["unpriced_tokens"].as_i64()? < 0 {
        return None;
    }
    for work_class in producer["by_work_class"].as_object()?.values() {
        work_class["events"].as_u64()?;
        work_class["list_nanos"].as_i64()?;
        let class_points = work_class["estimated_points"].as_f64()?;
        if class_points < 0.0 || !class_points.is_finite() {
            return None;
        }
    }
    Some(points)
}

/// Read pooled points from Astrocyte's complete hourly series. A gap or
/// malformed producer is incomplete evidence, not a zero pace.
fn head_pace(reply: &Value, provider: &str, from: i64, to: i64) -> Option<f64> {
    if reply["quota_provider"] != provider
        || reply["from_ms"].as_i64()? != from
        || reply["to_ms"].as_i64()? != to
    {
        return None;
    }
    if reply["basis"] != "pro_rata_list_value"
        || reply["per_point_nanos"].as_i64()? <= 0
        || reply["observation_day"].as_str()?.is_empty()
        || reply["observation_id"].as_i64()? <= 0
        || reply["undated_events"].as_i64()? < 0
    {
        return None;
    }
    let span = to.checked_sub(from)?;
    if from.rem_euclid(PACE_HOUR_MS) != 0
        || to.rem_euclid(PACE_HOUR_MS) != 0
        || span <= 0
        || span % PACE_HOUR_MS != 0
    {
        return None;
    }
    let window_hours = (span / PACE_HOUR_MS) as usize;
    if window_hours > 192 {
        return None;
    }
    let hours = reply["hours"].as_array()?;
    if hours.len() != window_hours {
        return None;
    }
    let mut points = 0.0;
    for (index, hour) in hours.iter().enumerate() {
        let expected_hour = from + index as i64 * PACE_HOUR_MS;
        if hour["hour_start_ms"].as_i64()? != expected_hour {
            return None;
        }
        for (name, producer) in hour["producers"].as_object()? {
            let estimated_points = producer_points(producer)?;
            if matches!(name.as_str(), "opencode" | "pi" | "claude-code") {
                points += estimated_points;
                if !points.is_finite() {
                    return None;
                }
            }
        }
    }
    let pace = points / window_hours as f64;
    pace.is_finite().then_some(pace)
}

fn head_pace_result(
    result: Result<Value, QuotaConsumerError>,
    provider: &str,
    from: i64,
    to: i64,
) -> Option<f64> {
    result
        .ok()
        .and_then(|reply| head_pace(&reply, provider, from, to))
}

fn pace_call_is_transient(error: &QuotaConsumerError) -> bool {
    // The Astrocyte management service returns `malformed`, `unknown_quota_provider`,
    // `invalid_range`, `range_too_large`, and `no_multiplier_observation` when the
    // request is bad or data is absent; repeating that query cannot help. Invalid
    // success JSON is also final; transport and other operation errors retry once.
    match error {
        QuotaConsumerError::Message(message) => {
            let Some(body) = message.strip_prefix("usage.get failed: ") else {
                return true;
            };
            let code = serde_json::from_str::<Value>(body)
                .ok()
                .and_then(|body| body["code"].as_str().map(str::to_owned));
            !matches!(
                code.as_deref(),
                Some(
                    "malformed"
                        | "unknown_quota_provider"
                        | "invalid_range"
                        | "range_too_large"
                        | "no_multiplier_observation"
                )
            )
        }
        QuotaConsumerError::Json(_) => false,
    }
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
            short_windows.push(prefrontal_routing_core::subscription::QuotaConstraint {
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
    })
}

fn build(
    state: &AccountQuotaState,
    report: &Value,
    prices: &Value,
    paces: &HashMap<String, f64>,
    now: i64,
) -> EconomicsCache {
    let mut out = EconomicsCache {
        refreshed_at_ms: now,
        ..Default::default()
    };
    let mut plans: BTreeMap<(String, Option<String>, u64), SubscriptionPlan> = BTreeMap::new();
    for reading in state.readings.readings.values() {
        if reading.tombstoned_at_ms.is_some()
            || reading.entry.get("error").is_some_and(|e| !e.is_null())
        {
            continue;
        }
        let Some(fetched) = reading.entry["fetchedAt"]
            .as_str()
            .and_then(parse_rfc3339_ms)
        else {
            continue;
        };
        if now < fetched || now - fetched > 15 * 60_000 {
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
        let row = tier
            .as_deref()
            .and_then(|tier| price_row(prices, provider, tier));
        let fee = row
            .and_then(|r| {
                (r["price"]["currency"] == "USD")
                    .then(|| decimal(&r["price"], "minor_units"))
                    .flatten()
            })
            .or_else(|| astro_fee(report, &reading.provider, &reading.account_id, now))
            .unwrap_or(DEFAULT_PLAN_FEE_USD);
        let Some(account) = account(&reading.entry, now) else {
            continue;
        };
        let plan = plans
            .entry((provider.into(), tier.clone(), fee.to_bits()))
            .or_insert_with(|| SubscriptionPlan {
                tier,
                fee_usd: fee,
                fusiform_multiplier: row.and_then(|r| fusiform_value(r, now)),
                accounts: Vec::new(),
                head_points_per_hour: paces.get(&reading.provider).copied(),
            });
        plan.accounts.push(account);
        let pool = out
            .pools
            .entry(provider.into())
            .or_insert_with(|| SubscriptionPool {
                provider: provider.into(),
                measured: measured(report, &reading.provider),
                now_ms: now,
                ..Default::default()
            });
        pool.keep_free_percent = get_keep_free_percent(provider);
    }
    for (quota, provider) in crate::quota_name_map() {
        if let Some(value) = measured(report, quota) {
            out.pools
                .entry(provider.clone())
                .or_insert_with(|| SubscriptionPool {
                    provider: provider.clone(),
                    now_ms: now,
                    ..Default::default()
                })
                .measured = Some(value);
        }
    }
    for ((provider, _, _), plan) in plans {
        out.pools.get_mut(&provider).unwrap().plans.push(plan);
    }
    for pool in out.pools.values_mut() {
        let accounts: usize = pool.plans.iter().map(|plan| plan.accounts.len()).sum();
        if accounts == 0 {
            continue;
        }
        // Astrocyte reports one provider-wide demand, not one demand per tier.
        // Each account contributes 100 weekly points, so split that demand by
        // account capacity before each tier projects its own reset horizon.
        for plan in &mut pool.plans {
            plan.head_points_per_hour = plan
                .head_points_per_hour
                .map(|pace| pace * plan.accounts.len() as f64 / accounts as f64);
        }
    }
    out
}

fn fallback_plan() -> SubscriptionPlan {
    SubscriptionPlan {
        tier: None,
        fee_usd: DEFAULT_PLAN_FEE_USD,
        fusiform_multiplier: None,
        accounts: Vec::new(),
        head_points_per_hour: None,
    }
}

pub(crate) async fn refresh(
    connection: &Path,
    project: &Path,
    state: &SharedAccountQuota,
    listing: Option<&prefrontal_ckcred_client::ScopedCredentialListing>,
) {
    let now = crate::now_ms() as i64;
    let from = now - 24 * 3_600_000;
    let (pace_from, pace_to) = pace_window(now);
    let mut report = Value::Null;
    let mut paces = HashMap::new();
    if let Ok(mut astro) =
        QuotaConsumer::connect_module(connection, project, "astrocyte", "management_surface").await
    {
        report = astro
            .query(
                json!({"method":"spend.multiplier.report", "params":{"from_ms":from,"to_ms":now}}),
            )
            .await
            .unwrap_or(Value::Null);
        let providers: Vec<String> = state
            .lock()
            .ok()
            .map(|s| {
                s.readings
                    .readings
                    .values()
                    .map(|r| r.provider.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default();
        let operations = astro
            .management_operations("astrocyte")
            .await
            .unwrap_or_default();
        for provider in providers
            .into_iter()
            .filter(|provider| matches!(provider.as_str(), "claude" | "codex"))
            .filter(|_| operations.contains("spend.multiplier.pace"))
        {
            // Only registered operations are read. Missing or unreadable pace
            // replies leave the reserve unavailable without a routing error.
            let request = pace_request(&provider, pace_from, pace_to);
            let mut result = astro.query(request.clone()).await;
            if matches!(&result, Err(error) if pace_call_is_transient(error)) {
                result = astro.query(request).await;
            }
            if let Some(pace) = head_pace_result(result, &provider, pace_from, pace_to) {
                paces.insert(provider, pace);
            }
        }
    }
    let mut prices = Value::Null;
    if let Ok(mut fusi) =
        QuotaConsumer::connect_module(connection, project, "fusiform", "tool_provider").await
    {
        prices = fusi
            .query(json!({"name":"plan.prices", "arguments":{}}))
            .await
            .unwrap_or(Value::Null);
    }
    let Some(snapshot) = state.lock().ok().map(|s| s.clone()) else {
        return;
    };
    let mut next = build(&snapshot, &report, &prices, &paces, now);
    if let Some(listing) = listing {
        next.unpriced_tiers = unpriced_tiers(listing, &prices, now);
    }
    if let Ok(mut guard) = cache().lock() {
        *guard = next;
    }
}

fn unpriced_tiers(
    listing: &prefrontal_ckcred_client::ScopedCredentialListing,
    prices: &Value,
    now: i64,
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for credential in &listing.credentials {
        if let Some(tier) = &credential.plan_tier {
            for provider in &credential.provider_ids {
                if price_row(prices, provider, tier)
                    .and_then(|r| fusiform_value(r, now))
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

pub(crate) fn overlay(snapshot: &mut ModelRouteStateSnapshot, facts: &[ModelFact], now: i64) {
    let mut pools = cache().lock().map(|c| c.pools.clone()).unwrap_or_default();
    for fact in facts {
        let pool = pools
            .entry(fact.provider_id.clone())
            .or_insert_with(|| SubscriptionPool {
                provider: fact.provider_id.clone(),
                ..Default::default()
            });
        pool.keep_free_percent = get_keep_free_percent(&fact.provider_id);
        if pool.plans.is_empty() {
            pool.plans.push(fallback_plan());
        }
    }
    for pool in pools.values_mut() {
        let elapsed = (now - pool.now_ms).max(0) as f64 / 3_600_000.0;
        for plan in &mut pool.plans {
            if elapsed > 0.25 {
                plan.accounts.clear();
                plan.head_points_per_hour = None;
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
    fn prices() -> Value {
        // Adapter tests use this saved upstream response to confirm the decoder
        // reads Fusiform's served quota_value rows, not a test-only schema.
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
    fn report_fixture_absence_and_real_column_units() {
        assert!(measured(&json!({"latest":[],"series":[]}), "claude").is_none());
        let captured = measured(&report(), "claude").unwrap();
        assert!((captured.usd_per_point - 28.4).abs() < 0.1);
        assert_eq!(captured.row_date, "2026-10-09");
        let report = json!({"latest":[{"quota_provider":"claude","observation_day":"2026-10-09","snapshot_at_ms":NOW,"interval_end_ms":NOW-1,"api_nanos":284000000000i64,"delta_percent":10.0,"subscription_nanos":4666666667i64,"multiplier":60.8,"error_deflating":"unpriced tokens","error_inflating":"excluded accounts"}],"series":[]});
        let row = measured(&report, "claude").unwrap();
        assert!((row.usd_per_point - 28.4).abs() < 1e-12);
        assert_eq!(row.interval_end_ms, NOW - 1);
        assert_eq!(row.row_date, "2026-10-09");
    }

    #[test]
    fn raw_used_percent_survives_banked_reset_zeroing() {
        let state = state(89.0, Some("max_20x"));
        let cache = build(&state, &report(), &prices(), &HashMap::new(), NOW);
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
    fn fusiform_conservative_low_requires_live_value_and_operator_tier() {
        let p = prices();
        let row = price_row(&p, "anthropic", "max_20x").unwrap();
        assert_eq!(fusiform_value(row, NOW), Some(57.4));
        assert_eq!(
            fusiform_value(price_row(&p, "openai", "pro_200").unwrap(), NOW),
            Some(29.0)
        );
        assert_eq!(fusiform_value(row, 1794182400001), None);
        assert_eq!(
            fusiform_value(price_row(&p, "anthropic", "max_5x").unwrap(), NOW),
            None
        );
        assert_eq!(
            fusiform_value(price_row(&p, "openai", "plus").unwrap(), NOW),
            None
        );
        let without_value = json!({"provider_id":"anthropic", "tier":"max_20x"});
        assert_eq!(fusiform_value(&without_value, NOW), None);
        let explicitly_retired = json!({"retired":true, "quota_value":row["quota_value"]});
        assert_eq!(fusiform_value(&explicitly_retired, NOW), None);
        let mut stale_retired_prices = p.clone();
        for tier in ["pro_5x", "pro_20x"] {
            stale_retired_prices["prices"]
                .as_array_mut()
                .unwrap()
                .push(json!({"provider_id":"anthropic", "tier":tier}));
            let retired_row = price_row(&stale_retired_prices, "anthropic", tier).unwrap();
            assert_eq!(fusiform_value(retired_row, NOW), None);
        }
        let refused_row = price_row(&p, "openai", "pro").unwrap();
        assert!(refused_row.get("refusal_reason").is_some());
        assert_eq!(fusiform_value(refused_row, NOW), None);
        let without_tier = build(&state(3.0, None), &report(), &p, &HashMap::new(), NOW);
        assert_eq!(
            without_tier.pools["anthropic"].plans[0].fusiform_multiplier,
            None
        );
        let with_tier = build(
            &state(3.0, Some("max_20x")),
            &report(),
            &p,
            &HashMap::new(),
            NOW,
        );
        assert_eq!(
            with_tier.pools["anthropic"].plans[0].fusiform_multiplier,
            Some(57.4)
        );
        assert_eq!(with_tier.pools["anthropic"].plans[0].fee_usd, 200.0);
    }

    #[test]
    fn unpriced_tier_health_is_deduplicated_and_names_unknown_and_refused_rows() {
        use prefrontal_ckcred_client::{ListedCredential, ScopedCredentialListing};
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
            unpriced_tiers(&listing, &prices(), NOW),
            BTreeSet::from([
                "unpriced plan tier: anthropic/pro_5x".into(),
                "unpriced plan tier: openai/pro".into()
            ])
        );
    }

    #[test]
    fn free_other_provider_disables_banked_reserves() {
        let mut pools = build(
            &state(3.0, None),
            &report(),
            &prices(),
            &HashMap::new(),
            NOW,
        )
        .pools;
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

    #[test]
    fn astrocyte_captured_pace_frames_are_sha256_pinned() {
        // These are production Response-frame bodies from Astrocyte's captured
        // `spend.multiplier.pace` vectors at commit `9e1841c`. The paths and
        // hashes identify the exact producer-owned bytes copied into this crate;
        // the assertions below prevent fixture edits from changing that contract.
        // `crates/astrocyte-core/tests/vectors/spend-multiplier-pace/claude-2026-10-09T08Z-24h.json`
        // SHA-256 `4092920808e92a18648e4db427c539b22f33b290a7a68b205f23551b09659273`, and
        // `crates/astrocyte-core/tests/vectors/spend-multiplier-pace/codex-2026-10-09T08Z-24h.json`
        // SHA-256 `a999a212e65672599a810a0578361b5fdeda1fc0753cc23e6e7a42838e3dc179`.
        let fixtures: [(&[u8], &str); 2] = [
            (
                include_bytes!(
                    "../fixtures/astrocyte-spend-multiplier-pace-claude-2026-10-09T08Z-24h.json"
                ),
                "4092920808e92a18648e4db427c539b22f33b290a7a68b205f23551b09659273",
            ),
            (
                include_bytes!(
                    "../fixtures/astrocyte-spend-multiplier-pace-codex-2026-10-09T08Z-24h.json"
                ),
                "a999a212e65672599a810a0578361b5fdeda1fc0753cc23e6e7a42838e3dc179",
            ),
        ];
        for (bytes, expected) in fixtures {
            assert_eq!(format!("{:x}", Sha256::digest(bytes)), expected);
        }
    }

    #[test]
    fn captured_pace_frames_decode_and_apply_the_head_reserve() {
        // These are production Response frame bodies from Astrocyte commit
        // `9e1841c`; each frame contains a `result` envelope. Claude's head
        // producers sum to 45.66346337585237 points over 24 hours, or
        // 1.9026443073271821 points/hour. Codex's sum is 26.10972709861734,
        // or 1.0879052957757225 points/hour. The broker (`broca`) is excluded.
        let claude_frame: Value = serde_json::from_slice(include_bytes!(
            "../fixtures/astrocyte-spend-multiplier-pace-claude-2026-10-09T08Z-24h.json"
        ))
        .unwrap();
        let codex_frame: Value = serde_json::from_slice(include_bytes!(
            "../fixtures/astrocyte-spend-multiplier-pace-codex-2026-10-09T08Z-24h.json"
        ))
        .unwrap();
        let from = 1_791_532_800_000;
        let to = 1_791_619_200_000;
        let claude_pace = head_pace(&claude_frame["result"], "claude", from, to).unwrap();
        let codex_pace = head_pace(&codex_frame["result"], "codex", from, to).unwrap();
        assert!((claude_pace - 1.9026443073271821).abs() < 1e-12);
        assert!((codex_pace - 1.0879052957757225).abs() < 1e-12);

        let mut quota = state(3.0, Some("max_20x"));
        quota
            .readings
            .readings
            .get_mut(&("claude".into(), "a".into()))
            .unwrap()
            .entry["usage"]["secondary"]["resetsAt"] = json!("2026-10-09T02:00:00Z");
        let pools = build(
            &quota,
            &report(),
            &prices(),
            &HashMap::from([("claude".into(), claude_pace)]),
            NOW,
        );
        let pool = &pools.pools["anthropic"];
        assert_eq!(pool.plans[0].head_points_per_hour, Some(claude_pace));
        let worker = pool.price(0.1, None, 20.0, false).unwrap();
        assert!(
            (worker.head_reserve_points.unwrap()
                - claude_pace * pool.plans[0].accounts[0].hours_to_reset)
                .abs()
                < 1e-12
        );
        assert_eq!(
            pool.price(0.1, None, 20.0, true)
                .unwrap()
                .head_reserve_points,
            Some(0.0)
        );
    }

    fn pi_incident_input(
        harness: Option<&str>,
        target_agent: &str,
        requirements: Value,
    ) -> (
        prefrontal_routing_core::model_router::SelectModelRouteInput<'static>,
        Value,
    ) {
        use prefrontal_routing_core::{
            billing::{BillingPlane, BillingRate},
            model_router::{RouteRuntimeState, SelectModelRouteInput},
        };
        let fixture: Value = serde_json::from_str(include_str!(
            "../fixtures/pi-mason-subscription-reserve.json"
        ))
        .unwrap();
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
        let mut pool = build(
            &state,
            &Value::Null,
            &prices(),
            &HashMap::from([(
                "claude".into(),
                fixture["head_points_per_hour"].as_f64().unwrap(),
            )]),
            now,
        )
        .pools
        .remove("anthropic")
        .unwrap();
        pool.keep_free_percent = fixture["keep_free_percent"].as_f64().unwrap();

        // The fixture stores account readings and demand vectors. This synthetic
        // workload exercises basket pricing on the roster-selected OAuth plane
        // without depending on live usage statistics.
        let mut fact: ModelFact = serde_json::from_value(json!({
            "providerID":"anthropic", "modelID":"claude-opus-5-5", "confidence":"curated",
            "scores":{"elo":1960.0,"eq":2200.0,"speed":0.0}, "costPerMTokOut":20.0
        }))
        .unwrap();
        fact.class_vector = Some(prefrontal_routing_core::ClassVector {
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
        credentials.refresh_serving_methods(&state, &[fact.clone()], now);
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
        let resolved = crate::resolve_select_request(
            &params,
            &Default::default(),
            Path::new("/unused/host-auth.json"),
        )
        .unwrap();
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
        (
            SelectModelRouteInput {
                target_agent: params.target_agent,
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
            },
            fixture,
        )
    }

    #[test]
    fn pi_masons_respect_the_same_incident_subscription_reserve_as_opencode() {
        use prefrontal_routing_core::model_router::select_model_route;
        let (_, fixture) = pi_incident_input(Some("pi"), "mason", json!({"iq":85,"eq":15}));
        for decision in fixture["decisions"].as_array().unwrap() {
            let mut rejections = Vec::new();
            for harness in [Some("pi"), Some("opencode"), None] {
                let (input, _) =
                    pi_incident_input(harness, "mason", decision["requirements"].clone());
                let error =
                    select_model_route(input).expect_err("workers cannot spend the head reserve");
                assert_eq!(error.rejected.len(), 1, "{harness:?}: {}", decision["id"]);
                let rejection = &error.rejected[0];
                assert_eq!(rejection.route_key, "anthropic/claude-opus-5-5");
                assert_eq!(rejection.reason, "subscription_reserve");
                let detail: Value =
                    serde_json::from_str(rejection.detail.as_deref().unwrap()).unwrap();
                assert_eq!(detail["pool"], "anthropic");
                assert_eq!(detail["unit"], "weekly_percentage_points_per_account");
                let plan = &detail["plans"][0];
                for (key, value) in fixture["expected_plan"].as_object().unwrap() {
                    if key == "headReservePoints" {
                        assert!(
                            (plan[key].as_f64().unwrap() - value.as_f64().unwrap()).abs() < 1e-6
                        );
                    } else {
                        assert_eq!(&plan[key], value, "{key}");
                    }
                }
                rejections.push(error.rejected);
            }
            assert_eq!(rejections[0], rejections[1]);
            assert_eq!(rejections[0], rejections[2]);
        }
    }

    #[test]
    fn incident_subscription_heads_keep_access_and_subscription_pricing() {
        for harness in ["pi", "opencode", "claude-code"] {
            let (input, _) = pi_incident_input(Some(harness), harness, json!({"iq":85,"eq":15}));
            let selected =
                prefrontal_routing_core::model_router::select_model_route(input).unwrap();
            assert_eq!(selected.route_key, "anthropic/claude-opus-5-5");
            let pricing = selected.score.objective.subscription_pricing.unwrap();
            assert_eq!(pricing.head_reserve_points, Some(0.0));
            assert_eq!(pricing.available_points, Some(232.0));
        }
    }

    #[test]
    fn incident_subscription_workers_get_subscription_pricing_when_head_reserve_clears() {
        for harness in ["pi", "opencode"] {
            let (mut input, _) =
                pi_incident_input(Some(harness), "mason", json!({"iq":85,"eq":15}));
            let pool = input
                .runtime_snapshot
                .as_mut()
                .unwrap()
                .by_capacity
                .get_mut("anthropic/claude-opus-5-5")
                .unwrap()
                .subscription_pool
                .as_mut()
                .unwrap();
            Arc::make_mut(pool).plans[0].head_points_per_hour = Some(0.0);
            let selected =
                prefrontal_routing_core::model_router::select_model_route(input).unwrap();
            let pricing = selected.score.objective.subscription_pricing.unwrap();
            assert_eq!(pricing.head_reserve_points, Some(0.0));
            assert_eq!(pricing.available_points, Some(166.0));
            assert_eq!(
                selected.score.pricing_mode,
                prefrontal_routing_core::model_router::PricingMode::Basket
            );
        }
    }

    #[test]
    fn incident_api_plane_is_still_metered_not_subscription_reserved() {
        let (mut input, _) = pi_incident_input(Some("pi"), "mason", json!({"iq":85,"eq":15}));
        let plane = input.facts[0].billing_plane.as_mut().unwrap();
        plane.auth_method = "apikey".into();
        plane.kind = "upstream".into();
        let selected = prefrontal_routing_core::model_router::select_model_route(input).unwrap();
        assert!(selected.score.objective.subscription_pricing.is_none());
        assert!(selected
            .explanation
            .iter()
            .any(|line| line.contains("metered")));
    }

    #[test]
    fn incident_panel_still_reports_weekly_headroom_reserved_for_heads() {
        let (input, _) = pi_incident_input(Some("opencode"), "mason", json!({"iq":70,"eq":0}));
        let panel =
            prefrontal_routing_core::select_panel(prefrontal_routing_core::SelectPanelInput {
                panel_spec: prefrontal_routing_core::PanelSpec {
                    size: 4,
                    iq_floor: 70.0,
                    diversity: prefrontal_routing_core::PanelDiversity::Family,
                    class: prefrontal_routing_core::PanelClass::Audit,
                    temperature_spread: None,
                },
                facts: input.facts,
                available_models: input.available_models,
                availability_substrate: prefrontal_routing_core::AvailabilitySubstrate::Broca,
                runtime_snapshot: input.runtime_snapshot,
                is_subscription_route: input.is_subscription_route,
                package_tokens: None,
                selection: prefrontal_routing_core::PanelSelection::NPick,
                roster: Vec::new(),
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: None,
            })
            .unwrap();
        assert!(panel.members.is_empty());
        let causes = panel.shortfall.unwrap().causes;
        assert_eq!(causes.len(), 1);
        assert_eq!(causes[0].model, "anthropic/claude-opus-5-5");
        assert_eq!(
            causes[0].reason,
            prefrontal_routing_core::PanelCauseReason::QuotaExhausted
        );
        assert_eq!(
            causes[0].detail.as_deref(),
            Some("weekly headroom is reserved for heads")
        );
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

    fn captured_head_paces() -> HashMap<String, f64> {
        [
            (
                "claude",
                include_bytes!(
                    "../fixtures/astrocyte-spend-multiplier-pace-claude-2026-10-09T08Z-24h.json"
                )
                .as_slice(),
            ),
            (
                "codex",
                include_bytes!(
                    "../fixtures/astrocyte-spend-multiplier-pace-codex-2026-10-09T08Z-24h.json"
                )
                .as_slice(),
            ),
        ]
        .into_iter()
        .map(|(provider, bytes)| {
            let frame: Value = serde_json::from_slice(bytes).unwrap();
            let reply = &frame["result"];
            (
                provider.into(),
                head_pace(
                    reply,
                    provider,
                    reply["from_ms"].as_i64().unwrap(),
                    reply["to_ms"].as_i64().unwrap(),
                )
                .unwrap(),
            )
        })
        .collect()
    }

    fn outage_selection(
        paces: &HashMap<String, f64>,
    ) -> Result<
        prefrontal_routing_core::model_router::SelectedModelRoute,
        prefrontal_routing_core::model_router::NoRouteError,
    > {
        use prefrontal_routing_core::model_router::{
            select_model_route, RouteRuntimeState, SelectModelRouteInput,
        };
        let (state, fixture) = outage_fixture();
        let now = fixture["now_ms"].as_i64().unwrap();
        let mut pools = build(&state, &Value::Null, &prices(), paces, now).pools;
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
            requirements: prefrontal_routing_core::model_facts::ModelRequirements {
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
        })
    }

    #[test]
    fn realistic_outage_snapshot_leaves_iq70_worker_route_without_spending_saved_resets() {
        let selected =
            outage_selection(&captured_head_paces()).expect("pooled quota can serve a worker");
        assert_eq!(selected.route_key, "openai/gpt-6.1-sol");
        assert!(selected
            .rejected
            .iter()
            .any(|r| r.route_key == "xai/grok-4.7" && r.reason == "quota_exhausted"));
        let pricing = selected.score.objective.subscription_pricing.unwrap();
        // Copied headroom is 90 points on Pro Lite. One third of the captured
        // provider pace projects to 60.467435696986634 points over its horizon.
        assert_eq!(pricing.plan_tier.as_deref(), Some("pro_100"));
        assert_eq!(pricing.banked_resets_applied, 0);
        assert!((pricing.available_points.unwrap() - 29.532564303013366).abs() < 1e-9);
        assert!((pricing.head_reserve_points.unwrap() - 60.467435696986634).abs() < 1e-9);
    }

    #[test]
    fn provider_head_pace_is_conserved_across_copied_plan_groups() {
        let (state, fixture) = outage_fixture();
        let paces = captured_head_paces();
        let pools = build(
            &state,
            &Value::Null,
            &prices(),
            &paces,
            fixture["now_ms"].as_i64().unwrap(),
        );
        let plans = &pools.pools["openai"].plans;
        assert_eq!(plans.len(), 2);
        let sum: f64 = plans.iter().map(|p| p.head_points_per_hour.unwrap()).sum();
        assert!((sum - 1.0879052957757225).abs() < 1e-12);
        let pro = plans
            .iter()
            .find(|p| p.tier.as_deref() == Some("pro_200"))
            .unwrap();
        assert_eq!(pro.accounts.len(), 2);
        assert!(pro.accounts.iter().all(|a| a.weekly_used == 100.0));
        assert_eq!(
            pro.accounts
                .iter()
                .map(|a| a.banked_resets)
                .collect::<Vec<_>>()
                .iter()
                .sum::<u32>(),
            3
        );
        assert!((pro.head_points_per_hour.unwrap() - 0.725270197183815).abs() < 1e-12);
    }

    #[test]
    fn projected_head_demand_exceeding_copied_pooled_headroom_still_blocks_workers() {
        let error = outage_selection(&HashMap::from([
            ("claude".into(), 10.0),
            ("codex".into(), 10.0),
        ]))
        .unwrap_err();
        assert_eq!(
            error
                .rejected
                .iter()
                .filter(|r| r.reason == "subscription_reserve")
                .count(),
            3
        );
        for rejection in error
            .rejected
            .iter()
            .filter(|r| r.reason == "subscription_reserve")
        {
            let detail: Value = serde_json::from_str(rejection.detail.as_deref().unwrap()).unwrap();
            assert_eq!(detail["unit"], "weekly_percentage_points_per_account");
            assert!(detail["plans"]
                .as_array()
                .unwrap()
                .iter()
                .all(|p| p["availablePoints"] == 0.0));
        }
    }

    #[test]
    fn one_hour_removed_from_captured_pace_frame_is_unavailable() {
        // This labelled one-edit derivation removes one hour from the captured
        // Claude frame; it is a gap test, not a separate Astrocyte capture.
        let mut frame: Value = serde_json::from_slice(include_bytes!(
            "../fixtures/astrocyte-spend-multiplier-pace-claude-2026-10-09T08Z-24h.json"
        ))
        .unwrap();
        frame["result"]["hours"].as_array_mut().unwrap().remove(0);
        assert!(head_pace(
            &frame["result"],
            "claude",
            1_791_532_800_000,
            1_791_619_200_000
        )
        .is_none());
    }

    #[test]
    fn no_observation_refusal_derived_from_capture_is_unavailable() {
        // One edit replaces the captured result with Astrocyte's documented
        // `no_multiplier_observation` ErrorBody (synthetic, not a capture).
        // The source refusal is at `crates/astrocyte-core/src/multiplier.rs`
        // in commit `9e1841c`.
        let mut frame: Value = serde_json::from_slice(include_bytes!(
            "../fixtures/astrocyte-spend-multiplier-pace-claude-2026-10-09T08Z-24h.json"
        ))
        .unwrap();
        frame["result"] = json!({
            "code": "no_multiplier_observation",
            "message": "no multiplier row is recorded for this provider",
            "detail": null
        });
        assert_eq!(frame["result"]["code"], "no_multiplier_observation");
        let body = &frame["result"];
        let error = QuotaConsumerError::Message(format!("usage.get failed: {body}"));
        assert!(!pace_call_is_transient(&error));
        assert!(pace_call_is_transient(&QuotaConsumerError::Message(
            "read timed out".into()
        )));
        assert!(
            head_pace_result(Err(error), "claude", 1_791_532_800_000, 1_791_619_200_000).is_none()
        );
    }

    #[test]
    fn pace_request_has_exact_fields_and_hour_aligned_times() {
        let (from, to) = pace_window(1_791_619_200_000 + 12_345);
        assert_eq!(to, 1_791_619_200_000);
        assert_eq!(from, 1_791_532_800_000);

        for provider in ["claude", "codex"] {
            let request = pace_request(provider, from, to);
            assert_eq!(request["method"], "spend.multiplier.pace");
            let params = request["params"].as_object().unwrap();
            assert_eq!(params.len(), 3);
            assert_eq!(params["quota_provider"], provider);
            assert_eq!(params["from_ms"], from);
            assert_eq!(params["to_ms"], to);
        }
    }
}
