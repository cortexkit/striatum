//! Subscription costs use plan value multipliers and pooled quota pressure.
//! This module consumes snapshots only; selection never calls a quota service.

use serde::Serialize;

pub const ASTRO_MAX_AGE_MS: i64 = 48 * 60 * 60 * 1000;
pub const DEFAULT_PLAN_FEE_USD: f64 = 200.0;
#[derive(Debug, Clone, PartialEq)]
pub struct MeasuredValue {
    /// API list value divided by the subscription cost of the quota used.
    pub multiplier: f64,
    pub interval_end_ms: i64,
    pub row_date: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscriptionPool {
    pub provider: String,
    pub keep_free_percent: f64,
    pub plans: Vec<SubscriptionPlan>,
    /// False while another available subscription plan has live, unreserved quota.
    pub allow_banked: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionPlan {
    pub tier: Option<String>,
    /// Exact plan name joined from Astrocyte's per-account plan prices.
    pub plan: Option<String>,
    pub measured: Option<MeasuredValue>,
    pub fee_usd: f64,
    pub accounts: Vec<QuotaAccount>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaAccount {
    pub weekly_used: f64,
    pub weekly_remaining_fraction: f64,
    pub hours_to_reset: f64,
    /// True if any non-weekly live window prevents sending on the account.
    pub short_walled: bool,
    pub short_windows: Vec<QuotaConstraint>,
    pub banked_resets: u32,
    pub banked_expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaConstraint {
    pub window_minutes: i64,
    pub remaining_points: f64,
    pub remaining_fraction: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionPricing {
    pub value_multiplier: f64,
    pub value_source: String,
    pub plan_values: Vec<SubscriptionPlanValue>,
    pub list_cost_usd: f64,
    /// The basket's message, or the common output-only reference unit.
    pub price_unit: &'static str,
    pub plan_pressure: f64,
    pub keep_free_percent: f64,
    pub available_points: Option<f64>,
    pub banked_resets_applied: u32,
    pub plan_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionPlanValue {
    pub plan_tier: Option<String>,
    pub plan: Option<String>,
    pub value_multiplier: f64,
    /// One of measured, operator, or default.
    pub source: &'static str,
    pub weight: f64,
}

impl MeasuredValue {
    fn usable(&self, now_ms: i64) -> bool {
        self.multiplier.is_finite()
            && self.multiplier > 0.0
            && now_ms >= self.interval_end_ms
            && now_ms - self.interval_end_ms < ASTRO_MAX_AGE_MS
    }
}

pub fn resolve_value(
    operator_multiplier: Option<f64>,
    measured: Option<&MeasuredValue>,
    now_ms: i64,
) -> (f64, String) {
    let valid = |v: &f64| v.is_finite() && *v > 0.0;
    if let Some(row) = measured.filter(|row| row.usable(now_ms)) {
        return (row.multiplier, format!("astrocyte {}", row.row_date));
    }
    if let Some(multiplier) = operator_multiplier.filter(valid) {
        return (multiplier, "operator override".into());
    }
    (1.0, "unmeasured".into())
}

/// Remaining time / squared remaining share. For fixed time this is monotone
/// in use and diverges near the wall; at fixed headroom a sooner reset is cheaper.
/// A full window evenly spent has pressure 1. No fitted or learned parameters.
pub fn plan_pressure(remaining_fraction: f64, headroom_share: f64) -> f64 {
    (remaining_fraction.clamp(0.0, 1.0) / headroom_share.max(0.000_001).powi(2))
        .clamp(0.01, 1_000_000.0)
}

impl SubscriptionPlan {
    fn budget(
        &self,
        keep_free: f64,
        head: bool,
        allow_banked: bool,
        now_ms: i64,
    ) -> (Option<f64>, f64, u32) {
        if self.accounts.is_empty() {
            return (None, 1.0, 0);
        }
        let margin = if head { 0.0 } else { keep_free };
        let live: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| !a.short_walled && a.weekly_used < 100.0)
            .collect();
        let live_points: f64 = live
            .iter()
            .map(|a| (100.0 - a.weekly_used - margin).max(0.0))
            .sum();
        let mut points = live_points;
        // Keep paid capacity fixed as accounts wall. Dropping a walled account
        // from the denominator would make the remaining pool suddenly cheaper.
        let capacity = (self.accounts.len().max(1) * 100) as f64;
        let time = self
            .accounts
            .iter()
            .map(|a| a.weekly_remaining_fraction)
            .fold(0.0, f64::max);
        let mut pressure = plan_pressure(time, points / capacity);
        let mut banked_applied = 0;
        // Saved resets are emergency reserves, not live quota. Only one reset is
        // credited at a time, in expiry order, after every plan loses free headroom.
        if allow_banked && points / capacity <= 0.1 && margin < 100.0 {
            let mut reserves: Vec<_> = self
                .accounts
                .iter()
                .filter(|a| {
                    !a.short_walled
                        && a.weekly_used >= 90.0
                        && a.banked_resets > 0
                        && a.banked_expires_at_ms.is_none_or(|expiry| expiry > now_ms)
                })
                .collect();
            reserves.sort_by_key(|a| a.banked_expires_at_ms.unwrap_or(i64::MAX));
            if let Some(account) = reserves.first() {
                points = live_points + 100.0 - margin;
                pressure = plan_pressure(account.weekly_remaining_fraction, points / capacity);
                banked_applied = 1;
            }
        }
        let mut short_pools = std::collections::BTreeMap::<i64, (f64, f64, usize)>::new();
        for account in &live {
            for window in &account.short_windows {
                let pool = short_pools.entry(window.window_minutes).or_default();
                pool.0 += window.remaining_points;
                pool.1 = pool.1.max(window.remaining_fraction);
                pool.2 += 1;
            }
        }
        let short = short_pools
            .values()
            .map(|(points, time, count)| plan_pressure(*time, points / (*count as f64 * 100.0)))
            .fold(0.0, f64::max);
        (Some(points), pressure.max(short), banked_applied)
    }

    pub fn has_free_headroom(&self, keep_free: f64, head: bool, now_ms: i64) -> bool {
        self.budget(keep_free, head, false, now_ms)
            .0
            .is_some_and(|p| p > 0.0)
    }
}

impl SubscriptionPool {
    /// Serialize admission budget arithmetic so refused tiers remain diagnosable.
    pub fn reserve_diagnostic(&self, keep_free: f64, head: bool) -> String {
        let margin = if head { 0.0 } else { keep_free };
        let plans: Vec<_> = self.plans.iter().enumerate().map(|(index, plan)| {
            let headroom: f64 = plan.accounts.iter()
                .filter(|a| !a.short_walled)
                .map(|a| (100.0 - a.weekly_used).max(0.0)).sum();
            let after_margin: f64 = plan.accounts.iter()
                .filter(|a| !a.short_walled)
                .map(|a| (100.0 - a.weekly_used - margin).max(0.0)).sum();
            let (available, _, banked) = plan.budget(
                keep_free, head, self.banked_for(index, keep_free, head), self.now_ms);
            serde_json::json!({
                "tier": plan.tier,
                "accountCount": plan.accounts.len(),
                "shortWalledAccountCount": plan.accounts.iter().filter(|a| a.short_walled).count(),
                "headroomPoints": headroom,
                "keepFreePercentPerAccount": margin,
                "keepFreePoints": headroom - after_margin,
                "availablePoints": available,
                "bankedResetsApplied": banked,
            })
        }).collect();
        serde_json::json!({
            "pool": self.provider,
            "unit": "weekly_percentage_points_per_account",
            "aggregation": "sum_within_tier",
            "rule": "workers may spend only live quota above the per-account keep-free margin; subscription_reserve means no account has quota above that margin",
            "plans": plans,
        })
        .to_string()
    }

    fn banked_for(&self, index: usize, keep_free: f64, head: bool) -> bool {
        self.allow_banked
            && !self.plans.iter().enumerate().any(|(other, plan)| {
                other != index && plan.has_free_headroom(keep_free, head, self.now_ms)
            })
    }

    pub fn available(&self, keep_free: f64, head: bool) -> bool {
        self.plans.is_empty() && (head || keep_free == 0.0)
            || self.plans.iter().enumerate().any(|(index, plan)| {
                match plan
                    .budget(
                        keep_free,
                        head,
                        self.banked_for(index, keep_free, head),
                        self.now_ms,
                    )
                    .0
                {
                    Some(points) => points > 0.0,
                    None => head || keep_free == 0.0,
                }
            })
    }

    pub fn price(
        &self,
        list_usd: f64,
        operator: Option<f64>,
        keep_free: f64,
        head: bool,
    ) -> Option<SubscriptionPricing> {
        let budgets: Vec<_> = self
            .plans
            .iter()
            .enumerate()
            .map(|(index, plan)| {
                plan.budget(
                    keep_free,
                    head,
                    self.banked_for(index, keep_free, head),
                    self.now_ms,
                )
            })
            .collect();
        let total_points: f64 = budgets.iter().filter_map(|b| b.0).sum();
        let plan_values: Vec<_> = self
            .plans
            .iter()
            .zip(&budgets)
            .map(|(plan, budget)| {
                let (value, _) = resolve_value(operator, plan.measured.as_ref(), self.now_ms);
                SubscriptionPlanValue {
                    plan_tier: plan.tier.clone(),
                    plan: plan.plan.clone(),
                    value_multiplier: value,
                    source: if plan
                        .measured
                        .as_ref()
                        .is_some_and(|row| row.usable(self.now_ms))
                    {
                        "measured"
                    } else if operator.is_some_and(|v| v.is_finite() && v > 0.0) {
                        "operator"
                    } else {
                        "default"
                    },
                    weight: if total_points > 0.0 {
                        budget.0.unwrap_or(0.0) / total_points
                    } else if self.plans.len() == 1 && budget.0.is_none() {
                        // A lone unknown-capacity plan retains the no-quota-snapshot fallback.
                        1.0
                    } else {
                        0.0
                    },
                }
            })
            .collect();
        let weighted: Vec<_> = plan_values.iter().filter(|p| p.weight > 0.0).collect();
        let (value, source) = if weighted.len() == 1 {
            let index = plan_values.iter().position(|p| p.weight > 0.0).unwrap();
            resolve_value(operator, self.plans[index].measured.as_ref(), self.now_ms)
        } else if !weighted.is_empty() {
            // Capacity-proportional usage pays list/M for each plan, so costs
            // combine arithmetically and the list-to-quota multiplier harmonically.
            (
                1.0 / weighted
                    .iter()
                    .map(|p| p.weight / p.value_multiplier)
                    .sum::<f64>(),
                "spendable-points weighted harmonic mean".into(),
            )
        } else {
            resolve_value(operator, None, self.now_ms)
        };
        let pricing = |points, pressure, banked, tier| SubscriptionPricing {
            value_multiplier: value,
            value_source: source.clone(),
            plan_values: plan_values.clone(),
            list_cost_usd: list_usd,
            price_unit: "message",
            plan_pressure: pressure,
            keep_free_percent: if head { 0.0 } else { keep_free },
            available_points: points,
            banked_resets_applied: banked,
            plan_tier: tier,
        };
        if self.plans.is_empty() {
            return (head || keep_free == 0.0).then(|| pricing(None, 1.0, 0, None));
        }
        self.plans
            .iter()
            .zip(budgets)
            .filter_map(|(plan, (points, pressure, banked))| {
                if points.is_some_and(|p| p <= 0.0) || points.is_none() && !head && keep_free > 0.0
                {
                    return None;
                }
                Some(pricing(points, pressure, banked, plan.tier.clone()))
            })
            .min_by(|a, b| a.dollars().total_cmp(&b.dollars()))
    }
}

impl SubscriptionPricing {
    pub fn dollars(&self) -> f64 {
        self.list_cost_usd / self.value_multiplier * self.plan_pressure
    }

    pub fn explanation(&self) -> String {
        format!("subscription value multiplier M {:.4} ({}); {} list ${:.6}; plan pressure {:.4}; cost ${:.6}; keep-free margin {:.1}%; banked resets applied {}",
            self.value_multiplier, self.value_source, self.price_unit, self.list_cost_usd, self.plan_pressure,
            self.dollars(), self.keep_free_percent, self.banked_resets_applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_measurement_beats_operator_and_stale_falls_back() {
        let row = MeasuredValue {
            multiplier: 18.7,
            interval_end_ms: 1_000,
            row_date: "2026-10-09".into(),
        };
        assert_eq!(
            resolve_value(Some(6.0), Some(&row), 1_001),
            (18.7, "astrocyte 2026-10-09".into())
        );
        assert_eq!(
            resolve_value(Some(6.0), Some(&row), 1_000 + ASTRO_MAX_AGE_MS - 1),
            (18.7, "astrocyte 2026-10-09".into())
        );
        assert_eq!(
            resolve_value(Some(6.0), Some(&row), 1_000 + ASTRO_MAX_AGE_MS),
            (6.0, "operator override".into())
        );
        assert_eq!(
            resolve_value(None, Some(&row), 1_000 + ASTRO_MAX_AGE_MS),
            (1.0, "unmeasured".into())
        );
        assert_eq!(resolve_value(None, None, 1_001), (1.0, "unmeasured".into()));
        assert_eq!(
            resolve_value(Some(6.0), Some(&row), 999),
            (6.0, "operator override".into())
        );
    }

    #[test]
    fn invalid_multipliers_never_supply_value() {
        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let row = MeasuredValue {
                multiplier: invalid,
                interval_end_ms: 1_000,
                row_date: "2026-10-09".into(),
            };
            assert_eq!(resolve_value(Some(6.0), Some(&row), 1_001).0, 6.0);
            assert_eq!(resolve_value(Some(invalid), Some(&row), 1_001).0, 1.0);
            assert_eq!(resolve_value(Some(invalid), None, 1_001).0, 1.0);
        }
    }

    #[test]
    fn list_divided_by_multiplier_is_cost_before_pressure_for_workers_and_heads() {
        for head in [false, true] {
            for (provider, list, multiplier, expected) in [
                ("codex", 0.84, 18.7, 0.044_919_786_096_256_68),
                ("muse", 0.063, 6.0, 0.0105),
            ] {
                let mut pool = SubscriptionPool {
                    provider: provider.into(),
                    ..Default::default()
                };
                for no_accounts in [false, true] {
                    if no_accounts {
                        pool.plans.push(SubscriptionPlan {
                            tier: None,
                            plan: None,
                            measured: None,
                            fee_usd: 999.0,
                            accounts: Vec::new(),
                        });
                    }
                    let price = pool.price(list, Some(multiplier), 0.0, head).unwrap();
                    assert_eq!(price.plan_pressure, 1.0);
                    assert!((price.dollars() - expected).abs() < 1e-12);
                    assert_eq!(price.list_cost_usd, list);
                }
            }
        }
    }

    #[test]
    fn account_pressure_multiplies_list_divided_by_value_not_plan_fee() {
        let mut pool = SubscriptionPool {
            plans: vec![plan(50.0)],
            ..Default::default()
        };
        for fee in [20.0, 200.0, 999.0] {
            pool.plans[0].fee_usd = fee;
            let price = pool.price(0.063, Some(6.0), 0.0, false).unwrap();
            assert_eq!(price.plan_pressure, 2.0);
            assert!((price.dollars() - 0.021).abs() < 1e-12);
        }
    }

    #[test]
    fn subscription_plan_values_use_spendable_points_weighted_harmonic_mean() {
        let mut measured = plan(10.0);
        measured.measured = Some(MeasuredValue {
            multiplier: 4.0,
            interval_end_ms: 999,
            row_date: "fresh".into(),
        });
        let mut fallback = plan(70.0);
        fallback.tier = Some("pro_100".into());
        fallback.plan = Some("openai/pro_100".into());
        let mut blocked = plan(0.0);
        blocked.tier = Some("blocked".into());
        blocked.plan = Some("blocked".into());
        blocked.accounts[0].short_walled = true;
        let mut pool = SubscriptionPool {
            plans: vec![measured, fallback, blocked],
            now_ms: 1000,
            ..Default::default()
        };
        let price = pool.price(1.0, Some(2.0), 10.0, false).unwrap();
        // Workers have 80 and 20 points after the reserve; the blocked plan has none.
        assert!((price.value_multiplier - 10.0 / 3.0).abs() < 1e-12);
        assert_eq!(
            price
                .plan_values
                .iter()
                .map(|p| (p.source, p.weight))
                .collect::<Vec<_>>(),
            vec![("measured", 0.8), ("operator", 0.2), ("operator", 0.0)]
        );
        assert_eq!(price.plan_values[0].value_multiplier, 4.0);
        assert_eq!(price.plan_values[1].value_multiplier, 2.0);
        let wire = serde_json::to_value(&price).unwrap();
        assert_eq!(wire["planValues"][0]["planTier"], "pro_200");
        assert_eq!(wire["planValues"][0]["plan"], "openai/pro_200");
        assert_eq!(wire["planValues"][0]["source"], "measured");
        assert_eq!(wire["planValues"][0]["weight"], 0.8);
        // Heads spend 90 and 30 points, without the worker reserve.
        assert!(
            (pool
                .price(1.0, Some(2.0), 10.0, true)
                .unwrap()
                .value_multiplier
                - 3.2)
                .abs()
                < 1e-12
        );
        let default = pool.price(1.0, None, 10.0, false).unwrap();
        assert!((default.value_multiplier - 2.5).abs() < 1e-12);
        assert_eq!(default.plan_values[1].source, "default");
        pool.plans.reverse();
        assert_eq!(
            pool.price(1.0, Some(2.0), 10.0, false)
                .unwrap()
                .value_multiplier,
            price.value_multiplier
        );
        pool.plans[1].accounts[0].weekly_used = 100.0;
        let single = pool.price(1.0, Some(2.0), 10.0, false).unwrap();
        assert_eq!(single.value_multiplier, 4.0);
        assert_eq!(single.plan_values[2].weight, 1.0);
    }

    #[test]
    fn pressure_monotone_and_steep_near_wall_and_expiring_headroom_cheap() {
        let mut previous = 0.0;
        for used in 0..100 {
            let pressure = plan_pressure(0.5, (100 - used) as f64 / 100.0);
            assert!(pressure >= previous);
            previous = pressure;
        }
        assert!(plan_pressure(0.5, 0.01) > 90.0 * plan_pressure(0.5, 0.1));
        assert!(plan_pressure(0.01, 0.5) < plan_pressure(0.5, 0.5) / 20.0);
    }

    fn plan(used: f64) -> SubscriptionPlan {
        SubscriptionPlan {
            tier: Some("pro_200".into()),
            plan: Some("openai/pro_200".into()),
            measured: None,
            fee_usd: 200.0,
            accounts: vec![QuotaAccount {
                weekly_used: used,
                weekly_remaining_fraction: 0.5,
                hours_to_reset: 84.0,
                short_walled: false,
                short_windows: Vec::new(),
                banked_resets: 3,
                banked_expires_at_ms: Some(10_000),
            }],
        }
    }

    #[test]
    fn banked_resets_only_near_wall_without_another_free_plan() {
        let p = plan(73.0);
        assert_eq!(p.budget(0.0, false, true, 1).2, 0);
        let p = plan(95.0);
        let ordinary = p.budget(0.0, false, false, 1);
        let banked = p.budget(0.0, false, true, 1);
        assert_eq!(ordinary.2, 0);
        assert_eq!(banked.2, 1);
        assert!(banked.1 < ordinary.1);
        assert_eq!(p.budget(100.0, false, true, 1).0, Some(0.0));
        assert_eq!(p.budget(0.0, false, true, 10_001).2, 0);
    }

    #[test]
    fn fixed_margin_is_the_only_worker_reservation() {
        let mut p = plan(0.0);
        p.accounts = [100.0, 97.0, 3.0, 27.0, 17.0]
            .into_iter()
            .map(|used| plan(used).accounts.remove(0))
            .collect();
        assert_eq!(p.budget(0.0, false, false, 1).0, Some(256.0));
        assert_eq!(p.budget(10.0, false, false, 1).0, Some(223.0));
        assert_eq!(p.budget(20.0, false, false, 1).0, Some(193.0));
        assert_eq!(p.budget(100.0, false, false, 1).0, Some(0.0));
        assert_eq!(p.budget(20.0, true, false, 1).0, Some(256.0));
    }

    #[test]
    fn short_walled_accounts_supply_neither_live_nor_banked_headroom() {
        let mut p = plan(95.0);
        p.accounts[0].short_walled = true;
        assert_eq!(p.budget(0.0, false, true, 1).0, Some(0.0));
        assert_eq!(p.budget(0.0, false, true, 1).2, 0);
        p.accounts[0].short_walled = false;
        assert_eq!(p.budget(0.0, false, true, 1).0, Some(105.0));
        assert_eq!(p.budget(0.0, false, true, 1).2, 1);
    }
}
