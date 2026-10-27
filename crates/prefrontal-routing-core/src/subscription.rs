//! Subscription economics in pooled percentage points (one account-window percent).
//! This module consumes snapshots only; selection never calls a quota service.

use serde::Serialize;

pub const ASTRO_MAX_AGE_MS: i64 = 48 * 60 * 60 * 1000;
pub const DEFAULT_PLAN_FEE_USD: f64 = 200.0;
/// No measured subsidy is assumed when all value sources are unknown.
pub const DEFAULT_VALUE_MULTIPLIER: f64 = 1.0;

#[derive(Debug, Clone, PartialEq)]
pub struct MeasuredValue {
    pub usd_per_point: f64,
    pub interval_end_ms: i64,
    pub row_date: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscriptionPool {
    pub provider: String,
    pub keep_free_percent: f64,
    pub measured: Option<MeasuredValue>,
    pub plans: Vec<SubscriptionPlan>,
    /// False while another available subscription plan has live, unreserved quota.
    pub allow_banked: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionPlan {
    pub tier: Option<String>,
    pub fee_usd: f64,
    pub fusiform_multiplier: Option<f64>,
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
    pub value_per_point_usd: f64,
    pub value_source: String,
    pub message_points: f64,
    /// The basket's message, or the common output-only reference unit.
    pub price_unit: &'static str,
    pub fee_per_point_usd: f64,
    pub plan_pressure: f64,
    pub keep_free_percent: f64,
    pub available_points: Option<f64>,
    pub banked_resets_applied: u32,
    pub plan_tier: Option<String>,
}

pub fn resolve_value(
    operator_multiplier: Option<f64>,
    measured: Option<&MeasuredValue>,
    fusiform_multiplier: Option<f64>,
    fee_per_point: f64,
    now_ms: i64,
) -> (f64, String) {
    let valid = |v: &f64| v.is_finite() && *v > 0.0;
    if let Some(multiplier) = operator_multiplier.filter(valid) {
        return (multiplier * fee_per_point, "operator override".into());
    }
    if let Some(row) = measured.filter(|row| {
        valid(&row.usd_per_point)
            && now_ms >= row.interval_end_ms
            && now_ms - row.interval_end_ms < ASTRO_MAX_AGE_MS
    }) {
        return (row.usd_per_point, format!("astrocyte {}", row.row_date));
    }
    if let Some(multiplier) = fusiform_multiplier.filter(valid) {
        return (
            multiplier * fee_per_point,
            "fusiform (conservative low)".into(),
        );
    }
    (
        DEFAULT_VALUE_MULTIPLIER * fee_per_point,
        "default (unmeasured)".into(),
    )
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
        self.plans
            .iter()
            .enumerate()
            .filter_map(|(index, plan)| {
                let (points, pressure, banked) = plan.budget(
                    keep_free,
                    head,
                    self.banked_for(index, keep_free, head),
                    self.now_ms,
                );
                if points.is_some_and(|p| p <= 0.0) || points.is_none() && !head && keep_free > 0.0
                {
                    return None;
                }
                let fee = plan.fee_usd / 100.0;
                let (value, source) = resolve_value(
                    operator,
                    self.measured.as_ref(),
                    plan.fusiform_multiplier,
                    fee,
                    self.now_ms,
                );
                Some(SubscriptionPricing {
                    value_per_point_usd: value,
                    value_source: source,
                    message_points: list_usd / value,
                    price_unit: "message",
                    fee_per_point_usd: fee,
                    plan_pressure: pressure,
                    keep_free_percent: if head { 0.0 } else { keep_free },
                    available_points: points,
                    banked_resets_applied: banked,
                    plan_tier: plan.tier.clone(),
                })
            })
            .min_by(|a, b| a.dollars().total_cmp(&b.dollars()))
    }
}

impl SubscriptionPricing {
    pub fn dollars(&self) -> f64 {
        self.message_points * self.fee_per_point_usd * self.plan_pressure
    }

    pub fn explanation(&self) -> String {
        format!("subscription value ${:.4}/point ({}); {} {:.6} pooled points; plan pressure {:.4}; keep-free margin {:.1}%; banked resets applied {}",
            self.value_per_point_usd, self.value_source, self.price_unit, self.message_points, self.plan_pressure,
            self.keep_free_percent, self.banked_resets_applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_precedence_override_fresh_stale_and_absent() {
        let row = MeasuredValue {
            usd_per_point: 28.4,
            interval_end_ms: 1_000,
            row_date: "2026-10-09".into(),
        };
        assert_eq!(
            resolve_value(Some(18.0), Some(&row), Some(27.5), 2.0, 1_001),
            (36.0, "operator override".into())
        );
        assert_eq!(
            resolve_value(None, Some(&row), Some(27.5), 2.0, 1_001),
            (28.4, "astrocyte 2026-10-09".into())
        );
        assert_eq!(
            resolve_value(None, Some(&row), None, 2.0, 1_000 + ASTRO_MAX_AGE_MS),
            (2.0, "default (unmeasured)".into())
        );
        assert_eq!(
            resolve_value(None, None, None, 2.0, 1_001),
            (2.0, "default (unmeasured)".into())
        );
        assert_eq!(
            resolve_value(None, Some(&row), Some(27.5), 2.0, 1_000 + ASTRO_MAX_AGE_MS),
            (55.0, "fusiform (conservative low)".into())
        );
        assert_eq!(
            resolve_value(None, Some(&row), None, 2.0, 999).1,
            "default (unmeasured)"
        );
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
            fee_usd: 200.0,
            fusiform_multiplier: None,
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
