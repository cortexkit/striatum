//! Subscription economics in pooled percentage points (one account-window percent).
//! This module consumes snapshots only; selection never calls a quota service.

use serde::Serialize;
use std::collections::BTreeMap;

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
    /// How the provider-wide head pace was assembled from usage per work class.
    /// Kept only so the reserve diagnostic can show what was counted and what
    /// was left out; admission arithmetic reads each plan's share instead.
    pub head_pace: Option<HeadPaceBreakdown>,
}

/// Provider-wide head demand split by the work class each usage event carried,
/// in pooled points per hour over the measured window. Usage from classes that
/// `is_head` rejects (workers, readers, reviewers) is reported separately and
/// is not part of `head_points_per_hour`. Usage without a class is counted as
/// head demand under the `unlabelled` key: it may come from a head, and
/// under-reserving for heads is the harmful direction.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeadPaceBreakdown {
    pub window_hours: u32,
    pub head_points_per_hour: f64,
    pub head_by_work_class: BTreeMap<String, f64>,
    pub excluded_worker_points_per_hour: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionPlan {
    pub tier: Option<String>,
    pub fee_usd: f64,
    pub fusiform_multiplier: Option<f64>,
    pub accounts: Vec<QuotaAccount>,
    /// This tier's share of provider-wide head demand in pooled points per hour.
    /// Shares across the provider's tiers must sum to the measured provider pace.
    pub head_points_per_hour: Option<f64>,
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
pub struct ReserveStep {
    pub hours_from_now: f64,
    pub phase: &'static str,
    pub restored_headroom_points: f64,
    pub headroom_points: f64,
    pub keep_free_points: f64,
    pub banked_points: f64,
    pub projected_head_points: f64,
    /// Signed budget before the admission floor, so deficits remain visible.
    pub unreserved_points: f64,
    pub available_points: f64,
}

struct ReserveProjection {
    steps: Vec<ReserveStep>,
    tightest_index: usize,
}

impl ReserveProjection {
    fn tightest(&self) -> &ReserveStep {
        &self.steps[self.tightest_index]
    }
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
    /// Full-horizon demand; reset credits are included in available_points.
    pub head_reserve_points: Option<f64>,
    pub available_points: Option<f64>,
    pub head_reserve_steps: Option<Vec<ReserveStep>>,
    pub tightest_point: Option<ReserveStep>,
    pub banked_resets_applied: u32,
    pub plan_tier: Option<String>,
}

/// True for agents whose demand the subscription reserve protects. The same
/// rule decides which Astrocyte work classes count toward head pace.
pub fn is_head(agent: &str) -> bool {
    matches!(
        agent.trim().to_ascii_lowercase().as_str(),
        "alfonso" | "head" | "opencode" | "pi" | "claude-code"
    )
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
    fn reserve_projection(
        &self,
        margin: f64,
        pace: f64,
        horizon_hours: f64,
        banked_points: f64,
    ) -> ReserveProjection {
        let mut headroom: f64 = self
            .accounts
            .iter()
            .filter(|a| !a.short_walled)
            .map(|a| (100.0 - a.weekly_used).max(0.0))
            .sum();
        let mut after_margin: f64 = self
            .accounts
            .iter()
            .filter(|a| !a.short_walled)
            .map(|a| (100.0 - a.weekly_used - margin).max(0.0))
            .sum();
        let mut restored = 0.0;
        let mut steps = Vec::new();
        let mut record = |hours, phase, headroom, after_margin, restored| {
            let projected = pace.max(0.0) * hours;
            let unreserved: f64 = after_margin + banked_points - projected;
            steps.push(ReserveStep {
                hours_from_now: hours,
                phase,
                restored_headroom_points: restored,
                headroom_points: headroom,
                keep_free_points: headroom - after_margin,
                banked_points,
                projected_head_points: projected,
                unreserved_points: unreserved,
                available_points: unreserved.max(0.0),
            });
        };
        record(0.0, "now", headroom, after_margin, restored);
        let mut resets: Vec<_> = self
            .accounts
            .iter()
            .filter(|a| {
                // A weekly reset cannot clear a separate short-window wall.
                !a.short_walled && a.hours_to_reset > 0.0 && a.hours_to_reset <= horizon_hours
            })
            .collect();
        resets.sort_by(|a, b| a.hours_to_reset.total_cmp(&b.hours_to_reset));
        let mut index = 0;
        while index < resets.len() {
            let hours = resets[index].hours_to_reset;
            // Demand accumulates before capacity returns. Checking only after a
            // reset would allow workers to spend quota needed to reach that reset.
            record(hours, "before_reset", headroom, after_margin, restored);
            while index < resets.len() && resets[index].hours_to_reset == hours {
                let account = resets[index];
                let current = (100.0 - account.weekly_used).max(0.0);
                let current_after_margin = (current - margin).max(0.0);
                let returned = 100.0 - current;
                headroom += returned;
                restored += returned;
                // Preserve the per-account margin: a walled account adds no
                // quota now and adds only 100 minus its margin when it resets.
                after_margin += (100.0 - margin).max(0.0) - current_after_margin;
                index += 1;
            }
            record(hours, "after_reset", headroom, after_margin, restored);
        }
        record(horizon_hours, "horizon", headroom, after_margin, restored);
        let tightest_index = steps
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.unreserved_points.total_cmp(&b.unreserved_points))
            .map(|(index, _)| index)
            .unwrap();
        ReserveProjection {
            steps,
            tightest_index,
        }
    }

    fn budget(
        &self,
        keep_free: f64,
        head: bool,
        allow_banked: bool,
        now_ms: i64,
    ) -> (
        Option<f64>,
        f64,
        Option<f64>,
        u32,
        Option<ReserveProjection>,
    ) {
        if self.accounts.is_empty() {
            return (None, 1.0, None, 0, None);
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
        let hours = self
            .accounts
            .iter()
            .map(|a| a.hours_to_reset)
            .fold(0.0, f64::max);
        let reserve = self
            .head_points_per_hour
            .map(|pace| if head { 0.0 } else { pace.max(0.0) * hours });
        let mut projection = (!head).then(|| {
            self.reserve_projection(margin, self.head_points_per_hour.unwrap_or(0.0), hours, 0.0)
        });
        let mut points = projection
            .as_ref()
            .map_or(live_points, |p| p.tightest().available_points);
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
                // One saved reset supplies 100 points minus the account margin
                // immediately. Include that same credit at every reset-event
                // checkpoint, subtracting the single pooled head demand once.
                projection = (!head).then(|| {
                    self.reserve_projection(
                        margin,
                        self.head_points_per_hour.unwrap_or(0.0),
                        hours,
                        100.0 - margin,
                    )
                });
                points = projection
                    .as_ref()
                    .map_or(live_points + 100.0 - margin, |p| {
                        p.tightest().available_points
                    });
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
        (
            Some(points),
            pressure.max(short),
            reserve,
            banked_applied,
            projection,
        )
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
            let (available, _, reserve, banked, projection) = plan.budget(
                keep_free, head, self.banked_for(index, keep_free, head), self.now_ms);
            serde_json::json!({
                "tier": plan.tier,
                "accountCount": plan.accounts.len(),
                "shortWalledAccountCount": plan.accounts.iter().filter(|a| a.short_walled).count(),
                "headroomPoints": headroom,
                "keepFreePercentPerAccount": margin,
                "keepFreePoints": headroom - after_margin,
                "headPointsPerHour": plan.head_points_per_hour,
                "headReservePoints": reserve,
                "availablePoints": available,
                "bankedResetsApplied": banked,
                "resetHorizonHours": plan.accounts.iter().map(|a| a.hours_to_reset).fold(0.0, f64::max),
                "headReserveSteps": projection.as_ref().map(|p| &p.steps),
                "tightestPoint": projection.as_ref().map(|p| p.tightest()),
            })
        }).collect();
        serde_json::json!({
            "pool": self.provider,
            "unit": "weekly_percentage_points_per_account",
            "aggregation": "sum_within_tier",
            "headPace": self.head_pace,
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
                let (points, pressure, reserve, banked, projection) = plan.budget(
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
                    head_reserve_points: reserve,
                    available_points: points,
                    tightest_point: projection.as_ref().map(|p| p.tightest().clone()),
                    head_reserve_steps: projection.map(|p| p.steps),
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
        format!("subscription value ${:.4}/point ({}); {} {:.6} pooled points; plan pressure {:.4}; keep-free margin {:.1}%; {}; banked resets applied {}",
            self.value_per_point_usd, self.value_source, self.price_unit, self.message_points, self.plan_pressure,
            self.keep_free_percent, self.head_reserve_points.map_or_else(|| "head reserve unavailable".into(), |p| format!("head demand {p:.4} points projected across resets")), self.banked_resets_applied)
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
            head_points_per_hour: None,
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
        assert_eq!(p.budget(0.0, false, true, 1).3, 0);
        let p = plan(95.0);
        let ordinary = p.budget(0.0, false, false, 1);
        let banked = p.budget(0.0, false, true, 1);
        assert_eq!(ordinary.3, 0);
        assert_eq!(banked.3, 1);
        assert!(banked.1 < ordinary.1);
        assert_eq!(p.budget(100.0, false, true, 1).0, Some(0.0));
        assert_eq!(p.budget(0.0, false, true, 10_001).3, 0);
    }

    #[test]
    fn head_reserve_is_projected_until_reset_and_heads_ignore_it() {
        let mut p = plan(75.0);
        p.head_points_per_hour = Some(0.1);
        let worker = p.budget(20.0, false, false, 1);
        assert_eq!(worker.0, Some(0.0));
        assert_eq!(worker.2, Some(8.4));
        assert_eq!(p.budget(20.0, true, false, 1).0, Some(25.0));
    }

    #[test]
    fn banked_reset_subtracts_head_demand_once_from_combined_headroom() {
        let mut p = plan(95.0);
        p.head_points_per_hour = Some(1.2);
        let worker = p.budget(0.0, false, true, 1);
        assert_eq!(worker.3, 1);
        assert!((worker.0.unwrap() - 4.2).abs() < 1e-12);
        assert!((worker.2.unwrap() - 100.8).abs() < 1e-12);
    }

    #[test]
    fn tightest_point_after_an_earlier_reset_still_blocks_workers() {
        let mut p = plan(20.0);
        p.accounts[0].hours_to_reset = 10.0;
        let mut walled = plan(100.0).accounts.remove(0);
        walled.hours_to_reset = 80.0;
        p.accounts.push(walled);
        p.head_points_per_hour = Some(3.0);
        // Start with 60 spendable points. Before the first reset: 60 - 30 = 30.
        // It returns only the 20 already spent, not another full account: at
        // hour 80 the budget is 60 + 20 - 240 = -160, so workers must refuse.
        let worker = p.budget(20.0, false, false, 1);
        assert_eq!(worker.0, Some(0.0));
        let tightest = worker.4.as_ref().unwrap().tightest();
        assert_eq!(tightest.hours_from_now, 80.0);
        assert_eq!(tightest.phase, "before_reset");
        assert_eq!(tightest.restored_headroom_points, 20.0);
        assert_eq!(tightest.unreserved_points, -160.0);
        let head = p.budget(20.0, true, false, 1);
        assert_eq!(head.0, Some(80.0));
        assert_eq!(head.2, Some(0.0));
    }

    #[test]
    fn walled_accounts_do_not_supply_capacity_before_resets_beyond_horizon() {
        let mut p = plan(100.0);
        p.head_points_per_hour = Some(1.0);
        p.accounts[0].banked_resets = 0;
        let projection = p.reserve_projection(20.0, 1.0, 24.0, 0.0);
        assert_eq!(projection.steps.len(), 2);
        assert_eq!(projection.tightest().restored_headroom_points, 0.0);
        assert_eq!(projection.tightest().available_points, 0.0);
        assert_eq!(p.budget(20.0, false, false, 1).0, Some(0.0));
        // Heads do not reserve against themselves, but a real quota wall still
        // requires a reset. The existing saved-reset allowance restores access.
        assert_eq!(p.budget(20.0, true, false, 1).2, Some(0.0));
        p.accounts[0].banked_resets = 1;
        assert_eq!(p.budget(20.0, true, true, 1).0, Some(100.0));
    }

    #[test]
    fn banked_credit_is_carried_once_through_the_unclamped_reset_walk() {
        let mut p = plan(20.0);
        p.accounts[0].hours_to_reset = 10.0;
        p.accounts.push(plan(100.0).accounts.remove(0));
        p.accounts[1].hours_to_reset = 80.0;
        p.head_points_per_hour = Some(3.0);
        let worker = p.budget(20.0, false, true, 1);
        // The tightest raw budget is -160; one saved reset adds 80, which is
        // still insufficient. Adding 80 to the clamped zero would admit workers.
        assert_eq!(worker.3, 1);
        assert_eq!(worker.0, Some(0.0));
        let tightest = worker.4.as_ref().unwrap().tightest();
        assert_eq!(tightest.banked_points, 80.0);
        assert_eq!(tightest.unreserved_points, -80.0);
    }

    #[test]
    fn weekly_resets_never_credit_short_walled_accounts() {
        let mut p = plan(100.0);
        p.accounts[0].short_walled = true;
        let projection = p.reserve_projection(20.0, 1.0, 100.0, 0.0);
        assert_eq!(projection.steps.len(), 2);
        assert_eq!(projection.tightest().restored_headroom_points, 0.0);
        assert_eq!(projection.tightest().available_points, 0.0);
    }
}
