//! Subscription costs use plan value multipliers and pooled quota pressure.
//! This module consumes snapshots only; selection never calls a quota service.

use std::borrow::Cow;

use serde::Serialize;

pub const ASTRO_MAX_AGE_MS: i64 = 48 * 60 * 60 * 1000;
// Bridge gaps in eligible observations without letting old plan economics win indefinitely.
pub const LAST_ELIGIBLE_MAX_AGE_MS: i64 = 30 * 24 * 60 * 60 * 1000;
pub const DEFAULT_PLAN_FEE_USD: f64 = 200.0;
/// Below this remaining share, a short window can constrain spending before
/// the weekly reset; above it, weekly pacing also accounts for expiring quota.
pub const SHORT_WINDOW_BINDING_SHARE: f64 = 0.25;

#[derive(Debug, Clone, PartialEq)]
pub struct MeasuredValue {
    /// API list value divided by the subscription cost of the quota used.
    pub multiplier: f64,
    pub interval_end_ms: i64,
    pub row_date: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanMeasurements {
    pub latest: Option<MeasuredValue>,
    pub last_eligible: Option<MeasuredValue>,
}

impl PlanMeasurements {
    pub fn from_latest(latest: MeasuredValue) -> Self {
        Self {
            latest: Some(latest),
            last_eligible: None,
        }
    }

    /// Use the pricing engine's freshness and historical fallback for this pool.
    pub fn multiplier_at(&self, now_ms: i64) -> Option<f64> {
        match resolve_measurement(None, Some(self), now_ms) {
            ResolvedValue::Latest(row) | ResolvedValue::LastEligible(row) => Some(row.multiplier),
            ResolvedValue::Operator(_) | ResolvedValue::Unmeasured => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscriptionPool {
    pub provider: String,
    pub keep_free_percent: f64,
    pub plans: Vec<SubscriptionPlan>,
    /// False while another available subscription plan has live, unreserved quota.
    pub allow_banked: bool,
    /// Count saved windows in pricing only; account admission still uses live quota.
    pub banked_as_capacity: bool,
    pub now_ms: i64,
    pub free_before_reset: Option<free_before_reset::PreviewPool>,
}

pub mod free_before_reset;

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionPlan {
    pub tier: Option<String>,
    /// Exact plan name joined from Astrocyte's per-account plan prices.
    pub plan: Option<String>,
    pub measured: Option<PlanMeasurements>,
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
    /// A named model scope has its own constraint. `None` means the last known
    /// reading is past its reset and cannot establish fresh headroom.
    pub scoped_windows: Vec<(&'static str, Option<QuotaConstraint>)>,
    pub banked_resets: u32,
    pub banked_expires_at_ms: Option<i64>,
    /// Individual expiries, when the producer supplies the saved-credit list.
    pub banked_credit_expiries_ms: Vec<i64>,
    /// Groups of saved resets sharing their kind's reported earliest expiry.
    pub banked_kind_expiries_ms: Vec<(i64, u32)>,
    /// The account holds a spendable prepaid-credits balance, which the
    /// provider draws once the plan's limit is reached. Such an account keeps
    /// serving with no weekly headroom, so it never leaves the pool walled.
    pub prepaid_credits: bool,
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
    pub weekly_pressure: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_window_pressure: Option<f64>,
    pub keep_free_percent: f64,
    pub available_points: Option<f64>,
    pub banked_resets_applied: u64,
    pub plan_tier: Option<String>,
    #[serde(skip)]
    pub free_before_reset: Option<Box<free_before_reset::PricePreview>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PlanPressure {
    plan: f64,
    weekly: f64,
    short_window: Option<f64>,
}

impl PlanPressure {
    fn new(weekly: f64, short_window: Option<f64>) -> Self {
        Self {
            plan: short_window.map_or(weekly, |short| weekly.max(short)),
            weekly,
            short_window,
        }
    }

    fn neutral() -> Self {
        Self::new(1.0, None)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionPlanValue {
    pub plan_tier: Option<String>,
    pub plan: Option<String>,
    pub value_multiplier: f64,
    /// `measured` for a fresh observation, `astrocyte last eligible <day>` for a
    /// historical observation, `operator` for a configured override, or `default` for 1.0.
    pub source: Cow<'static, str>,
    pub weight: f64,
}

impl MeasuredValue {
    fn usable_age(&self, now_ms: i64) -> Option<i64> {
        if !self.multiplier.is_finite() || self.multiplier <= 0.0 {
            return None;
        }
        now_ms
            .checked_sub(self.interval_end_ms)
            .filter(|age| *age >= 0)
    }
}

enum ResolvedValue<'a> {
    Latest(&'a MeasuredValue),
    LastEligible(&'a MeasuredValue),
    Operator(f64),
    Unmeasured,
}

impl ResolvedValue<'_> {
    fn multiplier(&self) -> f64 {
        match self {
            Self::Latest(row) | Self::LastEligible(row) => row.multiplier,
            Self::Operator(value) => *value,
            Self::Unmeasured => 1.0,
        }
    }

    fn source(&self) -> String {
        match self {
            Self::Latest(row) => format!("astrocyte {}", row.row_date),
            Self::LastEligible(row) => format!("astrocyte last eligible {}", row.row_date),
            Self::Operator(_) => "operator override".into(),
            Self::Unmeasured => "unmeasured".into(),
        }
    }

    fn plan_source(&self) -> Cow<'static, str> {
        match self {
            Self::Latest(_) => "measured".into(),
            Self::LastEligible(_) => self.source().into(),
            Self::Operator(_) => "operator".into(),
            Self::Unmeasured => "default".into(),
        }
    }
}

fn resolve_measurement(
    operator_multiplier: Option<f64>,
    measured: Option<&PlanMeasurements>,
    now_ms: i64,
) -> ResolvedValue<'_> {
    if let Some(rows) = measured {
        if let Some(row) = rows.latest.as_ref().filter(|row| {
            row.usable_age(now_ms)
                .is_some_and(|age| age < ASTRO_MAX_AGE_MS)
        }) {
            return ResolvedValue::Latest(row);
        }
        if let Some(row) = rows.last_eligible.as_ref().filter(|row| {
            row.usable_age(now_ms)
                .is_some_and(|age| age <= LAST_ELIGIBLE_MAX_AGE_MS)
        }) {
            return ResolvedValue::LastEligible(row);
        }
    }
    if let Some(multiplier) = operator_multiplier.filter(|v| v.is_finite() && *v > 0.0) {
        return ResolvedValue::Operator(multiplier);
    }
    ResolvedValue::Unmeasured
}

pub fn resolve_value(
    operator_multiplier: Option<f64>,
    measured: Option<&PlanMeasurements>,
    now_ms: i64,
) -> (f64, String) {
    let value = resolve_measurement(operator_multiplier, measured, now_ms);
    (value.multiplier(), value.source())
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
    ) -> (Option<f64>, PlanPressure, u64) {
        if self.accounts.is_empty() {
            return (None, PlanPressure::neutral(), 0);
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
        let pressure = PlanPressure::new(pressure, self.short_pressure());
        (Some(points), pressure, banked_applied)
    }

    fn short_pools(&self) -> std::collections::BTreeMap<i64, (f64, f64, usize)> {
        let mut short_pools = std::collections::BTreeMap::<i64, (f64, f64, usize)>::new();
        for account in self
            .accounts
            .iter()
            .filter(|a| !a.short_walled && a.weekly_used < 100.0)
        {
            for window in &account.short_windows {
                let pool = short_pools.entry(window.window_minutes).or_default();
                pool.0 += window.remaining_points;
                pool.1 = pool.1.max(window.remaining_fraction);
                pool.2 += 1;
            }
        }
        short_pools
    }

    fn short_pressure(&self) -> Option<f64> {
        self.short_pools()
            .values()
            .filter_map(|(points, time, count)| {
                let remaining_share = points / (*count as f64 * 100.0);
                (remaining_share < SHORT_WINDOW_BINDING_SHARE)
                    .then(|| plan_pressure(*time, remaining_share))
            })
            .reduce(f64::max)
    }

    fn capacity_pressure(&self, keep_free: f64, head: bool, now_ms: i64) -> (PlanPressure, u64) {
        let margin = if head { 0.0 } else { keep_free };
        let mut points = 0.0;
        let mut banked = 0_u64;
        let mut time: f64 = 0.0;
        for account in &self.accounts {
            time = time.max(account.weekly_remaining_fraction);
            if !account.short_walled && account.weekly_used < 100.0 {
                points += (100.0 - account.weekly_used - margin).max(0.0);
            }
            let mut unlisted = account.banked_resets;
            let mut count = 0_u64;
            for (expiry, size) in account
                .banked_credit_expiries_ms
                .iter()
                .map(|expiry| (*expiry, 1))
                .chain(account.banked_kind_expiries_ms.iter().copied())
            {
                let size = size.min(unlisted);
                unlisted -= size;
                if expiry > now_ms {
                    count += u64::from(size);
                }
            }
            if account
                .banked_expires_at_ms
                .is_none_or(|expiry| expiry > now_ms)
            {
                count += u64::from(unlisted);
            }
            banked += count;
            points += count as f64 * (100.0 - margin).max(0.0);
        }
        // A saved reset adds a whole window to both sides of the share. It is
        // not spendable live quota and cannot change admission or plan weights.
        let capacity = self.accounts.len() as f64 * 100.0 + banked as f64 * 100.0;
        let weekly = plan_pressure(time, points / capacity.max(100.0));
        (PlanPressure::new(weekly, self.short_pressure()), banked)
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
        // Prepaid credits carry an account past its plan limit, so the keep-free
        // margin, which protects the plan's own quota, cannot leave a pool with
        // such an account empty. The account allocator then spends the credits
        // under its paid-credits policy.
        let credit_backed = self.plans.iter().any(|plan| {
            plan.accounts
                .iter()
                .any(|account| account.prepaid_credits && !account.short_walled)
        });
        credit_backed
            || self.plans.is_empty() && (head || keep_free == 0.0)
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
                let budget = plan.budget(
                    keep_free,
                    head,
                    self.banked_for(index, keep_free, head),
                    self.now_ms,
                );
                if self.banked_as_capacity && !plan.accounts.is_empty() {
                    let (pressure, banked) = plan.capacity_pressure(keep_free, head, self.now_ms);
                    (budget.0, pressure, banked)
                } else {
                    budget
                }
            })
            .collect();
        let total_points: f64 = budgets.iter().filter_map(|b| b.0).sum();
        let plan_values: Vec<_> = self
            .plans
            .iter()
            .zip(&budgets)
            .map(|(plan, budget)| {
                let value = resolve_measurement(operator, plan.measured.as_ref(), self.now_ms);
                SubscriptionPlanValue {
                    plan_tier: plan.tier.clone(),
                    plan: plan.plan.clone(),
                    value_multiplier: value.multiplier(),
                    source: value.plan_source(),
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
        let pricing = |points, pressure: PlanPressure, banked, tier, preview| SubscriptionPricing {
            value_multiplier: value,
            value_source: source.clone(),
            plan_values: plan_values.clone(),
            list_cost_usd: list_usd,
            price_unit: "message",
            plan_pressure: pressure.plan,
            weekly_pressure: pressure.weekly,
            short_window_pressure: pressure.short_window,
            keep_free_percent: if head { 0.0 } else { keep_free },
            available_points: points,
            banked_resets_applied: banked,
            plan_tier: tier,
            free_before_reset: preview,
        };
        if self.plans.is_empty() {
            return (head || keep_free == 0.0)
                .then(|| pricing(None, PlanPressure::neutral(), 0, None, None));
        }
        self.plans
            .iter()
            .zip(budgets)
            .enumerate()
            .filter_map(|(index, (plan, (points, mut pressure, banked)))| {
                if points.is_some_and(|p| p <= 0.0) || points.is_none() && !head && keep_free > 0.0
                {
                    return None;
                }
                let preview = self.free_before_reset.as_ref().map(|pool| {
                    let (projected, mut preview) = pool.project(
                        index,
                        plan,
                        pressure,
                        if head { 0.0 } else { keep_free },
                        self.now_ms,
                    );
                    pressure = projected;
                    preview.final_cost_usd = Some(list_usd / value * pressure.plan);
                    preview.pressure = Some(pressure.plan);
                    preview.weekly_pressure = Some(pressure.weekly);
                    preview.short_window_pressure = pressure.short_window;
                    Box::new(preview)
                });
                Some(pricing(
                    points,
                    pressure,
                    banked,
                    plan.tier.clone(),
                    preview,
                ))
            })
            .min_by(|a, b| {
                a.dollars().total_cmp(&b.dollars()).then_with(|| {
                    a.free_reset_ms()
                        .unwrap_or(i64::MAX)
                        .cmp(&b.free_reset_ms().unwrap_or(i64::MAX))
                })
            })
    }
}

impl SubscriptionPricing {
    pub(crate) fn free_reset_ms(&self) -> Option<i64> {
        self.free_before_reset.as_ref()?.free_reset_ms
    }

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
        let row = PlanMeasurements::from_latest(MeasuredValue {
            multiplier: 18.7,
            interval_end_ms: 1_000,
            row_date: "2026-10-09".into(),
        });
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
            let row = PlanMeasurements::from_latest(MeasuredValue {
                multiplier: invalid,
                interval_end_ms: 1_000,
                row_date: "2026-10-09".into(),
            });
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
        measured.measured = Some(PlanMeasurements::from_latest(MeasuredValue {
            multiplier: 4.0,
            interval_end_ms: 999,
            row_date: "fresh".into(),
        }));
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
                .map(|p| (p.source.as_ref(), p.weight))
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
                scoped_windows: Vec::new(),
                banked_resets: 3,
                banked_expires_at_ms: Some(10_000),
                banked_credit_expiries_ms: Vec::new(),
                banked_kind_expiries_ms: Vec::new(),
                prepaid_credits: false,
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
        assert!(banked.1.plan < ordinary.1.plan);
        assert_eq!(p.budget(100.0, false, true, 1).0, Some(0.0));
        assert_eq!(p.budget(0.0, false, true, 10_001).2, 0);
    }

    #[test]
    fn banked_capacity_prices_every_unexpired_window_without_changing_admission() {
        let mut pro = plan(100.0);
        pro.accounts.push(plan(95.0).accounts.remove(0));
        let mut higher = plan(96.0);
        higher.tier = Some("pro_500".into());
        higher.plan = Some("openai/pro_500".into());
        let mut pool = SubscriptionPool {
            plans: vec![pro, higher],
            allow_banked: true,
            now_ms: 1,
            ..Default::default()
        };
        for account in pool.plans.iter_mut().flat_map(|p| &mut p.accounts) {
            account.banked_resets = 2;
            account.banked_credit_expiries_ms = vec![10_000, 20_000];
        }
        let emergency = pool.price(1.0, None, 0.0, false).unwrap();
        assert_eq!(emergency.plan_pressure, 312.5);
        assert_eq!(emergency.banked_resets_applied, 0);
        let available = pool.available(0.0, false);
        pool.banked_as_capacity = true;
        let capacity = pool.price(1.0, None, 0.0, false).unwrap();
        assert!((capacity.plan_pressure - 1.081_314_878_892_733_4).abs() < 1e-12);
        assert_eq!(capacity.banked_resets_applied, 2);
        assert_eq!(capacity.available_points, emergency.available_points);
        assert_eq!(capacity.plan_values, emergency.plan_values);
        assert_eq!(pool.available(0.0, false), available);
        pool.now_ms = 10_000;
        let partial = pool.price(1.0, None, 0.0, false).unwrap();
        assert_eq!(partial.banked_resets_applied, 1);
        assert!((partial.plan_pressure - 1.849_112_426_035_502_7).abs() < 1e-12);
        pool.now_ms = 20_000;
        assert_eq!(pool.price(1.0, None, 0.0, false).unwrap(), emergency);

        // A pricing-only setting cannot open a route with no serving account.
        pool.allow_banked = false;
        pool.now_ms = 1;
        for account in pool.plans.iter_mut().flat_map(|p| &mut p.accounts) {
            account.weekly_used = 100.0;
        }
        assert!(!pool.available(0.0, false));
        assert!(pool.price(1.0, None, 0.0, false).is_none());
    }

    #[test]
    fn banked_capacity_retains_reserve_and_short_window_pressure() {
        let mut p = plan(95.0);
        p.accounts[0].short_windows.push(QuotaConstraint {
            window_minutes: 300,
            remaining_points: 10.0,
            remaining_fraction: 0.5,
        });
        let (pressure, banked) = p.capacity_pressure(0.0, false, 1);
        assert!((pressure.plan - 50.0).abs() < 1e-12);
        assert_eq!(banked, 3);
        p.accounts[0].short_windows.clear();
        assert_eq!(p.capacity_pressure(100.0, false, 1).0.plan, 1_000_000.0);
        assert!(
            p.capacity_pressure(20.0, false, 1).0.plan > p.capacity_pressure(0.0, false, 1).0.plan
        );
        assert_eq!(
            p.capacity_pressure(20.0, true, 1),
            p.capacity_pressure(0.0, false, 1)
        );
    }

    #[test]
    fn banked_capacity_counter_keeps_totals_above_u32() {
        let mut p = plan(95.0);
        p.accounts[0].banked_resets = 1 << 31;
        p.accounts[0].banked_expires_at_ms = None;
        p.accounts.push(p.accounts[0].clone());
        let (_, banked) = p.capacity_pressure(0.0, false, 1);
        assert_eq!(banked as u64, 1_u64 << 32);
    }

    /// Live on 2026-10-30 after 14:20Z: every Codex account sat at its weekly
    /// limit while two held prepaid credits, and the keep-free check refused
    /// every worker before the account allocator, which spends credits, ran.
    #[test]
    fn prepaid_credits_keep_a_full_pool_available_to_workers() {
        let pool = |credits| SubscriptionPool {
            provider: "openai".into(),
            keep_free_percent: 5.0,
            plans: vec![{
                let mut p = plan(100.0);
                p.accounts[0].banked_resets = 0;
                p.accounts[0].prepaid_credits = credits;
                p
            }],
            allow_banked: true,
            now_ms: 1,
            ..Default::default()
        };
        assert!(!pool(false).available(5.0, false));
        assert!(pool(true).available(5.0, false));
        let mut walled = pool(true);
        walled.plans[0].accounts[0].short_walled = true;
        assert!(
            !walled.available(5.0, false),
            "a short window still walls the account"
        );
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

    fn expiring_pool(short_points: f64) -> SubscriptionPool {
        let mut plan = plan(6.2);
        plan.accounts[0].weekly_remaining_fraction = 2.0 / 168.0;
        plan.accounts[0].hours_to_reset = 2.0;
        plan.accounts[0].banked_resets = 0;
        plan.accounts[0].banked_expires_at_ms = None;
        plan.accounts[0].short_windows = vec![QuotaConstraint {
            window_minutes: 300,
            remaining_points: short_points,
            remaining_fraction: 1.1 / 5.0,
        }];
        SubscriptionPool {
            provider: "kimi".into(),
            keep_free_percent: 5.0,
            plans: vec![plan],
            ..Default::default()
        }
    }

    #[test]
    fn expiring_kimi_week_sets_pressure_with_an_abundant_short_window() {
        let price = expiring_pool(100.0)
            .price(1.0, Some(1.0), 5.0, false)
            .unwrap();

        let weekly = plan_pressure(2.0 / 168.0, 0.888);
        assert!((weekly - 0.015).abs() < 0.001);
        assert!((price.plan_pressure - weekly).abs() < 1e-12);
        assert_eq!(price.weekly_pressure, weekly);
        assert_eq!(price.short_window_pressure, None);
    }

    #[test]
    fn short_window_with_less_than_one_quarter_left_raises_pressure() {
        let price = expiring_pool(20.0)
            .price(1.0, Some(1.0), 5.0, false)
            .unwrap();
        let short = price.short_window_pressure.unwrap();
        assert_eq!(short, plan_pressure(1.1 / 5.0, 0.2));
        assert_eq!(price.plan_pressure, short);
        assert!(price.plan_pressure > price.weekly_pressure);
    }

    #[test]
    fn short_window_with_exactly_one_quarter_left_does_not_raise_pressure() {
        let price = expiring_pool(25.0)
            .price(1.0, Some(1.0), 5.0, false)
            .unwrap();
        assert_eq!(price.short_window_pressure, None);
        assert_eq!(price.plan_pressure, price.weekly_pressure);
    }

    #[test]
    fn short_windows_are_pooled_before_applying_the_threshold() {
        let mut pool = expiring_pool(10.0);
        let mut second = pool.plans[0].accounts[0].clone();
        second.short_windows[0].remaining_points = 20.0;
        second.short_windows[0].remaining_fraction = 0.1;
        let mut walled = second.clone();
        walled.short_walled = true;
        walled.short_windows[0].remaining_points = 0.0;
        walled.short_windows[0].remaining_fraction = 1.0;
        pool.plans[0].accounts.push(second);
        pool.plans[0].accounts.push(walled);
        let expected = plan_pressure(1.1 / 5.0, 30.0 / 200.0);

        let price = pool.price(1.0, Some(1.0), 5.0, false).unwrap();
        assert_eq!(price.short_window_pressure, Some(expected));
        assert_eq!(price.plan_pressure, expected);

        // One tight account cannot make the window bind when the pool has room.
        pool.plans[0].accounts[1].short_windows[0].remaining_points = 60.0;
        let price = pool.price(1.0, Some(1.0), 5.0, false).unwrap();
        assert_eq!(price.short_window_pressure, None);
        assert_eq!(price.plan_pressure, price.weekly_pressure);
    }
}
