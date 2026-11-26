use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;

use super::{plan_pressure, PlanPressure, SubscriptionPlan, SHORT_WINDOW_BINDING_SHARE};

pub const HOUR_MS: i64 = 3_600_000;

/// The latest accepted observation and the pace calculated when it arrived.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowReading {
    pub name: String,
    pub window_minutes: i64,
    pub resets_at_ms: i64,
    pub used_percent: f64,
    pub pace_points_per_hour: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AccountWindows {
    pub weekly: Option<WindowReading>,
    pub short: Vec<WindowReading>,
    pub scoped: Vec<(&'static str, WindowReading)>,
}

/// Read-only sidecar, ordered like the plan and account vectors of its pool.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PreviewPool {
    pub plans: Arc<Vec<Vec<AccountWindows>>>,
    pub scope: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowPreview {
    pub window: String,
    pub window_minutes: i64,
    pub resets_at_ms: i64,
    pub pace_points_per_hour: Option<f64>,
    pub hours_to_reset: f64,
    pub projected_leftover_points: Option<f64>,
    pub free_before_reset: bool,
}

impl WindowReading {
    fn project(&self, now: i64) -> WindowPreview {
        let hours_to_reset = (self.resets_at_ms - now) as f64 / HOUR_MS as f64;
        let leftover = self
            .pace_points_per_hour
            .map(|pace| 100.0 - self.used_percent - pace * hours_to_reset);
        WindowPreview {
            window: self.name.clone(),
            window_minutes: self.window_minutes,
            resets_at_ms: self.resets_at_ms,
            pace_points_per_hour: self.pace_points_per_hour,
            hours_to_reset,
            projected_leftover_points: leftover,
            free_before_reset: hours_to_reset > 0.0
                && hours_to_reset <= 24.0
                && leftover.is_some_and(|points| points > 5.0),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct PricePreview {
    pub window: Option<String>,
    pub window_minutes: Option<i64>,
    pub resets_at_ms: Option<i64>,
    pub pace_points_per_hour: Option<f64>,
    pub hours_to_reset: Option<f64>,
    pub projected_leftover_points: Option<f64>,
    pub binding_short_windows: Vec<WindowPreview>,
    pub free_before_reset: bool,
    pub final_cost_usd: Option<f64>,
    pub pressure: Option<f64>,
    pub weekly_pressure: Option<f64>,
    pub short_window_pressure: Option<f64>,
    #[serde(skip)]
    pub free_reset_ms: Option<i64>,
}

// Admission can choose any spendable member, so the pool is free only if every
// spendable member has an expiring surplus.
#[derive(Default)]
struct WindowPool {
    count: usize,
    free_count: usize,
    earliest: Option<WindowPreview>,
    first_not_free: Option<WindowPreview>,
}

impl WindowPool {
    fn add(&mut self, reading: Option<&WindowReading>, now: i64) {
        self.count += 1;
        if let Some(window) = reading.map(|reading| reading.project(now)) {
            self.free_count += usize::from(window.free_before_reset);
            if !window.free_before_reset
                && self
                    .first_not_free
                    .as_ref()
                    .is_none_or(|p| window.resets_at_ms < p.resets_at_ms)
            {
                self.first_not_free = Some(window.clone());
            }
            if self
                .earliest
                .as_ref()
                .is_none_or(|p| window.resets_at_ms < p.resets_at_ms)
            {
                self.earliest = Some(window);
            }
        }
    }

    fn is_free(&self) -> bool {
        self.count > 0 && self.count == self.free_count
    }

    fn representative(self) -> Option<WindowPreview> {
        let free = self.is_free();
        let mut window = self.first_not_free.or(self.earliest)?;
        window.free_before_reset = free;
        Some(window)
    }
}

impl PreviewPool {
    pub(super) fn project(
        &self,
        index: usize,
        plan: &SubscriptionPlan,
        today: PlanPressure,
        margin: f64,
        now: i64,
    ) -> (PlanPressure, PricePreview) {
        let readings = self.plans.get(index);
        let mut weekly = WindowPool::default();
        let mut short = BTreeMap::<i64, WindowPool>::new();
        for (i, account) in plan.accounts.iter().enumerate() {
            if account.short_walled || account.weekly_used >= 100.0 - margin {
                continue;
            }
            let account_readings = readings.and_then(|accounts| accounts.get(i));
            weekly.add(account_readings.and_then(|a| a.weekly.as_ref()), now);
            // The scope overlay appends its active windows after the general
            // windows. Keep the producer identities in that same order.
            let mut samples = account_readings.into_iter().flat_map(|a| {
                a.short.iter().chain(
                    a.scoped.iter().filter_map(|(scope, window)| {
                        (Some(*scope) == self.scope).then_some(window)
                    }),
                )
            });
            for window in &account.short_windows {
                let reading = samples
                    .next()
                    .filter(|r| r.window_minutes == window.window_minutes);
                short
                    .entry(window.window_minutes)
                    .or_default()
                    .add(reading, now);
            }
        }
        let mut window = weekly.representative();
        let weekly_free = window.as_ref().is_some_and(|w| w.free_before_reset);
        let mut binding_short_windows = Vec::new();
        let mut short_pressure: Option<f64> = None;
        for (minutes, (points, time, count)) in plan.short_pools() {
            let remaining = points / (count as f64 * 100.0);
            if remaining >= SHORT_WINDOW_BINDING_SHARE {
                continue;
            }
            let sample = short.remove(&minutes).and_then(WindowPool::representative);
            let pressure = if sample.as_ref().is_some_and(|w| w.free_before_reset) {
                0.0
            } else {
                plan_pressure(time, remaining)
            };
            short_pressure = Some(short_pressure.map_or(pressure, |other| other.max(pressure)));
            binding_short_windows.extend(sample);
        }
        let pressure =
            PlanPressure::new(if weekly_free { 0.0 } else { today.weekly }, short_pressure);
        let free_reset_ms = if pressure.plan == 0.0 {
            if let Some(earliest) = binding_short_windows.iter().min_by_key(|w| w.resets_at_ms) {
                if window
                    .as_ref()
                    .is_none_or(|w| earliest.resets_at_ms < w.resets_at_ms)
                {
                    window = Some(earliest.clone());
                }
            }
            window.as_ref().map(|w| w.resets_at_ms)
        } else {
            None
        };
        (
            pressure,
            PricePreview {
                free_before_reset: window.as_ref().is_some_and(|w| w.free_before_reset),
                window_minutes: window.as_ref().map(|w| w.window_minutes),
                resets_at_ms: window.as_ref().map(|w| w.resets_at_ms),
                pace_points_per_hour: window.as_ref().and_then(|w| w.pace_points_per_hour),
                hours_to_reset: window.as_ref().map(|w| w.hours_to_reset),
                projected_leftover_points: window
                    .as_ref()
                    .and_then(|w| w.projected_leftover_points),
                window: window.map(|w| w.window),
                binding_short_windows,
                free_reset_ms,
                ..Default::default()
            },
        )
    }
}
