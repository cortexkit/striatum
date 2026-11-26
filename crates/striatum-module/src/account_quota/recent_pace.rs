use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use serde_json::Value;
use striatum_core::subscription::free_before_reset::{WindowReading, HOUR_MS};

use super::{parse_rfc3339_ms, AccountReadings, MergeOutcome, StoredReading};

const HISTORY_MS: i64 = 6 * HOUR_MS;

#[derive(Debug, Clone, Default)]
pub(crate) struct RecentPace(Arc<BTreeMap<(String, String), AccountPace>>);

type AccountPace = BTreeMap<(String, i64), Series>;

#[derive(Debug, Clone, Default)]
struct Series {
    reset_ms: i64,
    samples: VecDeque<(i64, f64)>,
    points_per_hour: Option<f64>,
}

impl Series {
    fn observe(&mut self, at: i64, reset_ms: i64, used: f64) {
        if let Some(&(previous_at, previous_used)) = self.samples.back() {
            if at < previous_at
                || (at == previous_at && reset_ms == self.reset_ms && used == previous_used)
            {
                return;
            }
            if reset_ms != self.reset_ms || used < previous_used || at == previous_at {
                self.samples.clear();
            }
        }
        self.reset_ms = reset_ms;
        self.samples.push_back((at, used));
        while self
            .samples
            .front()
            .is_some_and(|&(time, _)| time < at - HISTORY_MS)
        {
            self.samples.pop_front();
        }
        self.points_per_hour = self.samples.front().and_then(|&(old_at, old_used)| {
            let elapsed = at - old_at;
            (elapsed >= HOUR_MS).then(|| (used - old_used) / (elapsed as f64 / HOUR_MS as f64))
        });
    }
}

impl RecentPace {
    pub(super) fn update(&mut self, readings: &AccountReadings, outcome: &MergeOutcome, now: i64) {
        if outcome.updated.is_empty() && outcome.tombstoned.is_empty() {
            return;
        }
        let accounts = Arc::make_mut(&mut self.0);
        accounts.retain(|_, windows| {
            windows.retain(|_, window| {
                window
                    .samples
                    .back()
                    .is_some_and(|&(at, _)| at >= now - HISTORY_MS)
            });
            !windows.is_empty()
        });
        for key in &outcome.tombstoned {
            accounts.remove(key);
        }
        for key in &outcome.updated {
            let entry = &readings.readings[key].entry;
            // A retained last-good payload is not a new observation. Use the
            // producer's timestamp so polling a cached reply cannot grow pace.
            if !entry["stale"].is_null() || !entry["error"].is_null() {
                continue;
            }
            let Some(at) = entry["fetchedAt"]
                .as_str()
                .and_then(parse_rfc3339_ms)
                .filter(|&at| (now - HISTORY_MS..=now).contains(&at))
            else {
                continue;
            };
            let usage = &entry["usage"];
            let general =
                ["primary", "secondary", "tertiary"].map(|name| (name.to_owned(), &usage[name]));
            let scoped = usage["extraRateWindows"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|extra| {
                    Some((scoped_key(extra["id"].as_str()?), extra.get("window")?))
                });
            let windows = accounts.entry(key.clone()).or_default();
            for (name, raw) in general.into_iter().chain(scoped) {
                if let Some((minutes, reset_ms, used)) = window_fields(raw) {
                    if reset_ms > at {
                        windows
                            .entry((name, minutes))
                            .or_default()
                            .observe(at, reset_ms, used);
                    }
                }
            }
        }
    }

    pub(crate) fn reading(
        &self,
        account: &StoredReading,
        name: &str,
        raw: &Value,
    ) -> Option<WindowReading> {
        let (window_minutes, resets_at_ms, used_percent) = window_fields(raw)?;
        let fetched_at = account.fetched_at().and_then(parse_rfc3339_ms);
        let pace_points_per_hour =
            self.0
                .get(&(account.provider.clone(), account.account_id.clone()))
                .and_then(|windows| windows.get(&(name.to_owned(), window_minutes)))
                .filter(|series| {
                    series.reset_ms == resets_at_ms
                        && series.samples.back().is_some_and(|&(at, used)| {
                            Some(at) == fetched_at && used == used_percent
                        })
                })
                .and_then(|series| series.points_per_hour);
        Some(WindowReading {
            name: name.to_owned(),
            window_minutes,
            resets_at_ms,
            used_percent,
            pace_points_per_hour,
        })
    }
}

pub(crate) fn scoped_key(id: &str) -> String {
    format!("scope:{id}")
}

fn window_fields(raw: &Value) -> Option<(i64, i64, f64)> {
    let minutes = raw["windowMinutes"].as_i64().filter(|m| *m > 0)?;
    let reset_ms = parse_rfc3339_ms(raw["resetsAt"].as_str()?)?;
    let used = raw["rawUsedPercent"]
        .as_f64()
        .or_else(|| raw["usedPercent"].as_f64())?;
    Some((minutes, reset_ms, used.clamp(0.0, 100.0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pace_uses_oldest_reading_inside_six_hours_and_restarts_on_reset_or_correction() {
        let mut series = Series::default();
        let reset = 12 * HOUR_MS;
        series.observe(0, reset, 1.0);
        series.observe(HOUR_MS, reset, 20.0);
        series.observe(7 * HOUR_MS, reset, 32.0);
        assert_eq!(series.points_per_hour, Some(2.0));
        assert_eq!(series.samples.len(), 2);
        series.observe(7 * HOUR_MS, reset, 32.0);
        assert_eq!(series.samples.len(), 2);
        series.observe(8 * HOUR_MS, reset, 24.0);
        assert_eq!(series.points_per_hour, None);
        series.observe(9 * HOUR_MS, reset, 26.0);
        assert_eq!(series.points_per_hour, Some(2.0));
        series.observe(10 * HOUR_MS, reset + 24 * HOUR_MS, 30.0);
        assert_eq!(series.points_per_hour, None);
    }
}
