use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use serde_json::Value;
use striatum_core::subscription::free_before_reset::{WindowReading, HOUR_MS};

use super::{parse_rfc3339_ms, AccountReadings, MergeOutcome, StoredReading};

const HISTORY_MS: i64 = 6 * HOUR_MS;
const RESET_JITTER_MS: u64 = 120_000;

fn same_reset(previous: i64, current: i64) -> bool {
    previous.abs_diff(current) <= RESET_JITTER_MS
}

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
            if at < previous_at {
                return;
            }
            let same_window = same_reset(self.reset_ms, reset_ms);
            if at == previous_at && same_window && used == previous_used {
                // Updating a jittered reset estimate is not a new usage sample.
                self.reset_ms = reset_ms;
                return;
            }
            if !same_window || used < previous_used || at == previous_at {
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
                    same_reset(series.reset_ms, resets_at_ms)
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
    use crate::account_quota::{apply_provider_reply, AccountQuotaState};
    use crate::account_selection::rfc3339;
    use serde_json::json;

    const POLL_NOW: i64 = 1_900_000_000_000;

    fn window(reset: &str, used: f64) -> Value {
        json!({"usedPercent":used,"windowMinutes":10080,"resetsAt":reset})
    }

    fn poll(state: &mut AccountQuotaState, at: i64, primary: Value) {
        let body = json!({"result":[{
            "provider":"claude", "apiProvider":"anthropic", "account":"work",
            "source":"vault", "fetchedAt":rfc3339(at), "usage":{"primary":primary}
        }], "completeProviders":["claude"]});
        apply_provider_reply(state, "claude", &serde_json::to_vec(&body).unwrap(), at).unwrap();
    }

    #[test]
    fn provider_poll_builds_pace_with_subsecond_reset_jitter() {
        let mut state = AccountQuotaState::default();
        let reset = rfc3339(POLL_NOW + HOUR_MS);
        let reset = reset.strip_suffix('Z').unwrap();
        poll(
            &mut state,
            POLL_NOW - 5 * HOUR_MS / 4,
            window(&format!("{reset}.729890+00:00"), 20.0),
        );
        for (at, offset, used) in [
            (POLL_NOW, 0, 25.0),
            (POLL_NOW, 120_000, 25.0),
            (POLL_NOW + HOUR_MS / 4, 240_000, 26.0),
        ] {
            let reset_ms = POLL_NOW + HOUR_MS + offset;
            let reset = rfc3339(reset_ms);
            let reset = reset.strip_suffix('Z').unwrap();
            poll(
                &mut state,
                at,
                window(&format!("{reset}.117982+00:00"), used),
            );
            let account = &state.readings.readings[&("claude".into(), "work".into())];
            let reading = state
                .recent_pace
                .reading(account, "primary", &account.entry["usage"]["primary"])
                .unwrap();
            assert_eq!(reading.resets_at_ms, reset_ms + 117);
            assert_eq!(
                reading.pace_points_per_hour,
                Some(4.0),
                "offset {offset} ms"
            );
        }
    }

    #[test]
    fn reading_matches_pace_only_within_two_minutes_of_the_tracked_reset() {
        let mut state = AccountQuotaState::default();
        let reset = POLL_NOW + HOUR_MS;
        poll(
            &mut state,
            POLL_NOW - HOUR_MS,
            window(&rfc3339(reset), 20.0),
        );
        poll(&mut state, POLL_NOW, window(&rfc3339(reset), 25.0));
        let account = &state.readings.readings[&("claude".into(), "work".into())];
        for (offset, pace) in [(-120_000, Some(5.0)), (120_000, Some(5.0)), (121_000, None)] {
            let reading = state
                .recent_pace
                .reading(account, "primary", &window(&rfc3339(reset + offset), 25.0))
                .unwrap();
            assert_eq!(reading.resets_at_ms, reset + offset);
            assert_eq!(reading.pace_points_per_hour, pace, "offset {offset} ms");
        }
    }

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
