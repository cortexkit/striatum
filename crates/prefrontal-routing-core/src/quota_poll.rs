use std::collections::{BTreeMap, BTreeSet};

use crate::quota::ProviderUsage;
use crate::quota_extract::{extract_windows, fetched_at_ms};
use crate::route_state_store::{
    QuotaReadingLogRow, QuotaStaleDisclosure, RouteStateResult, RouteStateStore, UsageObservation,
};

pub const READING_LOG_RETENTION_DAYS: i64 = 14;
pub const READING_INTERVAL_MIN_ELAPSED_MS: i64 = 10 * 60_000;
pub const READING_INTERVAL_MIN_ACCOUNT_TOKENS: f64 = 10_000.0;
pub const READING_RATE_MIN_INTERVALS: usize = 20;

const DAY_MS: i64 = 24 * 60 * 60_000;

/// The sole account-key rendering shared by quota readings and usage attribution.
pub fn render_account_key(provider_id: &str, account: Option<&str>) -> String {
    format!("{provider_id}/{}", account.unwrap_or_default())
}

/// Normalize one provider entry observed by the local poll into append-only rows.
/// `observed_at` is never derived from producer data; `fetched_at` is never replaced
/// by the poll clock when the producer omitted it.
pub fn reading_log_rows_for_poll(
    entry: &ProviderUsage,
    provider_id: &str,
    capacity_keys: &[String],
    observed_at: i64,
) -> Vec<QuotaReadingLogRow> {
    if !entry.is_healthy() {
        return Vec::new();
    }

    let account_key = render_account_key(provider_id, entry.account.as_deref());
    let fetched_at = fetched_at_ms(entry).and_then(|value| i64::try_from(value).ok());
    let banked_credit_count = entry
        .saved_resets
        .as_ref()
        .and_then(|saved| saved.available_count);
    let stale = entry.stale.as_ref().map(|disclosure| QuotaStaleDisclosure {
        since: disclosure.since.clone(),
        class: disclosure.class.clone(),
    });

    extract_windows(entry, provider_id, &entry.provider, capacity_keys)
        .into_iter()
        .map(|window| QuotaReadingLogRow {
            account_key: account_key.clone(),
            window_key: window.window_key,
            observed_at,
            used_percent: window.utilization * 100.0,
            raw_used_percent: window.raw_used_percent,
            resets_at: finite_i64(window.resets_at),
            window_minutes: window
                .window_ms
                .and_then(|millis| finite_i64(Some(millis / 60_000.0))),
            banked_credit_count,
            fetched_at,
            stale: stale.clone(),
        })
        .collect()
}

/// Persist one local poll and apply age retention at the same poll-clock boundary.
pub fn record_reading_log_poll(
    store: &(impl RouteStateStore + ?Sized),
    entry: &ProviderUsage,
    provider_id: &str,
    capacity_keys: &[String],
    observed_at: i64,
) -> RouteStateResult<usize> {
    store.prune_quota_readings(observed_at)?;
    store.append_quota_readings(&reading_log_rows_for_poll(
        entry,
        provider_id,
        capacity_keys,
        observed_at,
    ))
}

fn finite_i64(value: Option<f64>) -> Option<i64> {
    value
        .filter(|value| value.is_finite())
        .map(|value| value as i64)
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaReadingInterval {
    pub account_key: String,
    pub window_key: String,
    pub from_observed_at: i64,
    pub to_observed_at: i64,
    pub from_fetched_at: i64,
    pub to_fetched_at: i64,
    pub elapsed_ms: i64,
    pub raw_used_percent_delta: f64,
}

/// Admit anchored source-time intervals independently for every account/window.
/// Missing verification fields clear the anchor. Pair-level discontinuities make
/// the current clean row the new anchor, so no later interval spans the break.
pub fn admit_reading_intervals(rows: &[QuotaReadingLogRow]) -> Vec<QuotaReadingInterval> {
    let mut groups: BTreeMap<(&str, &str), Vec<&QuotaReadingLogRow>> = BTreeMap::new();
    for row in rows {
        groups
            .entry((row.account_key.as_str(), row.window_key.as_str()))
            .or_default()
            .push(row);
    }

    let mut intervals = Vec::new();
    for ((account_key, window_key), mut readings) in groups {
        readings.sort_by_key(|row| row.observed_at);
        let mut anchor: Option<&QuotaReadingLogRow> = None;
        for reading in readings {
            let Some((fetched_at, raw_used_percent, banked_credit_count)) = clean_parts(reading)
            else {
                anchor = None;
                continue;
            };
            let Some(previous) = anchor else {
                anchor = Some(reading);
                continue;
            };
            let Some((previous_fetched_at, previous_raw_used_percent, previous_banked_count)) =
                clean_parts(previous)
            else {
                anchor = Some(reading);
                continue;
            };

            if fetched_at == previous_fetched_at {
                continue;
            }
            if fetched_at < previous_fetched_at
                || raw_used_percent < previous_raw_used_percent
                || banked_credit_count != previous_banked_count
                || reading.resets_at != previous.resets_at
                || reading.window_minutes != previous.window_minutes
            {
                anchor = Some(reading);
                continue;
            }

            let elapsed_ms = fetched_at - previous_fetched_at;
            if elapsed_ms < READING_INTERVAL_MIN_ELAPSED_MS {
                continue;
            }

            intervals.push(QuotaReadingInterval {
                account_key: account_key.to_owned(),
                window_key: window_key.to_owned(),
                from_observed_at: previous.observed_at,
                to_observed_at: reading.observed_at,
                from_fetched_at: previous_fetched_at,
                to_fetched_at: fetched_at,
                elapsed_ms,
                raw_used_percent_delta: raw_used_percent - previous_raw_used_percent,
            });
            anchor = Some(reading);
        }
    }
    intervals.sort_by(|left, right| {
        (
            left.account_key.as_str(),
            left.window_key.as_str(),
            left.from_observed_at,
        )
            .cmp(&(
                right.account_key.as_str(),
                right.window_key.as_str(),
                right.from_observed_at,
            ))
    });
    intervals
}

fn clean_parts(row: &QuotaReadingLogRow) -> Option<(i64, f64, i64)> {
    let fetched_at = row.fetched_at?;
    let raw_used_percent = row.raw_used_percent?;
    if !raw_used_percent.is_finite() {
        return None;
    }
    let banked_credit_count = row.banked_credit_count?;
    (fetched_at >= 0 && banked_credit_count >= 0).then_some((
        fetched_at,
        raw_used_percent,
        banked_credit_count,
    ))
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaUsageInterval {
    pub reading: QuotaReadingInterval,
    pub tokens: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaReadingAnalysis {
    pub time_admitted_intervals: Vec<QuotaReadingInterval>,
    pub usage_admitted_intervals: Vec<QuotaUsageInterval>,
    /// `None` means usage attribution is not a producer and was not queried.
    pub token_population: Option<usize>,
    pub token_floor_applied: bool,
    /// Raw used-percent movement per token, fitted through the origin.
    pub fitted_raw_percent_per_token: Option<f64>,
}

/// Join time-admitted intervals to every usage consumer of the burning account.
/// The account-producer gate precedes both token population and token-floor logic.
pub fn analyze_reading_log(
    readings: &[QuotaReadingLogRow],
    usage: &[UsageObservation],
    account_producer: bool,
) -> QuotaReadingAnalysis {
    let time_admitted_intervals = admit_reading_intervals(readings);
    if !account_producer {
        return QuotaReadingAnalysis {
            time_admitted_intervals,
            usage_admitted_intervals: Vec::new(),
            token_population: None,
            token_floor_applied: false,
            fitted_raw_percent_per_token: None,
        };
    }

    let burning_accounts = time_admitted_intervals
        .iter()
        .map(|interval| interval.account_key.as_str())
        .collect::<BTreeSet<_>>();
    let token_population = usage
        .iter()
        .filter(|sample| {
            sample
                .account_key
                .as_deref()
                .is_some_and(|account| burning_accounts.contains(account))
                && total_tokens(sample).is_some_and(|tokens| tokens.is_finite() && tokens > 0.0)
        })
        .count();
    let mut usage_admitted_intervals = Vec::new();
    for interval in &time_admitted_intervals {
        let tokens = usage
            .iter()
            .filter(|sample| sample.account_key.as_deref() == Some(interval.account_key.as_str()))
            .filter(|sample| {
                sample.observed_at > interval.from_observed_at as f64
                    && sample.observed_at <= interval.to_observed_at as f64
            })
            .filter_map(total_tokens)
            .sum::<f64>();
        if tokens >= READING_INTERVAL_MIN_ACCOUNT_TOKENS && interval.raw_used_percent_delta > 0.0 {
            usage_admitted_intervals.push(QuotaUsageInterval {
                reading: interval.clone(),
                tokens,
            });
        }
    }

    let fitted_raw_percent_per_token =
        (usage_admitted_intervals.len() >= READING_RATE_MIN_INTERVALS).then(|| {
            let numerator = usage_admitted_intervals
                .iter()
                .map(|sample| sample.tokens * sample.reading.raw_used_percent_delta)
                .sum::<f64>();
            let denominator = usage_admitted_intervals
                .iter()
                .map(|sample| sample.tokens * sample.tokens)
                .sum::<f64>();
            numerator / denominator
        });

    QuotaReadingAnalysis {
        time_admitted_intervals,
        usage_admitted_intervals,
        token_population: Some(token_population),
        token_floor_applied: true,
        fitted_raw_percent_per_token: fitted_raw_percent_per_token
            .filter(|rate| rate.is_finite() && *rate > 0.0),
    }
}

fn total_tokens(sample: &UsageObservation) -> Option<f64> {
    // `output_tokens` already stores output plus reasoning. Requiring the separate
    // reasoning field keeps this a complete attribution without double-counting it.
    sample.reasoning_tokens?;
    Some(
        sample.input_tokens?
            + sample.output_tokens
            + sample.cached_read_tokens?
            + sample.cache_write_tokens?,
    )
}

pub fn reading_log_retention_cutoff(now_ms: i64) -> i64 {
    now_ms.saturating_sub(READING_LOG_RETENTION_DAYS.saturating_mul(DAY_MS))
}
