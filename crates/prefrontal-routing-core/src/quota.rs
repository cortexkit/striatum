//! Quota usage wire types: the decode contract for `insula` (formerly `ai-provider-quota`)'s
//! `usage.get` response, mirrored from `quota-core/src/model.rs`.
//!
//! These are DESERIALIZE-side mirrors: prefrontal-routing consumes quota windows
//! module-to-module, so it must decode the exact `ProviderUsage[]` shape the
//! quota module emits (camelCase keys, silent-degrade entries carrying `error`).
//! Kept in -core (not the module binary) because mapping windows into a routing
//! runtime snapshot is pure logic, testable without the wire.

use serde::Deserialize;

/// One rate-limit window: how much of a quota pool is spent and when it resets.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateWindow {
    /// 0..100 percent of the window's quota consumed.
    pub used_percent: f64,
    /// ISO 8601 / RFC 3339 timestamp when the window resets. Optional: QTA's
    /// `docs/consumer-contract.md` says the producer never fabricates a recovery
    /// time, so `None` means recovery was not reported, not that the window is idle.
    #[serde(default)]
    pub resets_at: Option<String>,
    /// Window length in minutes. Omitted when the provider does not report one.
    #[serde(default)]
    pub window_minutes: Option<i64>,
}

/// A per-model window bundled under one account (e.g. Antigravity's Geminis).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtraWindow {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub window: Option<RateWindow>,
}

/// The window topology for one account: up to three account-wide pools plus an
/// optional list of per-model pools.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    #[serde(default)]
    pub primary: Option<RateWindow>,
    #[serde(default)]
    pub secondary: Option<RateWindow>,
    #[serde(default)]
    pub tertiary: Option<RateWindow>,
    #[serde(default)]
    pub extra_rate_windows: Option<Vec<ExtraWindow>>,
}

/// One provider/account's usage entry. The `usage.get` result is an array of
/// these. A fetch failure becomes an entry carrying `error` (silent-degrade),
/// never a failure of the whole array: the consumer skips any entry with a
/// truthy `error`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsage {
    /// The quota module's provider name (e.g. "codex"); routing maps it to its own id.
    pub provider: String,
    /// The canonical models.dev slug, published by the quota module precisely so
    /// consumers do not hand-maintain a copy of its local-name table. Null for
    /// roughly a third of entries, so `provider` + the local map stays as the
    /// fallback. Keying on this first is load-bearing: QTA's namespace has
    /// near-collisions (provider="alibaba" is the CODING plan, degraded on this
    /// host, while provider="qwen-cloud" is the token plan we route), so a local
    /// map that guesses lands on a real-but-dead entry and reads a confident
    /// "no signal" for a provider that is serving.
    #[serde(default)]
    pub api_provider: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
    /// Which retrieval path produced this (e.g. "oauth"): observability only.
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub usage: Option<Usage>,
    /// Present only on a degraded entry. The consumer skips any entry with `error`.
    #[serde(default)]
    pub error: Option<String>,
    /// Stable machine class beside `error` on degraded entries (QTA, additive):
    /// credential_absent, credential_rejected, upstream_failed, decode_failed,
    /// no_quota_reported. Deliberately a String, not an enum: the producer's list
    /// grows, and an unknown value must stay a readable degraded entry, never a
    /// parse failure or a fold into a known bucket. Branch on `error` for health;
    /// this field only says WHY.
    #[serde(default)]
    pub error_class: Option<String>,
}

impl Usage {
    fn has_usable_windows(&self) -> bool {
        self.primary.is_some()
            || self.secondary.is_some()
            || self.tertiary.is_some()
            || self
                .extra_rate_windows
                .as_ref()
                .is_some_and(|windows| windows.iter().any(|window| window.window.is_some()))
    }
}

impl ProviderUsage {
    /// True when this entry carries real quota windows (not a silent-degrade error
    /// or an otherwise empty usage payload).
    pub fn is_healthy(&self) -> bool {
        self.error.is_none() && self.usage.as_ref().is_some_and(Usage::has_usable_windows)
    }
}

use std::collections::HashMap;

use crate::model_facts::{make_capacity_key, ModelFact};
use crate::model_router::ModelRouteStateSnapshot;
use crate::quota_extract::{extract_windows, WindowScopeKind};
use crate::quota_pace::{project_quota_pressure, PaceConfig, PaceInput, DEFAULT_PACE_CONFIG};

/// Hard-block threshold (utilization at/above which a window blocks routes until
/// reset). Matches the plugin's `DEFAULT_HARD_BLOCK_THRESHOLD`.
pub const DEFAULT_HARD_BLOCK_THRESHOLD: f64 = 0.99;

/// Local retry-probe horizon for exhausted windows whose provider reports no reset.
/// This is OUR pacing policy for when to re-ask, not a provider reset claim: QTA's
/// no-fabrication rule still holds on the wire.
pub const NO_RESET_REPROBE_HORIZON_MS: f64 = 6.0 * 60.0 * 60.0 * 1000.0;

/// The per-capacity quota signal the overlay derives: worst soft pressure + the
/// latest hard-exhaustion reset across all windows matching that capacity.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct CapacitySignal {
    pressure: Option<f64>,
    exhausted_until: Option<f64>,
}

/// Merge quota windows onto an EXISTING (store-built) route-state snapshot, with
/// FULL plugin parity (replacing the flat worst-utilization proxy):
///   - the wire name is mapped to a routing providerID via `name_map`;
///   - each provider's windows are EXTRACTED per its topology (account-wide pools
///     vs antigravity's per-model `extra_rate_windows` → capacity scope);
///   - past-reset windows are skipped when a reset is present; each live window's
///     soft pressure is the pace PROJECTION (lockout-cost), not raw utilization;
///   - a window with `limit_reached` or utilization >= the hard-block threshold
///     sets `quota_exhausted_until` to its reset, or to a local re-probe horizon
///     when no reset was reported, on its matching capacity (a hard block until
///     that local retry point), worst/latest-wins across windows.
/// Fresh soft pressure REPLACES any older stored pressure on `by_capacity`, so a
/// recovered provider can become cheaper again as soon as a new window is fetched.
/// Hard exhaustion still keeps the WORST/LATEST reset, because a confirmed future
/// lockout must stay in force until that reset passes. Route-state cooldown signals
/// on the snapshot are untouched.
///
/// `now_ms` is both the reset-cutoff and the pace `observed_at` (the windows are
/// recomputed live each call against an absolute `resets_at`, so elapsed grows
/// correctly with wall-clock: there is no cache-age decay to compensate for here;
/// stale-window RETENTION across an outage is the consumer cache's job, not this
/// pure overlay's).
pub fn merge_quota_overlay(
    snapshot: &mut ModelRouteStateSnapshot,
    windows: &[ProviderUsage],
    facts: &[ModelFact],
    name_map: &HashMap<String, String>,
) {
    merge_quota_overlay_with(
        snapshot,
        windows,
        facts,
        name_map,
        now_default(snapshot),
        &DEFAULT_PACE_CONFIG,
        DEFAULT_HARD_BLOCK_THRESHOLD,
    );
}

fn now_default(snapshot: &ModelRouteStateSnapshot) -> f64 {
    // The snapshot carries the `now` the store built it with; reuse it so the
    // overlay's reset-cutoff/pace anchor matches the rest of the selection.
    snapshot.now
}

/// The explicit-config form (testable with a fixed `now`, pace config, threshold).
pub fn merge_quota_overlay_with(
    snapshot: &mut ModelRouteStateSnapshot,
    windows: &[ProviderUsage],
    facts: &[ModelFact],
    name_map: &HashMap<String, String>,
    now_ms: f64,
    pace_config: &PaceConfig,
    hard_block_threshold: f64,
) {
    // provider_id -> its capacity keys present in the catalog (for window scoping +
    // the antigravity per-model title match).
    let mut caps_by_provider: HashMap<String, Vec<String>> = HashMap::new();
    for fact in facts {
        caps_by_provider
            .entry(fact.provider_id.clone())
            .or_default()
            .push(make_capacity_key(&fact.provider_id, &fact.model_id));
    }

    // (account, capacity_key) -> signal. Accounts accumulate SEPARATELY and
    // fold at the end: QTA's multi-account contract makes the entry atomic per
    // (provider, account), serving pressure selects the BEST account, and a
    // wall is per-account. Merging worst-wins across accounts inverted that:
    // one exhausted account (of anthropic's four) walled the whole provider
    // while its siblings sat at 5% (live 2026-08-10: opus-5 rejected
    // quota_exhausted off one 99% weekly account beside three nearly-free).
    let mut signals: HashMap<(String, String), HashMap<String, CapacitySignal>> = HashMap::new();

    for entry in windows {
        if !entry.is_healthy() {
            continue;
        }
        // The published canonical slug wins over our local wire-name map: QTA
        // serves apiProvider exactly so consumers stop maintaining copies of its
        // naming table, and the fallback map exists only for the ~third of
        // entries where the slug is null. Order matters: the local map knows
        // nothing about providers QTA added after it was written (alibaba's
        // token plan reached us as provider="qwen-cloud", absent from the map,
        // so its healthy windows were dropped at this line for weeks while the
        // provider ran at 72% of its 5h window).
        let provider_id = match entry.api_provider.as_ref() {
            Some(slug) if !slug.is_empty() => slug,
            _ => match name_map.get(&entry.provider) {
                Some(mapped) => mapped,
                None => continue, // unmapped and no published slug → no signal (degrade safe)
            },
        };
        let empty: Vec<String> = Vec::new();
        let provider_caps = caps_by_provider.get(provider_id).unwrap_or(&empty);
        let extracted = extract_windows(entry, provider_id, &entry.provider, provider_caps);

        for win in extracted {
            if win.resets_at.is_some_and(|resets_at| resets_at <= now_ms) {
                continue; // an explicitly past reset is no longer in force
            }
            let pace = project_quota_pressure(
                PaceInput {
                    utilization: win.utilization,
                    resets_at: win.resets_at,
                    window_ms: win.window_ms,
                    observed_at: now_ms,
                },
                pace_config,
            );
            let blocked = win.limit_reached || win.utilization >= hard_block_threshold;

            // The capacities this window applies to.
            let targets: Vec<String> = match win.scope_kind {
                WindowScopeKind::Account => provider_caps.clone(),
                WindowScopeKind::CapacityKey => win.applies_key.clone().into_iter().collect(),
                // ModelFamily/RouteKey are not emitted by the live extractors.
                WindowScopeKind::ModelFamily | WindowScopeKind::RouteKey => {
                    win.applies_key.clone().into_iter().collect()
                }
            };
            let account_key = (
                provider_id.clone(),
                entry.account.clone().unwrap_or_default(),
            );
            for cap in targets {
                let sig = signals
                    .entry(account_key.clone())
                    .or_default()
                    .entry(cap)
                    .or_default();
                // WITHIN one account, worst window wins (an account is as
                // constrained as its tightest live window).
                sig.pressure = Some(match sig.pressure {
                    Some(p) => p.max(pace.pressure),
                    None => pace.pressure,
                });
                if blocked {
                    let exhausted_until = win
                        .resets_at
                        .unwrap_or(now_ms + NO_RESET_REPROBE_HORIZON_MS);
                    sig.exhausted_until = Some(match sig.exhausted_until {
                        Some(e) => e.max(exhausted_until),
                        None => exhausted_until,
                    });
                }
            }
        }
    }

    // Fold ACROSS accounts per capacity: best account serves. Pressure takes the
    // MINIMUM (the account the send will actually use), and a capacity is
    // exhausted only when EVERY account holding a signal for it is exhausted,
    // recovering at the EARLIEST reset (the first account to free unblocks the
    // provider). One account's wall must never silence its siblings.
    let mut folded: HashMap<String, (Option<f64>, Option<f64>, bool)> = HashMap::new();
    for ((_, _account), caps) in &signals {
        for (cap, sig) in caps {
            let entry = folded.entry(cap.clone()).or_insert((None, None, true));
            if let Some(p) = sig.pressure {
                entry.0 = Some(match entry.0 {
                    Some(existing) => existing.min(p),
                    None => p,
                });
            }
            match sig.exhausted_until {
                Some(e) => {
                    entry.1 = Some(match entry.1 {
                        Some(existing) => existing.min(e),
                        None => e,
                    });
                }
                None => entry.2 = false, // an unwalled account exists
            }
        }
    }

    // Merge onto the snapshot. Fresh pressure replaces stored pressure; a hard
    // wall lands only when all accounts walled, and keeps the worst/latest
    // horizon against any existing stored state.
    for (cap, (pressure, earliest_wall, all_walled)) in folded {
        let state = snapshot.by_capacity.entry(cap).or_default();
        if let Some(p) = pressure {
            state.quota_pressure = Some(p);
        }
        if all_walled {
            if let Some(e) = earliest_wall {
                state.quota_exhausted_until = Some(match state.quota_exhausted_until {
                    Some(existing) => existing.max(e),
                    None => e,
                });
            }
        }
    }
}
