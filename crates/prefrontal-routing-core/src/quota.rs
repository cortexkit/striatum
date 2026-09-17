//! Quota usage wire types: the decode contract for `insula` (formerly `ai-provider-quota`)'s
//! `usage.get` response, mirrored from `quota-core/src/model.rs`.
//!
//! These are DESERIALIZE-side mirrors: prefrontal-routing consumes quota windows
//! module-to-module, so it must decode the exact `ProviderUsage[]` shape the
//! quota module emits (camelCase keys, silent-degrade entries carrying `error`).
//! Kept in -core (not the module binary) because mapping windows into a routing
//! runtime snapshot is pure logic, testable without the wire.

use serde::Deserialize;

use crate::quota_pace::ProviderWindowModel;

/// One rate-limit window: how much of a quota pool is spent and when it resets.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateWindow {
    /// Effective 0..100 percent consumed after the producer applies saved resets.
    pub used_percent: f64,
    /// Provider-reported utilization before saved resets are applied. Consumption
    /// intervals require this source value; effective utilization is not a substitute.
    #[serde(default)]
    pub raw_used_percent: Option<f64>,
    /// ISO 8601 / RFC 3339 timestamp when the window resets. Optional: QTA's
    /// `docs/consumer-contract.md` says the producer never fabricates a recovery
    /// time, so `None` means recovery was not reported, not that the window is idle.
    #[serde(default)]
    pub resets_at: Option<String>,
    /// Window length in minutes. Omitted when the provider does not report one.
    #[serde(default)]
    pub window_minutes: Option<i64>,
    /// Explicit provider semantics. A rolling value is rejected by pace projection
    /// until routing has a model that can account for a moving recovery horizon.
    #[serde(default, alias = "windowType")]
    pub window_model: Option<ProviderWindowModel>,
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

/// A fixed-precision balance used by a spend pool. `minor` carries the signed
/// amount in `10^-exponent` units; the sign alone is enough for exhaustion.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpendBalance {
    pub minor: i64,
    pub exponent: i32,
    pub unit: String,
}

impl SpendBalance {
    pub fn is_non_positive(&self) -> bool {
        self.minor <= 0
    }

    /// Human-readable amount for log lines, e.g. "24.01 CNY". Diagnostic only:
    /// policy math stays on the fixed-precision minor units.
    pub fn render(&self) -> String {
        let amount = self.minor as f64 * 10_f64.powi(-self.exponent);
        format!("{amount:.2} {}", self.unit)
    }

    fn ratio_of(&self, total: &Self) -> Option<f64> {
        if self.unit != total.unit || total.minor <= 0 {
            return None;
        }
        let remaining = self.minor as f64 * 10_f64.powi(-self.exponent);
        let total = total.minor as f64 * 10_f64.powi(-total.exponent);
        (total.is_finite() && total > 0.0).then_some((remaining / total).clamp(0.0, 1.0))
    }
}

/// One provider-scoped spend pool. `funding` and `basis` remain Strings because
/// insula extends both vocabularies additively; unsupported funding is deliberately
/// retained and treated as not granted rather than rejecting the whole entry.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpendPool {
    pub id: String,
    pub label: String,
    pub funding: String,
    pub remaining: SpendBalance,
    #[serde(default)]
    pub total: Option<SpendBalance>,
    pub basis: String,
    /// Producer contract (cortexkit-provider-usage 0.6, skip_serializing_if):
    /// ABSENT means the provider does not say - only a literal `false` is a
    /// closed-pool wall. Requiring this field made the decoder refuse every
    /// reply carrying a pool without the flag (deepseek with sufficient
    /// balance, openrouter always), taking the WHOLE usage.get down.
    pub spendable: Option<bool>,
}

impl SpendPool {
    /// Only these producer funding classes may authorize routing spend.
    pub fn has_supported_funding(&self) -> bool {
        matches!(self.funding.as_str(), "granted" | "subscription")
    }

    /// The producer's flag with the contract's absence rule applied: absent is
    /// "the provider does not say", which must not close a pool - only a
    /// literal `false` walls it.
    fn producer_allows_spend(&self) -> bool {
        self.spendable != Some(false)
    }

    /// A pool can serve routing only when its producer flag, supported funding,
    /// and reported remaining balance all agree. This must not be used to suppress
    /// an exhausted-pool wall: an empty allowed pool is intentionally unspendable.
    pub fn is_routing_spendable(&self) -> bool {
        self.producer_allows_spend()
            && self.has_supported_funding()
            && !self.remaining.is_non_positive()
    }

    fn can_inform_pacing(&self) -> bool {
        self.producer_allows_spend() && self.has_supported_funding()
    }

    fn utilization(&self) -> f64 {
        if self.remaining.is_non_positive() {
            return 1.0;
        }
        self.total
            .as_ref()
            .and_then(|total| self.remaining.ratio_of(total))
            .map(|remaining_ratio| 1.0 - remaining_ratio)
            .unwrap_or(0.0)
    }
}

/// Producer-reported banked quota resets for one account.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedResets {
    #[serde(default)]
    pub available_count: Option<i64>,
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
    /// Provider-scoped spend pools. Pool draw order is not published, so routing
    /// never assigns a request to an individual pool.
    #[serde(default)]
    pub spend: Vec<SpendPool>,
    /// Credits which can reset effective utilization without representing new quota.
    /// Older producers omit the object, and newer ones may omit the count.
    #[serde(default)]
    pub saved_resets: Option<SavedResets>,
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
    /// Present exactly when this entry is a PRESERVED reading served through an
    /// ongoing refresh failure (QTA, additive 2026-08-13). The entry still
    /// carries usage and no error - on the wire it would otherwise be
    /// indistinguishable from a fresh read. Not a fault signal: preserving a
    /// reading through a transient failure is correct producer behavior; this
    /// discloses that it is happening so consumers can price the aging.
    #[serde(default)]
    pub stale: Option<StaleDisclosure>,
    /// When the SOURCE observed this reading (RFC3339), not when the producer
    /// answered the poll. A cache-served lane (antigravity) can be healthy,
    /// undisclosed, and minutes old at once; freshness that stamps poll time
    /// would call it current. Absent on older producers.
    #[serde(default)]
    pub fetched_at: Option<String>,
}

/// Why a preserved reading is aging: `since` stamps the first failed refresh
/// after the served reading was obtained, `class` reuses the errorClass
/// vocabulary and is optional (a producer that cannot classify still
/// discloses the state).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaleDisclosure {
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub class: Option<String>,
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
    /// True when this entry carries a usable quota signal (rate windows or reported
    /// spend pools), not a silent-degrade error or an otherwise empty payload.
    pub fn is_healthy(&self) -> bool {
        self.error.is_none()
            && (self.usage.as_ref().is_some_and(Usage::has_usable_windows)
                || !self.spend.is_empty())
    }
}

/// Quota-poll normalization and interval admission live beside the wire decoder,
/// while durable storage remains behind `RouteStateStore`.
pub mod quota_poll {
    include!("quota_poll.rs");
}

use std::collections::HashMap;

use crate::model_facts::{make_capacity_key, ModelFact};
use crate::model_router::ModelRouteStateSnapshot;
use crate::quota_extract::{extract_windows, WindowScopeKind};
use crate::quota_pace::{
    project_quota_abundance, project_quota_pressure, PaceConfig, PaceError, PaceInput,
    PaceWindowModel, DEFAULT_PACE_CONFIG,
};

/// Hard-block threshold (utilization at/above which a window blocks routes until
/// reset). Matches the plugin's `DEFAULT_HARD_BLOCK_THRESHOLD`.
pub const DEFAULT_HARD_BLOCK_THRESHOLD: f64 = 0.99;

/// Local retry-probe horizon for exhausted windows whose provider reports no reset.
/// This is OUR pacing policy for when to re-ask, not a provider reset claim: QTA's
/// no-fabrication rule still holds on the wire.
pub const NO_RESET_REPROBE_HORIZON_MS: f64 = 6.0 * 60.0 * 60.0 * 1000.0;

/// The per-capacity quota signal one account derives from windows that target it.
#[derive(Debug, Clone, Default, PartialEq)]
struct CapacitySignal {
    pressure: Option<f64>,
    window_model: Option<PaceWindowModel>,
    abundance: Option<f64>,
    abundance_window: Option<String>,
    exhausted_until: Option<f64>,
}

type AccountSignals = HashMap<(String, String), HashMap<String, CapacitySignal>>;

/// `pressure` is optional because an OBSERVED wall and a PROJECTED burn rate are
/// different facts: a provider can report a window fully used while declaring
/// semantics this router will not project (a rolling pool). Refusing to guess the
/// trajectory must not also discard the measurement (Loop R140).
fn record_capacity_signal(
    signals: &mut AccountSignals,
    account_key: &(String, String),
    targets: &[String],
    pressure: Option<(f64, PaceWindowModel)>,
    abundance: Option<(f64, &str)>,
    exhausted_until: Option<f64>,
) {
    for cap in targets {
        let sig = signals
            .entry(account_key.clone())
            .or_default()
            .entry(cap.clone())
            .or_default();
        // Pressure is worst-window, while abundance is the minimum remaining pace
        // among only the windows that target this capacity.
        if let Some((pressure, window_model)) = pressure {
            if sig.pressure.is_none_or(|existing| pressure > existing) {
                sig.pressure = Some(pressure);
                sig.window_model = Some(window_model);
            }
        }
        if let Some((abundance, window_key)) = abundance {
            let replace = match (sig.abundance, sig.abundance_window.as_deref()) {
                (None, _) => true,
                (Some(existing), Some(existing_window)) => {
                    abundance < existing || (abundance == existing && window_key < existing_window)
                }
                (Some(_), None) => true,
            };
            if replace {
                sig.abundance = Some(abundance);
                sig.abundance_window = Some(window_key.to_owned());
            }
        }
        if let Some(exhausted_until) = exhausted_until {
            sig.exhausted_until = Some(match sig.exhausted_until {
                Some(existing) => existing.max(exhausted_until),
                None => exhausted_until,
            });
        }
    }
}

/// Merge quota windows and provider-scoped spend pools onto an existing store-built
/// route-state snapshot using the provider mapping, capacity extraction, reset
/// handling, pressure projection, and exhaustion rules below:
///   - the wire name is mapped to a routing providerID via `name_map`;
///   - each provider's windows are EXTRACTED per its topology (account-wide pools
///     vs antigravity's per-model `extra_rate_windows` → capacity scope);
///   - past-reset windows are skipped when a reset is present; each live window's
///     soft pressure is the pace PROJECTION (lockout-cost), not raw utilization;
///   - a window with `limit_reached` or utilization >= the hard-block threshold
///     sets `quota_exhausted_until` to its reset, or to a local re-probe horizon
///     when no reset was reported, on its matching capacity (a hard block until
///     that local retry point), worst/latest-wins across windows;
///   - granted/subscription spend pools apply only provider-wide: the producer does
///     not document pool draw order, so routing never targets an individual pool;
///     a non-derived empty balance walls through the same no-reset re-probe path,
///     while a `derived` balance contributes pacing only.
///
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
    let mut signals: AccountSignals = HashMap::new();

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

        let account_key = (
            provider_id.clone(),
            entry.account.clone().unwrap_or_default(),
        );
        for win in extracted {
            if win.resets_at.is_some_and(|resets_at| resets_at <= now_ms) {
                continue; // an explicitly past reset is no longer in force
            }
            let pace_input = PaceInput {
                utilization: win.utilization,
                resets_at: win.resets_at,
                window_ms: win.window_ms,
                observed_at: now_ms,
                provider_window_model: win.window_model,
            };
            let abundance = project_quota_abundance(pace_input, pace_config);
            let pace = match project_quota_pressure(pace_input, pace_config) {
                Ok(pace) => Some(pace),
                // A producer has made the fixed-window premise explicitly false.
                // Do not turn its rolling reset horizon into a confident projection.
                // The UTILIZATION is still a measurement the producer stands behind,
                // so the window continues below without a projection: skipping it
                // outright discarded a reported 100%-used window and let the
                // selector choose the exhausted route (Loop R140).
                Err(PaceError::RollingWindow) => None,
            };
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
            record_capacity_signal(
                &mut signals,
                &account_key,
                &targets,
                pace.map(|pace| (pace.pressure, pace.window_model)),
                abundance.map(|abundance| (abundance, win.window_key.as_str())),
                blocked.then(|| {
                    // A rolling pool reports no fixed boundary, so its recovery time
                    // is the local reprobe horizon rather than an invented provider
                    // reset - the same fallback an unreported reset already takes.
                    win.resets_at
                        .unwrap_or(now_ms + NO_RESET_REPROBE_HORIZON_MS)
                }),
            );
        }

        for pool in &entry.spend {
            if !pool.has_supported_funding() {
                continue;
            }
            // A derived remainder is a ceiling rather than an authoritative account
            // balance. It can raise soft pressure, but must never manufacture a wall.
            let exhausted_until = (pool.remaining.is_non_positive() && pool.basis != "derived")
                .then_some(now_ms + NO_RESET_REPROBE_HORIZON_MS);
            if !pool.can_inform_pacing() && exhausted_until.is_none() {
                continue;
            }
            let (pressure, window_model) = if pool.can_inform_pacing() {
                let pace = project_quota_pressure(
                    PaceInput {
                        utilization: pool.utilization(),
                        resets_at: None,
                        window_ms: None,
                        observed_at: now_ms,
                        provider_window_model: None,
                    },
                    pace_config,
                )
                .expect("spend pools have utilization-only pacing semantics");
                (pace.pressure, pace.window_model)
            } else {
                (0.0, PaceWindowModel::UtilizationFallback)
            };
            // The provider chooses its own draw order, so every pool remains scoped
            // to the provider's capacities rather than a pool-selected model route.
            record_capacity_signal(
                &mut signals,
                &account_key,
                provider_caps,
                Some((pressure, window_model)),
                None,
                exhausted_until,
            );
        }
    }

    // Regroup by capacity so the account that serves pressure also supplies that
    // capacity's abundance. A unique minimum-pressure account is authoritative,
    // including when it has no usable perishable window. Tied serving accounts (or
    // a population with no pressure) take MAX abundance because any tied account
    // can serve; a higher-pressure sibling is never used as a fallback.
    let mut accounts_by_capacity: HashMap<String, Vec<CapacitySignal>> = HashMap::new();
    for caps in signals.into_values() {
        for (cap, signal) in caps {
            accounts_by_capacity.entry(cap).or_default().push(signal);
        }
    }

    for (cap, accounts) in accounts_by_capacity {
        let min_pressure = accounts
            .iter()
            .filter_map(|signal| signal.pressure)
            .min_by(f64::total_cmp);
        let serving_count = min_pressure.map_or(accounts.len(), |minimum| {
            accounts
                .iter()
                .filter(|signal| signal.pressure == Some(minimum))
                .count()
        });
        let pressure = min_pressure.map(|minimum| {
            let model = accounts
                .iter()
                .find(|signal| signal.pressure == Some(minimum))
                .and_then(|signal| signal.window_model)
                .unwrap_or(PaceWindowModel::UtilizationFallback);
            (minimum, model)
        });
        let earliest_wall = accounts
            .iter()
            .filter_map(|signal| signal.exhausted_until)
            .min_by(f64::total_cmp);
        let all_walled = accounts
            .iter()
            .all(|signal| signal.exhausted_until.is_some());

        let mut abundance: Option<(f64, String)> = None;
        for signal in accounts
            .into_iter()
            .filter(|signal| min_pressure.is_none() || signal.pressure == min_pressure)
        {
            let Some(candidate) = signal.abundance.zip(signal.abundance_window) else {
                continue;
            };
            let replace = if serving_count == 1 {
                true
            } else {
                abundance.as_ref().is_none_or(|existing| {
                    candidate.0 > existing.0
                        || (candidate.0 == existing.0 && candidate.1 < existing.1)
                })
            };
            if replace {
                abundance = Some(candidate);
            }
        }

        // Fresh soft values replace stored values. In particular, a serving account
        // with pressure but no perishable window clears stale abundance instead of
        // reaching past that account to a sibling's window.
        let state = snapshot.by_capacity.entry(cap).or_default();
        if let Some((pressure, window_model)) = pressure {
            state.quota_pressure = Some(pressure);
            state.quota_window_model = Some(window_model);
        }
        match abundance {
            Some((value, window)) => {
                state.quota_abundance = Some(value);
                state.quota_abundance_window = Some(window);
            }
            None => {
                state.quota_abundance = None;
                state.quota_abundance_window = None;
            }
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::model_facts::{Confidence, ModelRequirements, ModelScores, ScoreProvenance};
    use crate::model_router::{select_model_route, RouteRuntimeState, SelectModelRouteInput};

    const NOW_MS: f64 = 1_893_456_000_000.0; // 2030-01-01T00:00:00Z

    fn pace_config_with_skew_ms(skew_ms: f64) -> PaceConfig {
        let mut config = DEFAULT_PACE_CONFIG;
        config.skew_ms = skew_ms;
        config
    }

    fn fact(provider: &str, model: &str, speed: f64) -> ModelFact {
        ModelFact {
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            speed_is_fleet_median: false,
            provider_id: provider.into(),
            model_id: model.into(),
            variant: None,
            family: None,
            scores: ModelScores {
                elo: 1_800.0,
                eq: 1_800.0,
                speed,
            },
            context_window: None,
            max_output_tokens: None,
            cost_tier: None,
            cost_per_m_tok_out: Some(0.0),
            supports_vision: None,
            supports_tool_use: None,
            prompt_defaults: None,
            confidence: Confidence::Curated,
            notes: None,
            speed_multiplier: None,
            subscription_multiplier: None,
            score_provenance: ScoreProvenance::Curated,
            aa_index: None,
            review_by_ms: None,
            eq_source: None,
        }
    }

    fn usage(value: serde_json::Value) -> ProviderUsage {
        serde_json::from_value(value).expect("valid quota fixture")
    }

    fn overlay(windows: &[ProviderUsage], facts: &[ModelFact]) -> ModelRouteStateSnapshot {
        let mut snapshot = ModelRouteStateSnapshot {
            now: NOW_MS,
            by_route: HashMap::new(),
            by_capacity: HashMap::new(),
        };
        merge_quota_overlay_with(
            &mut snapshot,
            windows,
            facts,
            &HashMap::new(),
            NOW_MS,
            &DEFAULT_PACE_CONFIG,
            DEFAULT_HARD_BLOCK_THRESHOLD,
        );
        snapshot
    }

    fn select(facts: Vec<ModelFact>, snapshot: ModelRouteStateSnapshot) -> String {
        let available_models: HashSet<String> = facts
            .iter()
            .map(|fact| make_capacity_key(&fact.provider_id, &fact.model_id))
            .collect();
        select_model_route(SelectModelRouteInput {
            target_agent: "mason".into(),
            requirements: ModelRequirements { iq: 20.0, eq: 20.0 },
            available_models,
            facts,
            runtime_snapshot: Some(snapshot),
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),
            prefer_fast_specs: Vec::new(),
            required_capabilities: None,
            is_subscription_route: None,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: HashSet::new(),
        })
        .expect("fixture has an admissible route")
        .route_key
    }

    fn assert_near(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("abundance sample");
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected abundance {expected}, got {actual}"
        );
    }

    #[test]
    fn pace_fixture_varies_skew_by_copy_and_assign() {
        let config = pace_config_with_skew_ms(250.0);
        assert_eq!(config.skew_ms, 250.0);
    }

    #[test]
    fn abundance_window_fold_is_minimum_and_capacity_scoped() {
        let openai_fact = fact("openai", "gpt-5.5", 50.0);
        let aggregate = usage(serde_json::json!({
            "provider": "codex",
            "apiProvider": "openai",
            "account": "one",
            "usage": {
                "primary": {
                    "usedPercent": 0.0,
                    "resetsAt": "2030-01-01T01:15:00Z",
                    "windowMinutes": 300
                },
                "secondary": {
                    "usedPercent": 75.0,
                    "resetsAt": "2030-01-08T00:00:00Z",
                    "windowMinutes": 10080
                }
            }
        }));
        let aggregate_snapshot = overlay(&[aggregate], std::slice::from_ref(&openai_fact));
        let aggregate_state = &aggregate_snapshot.by_capacity["openai/gpt-5.5"];
        assert_near(aggregate_state.quota_abundance, 0.25);
        assert_eq!(
            aggregate_state.quota_abundance_window.as_deref(),
            Some("codexbar:codex:secondary"),
            "the weekly minimum, not the 5h value of 4.0, gates the capacity"
        );

        let google_facts = vec![
            fact("google", "antigravity-gemini-x", 50.0),
            fact("google", "antigravity-gemini-y", 50.0),
        ];
        let per_model = usage(serde_json::json!({
            "provider": "antigravity",
            "apiProvider": "google",
            "account": "one",
            "usage": { "extraRateWindows": [
                {
                    "title": "Gemini X",
                    "id": "x-window",
                    "window": {
                        "usedPercent": 90.0,
                        "resetsAt": "2030-01-01T01:00:00Z",
                        "windowMinutes": 60
                    }
                },
                {
                    "title": "Gemini Y",
                    "id": "y-window",
                    "window": {
                        "usedPercent": 0.0,
                        "resetsAt": "2030-01-01T00:15:00Z",
                        "windowMinutes": 60
                    }
                }
            ]}
        }));
        let per_model_snapshot = overlay(&[per_model], &google_facts);
        let x = &per_model_snapshot.by_capacity["google/antigravity-gemini-x"];
        let y = &per_model_snapshot.by_capacity["google/antigravity-gemini-y"];
        assert_near(x.quota_abundance, 0.1);
        assert_near(y.quota_abundance, 4.0);
        assert_eq!(
            x.quota_abundance_window.as_deref(),
            Some("codexbar:antigravity:x-window")
        );
        assert_eq!(
            y.quota_abundance_window.as_deref(),
            Some("codexbar:antigravity:y-window")
        );
    }

    #[test]
    fn abundance_follows_the_minimum_pressure_serving_account() {
        let facts = vec![
            fact("anthropic", "claude", 10.0),
            fact("xai", "grok", 100.0),
        ];
        let windows = vec![
            usage(serde_json::json!({
                "provider": "claude",
                "apiProvider": "anthropic",
                "account": "abundant-serving",
                "usage": { "primary": {
                    "usedPercent": 0.0,
                    "resetsAt": "2030-01-01T01:15:00Z",
                    "windowMinutes": 300
                }}
            })),
            usage(serde_json::json!({
                "provider": "claude",
                "apiProvider": "anthropic",
                "account": "scarce-sibling",
                "usage": { "primary": {
                    "usedPercent": 95.0,
                    "resetsAt": "2030-01-02T09:36:00Z",
                    "windowMinutes": 10080
                }}
            })),
            usage(serde_json::json!({
                "provider": "grok",
                "apiProvider": "xai",
                "account": "only",
                "usage": { "primary": {
                    "usedPercent": 50.0,
                    "resetsAt": "2030-01-01T00:30:00Z",
                    "windowMinutes": 60
                }}
            })),
        ];
        let snapshot = overlay(&windows, &facts);
        let anthropic = &snapshot.by_capacity["anthropic/claude"];
        assert_near(anthropic.quota_abundance, 4.0);
        assert_eq!(
            anthropic.quota_abundance_window.as_deref(),
            Some("codexbar:claude:primary")
        );
        assert_eq!(
            select(facts, snapshot),
            "anthropic/claude",
            "the served account's 4.0 abundance must outrank the faster neutral route; MIN across accounts would select xai"
        );
    }

    #[test]
    fn serving_account_without_a_usable_window_does_not_reach_a_sibling() {
        let facts = vec![fact("anthropic", "claude", 50.0)];
        let windows = vec![
            usage(serde_json::json!({
                "provider": "claude",
                "apiProvider": "anthropic",
                "account": "serving-spend-pool",
                "usage": {},
                "spend": [{
                    "id": "seat",
                    "label": "Seat",
                    "funding": "subscription",
                    "remaining": {"minor": 100, "exponent": 0, "unit": "USD"},
                    "total": {"minor": 100, "exponent": 0, "unit": "USD"},
                    "basis": "reported",
                    "spendable": true
                }]
            })),
            usage(serde_json::json!({
                "provider": "claude",
                "apiProvider": "anthropic",
                "account": "window-sibling",
                "usage": { "primary": {
                    "usedPercent": 95.0,
                    "resetsAt": "2030-01-02T09:36:00Z",
                    "windowMinutes": 10080
                }}
            })),
        ];
        let mut snapshot = ModelRouteStateSnapshot {
            now: NOW_MS,
            by_route: HashMap::new(),
            by_capacity: HashMap::from([(
                "anthropic/claude".into(),
                RouteRuntimeState {
                    quota_abundance: Some(8.0),
                    quota_abundance_window: Some("stale-window".into()),
                    ..Default::default()
                },
            )]),
        };
        merge_quota_overlay_with(
            &mut snapshot,
            &windows,
            &facts,
            &HashMap::new(),
            NOW_MS,
            &DEFAULT_PACE_CONFIG,
            DEFAULT_HARD_BLOCK_THRESHOLD,
        );

        let state = &snapshot.by_capacity["anthropic/claude"];
        assert_eq!(state.quota_abundance, None);
        assert_eq!(state.quota_abundance_window, None);
    }

    #[test]
    fn tied_free_accounts_keep_anthropic_shaped_capacity_abundant() {
        let facts = vec![fact("anthropic", "claude", 50.0)];
        let windows: Vec<ProviderUsage> = [
            ("busy", 95.0, "2030-01-01T16:48:00Z"),
            ("free-a", 5.0, "2030-01-01T16:48:00Z"),
            ("free-b", 5.0, "2030-01-08T00:00:00Z"),
            ("free-c", 5.0, "2030-01-08T00:00:00Z"),
        ]
        .into_iter()
        .map(|(account, used_percent, resets_at)| {
            usage(serde_json::json!({
                "provider": "claude",
                "apiProvider": "anthropic",
                "account": account,
                "usage": { "primary": {
                    "usedPercent": used_percent,
                    "resetsAt": resets_at,
                    "windowMinutes": 10080
                }}
            }))
        })
        .collect();
        let snapshot = overlay(&windows, &facts);
        let state = &snapshot.by_capacity["anthropic/claude"];

        assert_near(state.quota_abundance, 9.5);
        assert!(
            state.quota_abundance.expect("tied account maximum") > 1.0,
            "three 5%-used serving accounts must not be demoted by the 95%-used sibling"
        );
    }
}
