//! Quota usage wire types: the decode contract for `insula` (formerly `ai-provider-quota`)'s
//! `usage.get` response, mirrored from `quota-core/src/model.rs`.
//!
//! These are DESERIALIZE-side mirrors: striatum consumes quota windows
//! module-to-module, so it must decode the exact `ProviderUsage[]` shape the
//! quota module emits (camelCase keys, silent-degrade entries carrying `error`).
//! They also serialize back to that same shape so the routing module can persist
//! its last accepted snapshot and restore it after a restart; only fields routing
//! decodes survive that round trip.
//! Kept in -core (not the module binary) because mapping windows into a routing
//! runtime snapshot is pure logic, testable without the wire.

use serde::{Deserialize, Serialize};

use crate::quota_pace::ProviderWindowModel;

/// Maximum source age of last-good quota served through a fetch failure.
pub const LAST_GOOD_MAX_AGE_MS: i64 = 30 * 60_000;

/// One rate-limit window: how much of a quota pool is spent and when it resets.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
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
    /// The window's period as Insula names it (`five_hour`, `weekly`). Additive:
    /// older producers omit it, and readers then fall back to the window id.
    /// Kept as a String so a new period is an unknown value, not a decode error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_kind: Option<String>,
}

/// A scoped window bundled under one account: a per-model window, or one of
/// Antigravity's pooled windows (`gemini-5h`, `3p-weekly`, ...).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtraWindow {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub window: Option<RateWindow>,
    /// The window's period (`five_hour`, `weekly`), when the producer publishes it
    /// beside the id rather than inside `window`. See `RateWindow::window_kind`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_kind: Option<String>,
}

/// The window topology for one account: up to three account-wide pools plus an
/// optional list of per-model pools.
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpendPool {
    pub id: String,
    pub label: String,
    pub funding: String,
    /// The pool's balance. ABSENT when the producer knows the pool exists but
    /// the provider stated no amount (insula's `basis: "unstated"`: a Codex
    /// account whose prepaid-credits pool carries no balance). Requiring it made
    /// the decoder refuse the whole `usage.get` array over one such pool, so
    /// every refresh failed and routing kept choosing on its last accepted
    /// snapshot. An absent balance is unknown: it never walls and never paces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<SpendBalance>,
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

    /// Whether Insula has named this funding class. `unknown` is an explicit
    /// wire value, but it still does not authorize routing spend.
    pub fn has_known_funding(&self) -> bool {
        matches!(
            self.funding.as_str(),
            "granted" | "subscription" | "purchased" | "unknown"
        )
    }

    /// The producer's flag with the contract's absence rule applied: absent is
    /// "the provider does not say", which must not close a pool - only a
    /// literal `false` walls it.
    fn producer_allows_spend(&self) -> bool {
        self.spendable != Some(false)
    }

    /// True only when the producer STATED a balance and it is empty. An
    /// unstated balance is unknown, which is not the same as empty.
    pub fn is_known_empty(&self) -> bool {
        self.remaining
            .as_ref()
            .is_some_and(SpendBalance::is_non_positive)
    }

    fn can_inform_pacing(&self) -> bool {
        self.remaining.is_some() && self.producer_allows_spend() && self.has_supported_funding()
    }

    /// A prepaid-credits pool (`credits`) with a stated positive balance that
    /// the producer has not closed. The provider draws it once the plan's limit
    /// is reached, so it keeps the account serving past a full window.
    pub fn carries_prepaid_credits(&self) -> bool {
        self.id == "credits"
            && self.producer_allows_spend()
            && self
                .remaining
                .as_ref()
                .is_some_and(|balance| !balance.is_non_positive())
    }

    fn utilization(&self) -> f64 {
        let Some(remaining) = self.remaining.as_ref() else {
            // Unreachable through `can_inform_pacing`; an unknown balance adds
            // no pressure rather than inventing one.
            return 0.0;
        };
        if remaining.is_non_positive() {
            return 1.0;
        }
        self.total
            .as_ref()
            .and_then(|total| remaining.ratio_of(total))
            .map(|remaining_ratio| 1.0 - remaining_ratio)
            .unwrap_or(0.0)
    }
}

/// Producer-reported banked quota resets for one account.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedResets {
    #[serde(default)]
    pub available_count: Option<i64>,
}

/// One provider/account's usage entry. The `usage.get` result is an array of
/// these. A fetch failure becomes an entry carrying `error` (silent-degrade),
/// never a failure of the whole array: the consumer skips any entry with a
/// truthy `error`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUsage {
    /// The quota module's provider name (e.g. "codex").
    pub provider: String,
    /// The canonical models.dev slug, published when a catalog counterpart exists.
    /// Entries without one are not inferred from their provider name, except for
    /// Antigravity's separately scoped Code Assist pools.
    #[serde(default)]
    pub api_provider: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
    /// Vault credential which produced this reading. For account-less rows the
    /// consumer resolves this through its listed credential roster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
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
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
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
    /// Group readings by the producer's explicit account, or by the vault
    /// credential when the producer supplied no account.
    pub fn account_identity(&self) -> Option<&str> {
        self.account.as_deref().or(self.credential_ref.as_deref())
    }

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
pub use crate::quota_poll;

use std::collections::HashMap;

use crate::model_facts::{make_capacity_key, ModelFact};
use crate::model_router::ModelRouteStateSnapshot;
use crate::quota_extract::{
    antigravity_pool_capacities, extract_windows, ModelPool, WindowScopeKind,
};
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
pub const NO_RESET_REPROBE_HORIZON_MS: f64 = crate::fence_lifetimes::QUOTA_FENCE_RECHECK_MS as f64;

/// The per-capacity quota signal one account derives from windows that target it.
#[derive(Debug, Clone, Default, PartialEq)]
struct CapacitySignal {
    pressure: Option<f64>,
    window_model: Option<PaceWindowModel>,
    /// Every window's abundance reading, kept raw until all of the account's
    /// windows for this capacity are in: the fold rule (scarcity wins, else the
    /// longest window) compares windows against each other, so it cannot be
    /// applied incrementally as each window arrives.
    abundance_samples: Vec<AbundanceSample>,
    abundance: Option<f64>,
    abundance_window: Option<String>,
    exhausted_until: Option<f64>,
}

/// One window's abundance reading with the identity and length the fold needs.
#[derive(Debug, Clone, PartialEq)]
struct AbundanceSample {
    abundance: f64,
    window_key: String,
    window_ms: Option<f64>,
}

/// Fold one account's window abundances for one capacity into the account's
/// single abundance. Scarcity always wins: if any window's abundance is below
/// 1.0 (that window runs out before its reset at the current pace), the account
/// reads the LOWEST value. Otherwise every window has surplus, and the account
/// reads the LONGEST window's value, because that surplus is what perishes
/// unused at reset while shorter windows only pace how fast it can be spent.
/// (Not the highest value: a window exactly on pace at 1.0 must not be lifted
/// by a shorter window's 3.0.) Ties on window length take the lower value;
/// ties on value take the lexicographically smaller window key, matching the
/// tie-break the previous incremental fold used.
fn fold_account_abundance(samples: &[AbundanceSample]) -> Option<(f64, String)> {
    if samples.iter().any(|sample| sample.abundance < 1.0) {
        return samples
            .iter()
            .min_by(|a, b| {
                a.abundance
                    .total_cmp(&b.abundance)
                    .then_with(|| a.window_key.cmp(&b.window_key))
            })
            .map(|sample| (sample.abundance, sample.window_key.clone()));
    }
    samples
        .iter()
        .max_by(|a, b| {
            // A missing length (no fixed boundary) sorts as the shortest window.
            a.window_ms
                .unwrap_or(0.0)
                .total_cmp(&b.window_ms.unwrap_or(0.0))
                // Reversed so that, at equal length, the LOWER value wins ...
                .then_with(|| b.abundance.total_cmp(&a.abundance))
                // ... and at equal value the smaller window key wins.
                .then_with(|| b.window_key.cmp(&a.window_key))
        })
        .map(|sample| (sample.abundance, sample.window_key.clone()))
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
    abundance: Option<(f64, &str, Option<f64>)>,
    exhausted_until: Option<f64>,
) {
    for cap in targets {
        let sig = signals
            .entry(account_key.clone())
            .or_default()
            .entry(cap.clone())
            .or_default();
        // Pressure is worst-window. Abundance readings are COLLECTED, not
        // folded here: a later window can still be the scarce one that wins
        // over an earlier long one (or vice versa), so the fold runs once all
        // of the account's windows are in (fold_account_abundance below).
        if let Some((pressure, window_model)) = pressure {
            if sig.pressure.is_none_or(|existing| pressure > existing) {
                sig.pressure = Some(pressure);
                sig.window_model = Some(window_model);
            }
        }
        if let Some((abundance, window_key, window_ms)) = abundance {
            sig.abundance_samples.push(AbundanceSample {
                abundance,
                window_key: window_key.to_owned(),
                window_ms,
            });
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
/// route-state snapshot using the published provider slug, capacity extraction, reset
/// handling, pressure projection, and exhaustion rules below:
///   - `apiProvider` joins entries to routing provider IDs; only Antigravity's
///     un-slugged Code Assist pools use the narrow exception below;
///   - each provider's windows are EXTRACTED per its topology (account-wide pools
///     vs antigravity's pooled `extra_rate_windows` → the capacities of the
///     models that draw on that pool);
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
    model_pools: &[ModelPool],
) {
    merge_quota_overlay_with(
        snapshot,
        windows,
        facts,
        now_default(snapshot),
        &DEFAULT_PACE_CONFIG,
        DEFAULT_HARD_BLOCK_THRESHOLD,
        model_pools,
    );
}

fn now_default(snapshot: &ModelRouteStateSnapshot) -> f64 {
    // The snapshot carries the `now` the store built it with; reuse it so the
    // overlay's reset-cutoff/pace anchor matches the rest of the selection.
    snapshot.now
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaRouteJoin<'a> {
    pub provider_id: &'a str,
    pub antigravity_family_only: bool,
}

#[derive(Debug, Clone, Copy)]
struct ExceptionalQuotaJoin {
    insula_provider: &'static str,
    route_model_prefix: &'static str,
}

/// Insula deliberately omits `apiProvider` for Antigravity: its pools are Code
/// Assist pools, not the catalog vendor's own API. The consumer contract keys
/// those pools by window `id`, never title: `gemini-*` serves
/// `antigravity-gemini-*`, while `3p-*` serves Antigravity's Claude/GPT family.
/// Extraction and pool routing keep those windows separate; this join is
/// restricted to the Antigravity route family. Its route provider is read from
/// the catalog: the one provider that serves models under the Antigravity
/// prefix.
const ANTIGRAVITY_QUOTA_JOIN: ExceptionalQuotaJoin = ExceptionalQuotaJoin {
    insula_provider: "antigravity",
    route_model_prefix: "antigravity-",
};

/// The one catalog provider serving models whose ids start with `prefix`, or
/// `None` when no provider or more than one does.
fn sole_provider_serving_prefix<'a>(facts: &'a [ModelFact], prefix: &str) -> Option<&'a str> {
    let mut providers = facts
        .iter()
        .filter(|fact| fact.model_id.starts_with(prefix))
        .map(|fact| fact.provider_id.as_str());
    let first = providers.next()?;
    providers.all(|provider| provider == first).then_some(first)
}

/// Resolve one Insula entry to its route-provider namespace. Published slugs are
/// authoritative; an absent slug is not guessed, except for Antigravity's
/// separately identified Code Assist pools, joined to the catalog provider
/// that serves the Antigravity route family.
pub fn quota_route_join<'a>(
    entry: &'a ProviderUsage,
    facts: &'a [ModelFact],
) -> Option<QuotaRouteJoin<'a>> {
    quota_route_join_for_source(&entry.provider, entry.api_provider.as_deref(), facts)
}

/// Resolve just the identity fields so account readers can share the join
/// without decoding the producer's spend balances.
pub fn quota_route_join_for_source<'a>(
    provider: &str,
    api_provider: Option<&'a str>,
    facts: &'a [ModelFact],
) -> Option<QuotaRouteJoin<'a>> {
    quota_route_join_with(provider, api_provider, || {
        sole_provider_serving_prefix(facts, ANTIGRAVITY_QUOTA_JOIN.route_model_prefix)
    })
}

fn quota_route_join_with<'a>(
    provider: &str,
    api_provider: Option<&'a str>,
    antigravity_provider: impl FnOnce() -> Option<&'a str>,
) -> Option<QuotaRouteJoin<'a>> {
    let antigravity = provider == ANTIGRAVITY_QUOTA_JOIN.insula_provider;
    if let Some(provider_id) = api_provider.filter(|slug| !slug.trim().is_empty()) {
        return Some(QuotaRouteJoin {
            provider_id,
            antigravity_family_only: antigravity,
        });
    }
    if !antigravity {
        return None;
    }
    Some(QuotaRouteJoin {
        provider_id: antigravity_provider()?,
        antigravity_family_only: true,
    })
}

fn quota_join_matches_capacity(join: QuotaRouteJoin<'_>, capacity_key: &str) -> bool {
    let Some((provider_id, model_id)) = capacity_key.split_once('/') else {
        return false;
    };
    provider_id == join.provider_id
        && (!join.antigravity_family_only
            || model_id.starts_with(ANTIGRAVITY_QUOTA_JOIN.route_model_prefix))
}

/// Capacity lookup for repeated quota readings against one resolved catalog.
#[derive(Clone, Default)]
pub struct QuotaCapacityIndex {
    by_provider: std::collections::HashMap<String, Vec<String>>,
    antigravity: std::collections::HashMap<String, Vec<String>>,
    antigravity_provider: Option<String>,
}

impl QuotaCapacityIndex {
    pub fn new(facts: &[ModelFact]) -> Self {
        let mut index = Self {
            antigravity_provider: sole_provider_serving_prefix(
                facts,
                ANTIGRAVITY_QUOTA_JOIN.route_model_prefix,
            )
            .map(str::to_owned),
            ..Default::default()
        };
        for fact in facts {
            let key = make_capacity_key(&fact.provider_id, &fact.model_id);
            // Use the same split as the linear matcher, including model IDs with slashes.
            let (provider, model) = key.split_once('/').expect("capacity key contains slash");
            if model.starts_with(ANTIGRAVITY_QUOTA_JOIN.route_model_prefix) {
                index
                    .antigravity
                    .entry(provider.to_owned())
                    .or_default()
                    .push(key.clone());
            }
            index
                .by_provider
                .entry(provider.to_owned())
                .or_default()
                .push(key);
        }
        index
    }

    pub fn lookup<'a>(&'a self, entry: &'a ProviderUsage) -> Option<(&'a str, &'a [String])> {
        let join = quota_route_join_with(&entry.provider, entry.api_provider.as_deref(), || {
            self.antigravity_provider.as_deref()
        })?;
        let index = if join.antigravity_family_only {
            &self.antigravity
        } else {
            &self.by_provider
        };
        Some((
            join.provider_id,
            index.get(join.provider_id).map_or(&[], Vec::as_slice),
        ))
    }
}

/// The route capacities an entry can inform, including Antigravity's family-only
/// restriction. Event persistence and live quota overlays use the same join.
pub fn quota_capacity_keys(entry: &ProviderUsage, facts: &[ModelFact]) -> Vec<String> {
    let Some(join) = quota_route_join(entry, facts) else {
        return Vec::new();
    };
    facts
        .iter()
        .filter_map(|fact| {
            let capacity_key = make_capacity_key(&fact.provider_id, &fact.model_id);
            quota_join_matches_capacity(join, &capacity_key).then_some(capacity_key)
        })
        .collect()
}

/// The explicit-config form (testable with a fixed `now`, pace config, threshold).
pub fn merge_quota_overlay_with(
    snapshot: &mut ModelRouteStateSnapshot,
    windows: &[ProviderUsage],
    facts: &[ModelFact],
    now_ms: f64,
    pace_config: &PaceConfig,
    hard_block_threshold: f64,
    model_pools: &[ModelPool],
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
        let Some(join) = quota_route_join(entry, facts) else {
            continue;
        };
        let empty: Vec<String> = Vec::new();
        let mut antigravity_caps = Vec::new();
        let provider_caps = match caps_by_provider.get(join.provider_id) {
            Some(caps) if join.antigravity_family_only => {
                antigravity_caps.extend(
                    caps.iter()
                        .filter(|capacity| quota_join_matches_capacity(join, capacity))
                        .cloned(),
                );
                &antigravity_caps
            }
            Some(caps) => caps,
            None => &empty,
        };
        let extracted = extract_windows(entry, &entry.provider, provider_caps);

        let account_key = (
            join.provider_id.to_string(),
            entry.account_identity().unwrap_or_default().to_owned(),
        );
        // The provider draws an account's prepaid credits once its plan limit
        // is reached, so a full window on an account that still holds credits
        // is not a wall. Whether new work may spend those credits is the account
        // allocator's decision under its paid-credits policy, made on the same
        // reading; walling the route here would refuse the work before the
        // allocator is asked. The full window still paces the route.
        let carries_prepaid_credits = entry.spend.iter().any(SpendPool::carries_prepaid_credits);
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
            let blocked = !carries_prepaid_credits
                && (win.limit_reached || win.utilization >= hard_block_threshold);

            // The capacities this window applies to.
            let targets: Vec<String> = match win.scope_kind {
                WindowScopeKind::Account => provider_caps.clone(),
                WindowScopeKind::CapacityKey => win.applies_key.clone().into_iter().collect(),
                // A pooled window pressures and walls every routed model that
                // draws on that pool, and no other model of the provider.
                WindowScopeKind::AntigravityPool => win
                    .applies_key
                    .as_deref()
                    .map(|pool| antigravity_pool_capacities(pool, provider_caps, model_pools))
                    .unwrap_or_default(),
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
                abundance.map(|abundance| (abundance, win.window_key.as_str(), win.window_ms)),
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
            let exhausted_until = (pool.is_known_empty() && pool.basis != "derived")
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
        for (cap, mut signal) in caps {
            // All of this account's windows for this capacity are in: fold its
            // abundance once (scarcity wins, otherwise the longest window).
            if let Some((value, window_key)) = fold_account_abundance(&signal.abundance_samples) {
                signal.abundance = Some(value);
                signal.abundance_window = Some(window_key);
            }
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

    /// The model-specific quota pools the config converter writes.
    fn model_pools() -> Vec<ModelPool> {
        let defaults: serde_json::Value = serde_json::from_str(include_str!(
            "../../striatum-module/data/config_convert_defaults.json"
        ))
        .unwrap();
        serde_json::from_value(defaults["quota_model_pools"].clone()).unwrap()
    }
    use crate::model_facts::{Confidence, ModelRequirements, ModelScores, ScoreProvenance};
    use crate::model_router::{RouteRuntimeState, SelectModelRouteInput};
    use crate::usage_facts::ACCOUNT_PRODUCER;

    const NOW_MS: f64 = 1_893_456_000_000.0; // 2030-01-01T00:00:00Z

    fn fact(provider: &str, model: &str, speed: f64) -> ModelFact {
        ModelFact {
            billing_plane: None,
            auth_methods: None,
            login_methods_documented: false,
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            speed_is_fleet_median: false,
            decommissioned: false,
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
            eq_source: None,
            class_vector: None,
            class_vector_is_overlay: false,
            upstream_status: None,
            model_type: None,
            model_kind: None,
            mode_of: None,
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
            ..Default::default()
        };
        merge_quota_overlay_with(
            &mut snapshot,
            windows,
            facts,
            NOW_MS,
            &DEFAULT_PACE_CONFIG,
            DEFAULT_HARD_BLOCK_THRESHOLD,
            &model_pools(),
        );
        snapshot
    }

    fn select(facts: Vec<ModelFact>, snapshot: ModelRouteStateSnapshot) -> String {
        let available_models: HashSet<String> = facts
            .iter()
            .map(|fact| make_capacity_key(&fact.provider_id, &fact.model_id))
            .collect();
        // Both fixture routes sit at the frontier and the demand is iq 20, so the
        // over-qualification term would be equal for both; it is switched off so
        // the abundance ordering under test is reached.
        crate::model_router::select_model_route_with_config(
            SelectModelRouteInput {
                target_agent: "mason".into(),
                target_is_head: false,
                requirements: ModelRequirements { iq: 20.0, eq: 20.0 },
                available_models,
                facts,
                runtime_snapshot: Some(snapshot),
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: None,
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: HashSet::new(),
                providers_without_cache_write_reports: HashSet::new(),
            },
            &crate::routing_config::ROUTING_CONFIG_WITHOUT_OVERQUALIFICATION,
        )
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
    fn codex_weekly_primary_is_joined_and_one_nonexhausted_account_prevents_a_raw_wall() {
        let facts = vec![fact("openai", "gpt-6-luna", 50.0)];
        let now = ms("2026-11-08T19:30:00Z");
        let entries: Vec<_> = [
            ("a", 100.0, "2026-11-11T06:38:29Z"),
            ("b", 100.0, "2026-11-13T11:40:07Z"),
            ("c", 98.0, "2026-11-13T15:50:27Z"),
        ]
        .into_iter()
        .map(|(account, used, reset)| {
            usage(serde_json::json!({
                "provider":"codex", "apiProvider":"openai", "account":account,
                "source":"vault", "fetchedAt":"2026-11-08T19:30:00Z", "spend":[],
                "savedResets":{"availableCount":2},
                "usage":{"primary":{"usedPercent":used,"resetsAt":reset,"windowMinutes":10080}}
            }))
        })
        .collect();
        let mut snapshot = ModelRouteStateSnapshot {
            now,
            ..Default::default()
        };
        merge_quota_overlay_with(
            &mut snapshot,
            &entries,
            &facts,
            now,
            &DEFAULT_PACE_CONFIG,
            DEFAULT_HARD_BLOCK_THRESHOLD,
            &model_pools(),
        );
        assert_eq!(
            quota_capacity_keys(&entries[0], &facts),
            vec!["openai/gpt-6-luna"]
        );
        let signal = &snapshot.by_capacity["openai/gpt-6-luna"];
        assert!(signal.quota_pressure.unwrap() > 0.0);
        // Raw usage alone cannot wall the 98% account. The consumer's allocator
        // floors, applied by the module, decide whether it can actually serve.
        assert_eq!(signal.quota_exhausted_until, None);
        let mut entries = entries;
        entries[2]
            .usage
            .as_mut()
            .unwrap()
            .primary
            .as_mut()
            .unwrap()
            .used_percent = 100.0;
        merge_quota_overlay_with(
            &mut snapshot,
            &entries,
            &facts,
            now,
            &DEFAULT_PACE_CONFIG,
            DEFAULT_HARD_BLOCK_THRESHOLD,
            &model_pools(),
        );
        assert_eq!(
            snapshot.by_capacity["openai/gpt-6-luna"].quota_exhausted_until,
            Some(ms("2026-11-11T06:38:29Z"))
        );
    }

    #[test]
    fn account_producer_deferral_preserves_operator_multiplier_and_one_fallback_selection() {
        assert!(!ACCOUNT_PRODUCER);

        let mut operator_route = fact("openai", "operator-priced", 50.0);
        operator_route.cost_per_m_tok_out = Some(20.0);
        operator_route.subscription_multiplier = Some(40.0);
        let mut alternative = fact("anthropic", "alternative", 50.0);
        alternative.cost_per_m_tok_out = Some(1.0);
        let operator_facts = vec![operator_route.clone(), alternative.clone()];
        assert_eq!(
            select(
                operator_facts.clone(),
                overlay(&[], operator_facts.as_slice())
            ),
            "openai/operator-priced"
        );

        operator_route.subscription_multiplier = None;
        let fallback_facts = vec![operator_route, alternative];
        assert_eq!(
            select(
                fallback_facts.clone(),
                overlay(&[], fallback_facts.as_slice())
            ),
            "anthropic/alternative"
        );
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
            "apiProvider": null,
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

    /// One Antigravity account in the live 2026-10-02 shape: no primary window,
    /// every pool an `extraRateWindows` entry keyed by `<pool>-<period>`.
    fn antigravity_account(account: &str, pools: &[(&str, f64, &str)]) -> ProviderUsage {
        let windows: Vec<serde_json::Value> = pools
            .iter()
            .map(|(id, used, resets_at)| {
                let minutes = if id.ends_with("-5h") { 300 } else { 10_080 };
                serde_json::json!({
                    "title": format!("{id} title"),
                    "id": id,
                    "window": {
                        "usedPercent": used,
                        "resetsAt": resets_at,
                        "windowMinutes": minutes
                    }
                })
            })
            .collect();
        usage(serde_json::json!({
            "provider": "antigravity",
            "apiProvider": null,
            "account": account,
            "usage": {
                "primary": null,
                "secondary": null,
                "tertiary": null,
                "extraRateWindows": windows
            }
        }))
    }

    fn antigravity_overlay(
        windows: &[ProviderUsage],
        facts: &[ModelFact],
    ) -> ModelRouteStateSnapshot {
        let mut snapshot = ModelRouteStateSnapshot {
            now: NOW_MS,
            by_route: HashMap::new(),
            by_capacity: HashMap::new(),
            ..Default::default()
        };
        merge_quota_overlay_with(
            &mut snapshot,
            windows,
            facts,
            NOW_MS,
            &DEFAULT_PACE_CONFIG,
            DEFAULT_HARD_BLOCK_THRESHOLD,
            &model_pools(),
        );
        snapshot
    }

    const AG_GEMINI: &str = "google/antigravity-gemini-3.8-flash";
    const AG_CLAUDE: &str = "google/antigravity-claude-opus-4-6-thinking";
    const API_GEMINI: &str = "google/gemini-3.8-flash";
    const ALTERNATIVE: &str = "openai/gpt-alternative";
    const AG_ARGON: &str = "google/antigravity-gemini-4-argon";
    const AG_GPT: &str = "google/antigravity-gpt-5.4";

    /// Antigravity Gemini is the cheapest-looking route; the alternative costs more.
    fn antigravity_facts() -> Vec<ModelFact> {
        let mut alternative = fact("openai", "gpt-alternative", 50.0);
        alternative.cost_per_m_tok_out = Some(5.0);
        vec![
            fact("google", "antigravity-gemini-3.8-flash", 100.0),
            fact("google", "antigravity-claude-opus-4-6-thinking", 10.0),
            fact("google", "gemini-3.8-flash", 10.0),
            alternative,
        ]
    }

    fn ms(rfc3339: &str) -> f64 {
        crate::quota_extract::chrono_parse_rfc3339(rfc3339).expect("fixture timestamp")
    }

    #[test]
    fn antigravity_quota_is_limited_to_its_route_family() {
        let facts = vec![
            fact("google", "antigravity-gemini-4-argon", 50.0),
            fact("google", "antigravity-gemini-3.8-flash", 50.0),
            fact("google", "gemini-3.8-flash", 50.0),
        ];
        let entry = antigravity_account(
            "only",
            &[
                ("gemini-5h", 90.0, "2030-01-01T01:00:00Z"),
                ("3p-weekly", 100.0, "2030-01-03T00:00:00Z"),
            ],
        );
        let snapshot = antigravity_overlay(&[entry], &facts);

        assert!(snapshot.by_capacity[AG_ARGON].quota_pressure.is_some());
        assert_eq!(snapshot.by_capacity[AG_ARGON].quota_exhausted_until, None);
        assert_eq!(snapshot.by_capacity[AG_GEMINI].quota_exhausted_until, None);
        assert!(!snapshot.by_capacity.contains_key(API_GEMINI));
    }

    #[test]
    fn antigravity_gemini_5h_at_100_walls_the_route_until_its_reset() {
        let facts = antigravity_facts();
        assert_eq!(
            select(facts.clone(), antigravity_overlay(&[], &facts)),
            AG_GEMINI,
            "without quota, Antigravity Gemini is the route that wins"
        );

        // Gemini 5-hour quota is exhausted, but the weekly quota is not.
        let reset = "2030-01-01T01:19:00Z";
        let entry = antigravity_account(
            "only",
            &[
                ("gemini-weekly", 29.4, "2030-01-05T00:00:00Z"),
                ("gemini-5h", 100.0, reset),
                ("3p-weekly", 0.05, "2030-01-04T00:00:00Z"),
                ("3p-5h", 0.0, "2030-01-01T04:00:00Z"),
            ],
        );
        let snapshot = antigravity_overlay(&[entry], &facts);
        assert_eq!(
            snapshot.by_capacity[AG_GEMINI].quota_exhausted_until,
            Some(ms(reset)),
            "walled until the reset of the window at 100%"
        );
        assert_eq!(
            snapshot
                .by_capacity
                .get(AG_CLAUDE)
                .and_then(|state| state.quota_exhausted_until),
            None,
            "the 3p pool is free, so Claude through Antigravity is not walled"
        );
        assert!(
            !snapshot.by_capacity.contains_key(API_GEMINI),
            "the Gemini API product draws on no Antigravity pool"
        );
        // Selection between the walled route and the alternative only: Claude
        // through Antigravity is free here, and its own admission is asserted above.
        let contest: Vec<ModelFact> = facts
            .into_iter()
            .filter(|fact| {
                let key = make_capacity_key(&fact.provider_id, &fact.model_id);
                key == AG_GEMINI || key == ALTERNATIVE
            })
            .collect();
        assert_eq!(
            select(contest, snapshot),
            ALTERNATIVE,
            "the walled route loses to the pricier alternative"
        );
    }

    #[test]
    fn antigravity_pool_worst_window_drives_projected_pressure_and_admits() {
        let facts = antigravity_facts();
        // 5h window: 40% used one hour in (4h left), a burn that runs out before
        // reset. Weekly: 29% used three days in, comfortably on pace.
        let five_hour_reset = "2030-01-01T04:00:00Z";
        let weekly_reset = "2030-01-05T00:00:00Z";
        let entry = antigravity_account(
            "only",
            &[
                ("gemini-weekly", 29.0, weekly_reset),
                ("gemini-5h", 40.0, five_hour_reset),
            ],
        );
        let snapshot = antigravity_overlay(&[entry], &facts);
        let state = &snapshot.by_capacity[AG_GEMINI];
        assert_eq!(state.quota_exhausted_until, None, "admitted, not walled");

        let projected = |used: f64, resets_at: &str, minutes: f64| {
            project_quota_pressure(
                PaceInput {
                    utilization: used,
                    resets_at: Some(ms(resets_at)),
                    window_ms: Some(minutes * 60_000.0),
                    observed_at: NOW_MS,
                    provider_window_model: None,
                },
                &DEFAULT_PACE_CONFIG,
            )
            .expect("fixed window")
            .pressure
        };
        let five_hour = projected(0.40, five_hour_reset, 300.0);
        let weekly = projected(0.29, weekly_reset, 10_080.0);
        assert!(
            five_hour > weekly,
            "fixture: the 5h window must be the worse one ({five_hour} vs {weekly})"
        );
        assert_eq!(
            state.quota_pressure,
            Some(five_hour),
            "pressure is the pool's worst window, projected like every provider's"
        );
        assert_eq!(state.quota_window_model, Some(PaceWindowModel::Fixed));
    }

    #[test]
    fn antigravity_third_party_models_read_the_3p_pool_not_the_gemini_pool() {
        let mut facts = antigravity_facts();
        facts.push(fact("google", "antigravity-gemini-4-argon", 50.0));
        facts.push(fact("google", "antigravity-gpt-5.4", 50.0));
        let gemini_walled = antigravity_account(
            "only",
            &[
                ("gemini-5h", 100.0, "2030-01-01T01:00:00Z"),
                ("3p-5h", 10.0, "2030-01-01T04:00:00Z"),
            ],
        );
        let snapshot = antigravity_overlay(&[gemini_walled], &facts);
        assert!(snapshot.by_capacity[AG_GEMINI]
            .quota_exhausted_until
            .is_some());
        assert_eq!(snapshot.by_capacity[AG_CLAUDE].quota_exhausted_until, None);
        assert_eq!(snapshot.by_capacity[AG_GPT].quota_exhausted_until, None);

        let third_party_walled = antigravity_account(
            "only",
            &[
                ("gemini-5h", 10.0, "2030-01-01T04:00:00Z"),
                ("3p-weekly", 100.0, "2030-01-03T00:00:00Z"),
            ],
        );
        let snapshot = antigravity_overlay(&[third_party_walled], &facts);
        assert_eq!(
            snapshot.by_capacity[AG_CLAUDE].quota_exhausted_until,
            Some(ms("2030-01-03T00:00:00Z"))
        );
        assert_eq!(snapshot.by_capacity[AG_GEMINI].quota_exhausted_until, None);
        assert_eq!(
            snapshot.by_capacity[AG_GPT].quota_exhausted_until,
            Some(ms("2030-01-03T00:00:00Z"))
        );
        assert_eq!(
            snapshot
                .by_capacity
                .get(AG_ARGON)
                .and_then(|state| state.quota_exhausted_until),
            None,
            "a 3p weekly exhaustion cannot wall Gemini Argon"
        );
        assert!(!snapshot.by_capacity.contains_key(API_GEMINI));
    }

    #[test]
    fn antigravity_exhausted_argon_weekly_walls_argon_but_not_other_gemini() {
        // A paid Pro account: Gemini pool free, separate Argon weekly limit spent.
        let facts = vec![
            fact("google", "antigravity-gemini-4-argon", 50.0),
            fact("google", "antigravity-gemini-3.8-flash", 50.0),
        ];
        let argon_reset = "2030-01-06T00:00:00Z";
        let pro = antigravity_account(
            "pro",
            &[
                ("gemini-5h", 10.0, "2030-01-01T04:00:00Z"),
                ("gemini-weekly", 20.0, "2030-01-05T00:00:00Z"),
                ("gemini-argon-wtu-weekly", 100.0, argon_reset),
            ],
        );
        let snapshot = antigravity_overlay(&[pro.clone()], &facts);
        assert_eq!(
            snapshot.by_capacity[AG_ARGON].quota_exhausted_until,
            Some(ms(argon_reset)),
            "the spent Argon weekly limit walls Argon"
        );
        assert_eq!(
            snapshot.by_capacity[AG_GEMINI].quota_exhausted_until, None,
            "the Argon limit never walls another Gemini route"
        );

        // Argon still counts against the Gemini pool: a spent Gemini 5h window
        // walls it even though its own Argon window is free.
        let gemini_spent = antigravity_account(
            "pro",
            &[
                ("gemini-5h", 100.0, "2030-01-01T01:00:00Z"),
                ("gemini-argon-wtu-weekly", 5.0, argon_reset),
            ],
        );
        let snapshot = antigravity_overlay(&[gemini_spent], &facts);
        assert_eq!(
            snapshot.by_capacity[AG_ARGON].quota_exhausted_until,
            Some(ms("2030-01-01T01:00:00Z"))
        );

        // An Ultra account publishes no Argon window: the Gemini windows alone
        // gate Argon there, and with them free the route stays open.
        let ultra = antigravity_account(
            "ultra",
            &[
                ("gemini-5h", 10.0, "2030-01-01T04:00:00Z"),
                ("gemini-weekly", 20.0, "2030-01-05T00:00:00Z"),
            ],
        );
        let snapshot = antigravity_overlay(&[ultra.clone()], &facts);
        assert_eq!(snapshot.by_capacity[AG_ARGON].quota_exhausted_until, None);
        assert!(snapshot.by_capacity[AG_ARGON].quota_pressure.is_some());
        let snapshot = antigravity_overlay(&[pro, ultra], &facts);
        assert_eq!(
            snapshot.by_capacity[AG_ARGON].quota_exhausted_until, None,
            "the Ultra account keeps Argon open beside a Pro account whose Argon limit is spent"
        );
    }

    #[test]
    fn antigravity_pool_walls_only_when_every_account_is_walled() {
        let facts = antigravity_facts();
        // Live 2026-10-02: one account's 5h pool spent, the other's weekly spent.
        let five_hour_spent = antigravity_account(
            "heidi",
            &[
                ("gemini-weekly", 29.4, "2030-01-05T00:00:00Z"),
                ("gemini-5h", 100.0, "2030-01-01T01:19:00Z"),
            ],
        );
        let weekly_spent =
            antigravity_account("grace", &[("gemini-weekly", 100.0, "2030-01-07T00:00:00Z")]);
        let free = antigravity_account(
            "free",
            &[
                ("gemini-weekly", 5.0, "2030-01-05T00:00:00Z"),
                ("gemini-5h", 5.0, "2030-01-01T04:00:00Z"),
            ],
        );

        let snapshot = antigravity_overlay(
            &[five_hour_spent.clone(), weekly_spent.clone(), free],
            &facts,
        );
        assert_eq!(
            snapshot.by_capacity[AG_GEMINI].quota_exhausted_until, None,
            "one usable account keeps the route open"
        );

        let snapshot = antigravity_overlay(&[five_hour_spent, weekly_spent], &facts);
        assert_eq!(
            snapshot.by_capacity[AG_GEMINI].quota_exhausted_until,
            Some(ms("2030-01-01T01:19:00Z")),
            "every account walled: the route reopens at the earliest account reset"
        );
    }

    // Replaces `antigravity_entry_without_extra_rate_windows_keeps_todays_behaviour`.
    // Extraction used to dispatch on the `google` provider id, which extracted
    // nothing from an Antigravity entry without extra windows. Windows are now
    // read by the reading's shape: without extra windows, the entry is an
    // aggregate reading, and its aggregate walls the routes the join reaches.
    #[test]
    fn an_antigravity_entry_without_extra_rate_windows_is_read_as_its_aggregate() {
        let facts = antigravity_facts();
        let reset = "2030-01-01T01:00:00Z";
        let entry = usage(serde_json::json!({
            "provider": "antigravity",
            "account": "only",
            "usage": { "primary": {
                "usedPercent": 100.0,
                "resetsAt": reset,
                "windowMinutes": 300
            }}
        }));
        let snapshot = antigravity_overlay(&[entry], &facts);
        assert_eq!(
            snapshot.by_capacity[AG_GEMINI].quota_exhausted_until,
            Some(ms(reset)),
            "the aggregate walls the Antigravity routes"
        );
        assert!(
            !snapshot.by_capacity.contains_key(API_GEMINI),
            "the join still reaches only the Antigravity route family"
        );
        assert_ne!(select(facts, snapshot), AG_GEMINI);
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
            ..Default::default()
        };
        merge_quota_overlay_with(
            &mut snapshot,
            &windows,
            &facts,
            NOW_MS,
            &DEFAULT_PACE_CONFIG,
            DEFAULT_HARD_BLOCK_THRESHOLD,
            &model_pools(),
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

    /// One account with a weekly (primary, 10080 min) and a 5-hour (secondary,
    /// 300 min) window, shaped like the live kimi-for-coding readings.
    fn weekly_and_five_hour(
        weekly_used_percent: f64,
        weekly_resets_at: &str,
        five_hour_used_percent: f64,
        five_hour_resets_at: &str,
    ) -> ModelRouteStateSnapshot {
        let kimi_fact = fact("kimi", "kimi-k2-thinking", 50.0);
        let windows = usage(serde_json::json!({
            "provider": "kimi-for-coding",
            "apiProvider": "kimi",
            "account": "plan-global",
            "usage": {
                "primary": {
                    "usedPercent": weekly_used_percent,
                    "resetsAt": weekly_resets_at,
                    "windowMinutes": 10080
                },
                "secondary": {
                    "usedPercent": five_hour_used_percent,
                    "resetsAt": five_hour_resets_at,
                    "windowMinutes": 300
                }
            }
        }));
        overlay(&[windows], std::slice::from_ref(&kimi_fact))
    }

    #[test]
    fn abundance_fold_picks_longest_window_when_both_windows_have_surplus() {
        // Live 2026-09-23 08:54Z shape: 5-hour 65% used with 74 min left
        // (0.35 / (74/300) = 1.4189), weekly 47% used with 1992 min left
        // (0.53 / (1992/10080) = 2.6819). Both above 1.0, so the account reads
        // the LONGEST window: the weekly surplus is what perishes at reset.
        let snapshot =
            weekly_and_five_hour(47.0, "2030-01-02T09:12:00Z", 65.0, "2030-01-01T01:14:00Z");
        let state = &snapshot.by_capacity["kimi/kimi-k2-thinking"];
        assert_near(state.quota_abundance, 2.681_927_710_843_373_5);
        assert_eq!(
            state.quota_abundance_window.as_deref(),
            Some("codexbar:kimi-for-coding:primary"),
            "the weekly, not the 5-hour value of 1.4189, must gate the capacity"
        );
    }

    #[test]
    fn abundance_fold_picks_weekly_when_five_hour_just_reset() {
        // A freshly reset 5-hour slice reads exactly 1.0 (idle window, time
        // fraction clamps to 1) while the weekly sits at 0.99 / 0.3 = 3.3.
        // The 5-hour only paces the weekly's spend, so the account reads 3.3.
        let snapshot =
            weekly_and_five_hour(1.0, "2030-01-03T02:24:00Z", 0.0, "2030-01-01T05:00:00Z");
        let state = &snapshot.by_capacity["kimi/kimi-k2-thinking"];
        assert_near(state.quota_abundance, 3.3);
        assert_eq!(
            state.quota_abundance_window.as_deref(),
            Some("codexbar:kimi-for-coding:primary")
        );
    }

    #[test]
    fn abundance_fold_scarcity_wins_when_five_hour_is_nearly_spent() {
        // 5-hour 90% used with 240 min left: 0.1 / 0.8 = 0.125, below 1.0, so
        // scarcity wins over the weekly's 3.3 surplus.
        let snapshot =
            weekly_and_five_hour(1.0, "2030-01-03T02:24:00Z", 90.0, "2030-01-01T04:00:00Z");
        let state = &snapshot.by_capacity["kimi/kimi-k2-thinking"];
        assert_near(state.quota_abundance, 0.125);
        assert_eq!(
            state.quota_abundance_window.as_deref(),
            Some("codexbar:kimi-for-coding:secondary"),
            "the scarce 5-hour, not the abundant weekly, must gate the capacity"
        );
    }

    #[test]
    fn abundance_fold_weekly_on_pace_is_not_lifted_by_five_hour_surplus() {
        // Weekly exactly on pace reads 1.0 (0.5 / 0.5); the 5-hour reads 3.0
        // (0.75 / 0.25). No window is scarce, and the LONGEST window's value is
        // 1.0 — the fold must not take the highest value.
        let snapshot =
            weekly_and_five_hour(50.0, "2030-01-04T12:00:00Z", 25.0, "2030-01-01T01:15:00Z");
        let state = &snapshot.by_capacity["kimi/kimi-k2-thinking"];
        assert_near(state.quota_abundance, 1.0);
        assert_eq!(
            state.quota_abundance_window.as_deref(),
            Some("codexbar:kimi-for-coding:primary")
        );
    }

    /// Claude-shaped account: primary is the 5-hour (300 min), secondary the
    /// weekly (10080 min) — the reverse slot assignment of kimi-for-coding.
    fn claude_five_hour_and_weekly(
        five_hour_used_percent: f64,
        five_hour_resets_at: &str,
        weekly_used_percent: f64,
        weekly_resets_at: &str,
    ) -> ModelRouteStateSnapshot {
        let claude_fact = fact("anthropic", "claude", 50.0);
        let windows = usage(serde_json::json!({
            "provider": "claude",
            "apiProvider": "anthropic",
            "account": "seat-one",
            "usage": {
                "primary": {
                    "usedPercent": five_hour_used_percent,
                    "resetsAt": five_hour_resets_at,
                    "windowMinutes": 300
                },
                "secondary": {
                    "usedPercent": weekly_used_percent,
                    "resetsAt": weekly_resets_at,
                    "windowMinutes": 10080
                }
            }
        }));
        overlay(&[windows], std::slice::from_ref(&claude_fact))
    }

    #[test]
    fn abundance_fold_longest_window_branch_is_provider_agnostic() {
        // Live 2026-09-23 08:54Z anthropic readings, verbatim: 5-hour 2% used
        // resetting in 96 min (0.98 / 0.32 = 3.0625), weekly 91% used resetting
        // in 306 min (0.09 / (306/10080) = 2.9647). The weekly is 91% used but
        // resets the same afternoon, so its leftover would perish at reset:
        // this is SURPLUS, not scarcity, and the fold takes the longest-window
        // branch on a non-Kimi account.
        let snapshot =
            claude_five_hour_and_weekly(2.0, "2030-01-01T01:36:00Z", 91.0, "2030-01-01T05:06:00Z");
        let state = &snapshot.by_capacity["anthropic/claude"];
        assert_near(state.quota_abundance, 2.964_705_882_352_941_1);
        assert_eq!(
            state.quota_abundance_window.as_deref(),
            Some("codexbar:claude:secondary")
        );
    }

    #[test]
    fn abundance_fold_scarcity_branch_is_provider_agnostic() {
        // Same Claude shape, but the weekly resets the next afternoon (1746 min
        // left): 0.09 / (1746/10080) = 0.5196, below 1.0, so scarcity wins over
        // the 5-hour's 3.0625 surplus on a non-Kimi account.
        let snapshot =
            claude_five_hour_and_weekly(2.0, "2030-01-01T01:36:00Z", 91.0, "2030-01-02T05:06:00Z");
        let state = &snapshot.by_capacity["anthropic/claude"];
        assert_near(state.quota_abundance, 0.519_587_628_865_979_4);
        assert_eq!(
            state.quota_abundance_window.as_deref(),
            Some("codexbar:claude:secondary")
        );
    }
}
