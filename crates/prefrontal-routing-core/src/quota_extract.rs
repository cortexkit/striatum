//! Per-provider quota window EXTRACTION. Ported VERBATIM from the TS
//! `quota/codexbar-window-extractors.ts`.
//!
//! `insula` (formerly `ai-provider-quota`) returns a uniform `ProviderUsage` shape, but providers
//! expose quota with DIFFERENT topologies. Most (codex/grok/opencode-go/ollama)
//! report one account-wide pool in primary/secondary/tertiary → one account-scoped
//! window each. Antigravity (Google AI Ultra) bundles MULTIPLE independent pools
//! under one account: its aggregate `primary`=100% reflects EXHAUSTED external
//! models (Claude, GPT-OSS), while the Geminis we route to sit in
//! `extra_rate_windows[]` at low utilization, so the aggregate must be IGNORED and
//! the scoped windows mapped to specific capacity keys instead. Antigravity's
//! scoped windows are POOLS (`gemini-5h`, `3p-weekly`, ...) shared by a model
//! family, mapped through `ANTIGRAVITY_POOL_BY_MODEL_FAMILY`.

use crate::quota::{ExtraWindow, ProviderUsage, RateWindow};
use crate::quota_pace::ProviderWindowModel;

/// The routing scope a window applies to. `account` pressures every route of the
/// provider; `capacity_key` pressures exactly one provider/model. (`model_family`
/// and `route_key` exist in the TS scope set for native descriptors that are dead
/// under the subc source; kept for fidelity, not emitted by the live extractors.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowScopeKind {
    Account,
    ModelFamily,
    CapacityKey,
    RouteKey,
    /// One Antigravity quota pool (`applies_key` is the pool name, e.g. `gemini`):
    /// it applies to every routed capacity whose model draws on that pool.
    AntigravityPool,
}

/// A normalized window an extractor emits; the overlay runs pace + worst-wins on it.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedWindow {
    pub window_key: String,
    pub scope_kind: WindowScopeKind,
    /// capacity key when scope is CapacityKey / family key for ModelFamily /
    /// pool name for AntigravityPool.
    pub applies_key: Option<String>,
    /// Effective utilization, normalized to 0..1.
    pub utilization: f64,
    /// Unadjusted producer utilization in percent. Missing means a burn interval
    /// cannot be verified; it must never be reconstructed from `utilization`.
    pub raw_used_percent: Option<f64>,
    /// ms epoch; `None` when the provider did not report a recovery time.
    pub resets_at: Option<f64>,
    /// `None` when the provider does not report a window length.
    pub window_ms: Option<f64>,
    /// Explicit provider semantics, if the producer has started reporting them.
    pub window_model: Option<ProviderWindowModel>,
    pub limit_reached: bool,
}

const WINDOW_SLOTS: [&str; 3] = ["primary", "secondary", "tertiary"];

/// Parse an RFC3339/ISO-8601 timestamp to ms epoch, mirroring JS `Date.parse`.
/// Returns `None` when the provider omitted or malformed the recovery time.
/// Ms epoch of the source's observation time, when the producer published one.
pub fn fetched_at_ms(entry: &ProviderUsage) -> Option<u64> {
    entry
        .fetched_at
        .as_deref()
        .and_then(chrono_parse_rfc3339)
        .filter(|ms| ms.is_finite() && *ms >= 0.0)
        .map(|ms| ms as u64)
}

fn parse_resets_at(s: &str) -> Option<f64> {
    // The wire emits RFC3339 (what the quota module serializes); chrono via the
    // `time`-free path isn't a dep here, so parse the common shape directly.
    // Accept a trailing 'Z' or a numeric offset.
    chrono_parse_rfc3339(s)
}

fn rate_window_to_extracted(
    w: Option<&RateWindow>,
    window_key: String,
    scope_kind: WindowScopeKind,
    applies_key: Option<String>,
) -> Option<ExtractedWindow> {
    let w = w?;
    // If the reported percent is NaN or infinite, skip this window: it is not
    // a measurement. Clamping NaN would make the window look unused and
    // clamping infinity would make it look fully used. Skipping matches how an
    // unhealthy entry is treated: it contributes no signal either way.
    if !w.used_percent.is_finite() {
        return None;
    }
    let utilization = (w.used_percent / 100.0).clamp(0.0, 1.0);
    // A garbled timestamp must not hide a burning window: the window keeps its
    // utilization and is paced through the no-recovery-time fallback.
    let resets_at = w.resets_at.as_deref().and_then(parse_resets_at);
    // An idle window used to return None here, on the reasoning that it carries no
    // pressure worth pacing. It carries something else: THIS ACCOUNT IS FREE. The
    // across-account fold walls a capacity only when every account holding a signal
    // for it is walled, and an account with no signal holds none - so beside one
    // exhausted sibling, a completely idle account vanished and the whole provider
    // was walled until the exhausted account's reset. The selector then returned a
    // healthy but more expensive route with no error anywhere (Loop R138). A
    // zero-use window is emitted instead: zero pressure, unwalled, and PRESENT,
    // which is what keeps its account in the population.
    Some(ExtractedWindow {
        window_key,
        scope_kind,
        applies_key,
        utilization,
        raw_used_percent: w.raw_used_percent,
        resets_at,
        window_ms: w.window_minutes.map(|m| m as f64 * 60_000.0),
        window_model: w.window_model,
        limit_reached: false,
    })
}

/// Default: a single account-wide pool. primary/secondary/tertiary → one
/// account-scoped window each (applies to every route of the provider).
fn aggregate_account_extractor(
    entry: &ProviderUsage,
    cb_provider_name: &str,
) -> Vec<ExtractedWindow> {
    let Some(usage) = entry.usage.as_ref() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for slot in WINDOW_SLOTS {
        let win = match slot {
            "primary" => usage.primary.as_ref(),
            "secondary" => usage.secondary.as_ref(),
            _ => usage.tertiary.as_ref(),
        };
        if let Some(extracted) = rate_window_to_extracted(
            win,
            format!("codexbar:{cb_provider_name}:{slot}"),
            WindowScopeKind::Account,
            None,
        ) {
            out.push(extracted);
        }
    }
    out
}

/// Tokenize a model id or window title into comparable lowercase tokens.
fn tokenize(value: &str) -> Vec<String> {
    value
        .to_lowercase()
        .replace(['(', ')'], " ")
        .split(|c: char| c.is_whitespace() || matches!(c, '.' | '/' | '_' | '-'))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// True when `seq` appears as a contiguous, in-order subsequence of `tokens`.
fn contains_ordered_subsequence(tokens: &[String], seq: &[String]) -> bool {
    if seq.is_empty() || seq.len() > tokens.len() {
        return false;
    }
    for i in 0..=(tokens.len() - seq.len()) {
        if tokens[i..i + seq.len()] == *seq {
            return true;
        }
    }
    false
}

/// Match a window title (e.g. "Gemini 3.5 Flash (High)") to one of the provider's
/// capacity keys (e.g. "google/antigravity-gemini-3.5-flash"). The capacity key's
/// model tokens must appear as a CONTIGUOUS, IN-ORDER subsequence of the title
/// tokens (after dropping the provider-alias token). Most-specific (most tokens)
/// wins; a tie is AMBIGUOUS → `None` (skip, never pick by iteration order).
pub fn match_capacity_key_by_title(
    title: &str,
    capacity_keys: &[String],
    cb_provider_name: &str,
) -> Option<String> {
    let title_tokens = tokenize(title);
    let mut best: Option<(String, usize)> = None;
    let mut tied_at_best = false;
    for key in capacity_keys {
        let model_id = match key.find('/') {
            Some(i) => &key[i + 1..],
            None => key.as_str(),
        };
        let cap_tokens: Vec<String> = tokenize(model_id)
            .into_iter()
            .filter(|t| t != cb_provider_name)
            .collect();
        if cap_tokens.is_empty() {
            continue;
        }
        if !contains_ordered_subsequence(&title_tokens, &cap_tokens) {
            continue;
        }
        match &best {
            None => {
                best = Some((key.clone(), cap_tokens.len()));
                tied_at_best = false;
            }
            Some((best_key, best_tokens)) => {
                if cap_tokens.len() > *best_tokens {
                    best = Some((key.clone(), cap_tokens.len()));
                    tied_at_best = false;
                } else if cap_tokens.len() == *best_tokens && key != best_key {
                    tied_at_best = true;
                }
            }
        }
    }
    if tied_at_best {
        return None;
    }
    best.map(|(key, _)| key)
}

fn slug(s: &str) -> String {
    let joined = tokenize(s).join("-");
    if joined.is_empty() {
        "window".to_string()
    } else {
        joined
    }
}

/// Routing model ids served through Antigravity start with this prefix
/// (`google/antigravity-gemini-3.8-flash`, `google/antigravity-claude-opus-...`).
const ANTIGRAVITY_MODEL_PREFIX: &str = "antigravity-";

/// Which Antigravity quota pool each routed model family draws on, keyed by the
/// family token right after `antigravity-` in the routing model id.
///
/// Source: QTA (Insula's owner), read live 2026-10-02. Antigravity publishes no
/// primary window; every pool is an `extraRateWindows` entry with one of the ids
/// in `ANTIGRAVITY_POOL_WINDOW_IDS`: `gemini-5h`, `gemini-weekly`, `3p-5h`,
/// `3p-weekly`. Gemini
/// models use the `gemini` pool; third-party models served through Antigravity
/// (Claude, GPT) use the `3p` pool. A model whose family is not listed here gets
/// no pool signal rather than a guessed one.
pub const ANTIGRAVITY_POOL_BY_MODEL_FAMILY: &[(&str, &str)] =
    &[("gemini", "gemini"), ("claude", "3p"), ("gpt", "3p")];

/// The Antigravity pool a routing capacity key draws on, if it is an
/// Antigravity-served model of a known family.
pub fn antigravity_pool_for_capacity(capacity_key: &str) -> Option<&'static str> {
    let model_id = capacity_key
        .split_once('/')
        .map_or(capacity_key, |(_, model)| model);
    let family = model_id
        .strip_prefix(ANTIGRAVITY_MODEL_PREFIX)?
        .split('-')
        .next()?;
    ANTIGRAVITY_POOL_BY_MODEL_FAMILY
        .iter()
        .find(|(known_family, _)| *known_family == family)
        .map(|(_, pool)| *pool)
}

/// A product route requiring pooled account quota, even when routing does not
/// yet know its model family's pool. Unknown families must not fall back to a
/// different product's account-wide windows.
pub fn requires_account_pool(capacity_key: &str) -> bool {
    capacity_key
        .split_once('/')
        .map_or(capacity_key, |(_, model)| model)
        .starts_with(ANTIGRAVITY_MODEL_PREFIX)
}

/// The routed capacities that draw on one Antigravity pool.
pub fn antigravity_pool_capacities(pool: &str, capacity_keys: &[String]) -> Vec<String> {
    capacity_keys
        .iter()
        .filter(|key| antigravity_pool_for_capacity(key) == Some(pool))
        .cloned()
        .collect()
}

/// The period of an Antigravity pool window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolPeriod {
    FiveHour,
    Weekly,
}

impl PoolPeriod {
    /// Insula's `windowKind` vocabulary.
    fn from_window_kind(kind: &str) -> Option<Self> {
        match kind {
            "five_hour" => Some(Self::FiveHour),
            "weekly" => Some(Self::Weekly),
            _ => None,
        }
    }

    fn window_ms(self) -> f64 {
        let minutes = match self {
            Self::FiveHour => 300.0,
            Self::Weekly => 10_080.0,
        };
        minutes * 60_000.0
    }
}

/// The Antigravity pool window ids, each with the pool it meters and its period.
/// Only these exact ids are pool windows. Antigravity also publishes separately
/// metered allowances under other ids, some starting with `gemini-` (QTA: paid
/// Pro accounts get a weekly Gemini 4 Argon quota); reading one of those as the
/// `gemini` pool would let its exhaustion wall every Gemini route.
const ANTIGRAVITY_POOL_WINDOW_IDS: &[(&str, &str, PoolPeriod)] = &[
    ("gemini-5h", "gemini", PoolPeriod::FiveHour),
    ("gemini-weekly", "gemini", PoolPeriod::Weekly),
    ("3p-5h", "3p", PoolPeriod::FiveHour),
    ("3p-weekly", "3p", PoolPeriod::Weekly),
];

/// Recognize an Antigravity pool window by its exact id. The id fixes both the
/// pool and the period. A `windowKind` we know must name the same period; when it
/// names another, the producer and this table disagree about what the window
/// meters, so it is not read as a pool window at all. An unknown `windowKind`
/// leaves the id's period in place. Any other id is not a pool window: an
/// unknown bucket is never guessed into a pool, since one wrong guess merges two
/// independent meters. A window rejected here is handled as an ordinary extra
/// window: extraction keeps it only if its title matches a routed capacity, and
/// the routing module's account status still lists it, so a new bucket stays
/// visible.
fn antigravity_pool_window(item: &ExtraWindow) -> Option<(&'static str, PoolPeriod)> {
    let id = item.id.as_deref()?;
    let (_, pool, period) = ANTIGRAVITY_POOL_WINDOW_IDS
        .iter()
        .find(|(known_id, _, _)| *known_id == id)?;
    let window_kind = item.window_kind.as_deref().or_else(|| {
        item.window
            .as_ref()
            .and_then(|window| window.window_kind.as_deref())
    });
    match window_kind.and_then(PoolPeriod::from_window_kind) {
        Some(declared) if declared != *period => None,
        _ => Some((pool, *period)),
    }
}

/// Account pools use the same identities as route walls. With a capacity key,
/// include only the pools that model draws on; without one, expose all pools
/// for health. A pooled reading's aggregate slots are not authoritative.
pub fn account_rate_windows(
    usage: &crate::quota::Usage,
    capacity_key: Option<&str>,
) -> Vec<(&'static str, RateWindow)> {
    let pools: Vec<_> = usage
        .extra_rate_windows
        .iter()
        .flatten()
        .filter_map(|extra| {
            let (pool, period) = antigravity_pool_window(extra)?;
            Some((extra, pool, period))
        })
        .collect();
    // A route that draws on a named pool cannot borrow aggregate windows from
    // another product of the same catalog vendor (for example Gemini CLI).
    if !pools.is_empty() || capacity_key.is_some_and(requires_account_pool) {
        return pools
            .into_iter()
            .filter(|(_, pool, _)| {
                capacity_key.is_none_or(|key| antigravity_pool_for_capacity(key) == Some(*pool))
            })
            .filter_map(|(extra, _, period)| {
                let mut window = extra.window.clone()?;
                window.window_kind = extra.window_kind.clone().or(window.window_kind);
                window.window_minutes = window
                    .window_minutes
                    .or(Some((period.window_ms() / 60_000.0) as i64));
                Some(("extra", window))
            })
            .collect();
    }
    [
        ("primary", &usage.primary),
        ("secondary", &usage.secondary),
        ("tertiary", &usage.tertiary),
    ]
    .into_iter()
    .filter_map(|(slot, window)| window.as_ref().map(|window| (slot, window.clone())))
    .collect()
}

pub fn is_account_pool_window(extra: &ExtraWindow) -> bool {
    antigravity_pool_window(extra).is_some()
}

/// A reading that carries `extra_rate_windows` (Antigravity, for one): its
/// aggregate windows are ignored, because a pooled account's aggregate reflects
/// pools the routed models may not draw on. Pool windows (`gemini-5h`,
/// `3p-weekly`, ...) are emitted pool-scoped, so they pressure and wall exactly
/// the routed models that draw on that pool. Any other window is mapped by
/// title to its matching capacity key, capacity-key-scoped; unmatched titles
/// (models we don't route to) are skipped.
fn extra_window_extractor(
    extra: &[ExtraWindow],
    cb_provider_name: &str,
    capacity_keys: &[String],
) -> Vec<ExtractedWindow> {
    let mut out = Vec::new();
    for item in extra {
        if let (Some((pool, period)), Some(window), Some(id)) = (
            antigravity_pool_window(item),
            item.window.as_ref(),
            item.id.as_deref(),
        ) {
            if let Some(mut extracted) = rate_window_to_extracted(
                Some(window),
                format!("codexbar:{cb_provider_name}:{id}"),
                WindowScopeKind::AntigravityPool,
                Some(pool.to_string()),
            ) {
                // The period names the window length when the producer did
                // not publish `windowMinutes` itself.
                extracted.window_ms = extracted.window_ms.or(Some(period.window_ms()));
                out.push(extracted);
            }
            continue;
        }
        let (Some(title), Some(window)) = (item.title.as_ref(), item.window.as_ref()) else {
            continue;
        };
        let Some(capacity_key) =
            match_capacity_key_by_title(title, capacity_keys, cb_provider_name)
        else {
            continue;
        };
        let window_key = format!(
            "codexbar:{cb_provider_name}:{}",
            item.id.clone().unwrap_or_else(|| slug(title))
        );
        if let Some(extracted) = rate_window_to_extracted(
            Some(window),
            window_key,
            WindowScopeKind::CapacityKey,
            Some(capacity_key),
        ) {
            out.push(extracted);
        }
    }
    out
}

/// Extract the windows for one provider-usage entry by the reading's shape,
/// never by provider id: a reading carrying a non-empty `extra_rate_windows`
/// list yields its pool and per-model windows, and any other reading its
/// aggregate windows. So the same reading body yields the same windows under
/// any provider id.
pub fn extract_windows(
    entry: &ProviderUsage,
    cb_provider_name: &str,
    capacity_keys: &[String],
) -> Vec<ExtractedWindow> {
    match entry
        .usage
        .as_ref()
        .and_then(|usage| usage.extra_rate_windows.as_deref())
    {
        Some(extra) if !extra.is_empty() => {
            extra_window_extractor(extra, cb_provider_name, capacity_keys)
        }
        _ => aggregate_account_extractor(entry, cb_provider_name),
    }
}

// ── minimal RFC3339 parse (no chrono dep in -core) ─────────────────────────
// Parses "YYYY-MM-DDTHH:MM:SS(.fff)?(Z|+HH:MM|-HH:MM)" to ms epoch. Returns None on
// any shape it cannot parse (the window is then skipped, matching JS Date.parse →
// NaN → skip).
pub(crate) fn chrono_parse_rfc3339(s: &str) -> Option<f64> {
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: i64 = s.get(5..7)?.parse().ok()?;
    let day: i64 = s.get(8..10)?.parse().ok()?;
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    let min: i64 = s.get(14..16)?.parse().ok()?;
    let sec: i64 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Optional fractional seconds + timezone, starting at index 19.
    let rest = &s[19..];
    let mut millis_frac: i64 = 0;
    let mut idx = 0usize;
    let rest_bytes = rest.as_bytes();
    if rest_bytes.first() == Some(&b'.') {
        idx = 1;
        let mut frac = String::new();
        while idx < rest_bytes.len() && rest_bytes[idx].is_ascii_digit() {
            frac.push(rest_bytes[idx] as char);
            idx += 1;
        }
        // Take the first 3 digits as ms.
        let frac3: String = frac.chars().take(3).collect();
        let padded = format!("{frac3:0<3}");
        millis_frac = padded.parse().unwrap_or(0);
    }
    // Timezone offset in minutes.
    let tz_min: i64 = match rest_bytes.get(idx) {
        Some(b'Z') | None => 0,
        Some(b'+') | Some(b'-') => {
            let sign = if rest_bytes[idx] == b'-' { -1 } else { 1 };
            let off = &rest[idx + 1..];
            let oh: i64 = off.get(0..2)?.parse().ok()?;
            let om: i64 = off.get(3..5).and_then(|v| v.parse().ok()).unwrap_or(0);
            sign * (oh * 60 + om)
        }
        _ => return None,
    };

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3600 + min * 60 + sec - tz_min * 60;
    Some((secs * 1000 + millis_frac) as f64)
}

/// Days since 1970-01-01 (Howard Hinnant's civil-from-days, inverse). Valid for the
/// Gregorian calendar; sufficient for quota reset timestamps.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::{ExtraWindow, Usage};

    fn win(used: f64, resets_at: &str, mins: Option<i64>) -> RateWindow {
        RateWindow {
            used_percent: used,
            raw_used_percent: Some(used),
            resets_at: Some(resets_at.into()),
            window_minutes: mins,
            window_model: None,
            window_kind: None,
        }
    }

    #[test]
    fn rfc3339_parses_z_and_offset() {
        // 2026-01-01T00:00:00Z = 1767225600000 ms.
        let z = chrono_parse_rfc3339("2026-01-01T00:00:00Z").unwrap();
        assert_eq!(z, 1_767_225_600_000.0);
        // Same instant via +00:00 and a fractional second.
        let frac = chrono_parse_rfc3339("2026-01-01T00:00:00.500Z").unwrap();
        assert_eq!(frac, 1_767_225_600_500.0);
        // A +02:00 offset is 2h earlier in UTC.
        let off = chrono_parse_rfc3339("2026-01-01T02:00:00+02:00").unwrap();
        assert_eq!(off, z);
        assert!(chrono_parse_rfc3339("not-a-date").is_none());
    }

    #[test]
    fn aggregate_extractor_emits_account_windows() {
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "codex".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(win(40.0, "2026-01-01T00:00:00Z", Some(300))),
                secondary: Some(win(10.0, "2026-01-08T00:00:00Z", Some(10080))),
                tertiary: None,
                extra_rate_windows: None,
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };
        let out = extract_windows(&entry, "codex", &[]);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|w| w.scope_kind == WindowScopeKind::Account));
        assert_eq!(out[0].window_key, "codexbar:codex:primary");
        assert_eq!(out[0].utilization, 0.4);
        assert_eq!(out[0].window_ms, Some(300.0 * 60_000.0));
    }

    #[test]
    fn exhausted_no_reset_window_is_emitted_without_fabricating_recovery() {
        // Hand-build this fixture: live wire sampling cannot generate it today,
        // which is exactly the trap this regression test must avoid. QTA permits
        // an exhausted weekly window to omit resetsAt.
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "ollama".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(RateWindow {
                    used_percent: 100.0,
                    raw_used_percent: Some(100.0),
                    resets_at: None,
                    window_minutes: Some(10_080),
                    window_model: None,
                    window_kind: None,
                }),
                secondary: None,
                tertiary: None,
                extra_rate_windows: None,
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };

        let out = extract_windows(&entry, "ollama", &[]);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].utilization, 1.0);
        assert_eq!(out[0].resets_at, None);
    }

    // Renamed from `idle_no_reset_window_stays_skipped`. Its intent was right and
    // its assertion was one level too strong: an idle window must contribute no
    // PRESSURE and no wall, which is what the original comment argued for. It
    // asserted the window was not emitted AT ALL, and that is what lost the
    // account from the across-account fold, letting one exhausted sibling wall a
    // provider whose other account was completely free (Loop R138).
    #[test]
    fn idle_no_reset_window_is_emitted_unwalled_rather_than_dropped() {
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "ollama".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(RateWindow {
                    used_percent: 0.0,
                    raw_used_percent: Some(0.0),
                    resets_at: None,
                    window_minutes: Some(10_080),
                    window_model: None,
                    window_kind: None,
                }),
                secondary: None,
                tertiary: None,
                extra_rate_windows: None,
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };

        let out = extract_windows(&entry, "ollama", &[]);
        assert_eq!(out.len(), 1, "the account must stay represented");
        assert_eq!(out[0].utilization, 0.0, "idle is zero pressure");
        assert_eq!(out[0].resets_at, None, "and no recovery time is fabricated");
        assert!(!out[0].limit_reached, "an idle window is not a wall");
    }

    #[test]
    fn malformed_reset_does_not_hide_burning_window() {
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "ollama".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(win(40.0, "not-a-timestamp", Some(300))),
                secondary: None,
                tertiary: None,
                extra_rate_windows: None,
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };

        let out = extract_windows(&entry, "ollama", &[]);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].utilization, 0.4);
        assert_eq!(out[0].resets_at, None);
    }

    #[test]
    fn non_finite_used_percent_emits_no_window() {
        let window = |used: f64| RateWindow {
            used_percent: used,
            raw_used_percent: None,
            resets_at: Some("2026-01-01T00:00:00Z".into()),
            window_minutes: Some(300),
            window_model: None,
            window_kind: None,
        };
        for used in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let extracted = rate_window_to_extracted(
                Some(&window(used)),
                "codexbar:codex:primary".into(),
                WindowScopeKind::Account,
                None,
            );
            assert_eq!(extracted, None, "used_percent {used} is not a measurement");
        }
    }

    #[test]
    fn antigravity_maps_extra_windows_to_capacity_keys_and_skips_unmatched() {
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "antigravity".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                // Misleading aggregate: must be IGNORED.
                primary: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                secondary: None,
                tertiary: None,
                extra_rate_windows: Some(vec![
                    ExtraWindow {
                        title: Some("Gemini 3.5 Flash (High)".into()),
                        id: Some("gemini-flash".into()),
                        window: Some(win(20.0, "2026-01-01T00:00:00Z", None)),
                        window_kind: None,
                    },
                    // GPT-OSS is not a routed capacity key → skipped.
                    ExtraWindow {
                        title: Some("GPT-OSS 120B".into()),
                        id: None,
                        window: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                        window_kind: None,
                    },
                ]),
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };
        let caps = vec![
            "google/antigravity-gemini-3.5-flash".to_string(),
            "google/antigravity-gemini-3.1-pro".to_string(),
        ];
        let out = extract_windows(&entry, "antigravity", &caps);
        // Only the matched Gemini Flash window: the aggregate + GPT-OSS are dropped.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].scope_kind, WindowScopeKind::CapacityKey);
        assert_eq!(
            out[0].applies_key.as_deref(),
            Some("google/antigravity-gemini-3.5-flash")
        );
        assert_eq!(out[0].utilization, 0.2);
    }

    #[test]
    fn antigravity_walled_external_pool_never_reads_as_provider_exhaustion() {
        // QTA's headless antigravity lane (2026-08-06) serves POOLED windows:
        // "Claude and GPT models" is a genuinely separate meter for models we
        // route through OTHER providers, so a 100%-exhausted external pool must
        // not pressure or wall any google/antigravity-* capacity. String
        // ownership is SPLIT BY LANE (QTA's correction, same day): the CLOUD
        // lane's pool ids ("Gemini Models", "Claude and GPT models", "Other
        // models") are QTA-owned and QTA flags changes before they ship - but
        // even there match on ID, never title: cloud titles carry a moving
        // model count ("Gemini Models (17 models)"). The LOCAL lane
        // republishes UPSTREAM strings (bucket ids like "3p-5h", titles like
        // "Claude and GPT models Five Hour Limit") that QTA can only observe;
        // newly observed bucket ids get reported to us on a channel, not just
        // logged. The same account switches lanes with an
        // unrelated editor process's lifecycle, so BOTH shapes are live inputs
        // and the vectors below cover both. The protection's real polarity is
        // POSITIVE matching - only windows matching a routed capacity key
        // contribute - which is why the dangerous local form ("3p-5h", sharing
        // no token with claude, gpt, OR gemini) drops correctly: never widen
        // this matcher to sweep unmatched windows in by prefix or pool them by
        // heuristic; "g-*" native and "3p-*" external are one such heuristic
        // away from merging. Today the
        // title-token matcher skips it because no capacity key matches - this
        // pins that behavior so a future matcher extension (e.g. loosening the
        // contiguous-subsequence rule) cannot silently convert a walled
        // external pool into a provider-wide exhaustion signal. The Gemini
        // pool alongside it must still extract, proving the skip is selective
        // rather than the whole entry being dropped.
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "antigravity".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: None,
                secondary: None,
                tertiary: None,
                extra_rate_windows: Some(vec![
                    ExtraWindow {
                        title: Some("Claude and GPT models".into()),
                        id: None,
                        window: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                        window_kind: None,
                    },
                    ExtraWindow {
                        title: Some("Gemini 3.6 Flash".into()),
                        id: Some("gemini-flash".into()),
                        window: Some(win(40.0, "2026-01-01T00:00:00Z", None)),
                        window_kind: None,
                    },
                ]),
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };
        let caps = vec!["google/antigravity-gemini-3.6-flash".to_string()];
        let out = extract_windows(&entry, "antigravity", &caps);
        assert_eq!(out.len(), 1, "external pool skipped, gemini pool kept");
        assert_eq!(
            out[0].applies_key.as_deref(),
            Some("google/antigravity-gemini-3.6-flash")
        );
        assert!(
            (out[0].utilization - 0.4).abs() < 1e-9,
            "the surviving window is the gemini pool, not the walled external one"
        );
    }

    #[test]
    fn gemini_wire_entries_never_pressure_antigravity_capacities() {
        // QTA's cross-product hazard, measured from their deployed wire
        // (2026-08-06): gemini-the-product publishes per-model ids that are
        // BYTE-IDENTICAL to antigravity's upstream model ids (both are Google
        // Code Assist, shared catalogue) - four collisions live today, kept
        // off the wire only because antigravity pools before publishing. Both
        // products resolve to the "google" providerID under apiProvider
        // keying, so gemini entries now REACH this extractor (they never did
        // under the wire-name map). They must still extract nothing against
        // our capacities: every routed google capacity is an ANTIGRAVITY
        // product (antigravity-gemini-*), and gemini-product quota describes a
        // DIFFERENT pool under different credentials. The vendor-segment
        // prefix in the capacity key is what keeps the products apart in the
        // title matcher; this vector pins that separation so a key rename or
        // matcher loosening cannot silently let one product's exhaustion wall
        // the other's. THE TWO FENCES ARE NOT REDUNDANT (QTA): their
        // (provider, id) keying prevents the collision ON THE WIRE, this
        // vendor segment prevents it IN EXTRACTION - either alone leaves the
        // other side able to fuse two products, so seeing one fence must not
        // be read as the hazard being handled.
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: Some("google".into()),
            provider: "gemini".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: None,
                secondary: None,
                tertiary: None,
                extra_rate_windows: Some(vec![
                    ExtraWindow {
                        title: Some("gemini-2.5-flash".into()),
                        id: Some("gemini-2.5-flash".into()),
                        window: Some(win(100.0, "2026-01-01T00:00:00Z", Some(1440))),
                        window_kind: None,
                    },
                    ExtraWindow {
                        title: Some("gemini-3.1-flash-lite".into()),
                        id: Some("gemini-3.1-flash-lite".into()),
                        window: Some(win(100.0, "2026-01-01T00:00:00Z", Some(1440))),
                        window_kind: None,
                    },
                ]),
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };
        let caps = vec![
            "google/antigravity-gemini-3.1-pro".to_string(),
            "google/antigravity-gemini-3.5-flash".to_string(),
            "google/antigravity-gemini-3.6-flash".to_string(),
        ];
        let out = extract_windows(&entry, "gemini", &caps);
        assert_eq!(
            out.len(),
            0,
            "gemini-product windows must not pressure antigravity-product capacities"
        );
    }

    #[test]
    fn antigravity_local_lane_pools_reach_only_their_own_family() {
        // The LOCAL lane's shapes: upstream bucket ids ("3p-5h") and composed
        // titles ("Claude and GPT models Five Hour Limit"). Antigravity has
        // published pooled windows on BOTH lanes since it shipped (QTA), so the
        // per-model title matcher never extracted anything from an antigravity
        // entry and routing walked into exhausted pools blind. Pools are now
        // read by id through ANTIGRAVITY_POOL_BY_MODEL_FAMILY. The hazard this
        // vector used to pin still holds, in its positive form: the walled
        // external "3p" pool must reach no Gemini capacity, only models that
        // draw on "3p" (Claude, GPT through Antigravity).
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "antigravity".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: None,
                secondary: None,
                tertiary: None,
                extra_rate_windows: Some(vec![
                    ExtraWindow {
                        title: Some("Claude and GPT models Five Hour Limit".into()),
                        id: Some("3p-5h".into()),
                        window: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                        window_kind: None,
                    },
                    ExtraWindow {
                        title: Some("Claude and GPT models Weekly Limit".into()),
                        id: Some("3p-weekly".into()),
                        window: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                        window_kind: None,
                    },
                    ExtraWindow {
                        title: Some("Gemini Models Five Hour Limit".into()),
                        id: Some("gemini-5h".into()),
                        window: Some(win(25.0, "2026-01-01T00:00:00Z", None)),
                        window_kind: None,
                    },
                ]),
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };
        let caps = vec![
            "google/antigravity-gemini-3.6-flash".to_string(),
            "google/antigravity-claude-opus-4-6-thinking".to_string(),
            // The Gemini API product shares the provider id; no pool applies.
            "google/gemini-3.6-flash".to_string(),
        ];
        let out = extract_windows(&entry, "antigravity", &caps);
        let pools: Vec<(&str, Option<&str>, Option<f64>)> = out
            .iter()
            .map(|w| {
                assert_eq!(w.scope_kind, WindowScopeKind::AntigravityPool);
                (w.window_key.as_str(), w.applies_key.as_deref(), w.window_ms)
            })
            .collect();
        assert_eq!(
            pools,
            vec![
                (
                    "codexbar:antigravity:3p-5h",
                    Some("3p"),
                    Some(300.0 * 60_000.0)
                ),
                (
                    "codexbar:antigravity:3p-weekly",
                    Some("3p"),
                    Some(10_080.0 * 60_000.0)
                ),
                (
                    "codexbar:antigravity:gemini-5h",
                    Some("gemini"),
                    Some(300.0 * 60_000.0)
                ),
            ],
            "the id suffix names the period when windowMinutes is absent"
        );
        assert_eq!(
            antigravity_pool_capacities("3p", &caps),
            vec!["google/antigravity-claude-opus-4-6-thinking".to_string()],
            "the external pool reaches Claude-through-Antigravity only"
        );
        assert_eq!(
            antigravity_pool_capacities("gemini", &caps),
            vec!["google/antigravity-gemini-3.6-flash".to_string()],
            "the Gemini pool reaches Antigravity Gemini only, never the Gemini API product"
        );
    }

    #[test]
    fn antigravity_pool_ids_are_exact_and_a_known_window_kind_must_agree() {
        // The id alone names the pool and the period. A known windowKind that
        // names the other period means the producer and this table disagree
        // about what the window meters, so it is no pool window at all rather
        // than a guess in either direction.
        let item = |id: &str, kind: Option<&str>| ExtraWindow {
            title: None,
            id: Some(id.into()),
            window: Some(win(10.0, "2026-01-01T00:00:00Z", None)),
            window_kind: kind.map(Into::into),
        };
        let expected = [
            (
                "gemini-5h",
                "gemini",
                PoolPeriod::FiveHour,
                "five_hour",
                "weekly",
            ),
            (
                "gemini-weekly",
                "gemini",
                PoolPeriod::Weekly,
                "weekly",
                "five_hour",
            ),
            ("3p-5h", "3p", PoolPeriod::FiveHour, "five_hour", "weekly"),
            ("3p-weekly", "3p", PoolPeriod::Weekly, "weekly", "five_hour"),
        ];
        for (id, pool, period, agreeing, contradicting) in expected {
            assert_eq!(
                antigravity_pool_window(&item(id, None)),
                Some((pool, period))
            );
            assert_eq!(
                antigravity_pool_window(&item(id, Some(agreeing))),
                Some((pool, period))
            );
            assert_eq!(
                antigravity_pool_window(&item(id, Some(contradicting))),
                None,
                "{id} with windowKind {contradicting}"
            );
            // windowKind inside the window is checked the same way.
            let mut nested = item(id, None);
            nested.window.as_mut().unwrap().window_kind = Some(contradicting.into());
            assert_eq!(antigravity_pool_window(&nested), None, "{id} nested kind");
            // A windowKind outside Insula's vocabulary leaves the id's period.
            assert_eq!(
                antigravity_pool_window(&item(id, Some("fortnightly"))),
                Some((pool, period))
            );
        }
        // A pool-looking id that is not one of the four is not a pool window.
        assert_eq!(antigravity_pool_window(&item("gemini-flash", None)), None);
        assert_eq!(antigravity_pool_window(&item("other-5h", None)), None);
        // Every pool the ids name is one some routed model family draws on.
        for (_, pool, _) in ANTIGRAVITY_POOL_WINDOW_IDS {
            assert!(
                ANTIGRAVITY_POOL_BY_MODEL_FAMILY
                    .iter()
                    .any(|(_, family_pool)| family_pool == pool),
                "{pool}"
            );
        }
    }

    #[test]
    fn antigravity_unknown_gemini_buckets_never_pressure_gemini_capacities() {
        // QTA: paid Pro accounts publish a separately metered weekly Gemini 4
        // Argon allowance under a new bucket id that may start with `gemini-`.
        // Read as the `gemini` weekly pool, an exhausted Argon allowance would
        // wall every Gemini route. Each exhausted unknown bucket below carries
        // windowKind weekly; the only recognized Gemini pool window in this
        // entry is `gemini-5h`, and it alone may reach Gemini capacities.
        let unknown_ids = [
            "gemini-argon",
            "gemini-4-argon-weekly",
            "gemini-weekly-extra",
        ];
        let mut windows: Vec<ExtraWindow> = unknown_ids
            .iter()
            .map(|id| ExtraWindow {
                title: Some("Gemini 4 Argon Weekly Limit".into()),
                id: Some((*id).into()),
                window: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                window_kind: Some("weekly".into()),
            })
            .collect();
        for window in &windows {
            assert_eq!(antigravity_pool_window(window), None, "{:?}", window.id);
            assert!(!is_account_pool_window(window), "{:?}", window.id);
        }
        windows.push(ExtraWindow {
            title: Some("Gemini Models Five Hour Limit".into()),
            id: Some("gemini-5h".into()),
            window: Some(win(25.0, "2026-01-01T00:00:00Z", None)),
            window_kind: Some("five_hour".into()),
        });
        let usage = Usage {
            primary: None,
            secondary: None,
            tertiary: None,
            extra_rate_windows: Some(windows),
        };
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "antigravity".into(),
            account: None,
            source: None,
            usage: Some(usage.clone()),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };
        let caps = vec![
            "google/antigravity-gemini-3.6-flash".to_string(),
            "google/antigravity-gemini-3.1-pro".to_string(),
            "google/antigravity-claude-opus-4-6-thinking".to_string(),
        ];
        let out = extract_windows(&entry, "antigravity", &caps);
        let emitted: Vec<(&str, Option<&str>, f64)> = out
            .iter()
            .map(|w| {
                (
                    w.window_key.as_str(),
                    w.applies_key.as_deref(),
                    w.utilization,
                )
            })
            .collect();
        assert_eq!(
            emitted,
            vec![("codexbar:antigravity:gemini-5h", Some("gemini"), 0.25)],
            "only the real Gemini pool is extracted"
        );
        for capacity_key in &caps[..2] {
            let reported = account_rate_windows(&usage, Some(capacity_key));
            let used: Vec<f64> = reported.iter().map(|(_, w)| w.used_percent).collect();
            assert_eq!(used, vec![25.0], "{capacity_key} reads only gemini-5h");
        }
    }

    // Replaces `antigravity_with_no_extras_emits_nothing`. Extraction used to
    // dispatch on the provider id and the wire name, so an Antigravity entry
    // without extra windows extracted nothing. Windows are now chosen by the
    // reading's shape alone: a reading without extra windows is an aggregate
    // reading, whichever provider sent it.
    #[test]
    fn a_reading_without_extra_windows_yields_its_aggregate_windows() {
        let entry = ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: "antigravity".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(win(100.0, "2026-01-01T00:00:00Z", Some(300))),
                secondary: None,
                tertiary: None,
                extra_rate_windows: Some(Vec::new()),
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        };
        let caps = vec!["google/antigravity-gemini-3.5-flash".to_string()];
        let out = extract_windows(&entry, "antigravity", &caps);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].scope_kind, WindowScopeKind::Account);
        assert_eq!(out[0].window_key, "codexbar:antigravity:primary");
    }

    /// The body of an Antigravity reading with one Gemini and one third-party
    /// pool, each with a five-hour and a weekly window.
    fn two_pool_entry(provider: &str) -> ProviderUsage {
        let pool = |id: &str, used: f64| ExtraWindow {
            title: None,
            id: Some(id.into()),
            window: Some(win(used, "2026-01-01T00:00:00Z", None)),
            window_kind: None,
        };
        ProviderUsage {
            stale: None,
            fetched_at: None,
            api_provider: None,
            provider: provider.into(),
            account: None,
            source: None,
            usage: Some(Usage {
                // An aggregate beside pools is not read.
                primary: Some(win(100.0, "2026-01-01T00:00:00Z", Some(300))),
                secondary: None,
                tertiary: None,
                extra_rate_windows: Some(vec![
                    pool("gemini-5h", 10.0),
                    pool("gemini-weekly", 20.0),
                    pool("3p-5h", 100.0),
                    pool("3p-weekly", 30.0),
                ]),
            }),
            spend: Vec::new(),
            error: None,
            error_class: None,
            saved_resets: None,
        }
    }

    #[test]
    fn an_antigravity_shaped_reading_yields_two_pools_under_any_provider() {
        let caps = vec![
            "google/antigravity-gemini-3.6-flash".to_string(),
            "google/antigravity-claude-opus-4-6-thinking".to_string(),
        ];
        let pools = |entry: &ProviderUsage, wire: &str| {
            extract_windows(entry, wire, &caps)
                .into_iter()
                .map(|w| {
                    assert_eq!(w.scope_kind, WindowScopeKind::AntigravityPool);
                    (w.applies_key, w.window_ms, w.utilization)
                })
                .collect::<Vec<_>>()
        };
        let first = pools(&two_pool_entry("antigravity"), "antigravity");
        let names: std::collections::BTreeSet<_> =
            first.iter().filter_map(|(pool, _, _)| pool.clone()).collect();
        assert_eq!(
            names,
            ["3p".to_string(), "gemini".to_string()].into(),
            "two pools, neither named five_hour nor seven_day"
        );
        assert_eq!(first.len(), 4, "each pool keeps its five-hour and weekly window");
        // The same body under another provider name and wire name yields the
        // same pools: nothing here reads the provider.
        assert_eq!(first, pools(&two_pool_entry("some-other-vendor"), "other-wire"));
    }

    #[test]
    fn title_match_prefers_most_specific_and_skips_ambiguous_ties() {
        let caps = vec![
            "google/antigravity-gemini-3.5-flash".to_string(),
            "google/antigravity-gemini-3.5-flash-lite".to_string(),
        ];
        // "Flash Lite" matches the longer (more specific) key.
        assert_eq!(
            match_capacity_key_by_title("Gemini 3.5 Flash Lite", &caps, "antigravity").as_deref(),
            Some("google/antigravity-gemini-3.5-flash-lite")
        );
        // Version transposition must not match (ordered subsequence).
        assert_eq!(
            match_capacity_key_by_title("Gemini 5.3 Flash", &caps, "antigravity"),
            None
        );
    }
}
