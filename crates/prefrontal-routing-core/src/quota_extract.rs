//! Per-provider quota window EXTRACTION. Ported VERBATIM from the TS
//! `quota/codexbar-window-extractors.ts`.
//!
//! `ai-provider-quota` returns a uniform `ProviderUsage` shape, but providers
//! expose quota with DIFFERENT topologies. Most (codex/grok/opencode-go/ollama)
//! report one account-wide pool in primary/secondary/tertiary → one account-scoped
//! window each. Antigravity (Google AI Ultra) bundles MULTIPLE independent pools
//! under one account: its aggregate `primary`=100% reflects EXHAUSTED external
//! models (Claude, GPT-OSS), while the Geminis we route to sit in
//! `extra_rate_windows[]` at low utilization, so the aggregate must be IGNORED and
//! the per-model windows mapped to specific capacity keys instead.

use crate::quota::{ProviderUsage, RateWindow};

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
}

/// A normalized window an extractor emits; the overlay runs pace + worst-wins on it.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedWindow {
    pub window_key: String,
    pub scope_kind: WindowScopeKind,
    /// capacity key when scope is CapacityKey / family key for ModelFamily.
    pub applies_key: Option<String>,
    /// 0..1
    pub utilization: f64,
    /// ms epoch; `None` when the provider did not report a recovery time.
    pub resets_at: Option<f64>,
    /// `None` when the provider does not report a window length.
    pub window_ms: Option<f64>,
    pub limit_reached: bool,
}

fn clamp01(n: f64) -> f64 {
    n.clamp(0.0, 1.0)
}

const WINDOW_SLOTS: [&str; 3] = ["primary", "secondary", "tertiary"];

/// Parse an RFC3339/ISO-8601 timestamp to ms epoch, mirroring JS `Date.parse`.
/// Returns `None` when the provider omitted or malformed the recovery time.
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
    let utilization = clamp01(w.used_percent / 100.0);
    let resets_at = w.resets_at.as_deref().and_then(|raw| {
        let parsed = parse_resets_at(raw);
        if parsed.is_none() && utilization > 0.0 {
            // A garbled timestamp must not hide a burning window: preserve its
            // utilization and pace it through the no-recovery-time fallback.
            return None;
        }
        parsed
    });
    if utilization == 0.0 && resets_at.is_none() {
        // QTA's `docs/consumer-contract.md` says resetsAt is optional and never
        // fabricated. None means no recovery time was reported, never "idle";
        // only a zero-use window with no recovery time is truly idle.
        return None;
    }
    Some(ExtractedWindow {
        window_key,
        scope_kind,
        applies_key,
        utilization,
        resets_at,
        window_ms: w.window_minutes.map(|m| m as f64 * 60_000.0),
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

/// Antigravity (Google AI Ultra): ignore the misleading aggregate windows; map each
/// per-model window in extra_rate_windows[] to its matching capacity key,
/// capacity-key-scoped. Unmatched titles (models we don't route to) are skipped. If
/// extra_rate_windows is absent, emit NOTHING (correct-or-nothing) rather than fall
/// back to the misleading aggregate.
fn antigravity_per_model_extractor(
    entry: &ProviderUsage,
    cb_provider_name: &str,
    capacity_keys: &[String],
) -> Vec<ExtractedWindow> {
    let Some(extra) = entry
        .usage
        .as_ref()
        .and_then(|u| u.extra_rate_windows.as_ref())
    else {
        return Vec::new();
    };
    if extra.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for item in extra {
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

/// Extract the windows for one provider-usage entry, dispatching to the
/// provider-specific extractor (registry keyed by RESOLVED providerID; providers
/// absent use the aggregate-account default).
pub fn extract_windows(
    entry: &ProviderUsage,
    provider_id: &str,
    cb_provider_name: &str,
    capacity_keys: &[String],
) -> Vec<ExtractedWindow> {
    match provider_id {
        "google" => antigravity_per_model_extractor(entry, cb_provider_name, capacity_keys),
        _ => aggregate_account_extractor(entry, cb_provider_name),
    }
}

// ── minimal RFC3339 parse (no chrono dep in -core) ─────────────────────────
// Parses "YYYY-MM-DDTHH:MM:SS(.fff)?(Z|+HH:MM|-HH:MM)" to ms epoch. Returns None on
// any shape it cannot parse (the window is then skipped, matching JS Date.parse →
// NaN → skip).
fn chrono_parse_rfc3339(s: &str) -> Option<f64> {
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
            resets_at: Some(resets_at.into()),
            window_minutes: mins,
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
            error: None,
        };
        let out = extract_windows(&entry, "openai", "codex", &[]);
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
            api_provider: None,
            provider: "ollama".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(RateWindow {
                    used_percent: 100.0,
                    resets_at: None,
                    window_minutes: Some(10_080),
                }),
                secondary: None,
                tertiary: None,
                extra_rate_windows: None,
            }),
            error: None,
        };

        let out = extract_windows(&entry, "ollama", "ollama", &[]);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].utilization, 1.0);
        assert_eq!(out[0].resets_at, None);
    }

    #[test]
    fn idle_no_reset_window_stays_skipped() {
        let entry = ProviderUsage {
            api_provider: None,
            provider: "ollama".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(RateWindow {
                    used_percent: 0.0,
                    resets_at: None,
                    window_minutes: Some(10_080),
                }),
                secondary: None,
                tertiary: None,
                extra_rate_windows: None,
            }),
            error: None,
        };

        assert!(extract_windows(&entry, "ollama", "ollama", &[]).is_empty());
    }

    #[test]
    fn malformed_reset_does_not_hide_burning_window() {
        let entry = ProviderUsage {
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
            error: None,
        };

        let out = extract_windows(&entry, "ollama", "ollama", &[]);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].utilization, 0.4);
        assert_eq!(out[0].resets_at, None);
    }

    #[test]
    fn antigravity_maps_extra_windows_to_capacity_keys_and_skips_unmatched() {
        let entry = ProviderUsage {
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
                    },
                    // GPT-OSS is not a routed capacity key → skipped.
                    ExtraWindow {
                        title: Some("GPT-OSS 120B".into()),
                        id: None,
                        window: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                    },
                ]),
            }),
            error: None,
        };
        let caps = vec![
            "google/antigravity-gemini-3.5-flash".to_string(),
            "google/antigravity-gemini-3.1-pro".to_string(),
        ];
        let out = extract_windows(&entry, "google", "antigravity", &caps);
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
    fn antigravity_with_no_extras_emits_nothing() {
        let entry = ProviderUsage {
            api_provider: None,
            provider: "antigravity".into(),
            account: None,
            source: None,
            usage: Some(Usage {
                primary: Some(win(100.0, "2026-01-01T00:00:00Z", None)),
                secondary: None,
                tertiary: None,
                extra_rate_windows: None,
            }),
            error: None,
        };
        let caps = vec!["google/antigravity-gemini-3.5-flash".to_string()];
        assert!(extract_windows(&entry, "google", "antigravity", &caps).is_empty());
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
