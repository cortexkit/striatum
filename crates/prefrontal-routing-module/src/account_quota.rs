//! Per-account quota views for account selection.
//!
//! Routing will choose which subscription account serves new work from these
//! views. This module only builds them: it polls Insula's complete feed with
//! `usage.get {}`, keeps every account's whole entry with its
//! producer date, joins accounts to vault identities by vault account id, and
//! says for each account whether its reading is fit to place new (cold) work
//! on, and why not when it is not: degraded, too old, missing windows, or no
//! vault identity. Nothing selects an account from these views yet, and model
//! selection does not read them.
//!
//! Rules this module keeps, from `docs/designs/account-selection.md` ("Insula
//! input and conservative degraded behavior"):
//! * A poll never clears readings it did not mention. An account a partial reply
//!   omits keeps its reading and keeps aging on its producer date. Only a reply
//!   that lists the provider in `completeProviders` may tombstone an absent
//!   account, and a tombstone keeps the row.
//! * Entries are taken whole. A windowless error may retain the same account's
//!   last-good entry for less than 30 minutes from fetchedAt, disclosed as stale.
//!   Accounts' windows are never mixed, and duplicate lanes resolve deterministically.
//! * Accounts join the vault by account id, never by email or provider aliases.
//! * General windows come from the reading, never from a provider table; a
//!   scoped window is identified by producer id plus `windowMinutes`, never by
//!   its title.

use std::collections::{BTreeMap, BTreeSet};

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cortexkit_claustrum_client::{
    CredentialErrorClass, CredentialResolver, CredentialResolverError, ScopedCredentialListing,
};
use prefrontal_routing_core::quota::{RateWindow, StaleDisclosure, Usage, LAST_GOOD_MAX_AGE_MS};
use prefrontal_routing_store::{
    PersistedAccountReading, PersistedProviderCompleteness, PersistedVaultRoster,
    SqliteRouteStateStore,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Length of Claude's five-hour general window, as Insula publishes it.
pub const FIVE_HOUR_MINUTES: i64 = 300;
/// Length of the seven-day general window and of the Fable scoped window.
pub const SEVEN_DAY_MINUTES: i64 = 10_080;

/// A reading younger than this is in the full age band (age factor 1).
pub const FULL_BAND_MS: i64 = 5 * 60_000;
/// A reading this old or older is excluded from cold work. Between the full
/// band and this bound the age factor falls linearly to `DISCOUNT_FLOOR`.
/// Insula's per-account retry backoff tops out at 900 s and a fetch may take
/// 35 s more, so an account whose usage endpoint returns routine 429s is still
/// retried well inside 20 minutes, even counting routing's own 60 s poll. Only
/// a longer silence excludes it.
pub const EXCLUDED_BAND_MS: i64 = 20 * 60_000;
/// Age factor at the end of the discounted band.
pub const DISCOUNT_FLOOR: f64 = 0.25;
/// A producer date more than this far ahead of routing's clock is clock-invalid.
pub const FUTURE_SKEW_MS: i64 = 60_000;
/// A vault identity with no Insula reading this long after it joined the
/// roster raises an operator alarm.
pub const FIRST_READING_ALARM_MS: i64 = 15 * 60_000;
/// A persisted vault roster older than this is no longer used for the join.
pub const ROSTER_MAX_AGE_MS: i64 = 24 * 60 * 60_000;

/// The only `stale.class` routing understands. Insula marks a reading it keeps
/// serving through a transient failure (429, 5xx, network) with this class;
/// any other class is treated as not fresh until routing learns it.
const UNDERSTOOD_STALE_CLASS: &str = "upstream_failed";
/// The spend pool id Insula uses for Claude's paid extra usage. Insula publishes
/// it only when Anthropic states a limit, so its absence means "off or no stated
/// limit", never "unlimited".
const OVERAGE_POOL_ID: &str = "extra_usage";

/// A per-model-family scoped quota window, and the API models it governs.
/// Insula does not publish which models a scoped window applies to; routing
/// owns that list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeFamily {
    pub family: &'static str,
    pub insula_provider: &'static str,
    /// Producer ids of this family's window (`extraRateWindows[].id`). Insula
    /// uses the upstream scope's model id when Anthropic sends one and the
    /// display name otherwise, so an id outside this list is never guessed at.
    pub scope_ids: &'static [&'static str],
    pub window_minutes: i64,
    /// Routing model ids this scope governs. Variants such as `[1m]` share the
    /// scope of their base model.
    pub models: &'static [&'static str],
}

pub const SCOPE_FAMILIES: &[ScopeFamily] = &[ScopeFamily {
    family: "fable",
    insula_provider: "claude",
    scope_ids: &["Fable"],
    window_minutes: SEVEN_DAY_MINUTES,
    models: &["claude-fable-5", "claude-fable-5-1"],
}];

/// The scope family governing a routing model, if any. Accepts a bare model id
/// or a `provider/model` route key, with or without a bracketed variant.
/// The first caller will be the step that assigns an account to new work; until
/// that exists only tests use it.
#[cfg_attr(not(test), allow(dead_code))]
pub fn scope_family_for_model(model: &str) -> Option<&'static ScopeFamily> {
    let bare = model.rsplit('/').next().unwrap_or(model);
    let base = bare.split('[').next().unwrap_or(bare);
    SCOPE_FAMILIES
        .iter()
        .find(|family| family.models.contains(&base))
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Display identity Insula attaches to an account. Informational only: joins
/// never use it.
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub plan_type: Option<String>,
}

/// The fields of one Insula `usage.get` entry that account views read.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountEntry {
    pub provider: String,
    #[serde(default)]
    pub api_provider: Option<String>,
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub account_info: Option<AccountInfo>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub fetched_at: Option<String>,
    #[serde(default)]
    pub stale: Option<StaleDisclosure>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub error_class: Option<String>,
    #[serde(default)]
    pub usage: Option<Usage>,
    /// Kept undecoded: account views only look for the `extra_usage` (paid
    /// overage) pool, and a strict decode of some other pool with fields routing
    /// does not model would otherwise reject the whole account entry.
    #[serde(default)]
    pub spend: Vec<Value>,
}

impl AccountEntry {
    fn account_id(&self) -> Option<&str> {
        self.account
            .as_deref()
            .filter(|account| !account.is_empty())
    }
}

/// One decoded `usage.get` reply: every entry routing could decode (kept with
/// its original JSON), the providers Insula declared complete, and what it
/// could not use.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DecodedEnvelope {
    pub entries: Vec<(AccountEntry, Value)>,
    pub complete_providers: Vec<String>,
    /// Entries without an account id; they cannot be keyed to an account.
    pub unkeyed_entries: usize,
    /// Decode errors for entries routing could not read. The rest of the reply
    /// is still used.
    pub undecodable_entries: Vec<String>,
}

/// Decode the whole `usage.get` envelope, `{"result": [...], "completeProviders": [...]}`.
/// A reply without a `result` array, or with a malformed `completeProviders`,
/// is rejected whole. A single malformed entry is reported and skipped.
pub fn decode_usage_envelope(bytes: &[u8]) -> Result<DecodedEnvelope, String> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| format!("reply is not JSON: {error}"))?;
    let result = value
        .get("result")
        .and_then(Value::as_array)
        .ok_or("reply has no `result` array")?;
    let complete_providers = match value.get("completeProviders") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or("`completeProviders` holds a non-string")
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err("`completeProviders` is not an array".into()),
    };
    let mut decoded = DecodedEnvelope {
        complete_providers,
        ..DecodedEnvelope::default()
    };
    for raw in result {
        match serde_json::from_value::<AccountEntry>(raw.clone()) {
            Ok(entry) if entry.account_id().is_none() => decoded.unkeyed_entries += 1,
            Ok(entry) => decoded.entries.push((entry, raw.clone())),
            Err(error) => decoded.undecodable_entries.push(error.to_string()),
        }
    }
    Ok(decoded)
}

// ---------------------------------------------------------------------------
// Timestamps and age bands
// ---------------------------------------------------------------------------

/// Parse an RFC 3339 timestamp (fractional seconds of any precision, `Z` or a
/// numeric offset) to Unix milliseconds. Fractions below a millisecond are
/// truncated.
pub fn parse_rfc3339_ms(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[13] != b':' {
        return None;
    }
    if !matches!(bytes[10], b'T' | b't' | b' ') || bytes[16] != b':' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| -> Option<i64> {
        let slice = text.get(range)?;
        slice
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            .then(|| slice.parse().ok())
            .flatten()
    };
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 {
        return None;
    }
    if second > 60 {
        return None;
    }
    let mut index = 19;
    let mut millis = 0;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == start {
            return None;
        }
        let fraction = &text[start..index];
        let padded: String = fraction.chars().chain("000".chars()).take(3).collect();
        millis = padded.parse::<i64>().ok()?;
    }
    let offset_minutes = match bytes.get(index)? {
        b'Z' | b'z' if index + 1 == bytes.len() => 0,
        sign @ (b'+' | b'-') if index + 6 == bytes.len() && bytes[index + 3] == b':' => {
            let hours = number(index + 1..index + 3)?;
            let minutes = number(index + 4..index + 6)?;
            let total = hours * 60 + minutes;
            if *sign == b'-' {
                -total
            } else {
                total
            }
        }
        _ => return None,
    };
    // Days from the civil date (Howard Hinnant's algorithm).
    let shifted_year = if month <= 2 { year - 1 } else { year };
    let era = shifted_year.div_euclid(400);
    let year_of_era = shifted_year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_minutes * 60;
    Some(seconds * 1_000 + millis)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgeBand {
    /// Under 5 minutes old: age factor 1.
    Full,
    /// 5 to 20 minutes old: still eligible, age factor falling to 0.25.
    Discounted,
    /// 20 minutes old or more: unknown for cold work.
    Excluded,
    /// More than 60 s ahead of routing's clock: cannot be trusted as fresh.
    ClockInvalid,
    /// No producer date, or one that does not parse.
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct AgeAssessment {
    pub band: AgeBand,
    /// `now - fetchedAt` on routing's clock; negative for a date slightly ahead.
    pub age_ms: Option<i64>,
    /// 1 in the full band, falling linearly to 0.25 across the discounted band,
    /// 0 when the reading cannot be used.
    pub factor: f64,
}

/// Age band of a reading whose producer date is `fetched_at_ms`, measured on
/// routing's clock at `now_ms`. Poll or receipt time is never used as the age.
pub fn age_band(fetched_at_ms: Option<i64>, now_ms: i64) -> AgeAssessment {
    age_band_with_bound(fetched_at_ms, now_ms, EXCLUDED_BAND_MS)
}

fn age_band_with_bound(fetched_at_ms: Option<i64>, now_ms: i64, excluded_ms: i64) -> AgeAssessment {
    let Some(fetched_at_ms) = fetched_at_ms else {
        return AgeAssessment {
            band: AgeBand::Missing,
            age_ms: None,
            factor: 0.0,
        };
    };
    let age_ms = now_ms - fetched_at_ms;
    let (band, factor) = if age_ms < -FUTURE_SKEW_MS {
        (AgeBand::ClockInvalid, 0.0)
    } else if age_ms < FULL_BAND_MS {
        (AgeBand::Full, 1.0)
    } else if age_ms < excluded_ms {
        let progress = (age_ms - FULL_BAND_MS) as f64 / (excluded_ms - FULL_BAND_MS) as f64;
        (AgeBand::Discounted, 1.0 - (1.0 - DISCOUNT_FLOOR) * progress)
    } else {
        (AgeBand::Excluded, 0.0)
    };
    AgeAssessment {
        band,
        age_ms: Some(age_ms),
        factor,
    }
}

impl AccountEntry {
    fn has_usable_windows(&self) -> bool {
        self.usage.as_ref().is_some_and(|usage| {
            [
                usage.primary.as_ref(),
                usage.secondary.as_ref(),
                usage.tertiary.as_ref(),
            ]
            .into_iter()
            .flatten()
            .chain(
                usage
                    .extra_rate_windows
                    .iter()
                    .flatten()
                    .filter_map(|extra| extra.window.as_ref()),
            )
            .any(|window| {
                window.used_percent.is_finite()
                    && (0.0..=100.0).contains(&window.used_percent)
                    && window.window_minutes.is_some_and(|minutes| minutes > 0)
            })
        })
    }

    pub(super) fn age(&self, now_ms: i64) -> AgeAssessment {
        age_band_with_bound(
            self.fetched_at.as_deref().and_then(parse_rfc3339_ms),
            now_ms,
            if self.stale.is_some() {
                LAST_GOOD_MAX_AGE_MS
            } else {
                EXCLUDED_BAND_MS
            },
        )
    }

    pub(super) fn has_error(&self) -> bool {
        self.error.is_some() || self.error_class.is_some()
    }

    pub(super) fn understood_stale(&self) -> bool {
        self.stale
            .as_ref()
            .and_then(|stale| stale.class.as_deref())
            .is_none_or(|class| class == UNDERSTOOD_STALE_CLASS)
    }
}

fn keep_last_good(previous: &StoredReading, incoming: &AccountEntry, now_ms: i64) -> Option<Value> {
    let old: AccountEntry = serde_json::from_value(previous.entry.clone()).ok()?;
    let fetched = old.fetched_at.as_deref().and_then(parse_rfc3339_ms)?;
    if previous.tombstoned_at_ms.is_some()
        || old.has_error()
        || !old.understood_stale()
        || !old.has_usable_windows()
        || !(0..LAST_GOOD_MAX_AGE_MS).contains(&now_ms.saturating_sub(fetched))
        || !incoming.has_error()
        || incoming.has_usable_windows()
    {
        return None;
    }
    let mut kept = previous.entry.clone();
    // Keep the producer date and every window verbatim. `since` is the first
    // failure, not a second age source; Insula may already be serving last-good.
    // The new error class belongs to the stale disclosure, not to the good entry.
    kept["stale"] = serde_json::json!({
        "since": old.stale.as_ref().and_then(|stale| stale.since.clone())
            .or_else(|| incoming.stale.as_ref().and_then(|stale| stale.since.clone()))
            .unwrap_or_else(|| crate::account_selection::rfc3339(now_ms)),
        "class": incoming.error_class.as_ref()
            .or_else(|| incoming.stale.as_ref().and_then(|stale| stale.class.as_ref())),
    });
    Some(kept)
}

// ---------------------------------------------------------------------------
// Readings and merge
// ---------------------------------------------------------------------------

/// One account's latest reading as routing holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredReading {
    pub provider: String,
    pub account_id: String,
    /// The whole Insula entry, with a stale disclosure if routing kept last-good.
    pub entry: Value,
    /// Local clock when routing accepted the entry (Unix ms). Not the age source.
    pub received_at_ms: i64,
    /// Set when a complete reply for the provider no longer listed the account.
    pub tombstoned_at_ms: Option<i64>,
}

impl StoredReading {
    /// Only the producer's catalog vendor can name a routing provider.
    pub fn routing_provider(&self) -> Option<&str> {
        self.entry
            .get("apiProvider")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
    }

    fn fetched_at(&self) -> Option<&str> {
        self.entry.get("fetchedAt").and_then(Value::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCompleteness {
    pub last_polled_at_ms: i64,
    pub last_poll_complete: bool,
    pub last_complete_at_ms: Option<i64>,
}

/// Every account reading routing holds, keyed by (Insula provider, account id).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AccountReadings {
    pub readings: BTreeMap<(String, String), StoredReading>,
    pub completeness: BTreeMap<String, ProviderCompleteness>,
}

/// What one merged poll changed, so exactly those rows can be persisted.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MergeOutcome {
    pub updated: Vec<(String, String)>,
    pub tombstoned: Vec<(String, String)>,
    /// Accounts that appeared more than once in the reply, with the lane kept.
    pub duplicate_lanes: Vec<String>,
}

/// Pick one entry per account when a reply carries several lanes for it: the
/// latest producer date wins, then the smaller `source`, then the smaller
/// serialized entry, so the choice never depends on reply order.
fn lane_order_key(
    entry: &AccountEntry,
    raw: &Value,
) -> (
    Option<i64>,
    std::cmp::Reverse<String>,
    std::cmp::Reverse<String>,
) {
    (
        entry.fetched_at.as_deref().and_then(parse_rfc3339_ms),
        std::cmp::Reverse(entry.source.clone().unwrap_or_default()),
        std::cmp::Reverse(raw.to_string()),
    )
}

/// Merge one provider's poll into `readings`. Accounts in the reply replace
/// their stored entry whole, except an error without windows may keep last-good
/// for less than 30 minutes from its producer date. Omitted accounts keep their reading; only
/// when Insula lists the provider in `completeProviders` are they tombstoned,
/// and even then the row and its source date are kept.
pub fn merge_provider_poll(
    readings: &mut AccountReadings,
    provider: &str,
    envelope: &DecodedEnvelope,
    received_at_ms: i64,
) -> MergeOutcome {
    let mut outcome = MergeOutcome::default();
    let mut chosen: BTreeMap<String, (&AccountEntry, &Value)> = BTreeMap::new();
    let mut seen_twice = BTreeSet::new();
    for (entry, raw) in &envelope.entries {
        if entry.provider != provider {
            continue;
        }
        let Some(account) = entry.account_id() else {
            continue;
        };
        match chosen.get(account) {
            Some((kept, kept_raw)) => {
                seen_twice.insert(account.to_string());
                if lane_order_key(entry, raw) > lane_order_key(kept, kept_raw) {
                    chosen.insert(account.to_string(), (entry, raw));
                }
            }
            None => {
                chosen.insert(account.to_string(), (entry, raw));
            }
        }
    }
    for account in seen_twice {
        let source = chosen
            .get(&account)
            .and_then(|(entry, _)| entry.source.clone())
            .unwrap_or_else(|| "unknown".into());
        outcome.duplicate_lanes.push(format!(
            "{provider} account {account}: kept the {source} lane"
        ));
    }
    for (account, (entry, raw)) in &chosen {
        let key = (provider.to_string(), account.clone());
        let entry = readings
            .readings
            .get(&key)
            .and_then(|previous| keep_last_good(previous, entry, received_at_ms))
            .unwrap_or_else(|| (*raw).clone());
        readings.readings.insert(
            key.clone(),
            StoredReading {
                provider: provider.to_string(),
                account_id: account.clone(),
                entry,
                received_at_ms,
                tombstoned_at_ms: None,
            },
        );
        outcome.updated.push(key);
    }
    let complete = envelope
        .complete_providers
        .iter()
        .any(|listed| listed == provider);
    // An entry routing could not decode may belong to any stored account, so a
    // reply carrying one never tombstones, even when Insula calls it complete.
    if complete && envelope.undecodable_entries.is_empty() {
        for (key, reading) in readings.readings.iter_mut() {
            if key.0 == provider
                && !chosen.contains_key(&key.1)
                && reading.tombstoned_at_ms.is_none()
            {
                reading.tombstoned_at_ms = Some(received_at_ms);
                outcome.tombstoned.push(key.clone());
            }
        }
    }
    let previous_complete_at = readings
        .completeness
        .get(provider)
        .and_then(|row| row.last_complete_at_ms);
    readings.completeness.insert(
        provider.to_string(),
        ProviderCompleteness {
            last_polled_at_ms: received_at_ms,
            last_poll_complete: complete,
            last_complete_at_ms: if complete {
                Some(received_at_ms)
            } else {
                previous_complete_at
            },
        },
    );
    outcome
}

// ---------------------------------------------------------------------------
// Vault roster
// ---------------------------------------------------------------------------

/// One vault account: every listed record for the same `(refresh_adapter,
/// account_id)`, coalesced. Several records may belong to one account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterIdentity {
    pub refresh_adapter: String,
    pub account_id: String,
    /// Default record first (`oauth:<provider>`), then byte order.
    pub credential_ids: Vec<String>,
    pub active_credential_ids: Vec<String>,
    pub first_seen_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterGrant {
    pub selector_kind: String,
    pub selector: String,
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultRoster {
    pub view: String,
    pub fetched_at_ms: i64,
    pub identities: Vec<RosterIdentity>,
    pub grants: Vec<RosterGrant>,
}

/// Vault account ids are the join key. Ambiguous ids across adapters are not
/// selectable: choosing the first would silently bind the wrong credential.
pub fn identity_for_reading<'a>(
    roster: &'a VaultRoster,
    reading: &StoredReading,
) -> Option<&'a RosterIdentity> {
    let mut matches = roster
        .identities
        .iter()
        .filter(|identity| identity.account_id == reading.account_id);
    let identity = matches.next()?;
    matches.next().is_none().then_some(identity)
}

fn is_default_record(credential_id: &str) -> bool {
    credential_id.split(':').count() == 2
}

/// Coalesce a vault listing into roster identities. Rows without both a
/// `refresh_adapter` and an `account_id` carry no account identity and are
/// left out. `previous` supplies the first-seen time of identities routing
/// already knew.
pub fn build_roster(
    listing: &ScopedCredentialListing,
    previous: Option<&VaultRoster>,
    fetched_at_ms: i64,
) -> VaultRoster {
    let mut grouped: BTreeMap<(String, String), (BTreeSet<String>, BTreeSet<String>)> =
        BTreeMap::new();
    for credential in &listing.credentials {
        let (Some(adapter), Some(account)) = (
            credential.refresh_adapter.as_deref(),
            credential.account_id.as_deref(),
        ) else {
            continue;
        };
        let slot = grouped
            .entry((adapter.to_string(), account.to_string()))
            .or_default();
        slot.0.insert(credential.id.clone());
        if credential.state == "active" {
            slot.1.insert(credential.id.clone());
        }
    }
    let order = |ids: BTreeSet<String>| -> Vec<String> {
        let mut ids: Vec<String> = ids.into_iter().collect();
        ids.sort_by(|left, right| {
            is_default_record(right)
                .cmp(&is_default_record(left))
                .then_with(|| left.as_bytes().cmp(right.as_bytes()))
        });
        ids
    };
    let identities = grouped
        .into_iter()
        .map(|((refresh_adapter, account_id), (all, active))| {
            let first_seen_ms = previous
                .and_then(|roster| {
                    roster.identities.iter().find(|known| {
                        known.refresh_adapter == refresh_adapter && known.account_id == account_id
                    })
                })
                .map_or(fetched_at_ms, |known| known.first_seen_ms);
            RosterIdentity {
                // When credentials for one account disagree on its billing tier,
                // leave it unknown rather than borrowing another plan's pricing.
                plan_tier: {
                    let tiers: BTreeSet<_> = listing
                        .credentials
                        .iter()
                        .filter(|c| {
                            c.refresh_adapter.as_deref() == Some(&refresh_adapter)
                                && c.account_id.as_deref() == Some(&account_id)
                        })
                        .filter_map(|c| c.plan_tier.clone())
                        .collect();
                    (tiers.len() == 1).then(|| tiers.into_iter().next().unwrap())
                },
                refresh_adapter,
                account_id,
                credential_ids: order(all),
                active_credential_ids: order(active),
                first_seen_ms,
            }
        })
        .collect();
    VaultRoster {
        view: listing.view.clone(),
        fetched_at_ms,
        identities,
        grants: listing
            .grant_tuples
            .iter()
            .map(|grant| RosterGrant {
                selector_kind: grant.selector_kind.clone(),
                selector: grant.selector.clone(),
                operation: grant.operation.clone(),
            })
            .collect(),
    }
}

/// The result of routing's latest attempt to list the vault roster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RosterPoll {
    /// No listing has been attempted by this process.
    NotPolled,
    /// This process does not consult the vault (the read-only dry run).
    NotConsulted {
        reason: String,
    },
    Listed {
        at_ms: i64,
    },
    /// The vault answered and refused the listing.
    Refused {
        at_ms: i64,
        code: String,
        message: String,
    },
    /// The vault could not be reached; no inference is drawn from it.
    Unavailable {
        at_ms: i64,
        message: String,
    },
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowView {
    /// `primary`, `secondary`, `tertiary` or `extra`, for tracing only.
    pub slot: &'static str,
    pub window_kind: Option<String>,
    pub window_minutes: Option<i64>,
    pub used_percent: f64,
    pub raw_used_percent: Option<f64>,
    pub resets_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScopedWindowView {
    pub id: Option<String>,
    pub title: Option<String>,
    pub window_minutes: Option<i64>,
    pub used_percent: Option<f64>,
    pub raw_used_percent: Option<f64>,
    pub resets_at: Option<String>,
    /// The family routing mapped this window to, by id plus duration.
    pub family: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopedListing {
    /// The entry carries no `extraRateWindows` at all.
    Missing,
    /// The entry carries `extraRateWindows: []`.
    ExplicitEmpty,
    Present,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FamilyScopeState {
    /// A window with a known id and the family's duration is present.
    Mapped,
    /// No scoped window was published; the scope check passes, which is not a
    /// claim of capacity.
    NotPublished,
    /// Scoped windows are present but none maps to this family; the family's
    /// scope is unknown and its cold work is excluded.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FamilyScopeView {
    pub family: &'static str,
    pub state: FamilyScopeState,
    pub models: &'static [&'static str],
    pub cold_input_usable: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VaultJoinView {
    pub joined: bool,
    pub refresh_adapter: Option<String>,
    pub credential_ids: Vec<String>,
    pub active_credential_ids: Vec<String>,
}

/// One account as account selection will see it, and why its reading can or
/// cannot be used for cold work. Reserve, weight and exhaustion fences are the
/// allocator's and are not evaluated here.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountView {
    pub provider: String,
    pub routing_provider: Option<String>,
    pub account_id: String,
    /// Display only; never used to join.
    pub email: Option<String>,
    pub source: Option<String>,
    pub vault: VaultJoinView,
    pub fetched_at: Option<String>,
    pub received_at_ms: i64,
    pub age: AgeAssessment,
    pub stale: Option<StaleDisclosure>,
    pub error_class: Option<String>,
    pub error: Option<String>,
    pub tombstoned_at_ms: Option<i64>,
    pub five_hour: Option<WindowView>,
    pub seven_day: Option<WindowView>,
    pub general_windows: Vec<WindowView>,
    pub scoped_listing: ScopedListing,
    pub scoped_windows: Vec<ScopedWindowView>,
    pub family_scopes: Vec<FamilyScopeView>,
    /// The account's paid-overage spend pool (Insula pool id `extra_usage`),
    /// verbatim, when Insula publishes one.
    pub overage_pool: Option<Value>,
    pub cold_input_usable: bool,
    /// Why the reading cannot be used for cold work; empty when it can.
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InsulaOnlyGap {
    pub provider: String,
    pub account_id: String,
    pub email: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VaultOnlyGap {
    pub refresh_adapter: String,
    pub account_id: String,
    pub credential_ids: Vec<String>,
    pub first_seen_ms: i64,
    /// True once the identity has waited 15 minutes for a first reading.
    pub first_reading_overdue: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct RosterGaps {
    pub insula_only: Vec<InsulaOnlyGap>,
    pub vault_only: Vec<VaultOnlyGap>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProviderStatusView {
    pub provider: String,
    pub routing_provider: Option<String>,
    pub routing_providers: Vec<String>,
    pub last_polled_at_ms: Option<i64>,
    pub last_poll_complete: bool,
    pub last_complete_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RosterView {
    pub poll: RosterPoll,
    pub view: Option<String>,
    pub fetched_at_ms: Option<i64>,
    pub identity_count: usize,
    /// Whether the join below used this roster; false when there is none or it
    /// is older than 24 hours.
    pub used_for_join: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AccountQuotaReport {
    pub now_ms: i64,
    pub providers: Vec<ProviderStatusView>,
    pub roster: RosterView,
    pub accounts: Vec<AccountView>,
    pub gaps: RosterGaps,
    pub alarms: Vec<String>,
    /// Producer facts that explain unavailable selection without indicating an
    /// operational failure, such as an unpublished catalog vendor.
    pub information: Vec<String>,
}

fn window_view(slot: &'static str, window: &RateWindow) -> WindowView {
    WindowView {
        slot,
        window_kind: window.window_kind.clone(),
        window_minutes: window.window_minutes,
        used_percent: window.used_percent,
        raw_used_percent: window.raw_used_percent,
        resets_at: window.resets_at.clone(),
    }
}

fn general_windows(usage: Option<&Usage>) -> Vec<WindowView> {
    let Some(usage) = usage else {
        return Vec::new();
    };
    prefrontal_routing_core::quota_extract::account_rate_windows(usage, None)
        .into_iter()
        .map(|(slot, window)| window_view(slot, &window))
        .collect()
}

fn valid_general_window(windows: &[WindowView], minutes: i64) -> Option<WindowView> {
    windows
        .iter()
        .find(|window| {
            let matches = match window.window_kind.as_deref() {
                Some("five_hour") => minutes == FIVE_HOUR_MINUTES,
                Some("weekly") => minutes == SEVEN_DAY_MINUTES,
                Some(_) => false,
                None => window.window_minutes == Some(minutes),
            };
            matches && window.used_percent.is_finite()
        })
        .cloned()
}

fn map_scoped_window(
    provider: &str,
    id: Option<&str>,
    minutes: Option<i64>,
) -> Option<&'static ScopeFamily> {
    SCOPE_FAMILIES.iter().find(|family| {
        family.insula_provider == provider
            && id.is_some_and(|id| family.scope_ids.contains(&id))
            && minutes == Some(family.window_minutes)
    })
}

fn family_scope_views(
    provider: &str,
    listing: ScopedListing,
    scoped: &[ScopedWindowView],
    general_usable: bool,
) -> Vec<FamilyScopeView> {
    SCOPE_FAMILIES
        .iter()
        .filter(|family| family.insula_provider == provider)
        .map(|family| {
            let mapped = scoped.iter().find(|window| window.family == Some(family.family));
            let mut reasons = Vec::new();
            let state = match (listing, mapped) {
                (_, Some(window)) => {
                    // A matched window with a finite remaining value at or below
                    // zero excludes the family; a non-finite value is not
                    // treated as exhaustion.
                    if let Some(used) = window.used_percent.filter(|used| used.is_finite()) {
                        if 100.0 - used <= 0.0 {
                            reasons.push(format!("{} scope is exhausted ({used}% used)", family.family));
                        }
                    }
                    FamilyScopeState::Mapped
                }
                (ScopedListing::Missing | ScopedListing::ExplicitEmpty, None) => {
                    FamilyScopeState::NotPublished
                }
                (ScopedListing::Present, None) => {
                    let unmapped: Vec<String> = scoped
                        .iter()
                        .filter(|window| window.family.is_none())
                        .map(|window| {
                            format!(
                                "{} ({} min)",
                                window.id.as_deref().unwrap_or("<no id>"),
                                window
                                    .window_minutes
                                    .map_or_else(|| "?".to_string(), |m| m.to_string())
                            )
                        })
                        .collect();
                    reasons.push(format!(
                        "{} scope is unknown: scoped windows {} match no known {} id with a {}-minute window",
                        family.family,
                        unmapped.join(", "),
                        family.family,
                        family.window_minutes
                    ));
                    FamilyScopeState::Unknown
                }
            };
            if !general_usable {
                reasons.push("the account's general reading is not usable".into());
            }
            FamilyScopeView {
                family: family.family,
                state,
                models: family.models,
                cold_input_usable: reasons.is_empty(),
                reasons,
            }
        })
        .collect()
}

/// The roster the join may use, and the reason to give when it has no
/// identity for an adapter.
struct RosterForJoin<'a> {
    roster: Option<&'a VaultRoster>,
    missing_reason: String,
}

fn roster_for_join<'a>(
    roster: Option<&'a VaultRoster>,
    poll: &RosterPoll,
    now_ms: i64,
) -> RosterForJoin<'a> {
    let base_reason = match poll {
        RosterPoll::NotPolled => "vault roster not listed yet".to_string(),
        RosterPoll::NotConsulted { reason } => reason.clone(),
        RosterPoll::Listed { .. } => String::new(),
        RosterPoll::Refused { code, .. } => format!("vault refused the roster listing ({code})"),
        RosterPoll::Unavailable { message, .. } => format!("vault unreachable ({message})"),
    };
    match roster {
        Some(roster) if now_ms - roster.fetched_at_ms >= ROSTER_MAX_AGE_MS => RosterForJoin {
            roster: None,
            missing_reason: format!(
                "the persisted vault roster is older than 24 hours{}",
                if base_reason.is_empty() {
                    String::new()
                } else {
                    format!("; {base_reason}")
                }
            ),
        },
        Some(roster) => RosterForJoin {
            roster: Some(roster),
            missing_reason: base_reason,
        },
        None => RosterForJoin {
            roster: None,
            missing_reason: if base_reason.is_empty() {
                "no vault roster".into()
            } else {
                base_reason
            },
        },
    }
}

/// Why an adapter has no identity in a listed roster.
fn adapter_absent_reason(roster: &VaultRoster, adapter: &str) -> String {
    let list_grants: Vec<String> = roster
        .grants
        .iter()
        .filter(|grant| grant.operation == "list" || grant.operation == "read")
        .map(|grant| {
            format!(
                "{} {} {}",
                grant.selector_kind, grant.selector, grant.operation
            )
        })
        .collect();
    if roster.grants.iter().any(|grant| grant.operation == "list") {
        format!("the vault roster lists no {adapter} identity with this account id")
    } else {
        format!(
            "vault roster not granted yet: routing holds no list grant covering {adapter} credentials (grants: {})",
            if list_grants.is_empty() {
                "none".to_string()
            } else {
                list_grants.join(", ")
            }
        )
    }
}

fn account_view(
    reading: &StoredReading,
    roster: &RosterForJoin<'_>,
    now_ms: i64,
    alarms: &mut Vec<String>,
    information: &mut Vec<String>,
) -> (AccountView, Option<InsulaOnlyGap>) {
    let entry: Option<AccountEntry> = serde_json::from_value(reading.entry.clone()).ok();
    let usage = entry.as_ref().and_then(|entry| entry.usage.as_ref());
    let general = general_windows(usage);
    let five_hour = valid_general_window(&general, FIVE_HOUR_MINUTES);
    let seven_day = valid_general_window(&general, SEVEN_DAY_MINUTES);
    let scoped_listing = match usage.and_then(|usage| usage.extra_rate_windows.as_ref()) {
        None => ScopedListing::Missing,
        Some(windows) if windows.is_empty() => ScopedListing::ExplicitEmpty,
        Some(_) => ScopedListing::Present,
    };
    let scoped_windows: Vec<ScopedWindowView> = usage
        .and_then(|usage| usage.extra_rate_windows.as_ref())
        .into_iter()
        .flatten()
        .map(|extra| {
            let minutes = extra
                .window
                .as_ref()
                .and_then(|window| window.window_minutes);
            ScopedWindowView {
                id: extra.id.clone(),
                title: extra.title.clone(),
                window_minutes: minutes,
                used_percent: extra.window.as_ref().map(|window| window.used_percent),
                raw_used_percent: extra
                    .window
                    .as_ref()
                    .and_then(|window| window.raw_used_percent),
                resets_at: extra
                    .window
                    .as_ref()
                    .and_then(|window| window.resets_at.clone()),
                family: map_scoped_window(&reading.provider, extra.id.as_deref(), minutes)
                    .map(|family| family.family),
            }
        })
        .collect();
    let unmapped: Vec<&ScopedWindowView> = scoped_windows
        .iter()
        .filter(|window| {
            window.family.is_none()
                && !usage
                    .and_then(|usage| usage.extra_rate_windows.as_ref())
                    .into_iter()
                    .flatten()
                    .any(|extra| {
                        extra.id == window.id
                            && prefrontal_routing_core::quota_extract::is_account_pool_window(extra)
                    })
        })
        .collect();
    if !unmapped.is_empty() {
        alarms.push(format!(
            "{} account {} publishes scoped windows routing cannot map ({}); update routing's scope map",
            reading.provider,
            reading.account_id,
            unmapped
                .iter()
                .map(|window| format!(
                    "id {} / {} min",
                    window.id.as_deref().unwrap_or("<none>"),
                    window.window_minutes.map_or_else(|| "?".into(), |m| m.to_string())
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let fetched_at = reading.fetched_at().map(str::to_owned);
    let age = entry
        .as_ref()
        .map_or_else(|| age_band(None, now_ms), |entry| entry.age(now_ms));

    let routing_provider = reading.routing_provider();
    let identity = roster
        .roster
        .and_then(|roster| identity_for_reading(roster, reading));
    let join_reason = match (identity, roster.roster) {
        (Some(_), _) => None,
        (None, Some(listed))
            if listed
                .identities
                .iter()
                .filter(|identity| identity.account_id == reading.account_id)
                .count()
                > 1 =>
        {
            Some("vault account id is ambiguous across refresh adapters".into())
        }
        (None, Some(listed)) => Some(adapter_absent_reason(
            listed,
            routing_provider.unwrap_or("unmapped"),
        )),
        (None, None) => Some(roster.missing_reason.clone()),
    };

    let mut reasons = Vec::new();
    if routing_provider.is_none() {
        let reason = "unmapped entry: no apiProvider vendor field";
        reasons.push(reason.into());
        information.push(format!(
            "{} account {}: {reason}",
            reading.provider, reading.account_id
        ));
    }
    if let Some(at) = reading.tombstoned_at_ms {
        reasons.push(format!(
            "absent from a complete Insula reply since {at} (tombstoned)"
        ));
    }
    match &entry {
        None => reasons.push("the stored entry no longer decodes".into()),
        Some(entry) => {
            if let Some(class) = entry.error_class.as_deref() {
                reasons.push(format!("degraded entry: errorClass {class}"));
            } else if entry.error.is_some() {
                reasons.push("degraded entry: error without a class".into());
            }
            if let Some(class) = entry
                .stale
                .as_ref()
                .and_then(|stale| stale.class.as_deref())
                .filter(|class| *class != UNDERSTOOD_STALE_CLASS)
            {
                reasons.push(format!(
                    "stale class '{class}' is not one routing understands; treated as not fresh"
                ));
            }
        }
    }
    match age.band {
        AgeBand::Full | AgeBand::Discounted => {}
        AgeBand::Missing => reasons.push(if fetched_at.is_some() {
            "fetchedAt does not parse".into()
        } else {
            "no fetchedAt source date".into()
        }),
        AgeBand::ClockInvalid => {
            reasons.push("fetchedAt is more than 60 s ahead of routing's clock".into())
        }
        AgeBand::Excluded => reasons.push(format!(
            "reading is {} min old (source age limit reached)",
            age.age_ms.unwrap_or_default() / 60_000
        )),
    }
    if general.is_empty() {
        reasons.push("no reported general windows".into());
    }
    for window in &general {
        if entry.as_ref().is_some_and(|entry| entry.stale.is_some())
            && window
                .resets_at
                .as_deref()
                .and_then(parse_rfc3339_ms)
                .is_some_and(|reset| reset <= now_ms)
        {
            reasons.push("reported general window reset has elapsed; usage is unknown".into());
        }
        if !window.used_percent.is_finite()
            || !(0.0..=100.0).contains(&window.used_percent)
            || !window.window_minutes.is_some_and(|minutes| minutes > 0)
        {
            reasons.push("invalid reported general window".into());
        }
    }
    match (identity, &join_reason) {
        (None, Some(reason)) => reasons.push(format!("no vault identity: {reason}")),
        (Some(identity), _) if identity.active_credential_ids.is_empty() => {
            reasons.push("the vault identity has no active credential".into())
        }
        _ => {}
    }
    let cold_input_usable = reasons.is_empty();
    let mut family_scopes = family_scope_views(
        &reading.provider,
        scoped_listing,
        &scoped_windows,
        cold_input_usable,
    );
    if entry.as_ref().is_some_and(|entry| entry.stale.is_some()) {
        for scope in &mut family_scopes {
            if scoped_windows.iter().any(|window| {
                window.family == Some(scope.family)
                    && window
                        .resets_at
                        .as_deref()
                        .and_then(parse_rfc3339_ms)
                        .is_some_and(|reset| reset <= now_ms)
            }) {
                scope.cold_input_usable = false;
                scope
                    .reasons
                    .push("reported scoped window reset has elapsed; usage is unknown".into());
            }
        }
    }
    let overage_pool = entry.as_ref().and_then(|entry| {
        entry
            .spend
            .iter()
            .find(|pool| pool.get("id").and_then(Value::as_str) == Some(OVERAGE_POOL_ID))
            .cloned()
    });
    let email = entry
        .as_ref()
        .and_then(|entry| entry.account_info.as_ref())
        .and_then(|info| info.email.clone());
    let gap = join_reason
        .as_ref()
        .filter(|_| reading.tombstoned_at_ms.is_none() && routing_provider.is_some())
        .map(|reason| InsulaOnlyGap {
            provider: reading.provider.clone(),
            account_id: reading.account_id.clone(),
            email: email.clone(),
            reason: reason.clone(),
        });
    let view = AccountView {
        provider: reading.provider.clone(),
        routing_provider: routing_provider.map(str::to_owned),
        account_id: reading.account_id.clone(),
        email,
        source: entry.as_ref().and_then(|entry| entry.source.clone()),
        vault: VaultJoinView {
            joined: identity.is_some(),
            refresh_adapter: identity.map(|identity| identity.refresh_adapter.clone()),
            credential_ids: identity
                .map(|identity| identity.credential_ids.clone())
                .unwrap_or_default(),
            active_credential_ids: identity
                .map(|identity| identity.active_credential_ids.clone())
                .unwrap_or_default(),
        },
        fetched_at,
        received_at_ms: reading.received_at_ms,
        age,
        stale: entry.as_ref().and_then(|entry| entry.stale.clone()),
        error_class: entry.as_ref().and_then(|entry| entry.error_class.clone()),
        error: entry.as_ref().and_then(|entry| entry.error.clone()),
        tombstoned_at_ms: reading.tombstoned_at_ms,
        five_hour,
        seven_day,
        general_windows: general,
        scoped_listing,
        scoped_windows,
        family_scopes,
        overage_pool,
        cold_input_usable,
        reasons,
    };
    (view, gap)
}

/// Build every account view, the roster gaps and the alarms at `now_ms`.
pub fn build_report(
    readings: &AccountReadings,
    roster: Option<&VaultRoster>,
    roster_poll: &RosterPoll,
    now_ms: i64,
) -> AccountQuotaReport {
    let join = roster_for_join(roster, roster_poll, now_ms);
    let mut alarms = Vec::new();
    let mut information = Vec::new();
    let mut accounts = Vec::new();
    let mut gaps = RosterGaps::default();
    for reading in readings.readings.values() {
        let (view, gap) = account_view(reading, &join, now_ms, &mut alarms, &mut information);
        accounts.push(view);
        gaps.insula_only.extend(gap);
    }
    if let Some(roster) = join.roster {
        for identity in &roster.identities {
            let has_reading = readings.readings.values().any(|reading| {
                reading.tombstoned_at_ms.is_none()
                    && identity_for_reading(roster, reading) == Some(identity)
            });
            if has_reading {
                continue;
            }
            let overdue = now_ms - identity.first_seen_ms >= FIRST_READING_ALARM_MS;
            if overdue {
                alarms.push(format!(
                    "vault {} account {} ({}) has had no Insula reading for 15 minutes or more",
                    identity.refresh_adapter,
                    identity.account_id,
                    identity.credential_ids.join(", ")
                ));
            }
            gaps.vault_only.push(VaultOnlyGap {
                refresh_adapter: identity.refresh_adapter.clone(),
                account_id: identity.account_id.clone(),
                credential_ids: identity.credential_ids.clone(),
                first_seen_ms: identity.first_seen_ms,
                first_reading_overdue: overdue,
            });
        }
    }
    if !gaps.insula_only.is_empty() {
        alarms.push(format!(
            "{} Insula account(s) have no vault identity",
            gaps.insula_only.len()
        ));
    }
    let provider_names: BTreeSet<_> = readings
        .completeness
        .keys()
        .cloned()
        .chain(
            readings
                .readings
                .values()
                .map(|reading| reading.provider.clone()),
        )
        .collect();
    let providers = provider_names
        .into_iter()
        .map(|provider| {
            let completeness = readings.completeness.get(&provider);
            let routing_providers: BTreeSet<_> = readings
                .readings
                .values()
                .filter(|reading| reading.provider == provider)
                .filter_map(|reading| reading.routing_provider().map(str::to_owned))
                .collect();
            ProviderStatusView {
                provider,
                routing_provider: (routing_providers.len() == 1)
                    .then(|| routing_providers.first().unwrap().clone()),
                routing_providers: routing_providers.into_iter().collect(),
                last_polled_at_ms: completeness.map(|row| row.last_polled_at_ms),
                last_poll_complete: completeness.is_some_and(|row| row.last_poll_complete),
                last_complete_at_ms: completeness.and_then(|row| row.last_complete_at_ms),
            }
        })
        .collect();
    AccountQuotaReport {
        now_ms,
        providers,
        roster: RosterView {
            poll: roster_poll.clone(),
            view: roster.map(|roster| roster.view.clone()),
            fetched_at_ms: roster.map(|roster| roster.fetched_at_ms),
            identity_count: roster.map_or(0, |roster| roster.identities.len()),
            used_for_join: join.roster.is_some(),
        },
        accounts,
        gaps,
        alarms,
        information,
    }
}

// ---------------------------------------------------------------------------
// Live state, persistence and the refresh loop
// ---------------------------------------------------------------------------

/// How often the vault roster and serving-method joins are refreshed.
pub const ACCOUNT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Everything the account views are built from, shared between the refresh
/// loop (writer) and the status operation (reader). The lock is never held
/// across an `.await`.
#[derive(Debug, Clone)]
pub struct AccountQuotaState {
    pub readings: AccountReadings,
    pub roster: Option<VaultRoster>,
    pub roster_poll: RosterPoll,
    pub last_insula_error: Option<String>,
    /// Readings restored from the store at boot.
    pub restored_readings: usize,
}

impl Default for AccountQuotaState {
    fn default() -> Self {
        Self {
            readings: AccountReadings::default(),
            roster: None,
            roster_poll: RosterPoll::NotPolled,
            last_insula_error: None,
            restored_readings: 0,
        }
    }
}

pub type SharedAccountQuota = Arc<Mutex<AccountQuotaState>>;

fn persisted_reading(reading: &StoredReading) -> PersistedAccountReading {
    PersistedAccountReading {
        provider: reading.provider.clone(),
        account_id: reading.account_id.clone(),
        entry_json: reading.entry.to_string(),
        fetched_at: reading.fetched_at().map(str::to_owned),
        received_at: reading.received_at_ms,
        tombstoned_at: reading.tombstoned_at_ms,
    }
}

/// The rows one merged poll changed, ready for `put_account_quota_poll`.
pub fn rows_to_persist(
    readings: &AccountReadings,
    provider: &str,
    outcome: &MergeOutcome,
) -> (
    Vec<PersistedAccountReading>,
    Vec<PersistedProviderCompleteness>,
) {
    let rows = outcome
        .updated
        .iter()
        .chain(&outcome.tombstoned)
        .filter_map(|key| readings.readings.get(key))
        .map(persisted_reading)
        .collect();
    let completeness = readings
        .completeness
        .get(provider)
        .map(|row| PersistedProviderCompleteness {
            provider: provider.to_string(),
            last_polled_at: row.last_polled_at_ms,
            last_poll_complete: row.last_poll_complete,
            last_complete_at: row.last_complete_at_ms,
        })
        .into_iter()
        .collect();
    (rows, completeness)
}

/// Rebuild the account state a previous process persisted: every reading with
/// its original entry, producer date, receipt time and tombstone, the
/// completeness rows and the last roster. A row that no longer parses is
/// skipped and logged; it never blocks the others.
pub fn restore_account_state(store: &SqliteRouteStateStore) -> AccountQuotaState {
    let mut state = AccountQuotaState::default();
    match store.account_quota_readings() {
        Ok(rows) => {
            for row in rows {
                match serde_json::from_str::<Value>(&row.entry_json) {
                    Ok(entry) => {
                        state.readings.readings.insert(
                            (row.provider.clone(), row.account_id.clone()),
                            StoredReading {
                                provider: row.provider,
                                account_id: row.account_id,
                                entry,
                                received_at_ms: row.received_at,
                                tombstoned_at_ms: row.tombstoned_at,
                            },
                        );
                        state.restored_readings += 1;
                    }
                    Err(error) => tracing::warn!(
                        error = %error,
                        provider = %row.provider,
                        account = %row.account_id,
                        "account quota reading undecodable; not restored"
                    ),
                }
            }
        }
        Err(error) => tracing::warn!(error = %error, "account quota readings unreadable"),
    }
    match store.account_quota_completeness() {
        Ok(rows) => {
            for row in rows {
                state.readings.completeness.insert(
                    row.provider,
                    ProviderCompleteness {
                        last_polled_at_ms: row.last_polled_at,
                        last_poll_complete: row.last_poll_complete,
                        last_complete_at_ms: row.last_complete_at,
                    },
                );
            }
        }
        Err(error) => tracing::warn!(error = %error, "account quota completeness unreadable"),
    }
    match store.account_vault_roster() {
        Ok(Some(row)) => match serde_json::from_str::<VaultRoster>(&row.roster_json) {
            Ok(roster) => state.roster = Some(roster),
            Err(error) => tracing::warn!(error = %error, "persisted vault roster undecodable"),
        },
        Ok(None) => {}
        Err(error) => tracing::warn!(error = %error, "persisted vault roster unreadable"),
    }
    state
}

/// Classify a failed vault listing. Only an answer from the vault is a refusal;
/// a transport failure or unavailable vault says nothing about grants.
pub fn roster_poll_failure(error: &CredentialResolverError, at_ms: i64) -> RosterPoll {
    match error.class {
        CredentialErrorClass::Transport | CredentialErrorClass::Unavailable => {
            RosterPoll::Unavailable {
                at_ms,
                message: error.to_string(),
            }
        }
        _ => RosterPoll::Refused {
            at_ms,
            code: error.code().to_string(),
            message: error.to_string(),
        },
    }
}

/// Apply one successful vault listing to the state and return the row to persist.
pub fn apply_roster_listing(
    state: &mut AccountQuotaState,
    listing: &ScopedCredentialListing,
    at_ms: i64,
) -> Option<PersistedVaultRoster> {
    let roster = build_roster(listing, state.roster.as_ref(), at_ms);
    let row = serde_json::to_string(&roster)
        .ok()
        .map(|roster_json| PersistedVaultRoster {
            view: roster.view.clone(),
            roster_json,
            fetched_at: at_ms,
        });
    state.roster = Some(roster);
    state.roster_poll = RosterPoll::Listed { at_ms };
    row
}

/// Decode one provider's reply and merge it. Returns the rows to persist, or
/// the decode error (the stored readings are then left untouched).
#[cfg(test)]
pub fn apply_provider_reply(
    state: &mut AccountQuotaState,
    provider: &str,
    body: &[u8],
    received_at_ms: i64,
) -> Result<
    (
        Vec<PersistedAccountReading>,
        Vec<PersistedProviderCompleteness>,
    ),
    String,
> {
    let envelope = decode_usage_envelope(body)?;
    if !envelope.undecodable_entries.is_empty() {
        tracing::warn!(
            provider,
            errors = ?envelope.undecodable_entries,
            "account quota: some Insula entries did not decode and were skipped"
        );
    }
    let outcome = merge_provider_poll(&mut state.readings, provider, &envelope, received_at_ms);
    for note in &outcome.duplicate_lanes {
        tracing::info!(note = %note, "account quota: duplicate Insula lanes");
    }
    Ok(rows_to_persist(&state.readings, provider, &outcome))
}

/// An unfiltered Insula reply discovers providers from entries and completeness
/// declarations, including providers with zero accounts. Partial replies keep
/// omitted accounts; only Insula's explicit completeness may tombstone them.
pub fn apply_usage_reply(
    state: &mut AccountQuotaState,
    body: &[u8],
    received_at_ms: i64,
) -> Result<
    (
        Vec<PersistedAccountReading>,
        Vec<PersistedProviderCompleteness>,
    ),
    String,
> {
    let envelope = decode_usage_envelope(body)?;
    let providers: BTreeSet<_> = envelope
        .entries
        .iter()
        .map(|(entry, _)| entry.provider.clone())
        .chain(envelope.complete_providers.iter().cloned())
        .chain(state.readings.completeness.keys().cloned())
        .collect();
    if !envelope.undecodable_entries.is_empty() {
        tracing::warn!(errors = ?envelope.undecodable_entries, "account quota: some Insula entries did not decode and were skipped");
    }
    let mut rows = Vec::new();
    let mut completeness = Vec::new();
    for provider in providers {
        let outcome =
            merge_provider_poll(&mut state.readings, &provider, &envelope, received_at_ms);
        for note in &outcome.duplicate_lanes {
            tracing::info!(note = %note, "account quota: duplicate Insula lanes");
        }
        let (provider_rows, provider_completeness) =
            rows_to_persist(&state.readings, &provider, &outcome);
        rows.extend(provider_rows);
        completeness.extend(provider_completeness);
    }
    Ok((rows, completeness))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

fn log_listing_decode_failures(listing: &ScopedCredentialListing, mut log: impl FnMut(&str)) {
    if listing.undecodable_credentials.is_empty() && listing.undecodable_grants.is_empty() {
        return;
    }
    // Malformed grant diagnostics identify tuple positions, not a credential.
    // Name the listing's credential IDs as context without attributing a bad
    // tuple to an unrelated credential or repeating the log for every consumer.
    let ids: Vec<_> = listing
        .credentials
        .iter()
        .map(|row| row.id.as_str())
        .collect();
    log(&format!(
        "credential.list_scoped credentials={ids:?} undecodable_credentials={} undecodable_grants={} credential_errors={:?} grant_errors={:?}",
        listing.undecodable_credentials.len(), listing.undecodable_grants.len(),
        listing.undecodable_credentials, listing.undecodable_grants,
    ));
}

#[test]
fn published_listing_keeps_credential_with_null_optional_metadata() {
    let listing = cortexkit_claustrum_client::decode_list_scoped_reply(serde_json::json!({
        "view": "null-metadata",
        "credentials": [{
            "id": "oauth:anthropic:lab", "categories": ["llm-provider"],
            "type": "oauth", "serves": ["anthropic"], "state": "active",
            "record_version": 1, "operations": ["get"], "plan_tier": null,
            "account_id": null, "auth_method": null, "refresh_adapter": null,
            "email": null, "org_name": null, "plan_tier_source": null
        }]
    }))
    .unwrap();
    assert!(listing.undecodable_credentials.is_empty());
    assert_eq!(listing.credentials.len(), 1);
    assert_eq!(listing.credentials[0].id, "oauth:anthropic:lab");
    assert_eq!(listing.credentials[0].plan_tier, None);
}

#[test]
fn malformed_grant_is_logged_once_with_listing_credentials_and_count() {
    let listing = cortexkit_claustrum_client::decode_list_scoped_reply(serde_json::json!({
        "view": "test-view",
        "credentials": [{
            "id": "oauth:anthropic:lab", "categories": ["llm-provider"],
            "type": "oauth", "serves": ["anthropic"], "state": "active",
            "record_version": 1, "operations": ["get"]
        }],
        "grant_tuples": [{"operation": "list"}]
    }))
    .unwrap();
    assert_eq!(listing.credentials.len(), 1);
    assert!(listing.grant_tuples.is_empty());
    assert_eq!(listing.undecodable_grants.len(), 1);
    let mut lines = Vec::new();
    log_listing_decode_failures(&listing, |line| lines.push(line.to_owned()));
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("oauth:anthropic:lab"));
    assert!(lines[0].contains("undecodable_grants=1"));
    assert!(lines[0].contains("grant tuple 0"));
    log_listing_decode_failures(&ScopedCredentialListing::default(), |_| {
        panic!("healthy read logged")
    });
}

/// Refresh the vault roster and serving-method joins independently of quota delivery.
/// Quota samples and fallback snapshots are written by the quota event consumer.
pub async fn account_quota_loop(
    connection_file_path: PathBuf,
    state: SharedAccountQuota,
    store: Arc<SqliteRouteStateStore>,
    resolver: Arc<dyn CredentialResolver>,
    broca_credentials: crate::broca_credentials::SharedBrocaCredentials,
) {
    use prefrontal_routing_core::RouteStateStore as _;
    let project_root =
        prefrontal_core_store::scratch_root().join("prefrontal-routing-account-quota");
    let _ = std::fs::create_dir_all(&project_root);
    loop {
        let listing = resolver.list_scoped().await;
        if let Ok(listing) = &listing {
            log_listing_decode_failures(listing, |message| tracing::warn!("{message}"));
        }
        let at_ms = now_ms();
        // Derive which catalog providers Broca has credentials for from each
        // credential's operator-set provider IDs, reusing this fetched listing.
        // The cache lock is taken after the fetch, never across the network call.
        if let Ok(mut credentials) = broca_credentials.lock() {
            match &listing {
                Ok(listing) => {
                    credentials.apply_listing(listing, at_ms.max(0) as u64);
                }
                Err(error) => credentials.note_failure(error),
            }
        }
        let roster_row = match state.lock() {
            Ok(mut guard) => match &listing {
                Ok(listing) => apply_roster_listing(&mut guard, listing, at_ms),
                Err(error) => {
                    guard.roster_poll = roster_poll_failure(error, at_ms);
                    None
                }
            },
            Err(_) => None,
        };
        if let Some(row) = roster_row {
            if let Err(error) = store.put_account_vault_roster(&row) {
                tracing::warn!(error = %error, "vault roster not persisted");
            }
        }

        // Readings that arrived since the last admission still end blocks and
        // grow whole-account recovery sets.
        // The state is copied first so no account lock is held while the
        // store transaction is open.
        let snapshot = state.lock().ok().map(|guard| guard.clone());
        if let Some(snapshot) = snapshot {
            if let Err(error) = crate::account_selection::refresh_all_blocks(
                &store,
                &snapshot,
                &prefrontal_routing_core::accounts::AllocatorPolicy::default(),
                now_ms(),
            ) {
                tracing::warn!(error = %error, "account blocks not refreshed");
            }
        }

        // Account selection and pricing must agree on the serving login. Build
        // the method map after both roster and readings have been refreshed.
        if let (Ok(guard), Ok(facts)) = (state.lock(), store.get_facts()) {
            // Providers with no catalog rows resolve from this listing and these
            // rows inside the refresh, so pricing follows the vault.
            if let Ok(mut credentials) = broca_credentials.lock() {
                credentials.refresh_serving_methods(&guard, &facts, now_ms());
            }
        }
        crate::subscription_economics::refresh(
            &connection_file_path,
            &project_root,
            &state,
            listing.as_ref().ok(),
            &broca_credentials,
        )
        .await;
        tokio::time::sleep(ACCOUNT_REFRESH_INTERVAL).await;
    }
}

/// The status operation's reply: the report plus the loop's own health.
pub fn status_json(state: &AccountQuotaState, now_ms: i64) -> Value {
    let report = build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        now_ms,
    );
    serde_json::json!({
        "report": report,
        "last_insula_error": state.last_insula_error,
        "restored_readings": state.restored_readings,
        "subscription_economics": crate::subscription_economics::health_detail(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortexkit_claustrum_client::{ListedCredential, ListedGrant};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    /// A live Insula reply to `usage.get {"provider":"claude"}`, captured
    /// read-only on 2026-09-30 with
    /// `ck-prefrontal-routing --account-quota-dry-run --capture`, with identities
    /// replaced: each account UUID became `00000000-0000-4000-8000-00000000000N`
    /// and each email `account-N@example.invalid` (N = 1..5 in reply order, the
    /// same substitution everywhere, org names included). Every other byte
    /// (utilization, resets, window ids, durations, structure) is as captured.
    /// The unredacted capture's sha256 was
    /// ecb4d60c4e42403ccc5f199460c69f9d58fe4dc295723634e0dde4f2877c51b4; it is
    /// kept outside the repository.
    const LIVE_CLAUDE_REPLY: &[u8] =
        include_bytes!("../fixtures/insula-usage-get-claude-20260930.json");
    /// sha256 of the redacted fixture above.
    const LIVE_CLAUDE_REPLY_SHA256: &str =
        "c25f21f8c78a3c1a180667f72bab9cf81e3437ea0aaaf1ab56d8966566c639de";
    /// 2026-09-29T12:00:00Z.
    const NOON_MS: i64 = 1_790_683_200_000;

    fn claude_entry(account: &str, fetched_at: &str) -> Value {
        json!({
            "provider": "claude",
            "apiProvider": "anthropic",
            "account": account,
            "accountInfo": { "email": format!("{account}@example.test") },
            "source": "vault",
            "fetchedAt": fetched_at,
            "usage": {
                "primary": { "usedPercent": 10.0, "windowMinutes": 300, "resetsAt": "2026-09-29T14:00:00Z" },
                "secondary": { "usedPercent": 40.0, "rawUsedPercent": 41.0, "windowMinutes": 10080 },
                "extraRateWindows": [
                    { "id": "Fable", "title": "7 Day (Fable)", "window": { "usedPercent": 5.0, "windowMinutes": 10080 } }
                ]
            }
        })
    }

    fn reply(entries: Vec<Value>, complete: &[&str]) -> Vec<u8> {
        serde_json::to_vec(&json!({ "result": entries, "completeProviders": complete })).unwrap()
    }

    fn poll(state: &mut AccountQuotaState, entries: Vec<Value>, complete: &[&str], at_ms: i64) {
        apply_provider_reply(state, "claude", &reply(entries, complete), at_ms).unwrap();
    }

    fn listing(rows: &[(&str, &str, &str)], grants: &[(&str, &str)]) -> ScopedCredentialListing {
        ScopedCredentialListing {
            credentials: rows
                .iter()
                .map(|(id, account, email)| ListedCredential {
                    id: (*id).into(),
                    categories: vec!["anthropic-native".into()],
                    credential_type: "oauth".into(),
                    refresh_adapter: Some("anthropic".into()),
                    state: "active".into(),
                    record_version: 1,
                    operations: vec!["list".into()],
                    account_id: Some((*account).into()),
                    email: Some((*email).into()),
                    ..Default::default()
                })
                .collect(),
            grant_tuples: grants
                .iter()
                .map(|(selector, operation)| ListedGrant {
                    selector_kind: "category".into(),
                    selector: (*selector).into(),
                    operation: (*operation).into(),
                })
                .collect(),
            view: "view".into(),
            ..Default::default()
        }
    }

    fn view<'a>(report: &'a AccountQuotaReport, account: &str) -> &'a AccountView {
        report
            .accounts
            .iter()
            .find(|view| view.account_id == account)
            .unwrap_or_else(|| panic!("no view for {account}"))
    }

    fn error_only(account: &str) -> Value {
        json!({
            "provider": "claude", "apiProvider": "anthropic", "account": account,
            "error": "fetch failed after restart", "errorClass": "upstream_failed",
        })
    }

    fn report_for_a(state: &AccountQuotaState, now: i64) -> AccountQuotaReport {
        let roster = build_roster(
            &listing(
                &[("oauth:anthropic", "a", "e")],
                &[("anthropic-native", "list")],
            ),
            None,
            now,
        );
        build_report(
            &state.readings,
            Some(&roster),
            &RosterPoll::Listed { at_ms: now },
            now,
        )
    }

    #[test]
    fn error_only_poll_keeps_last_good_until_thirty_minutes_from_fetched_at() {
        let mut state = AccountQuotaState::default();
        let good = claude_entry("a", "2026-09-29T12:00:00Z");
        poll(&mut state, vec![good.clone()], &["claude"], NOON_MS);
        let first_failure = NOON_MS + 27 * 60_000;
        poll(
            &mut state,
            vec![error_only("a")],
            &["claude"],
            first_failure,
        );
        let report = report_for_a(&state, first_failure);
        let kept = view(&report, "a");
        assert!(kept.cold_input_usable, "{:?}", kept.reasons);
        assert_eq!(kept.age.age_ms, Some(27 * 60_000));
        assert_eq!(
            kept.stale.as_ref().unwrap().class.as_deref(),
            Some("upstream_failed")
        );
        let entry = &state.readings.readings.values().next().unwrap().entry;
        assert_eq!(entry["usage"], good["usage"]);
        assert_eq!(entry["fetchedAt"], good["fetchedAt"]);

        poll(
            &mut state,
            vec![error_only("a")],
            &["claude"],
            NOON_MS + LAST_GOOD_MAX_AGE_MS - 1,
        );
        let kept = report_for_a(&state, NOON_MS + LAST_GOOD_MAX_AGE_MS - 1);
        assert!(view(&kept, "a").cold_input_usable);
        assert_eq!(
            view(&kept, "a").stale.as_ref().unwrap().since.as_deref(),
            Some("2026-09-29T12:27:00Z")
        );
        // A retained last-good reading must stop admitting cold work at its
        // source-age limit, even if no further poll arrives.
        assert!(
            !view(&report_for_a(&state, NOON_MS + LAST_GOOD_MAX_AGE_MS), "a").cold_input_usable
        );
        poll(
            &mut state,
            vec![error_only("a")],
            &["claude"],
            NOON_MS + LAST_GOOD_MAX_AGE_MS,
        );
        let expired = report_for_a(&state, NOON_MS + LAST_GOOD_MAX_AGE_MS);
        assert!(!view(&expired, "a").cold_input_usable);
        assert!(view(&expired, "a").general_windows.is_empty());
    }

    #[test]
    fn error_only_poll_without_prior_good_reading_is_unknown() {
        let mut state = AccountQuotaState::default();
        poll(&mut state, vec![error_only("a")], &["claude"], NOON_MS);
        let report = report_for_a(&state, NOON_MS);
        let account = view(&report, "a");
        assert!(!account.cold_input_usable);
        assert!(account.general_windows.is_empty());
        assert_eq!(account.error_class.as_deref(), Some("upstream_failed"));
    }

    #[test]
    fn last_good_with_a_past_reset_is_unknown_not_free() {
        let mut state = AccountQuotaState::default();
        let mut good = claude_entry("a", "2026-09-29T12:00:00Z");
        good["usage"]["primary"]["resetsAt"] = json!("2026-09-29T12:01:00Z");
        poll(&mut state, vec![good], &["claude"], NOON_MS);
        poll(
            &mut state,
            vec![error_only("a")],
            &["claude"],
            NOON_MS + 2 * 60_000,
        );
        let report = report_for_a(&state, NOON_MS + 2 * 60_000);
        let account = view(&report, "a");
        assert_eq!(account.five_hour.as_ref().unwrap().used_percent, 10.0);
        assert!(!account.cold_input_usable);
        assert!(account
            .reasons
            .iter()
            .any(|reason| reason.contains("reset has elapsed")));
    }

    #[test]
    fn insula_staleness_is_not_added_to_source_age_or_restarted_by_routing() {
        let mut state = AccountQuotaState::default();
        let mut good = claude_entry("a", "2026-09-29T11:33:00Z");
        good["stale"] = json!({"since": "2026-09-29T11:40:00Z", "class": "upstream_failed"});
        poll(&mut state, vec![good], &["claude"], NOON_MS);
        poll(&mut state, vec![error_only("a")], &["claude"], NOON_MS);
        let report = report_for_a(&state, NOON_MS);
        let account = view(&report, "a");
        assert!(account.cold_input_usable, "{:?}", account.reasons);
        assert_eq!(account.age.age_ms, Some(27 * 60_000));
        assert_eq!(
            account.stale.as_ref().unwrap().since.as_deref(),
            Some("2026-09-29T11:40:00Z")
        );
        assert!(!view(&report_for_a(&state, NOON_MS + 3 * 60_000), "a").cold_input_usable);
    }

    #[test]
    fn retained_windows_and_stale_disclosure_survive_store_restart() {
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-last-good-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db").to_string_lossy().into_owned();
        let open = || {
            SqliteRouteStateStore::open(&prefrontal_routing_store::dev_descriptor(
                "prefrontal-routing",
                path.clone(),
            ))
            .unwrap()
        };
        let mut state = AccountQuotaState::default();
        let store = open();
        let (rows, completeness) = apply_provider_reply(
            &mut state,
            "claude",
            &reply(vec![claude_entry("a", "2026-09-29T12:00:00Z")], &["claude"]),
            NOON_MS,
        )
        .unwrap();
        store.put_account_quota_poll(&rows, &completeness).unwrap();
        drop(store);
        let store = open();
        let mut restored = restore_account_state(&store);
        let (rows, completeness) = apply_provider_reply(
            &mut restored,
            "claude",
            &reply(vec![error_only("a")], &["claude"]),
            NOON_MS + 27 * 60_000,
        )
        .unwrap();
        store.put_account_quota_poll(&rows, &completeness).unwrap();
        drop(store);
        let store = open();
        let again = restore_account_state(&store);
        assert_eq!(again.readings, restored.readings);
        assert!(view(&report_for_a(&again, NOON_MS + 29 * 60_000), "a").cold_input_usable);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn live_insula_reply_decodes_whole_envelope_with_every_account_field() {
        assert_eq!(
            hex(&Sha256::digest(LIVE_CLAUDE_REPLY)),
            LIVE_CLAUDE_REPLY_SHA256,
            "the fixture must stay the exact captured bytes"
        );
        let envelope = decode_usage_envelope(LIVE_CLAUDE_REPLY).expect("live reply decodes");
        assert_eq!(envelope.complete_providers, vec!["claude".to_string()]);
        assert_eq!(envelope.entries.len(), 5);
        assert_eq!(envelope.unkeyed_entries, 0);
        assert!(envelope.undecodable_entries.is_empty());

        let mut readings = AccountReadings::default();
        let outcome = merge_provider_poll(&mut readings, "claude", &envelope, NOON_MS);
        assert_eq!(outcome.updated.len(), 5);
        let now = parse_rfc3339_ms("2026-09-30T00:46:00Z").unwrap();
        let listed = build_roster(
            &listing(
                &[(
                    "oauth:anthropic",
                    "00000000-0000-4000-8000-000000000001",
                    "account-1@example.invalid",
                )],
                &[("anthropic-native", "list")],
            ),
            None,
            now,
        );
        let report = build_report(
            &readings,
            Some(&listed),
            &RosterPoll::Listed { at_ms: now },
            now,
        );
        assert_eq!(report.accounts.len(), 5);
        assert!(report.providers[0].last_poll_complete);

        let account = view(&report, "00000000-0000-4000-8000-000000000001");
        assert_eq!(account.email.as_deref(), Some("account-1@example.invalid"));
        assert_eq!(
            account.fetched_at.as_deref(),
            Some("2026-09-30T00:44:44.672943000+00:00")
        );
        assert_eq!(account.age.band, AgeBand::Full);
        assert_eq!(account.age.age_ms, Some(now - 1_790_729_084_672));
        assert_eq!(account.stale, None);
        assert_eq!(account.error_class, None);
        let five = account.five_hour.as_ref().expect("5h window");
        assert_eq!((five.slot, five.used_percent), ("primary", 2.0));
        assert_eq!(
            five.resets_at.as_deref(),
            Some("2026-09-30T01:39:59.575908+00:00")
        );
        let seven = account.seven_day.as_ref().expect("7d window");
        assert_eq!((seven.slot, seven.used_percent), ("secondary", 100.0));
        assert_eq!(
            seven.resets_at.as_deref(),
            Some("2026-09-30T13:59:59.575931+00:00")
        );
        assert_eq!(account.scoped_listing, ScopedListing::Present);
        assert_eq!(account.scoped_windows.len(), 1);
        let fable = &account.scoped_windows[0];
        assert_eq!(fable.id.as_deref(), Some("Fable"));
        assert_eq!(fable.window_minutes, Some(SEVEN_DAY_MINUTES));
        assert_eq!(fable.used_percent, Some(25.0));
        assert_eq!(fable.family, Some("fable"));
        assert_eq!(account.family_scopes[0].state, FamilyScopeState::Mapped);
        assert!(account.vault.joined);
        assert_eq!(
            account.vault.credential_ids,
            vec!["oauth:anthropic".to_string()]
        );
        assert!(account.cold_input_usable, "{:?}", account.reasons);
        assert_eq!(
            account.overage_pool, None,
            "no extra_usage pool was published"
        );

        // A five-hour window with no reset is still a valid reading.
        let idle = view(&report, "00000000-0000-4000-8000-000000000002");
        assert_eq!(idle.five_hour.as_ref().unwrap().resets_at, None);
        // The four accounts the roster does not list are gaps.
        assert_eq!(report.gaps.insula_only.len(), 4);
        assert!(report.alarms.iter().all(|alarm| !alarm.contains("scoped")));
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn a_partial_poll_never_clears_a_sibling_and_only_a_complete_one_tombstones() {
        let mut state = AccountQuotaState::default();
        poll(
            &mut state,
            vec![
                claude_entry("acct-a", "2026-09-29T11:59:00Z"),
                claude_entry("acct-b", "2026-09-29T11:58:00Z"),
            ],
            &["claude"],
            NOON_MS,
        );
        // Insula's reply omits acct-b and does not list claude as complete: acct-b
        // is unknown or unfinished, not gone.
        poll(
            &mut state,
            vec![claude_entry("acct-a", "2026-09-29T12:00:30Z")],
            &[],
            NOON_MS + 60_000,
        );
        let kept = &state.readings.readings[&("claude".to_string(), "acct-b".to_string())];
        assert_eq!(kept.tombstoned_at_ms, None);
        assert_eq!(kept.received_at_ms, NOON_MS);
        assert_eq!(kept.fetched_at(), Some("2026-09-29T11:58:00Z"));
        let a = &state.readings.readings[&("claude".to_string(), "acct-a".to_string())];
        assert_eq!(a.fetched_at(), Some("2026-09-29T12:00:30Z"));
        assert!(!state.readings.completeness["claude"].last_poll_complete);
        assert_eq!(
            state.readings.completeness["claude"].last_complete_at_ms,
            Some(NOON_MS)
        );

        // A complete reply without acct-b tombstones it, and still keeps the row.
        poll(
            &mut state,
            vec![claude_entry("acct-a", "2026-09-29T12:01:30Z")],
            &["claude"],
            NOON_MS + 120_000,
        );
        let tombstoned = &state.readings.readings[&("claude".to_string(), "acct-b".to_string())];
        assert_eq!(tombstoned.tombstoned_at_ms, Some(NOON_MS + 120_000));
        assert_eq!(tombstoned.fetched_at(), Some("2026-09-29T11:58:00Z"));
        let report = build_report(
            &state.readings,
            None,
            &RosterPoll::NotPolled,
            NOON_MS + 150_000,
        );
        assert!(view(&report, "acct-b")
            .reasons
            .iter()
            .any(|reason| reason.contains("tombstoned")));

        // An account that returns clears its tombstone.
        poll(
            &mut state,
            vec![
                claude_entry("acct-a", "2026-09-29T12:02:30Z"),
                claude_entry("acct-b", "2026-09-29T12:02:31Z"),
            ],
            &["claude"],
            NOON_MS + 180_000,
        );
        assert_eq!(
            state.readings.readings[&("claude".to_string(), "acct-b".to_string())].tombstoned_at_ms,
            None
        );
    }

    #[test]
    fn duplicate_lanes_resolve_to_one_whole_entry_regardless_of_order() {
        let older = claude_entry("acct-a", "2026-09-29T11:50:00Z");
        let mut newer = claude_entry("acct-a", "2026-09-29T11:59:00Z");
        newer["source"] = json!("oauth");
        for entries in [
            vec![older.clone(), newer.clone()],
            vec![newer.clone(), older.clone()],
        ] {
            let mut state = AccountQuotaState::default();
            poll(&mut state, entries, &["claude"], NOON_MS);
            let kept = &state.readings.readings[&("claude".to_string(), "acct-a".to_string())];
            assert_eq!(kept.entry, newer);
        }
    }

    #[test]
    fn readings_restore_after_restart_with_their_source_dates_and_survive_a_partial_first_poll() {
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-account-quota-restore-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.db").to_string_lossy().into_owned();
        let open = || {
            SqliteRouteStateStore::open(&prefrontal_routing_store::dev_descriptor(
                "prefrontal-routing",
                path.clone(),
            ))
            .expect("open store")
        };
        let before = {
            let store = open();
            let mut state = AccountQuotaState::default();
            let (rows, completeness) =
                apply_provider_reply(&mut state, "claude", LIVE_CLAUDE_REPLY, NOON_MS).unwrap();
            store.put_account_quota_poll(&rows, &completeness).unwrap();
            let roster_row = apply_roster_listing(
                &mut state,
                &listing(
                    &[(
                        "oauth:anthropic",
                        "00000000-0000-4000-8000-000000000001",
                        "x",
                    )],
                    &[("anthropic-native", "list")],
                ),
                NOON_MS,
            )
            .unwrap();
            store.put_account_vault_roster(&roster_row).unwrap();
            state
        };

        let store = open();
        let mut restored = restore_account_state(&store);
        assert_eq!(restored.restored_readings, 5);
        assert_eq!(
            restored.readings, before.readings,
            "entries, source dates and receipt times restore exactly"
        );
        assert_eq!(restored.roster, before.roster);

        // The first poll after the restart is partial and names one account.
        let one = serde_json::from_slice::<Value>(LIVE_CLAUDE_REPLY).unwrap()["result"][0].clone();
        let (rows, completeness) = apply_provider_reply(
            &mut restored,
            "claude",
            &reply(vec![one], &[]),
            NOON_MS + 60_000,
        )
        .unwrap();
        store.put_account_quota_poll(&rows, &completeness).unwrap();
        assert_eq!(restored.readings.readings.len(), 5);
        assert!(restored
            .readings
            .readings
            .values()
            .all(|reading| reading.tombstoned_at_ms.is_none()));
        drop(store);
        let again = restore_account_state(&open());
        assert_eq!(again.readings.readings.len(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn producer_dates_parse_at_any_fraction_precision_and_offset() {
        assert_eq!(parse_rfc3339_ms("2026-09-29T12:00:00Z"), Some(NOON_MS));
        assert_eq!(
            parse_rfc3339_ms("2026-09-29T12:00:00.123456789+00:00"),
            Some(NOON_MS + 123)
        );
        assert_eq!(
            parse_rfc3339_ms("2026-09-29T14:00:00.5+02:00"),
            Some(NOON_MS + 500)
        );
        assert_eq!(parse_rfc3339_ms("2026-09-29T07:00:00-05:00"), Some(NOON_MS));
        assert_eq!(parse_rfc3339_ms("2026-09-29T12:00:00"), None);
        assert_eq!(parse_rfc3339_ms("not a date"), None);
    }

    #[test]
    fn unfiltered_poll_discovers_providers_and_only_explicit_completeness_tombstones() {
        let mut state = AccountQuotaState::default();
        let mut codex = claude_entry("codex-account", "2026-09-29T12:00:00Z");
        codex["provider"] = serde_json::json!("codex");
        codex["apiProvider"] = serde_json::json!("openai");
        let mut ag = codex.clone();
        ag["provider"] = serde_json::json!("antigravity");
        ag["apiProvider"] = serde_json::json!("google");
        let (rows, completeness) = apply_usage_reply(
            &mut state,
            &reply(
                vec![
                    claude_entry("claude-account", "2026-09-29T12:00:00Z"),
                    codex,
                    ag,
                ],
                &["claude", "codex", "antigravity", "empty-provider"],
            ),
            1,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(completeness.len(), 4);
        assert!(state.readings.completeness.contains_key("empty-provider"));
        apply_usage_reply(&mut state, &reply(vec![], &[]), 2).unwrap();
        assert!(state
            .readings
            .readings
            .values()
            .all(|reading| reading.tombstoned_at_ms.is_none()));
        let (rows, _) = apply_usage_reply(&mut state, &reply(vec![], &["codex"]), 3).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            state.readings.readings[&("codex".into(), "codex-account".into())].tombstoned_at_ms,
            Some(3)
        );
        assert!(
            state.readings.readings[&("claude".into(), "claude-account".into())]
                .tombstoned_at_ms
                .is_none()
        );
    }

    #[test]
    fn age_bands_follow_source_time_with_linear_discount_and_exact_bounds() {
        let band = |age_ms: i64| age_band(Some(NOON_MS - age_ms), NOON_MS);
        assert_eq!(age_band(None, NOON_MS).band, AgeBand::Missing);
        assert_eq!((band(0).band, band(0).factor), (AgeBand::Full, 1.0));
        assert_eq!(band(FULL_BAND_MS - 1).band, AgeBand::Full);
        assert_eq!(
            (band(FULL_BAND_MS).band, band(FULL_BAND_MS).factor),
            (AgeBand::Discounted, 1.0)
        );
        let middle = band(FULL_BAND_MS + (EXCLUDED_BAND_MS - FULL_BAND_MS) / 2);
        assert_eq!(middle.band, AgeBand::Discounted);
        assert!((middle.factor - 0.625).abs() < 1e-9, "{}", middle.factor);
        let last = band(EXCLUDED_BAND_MS - 1);
        assert_eq!(last.band, AgeBand::Discounted);
        assert!(last.factor > DISCOUNT_FLOOR && last.factor < DISCOUNT_FLOOR + 1e-6);
        assert_eq!(
            (band(EXCLUDED_BAND_MS).band, band(EXCLUDED_BAND_MS).factor),
            (AgeBand::Excluded, 0.0)
        );
        // Slightly ahead of routing's clock is tolerated; beyond 60 s it is not.
        assert_eq!(band(-FUTURE_SKEW_MS).band, AgeBand::Full);
        assert_eq!(band(-FUTURE_SKEW_MS - 1).band, AgeBand::ClockInvalid);

        // A reading backing off for 19 minutes stays usable; at 20 it is excluded.
        let mut state = AccountQuotaState::default();
        poll(
            &mut state,
            vec![claude_entry("acct-a", "2026-09-29T12:00:00Z")],
            &["claude"],
            NOON_MS,
        );
        let roster = build_roster(
            &listing(
                &[("oauth:anthropic", "acct-a", "e")],
                &[("anthropic-native", "list")],
            ),
            None,
            NOON_MS,
        );
        let at = |age_min: i64| {
            let now = NOON_MS + age_min * 60_000;
            build_report(
                &state.readings,
                Some(&roster),
                &RosterPoll::Listed { at_ms: now },
                now,
            )
        };
        assert!(view(&at(19), "acct-a").cold_input_usable);
        let excluded = at(20);
        let excluded = view(&excluded, "acct-a");
        assert!(!excluded.cold_input_usable);
        assert!(excluded
            .reasons
            .iter()
            .any(|reason| reason.contains("source age limit reached")));
    }

    #[test]
    fn an_insula_account_the_vault_does_not_list_is_a_gap_and_the_join_ignores_email() {
        let mut state = AccountQuotaState::default();
        poll(
            &mut state,
            vec![
                claude_entry("acct-a", "2026-09-29T12:00:00Z"),
                claude_entry("acct-b", "2026-09-29T12:00:00Z"),
            ],
            &["claude"],
            NOON_MS,
        );
        // acct-b's email matches a vault row, but the vault row names another
        // account id, so it must not join.
        let roster = build_roster(
            &listing(
                &[
                    ("oauth:anthropic:second", "acct-a", "acct-a@example.test"),
                    ("oauth:anthropic", "acct-a", "acct-a@example.test"),
                    ("oauth:anthropic:other", "acct-c", "acct-b@example.test"),
                ],
                &[("anthropic-native", "list")],
            ),
            None,
            NOON_MS - FIRST_READING_ALARM_MS,
        );
        let report = build_report(
            &state.readings,
            Some(&roster),
            &RosterPoll::Listed { at_ms: NOON_MS },
            NOON_MS,
        );
        let joined = view(&report, "acct-a");
        assert!(joined.vault.joined);
        assert_eq!(
            joined.vault.credential_ids,
            vec![
                "oauth:anthropic".to_string(),
                "oauth:anthropic:second".to_string()
            ],
            "the default record first, then byte order"
        );
        let unjoined = view(&report, "acct-b");
        assert!(!unjoined.vault.joined);
        assert!(!unjoined.cold_input_usable);
        assert_eq!(report.gaps.insula_only.len(), 1);
        assert_eq!(report.gaps.insula_only[0].account_id, "acct-b");
        assert!(report.gaps.insula_only[0]
            .reason
            .contains("lists no anthropic identity"));
        assert_eq!(report.gaps.vault_only.len(), 1);
        assert_eq!(report.gaps.vault_only[0].account_id, "acct-c");
        assert!(report.gaps.vault_only[0].first_reading_overdue);
        assert!(report.alarms.iter().any(|alarm| alarm.contains("acct-c")));
    }

    #[test]
    fn without_a_list_grant_the_gap_says_the_roster_is_not_granted_yet() {
        let mut state = AccountQuotaState::default();
        poll(
            &mut state,
            vec![claude_entry("acct-a", "2026-09-29T12:00:00Z")],
            &["claude"],
            NOON_MS,
        );
        // The grant routing holds today: one exact read grant, no list grant.
        let today = ScopedCredentialListing {
            credentials: Vec::new(),
            grant_tuples: vec![ListedGrant {
                selector_kind: "exact".into(),
                selector: "apikey:artificial-analysis".into(),
                operation: "read".into(),
            }],
            view: "view".into(),
            ..Default::default()
        };
        let roster = build_roster(&today, None, NOON_MS);
        let report = build_report(
            &state.readings,
            Some(&roster),
            &RosterPoll::Listed { at_ms: NOON_MS },
            NOON_MS,
        );
        assert_eq!(report.gaps.insula_only.len(), 1);
        assert!(
            report.gaps.insula_only[0]
                .reason
                .starts_with("vault roster not granted yet"),
            "{}",
            report.gaps.insula_only[0].reason
        );
    }

    #[test]
    fn a_roster_older_than_a_day_is_not_used_and_a_vault_outage_is_not_a_refusal() {
        let mut state = AccountQuotaState::default();
        poll(
            &mut state,
            vec![claude_entry("acct-a", "2026-09-29T12:00:00Z")],
            &["claude"],
            NOON_MS,
        );
        let roster = build_roster(
            &listing(
                &[("oauth:anthropic", "acct-a", "e")],
                &[("anthropic-native", "list")],
            ),
            None,
            NOON_MS - ROSTER_MAX_AGE_MS,
        );
        let outage = roster_poll_failure(&CredentialResolverError::transport("down"), NOON_MS);
        assert!(matches!(outage, RosterPoll::Unavailable { .. }));
        let report = build_report(&state.readings, Some(&roster), &outage, NOON_MS);
        assert!(!report.roster.used_for_join);
        assert!(report.gaps.insula_only[0]
            .reason
            .contains("older than 24 hours"));
        let refusal = roster_poll_failure(
            &CredentialResolverError::new(CredentialErrorClass::Unauthorized, "denied"),
            NOON_MS,
        );
        assert!(matches!(refusal, RosterPoll::Refused { .. }));
    }

    #[test]
    fn degraded_and_unknown_stale_readings_are_not_usable_but_upstream_failed_is() {
        let roster = build_roster(
            &listing(
                &[
                    ("oauth:anthropic", "ok", "e"),
                    ("oauth:anthropic:b", "degraded", "e"),
                    ("oauth:anthropic:c", "odd", "e"),
                ],
                &[("anthropic-native", "list")],
            ),
            None,
            NOON_MS,
        );
        let mut preserved = claude_entry("ok", "2026-09-29T11:50:00Z");
        preserved["stale"] = json!({ "since": "2026-09-29T11:51:00Z", "class": "upstream_failed" });
        let mut degraded = claude_entry("degraded", "2026-09-29T11:59:00Z");
        degraded["errorClass"] = json!("credential_unusable");
        degraded["error"] = json!("re-auth required");
        let mut odd = claude_entry("odd", "2026-09-29T11:59:00Z");
        odd["stale"] = json!({ "since": "2026-09-29T11:59:30Z", "class": "rejected" });
        let mut state = AccountQuotaState::default();
        poll(
            &mut state,
            vec![preserved, degraded, odd],
            &["claude"],
            NOON_MS,
        );
        let report = build_report(
            &state.readings,
            Some(&roster),
            &RosterPoll::Listed { at_ms: NOON_MS },
            NOON_MS,
        );
        let ok = view(&report, "ok");
        assert!(ok.cold_input_usable, "{:?}", ok.reasons);
        assert_eq!(ok.age.band, AgeBand::Discounted);
        assert!(!view(&report, "degraded").cold_input_usable);
        assert_eq!(
            view(&report, "degraded").error_class.as_deref(),
            Some("credential_unusable")
        );
        let odd = view(&report, "odd");
        assert!(!odd.cold_input_usable);
        assert!(odd
            .reasons
            .iter()
            .any(|reason| reason.contains("'rejected'")));
    }

    #[test]
    fn fable_scope_maps_by_id_and_duration_and_an_unknown_id_is_never_guessed() {
        let roster = build_roster(
            &listing(
                &[
                    ("oauth:anthropic", "renamed", "e"),
                    ("oauth:anthropic:b", "wrong-length", "e"),
                    ("oauth:anthropic:c", "empty", "e"),
                    ("oauth:anthropic:d", "absent", "e"),
                    ("oauth:anthropic:e", "spent", "e"),
                ],
                &[("anthropic-native", "list")],
            ),
            None,
            NOON_MS,
        );
        let mut renamed = claude_entry("renamed", "2026-09-29T12:00:00Z");
        renamed["usage"]["extraRateWindows"][0]["id"] = json!("claude-fable-6");
        renamed["usage"]["extraRateWindows"][0]["title"] = json!("7 Day (Fable)");
        let mut wrong_length = claude_entry("wrong-length", "2026-09-29T12:00:00Z");
        wrong_length["usage"]["extraRateWindows"][0]["window"]["windowMinutes"] = json!(300);
        let mut empty = claude_entry("empty", "2026-09-29T12:00:00Z");
        empty["usage"]["extraRateWindows"] = json!([]);
        let mut absent = claude_entry("absent", "2026-09-29T12:00:00Z");
        absent["usage"]
            .as_object_mut()
            .unwrap()
            .remove("extraRateWindows");
        let mut spent = claude_entry("spent", "2026-09-29T12:00:00Z");
        spent["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(100.0);
        spent["spend"] = json!([{ "id": "extra_usage", "spendable": true }]);
        let mut state = AccountQuotaState::default();
        poll(
            &mut state,
            vec![renamed, wrong_length, empty, absent, spent],
            &["claude"],
            NOON_MS,
        );
        let report = build_report(
            &state.readings,
            Some(&roster),
            &RosterPoll::Listed { at_ms: NOON_MS },
            NOON_MS,
        );

        for account in ["renamed", "wrong-length"] {
            let view = view(&report, account);
            assert!(
                view.cold_input_usable,
                "general work is unaffected: {:?}",
                view.reasons
            );
            assert_eq!(view.family_scopes[0].state, FamilyScopeState::Unknown);
            assert!(!view.family_scopes[0].cold_input_usable);
            assert!(report.alarms.iter().any(|alarm| alarm.contains(account)));
        }
        let empty = view(&report, "empty");
        assert_eq!(empty.scoped_listing, ScopedListing::ExplicitEmpty);
        assert_eq!(empty.family_scopes[0].state, FamilyScopeState::NotPublished);
        assert!(empty.family_scopes[0].cold_input_usable);
        let absent = view(&report, "absent");
        assert_eq!(absent.scoped_listing, ScopedListing::Missing);
        assert!(absent.family_scopes[0].cold_input_usable);
        let spent = view(&report, "spent");
        assert!(spent.cold_input_usable);
        assert!(!spent.family_scopes[0].cold_input_usable);
        assert_eq!(
            spent.overage_pool,
            Some(json!({ "id": "extra_usage", "spendable": true }))
        );

        assert_eq!(
            scope_family_for_model("anthropic/claude-fable-5-1[1m]").map(|f| f.family),
            Some("fable")
        );
        assert_eq!(
            scope_family_for_model("claude-fable-5").map(|f| f.family),
            Some("fable")
        );
        assert!(scope_family_for_model("claude-opus-5").is_none());
    }

    #[test]
    fn a_malformed_envelope_is_rejected_whole_and_a_malformed_entry_alone() {
        assert!(decode_usage_envelope(br#"{"completeProviders":["claude"]}"#).is_err());
        assert!(decode_usage_envelope(br#"{"result":[],"completeProviders":"claude"}"#).is_err());
        let decoded = decode_usage_envelope(
            &reply(
                vec![
                    claude_entry("acct-a", "2026-09-29T12:00:00Z"),
                    json!({ "provider": "claude", "account": "acct-b", "usage": { "primary": { "usedPercent": "high" } } }),
                    json!({ "provider": "claude", "usage": {} }),
                ],
                &["claude"],
            ),
        )
        .unwrap();
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.undecodable_entries.len(), 1);
        assert_eq!(decoded.unkeyed_entries, 1);

        // The undecodable entry may be acct-b's, so its stored reading is not
        // tombstoned even though the reply calls claude complete.
        let mut readings = AccountReadings::default();
        let first = decode_usage_envelope(&reply(
            vec![
                claude_entry("acct-a", "2026-09-29T11:59:00Z"),
                claude_entry("acct-b", "2026-09-29T11:59:00Z"),
            ],
            &["claude"],
        ))
        .unwrap();
        merge_provider_poll(&mut readings, "claude", &first, NOON_MS);
        let outcome = merge_provider_poll(&mut readings, "claude", &decoded, NOON_MS + 60_000);
        assert!(outcome.tombstoned.is_empty());
        assert_eq!(
            readings.readings[&("claude".to_string(), "acct-b".to_string())].tombstoned_at_ms,
            None
        );
    }
}
