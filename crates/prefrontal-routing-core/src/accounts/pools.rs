//! Quota-pool identities, the blocks keyed by them, and the account class
//! frozen at selection.
//!
//! A pool key is (window length in minutes, model scope, pool name). The only
//! cross-source equivalences are the unnamed keys: a report's `five_hour` and
//! `seven_day`, a header's `5h` and `7d`, and a reading's aggregate window of
//! 300 or 10080 minutes all key as `(300 | 10080, every model, no name)`. Two
//! named pools are distinct whenever their names differ, even at equal length
//! and scope, and a name whose length nobody stated keys with an unknown length.
//!
//! A block is keyed by (account, pool key); a block with no pool key covers
//! the whole account and carries a recovery set. Readings are ordered by their
//! observation time, never by arrival.

use serde::{Deserialize, Serialize};

use super::eligibility::age_band;
use super::types::{
    AllocatorPolicy, EpochMs, Fence, FenceScope, GeneralWindow, ModelFamily, QuotaReading,
    ScopedList,
};
use super::AgeBand;

/// The longest pool name a reading or report may give.
pub const POOL_NAME_MAX_LEN: usize = 64;

/// A pool name is 1-64 characters of `[a-z0-9._-]`. Any well-formed name is
/// accepted, including one routing has never seen; it is compared exactly.
pub fn validate_pool_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > POOL_NAME_MAX_LEN {
        return Err(format!(
            "a pool name is 1-{POOL_NAME_MAX_LEN} characters, got {}",
            name.len()
        ));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-')))
    {
        return Err(format!("a pool name uses only [a-z0-9._-], found {bad:?}"));
    }
    Ok(())
}

/// Scope supplied by quota evidence, independent of a provider's spelling.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PoolScope {
    EveryModel,
    Family(ModelFamily),
    Models(std::collections::BTreeSet<String>),
}

impl PoolScope {
    /// The scope evidence about `family` names: that family, or every model
    /// when the model belongs to no family the policy knows.
    pub fn for_family(family: ModelFamily) -> Self {
        match family {
            ModelFamily::Other => PoolScope::EveryModel,
            family => PoolScope::Family(family),
        }
    }

    /// Whether a block with this scope restricts `model` of `family`.
    pub fn applies_to(&self, model: &str, family: ModelFamily) -> bool {
        match self {
            PoolScope::EveryModel => true,
            PoolScope::Family(f) => *f == family,
            PoolScope::Models(models) => models.contains(model),
        }
    }

    /// Whether a reading window with this scope reports on every model that
    /// `other` names, so its value speaks for a block scoped to `other`.
    fn covers(&self, other: &PoolScope) -> bool {
        match (self, other) {
            (PoolScope::EveryModel, _) => true,
            (PoolScope::Family(a), PoolScope::Family(b)) => a == b,
            (PoolScope::Models(a), PoolScope::Models(b)) => b.is_subset(a),
            _ => false,
        }
    }

    /// Whether the two scopes have any model in common.
    fn overlaps(&self, other: &PoolScope) -> bool {
        match (self, other) {
            (PoolScope::EveryModel, _) | (_, PoolScope::EveryModel) => true,
            (PoolScope::Family(a), PoolScope::Family(b)) => a == b,
            (PoolScope::Models(a), PoolScope::Models(b)) => !a.is_disjoint(b),
            // Routing cannot tell which family a listed model belongs to here,
            // so it assumes they may share one.
            _ => true,
        }
    }
}

/// A named pool remains distinct from every other name, even at equal length.
/// `None` for the length means unknown, not an aggregate of known windows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PoolKey {
    pub window_minutes: Option<u32>,
    pub scope: PoolScope,
    pub name: Option<String>,
}

impl From<FenceScope> for PoolKey {
    /// The key a stored pre-pool fence scope migrates to: a general window
    /// keeps its length for every model, and a family or unknown-scope fence
    /// becomes an unnamed key of unknown length for its family.
    fn from(scope: FenceScope) -> Self {
        let (window_minutes, scope) = match scope {
            FenceScope::General(window) => (Some(window.window_minutes()), PoolScope::EveryModel),
            FenceScope::Family(family) | FenceScope::UnknownScope(family) => {
                (None, PoolScope::Family(family))
            }
        };
        Self {
            window_minutes,
            scope,
            name: None,
        }
    }
}

impl PoolKey {
    /// An unnamed window of every model, such as a header's `7d`.
    pub fn general(window: GeneralWindow) -> Self {
        Self {
            window_minutes: Some(window.window_minutes()),
            scope: PoolScope::EveryModel,
            name: None,
        }
    }

    /// The key an exhaustion report names, or `None` when it names no pool,
    /// which blocks the whole account. `pool` is the report's pool member and
    /// `limit_window` the window it names: `five_hour` and `seven_day` give a
    /// length, and any other value is itself the pool's name, of unknown
    /// length. A named pool is scoped to the reported model's family. A value
    /// that is not a well-formed pool name is ignored, so a report naming
    /// nothing routing can key blocks the whole account.
    pub fn for_report(
        limit_window: Option<&str>,
        pool: Option<&str>,
        family: ModelFamily,
    ) -> Option<Self> {
        let length = match limit_window {
            Some("five_hour") => Some(GeneralWindow::FiveHour.window_minutes()),
            Some("seven_day") => Some(GeneralWindow::SevenDay.window_minutes()),
            _ => None,
        };
        let pool = pool.filter(|name| validate_pool_name(name).is_ok());
        let named_window = limit_window
            .filter(|_| length.is_none())
            .filter(|name| validate_pool_name(name).is_ok());
        match (pool.or(named_window), length) {
            (Some(name), window_minutes) => Some(Self {
                window_minutes,
                scope: PoolScope::for_family(family),
                name: Some(name.to_string()),
            }),
            (None, Some(window_minutes)) => Some(Self {
                window_minutes: Some(window_minutes),
                scope: PoolScope::EveryModel,
                name: None,
            }),
            (None, None) => None,
        }
    }

    /// Whether a block on this key restricts a request for `model` of
    /// `family` that draws on `request_pool`. A named pool restricts every
    /// model in its scope except one known to draw on a different pool.
    pub fn applies_to(&self, model: &str, family: ModelFamily, request_pool: Option<&str>) -> bool {
        self.scope.applies_to(model, family)
            && match (&self.name, request_pool) {
                (Some(name), Some(pool)) => name == pool,
                _ => true,
            }
    }

    /// Whether the reading window `reported` speaks for this key. A named key
    /// is matched by name and scope, and by length only when the key states
    /// one. An unnamed key of known length is matched by length and scope. An
    /// unnamed key of unknown length, as migrated family fences are, is
    /// matched by every window whose scope shares a model with it.
    fn matched_by(&self, reported: &PoolKey) -> bool {
        match (&self.name, self.window_minutes) {
            (Some(name), minutes) => {
                reported.name.as_ref() == Some(name)
                    && reported.scope.covers(&self.scope)
                    && minutes.is_none_or(|m| reported.window_minutes == Some(m))
            }
            (None, Some(minutes)) => {
                reported.name.is_none()
                    && reported.window_minutes == Some(minutes)
                    && reported.scope.covers(&self.scope)
            }
            (None, None) => reported.scope.overlaps(&self.scope),
        }
    }
}

/// One aggregate or named pool window of an account's whole reading, before
/// the reading is narrowed to the windows one model draws on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolWindowReading {
    /// The pool the reading names for this window; `None` for an aggregate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    pub window_minutes: u32,
    pub used_percent: f64,
}

/// One window a reading reports, as block recovery reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct ReportedWindow {
    pub key: PoolKey,
    /// The window's spendable share is above its reserve.
    pub above_reserve: bool,
}

fn reserve_for_minutes(minutes: u32, policy: &AllocatorPolicy) -> f64 {
    if minutes == GeneralWindow::FiveHour.window_minutes() {
        policy.reserve_five_hour
    } else {
        policy.reserve_seven_day
    }
}

fn above(used_percent: f64, reserve: f64) -> bool {
    used_percent.is_finite()
        && (0.0..=100.0).contains(&used_percent)
        && 100.0 - used_percent - reserve > 0.0
}

/// Every window `reading` reports, keyed by pool. Aggregate and named pool
/// windows come from the whole reading when it carries them; scoped windows
/// the policy maps to a family key as that family. A scoped window routing
/// cannot map is left out: it names no pool routing can key.
pub fn reported_windows(reading: &QuotaReading, policy: &AllocatorPolicy) -> Vec<ReportedWindow> {
    let mut out = Vec::new();
    match &reading.pool_windows {
        Some(windows) => {
            for window in windows {
                out.push(ReportedWindow {
                    key: PoolKey {
                        window_minutes: Some(window.window_minutes),
                        scope: PoolScope::EveryModel,
                        name: window.pool.clone(),
                    },
                    above_reserve: above(
                        window.used_percent,
                        reserve_for_minutes(window.window_minutes, policy),
                    ),
                });
            }
        }
        None => {
            for id in reading.required_windows() {
                if let Some(window) = reading.window(id) {
                    out.push(ReportedWindow {
                        key: PoolKey {
                            window_minutes: Some(window.window_minutes),
                            scope: PoolScope::EveryModel,
                            name: None,
                        },
                        above_reserve: above(
                            window.used_percent,
                            reserve_for_minutes(window.window_minutes, policy),
                        ),
                    });
                }
            }
        }
    }
    if let ScopedList::Listed(windows) = &reading.scoped {
        for window in windows {
            if let Some(family) = policy.family_for_scope(window) {
                out.push(ReportedWindow {
                    key: PoolKey {
                        window_minutes: Some(window.window_minutes),
                        scope: PoolScope::Family(family),
                        name: None,
                    },
                    above_reserve: above(window.used_percent, policy.reserve_scoped),
                });
            }
        }
    }
    out
}

/// A reading recovery may act on: no entry-level error, a usable timestamp,
/// and younger than the excluded age band (20 minutes by default).
pub fn fresh_for_recovery(reading: &QuotaReading, now: EpochMs, policy: &AllocatorPolicy) -> bool {
    let stale_ok = reading
        .stale
        .as_ref()
        .and_then(|stale| stale.class.as_deref())
        .is_none_or(|class| class == "upstream_failed");
    reading.error_class.is_none()
        && stale_ok
        && matches!(
            age_band(reading.observed_at_ms, now, policy),
            Ok((AgeBand::Full | AgeBand::Discounted, _, _))
        )
}

/// The pool keys a whole-account block must see above reserve before a
/// reading lifts it: those of the account's latest fresh reading, or none
/// when it has no fresh reading.
pub fn recovery_set_from(
    reading: Option<&QuotaReading>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> Vec<PoolKey> {
    let Some(reading) = reading.filter(|reading| fresh_for_recovery(reading, now, policy)) else {
        return Vec::new();
    };
    let mut keys: Vec<PoolKey> = reported_windows(reading, policy)
        .into_iter()
        .map(|window| window.key)
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// Why a block no longer restricts its account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockEnd {
    /// Its known reset passed. The next admission is the post-reset trial
    /// run; a new confirmed exhaustion blocks the account again.
    ResetPassed,
    /// It had no known reset and its retry time came: the Retry-After hint,
    /// or `unknown_reset_retry_ms` after confirmation.
    RetryDue,
    /// A fresh reading observed after the confirmation shows headroom.
    Reading,
}

/// Whether a reading meeting the ordering and freshness conditions may be
/// considered for lifting `fence` at all.
fn later_fresh_reading<'r>(
    fence: &Fence,
    reading: Option<&'r QuotaReading>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> Option<&'r QuotaReading> {
    reading.filter(|reading| {
        // Ordered by observation time: an older reading arriving late, or one
        // observed at the same instant as the exhaustion, never lifts it.
        reading
            .observed_at_ms
            .is_some_and(|observed| observed > fence.observed_at_ms)
            && fresh_for_recovery(reading, now, policy)
    })
}

/// Whether `reading` reports `key`, with every matching window above reserve.
fn reports_above(key: &PoolKey, windows: &[ReportedWindow]) -> bool {
    let mut matching = windows
        .iter()
        .filter(|window| key.matched_by(&window.key))
        .peekable();
    matching.peek().is_some() && matching.all(|window| window.above_reserve)
}

/// When `fence` stops restricting its account, or `None` while it stands.
/// `reading` is the account's latest whole reading.
pub fn block_end(
    fence: &Fence,
    reading: Option<&QuotaReading>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> Option<BlockEnd> {
    // Reset dates schedule a trial, not evidence of recovery. Only a later,
    // fresh reading with reserve-aware headroom can retire quota evidence.
    let reading = later_fresh_reading(fence, reading, now, policy)?;
    let windows = reported_windows(reading, policy);
    let lifted = match &fence.pool {
        Some(key) => reports_above(key, &windows),
        // A whole-account block lifts only when the reading reports at least
        // one window, every window it reports is above reserve, and it
        // reports every key of the recovery set; a partial reading never does.
        None => {
            !windows.is_empty()
                && windows.iter().all(|window| window.above_reserve)
                && fence
                    .recovery_set
                    .iter()
                    .all(|key| reports_above(key, &windows))
        }
    };
    lifted.then_some(BlockEnd::Reading)
}

/// The recovery set of a whole-account block after a later fresh reading
/// adds the keys it reports, or `None` when it adds none. A key never leaves
/// the set here: only a reading that reports a window disabled may remove it.
pub fn grown_recovery_set(
    fence: &Fence,
    reading: Option<&QuotaReading>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> Option<Vec<PoolKey>> {
    if fence.pool.is_some() {
        return None;
    }
    let reading = later_fresh_reading(fence, reading, now, policy)?;
    let mut set = fence.recovery_set.clone();
    for window in reported_windows(reading, policy) {
        if !set.contains(&window.key) {
            set.push(window.key);
        }
    }
    if set.len() == fence.recovery_set.len() {
        return None;
    }
    set.sort();
    Some(set)
}

/// Authority recorded once with a selection. Account lookup must not infer it
/// again from a consumer's current configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllowedAccountClass {
    PricedPerTokenOnly,
    #[default]
    NotPayPerToken,
}

impl AllowedAccountClass {
    pub fn from_pay_per_token(value: bool) -> Self {
        if value {
            Self::PricedPerTokenOnly
        } else {
            Self::NotPayPerToken
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PricedPerTokenOnly => "priced_per_token_only",
            Self::NotPayPerToken => "not_pay_per_token",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "priced_per_token_only" => Ok(Self::PricedPerTokenOnly),
            "not_pay_per_token" => Ok(Self::NotPayPerToken),
            _ => Err(format!("unknown allowed account class: {value}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{ConfirmationSource, ResetTime, ScopedWindow};

    const NOW: EpochMs = 1_790_683_200_000;
    const MINUTE: i64 = 60_000;
    const HOUR: i64 = 60 * MINUTE;

    fn key(minutes: Option<u32>, scope: PoolScope, name: Option<&str>) -> PoolKey {
        PoolKey {
            window_minutes: minutes,
            scope,
            name: name.map(str::to_string),
        }
    }

    fn block(pool: Option<PoolKey>, reset: ResetTime) -> Fence {
        Fence {
            account_id: "acct".into(),
            pool,
            recovery_set: Vec::new(),
            observed_at_ms: NOW - 10 * MINUTE,
            reset,
            retry_hint_ms: None,
            source: ConfirmationSource::BrocaAccountLimit,
        }
    }

    /// A reading observed at `observed_at` reporting these pool windows.
    fn reading(observed_at: EpochMs, windows: &[(Option<&str>, u32, f64)]) -> QuotaReading {
        QuotaReading {
            observed_at_ms: Some(observed_at),
            error_class: None,
            stale: None,
            five_hour: None,
            seven_day: None,
            general_windows: Some(Vec::new()),
            scoped: ScopedList::Missing,
            pool_windows: Some(
                windows
                    .iter()
                    .map(|(pool, minutes, used)| PoolWindowReading {
                        pool: pool.map(str::to_string),
                        window_minutes: *minutes,
                        used_percent: *used,
                    })
                    .collect(),
            ),
        }
    }

    fn ends(fence: &Fence, reading: &QuotaReading) -> Option<BlockEnd> {
        block_end(fence, Some(reading), NOW, &AllocatorPolicy::default())
    }

    #[test]
    fn quota_fence_has_no_fixed_end_without_headroom() {
        for reset in [
            ResetTime::Missing,
            ResetTime::Unparsable,
            ResetTime::At(NOW - 1),
        ] {
            let fence = block(None, reset);
            assert_eq!(
                block_end(
                    &fence,
                    None,
                    NOW + 30 * 24 * HOUR,
                    &AllocatorPolicy::default()
                ),
                None
            );
        }
        let fence = block(None, ResetTime::At(NOW + 30 * 24 * HOUR));
        assert_eq!(
            ends(
                &fence,
                &reading(NOW, &[(None, 300, 0.0), (None, 10080, 0.0)])
            ),
            Some(BlockEnd::Reading)
        );
        let policy = AllocatorPolicy {
            reserve_seven_day: 5.0,
            ..Default::default()
        };
        assert_eq!(
            block_end(
                &fence,
                Some(&reading(NOW, &[(None, 300, 0.0), (None, 10080, 96.0)])),
                NOW,
                &policy
            ),
            None,
            "weekly reserve remains binding"
        );
    }

    #[test]
    fn legacy_scopes_have_unnamed_pool_identities() {
        for (window, minutes) in [
            (GeneralWindow::FiveHour, 300),
            (GeneralWindow::SevenDay, 10080),
            (GeneralWindow::Reported(90), 90),
        ] {
            assert_eq!(
                PoolKey::from(FenceScope::General(window)),
                key(Some(minutes), PoolScope::EveryModel, None)
            );
        }
        for scope in [
            FenceScope::Family(ModelFamily::Opus),
            FenceScope::UnknownScope(ModelFamily::Opus),
        ] {
            assert_eq!(
                PoolKey::from(scope),
                key(None, PoolScope::Family(ModelFamily::Opus), None)
            );
        }
    }

    #[test]
    fn pool_names_are_one_to_sixty_four_characters_of_the_closed_alphabet() {
        assert!(validate_pool_name("gemini").is_ok());
        assert!(validate_pool_name("3p.weekly_v-2").is_ok());
        assert!(validate_pool_name(&"a".repeat(64)).is_ok());
        assert!(validate_pool_name("").is_err());
        assert!(validate_pool_name(&"a".repeat(65)).is_err());
        assert!(validate_pool_name("Gemini").is_err());
        assert!(validate_pool_name("gemini pool").is_err());
        assert!(validate_pool_name("gemini/5h").is_err());
    }

    #[test]
    fn reports_key_their_windows_and_a_report_naming_no_pool_keys_nothing() {
        let opus = ModelFamily::Opus;
        assert_eq!(
            PoolKey::for_report(Some("five_hour"), None, opus),
            Some(key(Some(300), PoolScope::EveryModel, None))
        );
        assert_eq!(
            PoolKey::for_report(Some("seven_day"), None, opus),
            Some(key(Some(10080), PoolScope::EveryModel, None))
        );
        // Any other window name is a pool of unknown length, scoped to the
        // reported model's family; this replaces the old unclassified outcome.
        assert_eq!(
            PoolKey::for_report(Some("x"), None, opus),
            Some(key(None, PoolScope::Family(opus), Some("x")))
        );
        assert_eq!(
            PoolKey::for_report(Some("seven_day"), Some("gemini"), ModelFamily::Other),
            Some(key(Some(10080), PoolScope::EveryModel, Some("gemini")))
        );
        assert_eq!(PoolKey::for_report(None, None, opus), None);
        assert_eq!(PoolKey::for_report(Some("Not A Pool"), None, opus), None);
    }

    #[test]
    fn a_named_pool_block_restricts_only_its_scope_and_its_own_pool() {
        let x = block(
            Some(key(None, PoolScope::Family(ModelFamily::Opus), Some("x"))),
            ResetTime::Missing,
        );
        assert!(x.applies_to("claude-opus-5-5", ModelFamily::Opus, None));
        assert!(!x.applies_to("claude-fable-5", ModelFamily::Fable, None));
        let gemini = block(
            Some(key(Some(300), PoolScope::EveryModel, Some("gemini"))),
            ResetTime::Missing,
        );
        assert!(gemini.applies_to(
            "antigravity-gemini-3.6-flash",
            ModelFamily::Other,
            Some("gemini")
        ));
        assert!(!gemini.applies_to("antigravity-claude-opus", ModelFamily::Other, Some("3p")));
        assert!(block(None, ResetTime::Missing).applies_to("any", ModelFamily::Other, Some("3p")));
    }

    #[test]
    fn named_pools_of_equal_length_and_scope_stay_separate() {
        let a = block(
            Some(key(Some(300), PoolScope::EveryModel, Some("a"))),
            ResetTime::Missing,
        );
        let only_b = reading(
            NOW - MINUTE,
            &[(Some("b"), 300, 10.0), (Some("a"), 300, 100.0)],
        );
        assert_eq!(ends(&a, &only_b), None, "headroom in b never lifts a");
        let a_free = reading(NOW - MINUTE, &[(Some("a"), 300, 10.0)]);
        assert_eq!(ends(&a, &a_free), Some(BlockEnd::Reading));
        // Nor does the unnamed aggregate of the same length.
        let aggregate = reading(NOW - MINUTE, &[(None, 300, 10.0)]);
        assert_eq!(ends(&a, &aggregate), None);
    }

    #[test]
    fn named_unknown_length_pools_stay_separate_and_lift_by_name_once_a_length_is_stated() {
        let scope = PoolScope::Family(ModelFamily::Opus);
        let x = block(
            Some(key(None, scope.clone(), Some("x"))),
            ResetTime::Missing,
        );
        let y = block(Some(key(None, scope, Some("y"))), ResetTime::Missing);
        let x_free = reading(
            NOW - MINUTE,
            &[(Some("x"), 1440, 5.0), (Some("y"), 1440, 100.0)],
        );
        assert_eq!(ends(&x, &x_free), Some(BlockEnd::Reading));
        assert_eq!(ends(&y, &x_free), None, "headroom in x never lifts y");
    }

    #[test]
    fn a_newer_fresh_aggregate_week_lifts_a_reported_seven_day_exhaustion() {
        let seven_day = block(
            PoolKey::for_report(Some("seven_day"), None, ModelFamily::Opus),
            ResetTime::At(NOW + 3 * HOUR),
        );
        let insula = reading(NOW - MINUTE, &[(None, 300, 20.0), (None, 10080, 60.0)]);
        assert_eq!(ends(&seven_day, &insula), Some(BlockEnd::Reading));
        // Exhausted in the reading too: it stands.
        let still = reading(NOW - MINUTE, &[(None, 10080, 100.0)]);
        assert_eq!(ends(&seven_day, &still), None);
    }

    #[test]
    fn readings_are_ordered_by_observation_time() {
        let fence = block(
            Some(key(Some(10080), PoolScope::EveryModel, None)),
            ResetTime::At(NOW + 3 * HOUR),
        );
        let free = |observed_at| reading(observed_at, &[(None, 10080, 10.0)]);
        assert_eq!(
            ends(&fence, &free(fence.observed_at_ms + 1)),
            Some(BlockEnd::Reading)
        );
        assert_eq!(
            ends(&fence, &free(fence.observed_at_ms - 1)),
            None,
            "older headroom"
        );
        assert_eq!(
            ends(&fence, &free(fence.observed_at_ms)),
            None,
            "equal timestamps"
        );
        // Later than the block but 20 minutes old is not fresh.
        let mut late = fence.clone();
        late.observed_at_ms = NOW - 30 * MINUTE;
        assert_eq!(ends(&late, &free(NOW - 20 * MINUTE)), None);
        let mut errored = free(fence.observed_at_ms + 1);
        errored.error_class = Some("upstream_failed".into());
        assert_eq!(ends(&fence, &errored), None);
    }

    #[test]
    fn reset_and_retry_dates_do_not_retire_quota_evidence() {
        let policy = AllocatorPolicy::default();
        let reset = NOW + 10 * MINUTE;
        let known = block(None, ResetTime::At(reset));
        assert_eq!(block_end(&known, None, reset, &policy), None);
        assert_eq!(block_end(&known, None, reset + 1000, &policy), None);
        let unknown = block(None, ResetTime::Missing);
        let check = unknown.observed_at_ms + 15 * MINUTE;
        assert_eq!(block_end(&unknown, None, check - 1, &policy), None);
        assert_eq!(block_end(&unknown, None, check, &policy), None);
        let mut hinted = unknown.clone();
        hinted.retry_hint_ms = Some(unknown.observed_at_ms + 30 * MINUTE);
        assert_eq!(
            block_end(
                &hinted,
                None,
                unknown.observed_at_ms + 30 * MINUTE - 1,
                &policy
            ),
            None
        );
        assert_eq!(
            block_end(&hinted, None, unknown.observed_at_ms + 30 * MINUTE, &policy),
            None
        );
    }

    #[test]
    fn a_whole_account_block_lifts_only_on_a_complete_reading() {
        let p = key(Some(300), PoolScope::EveryModel, Some("p"));
        let q = key(Some(300), PoolScope::EveryModel, Some("q"));
        let mut whole = block(None, ResetTime::Missing);
        whole.recovery_set = vec![p, q];
        let at = NOW - MINUTE;
        assert_eq!(
            ends(
                &whole,
                &reading(at, &[(Some("p"), 300, 10.0), (Some("q"), 300, 20.0)])
            ),
            Some(BlockEnd::Reading)
        );
        for partial in [
            reading(at, &[(Some("p"), 300, 10.0), (Some("q"), 300, 100.0)]),
            reading(at, &[(Some("p"), 300, 10.0)]),
            reading(at, &[]),
        ] {
            assert_eq!(ends(&whole, &partial), None, "{partial:?}");
        }
        // With no reading before the block, any fresh reading with at least
        // one window, every window above reserve, lifts it.
        let empty = block(None, ResetTime::Missing);
        assert_eq!(
            ends(&empty, &reading(at, &[(Some("p"), 300, 10.0)])),
            Some(BlockEnd::Reading)
        );
        assert_eq!(
            ends(
                &empty,
                &reading(at, &[(Some("p"), 300, 10.0), (None, 10080, 99.5)])
            ),
            None,
            "a window inside its reserve"
        );
        assert_eq!(ends(&empty, &reading(at, &[])), None);
    }

    #[test]
    fn a_migrated_family_key_lifts_when_every_window_for_its_family_has_headroom() {
        let fable = PoolKey::from(FenceScope::UnknownScope(ModelFamily::Fable));
        let fence = block(Some(fable), ResetTime::Missing);
        let mut free = reading(NOW - MINUTE, &[(None, 300, 10.0), (None, 10080, 10.0)]);
        free.scoped = ScopedList::Listed(vec![ScopedWindow {
            scope_id: "Fable".into(),
            window_minutes: 10080,
            used_percent: 50.0,
            resets_at: ResetTime::Missing,
        }]);
        assert_eq!(ends(&fence, &free), Some(BlockEnd::Reading));
        let mut spent = free.clone();
        spent.scoped = ScopedList::Listed(vec![ScopedWindow {
            scope_id: "Fable".into(),
            window_minutes: 10080,
            used_percent: 100.0,
            resets_at: ResetTime::Missing,
        }]);
        assert_eq!(ends(&fence, &spent), None);
        assert_eq!(
            ends(&fence, &reading(NOW - MINUTE, &[])),
            None,
            "at least one window"
        );
    }

    #[test]
    fn a_later_reading_adds_the_keys_it_reports_to_the_recovery_set() {
        let policy = AllocatorPolicy::default();
        let whole = block(None, ResetTime::Missing);
        let first = reading(
            NOW - MINUTE,
            &[(Some("p"), 300, 10.0), (Some("q"), 300, 100.0)],
        );
        let grown = grown_recovery_set(&whole, Some(&first), NOW, &policy).expect("grows");
        assert_eq!(grown.len(), 2);
        let mut stored = whole.clone();
        stored.recovery_set = grown;
        // The next reading omits q, so it no longer lifts the block.
        let second = reading(NOW, &[(Some("p"), 300, 10.0)]);
        assert_eq!(block_end(&stored, Some(&second), NOW, &policy), None);
        assert_eq!(
            grown_recovery_set(&stored, Some(&second), NOW, &policy),
            None
        );
        // An older reading adds nothing.
        let older = reading(whole.observed_at_ms - 1, &[(Some("r"), 300, 10.0)]);
        assert_eq!(grown_recovery_set(&whole, Some(&older), NOW, &policy), None);
    }
}
