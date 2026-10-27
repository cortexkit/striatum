//! Per-account quota checks for cold work: the age bands, the family scope
//! check, the reserve and the weight (design steps 3-5 and "Insula input").

use super::decision::{
    AccountNote, AgeBand, ApplicableWindow, ExclusionReason, ScopedListState, WindowWeight,
};
use super::types::{
    AllocatorPolicy, CallerKind, EpochMs, ModelFamily, QuotaReading, ResetTime, ScopedList,
    ScopedWindow,
};

const MS_PER_HOUR: f64 = 3_600_000.0;

/// The shortest weighting horizon: one minute.
const MIN_HORIZON_HOURS: f64 = 1.0 / 60.0;

/// The only `stale.class` value routing knows the meaning of (a preserved
/// reading served through a 429, 5xx or network failure). Any other class is
/// treated as unknown quota until routing's mapping covers it.
const STALE_CLASS_UPSTREAM_FAILED: &str = "upstream_failed";

/// The age band and factor for a reading observed at `observed_at_ms`, or the
/// reason its timestamp is unusable. Age is measured on routing's clock at
/// admission; a timestamp slightly ahead of the clock (within the allowed
/// skew) counts as age 0.
pub fn age_band(
    observed_at_ms: Option<EpochMs>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> Result<(AgeBand, f64, i64), ExclusionReason> {
    let Some(observed_at_ms) = observed_at_ms else {
        return Err(ExclusionReason::TimestampMissing);
    };
    if observed_at_ms > now.saturating_add(policy.future_skew_ms) {
        return Err(ExclusionReason::ClockInvalid { observed_at_ms });
    }
    let age = (now - observed_at_ms).max(0);
    if age < policy.full_band_ms {
        Ok((AgeBand::Full, 1.0, age))
    } else if age < policy.excluded_band_ms {
        let span = (policy.excluded_band_ms - policy.full_band_ms) as f64;
        let into = (age - policy.full_band_ms) as f64;
        let factor = 1.0 - (1.0 - policy.discounted_floor_factor) * (into / span);
        Ok((AgeBand::Discounted, factor, age))
    } else {
        Ok((AgeBand::Excluded, 0.0, age))
    }
}

/// The result of the family scope predicate on one entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopeCheck {
    pub list_state: ScopedListState,
    /// A window mapped to the requested family is present.
    pub matched: bool,
    /// The reference's predicate: false only when the matched window has a
    /// finite remaining value at or below 0. Missing and explicitly empty
    /// lists pass; a non-finite value passes.
    pub passes: bool,
    /// The matched window, when it is valid (finite, 0-100 used) and so joins
    /// the weight.
    pub weight_window: Option<ScopedWindow>,
    /// The matched window is finite but outside 0-100 used without being exhausted.
    pub invalid: bool,
    /// Unmapped scope ids present while the family's known id is absent.
    pub unmapped_scope_ids: Vec<String>,
}

/// Evaluates the family scope check (design step 3). Scopes are keyed by
/// producer scope id plus window length; routing never matches titles or
/// display names.
pub fn scope_check(
    family: ModelFamily,
    scoped: &ScopedList,
    policy: &AllocatorPolicy,
) -> ScopeCheck {
    let windows: &[ScopedWindow] = match scoped {
        ScopedList::Missing => &[],
        ScopedList::Listed(windows) => windows,
    };
    let list_state = match scoped {
        ScopedList::Missing => ScopedListState::Missing,
        ScopedList::Listed(windows) if windows.is_empty() => ScopedListState::ExplicitEmpty,
        ScopedList::Listed(_) => ScopedListState::Present,
    };
    let mut check = ScopeCheck {
        list_state,
        matched: false,
        passes: true,
        weight_window: None,
        invalid: false,
        unmapped_scope_ids: Vec::new(),
    };
    if !policy.family_has_scope(family) {
        return check;
    }
    let matched = windows
        .iter()
        .find(|window| policy.family_for_scope(window) == Some(family));
    match matched {
        Some(window) => {
            check.matched = true;
            let used = window.used_percent;
            if used.is_finite() && 100.0 - used <= 0.0 {
                check.passes = false;
            } else if used.is_finite() && (0.0..=100.0).contains(&used) {
                check.weight_window = Some(window.clone());
            } else if used.is_finite() {
                check.invalid = true;
            }
        }
        None => {
            // The known id disappeared and an unknown one appeared: this
            // family's scope is unknown until the mapping is updated.
            check.unmapped_scope_ids = windows
                .iter()
                .filter(|window| policy.family_for_scope(window).is_none())
                .map(|window| window.scope_id.clone())
                .collect();
        }
    }
    check
}

/// Whether the entry's Fable scope reads exhausted in the full band: the
/// step-7 preference signal for direct Opus work.
pub fn fable_freshly_exhausted(
    reading: &QuotaReading,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> bool {
    let full = matches!(
        age_band(reading.observed_at_ms, now, policy),
        Ok((AgeBand::Full, _, _))
    );
    full && reading.error_class.is_none() && {
        let check = scope_check(ModelFamily::Fable, &reading.scoped, policy);
        check.matched && !check.passes
    }
}

/// `spendable / max(horizon_hours, 1/60)`. The horizon is the time until a
/// future reset; with no future reset, it is the window's own length. The
/// one-minute floor applies to both: a window that reports zero minutes would
/// otherwise divide by zero, and an exhausted one (0 / 0 = NaN) would drop out
/// of the minimum below instead of excluding the account.
fn window_weight(
    window: ApplicableWindow,
    used_percent: f64,
    reserve: f64,
    resets_at: ResetTime,
    window_minutes: u32,
    now: EpochMs,
) -> WindowWeight {
    let spendable = (100.0 - used_percent - reserve).max(0.0);
    let (horizon_hours, reset_passed) = match resets_at {
        ResetTime::At(at) if at > now => ((at - now) as f64 / MS_PER_HOUR, false),
        ResetTime::At(_) => (f64::from(window_minutes) / 60.0, true),
        ResetTime::Unparsable | ResetTime::Missing => (f64::from(window_minutes) / 60.0, false),
    };
    let horizon_hours = horizon_hours.max(MIN_HORIZON_HOURS);
    WindowWeight {
        window,
        used_percent,
        reserve,
        spendable,
        horizon_hours,
        weight: spendable / horizon_hours,
        reset_passed,
    }
}

/// The quota-side evaluation of one account for cold work.
#[derive(Debug, Clone)]
pub struct QuotaEval {
    /// Quota-unknown exclusions (step 6 "missing, degraded, too-old or clock-invalid").
    pub unknown: Vec<ExclusionReason>,
    /// Reserve-tier exclusions: scope exhausted, below reserve, below the Broca floor.
    pub reserve: Vec<ExclusionReason>,
    pub scoped_list: Option<ScopedListState>,
    pub age_band: Option<AgeBand>,
    pub age_factor: Option<f64>,
    pub window_weights: Vec<WindowWeight>,
    /// Minimum window weight times the age factor, when every check passed.
    pub weight: Option<f64>,
    pub observed_at_ms: Option<EpochMs>,
}

impl QuotaEval {
    pub fn eligible(&self) -> bool {
        self.unknown
            .iter()
            .all(|reason| matches!(reason, ExclusionReason::QuotaMissing))
            && self.reserve.is_empty()
            && self.weight.is_some()
    }

    pub fn write_note(&self, note: &mut AccountNote) {
        note.exclusions.extend(self.unknown.iter().cloned());
        note.exclusions.extend(self.reserve.iter().cloned());
        note.scoped_list = self.scoped_list;
        note.age_band = self.age_band;
        note.age_factor = self.age_factor;
        note.window_weights = self.window_weights.clone();
        note.weight = self.weight;
    }
}

/// Routing's reserve for a quota window. Callers with a producer-reported
/// window outside the known topology use the seven-day reserve.
pub fn reserve_for(window: ApplicableWindow, policy: &AllocatorPolicy) -> f64 {
    match window {
        ApplicableWindow::FiveHour => policy.reserve_five_hour,
        ApplicableWindow::SevenDay => policy.reserve_seven_day,
        ApplicableWindow::FamilyScope => policy.reserve_scoped,
        ApplicableWindow::Reported(_) => policy.reserve_seven_day,
    }
}

/// Assess the timestamp and degradation disclosures shared by cold placement
/// and quota-only reads. Window, scope and reserve checks are left to the caller.
pub fn assess_reading(
    reading: Option<&QuotaReading>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> QuotaEval {
    let mut eval = QuotaEval {
        unknown: Vec::new(),
        reserve: Vec::new(),
        scoped_list: None,
        age_band: None,
        age_factor: None,
        window_weights: Vec::new(),
        weight: None,
        observed_at_ms: None,
    };
    let Some(reading) = reading else {
        eval.unknown.push(ExclusionReason::QuotaMissing);
        return eval;
    };
    eval.observed_at_ms = reading.observed_at_ms;
    if let Some(error_class) = &reading.error_class {
        eval.unknown.push(ExclusionReason::EntryErrorClass {
            error_class: error_class.clone(),
        });
    }
    if let Some(class) = reading
        .stale
        .as_ref()
        .and_then(|stale| stale.class.as_ref())
    {
        if class != STALE_CLASS_UPSTREAM_FAILED {
            eval.unknown.push(ExclusionReason::StaleClassUnknown {
                class: class.clone(),
            });
        }
    }
    // Both subscription budgeting and worker admission may use last-good for less than
    // 30 minutes from fetchedAt, never from stale.since or local receipt time.
    // Stale is a disclosure, not unknown quota; unrecognized classes still fail closed.
    let mut freshness_policy = policy.clone();
    if reading.stale.is_some() {
        freshness_policy.excluded_band_ms = crate::quota::LAST_GOOD_MAX_AGE_MS;
    }
    match age_band(reading.observed_at_ms, now, &freshness_policy) {
        Ok((band, factor, age_ms)) => {
            eval.age_band = Some(band);
            if band == AgeBand::Excluded {
                eval.unknown.push(ExclusionReason::AgeExcluded { age_ms });
            } else {
                eval.age_factor = Some(factor);
            }
        }
        Err(reason) => eval.unknown.push(reason),
    }
    eval
}

/// Steps 3-5 for one account's reading: quota-unknown checks, the family
/// scope, reserve, Broca floor and weight.
pub fn evaluate_quota(
    reading: Option<&QuotaReading>,
    family: ModelFamily,
    caller: CallerKind,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> QuotaEval {
    let mut eval = assess_reading(reading, now, policy);
    let Some(reading) = reading else {
        // Absence is not exhaustion. A neutral allocation weight permits cold
        // work without inventing a measured window, headroom or timestamp.
        eval.weight = Some(1.0);
        return eval;
    };
    let required = reading.required_windows();
    if required.is_empty() {
        eval.unknown.insert(0, ExclusionReason::QuotaMissing);
    }
    let windows: Vec<_> = required
        .iter()
        .enumerate()
        .map(|(index, &id)| {
            (
                id,
                reading
                    .general_windows
                    .as_ref()
                    .and_then(|windows| windows.get(index))
                    .or_else(|| reading.window(id)),
            )
        })
        .collect();
    for &(window, quota) in &windows {
        match quota {
            None => eval
                .unknown
                .push(ExclusionReason::GeneralWindowMissing { window }),
            Some(w) if !(w.used_percent.is_finite() && (0.0..=100.0).contains(&w.used_percent)) => {
                eval.unknown.push(ExclusionReason::InvalidPercent {
                    window: window.into(),
                })
            }
            Some(w)
                if reading.stale.is_some()
                    && matches!(w.resets_at, ResetTime::At(reset) if reset <= now) =>
            {
                eval.unknown.push(ExclusionReason::WindowResetElapsed {
                    window: window.into(),
                });
            }
            Some(_) => {}
        }
    }
    let scope = scope_check(family, &reading.scoped, policy);
    let scope_reset_elapsed = match &reading.scoped {
        ScopedList::Missing => false,
        ScopedList::Listed(windows) => windows.iter().any(|window| {
            policy.family_for_scope(window) == Some(family)
                && matches!(window.resets_at, ResetTime::At(reset) if reset <= now)
        }),
    };
    if reading.stale.is_some() && scope_reset_elapsed {
        eval.unknown.push(ExclusionReason::WindowResetElapsed {
            window: ApplicableWindow::FamilyScope,
        });
    }
    eval.scoped_list = Some(scope.list_state);
    if !scope.unmapped_scope_ids.is_empty() {
        eval.unknown.push(ExclusionReason::ScopeUnmapped {
            family,
            unmapped_scope_ids: scope.unmapped_scope_ids.clone(),
        });
    }
    if scope.invalid {
        eval.unknown.push(ExclusionReason::InvalidPercent {
            window: ApplicableWindow::FamilyScope,
        });
    }
    if !scope.passes {
        eval.reserve
            .push(ExclusionReason::ScopeExhausted { family });
    }
    if !eval.unknown.is_empty() {
        return eval;
    }

    let mut weights = Vec::new();
    for (window, quota) in windows {
        let w = quota.expect("checked above");
        let applicable = ApplicableWindow::from(window);
        let weight = window_weight(
            applicable,
            w.used_percent,
            reserve_for(applicable, policy),
            w.resets_at,
            w.window_minutes,
            now,
        );
        if caller == CallerKind::Broca && weight.spendable < policy.broca_floor_points {
            eval.reserve.push(ExclusionReason::BelowBrocaFloor {
                window,
                spendable: weight.spendable,
            });
        }
        weights.push(weight);
    }
    if let Some(window) = &scope.weight_window {
        weights.push(window_weight(
            ApplicableWindow::FamilyScope,
            window.used_percent,
            policy.reserve_scoped,
            window.resets_at,
            window.window_minutes,
            now,
        ));
    }
    let min = weights
        .iter()
        .map(|w| w.weight)
        .fold(f64::INFINITY, f64::min);
    let weight = min * eval.age_factor.unwrap_or(0.0);
    if !(weight.is_finite() && weight > 0.0) {
        eval.reserve.push(ExclusionReason::BelowReserve {
            windows: weights
                .iter()
                .filter(|w| w.weight <= 0.0)
                .map(|w| w.window)
                .collect(),
        });
    } else {
        eval.weight = Some(weight);
    }
    eval.window_weights = weights;
    eval
}
