//! Quota pace projection: pure math. Ported VERBATIM from the TS
//! `quota/quota-pace.ts` (itself ported from CodexBar `UsagePace.weekly`),
//! including every boundary guard: without them NaN / +-Inf / inverted-pressure
//! results occur at exactly the edge states that show up under load (empty window,
//! zero elapsed, post-reset).
//!
//! All time is MILLISECONDS; `resets_at` is ms-epoch. Pace is measured against the
//! sample's `observed_at`, NOT wall-clock now (else pressure falsely decays as the
//! cached window ages). Output is a soft pressure 0..1 feeding the router's capped
//! quota penalty; a 429 (Tier B) bypasses this and writes a hard cooldown.

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PaceInput {
    /// Window utilization 0..1 (parsers normalize provider %/100 to this).
    pub utilization: f64,
    /// Window reset time, ms epoch. `None` means the provider reported no
    /// recovery time, so pacing uses the utilization-only fallback.
    pub resets_at: Option<f64>,
    /// Known window length in ms. `None` = unknown length (e.g. grok/antigravity
    /// report none) → cannot project burn rate; falls back to a utilization-only ramp.
    pub window_ms: Option<f64>,
    /// When this sample was observed, ms epoch. Pace is measured from here.
    pub observed_at: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PaceConfig {
    pub skew_ms: f64,
    pub min_elapsed_fraction: f64,
    pub pain_horizon_ms: f64,
    pub floor_start: f64,
    pub hard_block_threshold: f64,
    pub util_fallback_start: f64,
    pub util_fallback_max: f64,
}

pub const DEFAULT_PACE_CONFIG: PaceConfig = PaceConfig {
    skew_ms: 1000.0,
    min_elapsed_fraction: 0.1,
    pain_horizon_ms: 24.0 * 60.0 * 60.0 * 1000.0, // 24h locked-out = full avoidance
    floor_start: 0.85,
    hard_block_threshold: 0.95,
    util_fallback_start: 0.6,
    util_fallback_max: 0.8,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaceReason {
    ExpiredOrSkew,
    InsufficientData,
    NotBurning,
    Exhausted,
    OnTrackDeadband,
    ProjectedShortfall,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PaceResult {
    /// Soft pressure 0..1.
    pub pressure: f64,
    /// ms until exhaustion at the observed burn rate; +Inf if not burning.
    pub eta_ms: f64,
    /// ms until window reset (from observed_at).
    pub time_until_reset_ms: f64,
    /// true if projected to last until reset (or insufficient data).
    pub will_last_to_reset: bool,
    pub reason: PaceReason,
}

/// clamp to 0..1; NaN/-Inf → 0, +Inf → 1 (matches the TS clamp01 exactly).
pub fn clamp01(x: f64) -> f64 {
    if x.is_nan() {
        return 0.0;
    }
    if x == f64::INFINITY {
        return 1.0;
    }
    if x == f64::NEG_INFINITY {
        return 0.0;
    }
    x.clamp(0.0, 1.0)
}

/// Smoothstep ramp from edge0→edge1, clamped 0..1.
pub fn smoothstep(edge0: f64, edge1: f64, x: f64) -> f64 {
    if edge0 == edge1 {
        return if x >= edge1 { 1.0 } else { 0.0 };
    }
    let t = clamp01((x - edge0) / (edge1 - edge0));
    t * t * (3.0 - 2.0 * t)
}

/// Project quota pressure for a single window. Pure; every guard-table row is a
/// required test case (mirrors quota-pace.test.ts).
pub fn project_quota_pressure(input: PaceInput, config: &PaceConfig) -> PaceResult {
    let PaceInput {
        utilization,
        resets_at,
        window_ms,
        observed_at,
    } = input;
    let time_until_reset_ms = resets_at.map_or(f64::INFINITY, |reset| reset - observed_at);
    let actual = clamp01(utilization);

    // Near-limit utilization floor (independent of pace).
    let floor = smoothstep(config.floor_start, config.hard_block_threshold, actual);

    // Guard: window expired or clock skew → almost certainly reset to ~0.
    if time_until_reset_ms <= config.skew_ms {
        return PaceResult {
            pressure: 0.0,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            reason: PaceReason::ExpiredOrSkew,
        };
    }

    let remaining = (1.0 - actual).max(0.0);

    // A missing reset gives us no recovery horizon, even when a nominal window
    // length is present. As with an unknown window length, use utilization only:
    // inventing a reset would turn an absent provider signal into false precision.
    let has_window = resets_at.is_some() && matches!(window_ms, Some(w) if w > 0.0);
    if !has_window {
        let ramp = smoothstep(
            config.util_fallback_start,
            config.hard_block_threshold,
            actual,
        ) * config.util_fallback_max;
        let pressure = clamp01(ramp.max(floor));
        return PaceResult {
            pressure,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            reason: if pressure > 0.0 {
                PaceReason::ProjectedShortfall
            } else {
                PaceReason::InsufficientData
            },
        };
    }

    let duration_ms = window_ms.unwrap();
    let elapsed_ms = (duration_ms - time_until_reset_ms).clamp(0.0, duration_ms);

    // Guard: warmup / insufficient elapsed → don't trust rate yet. Floor still applies.
    if elapsed_ms <= config.min_elapsed_fraction * duration_ms {
        return PaceResult {
            pressure: floor,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            reason: if floor > 0.0 {
                PaceReason::ProjectedShortfall
            } else {
                PaceReason::InsufficientData
            },
        };
    }

    let rate = actual / elapsed_ms; // utilization per ms
    const EPS: f64 = 1e-12;

    // Guard: not burning.
    if rate <= EPS {
        if remaining <= 0.0 {
            return PaceResult {
                pressure: 1.0,
                eta_ms: 0.0,
                time_until_reset_ms,
                will_last_to_reset: false,
                reason: PaceReason::Exhausted,
            };
        }
        return PaceResult {
            pressure: floor,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            reason: if floor > 0.0 {
                PaceReason::ProjectedShortfall
            } else {
                PaceReason::NotBurning
            },
        };
    }

    let eta_ms = remaining / rate;
    let will_last_to_reset = eta_ms >= time_until_reset_ms;

    // On or ahead of pace → will last to reset → no pace pressure (floor may apply).
    if will_last_to_reset {
        return PaceResult {
            pressure: floor,
            eta_ms,
            time_until_reset_ms,
            will_last_to_reset,
            reason: if floor > 0.0 {
                PaceReason::ProjectedShortfall
            } else {
                PaceReason::OnTrackDeadband
            },
        };
    }

    // Projected to deplete BEFORE reset. Pressure = lockout cost (reset − runout)
    // normalized by the pain horizon, with the floor as the safety net.
    let lockout_ms = (time_until_reset_ms - eta_ms).max(0.0);
    let lockout_pressure = clamp01(lockout_ms / config.pain_horizon_ms);
    let pressure = clamp01(lockout_pressure.max(floor));

    PaceResult {
        pressure,
        eta_ms,
        time_until_reset_ms,
        will_last_to_reset,
        reason: PaceReason::ProjectedShortfall,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const C: PaceConfig = DEFAULT_PACE_CONFIG;

    #[test]
    fn clamp01_handles_nan_and_infinities() {
        assert_eq!(clamp01(f64::NAN), 0.0);
        assert_eq!(clamp01(f64::INFINITY), 1.0);
        assert_eq!(clamp01(f64::NEG_INFINITY), 0.0);
        assert_eq!(clamp01(0.5), 0.5);
        assert_eq!(clamp01(2.0), 1.0);
    }

    #[test]
    fn expired_or_skew_window_is_zero_pressure_not_max() {
        // time_until_reset <= skew → reset imminent → pressure 0 (the draft-bug guard).
        let r = project_quota_pressure(
            PaceInput {
                utilization: 1.0,
                resets_at: Some(1000.0),
                window_ms: Some(18_000_000.0),
                observed_at: 1000.0,
            },
            &C,
        );
        assert_eq!(r.pressure, 0.0);
        assert_eq!(r.reason, PaceReason::ExpiredOrSkew);
    }

    #[test]
    fn no_window_length_uses_utilization_only_ramp() {
        // grok/antigravity case: no windowMs. Below util_fallback_start → ~0.
        let low = project_quota_pressure(
            PaceInput {
                utilization: 0.3,
                resets_at: Some(1_000_000.0),
                window_ms: None,
                observed_at: 0.0,
            },
            &C,
        );
        assert_eq!(low.pressure, 0.0);
        // High utilization (past hard block) → ramp + floor near 1.
        let high = project_quota_pressure(
            PaceInput {
                utilization: 0.97,
                resets_at: Some(1_000_000.0),
                window_ms: None,
                observed_at: 0.0,
            },
            &C,
        );
        assert!(high.pressure > 0.8, "got {}", high.pressure);
    }

    #[test]
    fn no_reset_uses_utilization_only_ramp_even_with_window_length() {
        let r = project_quota_pressure(
            PaceInput {
                utilization: 0.97,
                resets_at: None,
                window_ms: Some(7.0 * 24.0 * 60.0 * 60_000.0),
                observed_at: 0.0,
            },
            &C,
        );

        assert!(r.time_until_reset_ms.is_infinite());
        assert!(r.pressure > 0.8, "got {}", r.pressure);
        assert_eq!(r.reason, PaceReason::ProjectedShortfall);
    }

    #[test]
    fn long_window_depleting_early_is_high_pressure() {
        // 7-day window, 50% used but only ~10% elapsed → burning ~5x → big lockout.
        let seven_day = 7.0 * 24.0 * 60.0 * 60_000.0;
        let observed = 0.0;
        let elapsed = 0.1 * seven_day + 1.0; // just past warmup
        let resets_at = observed + (seven_day - elapsed);
        let r = project_quota_pressure(
            PaceInput {
                utilization: 0.5,
                resets_at: Some(resets_at),
                window_ms: Some(seven_day),
                observed_at: observed,
            },
            &C,
        );
        assert!(!r.will_last_to_reset, "should project shortfall");
        assert!(
            r.pressure > 0.5,
            "long-window early depletion → high pressure, got {}",
            r.pressure
        );
    }

    #[test]
    fn short_window_depleting_early_is_low_pressure() {
        // 5-hour window depleting early → small lockout → ~0 (use it, it resets soon).
        let five_hour = 5.0 * 60.0 * 60_000.0;
        let observed = 0.0;
        let elapsed = 0.5 * five_hour;
        let resets_at = observed + (five_hour - elapsed);
        let r = project_quota_pressure(
            PaceInput {
                utilization: 0.9,
                resets_at: Some(resets_at),
                window_ms: Some(five_hour),
                observed_at: observed,
            },
            &C,
        );
        // lockout is at most a few hours << 24h pain horizon → low pace pressure
        // (the near-limit floor at 0.9 may add a little, but well below a long window).
        assert!(
            r.pressure < 0.6,
            "short-window early depletion → low pressure, got {}",
            r.pressure
        );
    }

    #[test]
    fn warmup_returns_floor_only() {
        let five_hour = 5.0 * 60.0 * 60_000.0;
        // 1% elapsed (below min_elapsed_fraction 0.1), util 0.5 (below floor_start) → 0.
        let resets_at = 0.99 * five_hour;
        let r = project_quota_pressure(
            PaceInput {
                utilization: 0.5,
                resets_at: Some(resets_at),
                window_ms: Some(five_hour),
                observed_at: 0.0,
            },
            &C,
        );
        assert_eq!(r.pressure, 0.0);
        assert_eq!(r.reason, PaceReason::InsufficientData);
    }
}
