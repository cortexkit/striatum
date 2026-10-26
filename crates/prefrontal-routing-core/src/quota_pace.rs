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

use serde::{Deserialize, Serialize};

/// The pacing model recorded with a quota projection. `Fixed` means the projection
/// used a reported reset and window length; `UtilizationFallback` names the path
/// that deliberately avoids inventing a burn-rate horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaceWindowModel {
    Fixed,
    UtilizationFallback,
}

/// A provider's explicit window semantics.
///
/// An absent marker defaults to `Fixed` only when both a reset time and a
/// positive window length are present, because those signals are consistent
/// with fixed-boundary windows. Without them, the window mechanism is unknown,
/// so a rolling window must be refused until its pacing math is modeled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderWindowModel {
    Fixed,
    Rolling,
}

/// A quota projection refused because its provider semantics are unsupported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaceError {
    RollingWindow,
}

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
    /// An explicit provider discriminator. `Rolling` is rejected rather than
    /// treating moving recovery time as a fixed-window reset horizon.
    pub provider_window_model: Option<ProviderWindowModel>,
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
    abundance_gain_exponent: f64,
    scarcity_gain_exponent: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaceConfigError {
    AbundanceGainExponentOutOfRange,
    ScarcityGainExponentOutOfRange,
}

impl PaceConfig {
    pub fn with_exponents(
        abundance_gain_exponent: f64,
        scarcity_gain_exponent: f64,
    ) -> Result<Self, PaceConfigError> {
        if !(0.0..=4.0).contains(&abundance_gain_exponent) {
            return Err(PaceConfigError::AbundanceGainExponentOutOfRange);
        }
        if !(0.0..=4.0).contains(&scarcity_gain_exponent) {
            return Err(PaceConfigError::ScarcityGainExponentOutOfRange);
        }
        Ok(Self {
            abundance_gain_exponent,
            scarcity_gain_exponent,
            ..DEFAULT_PACE_CONFIG
        })
    }
}

pub const DEFAULT_PACE_CONFIG: PaceConfig = PaceConfig {
    skew_ms: 1000.0,
    min_elapsed_fraction: 0.1,
    pain_horizon_ms: 24.0 * 60.0 * 60.0 * 1000.0, // 24h locked-out = full avoidance
    floor_start: 0.85,
    hard_block_threshold: 0.95,
    util_fallback_start: 0.6,
    util_fallback_max: 0.8,
    abundance_gain_exponent: 1.0,
    scarcity_gain_exponent: 1.0,
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
    /// Which pacing model produced this result; persisted with decision provenance.
    pub window_model: PaceWindowModel,
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

/// Score perishable quota headroom against the fraction of a fixed window left.
/// Windows without a trustworthy fixed boundary produce no sample; their absence is
/// interpreted as neutral by the caller that folds route abundance.
pub fn project_quota_abundance(input: PaceInput, config: &PaceConfig) -> Option<f64> {
    if input.provider_window_model == Some(ProviderWindowModel::Rolling) {
        return None;
    }

    let resets_at = input.resets_at.filter(|value| value.is_finite())?;
    let window_ms = input
        .window_ms
        .filter(|value| value.is_finite() && *value > 0.0)?;
    let remaining_ms = resets_at - input.observed_at;
    if !remaining_ms.is_finite() || remaining_ms <= 0.0 || remaining_ms <= config.skew_ms {
        return None;
    }

    let headroom = 1.0 - clamp01(input.utilization);
    if headroom <= 0.0 {
        return Some(0.0);
    }

    let time_fraction = (remaining_ms / window_ms).min(1.0);
    if !time_fraction.is_finite() || time_fraction <= 0.0 {
        return None;
    }
    let relative_headroom = (headroom / time_fraction).clamp(1.0 / 64.0, 64.0);
    let exponent = if relative_headroom >= 1.0 {
        config.abundance_gain_exponent
    } else {
        config.scarcity_gain_exponent
    };
    Some(relative_headroom.powf(exponent))
}

/// Project quota pressure for a single window. Pure; every guard-table row is a
/// required test case (mirrors quota-pace.test.ts).
pub fn project_quota_pressure(
    input: PaceInput,
    config: &PaceConfig,
) -> Result<PaceResult, PaceError> {
    let PaceInput {
        utilization,
        resets_at,
        window_ms,
        observed_at,
        provider_window_model,
    } = input;
    if provider_window_model == Some(ProviderWindowModel::Rolling) {
        return Err(PaceError::RollingWindow);
    }

    let time_until_reset_ms = resets_at.map_or(f64::INFINITY, |reset| reset - observed_at);
    let actual = clamp01(utilization);
    let has_window = resets_at.is_some() && matches!(window_ms, Some(w) if w > 0.0);
    let no_window_signals = resets_at.is_none() && window_ms.is_none();
    let window_model = if has_window {
        PaceWindowModel::Fixed
    } else {
        PaceWindowModel::UtilizationFallback
    };

    // Near-limit utilization floor (independent of pace).
    let floor = smoothstep(config.floor_start, config.hard_block_threshold, actual);

    // Guard: window expired or clock skew → almost certainly reset to ~0.
    if time_until_reset_ms <= config.skew_ms {
        return Ok(PaceResult {
            pressure: 0.0,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            window_model,
            reason: PaceReason::ExpiredOrSkew,
        });
    }

    let remaining = (1.0 - actual).max(0.0);

    // A missing reset gives us no recovery horizon, even when a nominal window
    // length is present. As with an unknown window length, use utilization only:
    // inventing a reset would turn an absent provider signal into false precision.
    if !has_window {
        let ramp = smoothstep(
            config.util_fallback_start,
            config.hard_block_threshold,
            actual,
        ) * config.util_fallback_max;
        let pressure = clamp01(ramp.max(floor));
        // With neither window signal, utilization is a safety signal rather
        // than evidence for a particular depletion horizon. Keep the pressure
        // ramp but report its mechanism honestly instead of projecting shortfall.
        return Ok(PaceResult {
            pressure,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            window_model,
            reason: if no_window_signals || pressure == 0.0 {
                PaceReason::InsufficientData
            } else {
                PaceReason::ProjectedShortfall
            },
        });
    }

    let duration_ms = window_ms.unwrap();
    // This derives elapsed time from a fixed start and fixed reset. A rolling or
    // regenerating pool has no such elapsed span, so an explicit rolling signal
    // was rejected above rather than producing a confident but invalid burn rate.
    let elapsed_ms = (duration_ms - time_until_reset_ms).clamp(0.0, duration_ms);

    // Guard: warmup / insufficient elapsed → don't trust rate yet. Floor still applies.
    if elapsed_ms <= config.min_elapsed_fraction * duration_ms {
        return Ok(PaceResult {
            pressure: floor,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            window_model,
            reason: if floor > 0.0 {
                PaceReason::ProjectedShortfall
            } else {
                PaceReason::InsufficientData
            },
        });
    }

    let rate = actual / elapsed_ms; // utilization per ms
    const EPS: f64 = 1e-12;

    // Guard: not burning.
    if rate <= EPS {
        if remaining <= 0.0 {
            return Ok(PaceResult {
                pressure: 1.0,
                eta_ms: 0.0,
                time_until_reset_ms,
                will_last_to_reset: false,
                window_model,
                reason: PaceReason::Exhausted,
            });
        }
        return Ok(PaceResult {
            pressure: floor,
            eta_ms: f64::INFINITY,
            time_until_reset_ms,
            will_last_to_reset: true,
            window_model,
            reason: if floor > 0.0 {
                PaceReason::ProjectedShortfall
            } else {
                PaceReason::NotBurning
            },
        });
    }

    let eta_ms = remaining / rate;
    let will_last_to_reset = eta_ms >= time_until_reset_ms;

    // On or ahead of pace → will last to reset → no pace pressure (floor may apply).
    if will_last_to_reset {
        return Ok(PaceResult {
            pressure: floor,
            eta_ms,
            time_until_reset_ms,
            will_last_to_reset,
            window_model,
            reason: if floor > 0.0 {
                PaceReason::ProjectedShortfall
            } else {
                PaceReason::OnTrackDeadband
            },
        });
    }

    // Projected to deplete BEFORE reset. Pressure = lockout cost (reset − runout)
    // normalized by the pain horizon, with the floor as the safety net.
    let lockout_ms = (time_until_reset_ms - eta_ms).max(0.0);
    let lockout_pressure = clamp01(lockout_ms / config.pain_horizon_ms);
    let pressure = clamp01(lockout_pressure.max(floor));

    Ok(PaceResult {
        pressure,
        eta_ms,
        time_until_reset_ms,
        will_last_to_reset,
        window_model,
        reason: PaceReason::ProjectedShortfall,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const C: PaceConfig = DEFAULT_PACE_CONFIG;

    fn abundance(
        utilization: f64,
        remaining_ms: f64,
        window_ms: Option<f64>,
        config: &PaceConfig,
    ) -> Option<f64> {
        project_quota_abundance(
            PaceInput {
                utilization,
                resets_at: Some(remaining_ms),
                window_ms,
                observed_at: 0.0,
                provider_window_model: None,
            },
            config,
        )
    }

    #[test]
    fn exponent_constructor_accepts_the_closed_interval_and_rejects_every_other_shape() {
        for value in [0.0, 0.5, 1.0, 4.0] {
            assert!(PaceConfig::with_exponents(value, 1.0).is_ok());
            assert!(PaceConfig::with_exponents(1.0, value).is_ok());
        }

        for value in [f64::NEG_INFINITY, -0.1, 4.000_001, f64::INFINITY, f64::NAN] {
            assert_eq!(
                PaceConfig::with_exponents(value, 1.0),
                Err(PaceConfigError::AbundanceGainExponentOutOfRange)
            );
            assert_eq!(
                PaceConfig::with_exponents(1.0, value),
                Err(PaceConfigError::ScarcityGainExponentOutOfRange)
            );
        }
    }

    #[test]
    fn equal_headroom_and_time_score_exactly_one_for_permitted_exponents() {
        for alpha in [0.0, 0.5, 1.0, 4.0] {
            for beta in [0.0, 0.5, 1.0, 4.0] {
                let config = PaceConfig::with_exponents(alpha, beta).unwrap();
                assert_eq!(
                    abundance(0.75, 25_000.0, Some(100_000.0), &config),
                    Some(1.0)
                );
            }
        }
    }

    #[test]
    fn time_fraction_clamps_at_one_for_an_idle_window() {
        let score = abundance(0.0, 200_000.0, Some(100_000.0), &C);

        assert_eq!(score, Some(1.0));
    }

    #[test]
    fn exhausted_headroom_stays_zero_when_scarcity_is_flattened() {
        let config = PaceConfig::with_exponents(1.0, 0.0).unwrap();
        assert_eq!(
            abundance(1.0, 50_000.0, Some(100_000.0), &config),
            Some(0.0)
        );
        assert_eq!(
            abundance(2.0, 50_000.0, Some(100_000.0), &config),
            Some(0.0)
        );
    }

    #[test]
    fn imminent_reset_contributes_no_sample_to_the_fleet_maximum() {
        let imminent = abundance(0.99, C.skew_ms, Some(100_000.0), &C);
        let stable = abundance(0.0, 50_000.0, Some(100_000.0), &C);
        let fleet_maximum = [imminent, stable]
            .into_iter()
            .flatten()
            .max_by(f64::total_cmp);

        assert_eq!(imminent, None);
        assert_eq!(fleet_maximum, stable);
    }

    #[test]
    fn relative_headroom_clamps_to_finite_bounds() {
        let below = abundance(127.0 / 128.0, 100_000.0, Some(100_000.0), &C);
        let above = abundance(0.0, 2_000.0, Some(256_000.0), &C);

        assert_eq!(below, Some(1.0 / 64.0));
        assert_eq!(above, Some(64.0));
    }

    #[test]
    fn zero_exponents_flatten_only_their_half_without_reversing_order() {
        let default_abundant = [
            abundance(0.0, 50_000.0, Some(100_000.0), &C).unwrap(),
            abundance(0.0, 25_000.0, Some(100_000.0), &C).unwrap(),
        ];
        let flat_abundant = PaceConfig::with_exponents(0.0, 1.0).unwrap();
        let flattened_abundant = [
            abundance(0.0, 50_000.0, Some(100_000.0), &flat_abundant).unwrap(),
            abundance(0.0, 25_000.0, Some(100_000.0), &flat_abundant).unwrap(),
        ];
        assert!(default_abundant[0] < default_abundant[1]);
        assert_eq!(flattened_abundant, [1.0, 1.0]);

        let default_scarce = [
            abundance(0.75, 100_000.0, Some(100_000.0), &C).unwrap(),
            abundance(0.5, 100_000.0, Some(100_000.0), &C).unwrap(),
        ];
        let flat_scarce = PaceConfig::with_exponents(1.0, 0.0).unwrap();
        let flattened_scarce = [
            abundance(0.75, 100_000.0, Some(100_000.0), &flat_scarce).unwrap(),
            abundance(0.5, 100_000.0, Some(100_000.0), &flat_scarce).unwrap(),
        ];
        assert!(default_scarce[0] < default_scarce[1]);
        assert_eq!(flattened_scarce, [1.0, 1.0]);
    }

    #[test]
    fn unusable_antigravity_shaped_windows_are_neutral() {
        let no_reset = project_quota_abundance(
            PaceInput {
                utilization: 0.1,
                resets_at: None,
                window_ms: Some(300_000.0),
                observed_at: 0.0,
                provider_window_model: None,
            },
            &C,
        );
        let no_length = abundance(0.1, 200_000.0, None, &C);

        assert_eq!(no_reset, None);
        assert_eq!(no_length, None);
        assert_eq!(no_reset.unwrap_or(1.0), 1.0);
        assert_eq!(no_length.unwrap_or(1.0), 1.0);
    }

    #[test]
    fn rolling_window_has_no_abundance_sample_and_keeps_pressure_refusal() {
        let input = PaceInput {
            utilization: 0.25,
            resets_at: Some(25_000.0),
            window_ms: Some(100_000.0),
            observed_at: 0.0,
            provider_window_model: Some(ProviderWindowModel::Rolling),
        };

        assert_eq!(project_quota_abundance(input, &C), None);
        assert_eq!(
            project_quota_pressure(input, &C),
            Err(PaceError::RollingWindow)
        );
    }

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
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");
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
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");
        assert_eq!(low.pressure, 0.0);
        assert_eq!(low.window_model, PaceWindowModel::UtilizationFallback);
        // High utilization (past hard block) → ramp + floor near 1.
        let high = project_quota_pressure(
            PaceInput {
                utilization: 0.97,
                resets_at: Some(1_000_000.0),
                window_ms: None,
                observed_at: 0.0,
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");
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
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");

        assert!(r.time_until_reset_ms.is_infinite());
        assert!(r.pressure > 0.8, "got {}", r.pressure);
        assert_eq!(r.reason, PaceReason::ProjectedShortfall);
    }

    #[test]
    fn no_window_signals_high_utilization_returns_insufficient_data() {
        let r = project_quota_pressure(
            PaceInput {
                utilization: 0.97,
                resets_at: None,
                window_ms: None,
                observed_at: 0.0,
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");

        assert!(r.pressure > 0.8, "got {}", r.pressure);
        assert_eq!(r.window_model, PaceWindowModel::UtilizationFallback);
        assert_eq!(r.reason, PaceReason::InsufficientData);
    }

    #[test]
    fn absent_window_model_with_window_signals_defaults_to_fixed() {
        let five_hour = 5.0 * 60.0 * 60_000.0;
        let r = project_quota_pressure(
            PaceInput {
                utilization: 0.9,
                resets_at: Some(five_hour / 2.0),
                window_ms: Some(five_hour),
                observed_at: 0.0,
                provider_window_model: None,
            },
            &C,
        )
        .expect("window signals select fixed pacing when the marker is absent");

        assert_eq!(r.window_model, PaceWindowModel::Fixed);
        assert_eq!(r.reason, PaceReason::ProjectedShortfall);
        assert!(!r.will_last_to_reset);
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
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");
        assert!(!r.will_last_to_reset, "should project shortfall");
        assert_eq!(r.window_model, PaceWindowModel::Fixed);
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
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");
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
                provider_window_model: None,
            },
            &C,
        )
        .expect("fixed and fallback pacing inputs are supported");
        assert_eq!(r.pressure, 0.0);
        assert_eq!(r.reason, PaceReason::InsufficientData);
    }
}
