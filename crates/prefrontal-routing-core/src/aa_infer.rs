//! AA-index → Elo inference for provisional router scores.
//!
//! New models must enter the router without a hand-written score. The Artificial
//! Analysis intelligence index is skewed by benchmaxxing, so an AA-derived Elo is
//! **provisional**: interpolated through the operator-approved curve, then pulled
//! 30% of the way toward the curve floor, and tagged `aa_inferred` so the operator
//! can list and replace it by hand.
//!
//! The curve is a static table compiled from the operator's placement sheet
//! (`aa-models-elo-interpolated.csv`): unique AA-index → mean Elo pairs, sorted
//! ascending. The module does not re-read that sheet at runtime.

use std::sync::OnceLock;

use crate::model_facts::ModelScores;

/// Uniform pull toward the floor, not 30% off the AA index. The chair's reading
/// of the ruling: a high-index model is discounted by a fraction of its *gap to
/// the weakest Elo on the curve*, so the penalty scales with how far above the
/// floor the interpolation sat, rather than shrinking the raw AA number.
pub const AA_INFERRED_PENALTY: f64 = 0.30;

const CURVE_CSV: &str = include_str!("aa_elo_curve.csv");

/// Operator-approved AA-index → Elo pairs, unique by index (duplicate indices
/// averaged), sorted ascending. Loaded once from the embedded CSV.
pub fn aa_elo_curve() -> &'static [(f64, f64)] {
    static CURVE: OnceLock<Vec<(f64, f64)>> = OnceLock::new();
    CURVE.get_or_init(parse_aa_elo_curve)
}

fn parse_aa_elo_curve() -> Vec<(f64, f64)> {
    parse_aa_elo_curve_csv(CURVE_CSV)
}

fn parse_aa_elo_curve_csv(csv: &str) -> Vec<(f64, f64)> {
    csv.lines()
        .skip(1)
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let (aa, elo) = line.split_once(',')?;
            let aa_index: f64 = aa.trim().parse().ok()?;
            let elo: f64 = elo.trim().parse().ok()?;
            if aa_index.is_finite() && elo.is_finite() {
                Some((aa_index, elo))
            } else {
                None
            }
        })
        .collect()
}

/// Lowest Elo on `curve`. Empty curve → 0.0.
pub fn elo_floor(curve: &[(f64, f64)]) -> f64 {
    curve
        .iter()
        .map(|(_, elo)| *elo)
        .fold(None, |acc: Option<f64>, elo| match acc {
            Some(floor) => Some(floor.min(elo)),
            None => Some(elo),
        })
        .unwrap_or(0.0)
}

/// Piecewise-linear interpolation of `aa_index` through `curve`.
///
/// Exact index hits return that point's Elo. Values below the first index or
/// above the last clamp to the endpoint Elo. Empty curve or non-finite index
/// returns `None`.
pub fn interpolate_elo(aa_index: f64, curve: &[(f64, f64)]) -> Option<f64> {
    if !aa_index.is_finite() || curve.is_empty() {
        return None;
    }
    if aa_index <= curve[0].0 {
        return Some(curve[0].1);
    }
    let last = curve.len() - 1;
    if aa_index >= curve[last].0 {
        return Some(curve[last].1);
    }
    let mut lo = 0usize;
    let mut hi = last;
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if curve[mid].0 <= aa_index {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let (x0, y0) = curve[lo];
    let (x1, y1) = curve[hi];
    if (x1 - x0).abs() < f64::EPSILON {
        return Some((y0 + y1) / 2.0);
    }
    let t = (aa_index - x0) / (x1 - x0);
    Some(y0 + t * (y1 - y0))
}

/// Interpolate `aa_index` through `curve`, then apply the 30% floor-gap penalty:
///
/// `elo_penalized = elo_interp - 0.30 * (elo_interp - elo_floor)`
///
/// where `elo_floor` is the curve's lowest Elo. This is a uniform pull toward
/// the floor, not 30% off the AA index: a model sitting on the floor is
/// unchanged, and a model far above it is discounted by 30% of that gap. The
/// AA index itself is only an interpolation key; the penalty lives on the Elo
/// scale the router actually ranks.
pub fn infer_elo_from_aa(aa_index: f64, curve: &[(f64, f64)]) -> Option<f64> {
    let elo_interp = interpolate_elo(aa_index, curve)?;
    let floor = elo_floor(curve);
    Some(elo_interp - AA_INFERRED_PENALTY * (elo_interp - floor))
}

/// Convenience: infer Elo from the static operator curve.
pub fn infer_elo_from_aa_static(aa_index: f64) -> Option<f64> {
    infer_elo_from_aa(aa_index, aa_elo_curve())
}

/// Neutral speed used when AA ingest creates a route that has no observed TPS.
pub const AA_INFERRED_DEFAULT_SPEED: f64 = 100.0;

/// Scores written for an `aa_inferred` row.
pub fn inferred_scores(elo: f64, eq: f64) -> ModelScores {
    ModelScores {
        elo,
        eq,
        speed: AA_INFERRED_DEFAULT_SPEED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tiny curve used by the exact-value proof: floor 15 at index 1, 1800 at 60.
    fn fixture_curve() -> Vec<(f64, f64)> {
        vec![(1.0, 15.0), (50.0, 1000.0), (60.0, 1800.0)]
    }

    #[test]
    fn infer_elo_from_aa_applies_floor_gap_penalty() {
        let curve = fixture_curve();
        let elo_interp = interpolate_elo(60.0, &curve).unwrap();
        assert!((elo_interp - 1800.0).abs() < 1e-9);
        let floor = elo_floor(&curve);
        assert!((floor - 15.0).abs() < 1e-9);
        let expected = elo_interp - 0.30 * (elo_interp - floor);
        assert!(
            (expected - 1264.5).abs() < 1e-9,
            "expected 1264.5, got {expected}"
        );
        let got = infer_elo_from_aa(60.0, &curve).unwrap();
        assert!(
            (got - 1264.5).abs() < 1e-9,
            "penalized elo must be interpolated elo minus 30% of the gap to the floor; got {got}"
        );
    }

    #[test]
    fn floor_index_is_unchanged_by_the_penalty() {
        let curve = fixture_curve();
        let got = infer_elo_from_aa(1.0, &curve).unwrap();
        assert!((got - 15.0).abs() < 1e-9);
    }

    #[test]
    fn interpolates_between_curve_points() {
        let curve = fixture_curve();
        // Midway 50 → 60 is aa 55 → elo 1400, then penalty toward 15.
        let interp = interpolate_elo(55.0, &curve).unwrap();
        assert!((interp - 1400.0).abs() < 1e-9);
        let got = infer_elo_from_aa(55.0, &curve).unwrap();
        let expected = 1400.0 - 0.30 * (1400.0 - 15.0);
        assert!((got - expected).abs() < 1e-9);
    }

    #[test]
    fn static_curve_has_operator_floor_and_ceiling() {
        let curve = aa_elo_curve();
        assert!(
            curve.len() >= 100,
            "operator curve should be the unique-index table, got {}",
            curve.len()
        );
        assert!((elo_floor(curve) - 15.0).abs() < 1e-9);
        let last = curve.last().unwrap();
        assert!((last.0 - 63.1).abs() < 1e-9);
        assert!((last.1 - 1920.0).abs() < 1e-9);
    }

    #[test]
    fn non_finite_index_is_absent() {
        let curve = fixture_curve();
        assert!(infer_elo_from_aa(f64::NAN, &curve).is_none());
        assert!(infer_elo_from_aa(f64::INFINITY, &curve).is_none());
        assert!(infer_elo_from_aa(60.0, &[]).is_none());
    }
}
