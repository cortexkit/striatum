//! The evidence package ceiling for a panel, derived from the seats that read it.
//!
//! A consult's evidence package is sent whole to every seat, so it must fit the
//! selected seat with the smallest context window. Oversized roster pins can be
//! substituted before selection is frozen; this budget itself never grows. For
//! each seat the ceiling is
//!
//! ```text
//! (context_window - output_ceiling - SEAT_PROMPT_RESERVE_TOKENS) / 2
//! ```
//!
//! and the consult takes the smallest over its seats. The output ceiling and the
//! prompt reserve are spent on every send regardless of the package. Halving what
//! remains gives the panel's own discussion (every other member's prior-round
//! verdict, the answers to the round's evidence requests, the correction digest)
//! as much room as the package itself, so a seat holds the package plus a full
//! round of discussion with margin. For current frontier windows (500k to 1M
//! tokens) that is roughly 200k to 450k tokens of package.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Tokens a seat's prompt spends outside the evidence package: the member role
/// material, the verdict contract, the consult prompt and the correction digest.
pub const SEAT_PROMPT_RESERVE_TOKENS: u64 = 16_000;
/// Window assumed for a seat whose catalog fact carries none. It is deliberately
/// small so an unknown model can shrink the package but never overfill a seat.
pub const UNKNOWN_SEAT_CONTEXT_WINDOW_TOKENS: u64 = 128_000;
/// The output limit Broca itself applies to a route whose catalog output limit is
/// unknown: missing, or a placeholder at least as large as the context window
/// (Broca 0.3.125). Core omits `generation.max_output_tokens` for such a route so
/// this fallback applies; core uses the value only where it has to assume an
/// output size, such as reserving output room when sizing an evidence package.
pub const BROCA_UNKNOWN_OUTPUT_FALLBACK_TOKENS: u64 = 32_000;
/// Output room assumed for a seat whose catalog fact carries no output ceiling:
/// the same figure Broca falls back to for that seat.
pub const UNKNOWN_SEAT_OUTPUT_TOKENS: u64 = BROCA_UNKNOWN_OUTPUT_FALLBACK_TOKENS;
/// Output room reserved for one seat when sizing the evidence package. Only the
/// package budget uses it; it never caps what a seat is sent, which is the route's
/// own catalog limit (see [`route_output_limit`]). Some catalog rows carry a
/// placeholder output limit equal to the context window (grok-4.7:
/// 500,000/500,000) that would leave no room for any package, so the reserve is
/// bounded.
pub const PACKAGE_OUTPUT_RESERVE_TOKENS: u64 = 64_000;
/// Catalog output limits above this are sent but treated as suspect: Fusiform
/// carries long-decode ceilings (openrouter's 943,718 for Gemini 4 Argon) that the
/// provider may not honour, and Broca flags the same threshold.
pub const SUSPECT_OUTPUT_LIMIT_TOKENS: u64 = 131_072;
/// Room kept free between the estimated prompt plus the requested output and the
/// context window, because the prompt figure is an estimate from its byte size.
pub const OUTPUT_CONTEXT_MARGIN_TOKENS: u64 = 4_096;

/// The output limit core sends for one route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOutputLimit {
    /// The catalog states no usable output limit (missing, a missing context
    /// window, or a placeholder at least as large as the window). The send omits
    /// `generation.max_output_tokens` and Broca applies its own fallback,
    /// [`BROCA_UNKNOWN_OUTPUT_FALLBACK_TOKENS`].
    Unknown,
    /// Send `tokens`: the catalog's limit, lowered only as far as the prompt
    /// estimate, the output and [`OUTPUT_CONTEXT_MARGIN_TOKENS`] need to fit the
    /// window. `suspect` marks a catalog limit above
    /// [`SUSPECT_OUTPUT_LIMIT_TOKENS`], which callers report so a wrong catalog
    /// value gets corrected.
    Send {
        tokens: u64,
        catalog: u64,
        suspect: bool,
    },
}

impl RouteOutputLimit {
    /// The value to put in `generation.max_output_tokens`; `None` omits it.
    pub fn tokens(self) -> Option<u64> {
        match self {
            Self::Unknown => None,
            Self::Send { tokens, .. } => Some(tokens),
        }
    }
}

/// The output limit to send on a route whose catalog fact carries
/// `catalog_output` and `context_window`, for a prompt of about `prompt_tokens`.
///
/// No fixed core ceiling applies: a seat that needs heavy thinking gets the
/// model's whole published limit (Opus publishes 128k).
pub fn route_output_limit(
    catalog_output: Option<u64>,
    context_window: Option<u64>,
    prompt_tokens: u64,
) -> RouteOutputLimit {
    let (Some(output), Some(window)) = (
        catalog_output.filter(|output| *output > 0),
        context_window.filter(|window| *window > 0),
    ) else {
        return RouteOutputLimit::Unknown;
    };
    if output >= window {
        return RouteOutputLimit::Unknown;
    }
    let room = window
        .saturating_sub(prompt_tokens)
        .saturating_sub(OUTPUT_CONTEXT_MARGIN_TOKENS);
    if room == 0 {
        // The prompt alone fills the window. Any limit sent would be refused as a
        // context overflow, which Broca reports by name without a limit from core.
        return RouteOutputLimit::Unknown;
    }
    RouteOutputLimit::Send {
        tokens: output.min(room),
        catalog: output,
        suspect: output > SUSPECT_OUTPUT_LIMIT_TOKENS,
    }
}

/// A package ceiling and the seat whose window set it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackageCeiling {
    pub tokens: usize,
    /// The seat whose window set the ceiling.
    pub route_key: String,
    pub context_window: u64,
    /// False when the seat's catalog fact had no window and the conservative
    /// default stood in for it.
    pub context_window_known: bool,
}

impl PackageCeiling {
    /// One line naming the ceiling and the seat that set it, for refusals.
    pub fn describe(&self) -> String {
        if self.context_window_known {
            format!(
                "{} tokens, set by {} (context window {} tokens)",
                self.tokens, self.route_key, self.context_window
            )
        } else {
            format!(
                "{} tokens, set by {} (no catalog context window; assumed {} tokens)",
                self.tokens, self.route_key, self.context_window
            )
        }
    }
}

/// The ceiling one seat can hold.
pub fn seat_package_ceiling(
    route_key: &str,
    context_window: Option<u64>,
    max_output_tokens: Option<u64>,
) -> PackageCeiling {
    let known = context_window.filter(|window| *window > 0);
    let window = known.unwrap_or(UNKNOWN_SEAT_CONTEXT_WINDOW_TOKENS);
    let output = max_output_tokens
        .unwrap_or(UNKNOWN_SEAT_OUTPUT_TOKENS)
        .min(PACKAGE_OUTPUT_RESERVE_TOKENS);
    let tokens = window
        .saturating_sub(output)
        .saturating_sub(SEAT_PROMPT_RESERVE_TOKENS)
        / 2;
    PackageCeiling {
        tokens: usize::try_from(tokens).unwrap_or(usize::MAX),
        route_key: route_key.to_string(),
        context_window: window,
        context_window_known: known.is_some(),
    }
}

/// The smallest seat ceiling among `seats` given as (route key, context window,
/// output ceiling); `None` for an empty roster.
pub fn roster_package_ceiling<'a>(
    seats: impl IntoIterator<Item = (&'a str, Option<u64>, Option<u64>)>,
) -> Option<PackageCeiling> {
    seats
        .into_iter()
        .map(|(route_key, window, output)| seat_package_ceiling(route_key, window, output))
        .min_by(|left, right| {
            left.tokens
                .cmp(&right.tokens)
                .then_with(|| left.route_key.cmp(&right.route_key))
        })
}

/// The ceiling for stored panel rows (`panel_round.panel_json`: an array of
/// optional members carrying `routeKey`, `contextWindow`, `maxOutputTokens`).
/// Every seat that ever sat, across all rows, counts: a replacement seat reads
/// the same package as the seat it replaced.
pub fn panel_rows_package_ceiling<'a>(
    panel_rows: impl IntoIterator<Item = &'a str>,
) -> Option<PackageCeiling> {
    let members = panel_rows
        .into_iter()
        .filter_map(|raw| serde_json::from_str::<Vec<Value>>(raw).ok())
        .flatten()
        .filter(|member| !member.is_null())
        .collect::<Vec<_>>();
    roster_package_ceiling(members.iter().filter_map(|member| {
        let route_key = member.get("routeKey")?.as_str()?;
        Some((
            route_key,
            member.get("contextWindow").and_then(Value::as_u64),
            member.get("maxOutputTokens").and_then(Value::as_u64),
        ))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seat_ceiling_halves_what_the_output_and_prompt_leave() {
        let ceiling = seat_package_ceiling("xai/grok-4.7", Some(500_000), Some(64_000));
        assert_eq!(ceiling.tokens, (500_000 - 64_000 - 16_000) / 2);
        assert!(ceiling.context_window_known);
    }

    #[test]
    fn the_smallest_seat_sets_the_roster_ceiling() {
        let ceiling = roster_package_ceiling([
            ("anthropic/claude-opus-5-5", Some(1_000_000), Some(64_000)),
            ("xai/grok-4.7", Some(500_000), Some(64_000)),
            ("moonshot/k3", Some(1_048_576), None),
        ])
        .unwrap();
        assert_eq!(ceiling.route_key, "xai/grok-4.7");
        assert!(ceiling.describe().contains("xai/grok-4.7"));
        assert!(roster_package_ceiling(std::iter::empty()).is_none());
    }

    #[test]
    fn a_placeholder_output_ceiling_equal_to_the_window_still_leaves_room() {
        // grok-4.7's catalog row carries output 500,000 on a 500,000 window; the
        // package reserves a bounded 64,000 for output rather than the placeholder.
        let ceiling = seat_package_ceiling("xai/grok-4.7", Some(500_000), Some(500_000));
        assert_eq!(ceiling.tokens, (500_000 - 64_000 - 16_000) / 2);
    }

    #[test]
    fn an_unknown_window_falls_back_to_the_conservative_default() {
        let ceiling = seat_package_ceiling("new/model", None, None);
        assert_eq!(ceiling.context_window, UNKNOWN_SEAT_CONTEXT_WINDOW_TOKENS);
        assert!(!ceiling.context_window_known);
        assert_eq!(ceiling.tokens, (128_000 - 32_000 - 16_000) / 2);
    }

    #[test]
    fn stored_panel_rows_count_every_seat_that_sat() {
        let round_zero = r#"[{"routeKey":"a/big","contextWindow":1000000},null]"#;
        let round_one = r#"[{"routeKey":"a/big","contextWindow":1000000},{"routeKey":"b/small","contextWindow":200000,"maxOutputTokens":32000}]"#;
        let ceiling = panel_rows_package_ceiling([round_zero, round_one]).unwrap();
        assert_eq!(ceiling.route_key, "b/small");
        assert_eq!(ceiling.tokens, (200_000 - 32_000 - 16_000) / 2);
    }

    #[test]
    fn the_route_output_limit_passes_the_catalog_value_through() {
        // Opus publishes 128,000 on a 1,000,000 window: the whole limit is sent,
        // with no core ceiling below it.
        assert_eq!(
            route_output_limit(Some(128_000), Some(1_000_000), 200_000),
            RouteOutputLimit::Send {
                tokens: 128_000,
                catalog: 128_000,
                suspect: false
            }
        );
    }

    #[test]
    fn the_route_output_limit_is_clamped_to_fit_the_context_window() {
        // 200,000 window, 150,000-token prompt: 200,000 - 150,000 - 4,096 remain.
        assert_eq!(
            route_output_limit(Some(128_000), Some(200_000), 150_000).tokens(),
            Some(45_904)
        );
        // A prompt that fills the window leaves nothing to send.
        assert_eq!(
            route_output_limit(Some(128_000), Some(200_000), 199_000),
            RouteOutputLimit::Unknown
        );
    }

    #[test]
    fn a_placeholder_or_missing_output_limit_is_unknown_and_omitted() {
        assert_eq!(
            route_output_limit(Some(1_048_576), Some(1_048_576), 10_000),
            RouteOutputLimit::Unknown
        );
        assert_eq!(
            route_output_limit(Some(500_000), Some(400_000), 10_000).tokens(),
            None
        );
        assert_eq!(
            route_output_limit(None, Some(1_000_000), 10_000).tokens(),
            None
        );
        assert_eq!(
            route_output_limit(Some(64_000), None, 10_000).tokens(),
            None
        );
    }

    #[test]
    fn a_long_decode_ceiling_below_the_window_is_sent_but_flagged_suspect() {
        // openrouter's Gemini 4 Argon row: 943,718 output on a 1,048,576 window.
        assert_eq!(
            route_output_limit(Some(943_718), Some(1_048_576), 50_000),
            RouteOutputLimit::Send {
                tokens: 943_718,
                catalog: 943_718,
                suspect: true
            }
        );
        assert!(matches!(
            route_output_limit(Some(SUSPECT_OUTPUT_LIMIT_TOKENS), Some(1_000_000), 0),
            RouteOutputLimit::Send { suspect: false, .. }
        ));
    }
}
