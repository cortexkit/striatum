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
/// Output room assumed for a seat whose catalog fact carries no output ceiling.
pub const UNKNOWN_SEAT_OUTPUT_TOKENS: u64 = 32_000;
/// The most output a panel member is ever asked for. Some catalog rows carry a
/// placeholder output ceiling equal to the context window (grok-4.7:
/// 500,000/500,000), which would leave no room for any package, so the ceiling
/// reserves what a member send actually requests, not the catalog figure.
pub const PANEL_MEMBER_MAX_OUTPUT_TOKENS: u64 = 64_000;

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
        .min(PANEL_MEMBER_MAX_OUTPUT_TOKENS);
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
        // grok-4.7's catalog row carries output 500,000 on a 500,000 window; a
        // member send asks for at most 64,000, so that is what the ceiling reserves.
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
}
