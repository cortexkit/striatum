//! `diagnostics.findings`: routing's read-only self-diagnosis.
//!
//! Routing can quietly wall a consumer off from every model it needs: a
//! credential fenced for days, a route cooled down for hours, a consumer whose
//! every request is refused, a band of requests that only one model can serve.
//! None of that is an error anywhere; each is a normal answer. This operation
//! reads the current blocking state and the decisions of a recent window and
//! reports each such condition as one finding, so a scheduled flow can wake
//! the operator's agent when one appears. A check with nothing to report adds
//! no finding, so a healthy routing module answers with an empty list.
//!
//! The checks:
//!
//! - `credential_fences`: every billing or entitlement fence on a vault
//!   credential. An error once its last confirmation is over an hour old.
//! - `route_blocks`: routes walled now (cooldown, exhausted quota, failure
//!   backoff), account holds, standing account pool blocks and walled
//!   key-pool credentials. An error when a whole route's block spans more
//!   than ten minutes.
//! - `refusal_rate`: consumers whose decisions in the window were mostly
//!   refusals, with counts by refusal code, read from the decision ledger.
//! - `empty_candidate_sets`: consumers and iq bands whose selections in the
//!   window had one adequate model or none, so a single wall stops them.
//!
//! Slot saturation is not checked: the caller sends its concurrency slot
//! counts with each request and routing stores none of them (see
//! `prefrontal_routing_core::slot_saturation`), so there is no slot usage
//! over time to read.
//!
//! The operation never writes. In particular it reads credential fences raw
//! instead of through `credential_fences::live`, which lifts fences whose
//! vault listing changed.

use std::collections::{BTreeMap, BTreeSet};

use prefrontal_routing_core::accounts::ResetTime;
use prefrontal_routing_core::route_state_store::CredentialEntitlementFence;
use prefrontal_routing_core::{RouteStateStore, RouteWallClass};
use prefrontal_routing_store::{
    DiagnosticAccountBlock, DiagnosticAccountHold, DiagnosticDecision, DiagnosticKeyWall,
    DiagnosticRouteWall, SqliteRouteStateStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use subc_protocol::{ErrorBody, Flags, Frame, FrameType, Priority};
use tokio::sync::mpsc;

use crate::{render_age, routing_v1, send, ModuleError, SharedStore};

/// The operation name, shared with prefrontal-core's operation of the same
/// shape.
pub(crate) const OP: &str = "diagnostics.findings";

/// The decision window when the request names none: six hours.
const DEFAULT_WINDOW_MS: u64 = 6 * 60 * 60 * 1000;
/// The longest window a request may ask for: seven days.
const MAX_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// The reply, envelope included, stays under 64 KiB.
const MAX_REPLY_BYTES: usize = 64 * 1024;
const MAX_SAMPLES: usize = 5;
const MAX_SUMMARY_CHARS: usize = 400;
const MAX_DETAIL_CHARS: usize = 160;
/// How many of the window's newest decisions are read at most. A window with
/// more is read from its newest end and the summaries say so.
const DECISION_SCAN_LIMIT: usize = 20_000;
/// A credential fence last confirmed longer ago than this is an error.
const FENCE_ERROR_AGE_MS: i64 = 60 * 60 * 1000;
/// A whole-route block spanning longer than this is an error.
const ROUTE_BLOCK_ERROR_MS: i64 = 10 * 60 * 1000;
/// The fewest decisions a consumer needs in the window before its refusal
/// share is judged, so one unlucky request is not reported as a pattern.
const REFUSAL_MIN_DECISIONS: usize = 3;
/// Requests are grouped by iq in bands of this width.
const IQ_BAND_WIDTH: f64 = 10.0;

const CHECK_CREDENTIAL_FENCES: &str = "credential_fences";
const CHECK_ROUTE_BLOCKS: &str = "route_blocks";
const CHECK_REFUSAL_RATE: &str = "refusal_rate";
const CHECK_EMPTY_CANDIDATE_SETS: &str = "empty_candidate_sets";

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    window_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Severity {
    Warning,
    Error,
}

/// One condition one check found.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Finding {
    pub check: &'static str,
    /// Identifies the condition within its check, stable across reports, so
    /// a reader can tell a new finding from one it has already seen.
    pub key: String,
    pub severity: Severity,
    pub count: u64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub summary: String,
    pub samples: Vec<Value>,
    pub hint: String,
}

/// Everything the checks read, gathered from the store in one pass.
pub(crate) struct Evidence {
    pub fences: Vec<CredentialEntitlementFence>,
    pub route_walls: Vec<DiagnosticRouteWall>,
    pub holds: Vec<DiagnosticAccountHold>,
    pub account_blocks: Vec<DiagnosticAccountBlock>,
    pub key_walls: Vec<DiagnosticKeyWall>,
    pub decisions: Vec<DiagnosticDecision>,
    /// The window held more decisions than were read.
    pub decisions_truncated: bool,
    /// Every catalog model as `provider/model`.
    pub catalog_models: BTreeSet<String>,
}

impl Evidence {
    pub(crate) fn gather(
        store: &SqliteRouteStateStore,
        now_ms: i64,
        window_ms: i64,
    ) -> Result<Self, String> {
        fn text(error: impl std::fmt::Display) -> String {
            error.to_string()
        }
        let decisions = store
            .diagnostic_decisions(
                now_ms.saturating_sub(window_ms),
                now_ms,
                DECISION_SCAN_LIMIT + 1,
            )
            .map_err(text)?;
        let decisions_truncated = decisions.len() > DECISION_SCAN_LIMIT;
        let decisions = if decisions_truncated {
            decisions[decisions.len() - DECISION_SCAN_LIMIT..].to_vec()
        } else {
            decisions
        };
        Ok(Self {
            fences: store.credential_entitlement_fences().map_err(text)?,
            route_walls: store.diagnostic_route_walls(now_ms).map_err(text)?,
            holds: store.diagnostic_account_holds(now_ms).map_err(text)?,
            account_blocks: store.diagnostic_account_blocks().map_err(text)?,
            key_walls: store.diagnostic_key_walls(now_ms).map_err(text)?,
            decisions,
            decisions_truncated,
            catalog_models: store
                .get_facts()
                .map_err(text)?
                .iter()
                .map(|fact| {
                    prefrontal_routing_core::make_capacity_key(&fact.provider_id, &fact.model_id)
                })
                .collect(),
        })
    }
}

/// The reply: the findings, most severe first. When they do not all fit
/// under the size cap, the least severe are left out.
pub(crate) fn report(evidence: &Evidence, now_ms: i64, window_ms: i64) -> Value {
    let mut findings = Vec::new();
    findings.extend(credential_fences(&evidence.fences, now_ms));
    findings.extend(route_blocks(evidence, now_ms));
    findings.extend(refusal_rate(evidence));
    findings.extend(empty_candidate_sets(evidence));
    let check_rank = |check: &str| {
        [
            CHECK_CREDENTIAL_FENCES,
            CHECK_ROUTE_BLOCKS,
            CHECK_REFUSAL_RATE,
            CHECK_EMPTY_CANDIDATE_SETS,
        ]
        .iter()
        .position(|known| *known == check)
    };
    findings.sort_by(|left, right| {
        right
            .severity
            .cmp(&left.severity)
            .then_with(|| check_rank(left.check).cmp(&check_rank(right.check)))
            .then_with(|| right.count.cmp(&left.count))
            .then_with(|| left.key.cmp(&right.key))
    });
    // Keep the most severe findings that fit: the reply frame body is the
    // envelope with an empty list, plus each kept finding and a separator.
    let envelope = json!({
        "result": {
            "generated_at_ms": now_ms,
            "window_ms": window_ms,
            "findings": [],
        }
    });
    let mut used = serde_json::to_vec(&envelope).map_or(0, |body| body.len());
    let mut kept = Vec::with_capacity(findings.len());
    for finding in findings {
        let Ok(value) = serde_json::to_value(&finding) else {
            continue;
        };
        let size = serde_json::to_vec(&value).map_or(usize::MAX, |body| body.len()) + 1;
        if used.saturating_add(size) >= MAX_REPLY_BYTES {
            break;
        }
        used += size;
        kept.push(value);
    }
    json!({
        "generated_at_ms": now_ms,
        "window_ms": window_ms,
        "findings": kept,
    })
}

#[cfg(test)]
fn reply_bytes(reply: &Value) -> usize {
    serde_json::to_vec(&json!({ "result": reply }))
        .map(|body| body.len())
        .unwrap_or(usize::MAX)
}

fn clip(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let mut clipped: String = text.chars().take(limit.saturating_sub(1)).collect();
    clipped.push('…');
    clipped
}

fn age(now_ms: i64, then_ms: i64) -> String {
    render_age(u64::try_from(now_ms.saturating_sub(then_ms)).unwrap_or(0))
}

// ---------------------------------------------------------------------------
// credential_fences
// ---------------------------------------------------------------------------

fn credential_fences(fences: &[CredentialEntitlementFence], now_ms: i64) -> Vec<Finding> {
    fences
        .iter()
        .map(|fence| {
            let age_ms = now_ms.saturating_sub(fence.observed_at_ms).max(0);
            let billing = crate::credential_fences::is_billing_code(&fence.code);
            let kind = if billing { "billing" } else { "entitlement" };
            let hint = if billing {
                format!(
                    "Top up the credential's paid balance, then lift the fence with route.clear_credential_fences {{\"credentialId\":\"{}\"}}. Routing also admits one probe request on it every 6h.",
                    fence.credential_id
                )
            } else {
                format!(
                    "The account has no plan for {}'s models. Change the plan or remove the credential from the vault (a vault listing change lifts the fence), or lift it by hand with route.clear_credential_fences {{\"credentialId\":\"{}\"}}.",
                    fence.provider_id, fence.credential_id
                )
            };
            Finding {
                check: CHECK_CREDENTIAL_FENCES,
                key: format!("{}/{}", fence.provider_id, fence.credential_id),
                severity: if age_ms > FENCE_ERROR_AGE_MS {
                    Severity::Error
                } else {
                    Severity::Warning
                },
                count: 1,
                first_seen_ms: fence.observed_at_ms,
                last_seen_ms: now_ms,
                summary: clip(
                    &format!(
                        "{} is fenced on credential {} ({kind}: {}), last confirmed {} ago; routing will not use {} through that credential",
                        fence.provider_id,
                        fence.credential_id,
                        fence.code,
                        age(now_ms, fence.observed_at_ms),
                        fence.provider_id,
                    ),
                    MAX_SUMMARY_CHARS,
                ),
                samples: vec![json!({
                    "provider_id": fence.provider_id,
                    "credential_id": fence.credential_id,
                    "code": fence.code,
                    "kind": kind,
                    "age_ms": age_ms,
                    "observed_at_ms": fence.observed_at_ms,
                })],
                hint,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// route_blocks
// ---------------------------------------------------------------------------

/// One live wall on one route, reduced to what the finding names.
struct RouteWall<'a> {
    row: &'a DiagnosticRouteWall,
    /// `cooldown_active`, `quota_exhausted` or `backoff_active`: the wall
    /// with the latest deadline, named as the selection's rejection names it.
    reason: &'static str,
    until_ms: i64,
    detail: Option<String>,
}

/// The route's latest live wall. A cooldown or quota wall recorded from an
/// error that concerns one account is set aside, exactly as selection sets it
/// aside, because it does not block the route for other accounts.
fn live_route_wall(row: &DiagnosticRouteWall, now_ms: i64) -> Option<RouteWall<'_>> {
    let account_scoped = prefrontal_routing_core::account_wall::account_scoped_wall(
        row.last_error.as_deref(),
        row.wall_class,
    );
    let mut candidates = Vec::new();
    if !account_scoped {
        candidates.push((
            "cooldown_active",
            row.cooldown_until,
            row.last_error.clone(),
        ));
        candidates.push((
            "quota_exhausted",
            row.quota_exhausted_until,
            row.last_error.clone(),
        ));
    }
    candidates.push((
        "backoff_active",
        row.failure_backoff_until,
        row.failure_backoff_reason.clone(),
    ));
    candidates
        .into_iter()
        .filter_map(|(reason, until, detail)| {
            let until_ms = until? as i64;
            (until_ms > now_ms).then_some((reason, until_ms, detail))
        })
        .max_by_key(|(_, until_ms, _)| *until_ms)
        .map(|(reason, until_ms, detail)| RouteWall {
            row,
            reason,
            until_ms,
            detail: detail.map(|detail| clip(&detail, MAX_DETAIL_CHARS)),
        })
}

fn route_blocks(evidence: &Evidence, now_ms: i64) -> Vec<Finding> {
    let mut findings = Vec::new();
    findings.extend(whole_route_blocks(&evidence.route_walls, now_ms));
    findings.extend(account_holds(&evidence.holds, now_ms));
    findings.extend(account_pool_blocks(&evidence.account_blocks, now_ms));
    findings.extend(key_walls(&evidence.key_walls, now_ms));
    findings
}

/// One finding per model whose routes are walled, so a wall on every
/// variant of one model reads as one block.
fn whole_route_blocks(rows: &[DiagnosticRouteWall], now_ms: i64) -> Vec<Finding> {
    let mut by_model: BTreeMap<&str, Vec<RouteWall<'_>>> = BTreeMap::new();
    for wall in rows.iter().filter_map(|row| live_route_wall(row, now_ms)) {
        by_model
            .entry(wall.row.capacity_key.as_str())
            .or_default()
            .push(wall);
    }
    by_model
        .into_iter()
        .map(|(model, walls)| {
            // The block started when its error was recorded; a wall with no
            // recorded error time is counted from now.
            let started_ms = walls
                .iter()
                .map(|wall| wall.row.last_error_at.filter(|at| *at <= now_ms).unwrap_or(now_ms))
                .min()
                .unwrap_or(now_ms);
            let expires_ms = walls.iter().map(|wall| wall.until_ms).max().unwrap_or(now_ms);
            let longest = walls
                .iter()
                .max_by_key(|wall| wall.until_ms)
                .expect("a model has at least one wall");
            let reasons: BTreeSet<&str> = walls.iter().map(|wall| wall.reason).collect();
            let reasons = reasons.into_iter().collect::<Vec<_>>().join(", ");
            let billing = walls
                .iter()
                .any(|wall| wall.row.wall_class == Some(RouteWallClass::Billing));
            let hint = if billing {
                "A billing wall: the provider wants payment on this account. Top it up, then route.clear_cooldown {\"routeKey\":...} lifts the wall."
            } else if longest.reason == "backoff_active" {
                "Repeated terminal failures backed this route off. Fix the cause the reason names, then route.clear_failure_backoff {\"routeKey\":...} lifts it."
            } else {
                "Routing returns to the route when the wall expires. If the provider's quota view shows headroom, route.clear_cooldown {\"routeKey\":...} lifts the wall now."
            };
            Finding {
                check: CHECK_ROUTE_BLOCKS,
                key: format!("route:{model}"),
                severity: if expires_ms.saturating_sub(started_ms) > ROUTE_BLOCK_ERROR_MS {
                    Severity::Error
                } else {
                    Severity::Warning
                },
                count: walls.len() as u64,
                first_seen_ms: started_ms,
                last_seen_ms: now_ms,
                summary: clip(
                    &format!(
                        "{model}: {} route(s) blocked ({reasons}) for {} more (until {}, blocked {} so far){}",
                        walls.len(),
                        age(expires_ms, now_ms),
                        crate::account_selection::rfc3339(expires_ms),
                        age(now_ms, started_ms),
                        longest
                            .detail
                            .as_deref()
                            .map(|detail| format!("; reason: {detail}"))
                            .unwrap_or_default(),
                    ),
                    MAX_SUMMARY_CHARS,
                ),
                samples: walls
                    .iter()
                    .take(MAX_SAMPLES)
                    .map(|wall| {
                        json!({
                            "route_key": wall.row.route_key,
                            "reason": wall.reason,
                            "detail": wall.detail,
                            "wall_class": wall.row.wall_class.map(|class| class.as_str()),
                            "retry_after_ms": wall.until_ms.saturating_sub(now_ms),
                            "expires_at_ms": wall.until_ms,
                        })
                    })
                    .collect(),
                hint: hint.to_owned(),
            }
        })
        .collect()
}

/// One finding per held account.
fn account_holds(holds: &[DiagnosticAccountHold], now_ms: i64) -> Vec<Finding> {
    let mut by_account: BTreeMap<(&str, &str), Vec<&DiagnosticAccountHold>> = BTreeMap::new();
    for hold in holds {
        by_account
            .entry((hold.provider.as_str(), hold.account_id.as_str()))
            .or_default()
            .push(hold);
    }
    by_account
        .into_iter()
        .map(|((provider, account), holds)| {
            let expiry = |hold: &DiagnosticAccountHold| {
                hold.created_at
                    .saturating_add(hold.retry_after_s.saturating_mul(1000))
            };
            let expires_ms = holds.iter().map(|hold| expiry(hold)).max().unwrap_or(now_ms);
            let first_ms = holds.iter().map(|hold| hold.created_at).min().unwrap_or(now_ms);
            let basis: BTreeSet<&str> = holds.iter().map(|hold| hold.basis.as_str()).collect();
            Finding {
                check: CHECK_ROUTE_BLOCKS,
                key: format!("hold:{provider}/{account}"),
                severity: Severity::Warning,
                count: holds.len() as u64,
                first_seen_ms: first_ms,
                last_seen_ms: now_ms,
                summary: clip(
                    &format!(
                        "{provider} account {account} is in a short hold ({}); conversations on it retry in {}",
                        basis.into_iter().collect::<Vec<_>>().join(", "),
                        age(expires_ms, now_ms),
                    ),
                    MAX_SUMMARY_CHARS,
                ),
                samples: holds
                    .iter()
                    .rev()
                    .take(MAX_SAMPLES)
                    .map(|hold| {
                        json!({
                            "provider": hold.provider,
                            "account_id": hold.account_id,
                            "credential_id": hold.credential_id,
                            "reason": hold.basis,
                            "retry_after_s": hold.retry_after_s,
                            "reset_at_ms": hold.reset_at,
                            "expires_at_ms": expiry(hold),
                        })
                    })
                    .collect(),
                hint: "A hold keeps a conversation on its account until a reset that is close. No action is needed unless holds on this account keep recurring.".to_owned(),
            }
        })
        .collect()
}

/// The part of a pool key a finding names: the pool's name, or its window
/// length and model scope, or the whole account for a block without a pool.
fn pool_label(pool_key_json: Option<&str>) -> String {
    let Some(pool) = pool_key_json.and_then(|text| serde_json::from_str::<Value>(text).ok()) else {
        return "whole_account".to_owned();
    };
    if let Some(name) = pool["name"].as_str() {
        return name.to_owned();
    }
    let window = pool["window_minutes"]
        .as_u64()
        .map_or_else(|| "unknown".to_owned(), |minutes| format!("{minutes}m"));
    let scope = match (
        pool["scope"]["kind"].as_str(),
        pool["scope"]["value"].as_str(),
    ) {
        (Some(kind), Some(value)) => format!("{kind}:{value}"),
        (Some(kind), None) => kind.to_owned(),
        _ => "unknown".to_owned(),
    };
    format!("{window}/{scope}")
}

/// One finding per standing pool block. A block whose reset has passed is
/// lifted by the next admission on its provider, so it no longer blocks.
fn account_pool_blocks(blocks: &[DiagnosticAccountBlock], now_ms: i64) -> Vec<Finding> {
    blocks
        .iter()
        .filter_map(|block| {
            let evidence = serde_json::from_str::<Value>(&block.fence_json).unwrap_or(Value::Null);
            let reset_ms = serde_json::from_value::<ResetTime>(evidence["reset"].clone())
                .ok()
                .and_then(ResetTime::at_ms);
            let retry_hint_ms = evidence["retry_hint_ms"].as_i64();
            let expires_ms = match (reset_ms, retry_hint_ms) {
                (Some(reset), Some(hint)) => Some(reset.min(hint)),
                (reset, hint) => reset.or(hint),
            };
            if expires_ms.is_some_and(|expires| expires <= now_ms) {
                return None;
            }
            let pool = pool_label(block.pool_key_json.as_deref());
            let until = expires_ms.map_or_else(
                || "with no known reset; a fresh quota reading lifts it".to_owned(),
                |expires| {
                    format!(
                        "for {} more (until {})",
                        age(expires, now_ms),
                        crate::account_selection::rfc3339(expires)
                    )
                },
            );
            Some(Finding {
                check: CHECK_ROUTE_BLOCKS,
                key: format!("pool_block:{}/{}/{pool}", block.provider, block.account_id),
                severity: Severity::Warning,
                count: 1,
                first_seen_ms: block.observed_at,
                last_seen_ms: now_ms,
                summary: clip(
                    &format!(
                        "{} account {} has pool {pool} blocked ({}) {until}",
                        block.provider, block.account_id, block.source
                    ),
                    MAX_SUMMARY_CHARS,
                ),
                samples: vec![json!({
                    "provider": block.provider,
                    "account_id": block.account_id,
                    "pool": pool,
                    "reason": block.source,
                    "retry_after_ms": expires_ms.map(|expires| expires.saturating_sub(now_ms)),
                    "expires_at_ms": expires_ms,
                    "observed_at_ms": block.observed_at,
                })],
                hint: "The account's quota pool is confirmed exhausted; routing serves the pool's models from other accounts until it resets. Check route.account_quota_status if it outlasts its reset.".to_owned(),
            })
        })
        .collect()
}

/// One finding per key-pool credential walled or cooling down.
fn key_walls(walls: &[DiagnosticKeyWall], now_ms: i64) -> Vec<Finding> {
    walls
        .iter()
        .map(|wall| {
            let live = |until: Option<i64>| until.filter(|until| *until > now_ms);
            let (reason, expires_ms) = match (live(wall.walled_until), live(wall.cooldown_until)) {
                (Some(walled), cooldown) if cooldown.is_none_or(|cooldown| walled >= cooldown) => {
                    (wall.wall_reason.as_deref().unwrap_or("billing_wall"), walled)
                }
                (_, Some(cooldown)) => (wall.cooldown_reason.as_deref().unwrap_or("cooldown"), cooldown),
                _ => ("unknown", now_ms),
            };
            Finding {
                check: CHECK_ROUTE_BLOCKS,
                key: format!("key_wall:{}/{}", wall.provider, wall.credential_id),
                severity: Severity::Warning,
                count: 1,
                first_seen_ms: wall.updated_at,
                last_seen_ms: now_ms,
                summary: clip(
                    &format!(
                        "{} pool key {} is walled ({}) for {} more",
                        wall.provider,
                        wall.credential_id,
                        clip(reason, MAX_DETAIL_CHARS),
                        age(expires_ms, now_ms),
                    ),
                    MAX_SUMMARY_CHARS,
                ),
                samples: vec![json!({
                    "provider": wall.provider,
                    "credential_id": wall.credential_id,
                    "reason": clip(reason, MAX_DETAIL_CHARS),
                    "retry_after_ms": expires_ms.saturating_sub(now_ms),
                    "expires_at_ms": expires_ms,
                })],
                hint: "A billing wall holds until its re-probe time; top the key up. A rate-limit cooldown ends at the provider's retry-after.".to_owned(),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// refusal_rate
// ---------------------------------------------------------------------------

/// The note decision-based summaries carry when the window held more
/// decisions than were read.
fn truncation_note(evidence: &Evidence) -> &'static str {
    if evidence.decisions_truncated {
        " (only the window's newest 20000 decisions were read)"
    } else {
        ""
    }
}

fn refusal_hint(code: &str) -> String {
    let lead = match code {
        "no_eligible_account" => {
            "No account could serve the models this consumer needs: see this report's route_blocks (holds, pool blocks) and credential_fences for their providers."
        }
        "not_available" => {
            "The models this consumer needs are not served by any provider the runner holds a usable credential for: check the runner's credentials and the consumer's allowed providers."
        }
        "credential_billing_exhausted" | "credential_unentitled" => {
            "Every credential serving these models is fenced: see this report's credential_fences."
        }
        "quota_exhausted" | "cooldown_active" | "backoff_active" => {
            "The models this consumer needs are walled: see this report's route_blocks."
        }
        "all_routes_inadequate" | "no_adequate_route" => {
            "No admissible model clears this consumer's demand: lower its iq/eq demand or admit a stronger model."
        }
        _ => "The consumer's requests are being refused.",
    };
    format!("{lead} route.explain {{\"decisionID\":...}} on a sample lists every rejected route.")
}

fn refusal_rate(evidence: &Evidence) -> Vec<Finding> {
    let mut by_consumer: BTreeMap<&str, Vec<&DiagnosticDecision>> = BTreeMap::new();
    for decision in &evidence.decisions {
        by_consumer
            .entry(decision.consumer.as_str())
            .or_default()
            .push(decision);
    }
    by_consumer
        .into_iter()
        .filter_map(|(consumer, decisions)| {
            let refused: Vec<&DiagnosticDecision> = decisions
                .iter()
                .copied()
                .filter(|decision| decision.selected_model.is_none())
                .collect();
            if decisions.len() < REFUSAL_MIN_DECISIONS || refused.len() * 2 <= decisions.len() {
                return None;
            }
            let mut codes: BTreeMap<&str, u64> = BTreeMap::new();
            for decision in &refused {
                *codes
                    .entry(decision.refusal_code.as_deref().unwrap_or("unknown"))
                    .or_default() += 1;
            }
            let top_code = codes
                .iter()
                .max_by(|left, right| left.1.cmp(right.1).then_with(|| right.0.cmp(left.0)))
                .map(|(code, _)| *code)
                .unwrap_or("unknown");
            let by_code = codes
                .iter()
                .map(|(code, count)| format!("{code}={count}"))
                .collect::<Vec<_>>()
                .join(", ");
            Some(Finding {
                check: CHECK_REFUSAL_RATE,
                key: format!("consumer:{consumer}"),
                severity: if refused.len() == decisions.len() {
                    Severity::Error
                } else {
                    Severity::Warning
                },
                count: refused.len() as u64,
                first_seen_ms: refused.first().map_or(0, |decision| decision.created_at),
                last_seen_ms: refused.last().map_or(0, |decision| decision.created_at),
                summary: clip(
                    &format!(
                        "{consumer}: {} of {} decisions in the window were refusals; by code: {by_code}{}",
                        refused.len(),
                        decisions.len(),
                        truncation_note(evidence),
                    ),
                    MAX_SUMMARY_CHARS,
                ),
                samples: refused
                    .iter()
                    .rev()
                    .take(MAX_SAMPLES)
                    .map(|decision| {
                        json!({
                            "decision_id": decision.id,
                            "created_at_ms": decision.created_at,
                            "code": decision.refusal_code,
                            "iq": decision.iq,
                        })
                    })
                    .collect(),
                hint: refusal_hint(top_code),
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// empty_candidate_sets
// ---------------------------------------------------------------------------

/// The model (`provider/model`) of one recorded adequate candidate. The
/// selection records a candidate as `provider/model` or
/// `provider/model:variant`, and a model id may itself contain a colon, so the
/// catalog's model keys decide which reading is meant.
fn candidate_model<'a>(candidate: &'a str, models: &BTreeSet<String>) -> &'a str {
    if models.contains(candidate) {
        return candidate;
    }
    candidate
        .rsplit_once(':')
        .map(|(model, _variant)| model)
        .filter(|model| models.contains(*model))
        .unwrap_or(candidate)
}

/// The distinct models that could have served a decision: the selected one
/// and every other route that cleared the quality gate. Empty for a refusal.
fn adequate_models<'a>(
    decision: &'a DiagnosticDecision,
    models: &BTreeSet<String>,
) -> BTreeSet<&'a str> {
    let Some(selected) = decision.selected_model.as_deref() else {
        return BTreeSet::new();
    };
    std::iter::once(selected)
        .chain(
            decision
                .also_adequate
                .iter()
                .map(|other| candidate_model(other, models)),
        )
        .collect()
}

fn iq_band(iq: Option<f64>) -> String {
    match iq.filter(|iq| iq.is_finite()) {
        Some(iq) => {
            let low = (iq / IQ_BAND_WIDTH).floor() * IQ_BAND_WIDTH;
            format!("iq{low:.0}-{:.0}", low + IQ_BAND_WIDTH - 1.0)
        }
        None => "iq_unset".to_owned(),
    }
}

fn empty_candidate_sets(evidence: &Evidence) -> Vec<Finding> {
    // A head's model pin names one model on purpose, so it is no sign of a
    // thin candidate set.
    let mut groups: BTreeMap<(&str, String), Vec<(&DiagnosticDecision, BTreeSet<&str>)>> =
        BTreeMap::new();
    for decision in evidence
        .decisions
        .iter()
        .filter(|decision| !decision.pinned)
    {
        groups
            .entry((decision.consumer.as_str(), iq_band(decision.iq)))
            .or_default()
            .push((
                decision,
                adequate_models(decision, &evidence.catalog_models),
            ));
    }
    groups
        .into_iter()
        .filter_map(|((consumer, band), decisions)| {
            let thin: Vec<&(&DiagnosticDecision, BTreeSet<&str>)> = decisions
                .iter()
                .filter(|(_, models)| models.len() <= 1)
                .collect();
            let (latest, latest_models) = decisions.last()?;
            let newest_thin = thin.last()?;
            let single_models: BTreeSet<&str> = thin
                .iter()
                .filter_map(|(_, models)| models.iter().next().copied())
                .collect();
            let none = thin.iter().filter(|(_, models)| models.is_empty()).count();
            let mut sets = Vec::new();
            if thin.len() > none {
                sets.push(format!(
                    "{} had one adequate model ({})",
                    thin.len() - none,
                    single_models.iter().copied().collect::<Vec<_>>().join(", ")
                ));
            }
            if none > 0 {
                sets.push(format!("{none} had none"));
            }
            Some(Finding {
                check: CHECK_EMPTY_CANDIDATE_SETS,
                key: format!("consumer:{consumer}/{band}"),
                // The group's latest decision in the window had no adequate
                // model at all: the consumer was already stopped, not just
                // one wall away from it.
                severity: if latest_models.is_empty() {
                    Severity::Error
                } else {
                    Severity::Warning
                },
                count: thin.len() as u64,
                first_seen_ms: thin.first().map_or(latest.created_at, |(decision, _)| decision.created_at),
                last_seen_ms: newest_thin.0.created_at,
                summary: clip(
                    &format!(
                        "{consumer} at {band}: of {} decisions in the window, {}; a single wall stops these requests{}",
                        decisions.len(),
                        sets.join(" and "),
                        truncation_note(evidence),
                    ),
                    MAX_SUMMARY_CHARS,
                ),
                samples: thin
                    .iter()
                    .rev()
                    .take(MAX_SAMPLES)
                    .map(|(decision, models)| {
                        json!({
                            "consumer": decision.consumer,
                            "iq": decision.iq,
                            "remaining_model": models.iter().next(),
                            "decision_id": decision.id,
                            "created_at_ms": decision.created_at,
                        })
                    })
                    .collect(),
                hint: "Give these requests a second model: add a credential or provider that serves another model clearing this band, widen the consumer's allowed providers, or lower its iq demand.".to_owned(),
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Admission and the wire
// ---------------------------------------------------------------------------

/// Who may read the diagnosis: a head route (a route bound under an agent
/// session's scope, owned by a verified agent host) and a flow scope (a
/// route bound under a scope the daemon stamped with a flow id from an
/// authorized scope owner). Everyone else is refused, including unscoped
/// module routes and the operator's direct routes, which have their own
/// operator ops.
pub(crate) fn admit(caller: &routing_v1::Caller) -> Result<(), String> {
    let refused = || {
        format!(
            "{OP} is served only to head routes and flow scopes; principal '{}' on this route is neither",
            caller.principal
        )
    };
    if !caller.binding_is_current() {
        return Err(refused());
    }
    let Some(scope) = caller.scope_authority.as_ref() else {
        return Err(refused());
    };
    if scope.flow_id.is_some() && scope.owner_authorized {
        return Ok(());
    }
    let head_session = scope.kind == subc_protocol::scope::ScopeKind::Head
        && scope.flow_id.is_none()
        && scope.run_id.is_none()
        && scope
            .owner
            .strip_prefix("reserved:")
            .is_some_and(routing_v1::consumers::provides_agent_host);
    if head_session {
        Ok(())
    } else {
        Err(refused())
    }
}

/// The operations a flow-scoped route may call. Routing declares
/// `flow-scopes/v1` only so flows can read this diagnosis; its other
/// operations have not been reviewed for flow behaviour (a flow must never
/// act as its owner agent), so they stay closed to flow scopes until they
/// are reviewed and added here.
pub(crate) const FLOW_SCOPE_OPS: &[&str] = &[OP];

/// The refusal code for an operation outside [`FLOW_SCOPE_OPS`] called under
/// a flow scope. Basal treats it as final and never retries the call.
pub(crate) const FLOW_SCOPE_NOT_SUPPORTED: &str = "flow_scope_not_supported";

/// The refusal a flow-scoped route gets for an operation outside
/// [`FLOW_SCOPE_OPS`]. A route is flow-scoped when the daemon stamped its
/// scope with a flow id; any other route, scoped or not, gets `None` and is
/// served as before.
pub(crate) fn flow_route_refusal(body: &[u8], channel: u16, epoch: u32) -> Option<ErrorBody> {
    crate::route_scope_authority(channel, epoch)?.flow_id?;
    let op = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|request| request.get("method")?.as_str().map(str::to_owned))
        .unwrap_or_default();
    (!FLOW_SCOPE_OPS.contains(&op.as_str())).then(|| flow_scope_refusal(&op))
}

fn flow_scope_refusal(op: &str) -> ErrorBody {
    ErrorBody::new(
        FLOW_SCOPE_NOT_SUPPORTED,
        format!(
            "{op} does not run under a flow scope; flows may call: {}",
            FLOW_SCOPE_OPS.join(", ")
        ),
    )
    .with_detail(json!({ "op": op, "allowed_ops": FLOW_SCOPE_OPS }))
}

fn parse_request(body: &[u8]) -> Result<Request, String> {
    let request: Value = serde_json::from_slice(body)
        .map_err(|error| format!("{OP} request body is not JSON: {error}"))?;
    match request.get("params") {
        None | Some(Value::Null) => Ok(Request::default()),
        Some(params) => serde_json::from_value::<Request>(params.clone())
            .map_err(|error| format!("{OP} params not decodable: {error}")),
    }
}

fn window_ms(request: &Request) -> Result<i64, String> {
    let window = request.window_ms.unwrap_or(DEFAULT_WINDOW_MS);
    if window == 0 || window > MAX_WINDOW_MS {
        return Err(format!(
            "{OP} window_ms must be between 1 and {MAX_WINDOW_MS} (seven days)"
        ));
    }
    Ok(window as i64)
}

/// Answer one `diagnostics.findings` request on its route.
pub(crate) async fn handle(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let reply = answer(&frame.body, channel, epoch, store);
    let (ty, body) = match reply {
        Ok(result) => (
            FrameType::Response,
            serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?,
        ),
        Err(error) => (
            FrameType::Error,
            serde_json::to_vec(&error).map_err(ModuleError::Json)?,
        ),
    };
    let response = Frame::build_with_version(
        ver,
        ty,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

fn answer(body: &[u8], channel: u16, epoch: u32, store: &SharedStore) -> Result<Value, ErrorBody> {
    admit(&routing_v1::Caller::for_route(channel, epoch))
        .map_err(|message| ErrorBody::new("not_permitted", message))?;
    let request =
        parse_request(body).map_err(|message| ErrorBody::new("invalid_request", message))?;
    let window_ms =
        window_ms(&request).map_err(|message| ErrorBody::new("invalid_request", message))?;
    let now_ms = crate::now_ms() as i64;
    let evidence = Evidence::gather(store, now_ms, window_ms)
        .map_err(|message| ErrorBody::new("store_error", format!("{OP}: {message}")))?;
    Ok(report(&evidence, now_ms, window_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000_000;
    const MINUTE: i64 = 60_000;

    fn empty() -> Evidence {
        Evidence {
            fences: Vec::new(),
            route_walls: Vec::new(),
            holds: Vec::new(),
            account_blocks: Vec::new(),
            key_walls: Vec::new(),
            decisions: Vec::new(),
            decisions_truncated: false,
            catalog_models: BTreeSet::new(),
        }
    }

    fn findings(evidence: &Evidence, check: &str) -> Vec<Value> {
        report(evidence, NOW, DEFAULT_WINDOW_MS as i64)["findings"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|finding| finding["check"] == check)
            .cloned()
            .collect()
    }

    fn fence(
        provider: &str,
        credential: &str,
        code: &str,
        age_ms: i64,
    ) -> CredentialEntitlementFence {
        CredentialEntitlementFence {
            provider_id: provider.into(),
            credential_id: credential.into(),
            code: code.into(),
            listing_fingerprint: None,
            observed_at_ms: NOW - age_ms,
        }
    }

    fn decision(
        id: &str,
        consumer: &str,
        at: i64,
        selected: Option<&str>,
        also: &[&str],
        code: Option<&str>,
        iq: Option<f64>,
    ) -> DiagnosticDecision {
        DiagnosticDecision {
            id: id.into(),
            created_at: at,
            consumer: consumer.into(),
            selected_model: selected.map(str::to_owned),
            iq,
            also_adequate: also.iter().map(|other| (*other).to_owned()).collect(),
            pinned: false,
            refusal_code: code.map(str::to_owned),
        }
    }

    #[test]
    fn a_clean_routing_module_reports_no_finding() {
        let reply = report(&empty(), NOW, 21_600_000);
        assert_eq!(
            reply,
            json!({"generated_at_ms": NOW, "window_ms": 21_600_000, "findings": []})
        );
    }

    #[test]
    fn credential_fences_name_every_fence_and_turn_error_after_an_hour() {
        let mut evidence = empty();
        evidence.fences = vec![
            fence(
                "inception",
                "apikey:inception:main",
                "billing_exhausted",
                2 * 60 * MINUTE,
            ),
            fence("xai", "apikey:xai:work", "ENTITLEMENT_ERROR", 10 * MINUTE),
        ];
        let found = findings(&evidence, CHECK_CREDENTIAL_FENCES);
        assert_eq!(found.len(), 2, "{found:?}");
        // The older fence is an error and sorts first.
        assert_eq!(found[0]["key"], "inception/apikey:inception:main");
        assert_eq!(found[0]["severity"], "error");
        assert_eq!(found[0]["samples"][0]["code"], "billing_exhausted");
        assert_eq!(found[0]["samples"][0]["kind"], "billing");
        assert_eq!(found[0]["samples"][0]["age_ms"], 2 * 60 * MINUTE);
        assert!(found[0]["hint"]
            .as_str()
            .unwrap()
            .contains("route.clear_credential_fences"));
        assert_eq!(found[1]["key"], "xai/apikey:xai:work");
        assert_eq!(found[1]["severity"], "warning");
        assert_eq!(found[1]["samples"][0]["kind"], "entitlement");
        assert!(findings(&empty(), CHECK_CREDENTIAL_FENCES).is_empty());
    }

    fn wall(route: &str, model: &str) -> DiagnosticRouteWall {
        DiagnosticRouteWall {
            route_key: route.into(),
            capacity_key: model.into(),
            provider_id: model.split('/').next().unwrap().into(),
            model_id: model.split('/').nth(1).unwrap().into(),
            cooldown_until: None,
            quota_exhausted_until: None,
            failure_backoff_until: None,
            failure_backoff_reason: None,
            last_error: None,
            last_error_at: None,
            wall_class: None,
        }
    }

    #[test]
    fn route_blocks_name_walls_holds_pool_blocks_and_key_walls_standing_now() {
        let mut evidence = empty();
        let mut long = wall("inception/mercury-3", "inception/mercury-3");
        long.quota_exhausted_until = Some((NOW + 60 * MINUTE) as f64);
        long.last_error = Some("429 quota window exhausted".into());
        long.last_error_at = Some(NOW - 20 * MINUTE);
        let mut variant = wall("inception/mercury-3/high", "inception/mercury-3");
        variant.cooldown_until = Some((NOW + 5 * MINUTE) as f64);
        let mut short = wall("openai/gpt-x", "openai/gpt-x");
        short.failure_backoff_until = Some((NOW + 5 * MINUTE) as f64);
        short.failure_backoff_reason = Some("terminal send failure".into());
        evidence.route_walls = vec![long, variant, short];
        evidence.holds = vec![DiagnosticAccountHold {
            provider: "anthropic".into(),
            account_id: "work".into(),
            credential_id: "oauth:anthropic:work".into(),
            basis: "reset_soon".into(),
            reset_at: NOW + 2 * MINUTE,
            retry_after_s: 90,
            created_at: NOW - 30_000,
        }];
        let block = |id: &str, reset_ms: i64, pool: Option<&str>| DiagnosticAccountBlock {
            fence_id: id.into(),
            provider: "anthropic".into(),
            account_id: "home".into(),
            pool_key_json: pool.map(str::to_owned),
            fence_json: json!({
                "account_id": "home", "observed_at_ms": NOW - MINUTE,
                "reset": {"kind": "at", "at_ms": reset_ms}, "source": "reading",
            })
            .to_string(),
            source: "reading".into(),
            observed_at: NOW - MINUTE,
            created_at: NOW - MINUTE,
        };
        evidence.account_blocks = vec![
            block(
                "standing",
                NOW + 3 * 60 * MINUTE,
                Some(r#"{"window_minutes":10080,"scope":{"kind":"every_model"},"name":null}"#),
            ),
            // Its reset passed: the next admission clears it, so it blocks nothing.
            block("lapsed", NOW - MINUTE, None),
        ];
        evidence.key_walls = vec![DiagnosticKeyWall {
            provider: "anthropic-platform".into(),
            credential_id: "apikey:anthropic:team".into(),
            walled_until: Some(NOW + 30 * MINUTE),
            wall_reason: Some("billing".into()),
            cooldown_until: None,
            cooldown_reason: None,
            updated_at: NOW - MINUTE,
        }];
        let found = findings(&evidence, CHECK_ROUTE_BLOCKS);
        let keys: Vec<&str> = found.iter().map(|f| f["key"].as_str().unwrap()).collect();
        assert_eq!(
            keys,
            [
                "route:inception/mercury-3",
                "hold:anthropic/work",
                "key_wall:anthropic-platform/apikey:anthropic:team",
                "pool_block:anthropic/home/10080m/every_model",
                "route:openai/gpt-x",
            ],
            "{found:#?}"
        );
        let mercury = &found[0];
        assert_eq!(mercury["severity"], "error", "blocked 80 minutes in all");
        assert_eq!(mercury["count"], 2);
        assert_eq!(mercury["first_seen_ms"], NOW - 20 * MINUTE);
        assert_eq!(mercury["samples"][0]["reason"], "quota_exhausted");
        assert_eq!(mercury["samples"][0]["retry_after_ms"], 60 * MINUTE);
        assert_eq!(mercury["samples"][0]["expires_at_ms"], NOW + 60 * MINUTE);
        assert_eq!(mercury["samples"][1]["reason"], "cooldown_active");
        let gpt = found.last().unwrap();
        assert_eq!(gpt["severity"], "warning", "a five-minute backoff");
        assert_eq!(gpt["samples"][0]["reason"], "backoff_active");
        assert_eq!(gpt["samples"][0]["detail"], "terminal send failure");
        assert_eq!(found[1]["samples"][0]["retry_after_s"], 90);
        assert_eq!(found[1]["samples"][0]["reason"], "reset_soon");
        assert_eq!(
            found[3]["samples"][0]["expires_at_ms"],
            NOW + 3 * 60 * MINUTE
        );

        let mut clean = empty();
        clean.account_blocks = vec![block("lapsed", NOW - MINUTE, None)];
        assert!(findings(&clean, CHECK_ROUTE_BLOCKS).is_empty());
    }

    #[test]
    fn refusal_rate_names_consumers_whose_decisions_were_mostly_refused() {
        let mut evidence = empty();
        let refused = |id: &str, consumer: &str, at: i64, code: &str| {
            decision(id, consumer, at, None, &[], Some(code), Some(40.0))
        };
        let served = |id: &str, consumer: &str, at: i64| {
            decision(
                id,
                consumer,
                at,
                Some("p/m"),
                &["q/n", "r/o"],
                None,
                Some(40.0),
            )
        };
        evidence.decisions = vec![
            // mason: three of four refused.
            refused("m1", "mason", NOW - 50 * MINUTE, "no_eligible_account"),
            served("m2", "mason", NOW - 40 * MINUTE),
            refused("m3", "mason", NOW - 30 * MINUTE, "not_available"),
            refused("m4", "mason", NOW - 20 * MINUTE, "no_eligible_account"),
            // flow: every decision refused.
            refused(
                "f1",
                "flow",
                NOW - 15 * MINUTE,
                "credential_billing_exhausted",
            ),
            refused(
                "f2",
                "flow",
                NOW - 14 * MINUTE,
                "credential_billing_exhausted",
            ),
            refused(
                "f3",
                "flow",
                NOW - 13 * MINUTE,
                "credential_billing_exhausted",
            ),
            // sidekick: one of four refused.
            refused("s1", "sidekick", NOW - 12 * MINUTE, "not_available"),
            served("s2", "sidekick", NOW - 11 * MINUTE),
            served("s3", "sidekick", NOW - 10 * MINUTE),
            served("s4", "sidekick", NOW - 9 * MINUTE),
            // lab: refused, but too few decisions to call a pattern.
            refused("l1", "lab", NOW - 8 * MINUTE, "not_available"),
            refused("l2", "lab", NOW - 7 * MINUTE, "not_available"),
        ];
        let found = findings(&evidence, CHECK_REFUSAL_RATE);
        assert_eq!(found.len(), 2, "{found:#?}");
        assert_eq!(found[0]["key"], "consumer:flow");
        assert_eq!(found[0]["severity"], "error");
        assert_eq!(found[0]["count"], 3);
        assert!(found[0]["hint"]
            .as_str()
            .unwrap()
            .contains("credential_fences"));
        let mason = &found[1];
        assert_eq!(mason["key"], "consumer:mason");
        assert_eq!(mason["severity"], "warning");
        assert_eq!(mason["count"], 3);
        assert_eq!(mason["first_seen_ms"], NOW - 50 * MINUTE);
        assert_eq!(mason["last_seen_ms"], NOW - 20 * MINUTE);
        let summary = mason["summary"].as_str().unwrap();
        assert!(summary.contains("3 of 4"), "{summary}");
        assert!(
            summary.contains("no_eligible_account=2, not_available=1"),
            "{summary}"
        );
        assert_eq!(mason["samples"][0]["decision_id"], "m4", "newest first");
        assert_eq!(mason["samples"][0]["code"], "no_eligible_account");

        let mut clean = empty();
        clean.decisions = (0..5)
            .map(|n| served(&format!("c{n}"), "mason", NOW - n * MINUTE))
            .collect();
        assert!(findings(&clean, CHECK_REFUSAL_RATE).is_empty());
    }

    #[test]
    fn empty_candidate_sets_name_bands_where_one_model_or_none_could_serve() {
        let mut evidence = empty();
        evidence.catalog_models = ["p/m", "q/n", "ollama/qwen3:32b"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let mut pinned = decision(
            "pin",
            "mason",
            NOW - 9 * MINUTE,
            Some("p/m"),
            &[],
            None,
            Some(20.0),
        );
        pinned.pinned = true;
        evidence.decisions = vec![
            // mason at iq 40-49: one decision had a single model (its other
            // adequate route is another variant of the same model), then a
            // healthy one.
            decision(
                "a",
                "mason",
                NOW - 30 * MINUTE,
                Some("p/m"),
                &["p/m:high"],
                None,
                Some(45.0),
            ),
            decision(
                "b",
                "mason",
                NOW - 20 * MINUTE,
                Some("p/m"),
                &["q/n"],
                None,
                Some(47.0),
            ),
            // athena at iq 60-69: the latest decision had no model at all.
            decision(
                "c",
                "athena",
                NOW - 10 * MINUTE,
                Some("q/n"),
                &[],
                None,
                Some(62.0),
            ),
            decision(
                "d",
                "athena",
                NOW - 5 * MINUTE,
                None,
                &[],
                Some("quota_exhausted"),
                Some(61.0),
            ),
            // sidekick: a model id containing a colon is still a second model.
            decision(
                "e",
                "sidekick",
                NOW - 4 * MINUTE,
                Some("p/m"),
                &["ollama/qwen3:32b"],
                None,
                Some(30.0),
            ),
            // A head's pin names one model on purpose.
            pinned,
        ];
        let found = findings(&evidence, CHECK_EMPTY_CANDIDATE_SETS);
        let keys: Vec<&str> = found.iter().map(|f| f["key"].as_str().unwrap()).collect();
        assert_eq!(
            keys,
            ["consumer:athena/iq60-69", "consumer:mason/iq40-49"],
            "{found:#?}"
        );
        let athena = &found[0];
        assert_eq!(athena["severity"], "error");
        assert_eq!(athena["count"], 2);
        assert_eq!(athena["samples"][0]["remaining_model"], Value::Null);
        assert_eq!(athena["samples"][1]["remaining_model"], "q/n");
        assert_eq!(athena["samples"][1]["iq"], 62.0);
        let mason = &found[1];
        assert_eq!(mason["severity"], "warning");
        assert_eq!(mason["count"], 1);
        assert_eq!(mason["samples"][0]["consumer"], "mason");
        assert_eq!(mason["samples"][0]["remaining_model"], "p/m");
        assert_eq!(mason["samples"][0]["decision_id"], "a");

        let mut clean = empty();
        clean.catalog_models = evidence.catalog_models.clone();
        clean.decisions = vec![decision(
            "z",
            "mason",
            NOW,
            Some("p/m"),
            &["q/n"],
            None,
            Some(45.0),
        )];
        assert!(findings(&clean, CHECK_EMPTY_CANDIDATE_SETS).is_empty());
    }

    #[test]
    fn the_reply_stays_under_64_kib_keeping_the_most_severe_findings() {
        let mut evidence = empty();
        evidence.fences = (0..2_000)
            .map(|n| {
                let age = if n < 10 { 2 * 60 * MINUTE } else { MINUTE };
                fence(
                    "provider",
                    &format!("credential-{n:04}-{}", "x".repeat(40)),
                    "billing_exhausted",
                    age,
                )
            })
            .collect();
        let reply = report(&evidence, NOW, DEFAULT_WINDOW_MS as i64);
        assert!(reply_bytes(&reply) < MAX_REPLY_BYTES);
        let kept = reply["findings"].as_array().unwrap();
        assert!(kept.len() > 10 && kept.len() < 2_000, "{}", kept.len());
        assert!(kept[..10]
            .iter()
            .all(|finding| finding["severity"] == "error"));
    }

    #[test]
    fn requests_take_an_optional_bounded_window_and_nothing_else() {
        let parse = |body: Value| {
            parse_request(&serde_json::to_vec(&body).unwrap()).and_then(|r| window_ms(&r))
        };
        assert_eq!(parse(json!({"method": OP})), Ok(DEFAULT_WINDOW_MS as i64));
        assert_eq!(
            parse(json!({"method": OP, "params": {}})),
            Ok(DEFAULT_WINDOW_MS as i64)
        );
        assert_eq!(
            parse(json!({"method": OP, "params": {"window_ms": 60_000}})),
            Ok(60_000)
        );
        assert!(parse(json!({"method": OP, "params": {"window_ms": 0}})).is_err());
        assert!(parse(json!({"method": OP, "params": {"window_ms": MAX_WINDOW_MS + 1}})).is_err());
        assert!(parse(json!({"method": OP, "params": {"since": 1}})).is_err());
    }
}
