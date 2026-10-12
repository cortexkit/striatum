//! Choosing one key from a pool of interchangeable credentials for one routing
//! provider (the paid Anthropic API keys behind `anthropic-platform`).
//!
//! Every vault credential whose `provider_ids` names the pool provider is a
//! member, keyed by its full credential id. A key belongs to a workspace: the
//! vault row's `account_id` when present, otherwise a workspace of its own.
//! Anthropic isolates prompt caches per workspace, so a session that has run on
//! one workspace stays on it while any of its keys is admissible, and moves
//! only when none is.
//!
//! This module is the pure decision. The store supplies runs in flight (live
//! leases), last use, walls and cooldowns; the caller records the result.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Reason a key is not admissible: its vault row is not usable (a 401 left it
/// `needs_reauth` or `invalid`).
pub const POOL_KEY_VAULT_STATE: &str = "vault_state";
/// Reason a key is not admissible: Broca reported `billing_exhausted` for it.
pub const POOL_KEY_BILLING_EXHAUSTED: &str = "billing_exhausted";
/// Reason a key is not admissible: a 429 ended a run on it and its retry-after
/// has not passed.
pub const POOL_KEY_RATE_LIMITED: &str = "rate_limited";
/// Refusal code when no key of an explicit-only pool provider is admissible.
/// There is no fallback: the subscription logins are never used instead.
pub const EXPLICIT_PROVIDER_POOL_EXHAUSTED: &str = "explicit_provider_pool_exhausted";

/// One member of a provider's key pool, from the vault listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolKey {
    /// The full vault credential id, for example `apikey:anthropic:work`.
    pub credential_id: String,
    /// The prompt-cache workspace: the vault row's `account_id`, or a workspace
    /// holding only this key when the row has none.
    pub workspace: String,
    /// The vault row's lifecycle state, verbatim (`active`, `needs_reauth`, ...).
    pub vault_state: String,
}

impl PoolKey {
    pub fn new(credential_id: &str, account_id: Option<&str>, vault_state: &str) -> Self {
        let workspace = account_id
            .map(str::trim)
            .filter(|account| !account.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("key:{credential_id}"));
        Self {
            credential_id: credential_id.to_string(),
            workspace,
            vault_state: vault_state.to_string(),
        }
    }
}

/// What the store knows about one key at decision time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolKeyState {
    /// Live leases on the key: runs in flight that are still renewed.
    pub in_flight: u32,
    pub last_used_at_ms: Option<i64>,
    /// Set when Broca reported the key out of credit: no run may use it until this
    /// time, after which one run re-probes it.
    pub walled_until_ms: Option<i64>,
    /// A rate-limit cooldown on this key, until its retry-after.
    pub cooldown_until_ms: Option<i64>,
}

/// Why one key could not be chosen, and when that may clear.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolBlockedKey {
    pub credential_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at_ms: Option<i64>,
}

/// The chosen key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolPick {
    pub credential_id: String,
    pub workspace: String,
    /// Set when the session was bound to another workspace with no admissible
    /// key: the workspace it left and why.
    pub moved_from: Option<String>,
    pub move_reason: Option<String>,
}

/// Whether `key` may take a new run now, or why not.
pub fn key_block(key: &PoolKey, state: &PoolKeyState, now_ms: i64) -> Option<PoolBlockedKey> {
    let blocked = |reason: String, reset_at_ms: Option<i64>| {
        Some(PoolBlockedKey {
            credential_id: key.credential_id.clone(),
            reason,
            reset_at_ms,
        })
    };
    if key.vault_state != "active" {
        return blocked(format!("{POOL_KEY_VAULT_STATE}:{}", key.vault_state), None);
    }
    if let Some(until) = state.walled_until_ms.filter(|until| *until > now_ms) {
        return blocked(POOL_KEY_BILLING_EXHAUSTED.into(), Some(until));
    }
    if let Some(until) = state.cooldown_until_ms.filter(|until| *until > now_ms) {
        return blocked(POOL_KEY_RATE_LIMITED.into(), Some(until));
    }
    None
}

/// Pick a key for one run.
///
/// - A session bound to a workspace picks among that workspace's admissible
///   keys: fewest runs in flight, ties to the least recently used.
/// - A new session, or one whose workspace has no admissible key, picks the
///   workspace with the fewest runs in flight (ties to the least recently used
///   workspace), then the key inside it the same way.
///   (So a bound session whose workspace has no admissible key falls back to
///   this workspace choice, and the pick records the move.)
/// - No admissible key anywhere: every key with its reason, never a guess.
pub fn pick_pool_key(
    keys: &[PoolKey],
    states: &BTreeMap<String, PoolKeyState>,
    bound_workspace: Option<&str>,
    now_ms: i64,
) -> Result<PoolPick, Vec<PoolBlockedKey>> {
    let empty = PoolKeyState::default();
    let state = |key: &PoolKey| states.get(&key.credential_id).unwrap_or(&empty);
    let mut blocked = Vec::new();
    let mut admissible = Vec::new();
    for key in keys {
        match key_block(key, state(key), now_ms) {
            Some(block) => blocked.push(block),
            None => admissible.push(key),
        }
    }
    if admissible.is_empty() {
        return Err(blocked);
    }
    // Ordering key: runs in flight, then last use (never used sorts first),
    // then the id so equal keys order the same way every time.
    let key_rank = |key: &&PoolKey| {
        let state = state(key);
        (
            state.in_flight,
            state.last_used_at_ms.unwrap_or(i64::MIN),
            key.credential_id.clone(),
        )
    };
    if let Some(bound) = bound_workspace {
        if let Some(key) = admissible
            .iter()
            .copied()
            .filter(|key| key.workspace == bound)
            .min_by_key(key_rank)
        {
            return Ok(PoolPick {
                credential_id: key.credential_id.clone(),
                workspace: key.workspace.clone(),
                moved_from: None,
                move_reason: None,
            });
        }
    }
    let workspaces: BTreeSet<&str> = admissible
        .iter()
        .map(|key| key.workspace.as_str())
        .collect();
    let workspace = workspaces
        .into_iter()
        .min_by_key(|workspace| {
            let members = keys.iter().filter(|key| key.workspace == *workspace);
            let in_flight: u32 = members.clone().map(|key| state(key).in_flight).sum();
            let last_used = members
                .filter_map(|key| state(key).last_used_at_ms)
                .max()
                .unwrap_or(i64::MIN);
            (in_flight, last_used, workspace.to_string())
        })
        .expect("admissible keys name at least one workspace");
    let key = admissible
        .iter()
        .copied()
        .filter(|key| key.workspace == workspace)
        .min_by_key(key_rank)
        .expect("the chosen workspace has an admissible key");
    let moved_from = bound_workspace
        .filter(|bound| *bound != workspace)
        .map(str::to_string);
    let move_reason = moved_from.as_ref().map(|from| {
        let reasons = blocked
            .iter()
            .filter(|block| {
                keys.iter()
                    .any(|key| key.credential_id == block.credential_id && key.workspace == *from)
            })
            .map(|block| format!("{} {}", block.credential_id, block.reason))
            .collect::<Vec<_>>();
        if reasons.is_empty() {
            format!("workspace {from} has no key in the vault listing")
        } else {
            format!(
                "no admissible key in workspace {from}: {}",
                reasons.join(", ")
            )
        }
    });
    Ok(PoolPick {
        credential_id: key.credential_id.clone(),
        workspace: key.workspace.clone(),
        moved_from,
        move_reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_000_000;

    fn states(rows: &[(&str, u32, Option<i64>)]) -> BTreeMap<String, PoolKeyState> {
        rows.iter()
            .map(|(id, in_flight, last_used)| {
                (
                    id.to_string(),
                    PoolKeyState {
                        in_flight: *in_flight,
                        last_used_at_ms: *last_used,
                        ..Default::default()
                    },
                )
            })
            .collect()
    }

    #[test]
    fn spread_takes_the_fewest_runs_in_flight_then_the_least_recent_use() {
        // Three keys with no workspace each stand alone.
        let keys = [
            PoolKey::new("apikey:anthropic", None, "active"),
            PoolKey::new("apikey:anthropic:a", None, "active"),
            PoolKey::new("apikey:anthropic:b", None, "active"),
        ];
        let pick = |rows: &[(&str, u32, Option<i64>)]| {
            pick_pool_key(&keys, &states(rows), None, NOW)
                .unwrap()
                .credential_id
        };
        assert_eq!(
            pick(&[
                ("apikey:anthropic", 2, Some(1)),
                ("apikey:anthropic:a", 1, Some(9)),
                ("apikey:anthropic:b", 1, Some(5))
            ]),
            "apikey:anthropic:b",
            "fewest in flight, then least recently used"
        );
        assert_eq!(
            pick(&[
                ("apikey:anthropic", 0, Some(1)),
                ("apikey:anthropic:a", 0, None),
                ("apikey:anthropic:b", 0, Some(5))
            ]),
            "apikey:anthropic:a",
            "a never-used key is the least recently used"
        );
    }

    #[test]
    fn a_walled_or_cooling_or_unusable_key_is_skipped() {
        let keys = [
            PoolKey::new("apikey:anthropic:walled", None, "active"),
            PoolKey::new("apikey:anthropic:cool", None, "active"),
            PoolKey::new("apikey:anthropic:reauth", None, "needs_reauth"),
            PoolKey::new("apikey:anthropic:busy", None, "active"),
        ];
        let mut rows = states(&[("apikey:anthropic:busy", 5, Some(1))]);
        rows.insert(
            "apikey:anthropic:walled".into(),
            PoolKeyState {
                walled_until_ms: Some(NOW + 1),
                ..Default::default()
            },
        );
        rows.insert(
            "apikey:anthropic:cool".into(),
            PoolKeyState {
                cooldown_until_ms: Some(NOW + 1),
                ..Default::default()
            },
        );
        let pick = pick_pool_key(&keys, &rows, None, NOW).unwrap();
        assert_eq!(pick.credential_id, "apikey:anthropic:busy");
        // An expired wall and cooldown no longer block.
        let pick = pick_pool_key(&keys, &rows, None, NOW + 2).unwrap();
        assert_eq!(pick.credential_id, "apikey:anthropic:cool");
    }

    #[test]
    fn no_admissible_key_lists_every_key_with_its_reason_and_reset() {
        let keys = [
            PoolKey::new("apikey:anthropic", None, "active"),
            PoolKey::new("apikey:anthropic:b", None, "invalid"),
        ];
        let mut rows = BTreeMap::new();
        rows.insert(
            "apikey:anthropic".into(),
            PoolKeyState {
                walled_until_ms: Some(NOW + 60_000),
                ..Default::default()
            },
        );
        let blocked = pick_pool_key(&keys, &rows, None, NOW).unwrap_err();
        assert_eq!(
            blocked,
            vec![
                PoolBlockedKey {
                    credential_id: "apikey:anthropic".into(),
                    reason: "billing_exhausted".into(),
                    reset_at_ms: Some(NOW + 60_000),
                },
                PoolBlockedKey {
                    credential_id: "apikey:anthropic:b".into(),
                    reason: "vault_state:invalid".into(),
                    reset_at_ms: None,
                },
            ]
        );
    }

    #[test]
    fn a_bound_session_stays_in_its_workspace_and_moves_only_when_it_has_no_admissible_key() {
        let keys = [
            PoolKey::new("apikey:anthropic:w1a", Some("ws-1"), "active"),
            PoolKey::new("apikey:anthropic:w1b", Some("ws-1"), "active"),
            PoolKey::new("apikey:anthropic:w2a", Some("ws-2"), "active"),
        ];
        // ws-1 is busier than ws-2, but the session's cache lives in ws-1.
        let rows = states(&[
            ("apikey:anthropic:w1a", 3, Some(5)),
            ("apikey:anthropic:w1b", 2, Some(6)),
        ]);
        let sticky = pick_pool_key(&keys, &rows, Some("ws-1"), NOW).unwrap();
        assert_eq!(sticky.credential_id, "apikey:anthropic:w1b");
        assert_eq!(sticky.moved_from, None);
        // A new session spreads to the idle workspace.
        let fresh = pick_pool_key(&keys, &rows, None, NOW).unwrap();
        assert_eq!(fresh.workspace, "ws-2");
        // Both ws-1 keys walled: the session moves, and says why.
        let mut walled = rows.clone();
        for id in ["apikey:anthropic:w1a", "apikey:anthropic:w1b"] {
            walled.get_mut(id).unwrap().walled_until_ms = Some(NOW + 1);
        }
        let moved = pick_pool_key(&keys, &walled, Some("ws-1"), NOW).unwrap();
        assert_eq!(moved.credential_id, "apikey:anthropic:w2a");
        assert_eq!(moved.moved_from.as_deref(), Some("ws-1"));
        assert!(
            moved
                .move_reason
                .as_deref()
                .unwrap()
                .contains("apikey:anthropic:w1a billing_exhausted"),
            "{moved:?}"
        );
    }

    #[test]
    fn keys_without_an_account_id_stick_per_key() {
        let keys = [
            PoolKey::new("apikey:anthropic:x", None, "active"),
            PoolKey::new("apikey:anthropic:y", None, "active"),
        ];
        assert_eq!(keys[0].workspace, "key:apikey:anthropic:x");
        // The bound key is busier, but the session stays on it.
        let rows = states(&[("apikey:anthropic:x", 4, Some(9))]);
        let pick = pick_pool_key(&keys, &rows, Some("key:apikey:anthropic:x"), NOW).unwrap();
        assert_eq!(pick.credential_id, "apikey:anthropic:x");
    }
}
