//! Demand history bounded by the allocator's own observation points must
//! decide exactly as the whole history does.

use super::*;
use striatum_core::accounts::{
    AccountStrategy, AccountView, AllocationContext, AllocatorPolicy, AllocatorState,
    CredentialRecord, CreditReading, Fence, ModelReach, ObservationSource, QuotaReading,
    QuotaWindow, ResetTime, Roster, ScopedList,
};

const NOW: i64 = 1_800_000_000_000;

fn reading(observed_at_ms: i64, used: f64) -> QuotaReading {
    let window = |minutes, reset: i64| QuotaWindow {
        used_percent: used,
        raw_utilization: None,
        source: ObservationSource::Poll,
        resets_at: ResetTime::At(reset),
        window_minutes: minutes,
        window_kind: None,
    };
    let five_hour = window(300, NOW + 3_600_000);
    let seven_day = window(10_080, NOW + 4 * 24 * 3_600_000);
    QuotaReading {
        observed_at_ms: Some(observed_at_ms),
        error_class: None,
        stale: None,
        five_hour: Some(five_hour.clone()),
        seven_day: Some(seven_day.clone()),
        general_windows: Some(vec![five_hour, seven_day]),
        scoped: ScopedList::Missing,
        pool_windows: None,
    }
}

fn view(account: &str, quota: Option<QuotaReading>) -> AccountView {
    AccountView {
        account_id: account.into(),
        routing_provider: "anthropic".into(),
        refresh_adapter: "anthropic".into(),
        credentials: vec![CredentialRecord {
            credential_id: format!("credential-{account}"),
            default_record: true,
            needs_reauth: false,
        }],
        model_reach: ModelReach::All,
        quota,
    }
}

/// Finished, started, expired and released work on each account, spread from
/// two hours before `NOW` up to `NOW`.
fn history(accounts: &[&str]) -> Vec<striatum_store::AccountDemandRow> {
    let states = ["finished", "started", "expired", "released", "reserved"];
    let mut rows = Vec::new();
    for (index, account) in accounts.iter().enumerate() {
        for step in 0..240_i64 {
            rows.push(striatum_store::AccountDemandRow {
                account_id: (*account).into(),
                state: states[(step as usize + index) % states.len()].into(),
                committed_at: NOW - 2 * 3_600_000 + step * 30_000 + index as i64 * 7_000,
                request_bytes: 1_000 + (step as u64 % 7) * 313 + index as u64 * 101,
            });
        }
    }
    rows
}

fn fence(account: &str) -> Fence {
    Fence {
        account_id: account.into(),
        pool: None,
        recovery_set: vec![],
        observed_at_ms: NOW - 60_000,
        reset: ResetTime::Missing,
        retry_hint_ms: None,
        source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
    }
}

struct Case {
    name: &'static str,
    roster: Vec<AccountView>,
    fences: Vec<Fence>,
    credits: Vec<(&'static str, i64)>,
    strategy: AccountStrategy,
}

/// The allocator's answer and every account's score, from the whole history
/// and from the history `demand_with` reads, must be identical; and the
/// bounded read must actually leave rows out where a floor applies.
#[test]
fn bounded_demand_scores_exactly_as_the_whole_history() {
    let quota_at = NOW - 10 * 60_000;
    let cases = [
        Case {
            name: "credit reading newer than the quota reading, accounts on free quota",
            roster: vec![
                view("a", Some(reading(quota_at, 40.0))),
                view("b", Some(reading(quota_at, 30.0))),
            ],
            fences: vec![],
            credits: vec![("a", NOW - 60_000), ("b", NOW - 30_000)],
            strategy: AccountStrategy::Sticky,
        },
        Case {
            name: "credit reading newer than the quota reading, accounts on credits",
            roster: vec![
                view("a", Some(reading(quota_at, 100.0))),
                view("b", Some(reading(quota_at, 100.0))),
            ],
            fences: vec![fence("a"), fence("b")],
            credits: vec![("a", NOW - 60_000), ("b", NOW - 30_000)],
            strategy: AccountStrategy::Sticky,
        },
        Case {
            name: "credit reading older than the quota reading, accounts on free quota",
            roster: vec![
                view("a", Some(reading(NOW - 60_000, 40.0))),
                view("b", Some(reading(NOW - 60_000, 30.0))),
            ],
            fences: vec![],
            credits: vec![("a", NOW - 14 * 60_000), ("b", NOW - 10 * 60_000)],
            strategy: AccountStrategy::Sticky,
        },
        Case {
            name: "credit reading older than the quota reading, accounts on credits",
            roster: vec![
                view("a", Some(reading(NOW - 60_000, 100.0))),
                view("b", Some(reading(NOW - 60_000, 100.0))),
            ],
            fences: vec![fence("a"), fence("b")],
            credits: vec![("a", NOW - 14 * 60_000), ("b", NOW - 10 * 60_000)],
            strategy: AccountStrategy::Sticky,
        },
        Case {
            name: "credit reading and no quota reading",
            roster: vec![view("a", None), view("b", None)],
            fences: vec![fence("a"), fence("b")],
            credits: vec![("a", NOW - 60_000), ("b", NOW - 30_000)],
            strategy: AccountStrategy::Sticky,
        },
        Case {
            name: "round robin",
            roster: vec![
                view("a", Some(reading(quota_at, 40.0))),
                view("b", Some(reading(quota_at, 30.0))),
            ],
            fences: vec![],
            credits: vec![("a", NOW - 60_000)],
            strategy: AccountStrategy::RoundRobin,
        },
        Case {
            name: "hybrid",
            roster: vec![
                view("a", Some(reading(quota_at, 40.0))),
                view("b", Some(reading(quota_at, 30.0))),
            ],
            fences: vec![],
            credits: vec![("a", NOW - 60_000)],
            strategy: AccountStrategy::Hybrid,
        },
    ];
    for case in cases {
        let policy = AllocatorPolicy {
            strategy: case.strategy,
            paid_credits: true,
            ..AllocatorPolicy::default()
        };
        let mut context = AllocationContext::default();
        for (account, observed_at_ms) in &case.credits {
            context.credits.insert(
                (*account).into(),
                CreditReading {
                    observed_at_ms: *observed_at_ms,
                    spendable: true,
                    prepaid: true,
                },
            );
        }
        let roster = Roster {
            view_digest: "vault".into(),
            fetched_at_ms: NOW,
            accounts: case.roster,
        };
        let rows = history(&["a", "b"]);
        let load = |bounded: bool| {
            demand_with(
                |_, account, floor| {
                    Ok(rows
                        .iter()
                        .filter(|row| {
                            row.account_id == account
                                && (!bounded || floor.is_none_or(|floor| row.committed_at > floor))
                        })
                        .cloned()
                        .collect())
                },
                "anthropic",
                Some(&roster),
                Some(&roster),
                &policy,
                &context,
            )
            .unwrap()
        };
        let whole = load(false);
        let bounded = load(true);
        let decide = |admissions| {
            let decision = striatum_core::accounts::decide_with_context(
                &synthetic_request("anthropic", "claude-opus-5-5", "demand-case".into()),
                &AllocatorState {
                    roster: Some(roster.clone()),
                    affinity: None,
                    fences: case.fences.clone(),
                    principal_exclusions: Vec::new(),
                    admissions,
                    operator_override: None,
                    policy: policy.clone(),
                },
                NOW,
                &context,
            );
            format!("{:?} {:?}", decision.outcome, decision.explanation)
        };
        let from_whole = decide(whole.clone());
        assert!(
            from_whole.contains("score: Some"),
            "{}: the case must score accounts: {from_whole}",
            case.name
        );
        assert_eq!(from_whole, decide(bounded.clone()), "{}", case.name);
        assert_eq!(
            from_whole.contains("PaidCredits"),
            !case.fences.is_empty(),
            "{}: the credit cases must place on credits: {from_whole}",
            case.name
        );
        eprintln!(
            "{}: {} of {} demand rows read",
            case.name,
            bounded.len(),
            whole.len()
        );
        let counting = matches!(
            case.strategy,
            AccountStrategy::RoundRobin | AccountStrategy::Hybrid
        );
        let without_quota = roster.accounts.iter().all(|view| view.quota.is_none());
        if counting || without_quota {
            assert_eq!(bounded.len(), whole.len(), "{}", case.name);
        } else {
            assert!(
                bounded.len() < whole.len() / 2,
                "{}: the read must stay bounded: {} of {}",
                case.name,
                bounded.len(),
                whole.len()
            );
        }
    }
}
