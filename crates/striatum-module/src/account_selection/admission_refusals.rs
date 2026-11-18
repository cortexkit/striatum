//! Service-tier validation and admission refusals.
//! The explanations and retry hints use the same roster evidence for preflight and admission.

use super::*;

pub(super) fn tier_candidate_matches(
    parsed: &AccountForParams,
    account: &AccountView,
    credential: &CredentialRecord,
) -> bool {
    parsed.candidates.as_ref().is_none_or(|candidates| {
        candidates.accounts.iter().any(|candidate| {
            candidate.account_id == account.account_id
                && candidate.credential_id == credential.credential_id
        })
    }) && parsed.requested_account.as_ref().is_none_or(|pin| {
        pin.account_id.as_ref().map_or_else(
            || {
                pin.credential_id
                    .as_ref()
                    .is_none_or(|id| *id == credential.credential_id)
            },
            |id| *id == account.account_id,
        )
    })
}

pub(super) fn service_tier_refusal_detail(
    params: &AccountForParams,
    reason: &str,
    retry_after_ms: Option<u64>,
) -> Value {
    let exclusions: Vec<_> = params
        .candidates
        .iter()
        .flat_map(|candidates| &candidates.accounts)
        .map(|candidate| json!({"account_id": candidate.account_id, "reason": reason}))
        .collect();
    let mut detail = json!({
        "code": "account_selection_unavailable",
        "retry_after_s": retry_after_ms.map(|ms| ms.div_ceil(1_000)),
        "decision_id": params.model_decision_id,
        "exclusions": exclusions,
    });
    if let Some(wait) = retry_after_ms {
        detail["retry_after_ms"] = json!(wait);
    }
    detail
}

pub(super) fn unservable_refusal(
    reason: cortexkit_role_routing::errors::AccountUnservableReason,
    offered: Vec<String>,
    eligible: Vec<String>,
) -> OpReply {
    let refusal =
        cortexkit_role_routing::errors::Refusal::account_unservable(reason, offered, eligible);
    OpReply::typed(
        refusal.code.as_str(),
        refusal.message,
        json!(refusal.detail),
    )
}

pub(super) fn disjoint_candidates_refusal(
    params: &AccountForParams,
    ctx: &Ctx<'_>,
    roster: &RosterBuild,
    class: striatum_core::accounts::AllowedAccountClass,
) -> Option<OpReply> {
    let candidates = params.candidates.as_ref()?;
    if roster
        .roster
        .iter()
        .flat_map(|r| &r.accounts)
        .any(|account| {
            account.credentials.iter().any(|credential| {
                candidates.accounts.iter().any(|candidate| {
                    candidate.account_id == account.account_id
                        && candidate.credential_id == credential.credential_id
                })
            })
        })
    {
        return None;
    }
    let classes = ctx.classes.as_ref()?;
    let eligible_rows: Vec<_> = classes
        .facts
        .iter()
        .filter(|fact| {
            fact.provider_id == normalize(&params.provider)
                && fact.model_id == normalize(&params.model)
        })
        .flat_map(|fact| {
            classes.eligible_rows(
                classes
                    .class(
                        fact,
                        class == striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
                    )
                    .rows,
                fact,
            )
        })
        .collect();
    let eligible: BTreeSet<_> = eligible_rows.iter().map(|row| row.id.clone()).collect();
    // Temporary exclusions are not a disagreement with the runner's list.
    if eligible.is_empty()
        || candidates
            .accounts
            .iter()
            .any(|candidate| eligible.contains(&candidate.credential_id))
    {
        return None;
    }
    let accounts: BTreeSet<_> = eligible_rows
        .iter()
        .map(|row| classes.account_id(row))
        .collect();
    crate::account_refusal_log::allocator_refusal(&json!({"exclusions":
        accounts.into_iter().map(|account| json!({
            "account_id":account, "reason":"not_in_runner_candidates",
        })).collect::<Vec<_>>()
    }));
    Some(because(
        "candidates_filter",
        unservable_refusal(
            cortexkit_role_routing::errors::AccountUnservableReason::CandidatesDisjoint,
            candidates
                .accounts
                .iter()
                .map(|candidate| candidate.credential_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            eligible.into_iter().collect(),
        ),
    ))
}

pub(super) fn bound_service_tier_refusal(_params: &AccountForParams) -> OpReply {
    checking("bound_service_tier");
    let mut refusal = cortexkit_role_routing::errors::Refusal::account_unservable(
        cortexkit_role_routing::errors::AccountUnservableReason::BoundTierUnavailable,
        vec![],
        vec![],
    );
    refusal.message = "No account can serve this routing decision's service tier any more, and a bound decision never steps down to a lower tier. Ask routing for a new decision.".into();
    OpReply::typed(
        refusal.code.as_str(),
        refusal.message,
        json!(refusal.detail),
    )
}

pub(super) fn prepare_service_tier(
    parsed: &mut AccountForParams,
    ctx: &Ctx<'_>,
    roster: &mut RosterBuild,
    class: striatum_core::accounts::AllowedAccountClass,
    held_until: Option<i64>,
) -> Result<Option<cortexkit_role_routing::service_tier::ServiceTier>, OpReply> {
    use cortexkit_role_routing::service_tier::{fallback, ServiceTier};
    let bound = parsed
        .model_decision_id
        .as_deref()
        .map(|id| ctx.store.decision_service_tier(id))
        .transpose()
        .map_err(store_error)?
        .flatten();
    // prepare_roster already revalidated the decision's tier and allowed
    // credentials. Do not step down or re-rank that stored credential set.
    if let Some((tier, _)) = bound {
        let tier = ServiceTier::parse(&tier).ok_or_else(|| {
            unservable_refusal(
                cortexkit_role_routing::errors::AccountUnservableReason::StoredStateInvalid,
                vec![],
                vec![],
            )
        })?;
        parsed.service_tier_bound = !tier.is_standard();
        if parsed.service_tier_bound
            && !roster.roster.as_ref().is_some_and(|roster| {
                roster.accounts.iter().any(|account| {
                    account
                        .credentials
                        .iter()
                        .any(|credential| tier_candidate_matches(parsed, account, credential))
                })
            })
        {
            // No account left can serve the tier, but some are only held
            // out for a while: wait for them, as a lost probe does.
            if let Some(until) = held_until {
                return Err(lost_probe_refusal(
                    until,
                    ctx.now_ms,
                    Value::Null,
                    json!([]),
                ));
            }
            return Err(bound_service_tier_refusal(parsed));
        }
        return Ok((!tier.is_standard() || parsed.service_tier.is_some()).then_some(tier));
    }
    let Some(requested) = parsed.service_tier else {
        return Ok(None);
    };
    for tier in fallback(requested).filter(|tier| !tier.is_standard()) {
        let Some(mut classes) = ctx.classes.clone() else {
            continue;
        };
        let mut facts: Vec<_> = classes
            .facts
            .iter()
            .filter(|fact| {
                fact.provider_id == normalize(&parsed.provider)
                    && fact.model_id == normalize(&parsed.model)
            })
            .cloned()
            .collect();
        let registry = crate::routing_v1::runners::Registry::build(
            classes.config.as_ref(),
            &classes.credentials,
            &classes.facts,
            classes.now_ms.max(0) as u64,
        );
        let runners: Vec<_> = registry
            .policies()
            .into_iter()
            .filter(|runner| runner.module == parsed.caller.runner)
            .collect();
        classes.apply_service_tier(
            &mut facts,
            tier.as_str(),
            &runners,
            &registry,
            class == striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
            None,
        );
        let allowed =
            classes.service_tier_accounts(&normalize(&parsed.provider), &normalize(&parsed.model));
        let eligible = |account: &AccountView, credential: &CredentialRecord| {
            allowed.contains(&credential.credential_id)
                && tier_candidate_matches(parsed, account, credential)
        };
        if roster.roster.as_ref().is_some_and(|roster| {
            roster.accounts.iter().any(|account| {
                account
                    .credentials
                    .iter()
                    .any(|credential| eligible(account, credential))
            })
        }) {
            if let Some(roster) = &mut roster.roster {
                for account in &mut roster.accounts {
                    account
                        .credentials
                        .retain(|credential| allowed.contains(&credential.credential_id));
                }
                roster
                    .accounts
                    .retain(|account| !account.credentials.is_empty());
            }
            return Ok(Some(tier));
        }
    }
    Ok(Some(ServiceTier::Standard))
}

/// The refusal for a request every account it could use is route-fenced on:
/// `account_pool_exhausted`, timed by the earliest next check among them.
/// Only one request may claim that check; the retry time is not an expiry.
pub(super) fn route_fence_refusal(
    roster: &RosterBuild,
    fenced: &BTreeMap<String, i64>,
    params: &AccountForParams,
    now: i64,
) -> Option<OpReply> {
    let wanted = |account: &str| {
        params.candidates.as_ref().is_none_or(|candidates| {
            candidates
                .accounts
                .iter()
                .any(|candidate| candidate.account_id == account)
        })
    };
    if roster
        .roster
        .iter()
        .flat_map(|roster| &roster.accounts)
        .any(|view| {
            params.candidates.as_ref().is_none_or(|candidates| {
                candidates.accounts.iter().any(|candidate| {
                    candidate.account_id == view.account_id
                        && view
                            .credentials
                            .iter()
                            .any(|credential| credential.credential_id == candidate.credential_id)
                })
            })
        })
    {
        return None;
    }
    let until = fenced
        .iter()
        .filter(|(account, _)| wanted(account))
        .map(|(_, until)| *until)
        .min()?;
    let retry_after_s = u64::try_from(until.saturating_sub(now))
        .unwrap_or(0)
        .div_ceil(1000);
    let mut detail = json!({
        "code": "account_pool_exhausted",
        "retry_after_s": retry_after_s,
        "decision_id": null,
        "exclusions": [],
        "reset_at_ms": until,
    });
    let unoffered: Vec<_> = roster
        .roster
        .iter()
        .flat_map(|roster| &roster.accounts)
        .filter(|view| !wanted(&view.account_id))
        .collect();
    if !unoffered.is_empty() {
        // The retry belongs to the offered accounts, not the whole route.
        // Credential ids (vault row names, not secrets) identify which eligible
        // accounts the runner left out of its candidates.
        let credentials: BTreeSet<_> = unoffered
            .iter()
            .flat_map(|view| &view.credentials)
            .map(|credential| &credential.credential_id)
            .collect();
        detail["eligible_accounts_not_offered"] = json!({
            "count": unoffered.len(), "credential_ids": credentials,
        });
    }
    Some(OpReply::typed(
        "account_pool_exhausted",
        format!(
            "a failed report closed {}/{} on every account this request may use until {}",
            params.provider,
            params.model,
            rfc3339(until)
        ),
        detail,
    ))
}

/// The answer to a request that lost a probe race, or whose accounts are
/// held out for a while, and was admitted nowhere: the timed
/// `account_pool_exhausted`, at `until` (the earliest time one of them may
/// return). Never an untimed code, which a runner would read as a missing
/// login or a decision to replace.
pub(super) fn lost_probe_refusal(
    until: i64,
    now: i64,
    decision_id: Value,
    exclusions: Value,
) -> OpReply {
    let retry_after_s = u64::try_from(until.saturating_sub(now))
        .unwrap_or(0)
        .div_ceil(1000);
    OpReply::typed(
        "account_pool_exhausted",
        format!(
            "no account admitted: the accounts this request needs are held until {}",
            rfc3339(until)
        ),
        json!({
            "code": "account_pool_exhausted",
            "retry_after_s": retry_after_s,
            "decision_id": decision_id,
            "exclusions": exclusions,
            "reset_at_ms": until,
        }),
    )
}

/// The contract code for an empty eligible roster with class-aware routing.
/// A route fence that closed every usable account is already answered as
/// `account_pool_exhausted` by `route_fence_refusal`. What remains is split
/// by cause, because Broca classes `account_not_authorized` as an auth
/// failure and pauses the worker: an account limit must never use it.
/// - An unknown provider is a caller error (`invalid_request`), not a login.
/// - No serving credential for the payment class at all needs a login
///   (`auth_required`).
/// - Every remaining credential excluded by a fence (a billing or
///   entitlement credential fence, a route fence, or quota exhaustion) is an
///   account limit (`account_pool_exhausted`), with `reset_at_ms` when any
///   fence or window names a known reset and without it otherwise.
/// - Every remaining credential excluded by operator policy for this request
///   (model or account eligibility, or the size band) is one the caller may
///   not use (`account_not_authorized`).
/// When causes mix, the most recoverable wins: `account_pool_exhausted`
/// beats `account_not_authorized`.
/// Return `None` when serving credentials exist but the calling model-execution
/// module offers none of them in its candidate list. The allocator preserves
/// that disagreement as permanent `account_unservable`, rather than requiring sign-in.
pub(super) fn empty_roster_refusal(
    classes: &ClassContext,
    provider: &str,
    model: &str,
    priced: bool,
    fenced: &BTreeMap<String, i64>,
    now: i64,
    params: Option<&AccountForParams>,
) -> Option<OpReply> {
    let untimed = |code: &str, message: String, exclusions: &[Value]| {
        Some(OpReply::typed(
            code,
            message,
            json!({
                "code": code,
                "retry_after_s": null,
                "decision_id": null,
                "exclusions": exclusions,
            }),
        ))
    };
    let exhausted = |until: Option<i64>, exclusions: &[Value]| {
        let (retry_after_s, reset_at_ms) = match until.filter(|until| *until > now) {
            Some(until) => (
                Some(
                    u64::try_from(until.saturating_sub(now))
                        .unwrap_or(0)
                        .div_ceil(1000),
                ),
                Some(until),
            ),
            None => (None, None),
        };
        Some(OpReply::typed(
            "account_pool_exhausted",
            "Routing found no usable account among the offered credentials. Each one is listed in exclusions with its reason and, where it has one, when it can be tried again.",
            json!({
                "code": "account_pool_exhausted",
                "retry_after_s": retry_after_s,
                "decision_id": null,
                "exclusions": exclusions,
                "reset_at_ms": reset_at_ms,
            }),
        ))
    };
    let fact = classes.fact_for(provider, model);
    let route_class = fact.map(|fact| classes.class(fact, priced));
    let paid_rows = if params.is_some_and(|params| params.model_decision_id.is_none())
        && route_class.as_ref().is_some_and(|class| class.subscription)
    {
        // Paid pools borrow the catalog model but keep their own provider id.
        // Their keys need the same spending decision as direct provider keys.
        classes
            .facts
            .iter()
            .filter(|fact| {
                fact.model_id == model
                    && (fact.provider_id == provider
                        || classes
                            .targets
                            .get(&fact.provider_id)
                            .is_some_and(|target| target == provider))
            })
            .flat_map(|fact| classes.class(fact, true).rows)
            .collect()
    } else {
        Vec::new()
    };
    let class_rows = route_class.map_or_else(Vec::new, |class| class.rows);
    let mut exclusions = Vec::new();
    if let Some(candidates) = params.and_then(|params| params.candidates.as_ref()) {
        for candidate in &candidates.accounts {
            let row = classes
                .credentials
                .listed_credentials()
                .iter()
                .find(|row| row.id == candidate.credential_id);
            let reason = match row {
                None => Some("unknown_credential"),
                Some(row)
                    if !paid_rows.iter().any(|paid| paid.id == row.id)
                        && !crate::broca_credentials::credential_providers(
                            row,
                            &classes.catalog,
                        )
                        .iter()
                        .any(|mapped| mapped == provider) =>
                {
                    Some("wrong_provider")
                }
                Some(row) if classes.account_id(row) != candidate.account_id => {
                    Some("account_id_mismatch")
                }
                Some(row) if !class_rows.iter().any(|allowed| allowed.id == row.id) => {
                    Some(if paid_rows.iter().any(|paid| paid.id == row.id) {
                        "routing_decision_required"
                    } else {
                        "not_in_account_class"
                    })
                }
                Some(_) => None,
            };
            if let Some(reason) = reason {
                exclusions.push(json!({"account_id":candidate.account_id,
                    "credential_id":candidate.credential_id, "reason":reason}));
            }
        }
        // Waiting or signing in cannot authorize spending. When the runner
        // offers only paid keys outside the subscription class, it must ask
        // for a selection decision before retrying the admission.
        if !candidates.accounts.is_empty()
            && exclusions.len() == candidates.accounts.len()
            && exclusions
                .iter()
                .all(|exclusion| exclusion["reason"] == "routing_decision_required")
        {
            return Some(because("routing_decision_required", OpReply::typed(
                "invalid_request",
                "A paid key on a model a subscription serves needs a routing decision; select with a pin first, then send its decision_id.",
                json!({"field":"decision_id", "exclusions":exclusions}),
            )));
        }
    }
    if fact.is_none() {
        // Missing model credentials only imply sign-in when the provider is
        // known. Credential-provider mappings also name routing-only aliases
        // and gateways without their own catalog model rows.
        let known_provider = classes.catalog.contains(provider)
            || classes.targets.contains_key(provider)
            || classes
                .facts
                .iter()
                .any(|fact| fact.provider_id == provider)
            || classes.credentials.listed_credentials().iter().any(|row| {
                row.categories
                    .iter()
                    .any(|category| matches!(category.as_str(), "llm-provider" | "llm-gateway"))
                    && crate::broca_credentials::credential_providers(row, &classes.catalog)
                        .iter()
                        .any(|mapped| mapped == provider)
            });
        if !known_provider {
            checking("unknown_provider");
            let providers: BTreeSet<_> = classes
                .facts
                .iter()
                .filter(|fact| fact.model_id == model)
                .map(|fact| fact.provider_id.as_str())
                .collect();
            let mut message = format!("Routing does not know provider `{provider}`.");
            if providers.len() == 1 {
                let suggested = providers.first().unwrap();
                message.push_str(&format!(
                    " Model `{model}` is served by provider `{suggested}`; use that provider id."
                ));
            }
            return Some(OpReply::typed(
                "invalid_request",
                message,
                json!({"field": "provider"}),
            ));
        }
        checking("missing_model_credentials");
        return untimed(
            "auth_required",
            format!("routing selects no accounts for provider `{provider}`"),
            &exclusions,
        );
    }
    let rows = class_rows;
    if rows.is_empty() {
        checking("missing_class_credentials");
        return untimed(
            "auth_required",
            format!("routing selects no accounts for provider `{provider}`"),
            &exclusions,
        );
    }
    let rows: Vec<_> = rows
        .into_iter()
        .filter(|row| {
            params
                .and_then(|params| params.candidates.as_ref())
                .is_none_or(|candidates| {
                    candidates.accounts.iter().any(|candidate| {
                        candidate.credential_id == row.id
                            && candidate.account_id == classes.account_id(row)
                    })
                })
        })
        .collect();
    if rows.is_empty() {
        return None;
    }
    let tokens = params.map_or(0, |params| {
        params
            .request_input_tokens
            .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4))
    });
    let size_class = striatum_core::accounts::input_size_class(tokens);
    let mut fenced_cause = false;
    let mut row_resets: Vec<Option<i64>> = Vec::new();
    for row in &rows {
        if let Some(until) = classes.reserved_probe_retry_at(provider, model, row) {
            fenced_cause = true;
            row_resets.push(Some(until));
            let code = classes
                .refusals
                .iter()
                .find(|fence| fence.provider_id == provider && fence.credential_id == row.id)
                .map(|fence| fence.code.as_str())
                .unwrap_or("probe_reserved");
            exclusions.push(json!({"account_id":classes.account_id(row),
                "credential_id":row.id, "reason":code, "reset_at_ms":until,
                "message":"Another selection holds this credential's one probe attempt for now. Retry after that hold expires, at most two minutes."}));
            continue;
        }
        if classes
            .pool_walls
            .contains(&(provider.to_owned(), row.id.clone()))
        {
            fenced_cause = true;
            row_resets.push(None);
            exclusions.push(json!({"account_id":classes.account_id(row),
                "credential_id":row.id, "reason":"pool_backoff", "reset_at_ms":null}));
            continue;
        }
        let account = classes.account_id(row).to_owned();
        let route_reset = fenced.get(&account).copied();
        if route_reset.is_some() {
            fenced_cause = true;
            // A reset adds a check, never postpones the next quota recheck.
            // Independent non-quota origins still combine by their deadlines.
            let row_reset = route_reset;
            row_resets.push(row_reset);
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":"route_fenced", "reset_at_ms":row_reset}));
            continue;
        }
        if let Some(refusal) = classes.refusals.iter().find(|fence| {
            fence.provider_id == provider
                && fence.credential_id == row.id
                && classes.rechecks.iter().any(|origin| {
                    origin.kind == "credential"
                        && origin.provider == provider
                        && origin.credential_id.as_deref() == Some(&row.id)
                        && !classes.origin_due(origin, &account, model)
                })
        }) {
            fenced_cause = true;
            // Only a billing refusal can inherit a quota reset. Entitlement
            // recovery depends on a successful request, not utilization.
            let quota_reset = crate::credential_fences::is_billing_code(&refusal.code)
                .then(|| classes.quota_hold_reset(provider, &row.id))
                .flatten();
            let probe_reset = classes
                .rechecks
                .iter()
                .filter(|origin| {
                    origin.kind == "credential"
                        && origin.provider == provider
                        && origin.credential_id.as_deref() == Some(&row.id)
                })
                .map(|origin| classes.recheck_hint(origin, &account, model))
                .filter(|reset| *reset > now)
                .min();
            let reset = probe_reset.or(quota_reset).filter(|reset| *reset > now);
            row_resets.push(reset);
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":refusal.code, "reset_at_ms":reset,
                "message":if reset.is_some() {
                    "This credential is blocked until its quota resets or its next single probe window opens; reset_at_ms says when."
                } else {
                    "This credential is blocked with no timed reset: an operator must clear it, or its vault listing must change."
                }}));
            continue;
        }
        if classes.eligibility.iter().any(|fact_| {
            fact_.provider == provider
                && fact_.credential_id == row.id
                && (fact_.model.is_empty() || fact_.model == model)
        }) {
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":"account_not_authorized"}));
            continue;
        }
        if classes.size_refusals.iter().any(|fact_| {
            fact_.provider == provider
                && fact_.model == model
                && fact_.credential_id == row.id
                && fact_.size_class == size_class
        }) {
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":"input_size"}));
            continue;
        }
        // A row none of the filters excludes would have left the roster
        // non-empty; treat an unclassified row as policy-filtered so a new
        // filter cannot silently become a timed pool refusal.
        exclusions.push(json!({"account_id":account, "credential_id":row.id,
            "reason":"account_policy_filter"}));
    }
    if fenced_cause {
        checking("credential_or_quota_block");
        // Retry when the first known hold ends; accounts with unknown
        // resets remain excluded while another account can recover.
        let earliest = row_resets.into_iter().flatten().min();
        return exhausted(earliest, &exclusions);
    }
    checking("account_policy_filter");
    untimed(
        "account_not_authorized",
        format!("routing selects no accounts for provider `{provider}`"),
        &exclusions,
    )
}

/// A pool recovers when its first eligible account clears every dated blocker.
/// Read only the windows that actually excluded each account: an unrelated
/// short-window reset must not shorten a weekly reserve hold.
pub(super) fn pool_recovery_reset(
    decision: &Decision,
    roster: Option<&Roster>,
    fences: &[Fence],
    policy: &AllocatorPolicy,
    model: &str,
    now: i64,
) -> Option<i64> {
    use striatum_core::accounts::{ApplicableWindow, ExclusionReason};
    let roster = roster?;
    let family = policy.family_for_model(model);
    let mut releases = Vec::new();
    for note in &decision.explanation.accounts {
        let release = (|| {
            let account = roster
                .accounts
                .iter()
                .find(|a| a.account_id == note.account_id)?;
            let mut resets = Vec::new();
            let window_reset = |window: ApplicableWindow| -> Option<i64> {
                let quota = account.quota.as_ref()?;
                let general = match window {
                    ApplicableWindow::FiveHour => GeneralWindow::FiveHour,
                    ApplicableWindow::SevenDay => GeneralWindow::SevenDay,
                    ApplicableWindow::Reported(minutes) => GeneralWindow::Reported(minutes),
                    ApplicableWindow::FamilyScope => {
                        let ScopedList::Listed(windows) = &quota.scoped else {
                            return None;
                        };
                        return windows
                            .iter()
                            .filter(|window| policy.family_for_scope(window) == Some(family))
                            .map(|window| window.resets_at.at_ms())
                            .collect::<Option<Vec<_>>>()?
                            .into_iter()
                            .max();
                    }
                };
                quota.window(general)?.resets_at.at_ms()
            };
            for reason in &note.exclusions {
                match reason {
                    ExclusionReason::BelowReserve { windows } => {
                        for window in windows {
                            resets.push(window_reset(*window)?);
                        }
                    }
                    ExclusionReason::BelowBrocaFloor { window, .. } => {
                        resets.push(window_reset((*window).into())?);
                    }
                    ExclusionReason::ScopeExhausted { .. } => {
                        resets.push(window_reset(ApplicableWindow::FamilyScope)?);
                    }
                    ExclusionReason::ConfirmedFence { pool } => {
                        let matching: Vec<_> = fences
                            .iter()
                            .filter(|fence| {
                                fence.account_id == account.account_id
                                    && fence.pool == *pool
                                    && fence.applies_to(
                                        model,
                                        family,
                                        request_pool(None, &account.routing_provider, model)
                                            .as_deref(),
                                    )
                            })
                            .collect();
                        if matching.is_empty() {
                            return None;
                        }
                        for fence in matching {
                            resets.push(fence.reset.at_ms()?);
                        }
                    }
                    // Missing evidence and permanent eligibility failures do not
                    // promise a recovery time.
                    _ => return None,
                }
            }
            let release = resets.into_iter().max()?;
            if release <= now {
                return None;
            }
            Some(release)
        })();
        if let Some(release) = release {
            releases.push(release);
        }
    }
    releases.into_iter().min()
}
