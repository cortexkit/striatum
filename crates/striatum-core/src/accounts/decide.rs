//! `decide`: the pure account allocator, implementing the design's "New-session
//! rule: sticky-balanced first" steps 1-7 and its precedence table
//! (`docs/designs/account-selection.md`). It performs no I/O: the caller loads
//! [`AllocatorState`] inside its selection transaction and commits the result.

use super::decision::{
    AccountNote, Admission, AdmitReason, AffinityNote, AffinityOutcome, ApplicableWindow, Decision,
    ExclusionReason, ExclusionTier, Explanation, Hold, HoldBasis, ImportNote, InvalidationReason,
    Outcome, OverrideNote, PreviousPin, Refusal, RefusalCode, RequestedAccountIgnored,
    RequestedAccountNote, RosterNote,
};
use super::eligibility::{age_band, evaluate_quota, fable_freshly_exhausted, QuotaEval};
use super::evidence::{hold_until, retry_after_with_jitter};
use super::policy::{AccountStrategy, AllocationContext};
use super::pools::{block_end, PoolKey, PoolScope};
use super::types::{
    AccountRef, AccountRequest, AccountView, AdmissionState, AllocatorPolicy, AllocatorState,
    ConversationAffinity, CredentialRecord, EpochMs, Fence, ModelFamily, OperatorOverride,
    QuotaReading, ResetTime,
};

fn previous_pin(affinity: &ConversationAffinity) -> PreviousPin {
    PreviousPin {
        pin_id: affinity.pin_id.clone(),
        account_id: affinity.account_id.clone(),
        credential_id: affinity.credential_id.clone(),
        pin_generation: affinity.pin_generation,
    }
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

/// Decides which account serves this admission.
pub fn decide(request: &AccountRequest, state: &AllocatorState, now: EpochMs) -> Decision {
    decide_with_context(request, state, now, &AllocationContext::default())
}

pub fn decide_with_context(
    request: &AccountRequest,
    state: &AllocatorState,
    now: EpochMs,
    context: &AllocationContext,
) -> Decision {
    let policy = &state.policy;
    let provider = normalize(&request.provider);
    let model = normalize(&request.model);
    let affinity_model = normalize(&request.affinity_model);
    let family = policy.family_for_model(&model);
    let (request_bytes, request_bytes_estimated) = if request.initial_input_estimated {
        (policy.estimated_request_bytes.max(1), true)
    } else {
        (request.initial_input_bytes.max(1), false)
    };
    let roster_note = state.roster.as_ref().map(|roster| RosterNote {
        view_digest: roster.view_digest.clone(),
        fetched_at_ms: roster.fetched_at_ms,
        expired: now - roster.fetched_at_ms >= policy.roster_max_age_ms,
        account_ids: roster
            .accounts
            .iter()
            .map(|view| view.account_id.clone())
            .collect(),
    });
    let explanation = Explanation {
        credential_fence_probe: None,
        policy_version: policy.policy_version,
        provider: provider.clone(),
        model: model.clone(),
        affinity_model: affinity_model.clone(),
        family,
        direct_opus: family == ModelFamily::Opus && !request.via_safety_recovery,
        conversation_key_source: request.conversation_key_source,
        request_bytes,
        request_bytes_estimated,
        roster: roster_note,
        runner_view_digest: request.candidates.as_ref().map(|c| c.view_digest.clone()),
        runner_candidates: request.candidates.as_ref().map(|c| c.accounts.clone()),
        affinity: AffinityNote::None,
        previous_pin: state.affinity.as_ref().map(previous_pin),
        import: None,
        operator_override: None,
        requested_account: None,
        accounts: Vec::new(),
    };
    let views: Vec<_> = state
        .roster
        .iter()
        .flat_map(|roster| roster.accounts.iter())
        .filter(|view| view.routing_provider == provider)
        .collect();
    if state.roster.is_some() && views.is_empty() {
        return refuse(explanation, RefusalCode::AccountSelectionUnsupported, None);
    }
    Allocator {
        request,
        state,
        policy,
        context,
        credit_mode: false,
        reserve_credit_mode: false,
        now,
        model,
        affinity_model,
        family,
        request_bytes,
        views,
        explanation,
    }
    .run()
}

fn refuse(explanation: Explanation, code: RefusalCode, retry_after_s: Option<u64>) -> Decision {
    let retryable = matches!(
        code,
        RefusalCode::AccountSelectionUnavailable
            | RefusalCode::AccountQuotaUnknown
            | RefusalCode::AccountPoolReserveLimited
            | RefusalCode::AccountPoolExhausted
    );
    Decision {
        outcome: Outcome::Refuse(Refusal {
            code,
            retryable,
            retry_after_s,
        }),
        explanation,
    }
}

/// Hard-filter result for one account (the precedence table's first row).
struct Hard {
    reasons: Vec<ExclusionReason>,
    /// The record this admission binds, when one survives.
    credential_id: Option<String>,
    reaches: bool,
    runner_listed: bool,
}

/// One account's full cold evaluation.
struct ColdEval<'a> {
    view: &'a AccountView,
    hard: Hard,
    quota: Option<QuotaEval>,
    note: AccountNote,
}

impl ColdEval<'_> {
    fn reasons(&self) -> impl Iterator<Item = &ExclusionReason> {
        self.note.exclusions.iter()
    }

    fn has_tier(&self, tier: ExclusionTier) -> bool {
        self.reasons().any(|reason| reason.tier() == tier)
    }

    fn eligible(&self) -> bool {
        self.hard.reasons.is_empty() && self.quota.as_ref().is_some_and(QuotaEval::eligible)
    }

    fn eligible_below_broca_floor(&self) -> bool {
        self.hard.reasons.is_empty()
            && self
                .quota
                .as_ref()
                .is_some_and(QuotaEval::eligible_below_broca_floor)
    }
}

/// One thing that blocks a conversation's next request on its account.
struct Blocker {
    reset: ResetTime,
    basis: HoldBasis,
    /// Why the conversation leaves when this blocker decides and cannot hold.
    leave: InvalidationReason,
}

enum AffinityVerdict {
    Retain { degraded: Vec<ExclusionReason> },
    Hold(Hold),
    Leave(InvalidationReason),
}

struct Allocator<'a> {
    request: &'a AccountRequest,
    state: &'a AllocatorState,
    policy: &'a AllocatorPolicy,
    context: &'a AllocationContext,
    credit_mode: bool,
    /// Credit mode entered because routing's reserve holds back free quota on
    /// some account, not because every plan is exhausted. Only prepaid credits
    /// count then.
    reserve_credit_mode: bool,
    now: EpochMs,
    model: String,
    affinity_model: String,
    family: ModelFamily,
    request_bytes: u64,
    views: Vec<&'a AccountView>,
    explanation: Explanation,
}

impl<'a> Allocator<'a> {
    // Precedence (design, "New-session rule"): hard filters always win; then
    // the operator override; then existing affinity; then a one-time import;
    // then `requested_account`; then scoring.
    fn run(mut self) -> Decision {
        let all_exhausted =
            !self.views.is_empty() && self.views.iter().all(|view| self.free_exhausted(view));
        // Routing's reserve stops new work a few points short of a plan's
        // limit, and a provider draws prepaid credits only once that limit is
        // reached, so an account the reserve holds back would never reach its
        // credits. When every account is exhausted or held by the reserve,
        // prepaid credits carry new work; metered overage still waits for real
        // exhaustion.
        let all_held = !all_exhausted
            && !self.views.is_empty()
            && self
                .views
                .iter()
                .all(|view| self.free_exhausted(view) || self.held_by_reserve(view));
        self.credit_mode = self.policy.paid_credits && (all_exhausted || all_held);
        self.reserve_credit_mode = self.credit_mode && !all_exhausted;
        if let Some(override_) = &self.state.operator_override {
            return self.operator_override(override_);
        }
        let mut previous = None;
        if let Some(affinity) = &self.state.affinity {
            if self.policy.strategy == AccountStrategy::Ordered
                || self.policy.strategy == AccountStrategy::RoundRobin
            {
                return self.cold(Some(previous_pin(affinity)));
            }
            match self.evaluate_affinity(affinity, true) {
                AffinityVerdict::Retain { degraded } => {
                    return self.sticky_hit(affinity, degraded);
                }
                AffinityVerdict::Hold(hold) => {
                    self.explanation.affinity = AffinityNote::Held {
                        account_id: affinity.account_id.clone(),
                    };
                    if let Some(view) = self.view_for(&affinity.account_id) {
                        self.explanation.accounts = vec![self.evaluate_cold(view).note];
                    }
                    return Decision {
                        outcome: Outcome::Hold(hold),
                        explanation: self.explanation,
                    };
                }
                AffinityVerdict::Leave(reason) => {
                    self.explanation.affinity = AffinityNote::Invalidated {
                        account_id: affinity.account_id.clone(),
                        reason,
                    };
                    previous = Some(previous_pin(affinity));
                }
            }
        } else if let Some(preferred) = &self.request.preferred_existing_account {
            match self.import(preferred) {
                Ok(decision) => return decision,
                Err(this) => return this.cold(None),
            }
        }
        self.cold(previous)
    }

    // ---- lookups -------------------------------------------------------

    fn view_for(&self, account_id: &str) -> Option<&'a AccountView> {
        self.views
            .iter()
            .copied()
            .find(|view| view.account_id == account_id)
    }

    /// Resolves an account reference to its view and, for a credential id,
    /// the one record it names.
    fn resolve(&self, target: &AccountRef) -> Option<(&'a AccountView, Option<String>)> {
        match target {
            AccountRef::AccountId(id) => self.view_for(id).map(|view| (view, None)),
            AccountRef::CredentialId(credential_id) => self
                .views
                .iter()
                .copied()
                .find(|view| {
                    view.credentials
                        .iter()
                        .any(|record| &record.credential_id == credential_id)
                })
                .map(|view| (view, Some(credential_id.clone()))),
        }
    }

    /// The roster is present and younger than `roster_max_age_ms`, the time a
    /// persisted roster keeps serving cold admissions through a vault outage.
    fn roster_usable(&self) -> bool {
        self.state
            .roster
            .as_ref()
            .is_some_and(|roster| self.now - roster.fetched_at_ms < self.policy.roster_max_age_ms)
    }

    fn runner_lists(&self, account_id: &str, credential_id: &str) -> bool {
        self.request
            .candidates
            .as_ref()
            .is_none_or(|candidates| candidates.contains(account_id, credential_id))
    }

    /// A refusal exclusion lasts until the runner reports a different view
    /// digest, routing's roster digest moves, or its cap passes.
    fn principal_excluded(&self, credential_id: &str) -> bool {
        self.state.principal_exclusions.iter().any(|exclusion| {
            exclusion.principal == self.request.principal
                && exclusion.credential_id == credential_id
                && self.now - exclusion.recorded_at_ms
                    < self
                        .policy
                        .principal_exclusion_max_ms
                        .min(crate::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS)
                && self
                    .state
                    .roster
                    .as_ref()
                    .is_none_or(|roster| roster.view_digest == exclusion.roster_view_digest)
                && self
                    .request
                    .candidates
                    .as_ref()
                    .is_none_or(|c| c.view_digest == exclusion.runner_view_digest)
        })
    }

    /// Blocks on this account that restrict the requested model, newest
    /// first. Only newer fresh headroom retires evidence. For a due probe, the
    /// module omits its own fence from allocation and atomically advances its
    /// recheck clock at admission, leaving the stored evidence in place.
    fn fences_for(&self, account_id: &str) -> Vec<&'a Fence> {
        if self.using_credits(account_id) {
            return Vec::new();
        }
        let reading = self
            .views
            .iter()
            .find(|view| view.account_id == account_id)
            .and_then(|view| view.quota.as_ref());
        let mut fences: Vec<&Fence> = self
            .state
            .fences
            .iter()
            .filter(|fence| {
                fence.account_id == account_id
                    && fence.applies_to(&self.model, self.family, self.request.pool.as_deref())
            })
            .filter(|fence| block_end(fence, reading, self.now, self.policy).is_none())
            .collect();
        fences.sort_by_key(|fence| std::cmp::Reverse(fence.observed_at_ms));
        fences
    }

    // ---- hard filters (step 1 and step 3's non-quota exclusions) -------

    /// Deterministic record binding: the default record first, then the
    /// smallest credential id in byte order.
    fn bind_record(records: &[&CredentialRecord]) -> Option<String> {
        records
            .iter()
            .min_by(|a, b| {
                (!a.default_record)
                    .cmp(&!b.default_record)
                    .then_with(|| a.credential_id.cmp(&b.credential_id))
            })
            .map(|record| record.credential_id.clone())
    }

    fn hard_filters(&self, view: &AccountView, only_credential: Option<&str>) -> Hard {
        let mut reasons = Vec::new();
        let reaches = view.model_reach.reaches(&self.model);
        if !reaches {
            reasons.push(ExclusionReason::UnsupportedModel);
        }
        let listed: Vec<&CredentialRecord> = view
            .credentials
            .iter()
            .filter(|record| only_credential.is_none_or(|c| record.credential_id == c))
            .filter(|record| self.runner_lists(&view.account_id, &record.credential_id))
            .collect();
        let runner_listed = !listed.is_empty();
        let mut credential_id = None;
        if !runner_listed {
            reasons.push(ExclusionReason::NotInRunnerCandidates);
        } else {
            let (refused, allowed): (Vec<&CredentialRecord>, Vec<&CredentialRecord>) = listed
                .into_iter()
                .partition(|record| self.principal_excluded(&record.credential_id));
            if allowed.is_empty() {
                reasons.push(ExclusionReason::PrincipalExcluded {
                    credential_ids: refused
                        .iter()
                        .map(|record| record.credential_id.clone())
                        .collect(),
                });
            } else {
                let mut usable: Vec<&CredentialRecord> = allowed
                    .into_iter()
                    .filter(|record| !record.needs_reauth)
                    .collect();
                if usable.is_empty() {
                    reasons.push(ExclusionReason::ReauthRequired);
                } else {
                    if usable.iter().any(|record| {
                        !self
                            .context
                            .credential_probe_notes
                            .contains_key(&record.credential_id)
                    }) {
                        usable.retain(|record| {
                            !self
                                .context
                                .credential_probe_notes
                                .contains_key(&record.credential_id)
                        });
                    }
                    credential_id = Self::bind_record(&usable);
                }
            }
        }
        let mut pools: Vec<&Option<PoolKey>> = Vec::new();
        if self.budget_blocked(&view.account_id) {
            reasons.push(ExclusionReason::SpendBudget);
        }
        for fence in self.fences_for(&view.account_id) {
            if !pools.contains(&&fence.pool) {
                pools.push(&fence.pool);
                reasons.push(ExclusionReason::ConfirmedFence {
                    pool: fence.pool.clone(),
                });
            }
        }
        Hard {
            reasons,
            credential_id,
            reaches,
            runner_listed,
        }
    }

    fn evaluate_quota(&self, view: &AccountView) -> QuotaEval {
        let mut quota = evaluate_quota(
            if self.using_credits(&view.account_id) {
                None
            } else {
                view.quota.as_ref()
            },
            &self.model,
            self.request.caller,
            self.now,
            self.policy,
        );
        if !self.using_credits(&view.account_id) {
            if let Some(reading) = view
                .quota
                .as_ref()
                .filter(|reading| super::pools::fresh_for_recovery(reading, self.now, self.policy))
            {
                let soft = self.views.len() > 1
                    && self
                        .policy
                        .soft_quota_remaining_percent
                        .is_some_and(|floor| {
                            reading
                                .required_windows()
                                .iter()
                                .filter_map(|id| reading.window(*id))
                                .any(|window| 100.0 - window.used_percent < floor)
                        });
                if soft || self.pool_floor_failed(view) {
                    quota.reserve.push(ExclusionReason::PolicyFloor);
                }
            }
        }
        quota
    }

    fn pool_floor_failed(&self, view: &AccountView) -> bool {
        view.quota
            .as_ref()
            .filter(|reading| super::pools::fresh_for_recovery(reading, self.now, self.policy))
            .is_some_and(|reading| {
                self.policy.pool_floors.iter().any(|floor| {
                    floor.key.scope.applies_to(&self.model, self.family)
                        && (reading
                            .pool_windows
                            .as_ref()
                            .into_iter()
                            .flatten()
                            .any(|window| {
                                floor.key.name == window.pool
                                    && floor
                                        .key
                                        .window_minutes
                                        .is_none_or(|minutes| minutes == window.window_minutes)
                                    && 100.0 - window.used_percent < floor.remaining_percent
                            })
                            || (floor.key.name.is_none()
                                && reading
                                    .required_windows()
                                    .iter()
                                    .filter_map(|id| reading.window(*id))
                                    .any(|window| {
                                        floor.key.window_minutes == Some(window.window_minutes)
                                            && 100.0 - window.used_percent < floor.remaining_percent
                                    })))
                })
            })
    }

    fn using_credits(&self, account: &str) -> bool {
        self.credit_mode
            && self.context.credits.get(account).is_some_and(|credit| {
                credit.spendable
                    && (!self.reserve_credit_mode || credit.prepaid)
                    && (0..15 * 60_000).contains(&(self.now - credit.observed_at_ms))
            })
    }

    /// The account passes every hard filter and has a usable reading, but its
    /// remaining free quota sits inside routing's reserve for this caller: the
    /// kept-back points, the Broca floor, a soft or pool floor, or an exhausted
    /// family scope. Only evaluated before credit mode is decided, so the quota
    /// evaluation here never sets the reading aside for credits.
    fn held_by_reserve(&self, view: &AccountView) -> bool {
        if !self.hard_filters(view, None).reasons.is_empty() {
            return false;
        }
        let quota = self.evaluate_quota(view);
        quota.unknown.is_empty() && !quota.reserve.is_empty()
    }

    fn budget_blocked(&self, account: &str) -> bool {
        self.policy.spend_budget.is_some_and(|limit| {
            self.context.spend.get(account).is_some_and(|spend| {
                spend.used.is_finite()
                    && spend.used >= limit
                    && spend
                        .reset_at_ms
                        .or(self.policy.spend_budget_reset_at_ms)
                        .is_none_or(|reset| reset > self.now)
            })
        })
    }

    fn free_exhausted(&self, view: &AccountView) -> bool {
        let hard = self.hard_filters(view, None);
        if hard
            .reasons
            .iter()
            .any(|reason| !matches!(reason, ExclusionReason::ConfirmedFence { .. }))
        {
            return false;
        }
        if !self.fences_for(&view.account_id).is_empty() {
            return true;
        }
        view.quota.as_ref().filter(|reading| super::pools::fresh_for_recovery(reading, self.now, self.policy)).is_some_and(|reading| {
            reading.required_windows().iter().filter_map(|id| reading.window(*id)).any(|window| window.used_percent >= 100.0 && window.resets_at.at_ms().is_none_or(|reset| reset > self.now))
            || matches!(&reading.scoped, super::types::ScopedList::Listed(windows) if windows.iter().any(|window| super::types::model_matches_scope(&self.model, &window.scope_id) && window.used_percent>=100.0))
        })
    }

    fn evaluate_cold(&self, view: &'a AccountView) -> ColdEval<'a> {
        let hard = self.hard_filters(view, None);
        let mut note = AccountNote::new(&view.account_id);
        note.credential_id = hard.credential_id.clone();
        note.exclusions = hard.reasons.clone();
        // Hard filters always win; quota is evaluated only for accounts that
        // passed them.
        let quota = hard.reasons.is_empty().then(|| {
            let quota = self.evaluate_quota(view);
            quota.write_note(&mut note);
            quota
        });
        ColdEval {
            view,
            hard,
            quota,
            note,
        }
    }

    // ---- affinity (step 2) ---------------------------------------------

    /// A full-band reading routing can trust: no entry-level error, a valid
    /// timestamp under the full-band age, and a covered `stale.class`.
    fn fresh_full(&self, reading: &QuotaReading) -> bool {
        let stale_ok = reading
            .stale
            .as_ref()
            .and_then(|stale| stale.class.as_deref())
            .is_none_or(|class| class == "upstream_failed");
        reading.error_class.is_none()
            && stale_ok
            && matches!(
                age_band(reading.observed_at_ms, self.now, self.policy),
                Ok((super::decision::AgeBand::Full, _, _))
            )
    }

    /// Step 2: retain the affinity unless an invalidation rule applies. A
    /// better score elsewhere or a quota reading, even at 100%, never moves it
    /// on its own. Explicit policy floors and confirmed blocks still apply.
    fn evaluate_affinity(
        &self,
        affinity: &ConversationAffinity,
        existing_affinity: bool,
    ) -> AffinityVerdict {
        use AffinityVerdict::Leave;
        if normalize(&affinity.affinity_model) != self.affinity_model {
            return Leave(InvalidationReason::UserModelChange);
        }
        if existing_affinity && self.now >= affinity.expires_at_ms && !affinity.lease_active {
            return Leave(InvalidationReason::IdleExpired);
        }
        let view = match &self.state.roster {
            Some(_) => {
                let Some(view) = self.view_for(&affinity.account_id) else {
                    return Leave(InvalidationReason::IdentityWithdrawn);
                };
                let bound = view
                    .credentials
                    .iter()
                    .any(|record| record.credential_id == affinity.credential_id);
                if !bound {
                    let rebound = self.views.iter().any(|other| {
                        other.account_id != affinity.account_id
                            && other
                                .credentials
                                .iter()
                                .any(|record| record.credential_id == affinity.credential_id)
                    });
                    return Leave(if rebound {
                        InvalidationReason::CredentialRebound
                    } else {
                        InvalidationReason::IdentityWithdrawn
                    });
                }
                let hard = self.hard_filters(view, Some(&affinity.credential_id));
                for reason in &hard.reasons {
                    let invalidation = match reason {
                        ExclusionReason::UnsupportedModel => InvalidationReason::UnsupportedModel,
                        ExclusionReason::NotInRunnerCandidates => {
                            InvalidationReason::NotInRunnerCandidates
                        }
                        ExclusionReason::PrincipalExcluded { .. } => {
                            InvalidationReason::PrincipalExcluded
                        }
                        ExclusionReason::ReauthRequired => InvalidationReason::ReauthRequired,
                        // Fences are weighed below, in the reference's order.
                        _ => continue,
                    };
                    return Leave(invalidation);
                }
                Some(view)
            }
            // At a boot with no persisted roster, existing affinity continues
            // while its runner can still read the credential.
            None => {
                if !self.runner_lists(&affinity.account_id, &affinity.credential_id) {
                    return Leave(InvalidationReason::NotInRunnerCandidates);
                }
                if self.principal_excluded(&affinity.credential_id) {
                    return Leave(InvalidationReason::PrincipalExcluded);
                }
                None
            }
        };

        if view.is_some_and(|view| {
            !self.using_credits(&view.account_id) && self.pool_floor_failed(view)
        }) {
            return Leave(InvalidationReason::PolicyFloor);
        }
        let fences = self.fences_for(&affinity.account_id);
        // Rounded quota readings are not refusals: keep an existing session's
        // prompt cache. Synthetic affinities used to compute cold-session
        // holds still obey the reading and the caller and provider wait caps.
        let reading = view
            .filter(|_| !existing_affinity)
            .and_then(|view| view.quota.as_ref())
            .filter(|reading| self.fresh_full(reading));
        let mut blockers: Vec<Blocker> = Vec::new();
        if self.budget_blocked(&affinity.account_id) {
            blockers.push(Blocker {
                reset: self
                    .context
                    .spend
                    .get(&affinity.account_id)
                    .and_then(|spend| spend.reset_at_ms)
                    .or(self.policy.spend_budget_reset_at_ms)
                    .map_or(ResetTime::Missing, ResetTime::At),
                basis: HoldBasis::ProactiveReading,
                leave: InvalidationReason::SpendBudget,
            });
        }
        if let Some(reading) = reading.filter(|_| !self.using_credits(&affinity.account_id)) {
            if let super::types::ScopedList::Listed(windows) = &reading.scoped {
                for window in windows.iter().filter(|window| {
                    super::types::model_matches_scope(&self.model, &window.scope_id)
                        && window.used_percent.is_finite()
                        && window.used_percent >= 100.0
                }) {
                    blockers.push(Blocker {
                        reset: window.resets_at,
                        basis: HoldBasis::ProactiveReading,
                        leave: InvalidationReason::FreshFullReading {
                            window: ApplicableWindow::FamilyScope,
                        },
                    });
                }
            }
            for id in reading.required_windows() {
                if let Some(window) = reading.window(id).filter(|window| {
                    window.used_percent.is_finite() && window.used_percent >= 100.0
                }) {
                    blockers.push(Blocker {
                        reset: window.resets_at,
                        basis: HoldBasis::ProactiveReading,
                        leave: InvalidationReason::FreshFullReading { window: id.into() },
                    });
                }
            }
        }
        // Standing blocks last, so that at an equal reset a confirmed block,
        // not a reading, is named as the hold's basis.
        blockers.extend(fences.iter().map(|fence| Blocker {
            reset: fence.reset,
            basis: HoldBasis::ConfirmedFence,
            leave: InvalidationReason::ConfirmedExhaustion {
                pool: fence.pool.clone(),
            },
        }));
        // The hold rule (design, "Accounts"): the window that blocks
        // continuation decides, whatever kind of window it is. With several,
        // the account frees only when the last of them resets, so a blocker
        // with an unknown reset, or the latest known reset, decides.
        let deciding = blockers.iter().max_by_key(|blocker| match blocker.reset {
            ResetTime::At(at) => (1, at),
            // Unknown recovery outranks every known reset.
            ResetTime::Missing | ResetTime::Unparsable => (2, 0),
        });
        if let Some(blocker) = deciding {
            if self.context.hold_ok == Some(false)
                || self.policy.strategy == AccountStrategy::Ordered
            {
                return Leave(blocker.leave.clone());
            }
            let Some((reset_at_ms, base_retry_after_s)) = hold_until(blocker.reset, self.now)
            else {
                return Leave(blocker.leave.clone());
            };
            let retry_after_s =
                retry_after_with_jitter(&self.request.conversation_key, base_retry_after_s as f64);
            // A reset past the provider's ceiling could never be waited out, so
            // the blocking window explains the move. One within the ceiling but
            // past the caller's own cap moves because the caller chose not to wait.
            if retry_after_s > self.policy.max_hold_s {
                return Leave(blocker.leave.clone());
            }
            if retry_after_s > self.request.hold_wait_max_s {
                return Leave(InvalidationReason::HoldExceedsWait { retry_after_s });
            }
            return AffinityVerdict::Hold(Hold {
                account_id: affinity.account_id.clone(),
                credential_id: affinity.credential_id.clone(),
                pin_id: affinity.pin_id.clone(),
                pin_generation: affinity.pin_generation,
                reset_at_ms,
                base_retry_after_s,
                retry_after_s,
                basis: blocker.basis,
            });
        }
        // Existing authorized, non-fenced affinity continues under missing or
        // stale quota, with the degraded reason recorded.
        let degraded = view.map_or_else(
            || vec![ExclusionReason::QuotaMissing],
            |view| self.evaluate_quota(view).unknown,
        );
        AffinityVerdict::Retain { degraded }
    }

    fn sticky_hit(
        mut self,
        affinity: &ConversationAffinity,
        degraded: Vec<ExclusionReason>,
    ) -> Decision {
        self.explanation.affinity = AffinityNote::Retained {
            account_id: affinity.account_id.clone(),
            degraded,
        };
        let view = self.view_for(&affinity.account_id);
        if let Some(view) = view {
            self.explanation.accounts = vec![self.evaluate_cold(view).note];
        }
        self.admit(
            &affinity.account_id,
            &affinity.credential_id,
            AdmitReason::StickyHit,
            AffinityOutcome::Retained {
                pin_id: affinity.pin_id.clone(),
                pin_generation: affinity.pin_generation,
            },
            view.and_then(|view| view.quota.as_ref()),
        )
    }

    // ---- import --------------------------------------------------------

    /// A `preferred_existing_account` hint is treated as affinity only when the
    /// account passes the cold hard filters and quota checks; otherwise it is
    /// ignored and the rejection recorded. `Err` hands the allocator back for cold work;
    /// it is moved once per decision, so its size does not matter.
    #[allow(clippy::result_large_err)]
    fn import(mut self, preferred: &AccountRef) -> Result<Decision, Self> {
        let resolved = self
            .roster_usable()
            .then(|| self.resolve(preferred))
            .flatten();
        let Some((view, only_credential)) = resolved else {
            self.explanation.import = Some(ImportNote::Rejected {
                reasons: Vec::new(),
                not_in_roster: true,
            });
            return Err(self);
        };
        let hard = self.hard_filters(view, only_credential.as_deref());
        let mut reasons = hard.reasons.clone();
        if reasons.is_empty() {
            // A caller's local account is only a preference, not an established
            // routing affinity. It must leave the same reserve and cold-session
            // headroom as a fresh placement before it can become a pin.
            let quota = self.evaluate_quota(view);
            if !quota.eligible() {
                reasons.extend(quota.unknown);
                reasons.extend(quota.reserve);
            }
        }
        let Some(credential_id) = hard.credential_id.filter(|_| reasons.is_empty()) else {
            self.explanation.import = Some(ImportNote::Rejected {
                reasons,
                not_in_roster: false,
            });
            return Err(self);
        };
        self.explanation.import = Some(ImportNote::Accepted {
            account_id: view.account_id.clone(),
        });
        let imported = ConversationAffinity {
            pin_id: String::new(),
            account_id: view.account_id.clone(),
            credential_id: credential_id.clone(),
            affinity_model: self.affinity_model.clone(),
            pin_generation: 0,
            expires_at_ms: EpochMs::MAX,
            lease_active: false,
        };
        match self.evaluate_affinity(&imported, false) {
            AffinityVerdict::Retain { degraded } => {
                self.explanation.affinity = AffinityNote::Retained {
                    account_id: view.account_id.clone(),
                    degraded,
                };
                self.explanation.accounts = vec![self.evaluate_cold(view).note];
                Ok(self.admit(
                    &view.account_id,
                    &credential_id,
                    AdmitReason::ImportedAffinity,
                    AffinityOutcome::New {
                        previous: None,
                        pin_generation: 1,
                    },
                    view.quota.as_ref(),
                ))
            }
            // Import requires passing the hard filters, which include every
            // fence, so an imported pin can only move here, never hold.
            AffinityVerdict::Hold(_) => Err(self),
            AffinityVerdict::Leave(reason) => {
                self.explanation.affinity = AffinityNote::Invalidated {
                    account_id: view.account_id.clone(),
                    reason,
                };
                Err(self)
            }
        }
    }

    // ---- operator override ---------------------------------------------

    /// An authorized operator override beats affinity and scoring, admits even
    /// with unknown quota, and fails visibly on a hard filter with the code
    /// that filter implies; it never silently picks another account.
    fn operator_override(mut self, override_: &OperatorOverride) -> Decision {
        let resolved = self
            .roster_usable()
            .then(|| self.resolve(&override_.target))
            .flatten();
        let Some((view, only_credential)) = resolved else {
            self.explanation.operator_override = Some(OverrideNote::Rejected {
                reasons: Vec::new(),
                not_in_roster: true,
            });
            return refuse(
                self.explanation,
                RefusalCode::AccountSelectionUnavailable,
                None,
            );
        };
        let hard = self.hard_filters(view, only_credential.as_deref());
        let mut eval = self.evaluate_cold(view);
        if !hard.reasons.is_empty() {
            eval.note.exclusions = hard.reasons.clone();
            eval.note.credential_id = hard.credential_id.clone();
            let code = hard_filter_code(&hard.reasons);
            let retry = matches!(code, RefusalCode::AccountPoolExhausted)
                .then(|| self.pool_retry_after(&[&eval]));
            self.explanation.operator_override = Some(OverrideNote::Rejected {
                reasons: hard.reasons,
                not_in_roster: false,
            });
            self.explanation.accounts = vec![eval.note];
            return refuse(self.explanation, code, retry);
        }
        let credential_id = hard
            .credential_id
            .expect("no hard reason means a usable record");
        let unknown = self.evaluate_quota(view).unknown;
        let reason = if unknown.is_empty() {
            AdmitReason::OperatorOverride
        } else {
            AdmitReason::OperatorOverrideUnknownQuota
        };
        self.explanation.operator_override = Some(OverrideNote::Applied {
            account_id: view.account_id.clone(),
        });
        let affinity = match &self.state.affinity {
            Some(affinity)
                if affinity.account_id == view.account_id
                    && affinity.credential_id == credential_id
                    && normalize(&affinity.affinity_model) == self.affinity_model =>
            {
                self.explanation.affinity = AffinityNote::Retained {
                    account_id: affinity.account_id.clone(),
                    degraded: unknown,
                };
                AffinityOutcome::Retained {
                    pin_id: affinity.pin_id.clone(),
                    pin_generation: affinity.pin_generation,
                }
            }
            Some(affinity) => {
                self.explanation.affinity = AffinityNote::Invalidated {
                    account_id: affinity.account_id.clone(),
                    reason: InvalidationReason::OperatorOverride,
                };
                AffinityOutcome::New {
                    previous: Some(previous_pin(affinity)),
                    pin_generation: affinity.pin_generation + 1,
                }
            }
            None => AffinityOutcome::New {
                previous: None,
                pin_generation: 1,
            },
        };
        self.explanation.accounts = vec![eval.note];
        self.admit(
            &view.account_id,
            &credential_id,
            reason,
            affinity,
            view.quota.as_ref(),
        )
    }

    // ---- cold work (steps 3-7) -----------------------------------------

    fn pending_bytes(&self, account_id: &str, observation_point: Option<EpochMs>) -> u64 {
        // An admission counts until a reading taken `reflection_lag` after its
        // commit could reflect it. Released admissions never count; expired
        // ones keep counting so a lost `started` report cannot hide a run.
        // Without a reading, none of the admitted work is known to have been
        // reflected yet. Keep charging it so new conversations still spread.
        let cutoff = observation_point.map(|point| point - self.policy.reflection_lag_ms);
        self.state
            .admissions
            .iter()
            .filter(|admission| {
                admission.account_id == account_id
                    && admission.state != AdmissionState::Released
                    && cutoff.is_none_or(|cutoff| admission.committed_at_ms > cutoff)
            })
            .map(|admission| admission.request_bytes)
            .sum()
    }

    fn order_index(&self, account_id: &str) -> usize {
        self.policy
            .account_order
            .iter()
            .position(|id| id == account_id)
            .unwrap_or(usize::MAX)
    }

    fn cold(mut self, previous: Option<PreviousPin>) -> Decision {
        if !self.roster_usable() {
            return refuse(
                self.explanation,
                RefusalCode::AccountSelectionUnavailable,
                None,
            );
        }
        let mut evals: Vec<ColdEval> = self
            .views
            .iter()
            .map(|view| self.evaluate_cold(view))
            .collect();
        if evals.is_empty() {
            return refuse(
                self.explanation,
                RefusalCode::AccountSelectionUnavailable,
                None,
            );
        }

        // The Broca floor is a preference, not a wall: accounts that clear it
        // compete as usual, and only when none does are accounts held back by
        // the floor alone admitted, most headroom first. A refused run fails
        // for certain; a run on a nearly used account fails only if it reaches
        // the limit. Every other limit still excludes.
        let floor_fallback = !evals.iter().any(ColdEval::eligible);
        let admissible = |eval: &ColdEval| {
            eval.eligible() || (floor_fallback && eval.eligible_below_broca_floor())
        };

        // Score every admissible account (cold placement steps 5-7 in the
        // prefrontal repository's docs/designs/account-selection.md).
        let direct_opus = self.explanation.direct_opus;
        for eval in evals.iter_mut().filter(|eval| admissible(eval)) {
            let quota = eval.quota.as_ref().expect("admissible has quota");
            let weight = quota.weight.expect("admissible has weight");
            // An account carried by prepaid credits has its quota reading set
            // aside, so it has no observation point of its own. Its credit
            // reading is one: without it, every admission the account ever had
            // would count as pending, and the account with the shortest history
            // would win every new conversation while the others' credits sat
            // unused.
            let observed = quota.observed_at_ms.or_else(|| {
                self.using_credits(&eval.view.account_id)
                    .then(|| self.context.credits.get(&eval.view.account_id))
                    .flatten()
                    .map(|credit| credit.observed_at_ms)
            });
            let pending = self.pending_bytes(&eval.view.account_id, observed);
            let mut score = (pending + self.request_bytes) as f64 / weight;
            let preserved =
                direct_opus
                    && eval.view.quota.as_ref().is_some_and(|reading| {
                        fable_freshly_exhausted(reading, self.now, self.policy)
                    });
            if preserved {
                score /= self.policy.fable_preservation_factor;
            }
            eval.note.pending_bytes = Some(pending);
            eval.note.score = Some(score);
            eval.note.fable_preserved = preserved;
        }
        self.explanation.accounts = evals.iter().map(|eval| eval.note.clone()).collect();

        // Pools emptied by the two non-quota hard filters. When no account can
        // serve the model at all, that is a configuration or catalog fact, so
        // the reply is `account_selection_unsupported` and retrying changes
        // nothing. When the runner's `candidates` list names none of the
        // accounts routing would pick, routing's roster and the runner's
        // listing disagree, and a re-list or roster refresh resolves it, so the
        // reply is the retryable `account_selection_unavailable`. The design's
        // step 6 "No candidate" list (docs/designs/account-selection.md lines
        // 514-522 at f9a0e5d3a) classifies neither case; the operator ruled
        // these codes when this allocator was written. Step 6's precedence
        // then applies only to the accounts that survived reach and candidacy.
        if evals.iter().all(|eval| !eval.hard.reaches) {
            return refuse(
                self.explanation,
                RefusalCode::AccountSelectionUnsupported,
                None,
            );
        }
        let survivors: Vec<&ColdEval> = evals
            .iter()
            .filter(|eval| eval.hard.reaches && eval.hard.runner_listed)
            .collect();
        if survivors.is_empty() {
            return refuse(
                self.explanation,
                RefusalCode::AccountSelectionUnavailable,
                None,
            );
        }
        let mut eligible: Vec<&ColdEval> = survivors
            .iter()
            .copied()
            .filter(|eval| admissible(eval))
            .collect();
        if eligible.is_empty() {
            let holds: Option<Vec<Hold>> = survivors
                .iter()
                .map(|eval| {
                    let affinity = ConversationAffinity {
                        account_id: eval.view.account_id.clone(),
                        credential_id: eval.hard.credential_id.clone()?,
                        affinity_model: self.affinity_model.clone(),
                        pin_id: String::new(),
                        pin_generation: 0,
                        expires_at_ms: EpochMs::MAX,
                        lease_active: false,
                    };
                    match self.evaluate_affinity(&affinity, false) {
                        AffinityVerdict::Hold(hold) => Some(hold),
                        _ => None,
                    }
                })
                .collect();
            if let Some(hold) =
                holds.and_then(|holds| holds.into_iter().min_by_key(|hold| hold.reset_at_ms))
            {
                return Decision {
                    outcome: Outcome::Hold(hold),
                    explanation: self.explanation,
                };
            }
            let (code, retry) = self.no_candidate(&survivors);
            return refuse(self.explanation, code, retry);
        }
        // Prefer a healthy offered identity, but only after runner-candidate
        // filtering. An unavailable sibling must not hide the selected probe.
        if eligible.iter().any(|eval| {
            eval.hard.credential_id.as_ref().is_some_and(|credential| {
                !self.context.credential_probe_notes.contains_key(credential)
            })
        }) {
            eligible.retain(|eval| {
                eval.hard.credential_id.as_ref().is_none_or(|credential| {
                    !self.context.credential_probe_notes.contains_key(credential)
                })
            });
        }
        if eligible.len() > 1 {
            if let Some(parent) = &self.context.parent_account_id {
                if eligible.iter().any(|eval| &eval.view.account_id != parent) {
                    eligible.retain(|eval| &eval.view.account_id != parent);
                }
            }
        }
        eligible.sort_by(|a, b| {
            if floor_fallback {
                // Below the floor, the most spendable points left wins first:
                // that run is the least likely to reach its account's limit.
                let headroom =
                    |eval: &ColdEval| eval.quota.as_ref().map_or(0.0, QuotaEval::headroom);
                let by_headroom = headroom(b).total_cmp(&headroom(a));
                if by_headroom != std::cmp::Ordering::Equal {
                    return by_headroom;
                }
            }
            let order = || {
                let order = self
                    .order_index(&a.view.account_id)
                    .cmp(&self.order_index(&b.view.account_id))
                    .then_with(|| a.view.account_id.cmp(&b.view.account_id));
                if self.policy.fallback_first {
                    order.reverse()
                } else {
                    order
                }
            };
            if self.policy.strategy == AccountStrategy::Ordered {
                return order();
            }
            if matches!(
                self.policy.strategy,
                AccountStrategy::RoundRobin | AccountStrategy::Hybrid
            ) {
                let count = |id: &str| {
                    self.state
                        .admissions
                        .iter()
                        .filter(|admission| admission.account_id == id)
                        .count()
                };
                return count(&a.view.account_id)
                    .cmp(&count(&b.view.account_id))
                    .then_with(order);
            }
            let score = |eval: &ColdEval| eval.note.score.unwrap_or(f64::INFINITY);
            // A score that is not a number never outranks a numeric one,
            // whatever its sign bit; numbers compare by value. A partial
            // comparison that called NaN equal to everything made the order
            // depend on input position, and could make the sort panic.
            // Known headroom is preferred to an absent reading; absent accounts
            // remain available when no measured account can serve the request.
            a.view
                .quota
                .is_none()
                .cmp(&b.view.quota.is_none())
                .then_with(|| {
                    score(a)
                        .is_nan()
                        .cmp(&score(b).is_nan())
                        .then_with(|| score(a).total_cmp(&score(b)))
                        .then_with(|| {
                            self.order_index(&a.view.account_id)
                                .cmp(&self.order_index(&b.view.account_id))
                        })
                        .then_with(|| a.view.account_id.cmp(&b.view.account_id))
                })
        });
        let best = eligible[0];
        let best_score = best.note.score.unwrap_or(f64::INFINITY);
        let mut chosen = (
            best.view,
            best.hard
                .credential_id
                .clone()
                .expect("eligible has a record"),
            if floor_fallback {
                AdmitReason::ColdBelowBrocaFloor
            } else {
                AdmitReason::ColdScored
            },
        );
        if let Some(requested) = &self.request.requested_account {
            let note = self.requested_account(requested, &eligible, best_score);
            if let RequestedAccountNote::Honoured { .. } = note.0 {
                if let Some(pick) = note.1 {
                    chosen = (pick.0, pick.1, AdmitReason::RequestedAccount);
                }
            }
            self.explanation.requested_account = Some(note.0);
        }
        let pin_generation = previous.as_ref().map_or(1, |p| p.pin_generation + 1);
        let (view, credential_id, reason) = chosen;
        self.admit(
            &view.account_id,
            &credential_id,
            reason,
            AffinityOutcome::New {
                previous,
                pin_generation,
            },
            view.quota.as_ref(),
        )
    }

    /// `requested_account` is honoured only for principals granted
    /// `account_request`, and then only if the account is eligible and scores
    /// within the policy factor of the best.
    #[allow(clippy::type_complexity)]
    fn requested_account(
        &self,
        requested: &AccountRef,
        eligible: &[&ColdEval<'a>],
        best_score: f64,
    ) -> (RequestedAccountNote, Option<(&'a AccountView, String)>) {
        let ignored = |reason| (RequestedAccountNote::Ignored { reason }, None);
        if !self
            .policy
            .account_request_principals
            .contains(&self.request.principal)
        {
            return ignored(RequestedAccountIgnored::NotGranted);
        }
        let Some((view, only_credential)) = self.resolve(requested) else {
            return ignored(RequestedAccountIgnored::NotEligible);
        };
        let Some(eval) = eligible
            .iter()
            .find(|eval| eval.view.account_id == view.account_id)
        else {
            return ignored(RequestedAccountIgnored::NotEligible);
        };
        let credential_id = match only_credential {
            Some(credential_id) => {
                let hard = self.hard_filters(view, Some(&credential_id));
                if !hard.reasons.is_empty() {
                    return ignored(RequestedAccountIgnored::NotEligible);
                }
                credential_id
            }
            None => eval
                .hard
                .credential_id
                .clone()
                .expect("eligible has a record"),
        };
        let score = eval.note.score.unwrap_or(f64::INFINITY);
        if score > self.policy.requested_account_factor * best_score {
            return ignored(RequestedAccountIgnored::OutsideFactor);
        }
        (
            RequestedAccountNote::Honoured {
                account_id: view.account_id.clone(),
            },
            Some((view, credential_id)),
        )
    }

    /// Step 6 "No candidate" precedence over the accounts that survived model
    /// reach and runner candidacy.
    fn no_candidate(&self, survivors: &[&ColdEval]) -> (RefusalCode, Option<u64>) {
        if survivors
            .iter()
            .all(|eval| eval.has_tier(ExclusionTier::Principal))
        {
            return (RefusalCode::AccountNotAuthorized, None);
        }
        let remaining: Vec<&ColdEval> = survivors
            .iter()
            .copied()
            .filter(|eval| !eval.has_tier(ExclusionTier::Principal))
            .collect();
        if remaining
            .iter()
            .all(|eval| eval.has_tier(ExclusionTier::Reauth))
        {
            return (RefusalCode::AccountReauthRequired, None);
        }
        let only_quota_unknown = |eval: &&ColdEval| {
            let mut reasons = eval.reasons().peekable();
            reasons.peek().is_some()
                && eval
                    .reasons()
                    .all(|reason| reason.tier() == ExclusionTier::QuotaUnknown)
        };
        if remaining.iter().any(only_quota_unknown) {
            let retry = retry_after_with_jitter(
                &self.request.conversation_key,
                self.policy.quota_unknown_retry_s as f64,
            );
            return (RefusalCode::AccountQuotaUnknown, Some(retry));
        }
        let retry = self.pool_retry_after(survivors);
        if remaining
            .iter()
            .any(|eval| eval.has_tier(ExclusionTier::Reserve))
        {
            return (RefusalCode::AccountPoolReserveLimited, Some(retry));
        }
        (RefusalCode::AccountPoolExhausted, Some(retry))
    }

    /// The earliest relevant future reset plus the margin, or the fallback
    /// when none is known, as the reference's `killswitchRetryAfterSeconds`
    /// computes: general 5h/7d resets, or only the family's scoped resets when
    /// every account is blocked on that scope. Confirmed fences contribute
    /// their header resets; a Retry-After hint is never a reset.
    fn pool_retry_after(&self, accounts: &[&ColdEval]) -> u64 {
        let family = self.family;
        let scope_blocked = accounts.iter().all(|eval| {
            eval.reasons().any(|reason| match reason {
                ExclusionReason::ScopeExhausted { .. } => true,
                ExclusionReason::ConfirmedFence { pool } => {
                    pool.as_ref().map(|key| &key.scope) == Some(&PoolScope::Family(family))
                }
                _ => false,
            })
        });
        let mut resets: Vec<EpochMs> = Vec::new();
        for eval in accounts {
            let reading = eval.view.quota.as_ref();
            if scope_blocked {
                if let Some(reading) = reading {
                    if let super::types::ScopedList::Listed(windows) = &reading.scoped {
                        resets.extend(
                            windows
                                .iter()
                                .filter(|w| {
                                    super::types::model_matches_scope(&self.model, &w.scope_id)
                                })
                                .filter_map(|w| w.resets_at.at_ms()),
                        );
                    }
                }
            } else if let Some(reading) = reading {
                resets.extend(
                    reading
                        .required_windows()
                        .into_iter()
                        .filter_map(|window| reading.window(window))
                        .filter_map(|w| w.resets_at.at_ms()),
                );
            }
            for fence in self.fences_for(&eval.view.account_id) {
                let family_block = matches!(
                    fence.pool.as_ref().map(|key| &key.scope),
                    Some(PoolScope::Family(_))
                );
                let relevant = family_block == scope_blocked;
                if let (true, ResetTime::At(at)) = (relevant, fence.reset) {
                    resets.push(at);
                }
            }
        }
        resets.into_iter().filter(|at| *at > self.now).min().map_or(
            self.policy.pool_retry_fallback_s,
            |earliest| {
                super::evidence::base_retry_after_s(earliest - self.now)
                    + self.policy.pool_retry_margin_s
            },
        )
    }

    fn admit(
        mut self,
        account_id: &str,
        credential_id: &str,
        reason: AdmitReason,
        affinity: AffinityOutcome,
        reading: Option<&QuotaReading>,
    ) -> Decision {
        self.explanation.credential_fence_probe = self
            .context
            .credential_probe_notes
            .get(credential_id)
            .cloned();
        let reason = if self.explanation.credential_fence_probe.is_some() {
            AdmitReason::CredentialFenceProbe
        } else if self.using_credits(account_id) {
            AdmitReason::PaidCredits
        } else {
            reason
        };
        let observed_at_ms = reading.and_then(|reading| reading.observed_at_ms);
        let band = age_band(observed_at_ms, self.now, self.policy)
            .ok()
            .map(|(band, _, _)| band);
        Decision {
            outcome: Outcome::Admit(Admission {
                account_id: account_id.to_string(),
                credential_id: credential_id.to_string(),
                reason,
                affinity,
                request_bytes: self.request_bytes,
                request_bytes_estimated: self.explanation.request_bytes_estimated,
                quota_observed_at_ms: observed_at_ms,
                quota_age_band: band,
            }),
            explanation: self.explanation,
        }
    }
}

/// The reply code a hard filter implies for a one-account decision (the
/// operator override), in the same order the pool derivation applies them.
fn hard_filter_code(reasons: &[ExclusionReason]) -> RefusalCode {
    let has = |tier| reasons.iter().any(|reason| reason.tier() == tier);
    if has(ExclusionTier::Reach) {
        RefusalCode::AccountSelectionUnsupported
    } else if has(ExclusionTier::Candidacy) {
        RefusalCode::AccountSelectionUnavailable
    } else if has(ExclusionTier::Principal) {
        RefusalCode::AccountNotAuthorized
    } else if has(ExclusionTier::Reauth) {
        RefusalCode::AccountReauthRequired
    } else {
        RefusalCode::AccountPoolExhausted
    }
}
