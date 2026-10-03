//! `decide`: the pure account allocator, implementing the design's "New-session
//! rule: sticky-balanced first" steps 1-7 and its precedence table
//! (`docs/designs/account-selection.md`). It performs no I/O: the caller loads
//! [`AllocatorState`] inside its selection transaction and commits the result.

use std::cmp::Ordering;

use super::decision::{
    AccountNote, Admission, AdmitReason, AffinityNote, AffinityOutcome, ApplicableWindow, Decision,
    ExclusionReason, ExclusionTier, Explanation, Hold, HoldBasis, ImportNote, InvalidationReason,
    Outcome, OverrideNote, PreviousPin, Refusal, RefusalCode, RequestedAccountIgnored,
    RequestedAccountNote, RosterNote,
};
use super::eligibility::{
    age_band, evaluate_quota, fable_freshly_exhausted, scope_check, QuotaEval,
};
use super::evidence::{retry_after_with_jitter, short_hold_rule, ExhaustionAction, MigrateReason};
use super::types::{
    AccountRef, AccountRequest, AccountView, AdmissionState, AllocatorPolicy, AllocatorState,
    ConversationAffinity, CredentialRecord, EpochMs, Fence, FenceScope, GeneralWindow, ModelFamily,
    OperatorOverride, QuotaReading, ResetTime,
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
        if let Some(override_) = &self.state.operator_override {
            return self.operator_override(override_);
        }
        let mut previous = None;
        if let Some(affinity) = &self.state.affinity {
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
        self.request.candidates.as_ref().is_none_or(|candidates| {
            candidates
                .accounts
                .iter()
                .any(|c| c.account_id == account_id && c.credential_id == credential_id)
        })
    }

    /// A refusal exclusion lasts until the runner reports a different view
    /// digest, routing's roster digest moves, or its cap passes.
    fn principal_excluded(&self, credential_id: &str) -> bool {
        self.state.principal_exclusions.iter().any(|exclusion| {
            exclusion.principal == self.request.principal
                && exclusion.credential_id == credential_id
                && self.now - exclusion.recorded_at_ms < self.policy.principal_exclusion_max_ms
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

    /// Fences on this account that restrict the requested family.
    fn fences_for(&self, account_id: &str) -> Vec<&'a Fence> {
        let mut fences: Vec<&Fence> = self
            .state
            .fences
            .iter()
            .filter(|fence| fence.account_id == account_id && fence.scope.applies_to(self.family))
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
                let usable: Vec<&CredentialRecord> = allowed
                    .into_iter()
                    .filter(|record| !record.needs_reauth)
                    .collect();
                if usable.is_empty() {
                    reasons.push(ExclusionReason::ReauthRequired);
                } else {
                    credential_id = Self::bind_record(&usable);
                }
            }
        }
        let mut scopes: Vec<FenceScope> = Vec::new();
        for fence in self.fences_for(&view.account_id) {
            if !scopes.contains(&fence.scope) {
                scopes.push(fence.scope);
                reasons.push(ExclusionReason::ConfirmedFence { scope: fence.scope });
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
        evaluate_quota(
            view.quota.as_ref(),
            self.family,
            self.request.caller,
            self.now,
            self.policy,
        )
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
    /// better score elsewhere, a reading below the reserve, or a stale reading
    /// never moves it.
    fn evaluate_affinity(
        &self,
        affinity: &ConversationAffinity,
        check_expiry: bool,
    ) -> AffinityVerdict {
        use AffinityVerdict::Leave;
        if normalize(&affinity.affinity_model) != self.affinity_model {
            return Leave(InvalidationReason::UserModelChange);
        }
        if check_expiry && self.now >= affinity.expires_at_ms && !affinity.lease_active {
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

        let fences = self.fences_for(&affinity.account_id);
        let has_fence = |scope: FenceScope| fences.iter().any(|fence| fence.scope == scope);
        if fences
            .iter()
            .any(|fence| matches!(fence.scope, FenceScope::UnknownScope(_)))
        {
            return Leave(InvalidationReason::UnknownScopeFence);
        }
        let reading = view
            .and_then(|view| view.quota.as_ref())
            .filter(|reading| self.fresh_full(reading));
        let fresh_scoped = reading.is_some_and(|reading| {
            let check = scope_check(self.family, &reading.scoped, self.policy);
            check.matched && !check.passes
        });
        let fresh_full_used = |window: GeneralWindow| {
            reading
                .and_then(|reading| reading.window(window))
                .is_some_and(|w| w.used_percent.is_finite() && w.used_percent >= 100.0)
        };
        let confirmed = |migrate| Leave(InvalidationReason::ConfirmedExhaustion { migrate });
        let fresh = |window| Leave(InvalidationReason::FreshFullReading { window });

        // The reference's order (anthropic-auth@b504bc84
        // packages/core/src/sticky-routing.ts:369-413): model-scoped
        // exhaustion, then 7d, migrate first, whether confirmed by a fence or
        // shown by a full-band reading at 100%. Only then can a 5h exhaustion
        // hold: a confirmed 5h fence, or failing that a full-band 5h reading at
        // 100% (the reference's proactive pre-send check,
        // packages/opencode/src/index.ts:7500-7535). The reading-based hold is
        // soft and local to this conversation: it creates no fence, and it is
        // re-evaluated at every admission, so a newer reading below 100 ends it.
        let hold_or_leave = |reset: ResetTime, basis: HoldBasis| {
            let action = short_hold_rule(
                false,
                false,
                Some(reset),
                self.now,
                self.policy.short_hold_max_ms,
            );
            match action {
                ExhaustionAction::Hold {
                    reset_at_ms,
                    base_retry_after_s,
                } => {
                    let retry_after_s = retry_after_with_jitter(
                        &self.request.conversation_key,
                        base_retry_after_s as f64,
                    );
                    if retry_after_s <= self.request.hold_wait_max_s {
                        Some(AffinityVerdict::Hold(Hold {
                            account_id: affinity.account_id.clone(),
                            credential_id: affinity.credential_id.clone(),
                            pin_id: affinity.pin_id.clone(),
                            pin_generation: affinity.pin_generation,
                            reset_at_ms,
                            base_retry_after_s,
                            retry_after_s,
                            basis,
                        }))
                    } else {
                        Some(Leave(InvalidationReason::HoldExceedsWait { retry_after_s }))
                    }
                }
                // A missing, unparsable, past or distant reset never holds.
                _ => None,
            }
        };
        if has_fence(FenceScope::Family(self.family)) {
            return confirmed(MigrateReason::ModelScoped);
        }
        if fresh_scoped {
            return fresh(ApplicableWindow::FamilyScope);
        }
        if has_fence(FenceScope::General(GeneralWindow::SevenDay)) {
            return confirmed(MigrateReason::SevenDay);
        }
        if fresh_full_used(GeneralWindow::SevenDay) {
            return fresh(ApplicableWindow::SevenDay);
        }
        if let Some(window) = reading.and_then(|reading| {
            reading.required_windows().into_iter().find(|id| {
                matches!(id, GeneralWindow::Reported(_))
                    && reading.window(*id).is_some_and(|window| {
                        window.used_percent.is_finite() && window.used_percent >= 100.0
                    })
            })
        }) {
            return fresh(window.into());
        }
        if let Some(five_hour) = fences
            .iter()
            .find(|fence| fence.scope == FenceScope::General(GeneralWindow::FiveHour))
        {
            return hold_or_leave(five_hour.reset, HoldBasis::ConfirmedFence)
                .unwrap_or_else(|| confirmed(MigrateReason::FiveHour));
        }
        if fresh_full_used(GeneralWindow::FiveHour) {
            let reset = reading
                .and_then(|reading| reading.window(GeneralWindow::FiveHour))
                .map_or(ResetTime::Missing, |w| w.resets_at);
            return hold_or_leave(reset, HoldBasis::ProactiveReading)
                .unwrap_or_else(|| fresh(ApplicableWindow::FiveHour));
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
    /// account passes the hard filters and step 3; otherwise it is ignored and
    /// the rejection recorded. `Err` hands the allocator back for cold work;
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
            reasons.extend(self.evaluate_quota(view).unknown);
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
        let Some(point) = observation_point else {
            return 0;
        };
        let cutoff = point - self.policy.reflection_lag_ms;
        self.state
            .admissions
            .iter()
            .filter(|admission| {
                admission.account_id == account_id
                    && admission.state != AdmissionState::Released
                    && admission.committed_at_ms > cutoff
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

        // Scoring (steps 5-7) for every eligible account.
        let direct_opus = self.explanation.direct_opus;
        for eval in evals.iter_mut().filter(|eval| eval.eligible()) {
            let quota = eval.quota.as_ref().expect("eligible has quota");
            let weight = quota.weight.expect("eligible has weight");
            let pending = self.pending_bytes(&eval.view.account_id, quota.observed_at_ms);
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
            .filter(|eval| eval.eligible())
            .collect();
        if eligible.is_empty() {
            let (code, retry) = self.no_candidate(&survivors);
            return refuse(self.explanation, code, retry);
        }
        eligible.sort_by(|a, b| {
            let score = |eval: &ColdEval| eval.note.score.unwrap_or(f64::INFINITY);
            score(a)
                .partial_cmp(&score(b))
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    self.order_index(&a.view.account_id)
                        .cmp(&self.order_index(&b.view.account_id))
                })
                .then_with(|| a.view.account_id.cmp(&b.view.account_id))
        });
        let best = eligible[0];
        let best_score = best.note.score.unwrap_or(f64::INFINITY);
        let mut chosen = (
            best.view,
            best.hard
                .credential_id
                .clone()
                .expect("eligible has a record"),
            AdmitReason::ColdScored,
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
        let scope_blocked = self.policy.family_has_scope(family)
            && accounts.iter().all(|eval| {
                eval.reasons().any(|reason| match reason {
                    ExclusionReason::ScopeExhausted { .. } => true,
                    ExclusionReason::ConfirmedFence { scope } => {
                        *scope == FenceScope::Family(family)
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
                                .filter(|w| self.policy.family_for_scope(w) == Some(family))
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
                let relevant = match fence.scope {
                    FenceScope::General(_) => !scope_blocked,
                    FenceScope::Family(_) => scope_blocked,
                    FenceScope::UnknownScope(_) => false,
                };
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
        self,
        account_id: &str,
        credential_id: &str,
        reason: AdmitReason,
        affinity: AffinityOutcome,
        reading: Option<&QuotaReading>,
    ) -> Decision {
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
