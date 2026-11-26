use super::*;

impl ClassContext {
    pub(super) fn blocked_account_with_policy(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
        policy: &AllocatorPolicy,
    ) -> Option<AccountBlock> {
        // An empty roster may have lost accounts to route or credential
        // fences. Reuse admission's classification so only a dated account
        // limit, not missing credentials or policy exclusions, supplies a reset.
        let (roster, fenced) = self.roster_and_fences_for(provider, model, priced);
        if roster
            .roster
            .as_ref()
            .is_some_and(|roster| roster.accounts.is_empty())
        {
            if let Some(OpReply::Error { code, detail, .. }) =
                empty_roster_refusal(self, provider, model, priced, &fenced, self.now_ms, None)
            {
                if code == "account_pool_exhausted" {
                    let reset = detail.and_then(|detail| detail["reset_at_ms"].as_i64());
                    let mut block = self.account_block_at(
                        provider,
                        model,
                        priced,
                        reset,
                        striatum_core::blocks::LifetimeClass::NonQuota,
                    );
                    block.test_check_at_ms =
                        self.in_flight_test_check_at(provider, model, priced, policy, None);
                    return Some(block);
                }
            }
        }
        let blocked = self.probe_with_policy(
            provider,
            model,
            priced,
            false,
            None,
            "blocked-preflight",
            policy.clone(),
        );
        if !blocked.refusal().is_some_and(|refusal| {
            matches!(
                refusal.code,
                striatum_core::accounts::RefusalCode::AccountPoolExhausted
                    | striatum_core::accounts::RefusalCode::AccountPoolReserveLimited
            )
        }) {
            return None;
        }
        let reset = pool_recovery_reset(
            &blocked,
            roster.roster.as_ref(),
            self.fences.get(provider).map_or(&[], Vec::as_slice),
            policy,
            model,
            self.now_ms,
        );
        let reset = reset.into_iter().chain(fenced.values().copied()).min();
        // The allocator recovery above comes only from typed quota exclusions.
        // A route or credential origin at that recovery may instead be non-quota.
        let mut block = self.account_block_at(
            provider,
            model,
            priced,
            reset,
            striatum_core::blocks::LifetimeClass::Quota,
        );
        block.test_check_at_ms =
            self.in_flight_test_check_at(provider, model, priced, policy, None);
        Some(block)
    }

    pub(super) fn account_test_check_refusal(
        &self,
        params: &AccountForParams,
        provider: &str,
        priced: bool,
        policy: &AllocatorPolicy,
        mut reply: OpReply,
    ) -> OpReply {
        let OpReply::Error {
            code,
            message,
            detail: Some(detail),
        } = &mut reply
        else {
            return reply;
        };
        if code != "account_pool_exhausted" {
            return reply;
        }
        let Some(until) = detail["reset_at_ms"].as_i64() else {
            return reply;
        };
        let model = normalize(&params.model);
        let Some(check_at) = self
            .in_flight_test_check_at(provider, &model, priced, policy, Some(params))
            .filter(|check| *check <= until)
        else {
            return reply;
        };
        // The check is a caller hint, not a change to the probe's recheck clock.
        detail["reset_at_ms"] = json!(check_at);
        let wait_ms = check_at.saturating_sub(self.now_ms).max(0) as u64;
        detail["retry_after_s"] = json!(wait_ms.div_ceil(1_000));
        *message =
            format!("A recovery test is in progress for {provider}/{model}. Check again shortly.");
        because("recovery_test_in_progress", reply)
    }

    fn in_flight_test_check_at(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
        policy: &AllocatorPolicy,
        admission: Option<&AccountForParams>,
    ) -> Option<i64> {
        let fact = self.fact_for(provider, model)?;
        self.class(fact, priced)
            .rows
            .into_iter()
            .filter_map(|row| {
                let account = self.account_id(row);
                if admission.is_some_and(|params| {
                    !params.offers_pair(account, &row.id)
                        || params
                            .bound_tier
                            .as_ref()
                            .is_some_and(|tier| !tier.supports(&row.id))
                }) {
                    return None;
                }
                let blocked: Vec<_> = self
                    .indexed_origins(provider, account, &row.id, model)
                    .filter(|origin| {
                        self.origin_applies(origin, provider, account, &row.id, model)
                            && !self.origin_due(origin, account, model)
                    })
                    .collect();
                if blocked.is_empty()
                    || !blocked.iter().all(|origin| {
                        let authorizations: Vec<_> = self
                            .probe_authorizations
                            .iter()
                            .filter(|proof| {
                                proof.kind == origin.kind
                                    && proof.origin_key == origin.origin_key
                                    && proof.evidence == origin.evidence
                                    && proof.evidence_at == origin.evidence_at
                            })
                            .collect();
                        !origin.quota
                            && (admission.is_none()
                                || matches!(
                                    origin.kind.as_str(),
                                    "route" | "transient_route" | "observation" | "pool_rate"
                                ))
                            && origin.due_at() > self.now_ms
                            && !(self.probe_owner.is_some()
                                && (origin.claim_owner == self.probe_owner
                                    || authorizations.iter().any(|proof| {
                                        Some(&proof.owner) == self.probe_owner.as_ref()
                                    })))
                            && (origin.claim_owner.is_some() || !authorizations.is_empty())
                    })
                {
                    return None;
                }
                // Relax only these tests in a private view. A quota, reserve or
                // credential restriction on the same account must still prevent
                // this route from advertising a short status check.
                let mut recovered = self.clone();
                recovered.replace_rechecks(
                    self.rechecks
                        .iter()
                        .cloned()
                        .map(|mut origin| {
                            if blocked.iter().any(|held| {
                                held.kind == origin.kind && held.origin_key == origin.origin_key
                            }) {
                                origin.next_recheck_at = self.now_ms;
                                origin.reset_check_at = None;
                                origin.claim_owner = None;
                            }
                            origin
                        })
                        .collect(),
                );
                if !matches!(
                    recovered
                        .probe_with_policy_for(
                            provider,
                            model,
                            priced,
                            false,
                            None,
                            "in-flight-test-recovery",
                            policy.clone(),
                            admission.map(|params| (params, row.id.as_str())),
                        )
                        .outcome,
                    Outcome::Admit(_)
                ) {
                    return None;
                }
                blocked
                    .iter()
                    .map(|origin| origin.due_at())
                    .min()
                    .map(|clock| clock.min(self.now_ms.saturating_add(30_000)))
            })
            .min()
    }

    fn account_block_at(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
        reset_at_ms: Option<i64>,
        fallback: striatum_core::blocks::LifetimeClass,
    ) -> AccountBlock {
        use striatum_core::blocks::LifetimeClass;
        let mut block = AccountBlock {
            reset_at_ms,
            test_check_at_ms: None,
            lifetime: fallback,
            credential_code: None,
        };
        let Some(fact) = self.fact_for(provider, model) else {
            return block;
        };
        let mut non_quota = false;
        for row in self.class(fact, priced).rows {
            let account = self.account_id(row);
            for origin in self.indexed_origins(provider, account, &row.id, model) {
                if !self.origin_applies(origin, provider, account, &row.id, model)
                    || self.origin_due(origin, account, model)
                    || reset_at_ms.is_some_and(|reset| {
                        self.recheck_hint(origin, account, model) != reset
                            && self.probe_reserved_until(provider, &row.id) != Some(reset)
                    })
                {
                    continue;
                }
                // Only origins participating in the named recovery determine
                // its class; a later non-quota block origin on another account is unrelated.
                if origin.quota {
                    if !non_quota {
                        block.lifetime = LifetimeClass::Quota;
                    }
                    continue;
                }
                non_quota = true;
                block.lifetime = LifetimeClass::NonQuota;
                if origin.kind == "credential" {
                    block.credential_code = self
                        .refusals
                        .iter()
                        .find(|refusal| {
                            refusal.provider_id == provider && refusal.credential_id == row.id
                        })
                        .map(|refusal| refusal.code.clone());
                }
            }
        }
        block
    }
}
