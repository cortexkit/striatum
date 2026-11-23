//! A stored tier restricts both allocation and which blocked logins can justify a retry.

use super::*;
use cortexkit_role_routing::service_tier::ServiceTier;

#[derive(Clone, Debug)]
pub(super) struct BoundTier {
    pub(super) tier: ServiceTier,
    pub(super) route_fenced: BTreeMap<String, i64>,
    // None means the credential supports the bound tier, whether or not a
    // request-time filter (for example a quota block) keeps it out right now.
    credentials: BTreeMap<String, Option<&'static str>>,
}

impl BoundTier {
    pub(super) fn read(
        params: &AccountForParams,
        ctx: &Ctx<'_>,
        provider: &str,
        selection: Option<&striatum_store::V1Decision>,
    ) -> Result<Option<Self>, OpReply> {
        let Some((tier, allowed)) = params
            .model_decision_id
            .as_deref()
            .map(|id| ctx.store.decision_service_tier(id))
            .transpose()
            .map_err(store_error)?
            .flatten()
        else {
            return Ok(None);
        };
        let tier = ServiceTier::parse(&tier).ok_or_else(|| {
            unservable_refusal(
                cortexkit_role_routing::errors::AccountUnservableReason::StoredStateInvalid,
                vec![],
                vec![],
            )
        })?;
        let checks = ctx
            .classes
            .as_ref()
            .filter(|_| !tier.is_standard())
            .map(|classes| {
                let registry = crate::routing_v1::runners::Registry::build(
                    classes.config.as_ref(),
                    &classes.credentials,
                    &classes.facts,
                    classes.now_ms.max(0) as u64,
                );
                let model = normalize(&params.model);
                let fact = classes
                    .facts
                    .iter()
                    .find(|fact| fact.provider_id == provider && fact.model_id == model);
                (classes, registry, fact)
            });
        // Old model decisions do not name a runner, so use the one serving the request.
        let runner = selection.map_or_else(
            || normalize(&params.caller.runner),
            |d| d.runner_module.clone(),
        );
        let credentials = allowed
            .into_iter()
            .map(|id| {
                let reason = if tier.is_standard() {
                    None
                } else if let Some((classes, registry, Some(fact))) = &checks {
                    let credentials = &classes.credentials;
                    let level = tier.as_str();
                    if !crate::service_tiers::model_offers(credentials, fact, level) {
                        Some("model_tier_unavailable")
                    } else if let Some(row) = credentials
                        .listed_credentials()
                        .iter()
                        .find(|row| row.id == id)
                    {
                        if crate::service_tiers::plan_rate(
                            &credentials.plan_prices,
                            row,
                            provider,
                            level,
                            false,
                        )
                        .is_none()
                        {
                            Some("plan_tier_unavailable")
                        } else if !registry.get(&runner).is_some_and(|runner| {
                            crate::service_tiers::runner_supports(
                                credentials,
                                runner,
                                fact,
                                row,
                                level,
                            )
                        }) {
                            Some("runner_tier_unavailable")
                        } else {
                            None
                        }
                    } else {
                        Some("credential_not_listed")
                    }
                } else {
                    Some("model_tier_unavailable")
                };
                (id, reason)
            })
            .collect();
        Ok(Some(Self {
            tier,
            credentials,
            route_fenced: BTreeMap::new(),
        }))
    }

    pub(super) fn supports(&self, credential: &str) -> bool {
        self.credentials.get(credential) == Some(&None)
    }

    fn exclusion(&self, credential: &str) -> Option<&'static str> {
        self.credentials
            .get(credential)
            .copied()
            .unwrap_or(Some("outside_tier_allowance"))
    }
}

pub(super) fn bound_tier_empty_refusal(
    params: &AccountForParams,
    ctx: &Ctx<'_>,
    principal: &str,
    provider: &str,
    class: striatum_core::accounts::AllowedAccountClass,
    roster: &RosterBuild,
) -> OpReply {
    let ordinary = prepared_bound_refusal(
        params,
        ctx.classes.as_ref(),
        provider,
        class == striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
        ctx.now_ms,
    );
    // This path records no decision. Read the pin for the operator log
    // without creating one or changing the answer if the read fails.
    if let Ok(Some(pin)) = ctx.store.account_read(|tx| {
        let Some(salt) = tx.existing_salt()? else {
            return Ok(None);
        };
        tx.affinity(
            principal,
            &conversation_hash(&salt, principal, &params.conversation_key),
            provider,
        )
    }) {
        let reason = bound_pin_exclusion(params, &pin)
            .or_else(|| prepared_exclusion(ordinary.as_ref(), &pin.credential_id))
            .unwrap_or("not_in_eligible_roster");
        crate::account_refusal_log::pinned_ineligibility(json!({
            "credential_id": pin.credential_id, "reason": reason,
        }));
    }
    if prepared_reset(ordinary.as_ref(), ctx.now_ms).is_some() {
        return ordinary.unwrap();
    }
    if let Some(candidates) = &params.candidates {
        let eligible: BTreeSet<_> = roster
            .roster
            .iter()
            .flat_map(|r| &r.accounts)
            .flat_map(|account| &account.credentials)
            .map(|credential| credential.credential_id.clone())
            .collect();
        if !eligible.is_empty() {
            let offered: BTreeSet<_> = candidates
                .accounts
                .iter()
                .map(|candidate| candidate.credential_id.clone())
                .collect();
            return because(
                "candidates_filter",
                unservable_refusal(
                    cortexkit_role_routing::errors::AccountUnservableReason::CandidatesDisjoint,
                    offered.into_iter().collect(),
                    eligible.into_iter().collect(),
                ),
            );
        }
    }
    bound_service_tier_refusal(params)
}

pub(super) fn prepared_bound_refusal(
    params: &AccountForParams,
    classes: Option<&ClassContext>,
    provider: &str,
    priced: bool,
    now: i64,
) -> Option<OpReply> {
    let bound = params.bound_tier.as_ref()?;
    empty_roster_refusal(
        classes?,
        provider,
        &normalize(&params.model),
        priced,
        &bound.route_fenced,
        now,
        Some(params),
    )
}

fn refusal_detail(reply: Option<&OpReply>) -> Option<&Value> {
    match reply? {
        OpReply::Error { detail, .. } => detail.as_ref(),
        _ => None,
    }
}

pub(super) fn prepared_reset(reply: Option<&OpReply>, now: i64) -> Option<i64> {
    refusal_detail(reply)?["reset_at_ms"]
        .as_i64()
        .filter(|until| *until > now)
}

fn prepared_exclusion<'a>(reply: Option<&'a OpReply>, credential: &str) -> Option<&'a str> {
    refusal_detail(reply)?["exclusions"]
        .as_array()?
        .iter()
        .find(|entry| entry["credential_id"] == credential)?["reason"]
        .as_str()
}

fn bound_pin_exclusion(
    params: &AccountForParams,
    pin: &AccountAffinityRow,
) -> Option<&'static str> {
    let bound = params.bound_tier.as_ref()?;
    if !params.offers_pair(&pin.account_id, &pin.credential_id) {
        return Some("not_in_runner_candidates");
    }
    bound.exclusion(&pin.credential_id).or_else(|| {
        bound
            .route_fenced
            .contains_key(&pin.account_id)
            .then_some("route_fenced")
    })
}

pub(super) fn bound_pin_diagnostic(
    params: &AccountForParams,
    pin: &AccountAffinityRow,
    decision: &Decision,
    lost_probes: &BTreeMap<String, i64>,
    prepared_refusal: Option<&OpReply>,
) -> Option<Value> {
    use striatum_core::accounts::{AffinityNote, InvalidationReason};
    if matches!(&decision.outcome, Outcome::Admit(pick)
        if pick.account_id == pin.account_id && pick.credential_id == pin.credential_id)
    {
        return None;
    }
    let reason = if let Some(reason) = bound_pin_exclusion(params, pin) {
        json!(reason)
    } else if lost_probes.contains_key(&pin.account_id) {
        json!("probe_blocked")
    } else if let AffinityNote::Invalidated { reason, .. } = &decision.explanation.affinity {
        let detail = matches!(reason, InvalidationReason::IdentityWithdrawn)
            .then(|| prepared_exclusion(prepared_refusal, &pin.credential_id))
            .flatten();
        detail.map_or_else(|| wire(reason)["reason"].clone(), |reason| json!(reason))
    } else if let Outcome::Hold(hold) = &decision.outcome {
        if hold.credential_id != pin.credential_id {
            return None;
        }
        wire(&hold.basis)
    } else {
        return None;
    };
    Some(json!({"credential_id": pin.credential_id, "reason": reason}))
}
