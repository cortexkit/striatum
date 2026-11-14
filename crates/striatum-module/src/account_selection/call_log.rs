//! Observe admission pressure without changing allocator inputs or stored replies.

use super::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct AccountCall {
    surface: &'static str,
    consumer: Option<String>,
    runner_module: Option<String>,
    pub(crate) head_call: bool,
    would_refuse: Arc<Mutex<Option<Value>>>,
}

impl AccountCall {
    pub(crate) fn legacy(params: &Value, principal: &str, store: &SqliteRouteStateStore) -> Self {
        Self::record(
            "route.account_for",
            params,
            principal,
            params["caller"]["runner"].as_str(),
            None,
            None,
            false,
            params["caller"]["harness"].as_str(),
            store,
        )
    }

    pub(crate) fn routing(
        params: &Value,
        caller: &crate::routing_v1::Caller,
        runner: Option<&str>,
        head_call: bool,
        store: &SqliteRouteStateStore,
    ) -> Self {
        let scope = caller.scope_authority.as_ref();
        Self::record(
            "routing/v1",
            params,
            &caller.principal,
            runner,
            scope.map(|scope| wire(&scope.kind)),
            scope.map(|scope| scope.owner.as_str()),
            head_call,
            None,
            store,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        surface: &'static str,
        params: &Value,
        principal: &str,
        runner: Option<&str>,
        scope_kind: Option<Value>,
        scope_owner: Option<&str>,
        head_call: bool,
        harness: Option<&str>,
        store: &SqliteRouteStateStore,
    ) -> Self {
        let decision_id = if surface == "routing/v1" && params["mode"] == "continue" {
            params["previous_decision_id"].as_str()
        } else if surface == "route.account_for" {
            params["model_decision_id"]
                .as_str()
                .or_else(|| params["decision_id"].as_str())
        } else {
            params["decision_id"].as_str()
        };
        let decision = decision_id.and_then(|id| store.v1_decision(id).ok().flatten());
        let consumer = decision
            .filter(|decision| {
                surface == "route.account_for" || runner == Some(&decision.runner_module)
            })
            .map(|decision| decision.consumer)
            .or_else(|| {
                // Older selections store the consumer as target_agent and have
                // no routing/v1 binding row. They still bind legacy admissions.
                (surface == "route.account_for")
                    .then(|| {
                        decision_id
                            .and_then(|id| store.get_decision_by_id(id).ok().flatten())
                            .map(|decision| decision.target_agent)
                    })
                    .flatten()
            });
        crate::info_operator(format!(
            "routing account call {}",
            json!({"surface":surface,"principal":principal,"runner_module":runner,
                "scope_kind":scope_kind,"scope_owner":scope_owner,"consumer":consumer,
                "harness":harness,"head_call":head_call})
        ));
        Self {
            surface,
            consumer,
            runner_module: runner.map(str::to_owned),
            head_call,
            would_refuse: Arc::default(),
        }
    }

    pub(crate) fn finish(&self) {
        if let Some(detail) = self.would_refuse.lock().unwrap().take() {
            crate::info_operator(format!("routing account concurrency would refuse {detail}"));
        }
    }

    pub(super) fn reset(&self) {
        // A writer can retry a command after reloading state. Only its final
        // evaluation describes the request, not an abandoned attempt.
        *self.would_refuse.lock().unwrap() = None;
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn evaluate(
        &self,
        tx: &mut crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        principal: &str,
        hash: &str,
        decision: &Decision,
        state: &AllocatorState,
        inputs: &striatum_core::accounts::AllocationContext,
    ) {
        let Outcome::Admit(admission) = &decision.outcome else {
            return;
        };
        if self.head_call {
            return;
        }
        let caps = &ctx.concurrency_caps;
        // Retained affinity can use capacity reserved against new conversations.
        // Judge its account alone: a free sibling cannot hide pressure there.
        let retained = matches!(
            admission.affinity,
            striatum_core::accounts::AffinityOutcome::Retained { .. }
        );
        // An accepted import skips cold scoring entirely, but is only a
        // preference, not an established affinity. Include its free siblings.
        let cold;
        let eligibility = if !retained
            && matches!(
                decision.explanation.import,
                Some(striatum_core::accounts::ImportNote::Accepted { .. })
            ) {
            let mut request = params.request(principal, false);
            request.preferred_existing_account = None;
            let mut state = state.clone();
            state.affinity = None;
            cold =
                striatum_core::accounts::decide_with_context(&request, &state, ctx.now_ms, inputs);
            &cold
        } else {
            decision
        };
        let mut eligible: BTreeSet<_> = eligibility
            .explanation
            .accounts
            .iter()
            .filter(|note| !retained && note.score.is_some())
            .map(|note| note.account_id.clone())
            .collect();
        eligible.insert(admission.account_id.clone());
        let provider = normalize(&params.provider);
        if eligible
            .iter()
            .any(|account| caps.cap(&provider, account, &params.model).limit.is_none())
        {
            return;
        }
        let mut accounts = Vec::new();
        let mut room = false;
        for account in eligible {
            let cap = caps.cap(&provider, &account, &params.model).limit;
            let count =
                tx.inflight_count_for_model(caps, &provider, &account, &params.model, ctx.now_ms);
            let adds = params.mode != RequestMode::Renew
                || !tx.inflight_conversation(
                    caps,
                    &provider,
                    &account,
                    &params.model,
                    principal,
                    hash,
                    ctx.now_ms,
                );
            room |= !adds || cap.is_none_or(|cap| (count as u64) < cap);
            accounts.push(json!({"account_id":account,"count":count,"cap":cap}));
        }
        if !room {
            *self.would_refuse.lock().unwrap() = Some(json!({
                "surface":self.surface,"consumer":self.consumer.as_ref().or(self.runner_module.as_ref()),
                "model":params.model,"accounts":accounts
            }));
        }
    }
}
