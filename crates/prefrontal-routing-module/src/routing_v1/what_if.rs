//! Operator-only, call-local evaluation of the live v1 selection pipeline.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cortexkit_role_routing::errors::Refusal;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{operator_config, provider_refusal, select, store_refusal, Caller, Deps};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    consumer: String,
    iqs: Vec<f64>,
    eq: Option<f64>,
    runner: Option<String>,
    policy_patch: Option<PolicyPatch>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PolicyPatch {
    models: BTreeMap<String, crate::config_overrides::ModelPolicyPatch>,
}

pub(super) async fn what_if(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    what_if_with_profile_lookup(params, caller, deps, &select::live_profile_lookup).await
}

pub(super) async fn what_if_with_profile_lookup(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    profile_lookup: &select::ProfileLookup,
) -> Result<Value, Refusal> {
    if !crate::is_fence_lift_operator(&caller.principal) {
        return Err(provider_refusal(
            "not_permitted",
            "route.what_if is operator-only (direct or reserved:callosum)",
        ));
    }
    let params: Params = serde_json::from_value(params.clone())
        .map_err(|error| Refusal::invalid_request("params", error.to_string()))?;
    if params.iqs.is_empty()
        || params.iqs.len() > 101
        || params
            .iqs
            .iter()
            .any(|iq| !iq.is_finite() || !(0.0..=100.0).contains(iq))
    {
        return Err(Refusal::invalid_request(
            "iqs",
            "iqs must contain 1-101 finite numbers in 0..100",
        ));
    }
    if params
        .eq
        .is_some_and(|eq| !eq.is_finite() || !(0.0..=100.0).contains(&eq))
    {
        return Err(Refusal::invalid_request(
            "eq",
            "eq must be a finite number in 0..100",
        ));
    }
    if params
        .runner
        .as_deref()
        .is_some_and(|runner| runner != "broca")
    {
        return Err(Refusal::invalid_request(
            "runner",
            "runner must be broca when supplied",
        ));
    }
    let started = std::time::Instant::now();
    let live = crate::config_overrides::live_config(deps.routing_config_path);
    let operator = operator_config(deps);
    let patch = params.policy_patch.unwrap_or_default();
    let live = Arc::new(
        live.with_model_policy_patch(&patch.models)
            .map_err(|error| Refusal::invalid_request("policy_patch", error))?,
    );
    let operator = Arc::new(
        operator
            .with_model_policy_patch(&patch.models)
            .map_err(|error| Refusal::invalid_request("policy_patch", error))?,
    );
    if !super::consumers::effective(&operator).knows(&params.consumer) {
        return Err(Refusal::invalid_request(
            "consumer",
            "consumer is not registered",
        ));
    }
    // Account loading heals stale evidence. Heal only a disposable projection,
    // copied once for the whole sweep; no decision or usage history is copied.
    let account_store = Arc::new(
        deps.store
            .account_evaluation_snapshot(super::now_ms() as i64)
            .map_err(store_refusal)?,
    );
    let snapshot_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut results = Vec::with_capacity(params.iqs.len());
    for iq in params.iqs {
        let mut request =
            json!({"consumer": params.consumer, "requirements": {"iq": iq.round() as u8}});
        if let Some(eq) = params.eq {
            request["requirements"]["eq"] = json!(eq.round() as u8);
        }
        if let Some(runner) = &params.runner {
            request["runners"] = json!([runner]);
        }
        let view = select::Evaluation {
            live_config: live.clone(),
            operator_config: operator.clone(),
            account_store: &account_store,
            iq,
            eq: params.eq,
            evidence: Mutex::new(Default::default()),
        };
        let result = select::evaluate(&request, caller, deps, &view, profile_lookup).await;
        let evidence = view.evidence.into_inner().unwrap();
        let excluded: Vec<_> = evidence
            .excluded
            .into_iter()
            .map(|rejected| {
                let reason = operator
                    .route_policy_refusal(&params.consumer, &rejected.route_key)
                    .unwrap_or(rejected.reason);
                json!({"route": rejected.route_key, "reason": reason})
            })
            .collect();
        let mut row = json!({"iq": iq, "bar": evidence.bar, "top_routable": evidence.top_routable,
            "adequate": evidence.adequate, "pick": null, "excluded": excluded});
        match result {
            Ok(pick) => row["pick"] = pick,
            Err(refusal) => {
                row["refusal"] = json!({"code": refusal.code, "detail": refusal.detail})
            }
        }
        results.push(row);
    }
    Ok(
        json!({"consumer": params.consumer, "results": results, "snapshot_ms": snapshot_ms, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0}),
    )
}
