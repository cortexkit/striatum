//! Media previews freeze mutable evidence once and never record a routing decision.

use std::sync::{Arc, Mutex};

use cortexkit_role_routing::{errors::Refusal, select_media};
use serde_json::{json, Value};

use super::super::{engines, operator_config, store_refusal, Caller, Deps};

pub(super) fn what_if(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    let started = std::time::Instant::now();
    let object = params
        .as_object()
        .ok_or_else(|| Refusal::invalid_request("params", "expected an object"))?;
    for field in object.keys() {
        if !["consumer", "kind", "iqs", "privacy", "requirements"].contains(&field.as_str()) {
            return Err(Refusal::invalid_request(
                field,
                "member is not supported for media previews",
            ));
        }
    }
    let iqs: Option<Vec<f64>> = object
        .get("iqs")
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|error| Refusal::invalid_request("iqs", error.to_string()))
        })
        .transpose()?;
    if iqs.as_ref().is_some_and(|iqs| {
        iqs.is_empty()
            || iqs.len() > 101
            || iqs
                .iter()
                .any(|iq| !iq.is_finite() || !(0.0..=100.0).contains(iq))
    }) {
        return Err(Refusal::invalid_request(
            "iqs",
            "iqs must contain 1-101 finite numbers in 0..100",
        ));
    }
    let mut select_params = params.clone();
    select_params.as_object_mut().unwrap().remove("iqs");
    let request = select_media::parse_select_media_request(
        &select_params,
        &super::super::describe::capabilities(),
        engines::MEDIA_KINDS,
    )?;
    let config = operator_config(deps);
    let consumers = Arc::new(super::super::consumers::effective(&config));
    if !consumers.knows(&request.consumer) {
        return Err(Refusal::invalid_request(
            "consumer",
            "consumer is not registered",
        ));
    }
    let live = crate::config_overrides::live_config(deps.routing_config_path);
    let credentials = Arc::new(Mutex::new(
        deps.broca_credentials
            .lock()
            .map_err(store_refusal)?
            .clone(),
    ));
    let quota = Arc::new(Mutex::new(
        deps.quota_cache.lock().map_err(store_refusal)?.clone(),
    ));
    let account_quota = crate::ACCOUNT_QUOTA
        .get()
        .map(|quota| {
            quota
                .lock()
                .map(|state| Arc::new(Mutex::new(state.clone())))
                .map_err(store_refusal)
        })
        .transpose()?;
    // Capture account state and catalogue once before reading preview rows.
    let accounts =
        Arc::new(crate::account_selection::Sweep::capture(deps.store).map_err(store_refusal)?);
    let inputs = Arc::new(
        crate::select::prepare_selection_inputs_for_sweep(
            &quota,
            deps.store,
            config.pricing(),
            &config.quota_model_pools(),
            Some(&accounts),
        )
        .map_err(store_refusal)?,
    );
    let concurrency = super::concurrency::Snapshot::from_snapshot(
        deps,
        &live,
        accounts.initial.clone(),
        &inputs.catalog_snapshot.facts,
        engines::now(deps) as i64,
    )?;
    let joined = super::super::consumers::media_routes(
        credentials
            .lock()
            .map_err(store_refusal)?
            .catalog_snapshot
            .as_ref(),
    );
    let identities: Vec<_> = joined
        .by_kind
        .get(request.kind.as_str())
        .into_iter()
        .flat_map(|kind| &kind.routes)
        .map(|route| (route.engine.provider.as_str(), route.engine.model.as_str()))
        .collect();
    let metric = request
        .requirements
        .engine
        .metric
        .as_deref()
        .or((request.kind.as_str() == "transcribe").then_some("wer"));
    let (mut evaluations, samples) = deps
        .store
        .engine_measurements(request.kind.as_str(), metric, &identities)
        .map_err(store_refusal)?;
    if let Some(limit) = request
        .requirements
        .metric_limit
        .as_ref()
        .filter(|limit| Some(limit.metric.as_str()) != metric)
    {
        evaluations.extend(
            deps.store
                .engine_measurements(request.kind.as_str(), Some(&limit.metric), &identities)
                .map_err(store_refusal)?
                .0,
        );
    }
    let deps = Deps {
        quota_cache: &quota,
        store: deps.store,
        broca_credentials: &credentials,
        routing_config_path: deps.routing_config_path,
        operator_config_path: deps.operator_config_path,
    };
    let snapshot_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut results = Vec::new();
    for iq in iqs.map_or_else(|| vec![None], |iqs| iqs.into_iter().map(Some).collect()) {
        let mut view = engines::MediaEvaluation {
            accounts: accounts.clone(),
            config: config.clone(),
            consumers: consumers.clone(),
            joined: joined.clone(),
            inputs: inputs.clone(),
            account_quota: account_quota.clone(),
            iq,
            evaluations: evaluations.clone(),
            samples: samples.clone(),
            evidence: json!({"iq":null, "bar":{}, "top_routable":{}, "adequate":[], "pick":null, "excluded":[]}),
        };
        let pick = engines::preview_media(&request, caller, &deps, &mut view);
        match pick {
            Ok(pick) => view.evidence["pick"] = pick,
            Err(refusal) => {
                view.evidence["refusal"] = json!({"code":refusal.code, "detail":refusal.detail})
            }
        }
        results.push(view.evidence);
    }
    Ok(
        json!({"consumer": request.consumer, "results":results, "snapshot_ms":snapshot_ms,
        "anchor_version":live.scoring_anchor_version(), "concurrency":concurrency.report(),
        "elapsed_ms":started.elapsed().as_secs_f64()*1000.0}),
    )
}
