//! Operator-only, call-local evaluation of the live v1 selection pipeline.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cortexkit_role_routing::errors::Refusal;
use cortexkit_role_routing::panel::{
    parse_select_panel_request_serving, PanelPhase, SelectPanelRequest,
};
use serde::Deserialize;
use serde_json::{json, Value};
use striatum_core::panel::{ClassPanelPolicy, PanelExtraOrder, PanelOrder};

use super::{operator_config, provider_refusal, select, store_refusal, Caller, Deps};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    consumer: String,
    iqs: Option<Vec<f64>>,
    eq: Option<f64>,
    runner: Option<String>,
    policy_patch: Option<PolicyPatch>,
    pricing_patch: Option<crate::config_overrides::PricingPatch>,
    runner_declarations: Option<BTreeMap<String, Value>>,
    head_choices: Option<Value>,
    panel: Option<PanelParams>,
}

/// A dry-run panel. Member values are passed to `route.select_panel`'s own
/// request parser, so they are validated and refused exactly as there.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PanelParams {
    class: PanelClasses,
    package_tokens: Option<Value>,
    runners: Option<Value>,
    seed: Option<Value>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PanelClasses {
    One(String),
    Many(Vec<String>),
}

/// More would only repeat a class: routing has seven.
const MAX_PANEL_CLASSES: usize = 16;

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
    let mut params: Params = serde_json::from_value(params.clone())
        .map_err(|error| Refusal::invalid_request("params", error.to_string()))?;
    // A panel-only call may omit the IQ sweep; a supplied sweep is never empty.
    let iqs = params.iqs.take();
    if iqs.as_ref().map_or(params.panel.is_none(), |iqs| {
        iqs.is_empty()
            || iqs.len() > 101
            || iqs
                .iter()
                .any(|iq| !iq.is_finite() || !(0.0..=100.0).contains(iq))
    }) {
        return Err(Refusal::invalid_request(
            "iqs",
            "iqs must contain 1-101 finite numbers in 0..100 (it may be omitted only with panel)",
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
        .is_some_and(|runner| runner != crate::legacy_wire::BROCA)
    {
        return Err(Refusal::invalid_request(
            "runner",
            format!("runner must be {} when supplied", crate::legacy_wire::BROCA),
        ));
    }
    let started = std::time::Instant::now();
    let declarations = params
        .runner_declarations
        .unwrap_or_default()
        .into_iter()
        .map(|(module, slot)| {
            let field = format!("runner_declarations.{module}");
            let declaration = super::runners::parse_declaration(&slot)
                .map_err(|error| Refusal::invalid_request(&field, error))?
                .ok_or_else(|| Refusal::invalid_request(&field, "slot must contain runner"))?;
            Ok((module, declaration))
        })
        .collect::<Result<BTreeMap<_, _>, Refusal>>()?;
    let live = crate::config_overrides::live_config(deps.routing_config_path);
    let operator = operator_config(deps);
    let patch = params.policy_patch.unwrap_or_default();
    let pricing = params.pricing_patch.unwrap_or_default();
    let live = Arc::new(
        live.with_model_policy_patch(&patch.models)
            .map_err(|error| Refusal::invalid_request("policy_patch", error))?
            .with_pricing_patch(&pricing)
            .map_err(|error| Refusal::invalid_request("pricing_patch", error))?,
    );
    let operator = Arc::new(
        operator
            .with_model_policy_patch(&patch.models)
            .map_err(|error| Refusal::invalid_request("policy_patch", error))?
            .with_pricing_patch(&pricing)
            .map_err(|error| Refusal::invalid_request("pricing_patch", error))?,
    );
    if !super::consumers::effective(&operator).knows(&params.consumer) {
        return Err(Refusal::invalid_request(
            "consumer",
            "consumer is not registered",
        ));
    }
    let panels = params
        .panel
        .map(|panel| panel_requests(panel, &params.consumer, params.runner.as_deref(), &operator))
        .transpose()?
        .unwrap_or_default();
    // Keep declaration overlays and all credential reads on a disposable copy.
    // Never publish the proposed slot or copy reconciled state back to the cache.
    let mut credentials = deps
        .broca_credentials
        .lock()
        .map_err(store_refusal)?
        .clone();
    credentials.runner_declarations.extend(declarations);
    let credentials = Arc::new(Mutex::new(credentials));
    let deps = Deps {
        broca_credentials: &credentials,
        quota_cache: deps.quota_cache,
        store: deps.store,
        routing_config_path: deps.routing_config_path,
        operator_config_path: deps.operator_config_path,
    };
    let head_choices = params
        .head_choices
        .as_ref()
        .map(|request| super::reads::head_choices_with_config(request, &deps, &operator))
        .transpose()?;
    // Account loading heals stale evidence. Heal only a disposable projection,
    // copied once for the whole sweep; no decision or usage history is copied.
    let account_store = Arc::new(
        deps.store
            .account_evaluation_snapshot(super::now_ms() as i64)
            .map_err(store_refusal)?,
    );
    let snapshot_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut results = Vec::with_capacity(iqs.as_ref().map_or(0, Vec::len));
    for iq in iqs.unwrap_or_default() {
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
        let result = select::evaluate(&request, caller, &deps, &view, profile_lookup).await;
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
    let mut result =
        json!({"consumer": params.consumer, "results": results, "snapshot_ms": snapshot_ms,
            "anchor_version": live.scoring_anchor_version()});
    if !panels.is_empty() {
        let dry = super::panels::DryRun {
            live_config: live.clone(),
            operator_config: operator.clone(),
            account_store: &account_store,
            profile_lookup,
        };
        let mut rows = Vec::with_capacity(panels.len());
        for panel in panels {
            let evaluated =
                super::panels::evaluate_panel(&panel.request, panel.policy, caller, &deps, &dry)
                    .await;
            rows.push(panel_row(&panel, evaluated));
        }
        result["panels"] = json!(rows);
    }
    result["elapsed_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
    if let Some(choices) = head_choices {
        result["head_choices"] = choices;
    }
    Ok(result)
}

/// One class's dry-run panel, validated before any snapshot is taken.
struct PanelRequest {
    class: String,
    request: SelectPanelRequest,
    policy: ClassPanelPolicy,
    /// Whether operator settings name this class's policy, rather than the
    /// class's built-in default.
    configured: bool,
}

/// Build each class's request through `route.select_panel`'s parser and class
/// policy lookup, so malformed members and unknown classes are refused the same way.
fn panel_requests(
    panel: PanelParams,
    consumer: &str,
    runner: Option<&str>,
    operator: &crate::config_overrides::LiveConfig,
) -> Result<Vec<PanelRequest>, Refusal> {
    let classes = match panel.class {
        PanelClasses::One(class) => vec![class],
        PanelClasses::Many(classes) => classes,
    };
    if classes.is_empty() || classes.len() > MAX_PANEL_CLASSES {
        return Err(Refusal::invalid_request(
            "panel.class",
            format!("panel.class must name a class or list 1-{MAX_PANEL_CLASSES} classes"),
        ));
    }
    classes
        .into_iter()
        .map(|class| {
            let mut params = json!({"class": class, "consumer": consumer});
            if let Some(tokens) = &panel.package_tokens {
                params["package_tokens"] = tokens.clone();
            }
            // The panel's own runner list wins; otherwise the sweep's runner applies.
            match (&panel.runners, runner) {
                (Some(runners), _) => params["runners"] = runners.clone(),
                (None, Some(runner)) => params["runners"] = json!([runner]),
                (None, None) => {}
            }
            if let Some(seed) = &panel.seed {
                params["seed"] = seed.clone();
            }
            let request =
                parse_select_panel_request_serving(&params, super::describe::CAPABILITIES)?;
            let policy = super::panels::request_policy(&request, operator)?;
            let configured = crate::panel_policy::configured_class_policy(
                operator.panels(),
                super::panels::class(&request)?,
            )
            .is_some();
            Ok(PanelRequest {
                class,
                request,
                policy,
                configured,
            })
        })
        .collect()
}

fn panel_row(
    panel: &PanelRequest,
    evaluated: Result<super::panels::PanelEvaluation, Refusal>,
) -> Value {
    let policy = panel.policy;
    // An unseeded `route.select_panel` draws random extras with the empty seed;
    // the dry run defaults to the same one so the two agree.
    let (seed, seed_source) = match &panel.request.seed {
        Some(seed) => (seed.as_str(), "request"),
        None => ("", "default"),
    };
    let mut row = json!({
        "class": panel.class,
        "policy": {
            "source": if panel.configured { "settings" } else { "default" },
            "top_k": policy.top_k,
            "by": policy.by,
            "extra": policy.extra,
            "extra_by": policy.extra_by,
            "floor": policy.floor,
            "width": policy.width(),
        },
        "seed": seed,
        "seed_source": seed_source,
        "seats": [],
        "shortfall": null,
        "funnel": null,
    });
    match evaluated {
        Ok(evaluated) => {
            row["seats"] = evaluated
                .seats
                .iter()
                .map(|seat| {
                    json!({
                        "slot": seat.slot,
                        "phase": seat.phase,
                        "route": seat.route,
                        "variant": seat.variant,
                        "family": seat.family,
                        "rank": seat.rank,
                        "cost_usd": seat.cost_usd,
                        "chosen_by": chosen_by(seat, policy, seed),
                    })
                })
                .collect();
            row["shortfall"] = json!(evaluated.shortfall);
            row["funnel"] = json!(evaluated.funnel);
        }
        Err(refusal) => {
            row["refusal"] = json!({"code": refusal.code, "detail": refusal.detail});
        }
    }
    row
}

/// Plain-language account of the class-policy rule that seated a route.
fn chosen_by(seat: &super::panels::EvaluatedSeat, policy: ClassPanelPolicy, seed: &str) -> String {
    let name = |order: Value| order.as_str().unwrap_or_default().to_owned();
    let rank = seat
        .rank
        .map_or_else(String::new, |rank| format!(" ranked {rank}"));
    let (rule, floor_applies) = if seat.phase == PanelPhase::Top {
        (
            format!(
                "top-{} seat{rank} by {}",
                policy.top_k,
                name(json!(policy.by))
            ),
            policy.by != PanelOrder::Iq,
        )
    } else if policy.extra_by == Some(PanelExtraOrder::Random) {
        (
            format!("extra seat drawn at random with seed {seed:?} from families not yet seated"),
            true,
        )
    } else {
        (
            format!(
                "extra seat{rank} by {} among families not yet seated",
                name(json!(policy.extra_by))
            ),
            true,
        )
    };
    match policy.floor {
        Some(floor) if floor_applies => format!("{rule}; clears class floor {floor}"),
        Some(floor) => {
            format!("{rule}; class floor {floor} does not apply to IQ-ranked top seats")
        }
        None => rule,
    }
}
