//! Class-policy panels reuse the scorer, while each seat is a retained v1 select.

use std::collections::{BTreeSet, HashMap, HashSet};

use cortexkit_role_routing::{errors::Refusal, panel::*, select::SelectAnswer};
use prefrontal_routing_core::{fact_to_route, panel as scoring};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{operator_config, store_refusal, Caller, Deps};

// Replacement reads and writes the whole history. Serialize panel operations so
// concurrent requests cannot both replace a slot from the same old history.
static PANEL_WRITER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Serialize, Deserialize)]
struct StoredPanel {
    request: SelectPanelRequest,
    answer: SelectPanelAnswer,
    history: BTreeSet<String>,
}

pub(super) async fn select_panel(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let _writer = PANEL_WRITER.lock().await;
    if params.get("replace").is_some() {
        super::shapes::closed(params, "replace", &["panel_id", "slot"])?;
        let request = parse_panel_replacement_request(params)?;
        return replace(request, caller, deps).await;
    }
    super::shapes::settings(params)?;
    super::shapes::closed(params, "requirements", &["context_window", "modalities"])?;
    let request = parse_select_panel_request_serving(params, super::describe::CAPABILITIES)?;
    let config = operator_config(deps);
    super::consumers::effective(&config).admit(&request.consumer, caller)?;
    let class = class(&request)?;
    let policy = crate::panel_policy::class_policy(config.panels(), class);
    if !(1..=8).contains(&policy.width()) {
        return Err(Refusal::invalid_request(
            "class",
            "panel policy size must be 1-8",
        ));
    }
    let (panel, _) = candidates(&request, &[], &[], caller, deps).await?;
    let mut seats = Vec::new();
    let mut families = HashSet::new();
    for member in &panel.members {
        let family = family(member.family.as_deref());
        if families.contains(&family) {
            continue;
        }
        let slot = panel
            .policy
            .as_ref()
            .and_then(|p| p.seats.iter().find(|s| s.route_key == member.route_key))
            .map_or(seats.len() as u64, |seat| seat.slot as u64);
        if let Some(seat) =
            admit_seat(&request, member, slot, policy.top_k as u64, caller, deps).await?
        {
            families.insert(family);
            seats.push(seat);
        }
    }
    seats.sort_by_key(|seat| seat.slot);
    let mut answer = SelectPanelAnswer::new(
        "pending",
        PanelPolicy::new(
            policy.width() as u64,
            policy.top_k as u64,
            policy.extra as u64,
        ),
        seats,
    );
    answer.policy.ranking = Some(json!(policy.by));
    answer.policy.extra_ranking = policy.extra_by.map(|order| json!(order));
    answer.policy.floor = policy.floor.map(|floor| json!(floor));
    answer.policy.seed = request.seed.as_ref().map(|seed| json!(seed));
    if answer.seats.len() < policy.width() {
        let code = panel
            .shortfall
            .as_ref()
            .map(|s| match s.code {
                scoring::PanelShortfallCode::PackageUnmeetable => ShortfallCode::PackageUnmeetable,
                scoring::PanelShortfallCode::IqFloorUnmeetable => ShortfallCode::FloorUnmeetable,
                scoring::PanelShortfallCode::DiversityUnmeetable => {
                    ShortfallCode::DiversityUnmeetable
                }
                scoring::PanelShortfallCode::SizeReduced => ShortfallCode::SizeReduced,
            })
            .unwrap_or(ShortfallCode::DiversityUnmeetable);
        answer.shortfall = Some(PanelShortfall::new(code));
    }
    let mut state = StoredPanel {
        request,
        history: answer
            .seats
            .iter()
            .map(|s| s.answer.route.route_key())
            .collect(),
        answer,
    };
    let id = deps
        .store
        .v1_create_panel(
            &caller.principal,
            &serde_json::to_string(&state).map_err(store_refusal)?,
            super::now_ms() as i64,
        )
        .map_err(store_refusal)?;
    state.answer.panel_id = id.clone();
    deps.store
        .v1_update_panel(&id, &serde_json::to_string(&state).map_err(store_refusal)?)
        .map_err(store_refusal)?;
    serde_json::to_value(state.answer).map_err(store_refusal)
}

fn class(request: &SelectPanelRequest) -> Result<scoring::PanelClass, Refusal> {
    serde_json::from_value(json!(request.class.as_str().to_ascii_uppercase()))
        .map_err(|_| Refusal::invalid_request("class", "no policy for panel class"))
}

fn family(value: Option<&str>) -> String {
    value.unwrap_or("unknown").to_owned()
}

async fn settings(
    request: &SelectPanelRequest,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<crate::tier_settings::Layers, Refusal> {
    if let Some(settings) = &request.settings {
        settings.check_project_stamp(
            caller
                .scope_authority
                .as_ref()
                .and_then(|s| s.project_id.as_deref())
                .or_else(|| caller.scope.as_ref().and_then(|s| s.project_id.as_deref())),
        )?;
        if let Some(project) = &settings.project_id {
            return Ok(
                crate::project_settings::layers(project, deps.routing_config_path)
                    .await
                    .0,
            );
        }
    }
    Ok(crate::tier_settings::Layers::new(None, None))
}

async fn admit_seat(
    request: &SelectPanelRequest,
    member: &scoring::PanelMember,
    slot: u64,
    top: u64,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Option<PanelSeat>, Refusal> {
    let mut select = serde_json::to_value(request.seat_request(&[])).map_err(store_refusal)?;
    let mut route = cortexkit_role_routing::select::RouteRef::parse_pin(&member.route_key)
        .ok_or_else(|| store_refusal("scorer returned an invalid route"))?;
    if let Some(variant) = member.prompt_params.get("variant").and_then(Value::as_str) {
        route.variant = Some(variant.to_owned());
    }
    select["pin"] = json!(route.pin());
    // Panel policy, not the consumer's ordinary single-model quality defaults,
    // has already ranked and admitted this candidate.
    select["requirements"] = json!({"iq":0});
    if let Some(requirements) = &request.requirements {
        if let Some(window) = requirements.context_window {
            select["requirements"]["context_window"] = json!(window);
        }
        if let Some(modalities) = &requirements.modalities {
            select["requirements"]["modalities"] = json!(modalities);
        }
    }
    let answer = match super::select::panel_seat(&select, caller, deps).await {
        Ok(answer) => serde_json::from_value::<SelectAnswer>(answer).map_err(store_refusal)?,
        Err(refusal)
            if matches!(
                refusal.code.as_str(),
                "no_route"
                    | "pin_unavailable"
                    | "accounts_blocked"
                    | "account_hold"
                    | "auth_required"
            ) =>
        {
            return Ok(None)
        }
        Err(refusal) => return Err(refusal),
    };
    let mut seat = PanelSeat::new(
        slot,
        if slot < top {
            PanelPhase::Top
        } else {
            PanelPhase::Extra
        },
        answer,
    );
    seat.family = member.family.clone();
    seat.temperature = request
        .temperatures
        .as_ref()
        .and_then(|t| t.get(slot as usize))
        .copied();
    Ok(Some(seat))
}

async fn replace(
    request: PanelReplacementRequest,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    if !caller.binding_is_current() {
        return Err(Refusal::unknown_decision());
    }
    let id = &request.replace.panel_id;
    let raw = deps
        .store
        .v1_panel(id, &caller.principal)
        .map_err(store_refusal)?
        .ok_or_else(Refusal::unknown_decision)?;
    let mut state: StoredPanel = serde_json::from_str(&raw).map_err(store_refusal)?;
    let slot = request.replace.slot;
    if slot >= state.answer.policy.size {
        return Err(Refusal::invalid_request(
            "replace.slot",
            "slot outside panel policy",
        ));
    }
    let mut exclude: Vec<_> = state.history.iter().cloned().collect();
    exclude.extend(request.exclude.clone().unwrap_or_default());
    let families: Vec<_> = state
        .answer
        .seats
        .iter()
        .filter(|s| s.slot != slot)
        .map(|s| family(s.family.as_deref()))
        .collect();
    let (panel, package_blocked) =
        candidates(&state.request, &exclude, &families, caller, deps).await?;
    let mut answer = PanelReplacement::unseated(
        id,
        slot,
        if package_blocked {
            ReplacementCause::PackageUnmeetable
        } else {
            ReplacementCause::NoCandidate
        },
    );
    for member in &panel.members {
        if let Some(seat) = admit_seat(
            &state.request,
            member,
            slot,
            state.answer.policy.top,
            caller,
            deps,
        )
        .await?
        {
            state.history.insert(seat.answer.route.route_key());
            state.answer.seats.retain(|s| s.slot != slot);
            state.answer.seats.push(seat.clone());
            state.answer.seats.sort_by_key(|s| s.slot);
            answer = PanelReplacement::seated(id, seat);
            deps.store
                .v1_update_panel(id, &serde_json::to_string(&state).map_err(store_refusal)?)
                .map_err(store_refusal)?;
            break;
        }
    }
    serde_json::to_value(answer).map_err(store_refusal)
}

async fn candidates(
    request: &SelectPanelRequest,
    extra_exclude: &[String],
    standing_families: &[String],
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<(scoring::SelectedPanel, bool), Refusal> {
    let config = operator_config(deps);
    let live = crate::config_overrides::live_config(deps.routing_config_path);
    // Validate the requested project id against the caller's daemon-stamped
    // project before reading candidates, even if the panel will be empty.
    let layers = settings(request, caller, deps).await?;
    let mut prepared = crate::prepare_selection_inputs(
        deps.quota_cache,
        deps.store,
        live.pricing(),
        &live.quota_model_pools(),
    )
    .map_err(store_refusal)?;
    let aliases = deps
        .broca_credentials
        .lock()
        .map_err(store_refusal)?
        .catalog_targets(&prepared.facts);
    let consumers = super::consumers::effective(&config);
    let policy =
        config.consumer_policy(&request.consumer, &Default::default(), &consumers.policies);
    let metered = crate::select_policy::priced_per_token(
        &policy,
        crate::select_policy::Consumer::from_id(&request.consumer),
    );
    let sources = crate::SelectionPolicySources {
        live_config: &live,
        operator_config: &config,
        operator_config_path: deps.operator_config_path,
        catalog_targets: &aliases,
    };
    let mut priced = crate::price_selection_inputs(
        &mut prepared.facts,
        prepared.quota_signal_json.clone(),
        &sources,
        deps.store,
        deps.broca_credentials,
        true,
        metered,
    )
    .map_err(store_refusal)?;
    let registry = {
        let credentials = deps.broca_credentials.lock().map_err(store_refusal)?;
        super::runners::Registry::build(
            config.v1_runners(),
            &credentials,
            &prepared.facts,
            super::now_ms(),
        )
    };
    let runners: Vec<_> = registry
        .policies()
        .into_iter()
        .filter(|r| {
            request
                .runners
                .as_ref()
                .is_none_or(|names| names.contains(&r.module))
                && policy.runner.as_ref().is_none_or(|name| name == &r.module)
                && request.harness.as_deref().is_none_or(|h| {
                    registry
                        .get(&r.module)
                        .is_some_and(|r| r.harnesses.iter().any(|name| name == h))
                })
        })
        .collect();
    let tier_consumer = request
        .settings
        .as_ref()
        .and_then(|s| s.tier_consumer.as_deref())
        .unwrap_or(&request.consumer);
    if let Some(classes) = &mut priced.classes {
        classes.apply_configured_service_tiers(
            &mut prepared.facts,
            |fact| {
                layers.resolve(
                    &config,
                    tier_consumer,
                    &fact.provider_id,
                    &fact.model_id,
                    request.service_tier,
                )
            },
            &runners,
            &registry,
            metered,
        );
        classes.apply_eligibility(&mut prepared.snapshot, &prepared.facts, metered, false);
        classes.apply_legacy_route_fences(&mut prepared.snapshot, &prepared.facts, metered, false);
    }
    let required = request.requirements.as_ref();
    let mut facts = prepared.facts;
    facts.retain(|fact| {
        let route = fact_to_route(fact);
        required.and_then(|r| r.context_window).is_none_or(|min| {
            fact.context_window
                .is_some_and(|window| window > 0.0 && window >= min as f64)
        }) && required
            .and_then(|r| r.modalities.as_ref())
            .is_none_or(|modalities| {
                modalities.iter().all(|m| match m.as_str() {
                    "text" => true,
                    "image" => fact.supports_vision == Some(true),
                    _ => false,
                })
            })
            && !standing_families.contains(&family(fact.family.as_deref()))
            && request
                .available_routes
                .as_ref()
                .is_none_or(|routes| routes.contains(&route.capacity_key))
            && runners
                .iter()
                .any(|r| registry.runs(&r.module, &route.route_key, &route.capacity_key))
    });
    // Zero is an unknown output limit in the public budget rules, not a zero
    // reserve. Preserve the raw fact in the select answer, but score packages
    // with the same unknown-capacity reserve as budget::quote.
    for fact in &mut facts {
        if fact.max_output_tokens == Some(0) {
            fact.max_output_tokens = None;
        }
    }
    let class = class(request)?;
    let panel_policy = crate::panel_policy::class_policy(config.panels(), class);
    let groups = config.exclude_groups();
    let group = if class == scoring::PanelClass::Lab {
        &groups.lab
    } else {
        &groups.athena
    };
    let mut operator_exclude =
        crate::panel_policy::operator_exclusions(&facts, &config.operator_policy().exclude, group);
    operator_exclude.extend(
        facts
            .iter()
            .filter(|f| {
                !config.route_allowed(&request.consumer, &fact_to_route(f).route_key, false)
            })
            .map(|f| fact_to_route(f).route_key),
    );
    let mut exclude = request.exclude.clone().unwrap_or_default();
    exclude.extend_from_slice(extra_exclude);
    let available: Vec<_> = facts.iter().map(|f| fact_to_route(f).route_key).collect();
    let resolved = crate::resolve_routing_policy_with_catalog_targets(
        available.iter().cloned(),
        &config.operator_policy(),
        &Default::default(),
        &crate::provider_auth_types(None),
        &aliases,
    );
    let subscription: HashSet<_> = resolved.subscription_keys.into_iter().collect();
    let context = scoring::PanelSelectionContext {
        seed: request.seed.clone().unwrap_or_default(),
        pins: scoring::PanelPins {
            synthesizer: config
                .slot_pin("synthesizer")
                .map(|pin| scoring::PanelSynthesizerPin {
                    model: pin.model.clone(),
                    variant: pin.variant.clone(),
                }),
        },
        canonical_ids: crate::panel_canonical_ids(deps.store, &facts, &aliases)
            .map_err(store_refusal)?,
    };
    let families: HashMap<_, _> = facts
        .iter()
        .map(|fact| (fact_to_route(fact).capacity_key, fact.family.clone()))
        .collect();
    let outcome = live.apply(&mut facts);
    let mut panel = scoring::select_panel_with_availability(
        scoring::SelectPanelInput {
            package_tokens: request.package_tokens.map(|tokens| tokens as usize),
            panel_spec: scoring::PanelSpec {
                class,
                size: panel_policy.width(),
                iq_floor: 0.0,
                diversity: scoring::PanelDiversity::Family,
                temperature_spread: request.temperatures.clone(),
            },
            selection: scoring::PanelSelection::NPick,
            roster: Vec::new(),
            facts,
            runtime_snapshot: Some(prepared.snapshot),
            exclude_route_keys: exclude,
            operator_exclude_keys: operator_exclude,
            required_capabilities: None,
            available_models: available.into_iter().collect(),
            availability_substrate: scoring::AvailabilitySubstrate::Broca,
            availability_label: crate::legacy_wire::availability_label(
                scoring::AvailabilitySubstrate::Broca,
            )
            .to_owned(),
            is_subscription_route: Some(Box::new(|route| {
                subscription.contains(&route.route_key)
                    || subscription.contains(&route.capacity_key)
            })),
        },
        &outcome.routing_config,
        &panel_policy,
        &context,
        &HashMap::new(),
    )
    .map_err(|e| Refusal::invalid_request("class", e.0))?;
    // The scorer groups unlabeled lineages as `other`; the public answer keeps
    // the catalog's absence rather than inventing a family label for it.
    for member in &mut panel.members {
        if let Some(route) = cortexkit_role_routing::select::RouteRef::parse_pin(&member.route_key)
        {
            member.family = families.get(&route.route_key()).cloned().flatten();
        }
    }
    let package_blocked = panel
        .shortfall
        .as_ref()
        .is_some_and(|s| s.code == scoring::PanelShortfallCode::PackageUnmeetable);
    let models: HashMap<_, _> = prepared_models(&panel);
    let variant_consumer = crate::select_policy::Consumer::from_id(&request.consumer).unwrap_or(
        if class == scoring::PanelClass::Lab {
            crate::select_policy::Consumer::Lab
        } else {
            crate::select_policy::Consumer::Athena
        },
    );
    crate::select::apply_panel_variant_policy(
        &mut panel,
        &config,
        deps.operator_config_path,
        &aliases,
        variant_consumer,
        &models,
    );
    Ok((panel, package_blocked))
}

fn prepared_models(panel: &scoring::SelectedPanel) -> HashMap<String, (String, String)> {
    panel
        .members
        .iter()
        .filter_map(|m| {
            m.route_key.split_once('/').map(|(provider, model)| {
                (m.route_key.clone(), (provider.to_owned(), model.to_owned()))
            })
        })
        .collect()
}
