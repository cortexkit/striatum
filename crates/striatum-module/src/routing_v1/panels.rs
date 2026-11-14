//! Class-policy panels reuse the scorer, while each seat is a retained v1 select.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use cortexkit_role_routing::{errors::Refusal, panel::*, select::SelectAnswer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use striatum_core::{fact_to_route, panel as scoring};

use super::{operator_config, store_refusal, Caller, Deps};

// Replacement reads and writes the whole history. Only replacements of the
// same panel must wait for one another; creation and unrelated panels proceed.
static REPLACEMENT_LOCKS: LazyLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) fn replacement_lock(id: &str) -> Result<Arc<tokio::sync::Mutex<()>>, Refusal> {
    let mut locks = REPLACEMENT_LOCKS.lock().map_err(store_refusal)?;
    if let Some(lock) = locks.get(id).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    // Idle panels own no lock. Reclaim their weak entries when another panel
    // first needs a lock, rather than retaining every panel id indefinitely.
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(id.to_owned(), Arc::downgrade(&lock));
    Ok(lock)
}

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
    if params.get("replace").is_some() {
        super::shapes::closed(params, "replace", &["panel_id", "slot"])?;
        let request = parse_panel_replacement_request(params)?;
        return replace(request, caller, deps).await;
    }
    super::shapes::settings(params)?;
    super::shapes::closed(params, "requirements", &["context_window", "modalities"])?;
    let request = parse_select_panel_request_serving(params, &super::describe::capabilities())?;
    let config = operator_config(deps);
    super::consumers::effective(&config).admit(&request.consumer, caller)?;
    let policy = request_policy(&request, &config)?;
    if request.estimate == Some(true) {
        // Account loading can reconcile stale evidence. Keep those writes on a
        // disposable snapshot, and never reserve a live probe or recovery test.
        let account_store = Arc::new(
            deps.store
                .account_evaluation_snapshot(super::now_ms() as i64)
                .map_err(store_refusal)?,
        );
        let dry = DryRun {
            live_config: crate::config_overrides::live_config(deps.routing_config_path),
            operator_config: config,
            account_store: &account_store,
            profile_lookup: &super::select::live_profile_lookup,
        };
        let evaluated = evaluate_panel(&request, policy, caller, deps, &dry).await?;
        let seats = evaluated
            .seats
            .into_iter()
            .map(|seat| {
                let mut estimated = PanelEstimateSeat::new(seat.slot, seat.phase, seat.answer);
                estimated.family = seat.family;
                estimated.temperature = request
                    .temperatures
                    .as_ref()
                    .and_then(|values| values.get(seat.slot as usize).copied());
                estimated
            })
            .collect();
        let mut answer = SelectPanelEstimate::new(answer_policy(&request, policy), seats);
        answer.shortfall = evaluated.shortfall;
        return serde_json::to_value(answer).map_err(store_refusal);
    }
    let (_, seats, shortfall) = seat_panel(&request, policy, caller, deps, None).await?;
    // A live pass admits every seat through `route.select`, so each is `Live`.
    let seats = seats
        .into_iter()
        .filter_map(|seat| match seat {
            Seat::Live(seat) => Some(seat),
            Seat::Evaluated(_) => None,
        })
        .collect();
    let mut answer = SelectPanelAnswer::new(
        striatum_store::new_decision_id(),
        answer_policy(&request, policy),
        seats,
    );
    answer.shortfall = shortfall;
    let state = StoredPanel {
        request,
        history: answer
            .seats
            .iter()
            .map(|s| s.answer.route.route_key())
            .collect(),
        answer,
    };
    deps.store
        .v1_create_panel(
            &state.answer.panel_id,
            &caller.principal,
            &serde_json::to_string(&state).map_err(store_refusal)?,
            super::now_ms() as i64,
        )
        .map_err(store_refusal)?;
    serde_json::to_value(state.answer).map_err(store_refusal)
}

fn answer_policy(request: &SelectPanelRequest, policy: scoring::ClassPanelPolicy) -> PanelPolicy {
    let mut answer = PanelPolicy::new(
        policy.width() as u64,
        policy.top_k as u64,
        policy.extra as u64,
    );
    answer.ranking = Some(json!(policy.by));
    answer.extra_ranking = policy.extra_by.map(|order| json!(order));
    answer.floor = policy.floor.map(|floor| json!(floor));
    answer.seed = request.seed.as_ref().map(|seed| json!(seed));
    answer
}

/// The class policy a new panel runs under, refusing a class routing has no
/// policy for and a configured width outside 1-8.
pub(super) fn request_policy(
    request: &SelectPanelRequest,
    config: &crate::config_overrides::LiveConfig,
) -> Result<scoring::ClassPanelPolicy, Refusal> {
    let class = class(request)?;
    let policy = crate::panel_policy::class_policy(config.panels(), class);
    if !(1..=8).contains(&policy.width()) {
        return Err(Refusal::invalid_request(
            "class",
            "panel policy size must be 1-8",
        ));
    }
    Ok(policy)
}

/// Call-local inputs for read-only panel sizing or an operator what-if:
/// configuration and a disposable account-evidence store. Seats are checked by
/// select's evaluation path, which returns before recording a decision, taking
/// a hold or acquiring a pool key, and no panel row is written.
pub(super) struct DryRun<'a> {
    pub live_config: Arc<crate::config_overrides::LiveConfig>,
    pub operator_config: Arc<crate::config_overrides::LiveConfig>,
    pub account_store: &'a crate::SharedStore,
    pub profile_lookup: &'a super::select::ProfileLookup,
}

/// A seat a dry-run panel would fill. No decision backs it.
pub(super) struct EvaluatedSeat {
    pub slot: u64,
    pub phase: PanelPhase,
    pub route: Value,
    pub variant: Value,
    pub cost_usd: Value,
    pub answer: PanelEstimateSelection,
    pub family: Option<String>,
    /// The scorer's rank within the seat's phase; absent for a random extra draw.
    pub rank: Option<usize>,
}

/// What a dry-run panel would seat, with the scorer's admission evidence.
pub(super) struct PanelEvaluation {
    pub seats: Vec<EvaluatedSeat>,
    pub shortfall: Option<PanelShortfall>,
    pub funnel: Option<scoring::PanelCandidateFunnel>,
}

/// Seat a new panel exactly as live selection does, against call-local
/// configuration and an account snapshot, and record nothing.
pub(super) async fn evaluate_panel(
    request: &SelectPanelRequest,
    policy: scoring::ClassPanelPolicy,
    caller: &Caller,
    deps: &Deps<'_>,
    dry: &DryRun<'_>,
) -> Result<PanelEvaluation, Refusal> {
    let (panel, seats, shortfall) = seat_panel(request, policy, caller, deps, Some(dry)).await?;
    Ok(PanelEvaluation {
        // A dry pass checks every seat through select's evaluation path.
        seats: seats
            .into_iter()
            .filter_map(|seat| match seat {
                Seat::Evaluated(seat) => Some(seat),
                Seat::Live(_) => None,
            })
            .collect(),
        shortfall,
        funnel: panel.policy.and_then(|policy| policy.funnel),
    })
}

enum Seat {
    Live(PanelSeat),
    Evaluated(EvaluatedSeat),
}

impl Seat {
    fn slot(&self) -> u64 {
        match self {
            Seat::Live(seat) => seat.slot,
            Seat::Evaluated(seat) => seat.slot,
        }
    }
}

/// Rank, dedupe by family and admit every scored member, in slot order. A
/// shortfall is returned when fewer seats than the policy's width were filled.
async fn seat_panel(
    request: &SelectPanelRequest,
    policy: scoring::ClassPanelPolicy,
    caller: &Caller,
    deps: &Deps<'_>,
    dry: Option<&DryRun<'_>>,
) -> Result<(scoring::SelectedPanel, Vec<Seat>, Option<PanelShortfall>), Refusal> {
    let shared = panel_inputs(deps, dry)?;
    let (panel, _, mut rejected, canonical_ids) =
        candidates(request, &[], &[], caller, deps, dry, &shared).await?;
    let mut seats = Vec::new();
    let mut families = HashSet::new();
    let mut models = HashSet::new();
    let mut attempted = HashSet::new();
    let mut vacancies = Vec::new();
    for member in &panel.members {
        attempted.insert(member.route_key.clone());
        let family = family(member.family.as_deref());
        if families.contains(&family) {
            let mut cause = PanelRejectedCandidate::new(&member.route_key, "family_duplicate");
            cause.detail = Some(json!(format!(
                "a seated route already represents family {family}"
            )));
            rejected.push(cause);
            continue;
        }
        let scored = panel
            .policy
            .as_ref()
            .and_then(|p| p.seats.iter().find(|s| s.route_key == member.route_key));
        let slot = scored.map_or(seats.len() as u64, |seat| seat.slot as u64);
        let top = policy.top_k as u64;
        let admission = match dry {
            None => admit_seat(request, member, slot, top, caller, deps, &shared)
                .await?
                .map(Seat::Live),
            Some(dry) => {
                let rank = scored.and_then(|seat| seat.rank);
                evaluate_seat(request, member, slot, top, rank, caller, deps, dry, &shared)
                    .await?
                    .map(Seat::Evaluated)
            }
        };
        match admission {
            SeatAdmission::Seated(seat) => {
                families.insert(family);
                models.extend(
                    canonical_ids
                        .get(&member.route_key)
                        .filter(|id| !id.trim().is_empty())
                        .cloned(),
                );
                seats.push(seat);
            }
            SeatAdmission::Rejected(cause) => {
                if concurrency_rejection(&cause) {
                    vacancies.push(slot);
                }
                rejected.push(cause);
            }
        }
    }
    // Keep successful original seats before filling holes: a spare must not
    // displace a later seat's family or canonical model. The scorer already
    // dropped spares below the panel class's minimum quality grade and spares
    // whose context window can't hold the evidence package, so only family
    // and canonical-model uniqueness are checked here.
    for slot in vacancies {
        for spare in &panel.runner_up {
            if spare.phase
                != if slot < policy.top_k as u64 {
                    scoring::PanelPhase::TopK
                } else {
                    scoring::PanelPhase::Extra
                }
                || !attempted.insert(spare.route_key.clone())
                || families.contains(&family(spare.family.as_deref()))
                || canonical_ids
                    .get(&spare.route_key)
                    .is_some_and(|id| !id.trim().is_empty() && models.contains(id))
            {
                continue;
            }
            let member = scoring::PanelMember {
                route_key: spare.route_key.clone(),
                prompt_params: spare.prompt_params.clone(),
                family: spare.family.clone(),
                temperature: None,
                max_output_tokens: spare.max_output_tokens,
                context_window: spare.context_window,
            };
            let admission = match dry {
                None => admit_seat(
                    request,
                    &member,
                    slot,
                    policy.top_k as u64,
                    caller,
                    deps,
                    &shared,
                )
                .await?
                .map(Seat::Live),
                Some(dry) => evaluate_seat(
                    request,
                    &member,
                    slot,
                    policy.top_k as u64,
                    None,
                    caller,
                    deps,
                    dry,
                    &shared,
                )
                .await?
                .map(Seat::Evaluated),
            };
            match admission {
                SeatAdmission::Seated(seat) => {
                    families.insert(family(member.family.as_deref()));
                    models.extend(
                        canonical_ids
                            .get(&member.route_key)
                            .filter(|id| !id.trim().is_empty())
                            .cloned(),
                    );
                    seats.push(seat);
                    break;
                }
                SeatAdmission::Rejected(cause) => rejected.push(cause),
            }
        }
    }
    seats.sort_by_key(Seat::slot);
    let shortfall = (seats.len() < policy.width()).then(|| {
        panel_shortfall(
            panel.shortfall.as_ref(),
            seats.len(),
            policy.width(),
            request.package_tokens,
            rejected,
        )
    });
    Ok((panel, seats, shortfall))
}

fn concurrency_rejection(cause: &PanelRejectedCandidate) -> bool {
    cause.detail.as_ref().is_some_and(|detail| {
        detail["code"] == "no_route" && detail["detail"]["reason"] == "concurrency_full"
    })
}

fn panel_shortfall(
    scored: Option<&scoring::PanelShortfall>,
    seated: usize,
    required: usize,
    package_tokens: Option<u64>,
    rejected: Vec<PanelRejectedCandidate>,
) -> PanelShortfall {
    let code = scored
        .map(|s| match s.code {
            scoring::PanelShortfallCode::PackageUnmeetable => ShortfallCode::PackageUnmeetable,
            scoring::PanelShortfallCode::IqFloorUnmeetable => ShortfallCode::FloorUnmeetable,
            scoring::PanelShortfallCode::DiversityUnmeetable => ShortfallCode::DiversityUnmeetable,
            scoring::PanelShortfallCode::SizeReduced => ShortfallCode::SizeReduced,
        })
        .unwrap_or(ShortfallCode::SizeReduced);
    let message = match scored {
            Some(scored) => format!(
                "{} Final admission seated {} of {} requested slots.",
                scored.detail, seated, required
            ),
            None => format!(
                "Final runner or credential admission seated only {} of {} requested slots; see candidate causes.",
                seated, required
            ),
        };
    let mut detail = PanelShortfallDetail::new(message);
    detail.package_tokens = package_tokens;
    detail.causes = rejected
        .into_iter()
        .filter(|cause| cause.reason.as_str() != "decommissioned")
        .collect();
    PanelShortfall::new(code).with_typed_detail(detail)
}

pub(super) fn class(request: &SelectPanelRequest) -> Result<scoring::PanelClass, Refusal> {
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

enum SeatAdmission<S = PanelSeat> {
    Seated(S),
    Rejected(PanelRejectedCandidate),
}

impl<S> SeatAdmission<S> {
    fn map<T>(self, seat: impl FnOnce(S) -> T) -> SeatAdmission<T> {
        match self {
            SeatAdmission::Seated(seated) => SeatAdmission::Seated(seat(seated)),
            SeatAdmission::Rejected(cause) => SeatAdmission::Rejected(cause),
        }
    }
}

/// The pinned select request that checks one scored member, and its pin.
fn seat_select(
    request: &SelectPanelRequest,
    member: &scoring::PanelMember,
) -> Result<(Value, cortexkit_role_routing::select::RouteRef), Refusal> {
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
    Ok((select, route))
}

/// A seat refusal that leaves the slot to the next member, or `None` when the
/// refusal must fail the whole panel.
fn seat_rejection(
    route: &cortexkit_role_routing::select::RouteRef,
    refusal: &Refusal,
) -> Option<PanelRejectedCandidate> {
    if !matches!(
        refusal.code,
        cortexkit_role_routing::errors::RefusalCode::NoRoute
            | cortexkit_role_routing::errors::RefusalCode::PinUnavailable
            | cortexkit_role_routing::errors::RefusalCode::AccountHold
            | cortexkit_role_routing::errors::RefusalCode::AuthRequired
    ) {
        return None;
    }
    let mut cause = PanelRejectedCandidate::new(route.pin(), refusal.code.as_str());
    cause.reset_at_ms = refusal.detail.get("reset_at_ms").and_then(Value::as_u64);
    cause.detail = Some(json!(refusal));
    Some(cause)
}

fn seat_phase(slot: u64, top: u64) -> PanelPhase {
    if slot < top {
        PanelPhase::Top
    } else {
        PanelPhase::Extra
    }
}

async fn admit_seat(
    request: &SelectPanelRequest,
    member: &scoring::PanelMember,
    slot: u64,
    top: u64,
    caller: &Caller,
    deps: &Deps<'_>,
    shared: &super::select::RequestInputs,
) -> Result<SeatAdmission, Refusal> {
    let (select, route) = seat_select(request, member)?;
    let answer = match super::select::panel_seat(&select, caller, deps, shared).await {
        Ok(answer) => serde_json::from_value::<SelectAnswer>(answer).map_err(store_refusal)?,
        Err(refusal) => {
            return seat_rejection(&route, &refusal)
                .map(SeatAdmission::Rejected)
                .ok_or(refusal)
        }
    };
    let mut seat = PanelSeat::new(slot, seat_phase(slot, top), answer);
    seat.family = member.family.clone();
    seat.temperature = request
        .temperatures
        .as_ref()
        .and_then(|t| t.get(slot as usize))
        .copied();
    Ok(SeatAdmission::Seated(seat))
}

/// `admit_seat` for a dry run: the same pinned select request, checked by
/// select's evaluation path against the dry run's configuration and accounts.
#[allow(clippy::too_many_arguments)]
async fn evaluate_seat(
    request: &SelectPanelRequest,
    member: &scoring::PanelMember,
    slot: u64,
    top: u64,
    rank: Option<usize>,
    caller: &Caller,
    deps: &Deps<'_>,
    dry: &DryRun<'_>,
    shared: &super::select::RequestInputs,
) -> Result<SeatAdmission<EvaluatedSeat>, Refusal> {
    let (select, route) = seat_select(request, member)?;
    // The seat request's requirements are the inert `iq: 0`; the evaluation's
    // demand must match it, as the live seat check reads it from the request.
    let view = super::select::Evaluation {
        live_config: dry.live_config.clone(),
        operator_config: dry.operator_config.clone(),
        account_store: dry.account_store,
        iq: 0.0,
        eq: None,
        evidence: Mutex::new(Default::default()),
    };
    let answer = match super::select::evaluate_with_inputs(
        &select,
        caller,
        deps,
        &view,
        dry.profile_lookup,
        shared,
        true,
    )
    .await
    {
        Ok(answer) => answer,
        Err(refusal) => {
            return seat_rejection(&route, &refusal)
                .map(SeatAdmission::Rejected)
                .ok_or(refusal)
        }
    };
    Ok(SeatAdmission::Seated(EvaluatedSeat {
        slot,
        phase: seat_phase(slot, top),
        route: answer["route"].clone(),
        variant: answer["variant"].clone(),
        cost_usd: answer["cost_usd"].clone(),
        answer: serde_json::from_value(answer["selection"].clone()).map_err(store_refusal)?,
        family: member.family.clone(),
        rank,
    }))
}

async fn replace(
    request: PanelReplacementRequest,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let id = &request.replace.panel_id;
    let lock = replacement_lock(id)?;
    let _writer = lock.lock().await;
    if !caller.binding_is_current() {
        return Err(Refusal::unknown_decision());
    }
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
    let shared = panel_inputs(deps, None)?;
    let (panel, package_blocked, mut rejected, _) = candidates(
        &state.request,
        &exclude,
        &families,
        caller,
        deps,
        None,
        &shared,
    )
    .await?;
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
        match admit_seat(
            &state.request,
            member,
            slot,
            state.answer.policy.top,
            caller,
            deps,
            &shared,
        )
        .await?
        {
            SeatAdmission::Rejected(cause) => rejected.push(cause),
            SeatAdmission::Seated(seat) => {
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
    }
    if answer.cause.is_some() {
        answer.detail = panel_shortfall(
            panel.shortfall.as_ref(),
            0,
            1,
            state.request.package_tokens,
            rejected,
        )
        .detail;
    }
    serde_json::to_value(answer).map_err(store_refusal)
}

fn panel_inputs(
    deps: &Deps<'_>,
    dry: Option<&DryRun<'_>>,
) -> Result<super::select::RequestInputs, Refusal> {
    super::select::RequestInputs::read_with_config(
        deps,
        dry.map_or_else(
            || crate::config_overrides::live_config(deps.routing_config_path),
            |dry| dry.live_config.clone(),
        ),
        dry.map_or_else(|| operator_config(deps), |dry| dry.operator_config.clone()),
    )
}

async fn candidates(
    request: &SelectPanelRequest,
    extra_exclude: &[String],
    standing_families: &[String],
    caller: &Caller,
    deps: &Deps<'_>,
    dry: Option<&DryRun<'_>>,
    shared: &super::select::RequestInputs,
) -> Result<
    (
        scoring::SelectedPanel,
        bool,
        Vec<PanelRejectedCandidate>,
        HashMap<String, String>,
    ),
    Refusal,
> {
    let config = &shared.operator_config;
    let live = &shared.live_config;
    // Validate the requested project id against the caller's daemon-stamped
    // project before reading candidates, even if the panel will be empty.
    let layers = settings(request, caller, deps).await?;
    let mut prepared = shared.prepare(deps)?;
    let aliases = &shared.catalog_targets;
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
    // A dry run reads accounts from its disposable snapshot and reserves no
    // probe, exactly as select's evaluation path does.
    let mut priced = crate::select::price_selection_inputs_with_catalog(
        &mut prepared.facts,
        prepared.quota_signal_json.clone(),
        &sources,
        Some(&prepared.catalog_snapshot),
        dry.map_or(deps.store, |dry| dry.account_store),
        deps.broca_credentials,
        true,
        metered,
        dry.is_none(),
    )
    .map_err(store_refusal)?;
    let registry = {
        let credentials = deps.broca_credentials.lock().map_err(store_refusal)?;
        super::runners::Registry::build_for_selection(
            config.as_ref(),
            &credentials,
            &prepared.facts,
            super::now_ms(),
            request.harness.as_deref(),
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
    let mut rejected = Vec::new();
    facts.retain(|fact| {
        // Keep withdrawn facts until core admission can count their rejection
        // before runner or capability shortfalls in the candidate funnel.
        if fact.decommissioned {
            return true;
        }
        let route = fact_to_route(fact);
        if let Some(reason) = striatum_core::model_facts::llm_classification_rejection(fact, false)
        {
            rejected.push(PanelRejectedCandidate::new(route.route_key, reason));
            return false;
        }
        let window_fits = required.and_then(|r| r.context_window).is_none_or(|min| {
            fact.context_window
                .is_some_and(|window| window > 0.0 && window >= min as f64)
        });
        let modalities_fit =
            required
                .and_then(|r| r.modalities.as_ref())
                .is_none_or(|modalities| {
                    modalities.iter().all(|m| match m.as_str() {
                        "text" => true,
                        "image" => fact.supports_vision == Some(true),
                        _ => false,
                    })
                });
        let cause = if !window_fits {
            Some((
                "capability",
                "catalog window does not meet requirements.context_window",
            ))
        } else if !modalities_fit {
            Some((
                "capability",
                "catalog modalities do not meet requirements.modalities",
            ))
        } else if standing_families.contains(&family(fact.family.as_deref())) {
            Some((
                "family_duplicate",
                "family is already represented by a standing seat",
            ))
        } else if !request
            .available_routes
            .as_ref()
            .is_none_or(|routes| routes.contains(&route.capacity_key))
        {
            Some((
                "unavailable",
                "route is not in the request's available_routes",
            ))
        } else if !runners
            .iter()
            .any(|r| registry.runs(&r.module, &route.route_key, &route.capacity_key))
        {
            Some((
                "unavailable",
                "no eligible requested runner serves this route",
            ))
        } else {
            None
        };
        if let Some((reason, detail)) = cause {
            let mut cause = PanelRejectedCandidate::new(route.route_key, reason);
            cause.detail = Some(json!(detail));
            rejected.push(cause);
            false
        } else {
            true
        }
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
    let class_consumer = if class == scoring::PanelClass::Lab {
        "lab"
    } else {
        "athena"
    };
    let group_consumer =
        if crate::config_policy::consumer_matches(class_consumer, &request.consumer) {
            request.consumer.as_str()
        } else {
            class_consumer
        };
    let group = config.consumer_exclusions(group_consumer);
    let mut operator_exclude = crate::panel_policy::operator_exclusions(
        &facts,
        &config.operator_policy_ref().exclude,
        &group,
    );
    operator_exclude.extend(
        facts
            .iter()
            .filter(|f| {
                !config.route_allowed_on_plane(
                    &request.consumer,
                    &fact_to_route(f).route_key,
                    false,
                    f.billing_plane.as_ref(),
                )
            })
            .map(|f| fact_to_route(f).route_key),
    );
    let mut exclude = request.exclude.clone().unwrap_or_default();
    exclude.extend_from_slice(extra_exclude);
    let available: Vec<_> = facts.iter().map(|f| fact_to_route(f).route_key).collect();
    let resolved = striatum_core::launch_policy::resolve_compiled_routing_policy(
        available.iter().cloned(),
        config.compiled_routing_policy(),
        &Default::default(),
        &crate::provider_auth_types(None),
        &aliases,
        true,
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
    let mut evidence = HashMap::new();
    for fact in &facts {
        let route = fact_to_route(fact);
        let fit = request
            .package_tokens
            .map(|tokens| package_fit(tokens, fact));
        let reset = striatum_core::model_router::route_wall_until(
            &prepared.snapshot,
            &route.route_key,
            &route.capacity_key,
        )
        .filter(|until| until.is_finite() && *until > 0.0)
        .map(|until| until.ceil() as u64);
        evidence
            .entry(route.capacity_key)
            .or_insert_with(|| (fit.clone(), reset));
        evidence.insert(route.route_key, (fit, reset));
    }
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
    rejected.extend(panel.rejected_causes.iter().map(|cause| {
        let reason = serde_json::to_value(cause.reason).expect("panel reason is JSON");
        let mut candidate = PanelRejectedCandidate::new(
            &cause.model,
            reason.as_str().expect("panel reason is a string"),
        );
        candidate.detail = cause.detail.as_ref().map(|detail| json!(detail));
        candidate.reset_at_ms = cause
            .reset_at_ms
            .and_then(|reset| u64::try_from(reset).ok());
        let capacity = cortexkit_role_routing::select::RouteRef::parse_pin(&cause.model)
            .map(|route| format!("{}/{}", route.provider, route.model));
        if let Some((fit, reset)) = evidence
            .get(&cause.model)
            .or_else(|| capacity.as_ref().and_then(|key| evidence.get(key)))
        {
            if matches!(
                cause.reason,
                scoring::PanelCauseReason::PackageTooLarge
                    | scoring::PanelCauseReason::PackageExceedsCeiling
            ) {
                if let Some(fit) = fit {
                    candidate = candidate.with_typed_detail(fit.clone());
                }
            }
            if matches!(
                cause.reason,
                scoring::PanelCauseReason::QuotaExhausted
                    | scoring::PanelCauseReason::BackoffActive
            ) {
                candidate.reset_at_ms = reset.or(candidate.reset_at_ms);
            }
        }
        candidate
    }));
    Ok((panel, package_blocked, rejected, context.canonical_ids))
}

fn package_fit(tokens: u64, fact: &striatum_core::ModelFact) -> PanelPackageFit {
    use striatum_core::package_ceiling::{
        seat_package_ceiling, PACKAGE_OUTPUT_RESERVE_TOKENS, SEAT_PROMPT_RESERVE_TOKENS,
        UNKNOWN_SEAT_OUTPUT_TOKENS,
    };
    let output = fact.max_output_tokens.filter(|tokens| *tokens > 0);
    let window = fact
        .context_window
        .filter(|window| window.is_finite() && *window >= 1.0)
        .map(|window| window as u64);
    let ceiling = seat_package_ceiling("", window, output);
    PanelPackageFit::new(
        tokens,
        ceiling.tokens as u64,
        ceiling.context_window,
        ceiling.context_window_known,
        output,
        output
            .unwrap_or(UNKNOWN_SEAT_OUTPUT_TOKENS)
            .min(PACKAGE_OUTPUT_RESERVE_TOKENS),
        SEAT_PROMPT_RESERVE_TOKENS,
    )
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

#[cfg(test)]
mod lock_tests {
    use super::*;

    #[test]
    fn final_admission_shortfall_retains_refusal_without_inventing_diversity() {
        let mut refused = PanelRejectedCandidate::new("example/fourth:xhigh", "auth_required");
        refused.detail =
            Some(json!({"code": "auth_required", "message": "runner requires credentials"}));
        let shortfall = panel_shortfall(None, 3, 4, Some(46_960), vec![refused.clone()]);
        assert_eq!(shortfall.code, ShortfallCode::SizeReduced);
        let decoded: PanelShortfall = serde_json::from_value(json!(shortfall)).unwrap();
        let typed = decoded.typed_detail().unwrap().unwrap();
        assert_eq!(typed.causes, vec![refused.clone()]);
        let detail = shortfall.detail.unwrap();
        assert_eq!(detail["package_tokens"], 46_960);
        assert_eq!(detail["causes"], json!([refused]));
        assert!(detail["message"].as_str().unwrap().contains("3 of 4"));
        let unknown_package = panel_shortfall(None, 0, 4, None, Vec::new());
        assert_eq!(
            unknown_package.detail.unwrap()["package_tokens"],
            Value::Null
        );
    }

    #[test]
    fn replacement_lock_registry_reuses_live_locks_and_prunes_dead_entries_on_insert() {
        let first = replacement_lock("lock-registry-first").unwrap();
        let same = replacement_lock("lock-registry-first").unwrap();
        assert!(Arc::ptr_eq(&first, &same));
        let weak = Arc::downgrade(&first);
        drop(first);
        drop(same);
        assert!(weak.upgrade().is_none());
        let second = replacement_lock("lock-registry-second").unwrap();
        assert!(!REPLACEMENT_LOCKS
            .lock()
            .unwrap()
            .contains_key("lock-registry-first"));
        assert!(Arc::ptr_eq(
            &second,
            &replacement_lock("lock-registry-second").unwrap()
        ));
    }
}
