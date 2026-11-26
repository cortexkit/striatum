//! Legacy catalog and policy operations.
//! Keep their request and reply shapes with the durable catalog operations they adapt.

use super::*;

#[derive(Debug, Deserialize)]
struct ResolvePolicyBody {
    pub(super) params: ResolvePolicyParams,
}

#[derive(Debug, Deserialize)]
struct ModelFactBody {
    pub(super) params: ModelFactParams,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelFactParams {
    #[serde(rename = "providerID")]
    pub(super) provider_id: String,
    #[serde(rename = "modelID")]
    pub(super) model_id: String,
}

#[derive(Debug, Deserialize)]
struct ModelUpsertBody {
    pub(super) params: ModelUpsertParams,
}

/// Partial fact: absent fields keep the stored value on merge, and a NEW model
/// requires elo + eq (the two judgment axes nothing can default honestly).
///
/// `canonicalID` instead of `providerID`/`modelID` places Elo and EQ on every
/// stored route of one model (all rows whose Fusiform canonical id matches, at
/// the given variant) in one write. Only `elo`, `eq`, `variant` and `notes`
/// apply then: every other field describes one provider's listing.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelUpsertParams {
    #[serde(rename = "providerID", default)]
    pub(super) provider_id: String,
    #[serde(rename = "modelID", default)]
    pub(super) model_id: String,
    #[serde(rename = "canonicalID", default)]
    pub(super) canonical_id: Option<String>,
    #[serde(default)]
    pub(super) variant: Option<String>,
    #[serde(default)]
    pub(super) family: Option<String>,
    #[serde(default)]
    pub(super) elo: Option<f64>,
    #[serde(default)]
    pub(super) eq: Option<f64>,
    #[serde(default)]
    pub(super) speed: Option<f64>,
    #[serde(default)]
    pub(super) context_window: Option<f64>,
    #[serde(default)]
    pub(super) max_output_tokens: Option<u64>,
    #[serde(default)]
    pub(super) cost_per_m_tok_out: Option<f64>,
    #[serde(default)]
    pub(super) supports_vision: Option<bool>,
    #[serde(default)]
    pub(super) supports_tool_use: Option<bool>,
    #[serde(default)]
    pub(super) notes: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolvePolicyParams {
    #[serde(default)]
    pub(super) available_models: Vec<String>,
    #[serde(default)]
    routing_policy: Option<ModelRoutingPolicyInput>,
    #[serde(default)]
    provider_auth_types: Option<HashMap<String, String>>,
}

/// Merge partial upsert params over the stored fact. A new route requires Elo
/// and EQ: defaulting either would silently place a model in the ladder.
/// Absent fields keep the stored value; a new route's speed defaults to 100
/// (mid-catalog neutral) until observed samples replace it.
pub(super) fn merge_model_upsert(
    existing: Option<ModelFact>,
    params: &ModelUpsertParams,
) -> Result<ModelFact, String> {
    if params.family.is_some() {
        return Err(
            "model_upsert: family now comes from Fusiform's model.lineage; remove family".into(),
        );
    }
    let mut fact = match existing {
        Some(fact)
            if fact.score_provenance == ScoreProvenance::Unscored
                && (params.elo.is_none() || params.eq.is_none()) =>
        {
            return Err(format!(
                "model_upsert: route {}/{} is unscored; elo and eq are required to place it",
                params.provider_id, params.model_id
            ));
        }
        Some(fact) => fact,
        None => {
            let (Some(elo), Some(eq)) = (params.elo, params.eq) else {
                return Err(format!(
                    "model_upsert: route {}/{} is new; elo and eq are required to place it",
                    params.provider_id, params.model_id
                ));
            };
            ModelFact {
                billing_plane: None,
                auth_methods: None,
                login_methods_documented: false,
                speed_is_fleet_median: false,
                decommissioned: false,
                provider_id: params.provider_id.clone(),
                model_id: params.model_id.clone(),
                variant: params.variant.clone(),
                family: None,
                scores: ModelScores {
                    elo,
                    eq,
                    speed: params.speed.unwrap_or(100.0),
                },
                context_window: None,
                max_output_tokens: None,
                cost_tier: None,
                cost_per_m_tok_out: None,
                input_rate: None,
                cache_read_rate: None,
                cache_write_rate: None,
                reasoning_rate: None,
                supports_vision: None,
                supports_tool_use: None,
                prompt_defaults: None,
                confidence: Confidence::Curated,
                notes: None,
                speed_multiplier: None,
                subscription_multiplier: None,
                score_provenance: ScoreProvenance::Curated,
                eq_source: None,
                class_vector: None,
                class_vector_is_overlay: false,
                upstream_status: None,
                model_type: None,
                model_kind: None,
                mode_of: None,
            }
        }
    };
    if let Some(elo) = params.elo {
        fact.scores.elo = elo;
    }
    if let Some(eq) = params.eq {
        fact.scores.eq = eq;
    }
    // A Fusiform-discovered row starts with unknown confidence because it has no
    // scores. Supplying both scores confirms it; otherwise the adequacy penalty
    // can make it lose to confirmed routes when the free bucket groups routes
    // with negligible selection cost and compares their adequacy, so it never runs.
    if params.elo.is_some() && params.eq.is_some() {
        fact.confidence = Confidence::Curated;
        fact.score_provenance = ScoreProvenance::Curated;
    }
    if let Some(speed) = params.speed {
        fact.scores.speed = speed;
    }
    if params.context_window.is_some() {
        fact.context_window = params.context_window;
    }
    if params.max_output_tokens.is_some() {
        fact.max_output_tokens = params.max_output_tokens;
    }
    if params.cost_per_m_tok_out.is_some() {
        fact.cost_per_m_tok_out = params.cost_per_m_tok_out;
    }
    if params.supports_vision.is_some() {
        fact.supports_vision = params.supports_vision;
    }
    if params.supports_tool_use.is_some() {
        fact.supports_tool_use = params.supports_tool_use;
    }
    if params.notes.is_some() {
        fact.notes = params.notes.clone();
    }
    Ok(fact)
}

/// route.model_upsert: live catalog write. Merges the partial params over the
/// stored fact when the route exists; a new route requires elo and eq (the
/// operator-judgment axes). Writes land as FactSource::Override so re-seeds
/// never revert them. Operators can add models without writing a migration.
pub(super) async fn handle_model_upsert(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    ladder_sources: &winner_ladder::LadderSources,
    broca_credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    if serde_json::from_slice::<serde_json::Value>(&frame.body)
        .is_ok_and(|body| body["params"].get("defaultVariant").is_some())
    {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "defaultVariant was removed; reasoning effort defaults come from the model catalog",
        )
        .await;
    }
    let params: ModelUpsertParams = match serde_json::from_slice::<ModelUpsertBody>(&frame.body) {
        Ok(body) if body.params.canonical_id.is_some() => {
            return handle_canonical_model_upsert(
                body.params,
                (ver, channel, epoch, corr),
                writer,
                store,
                ladder_sources,
            )
            .await;
        }
        Ok(body)
            if !body.params.provider_id.trim().is_empty()
                && !body.params.model_id.trim().is_empty() =>
        {
            body.params
        }
        Ok(_) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "model_upsert requires non-empty providerID and modelID, or canonicalID",
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("model_upsert params not decodable: {error}"),
            )
            .await;
        }
    };
    for (label, value) in [
        ("elo", params.elo),
        ("eq", params.eq),
        ("speed", params.speed),
    ] {
        if let Some(score) = value {
            if !score.is_finite() || score <= 0.0 {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("model_upsert {label} must be a positive finite number, got {score}"),
                )
                .await;
            }
        }
    }
    if params.model_id.ends_with("-fast") {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "model_upsert does not accept -fast model IDs; use service_tier for speed",
        )
        .await;
    }
    let route_key = make_route_key(
        &params.provider_id,
        &params.model_id,
        params.variant.as_deref(),
    );
    if let Err(error) = fusiform_catalog::ingest_before_upsert(
        store,
        broca_credentials,
        &params.provider_id,
        &params.model_id,
    )
    .await
    {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "catalog_unavailable",
            &format!("model_upsert catalog ingest: {error}"),
        )
        .await;
    }
    let existing = match store.get_fact_by_route_key(&route_key) {
        Ok(existing) => existing,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("model_upsert read: {error}"),
            )
            .await;
        }
    };
    let created = existing.is_none();
    let existing = if existing.is_none() && params.variant.is_some() {
        store
            .get_fact_by_route_key(&make_route_key(&params.provider_id, &params.model_id, None))
            .map_err(|error| ModuleError::Message(error.to_string()))?
            .map(|mut fact| {
                fact.variant = params.variant.clone();
                fact
            })
    } else {
        existing
    };
    let fact = match merge_model_upsert(existing, &params) {
        Ok(fact) => fact,
        Err(message) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &message,
            )
            .await;
        }
    };
    // A placement can make this route the winner at some demand tier (or unseat
    // one), so compare the winner ladder after the write and announce a move.
    if let Err(error) =
        crate::account_writer::upsert_fact(store.as_ref(), &fact, FactSource::Override)
    {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "store_error",
            &format!("model_upsert write: {error}"),
        )
        .await;
    }
    winner_ladder::announce(
        store.as_ref(),
        ladder_sources,
        &format!("route.model_upsert {route_key}"),
    );
    let mut fact_fields = Vec::new();
    if params.context_window.is_some() {
        fact_fields.push(CatalogFactField::ContextWindow);
    }
    if params.max_output_tokens.is_some() {
        fact_fields.push(CatalogFactField::MaxOutputTokens);
    }
    if params.cost_per_m_tok_out.is_some() {
        fact_fields.push(CatalogFactField::CostPerMtokOut);
    }
    if params.supports_vision.is_some() {
        fact_fields.push(CatalogFactField::SupportsVision);
    }
    if params.supports_tool_use.is_some() {
        fact_fields.push(CatalogFactField::SupportsToolUse);
    }
    if let Err(error) = store.mark_operator_fact_fields(&route_key, &fact_fields) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "store_error",
            &format!("model_upsert provenance write: {error}"),
        )
        .await;
    }
    let result = json!({
        "routeKey": route_key,
        "created": created,
        "scores": fact.scores,
        "source": "override",
    });
    let body = serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

/// The canonical form of route.model_upsert: one Elo/EQ placement written to
/// every stored route of one model (rows whose Fusiform canonical id matches,
/// at the requested variant). Each row is written as its own override, so the
/// routes agree and none is left reading an older per-route score. Routes that
/// Fusiform adds later with the same canonical id borrow it at read time.
async fn handle_canonical_model_upsert(
    params: ModelUpsertParams,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    ladder_sources: &winner_ladder::LadderSources,
) -> Result<(), ModuleError> {
    let invalid = |message: String| async move {
        send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            &message,
        )
        .await
    };
    let canonical_id = params.canonical_id.clone().unwrap_or_default();
    if canonical_id.trim().is_empty() {
        return invalid("model_upsert canonicalID must be non-empty".into()).await;
    }
    if !params.provider_id.is_empty() || !params.model_id.is_empty() {
        return invalid("model_upsert takes canonicalID or providerID/modelID, not both".into())
            .await;
    }
    let per_route_fields = [
        ("family", params.family.is_some()),
        ("speed", params.speed.is_some()),
        ("contextWindow", params.context_window.is_some()),
        ("maxOutputTokens", params.max_output_tokens.is_some()),
        ("costPerMTokOut", params.cost_per_m_tok_out.is_some()),
        ("supportsVision", params.supports_vision.is_some()),
        ("supportsToolUse", params.supports_tool_use.is_some()),
    ]
    .into_iter()
    .filter_map(|(name, set)| set.then_some(name))
    .collect::<Vec<_>>();
    if !per_route_fields.is_empty() {
        return invalid(format!(
            "model_upsert by canonicalID places only elo and eq; {} describe one route, use providerID/modelID",
            per_route_fields.join(", ")
        ))
        .await;
    }
    let (Some(elo), Some(eq)) = (params.elo, params.eq) else {
        return invalid("model_upsert by canonicalID requires both elo and eq".into()).await;
    };
    for (label, score) in [("elo", elo), ("eq", eq)] {
        if !score.is_finite() || score <= 0.0 {
            return invalid(format!(
                "model_upsert {label} must be a positive finite number, got {score}"
            ))
            .await;
        }
    }
    let store_error = |message: String| async move {
        send_route_error(writer, ver, channel, epoch, corr, "store_error", &message).await
    };
    let route_keys =
        match store.route_keys_for_canonical_id(&canonical_id, params.variant.as_deref()) {
            Ok(route_keys) => route_keys,
            Err(error) => return store_error(format!("model_upsert read: {error}")).await,
        };
    if route_keys.is_empty() {
        return invalid(format!(
            "model_upsert: no stored route carries canonical id {canonical_id}{}; \
             place a route by providerID/modelID or wait for Fusiform to serve one",
            params
                .variant
                .as_deref()
                .map(|variant| format!(" at variant {variant}"))
                .unwrap_or_default()
        ))
        .await;
    }
    for route_key in &route_keys {
        let mut fact = match store.get_fact_by_route_key(route_key) {
            Ok(Some(fact)) => fact,
            Ok(None) => continue,
            Err(error) => return store_error(format!("model_upsert read: {error}")).await,
        };
        fact.scores.elo = elo;
        fact.scores.eq = eq;
        // Placing both judgment scores confirms the row, as merge_model_upsert
        // does for a per-route write: curated confidence lifts the unknown-fact
        // adequacy penalty a Fusiform-discovered row starts with.
        fact.confidence = Confidence::Curated;
        fact.score_provenance = ScoreProvenance::Curated;
        if params.notes.is_some() {
            fact.notes = params.notes.clone();
        }
        if let Err(error) =
            crate::account_writer::upsert_fact(store.as_ref(), &fact, FactSource::Override)
        {
            return store_error(format!("model_upsert write: {error}")).await;
        }
    }
    winner_ladder::announce(
        store.as_ref(),
        ladder_sources,
        &format!("route.model_upsert canonical {canonical_id}"),
    );
    let result = json!({
        "canonicalID": canonical_id,
        "variant": params.variant,
        "routeKeys": route_keys,
        "scores": { "elo": elo, "eq": eq },
        "source": "override",
    });
    let body = serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

/// route.list_canonical_gaps: operator-scored routes whose catalog row states
/// no canonical model id, so their score is not shared with the model's other
/// routes. This is the list to send Fusiform for identification.
pub(super) async fn handle_list_canonical_gaps(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    match store.list_canonical_gaps() {
        Ok(rows) => {
            let body = serde_json::to_vec(&json!({ "result": rows })).map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                ver,
                FrameType::Response,
                Flags::new(false, Priority::Interactive, false),
                channel,
                epoch,
                corr,
                body,
            )
            .map_err(|error| ModuleError::Message(error.to_string()))?;
            send(writer, response).await
        }
        Err(error) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("list_canonical_gaps: {error}"),
            )
            .await
        }
    }
}

/// The canonical model id of each panel candidate, so a panel never seats two
/// routes of one model. A row borrowed by a provider with no catalog rows
/// (`catalog_targets`) serves its catalog provider's model, so it takes that
/// row's id.
pub(super) fn panel_canonical_ids(
    store: &SqliteRouteStateStore,
    facts: &[ModelFact],
    catalog_targets: &std::collections::BTreeMap<String, String>,
) -> Result<HashMap<String, String>, striatum_core::RouteStateStoreError> {
    let stored = store.canonical_ids_by_route_key()?;
    Ok(facts
        .iter()
        .filter_map(|fact| {
            let route_key = fact_to_route(fact).route_key;
            let catalog_key = striatum_core::score_alias::catalog_key(catalog_targets, &route_key);
            stored
                .get(&catalog_key)
                .map(|canonical_id| (route_key, canonical_id.clone()))
        })
        .collect())
}

pub(super) async fn handle_list_unscored(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    match store.list_unscored_rows() {
        Ok(rows) => {
            let body = serde_json::to_vec(&json!({ "result": rows })).map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                ver,
                FrameType::Response,
                Flags::new(false, Priority::Interactive, false),
                channel,
                epoch,
                corr,
                body,
            )
            .map_err(|error| ModuleError::Message(error.to_string()))?;
            send(writer, response).await
        }
        Err(error) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("list_unscored: {error}"),
            )
            .await
        }
    }
}

/// route.model_facts: serve the durable catalog without reconstructing facts in
/// the plugin. Inventory consumers receive every stored capability field and the
/// explicit exclusion bit, so an absent capability remains unknown rather than
/// being fabricated from a local fallback.
pub(super) async fn handle_model_facts(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    operator_config_path: &Path,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let mut facts = match store.get_catalog_facts() {
        Ok(facts) => facts,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("model_facts: {error}"),
            )
            .await;
        }
    };
    // `excluded` is stamped from the live operator policy at read time, using the
    // same predicate selection applies. The stored column has never been written by
    // anything (it reads 0 on every row), so serving it raw told every inventory
    // consumer that nothing is excluded while routing was excluding 30+ routes.
    let operator_policy = config_overrides::read_operator_routing_policy(operator_config_path);
    let exclusions = ExclusionSpecs::parse(ExclusionPolicy {
        operator: &operator_policy.exclude,
        caller: &[],
    });
    for fact in &mut facts {
        let route_key = make_route_key(
            &fact.fact.provider_id,
            &fact.fact.model_id,
            fact.fact.variant.as_deref(),
        );
        fact.excluded = exclusions.excludes(&route_key);
    }
    let engine_speed_samples = match store.engine_speed_samples() {
        Ok(samples) => samples,
        Err(_) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                "Routing couldn't read its engine speed samples just now.",
            )
            .await;
        }
    };
    let body = serde_json::to_vec(&json!({ "result": {
        "facts": facts,
        "engine_speed_samples": engine_speed_samples,
        "engine_speed_sample_limit": striatum_store::ENGINE_SPEED_SAMPLE_LIMIT,
    } }))
    .map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

pub(super) fn warn_missing_session_model_once(provider_id: &str, model_id: &str) -> bool {
    static WARNED: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    let warned = WARNED.get_or_init(|| Mutex::new(HashSet::new()));
    let Ok(mut warned) = warned.lock() else {
        return false;
    };
    if !warned.insert((provider_id.to_string(), model_id.to_string())) {
        return false;
    }
    tracing::warn!(
        provider_id,
        model_id,
        pair = %format!("({provider_id}, {model_id})"),
        "session model has no model_facts row"
    );
    true
}

pub(super) async fn handle_model_fact(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: ModelFactParams = match serde_json::from_slice::<ModelFactBody>(&frame.body) {
        Ok(body)
            if !body.params.provider_id.trim().is_empty()
                && !body.params.model_id.trim().is_empty() =>
        {
            body.params
        }
        Ok(_) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "model_fact requires non-empty providerID and modelID",
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("model_fact params not decodable: {error}"),
            )
            .await;
        }
    };
    let route_key = make_route_key(&params.provider_id, &params.model_id, None);
    let result = match store.get_fact_by_route_key(&route_key) {
        Ok(Some(fact)) => {
            let (_, family_source) = match store.get_model_family(&route_key) {
                Ok(Some(family)) => family,
                Ok(None) => (None, striatum_core::FamilySource::Absent),
                Err(error) => {
                    return send_route_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "store_error",
                        &format!("model_fact family: {error}"),
                    )
                    .await;
                }
            };
            json!({
                "known": true,
                // Capability reads preserve an absent flag as cannot-verify. Routing admission
                // remains conservative in its own path and may treat the same absence as no.
                "supportsVision": fact.supports_vision,
                // The provider's own output ceiling lets pinned callers size the send for
                // this exact route instead of inheriting another panel member's limit.
                "maxOutputTokens": fact.max_output_tokens,
                // The context window lets a consult size its evidence package for
                // the roster it would seat before any seat is selected or paid for.
                "contextWindow": fact
                    .context_window
                    .filter(|window| window.is_finite() && *window >= 1.0)
                    .map(|window| window as u64),
                "family": fact.grand_family_id(),
                "familySource": family_source.as_str(),
            })
        }
        Ok(None) => {
            warn_missing_session_model_once(&params.provider_id, &params.model_id);
            json!({
                "known": false,
                "supportsVision": null,
                "maxOutputTokens": null,
                "contextWindow": null,
                "family": "other",
                "familySource": "absent"
            })
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("model_fact: {error}"),
            )
            .await;
        }
    };
    let body = serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

pub(super) async fn handle_resolve_policy(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    operator_config_path: &Path,
) -> Result<(), ModuleError> {
    let channel = frame.header.channel;
    let epoch = frame.header.epoch;
    let corr = frame.header.corr;
    let ver = frame.header.ver;
    let params = match serde_json::from_slice::<ResolvePolicyBody>(&frame.body) {
        Ok(body) => body.params,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("route.resolve_policy params not decodable: {error}"),
            )
            .await;
        }
    };
    warn_ignored_caller_policy_once(params.routing_policy.is_some());
    // Policy queries resolve the same routing-owned policy selection enforces.
    let operator_policy = config_overrides::read_operator_routing_policy(operator_config_path);
    let resolved = resolve_routing_policy(
        params.available_models,
        &operator_policy,
        &ModelRoutingPolicyInput::default(),
        &provider_auth_types(params.provider_auth_types.as_ref()),
    );
    let body = serde_json::to_vec(&json!({ "result": resolved })).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}
