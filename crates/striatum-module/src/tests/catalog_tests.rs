//! Catalog tests exercise fact reads and operator placement through the legacy operations.

use super::*;

fn upsert_params(provider: &str, model: &str) -> ModelUpsertParams {
    ModelUpsertParams {
        provider_id: provider.into(),
        model_id: model.into(),
        canonical_id: None,
        variant: None,
        family: None,
        elo: None,
        eq: None,
        speed: None,
        context_window: None,
        max_output_tokens: None,
        cost_per_m_tok_out: None,
        supports_vision: None,
        supports_tool_use: None,
        notes: None,
    }
}

#[test]
fn model_upsert_new_route_requires_both_judgment_scores() {
    // elo without eq (and vice versa) must refuse: a defaulted judgment
    // score places a model in the ladder silently.
    let mut params = upsert_params("newco", "shiny-1");
    params.elo = Some(1500.0);
    assert!(merge_model_upsert(None, &params).is_err());
    params.elo = None;
    params.eq = Some(1700.0);
    assert!(merge_model_upsert(None, &params).is_err());
    params.elo = Some(1500.0);
    let created_fact = merge_model_upsert(None, &params).expect("both scores create");
    assert_eq!(created_fact.scores.elo, 1500.0);
    assert_eq!(created_fact.scores.eq, 1700.0);
    assert_eq!(
        created_fact.scores.speed, 100.0,
        "new route speed defaults neutral"
    );
    assert_eq!(created_fact.confidence, Confidence::Curated);

    let mut unscored = fact("newco", "catalog-only");
    unscored.score_provenance = ScoreProvenance::Unscored;
    let mut incomplete = upsert_params("newco", "catalog-only");
    incomplete.supports_vision = Some(false);
    assert!(merge_model_upsert(Some(unscored.clone()), &incomplete).is_err());
    incomplete.elo = Some(1500.0);
    incomplete.eq = Some(1700.0);
    assert!(merge_model_upsert(Some(unscored), &incomplete).is_ok());
}

/// A discovered row is minted with unknown confidence; placing both scores
/// on it is the confirmation, so the placed row ranks without the
/// unknown-fact adequacy penalty. A score-less field edit on a curated row
/// leaves confidence alone.
#[test]
fn model_upsert_placing_scores_on_a_discovered_row_confirms_it() {
    let mut discovered = fact("ollama-cloud", "glm-5.3");
    discovered.confidence = Confidence::Unknown;
    discovered.score_provenance = ScoreProvenance::Unscored;
    let mut placement = upsert_params("ollama-cloud", "glm-5.3");
    placement.elo = Some(1732.0);
    placement.eq = Some(1579.0);
    let placed = merge_model_upsert(Some(discovered), &placement).unwrap();
    assert_eq!(placed.confidence, Confidence::Curated);
    assert_eq!(placed.score_provenance, ScoreProvenance::Curated);

    let mut curated = fact("openai", "gpt-x");
    curated.confidence = Confidence::Unknown;
    let mut edit = upsert_params("openai", "gpt-x");
    edit.supports_vision = Some(true);
    let edited = merge_model_upsert(Some(curated), &edit).unwrap();
    assert_eq!(
        edited.confidence,
        Confidence::Unknown,
        "no placement, no promotion"
    );
}

#[test]
fn model_upsert_merge_touches_only_supplied_fields() {
    let stored = fact("openai", "gpt-x");
    let mut params = upsert_params("openai", "gpt-x");
    params.elo = Some(1615.0);
    let merged = merge_model_upsert(Some(stored.clone()), &params).unwrap();
    assert_eq!(merged.scores.elo, 1615.0);
    // Every untouched field survives the merge byte-for-byte.
    assert_eq!(merged.scores.eq, stored.scores.eq);
    assert_eq!(merged.scores.speed, stored.scores.speed);
    assert_eq!(merged.context_window, stored.context_window);
    assert_eq!(merged.cost_per_m_tok_out, stored.cost_per_m_tok_out);
    assert_eq!(merged.supports_vision, stored.supports_vision);
    assert_eq!(merged.family, stored.family);
}

#[test]
fn model_upsert_refuses_operator_family_in_favor_of_catalog_lineage() {
    let mut params = upsert_params("lineage-fixture", "m");
    params.family = Some("claude".into());
    let error = merge_model_upsert(Some(fact("lineage-fixture", "m")), &params).unwrap_err();
    assert!(error.contains("family"));
    assert!(error.contains("Fusiform's model.lineage"));
}

#[tokio::test]
async fn model_upsert_refuses_default_variant_without_writing_a_row() {
    let rig = Rig::new("route-model-upsert-effort-refusal");
    let store = Arc::new(rig.open_store());
    let response = run_route_wire_request(
        &store,
        json!({
            "method": ROUTE_MODEL_UPSERT_OP,
            "params": {"providerID":"xai", "modelID":"grok-z", "elo":1500, "eq":1000,
                "defaultVariant":"xhigh"},
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["code"], "invalid_request");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("defaultVariant") && message.contains("model catalog"),
        "{body}"
    );
    assert!(store.get_fact_by_route_key("xai/grok-z").unwrap().is_none());
}

#[test]
fn model_upsert_written_as_override_survives_reseed() {
    // The store contract the op leans on: an Override row is never
    // reverted by the seed pass (seed fills absent rows only).
    let rig = Rig::new("upsert-override");
    let store = rig.open_store();
    let mut params = upsert_params("newco", "shiny-2");
    params.elo = Some(1400.0);
    params.eq = Some(1600.0);
    let fact = merge_model_upsert(None, &params).unwrap();
    store.upsert_fact(&fact, FactSource::Override).unwrap();
    let read = store
        .get_fact_by_route_key("newco/shiny-2")
        .unwrap()
        .expect("written route reads back");
    assert_eq!(read.scores.elo, 1400.0);
    assert!(store
        .list_override_route_keys()
        .unwrap()
        .contains(&"newco/shiny-2".to_string()));
}

#[tokio::test]
async fn route_model_fact_reads_a_seed_absent_anthropic_fact_from_the_live_store() {
    let request = json!({
        "method": ROUTE_MODEL_FACT_OP,
        "params": { "providerID": "anthropic", "modelID": "claude-opus-5" }
    });
    assert!(
            striatum_core::model_facts_seed()
                .iter()
                .all(|fact| !(fact.provider_id == "anthropic" && fact.model_id == "claude-opus-5")),
            "claude-opus-5 must remain absent from the frozen seed so this test proves the live store is authoritative"
        );

    let (response, _rig, _store) = run_select_panel_wire_request(
        "live-model-fact-anthropic",
        &serde_json::to_string(&request).unwrap(),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(
            body["result"],
            json!({
                "known": true,
                "supportsVision": true,
                "maxOutputTokens": 64000,
                "contextWindow": 1000000,
                "family": "claude",
                "familySource": "curated"
            }),
            "the reply carries the catalog's own output ceiling and context window so a caller can size its send and its evidence package to this route"
        );
}

#[tokio::test]
async fn route_model_facts_exposes_bounded_engine_speed_samples_separately() {
    let rig = Rig::new("engine-speed-inspection");
    let store = Arc::new(rig.open_store());
    rusqlite::Connection::open(rig.dir.join("store.db"))
        .unwrap()
        .execute(
            "INSERT INTO routing_engine_speed
             (kind, provider, model, outcome, duration_ms, wall_ms, first_output_ms)
             VALUES ('transcribe','media','engine','done','1000','250','10')",
            [],
        )
        .unwrap();
    let operator_config = rig.dir.join("striatum.jsonc");
    crate::test_config::write(&operator_config, "{}").unwrap();
    let response = run_route_wire_request_with_operator_config(
        &store,
        json!({"method":ROUTE_MODEL_FACTS_OP,"params":{}}),
        &operator_config,
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["result"]["engine_speed_sample_limit"], 20);
    assert_eq!(
        body["result"]["engine_speed_samples"],
        json!([{
            "kind":"transcribe","provider":"media","model":"engine","outcome":"done",
            "duration_ms":1000,"wall_ms":250,"first_output_ms":10
        }])
    );
    assert!(body["result"]["facts"].is_array());
}

#[tokio::test]
async fn route_model_facts_reads_a_live_store_row_with_catalog_fields() {
    let rig = Rig::new("live-model-facts");
    let store = Arc::new(rig.open_store());
    let mut live_fact = fact("fixture", "wire-only");
    live_fact.scores = ModelScores {
        elo: 1_701.0,
        eq: 1_801.5,
        speed: 91.0,
    };
    live_fact.cost_per_m_tok_out = Some(7.25);
    live_fact.supports_vision = Some(false);
    live_fact.supports_tool_use = Some(true);
    store
        .upsert_fact(&live_fact, FactSource::Override)
        .expect("write fixture directly to the routing store");
    store
        .upsert_fact(&fact("fixture", "wire-kept"), FactSource::Override)
        .expect("write the companion row the operator does not exclude");
    let mut unpriced = fact("fixture", "wire-unpriced");
    unpriced.cost_per_m_tok_out = None;
    unpriced.cost_tier = None;
    store
        .upsert_fact(&unpriced, FactSource::Override)
        .expect("write the row nobody has published a price for");
    // Stamp the STORED column on the row the operator does NOT exclude. Nothing
    // in the running system has ever written that column, so a reader serving it
    // raw answers with a value no policy produced; this row proves the served
    // answer comes from the live operator policy instead.
    rusqlite::Connection::open(rig.dir.join("store.db"))
        .expect("open fixture database")
        .execute(
            "UPDATE model_facts SET excluded = 1 WHERE route_key = 'fixture/wire-kept'",
            [],
        )
        .expect("stamp the stale exclusion column");
    let operator_config = rig.dir.join("striatum.jsonc");
    crate::test_config::write(
        &operator_config,
        r#"{ "model_routing": { "exclude": {"all": ["fixture/wire-only"]} } }"#,
    )
    .unwrap();

    let response = run_route_wire_request_with_operator_config(
        &store,
        json!({ "method": ROUTE_MODEL_FACTS_OP, "params": {} }),
        &operator_config,
    )
    .await;

    assert_eq!(response.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let facts = body["result"]["facts"]
        .as_array()
        .expect("model_facts returns a facts array");
    let fact = facts
        .iter()
        .find(|candidate| {
            candidate["providerID"] == "fixture" && candidate["modelID"] == "wire-only"
        })
        .expect("the wire response includes the store-only fixture row");
    assert_eq!(
        fact["scores"],
        json!({ "elo": 1701.0, "eq": 1801.5, "speed": 91.0 })
    );
    assert_eq!(fact["costPerMTokOut"], 7.25);
    assert_eq!(fact["supportsVision"], false);
    assert_eq!(fact["supportsToolUse"], true);
    assert_eq!(
        fact["excluded"], true,
        "the operator's exclude list decides what model_facts reports as excluded"
    );
    let kept = facts
        .iter()
        .find(|candidate| {
            candidate["providerID"] == "fixture" && candidate["modelID"] == "wire-kept"
        })
        .expect("the companion row is served too");
    assert_eq!(
        kept["excluded"], false,
        "a stale stored excluded=1 must not be served as policy state"
    );
    // A route nobody has priced is ranked at MAXIMUM cost by selection, so an
    // inventory reader must be able to tell it apart from an expensive one.
    assert_eq!(kept["priceUnknown"], false);
    let unpriced_row = facts
        .iter()
        .find(|candidate| {
            candidate["providerID"] == "fixture" && candidate["modelID"] == "wire-unpriced"
        })
        .expect("the unpriced row is served too");
    assert_eq!(unpriced_row["priceUnknown"], true);
}

#[tokio::test]
async fn placement_before_provider_ingestion_merges_fusiform_price_and_login_policy() {
    let rig = Rig::new("upsert-catalog-merge");
    let store = Arc::new(rig.open_store());
    let snapshot = fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({
            "catalog_version": 1, "resolved_at_ms": 1,
            "curated": [{"model":"meta/muse-wonder-1.0","auth_methods":["oauth"]}],
            "models": {"meta/muse-wonder-1.0": {
                "existence":"present", "limit.context":200000,
                "rate.input":{"state":"priced","currency":"USD","units":4,"exponent":0},
                "rate.output":{"state":"priced","currency":"USD","units":20,"exponent":0}
            }}
        }))
        .unwrap(),
    )
    .unwrap();
    let credentials = empty_broca_credentials();
    credentials.lock().unwrap().catalog_snapshot = Some(snapshot);
    for variant in [None, Some("thinking")] {
        let response = run_route_wire_request_with_paths(&store, json!({"method": ROUTE_MODEL_UPSERT_OP,
                "params":{"providerID":"meta","modelID":"muse-wonder-1.0","variant":variant,"elo":1808,"eq":1600}
            }), &test_operator_config(), &credentials).await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let fact = store
            .get_fact_by_route_key(&make_route_key("meta", "muse-wonder-1.0", variant))
            .unwrap()
            .unwrap();
        assert_eq!(fact.cost_per_m_tok_out, Some(20.0));
        assert_eq!(fact.input_rate, Some(4.0));
        assert_eq!(fact.auth_methods, Some(vec!["oauth".into()]));
        assert_eq!(fact.scores.elo, 1808.0);
        assert_eq!(fact.score_provenance, ScoreProvenance::Override);
    }
}

#[test]
fn panel_canonical_ids_follow_score_alias_rows_to_their_target() {
    let rig = Rig::new("panel-canonical-ids");
    let store = rig.open_store();
    let served =
        |provider: &str, model: &str, canonical: Option<&str>| striatum_store::FusiformFactUpdate {
            provider_id: provider.into(),
            model_id: model.into(),
            auth_methods: None,
            login_methods_documented: false,
            present: true,
            context_window: Some(200_000.0),
            max_output_tokens: None,
            cost_tier: None,
            cost_per_mtok_out: None,
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: None,
            supports_tool_use: None,
            reasoning_efforts: None,
            catalog_efforts: None,
            canonical_id: canonical.map(str::to_string),
            upstream_status: None,
            model_type: None,
            model_kind: None,
            mode_of: None,
            lineage: None,
        };
    store
        .apply_fusiform_catalog_for_providers(
            &[
                served(
                    "openrouter",
                    "meta/muse-spark-1.3",
                    Some("meta/muse-spark-1.3"),
                ),
                served("opencode", "muse-spark-1.3-contributor-free", None),
            ],
            striatum_store::FusiformCatalogCursor {
                catalog_version: 1,
                resolved_at_ms: 1,
            },
            &["openrouter", "opencode"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
        .unwrap();
    let aliases = std::collections::BTreeMap::from([(
        "openrouter-byok".to_string(),
        "openrouter".to_string(),
    )]);
    let mut facts = store.get_facts().unwrap();
    striatum_core::score_alias::append_alias_rows(&mut facts, &aliases);
    let ids = panel_canonical_ids(&store, &facts, &aliases).unwrap();
    assert_eq!(
        ids.get("openrouter/meta/muse-spark-1.3")
            .map(String::as_str),
        Some("meta/muse-spark-1.3")
    );
    assert_eq!(
        ids.get("openrouter-byok/meta/muse-spark-1.3")
            .map(String::as_str),
        Some("meta/muse-spark-1.3"),
        "an alias row serves its target's model"
    );
    assert!(!ids.contains_key("opencode/muse-spark-1.3-contributor-free"));
}

#[tokio::test]
async fn canonical_upsert_scores_every_stored_route_of_the_model() {
    const MUSE: &str = "meta/muse-spark-1.3";
    let rig = Rig::new("canonical-upsert");
    let store: SharedStore = Arc::new(rig.open_store());
    let served = |canonical: Option<&str>| json!({"existence": "present", "limit.context": 200000, "model.canonical_id": canonical});
    let snapshot = fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({
            "catalog_version": 1, "resolved_at_ms": 1,
            "models": {
                "meta/muse-spark-1.3": served(Some(MUSE)),
                "meta/muse-spark-1.3-contributor": served(Some(MUSE)),
                "openrouter/meta/muse-spark-1.3": served(Some(MUSE)),
                "opencode/muse-spark-1.3-contributor-free": served(None),
                "opencode/unstated-1": {"existence": "present"},
                "opencode/blank-1": served(Some(" ")),
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let (served_facts, cursor) = snapshot.parts();
    let identities = served_facts
        .iter()
        .map(|fact| {
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                fact.canonical_id.clone(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        identities["openrouter/meta/muse-spark-1.3"].as_deref(),
        Some(MUSE)
    );
    assert_eq!(identities["opencode/muse-spark-1.3-contributor-free"], None);
    assert_eq!(
        identities["opencode/unstated-1"], None,
        "an unserved key states no identity"
    );
    assert_eq!(
        identities["opencode/blank-1"], None,
        "a blank id names no model"
    );
    let providers = ["meta", "openrouter", "opencode"]
        .into_iter()
        .map(str::to_string)
        .collect::<HashSet<_>>();
    store
        .apply_fusiform_catalog_for_providers(served_facts, cursor, &providers)
        .unwrap();

    let response = run_route_wire_request(
        &store,
        json!({"method": ROUTE_MODEL_UPSERT_OP,
                "params": {"canonicalID": MUSE, "elo": 1810, "eq": 1520}}),
    )
    .await;
    assert_eq!(
        response.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let result =
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone();
    assert_eq!(
        result["routeKeys"],
        json!([
            "meta/muse-spark-1.3",
            "meta/muse-spark-1.3-contributor",
            "openrouter/meta/muse-spark-1.3"
        ])
    );
    for route in [
        "meta/muse-spark-1.3",
        "meta/muse-spark-1.3-contributor",
        "openrouter/meta/muse-spark-1.3",
    ] {
        let fact = store.get_fact_by_route_key(route).unwrap().unwrap();
        assert_eq!(
            (fact.scores.elo, fact.scores.eq),
            (1810.0, 1520.0),
            "{route}"
        );
        assert_eq!(fact.score_provenance, ScoreProvenance::Override, "{route}");
        assert_eq!(
            fact.context_window,
            Some(200_000.0),
            "{route} keeps its served facts"
        );
    }
    let unidentified = store
        .get_fact_by_route_key("opencode/muse-spark-1.3-contributor-free")
        .unwrap()
        .unwrap();
    assert_eq!(unidentified.score_provenance, ScoreProvenance::Unscored);

    for (params, needle) in [
        (
            json!({"canonicalID": MUSE, "elo": 1810, "eq": 1520, "contextWindow": 1000}),
            "contextWindow",
        ),
        (
            json!({"canonicalID": MUSE, "elo": 1810}),
            "requires both elo and eq",
        ),
        (
            json!({"canonicalID": MUSE, "providerID": "meta", "modelID": "x", "elo": 1, "eq": 1}),
            "not both",
        ),
        (
            json!({"canonicalID": "acme/unknown-1", "elo": 1810, "eq": 1520}),
            "no stored route carries canonical id acme/unknown-1",
        ),
    ] {
        let refused = run_route_wire_request(
            &store,
            json!({"method": ROUTE_MODEL_UPSERT_OP, "params": params}),
        )
        .await;
        assert_eq!(refused.header.ty, FrameType::Error, "{params}");
        let body = String::from_utf8_lossy(&refused.body).into_owned();
        assert!(body.contains(needle), "{params}: {body}");
    }

    let gaps = run_route_wire_request(
        &store,
        json!({"method": ROUTE_LIST_CANONICAL_GAPS_OP, "params": {}}),
    )
    .await;
    assert_eq!(gaps.header.ty, FrameType::Response);
    let gaps = serde_json::from_slice::<serde_json::Value>(&gaps.body).unwrap()["result"].clone();
    let keys = gaps
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["routeKey"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert!(
        !keys.iter().any(|key| key.contains("muse-spark")),
        "{keys:?}"
    );
    assert!(handles(ROUTE_LIST_CANONICAL_GAPS_OP));
}

#[test]
fn missing_session_model_warning_is_deduplicated_by_exact_pair() {
    let provider = "warning-proof-provider";
    let model = "warning-proof-model";
    assert!(warn_missing_session_model_once(provider, model));
    assert!(!warn_missing_session_model_once(provider, model));
    assert!(warn_missing_session_model_once(
        provider,
        "warning-proof-model-2"
    ));
    assert!(warn_missing_session_model_once(
        "warning-proof-provider-2",
        model
    ));
}

#[tokio::test]
async fn route_model_upsert_refuses_fast_id_without_writing_a_row() {
    let rig = Rig::new("route-model-upsert-fast-refusal");
    let store = Arc::new(rig.open_store());
    let response = run_route_wire_request(
        &store,
        json!({
            "method": ROUTE_MODEL_UPSERT_OP,
            "params": {
                "providerID": "openai",
                "modelID": "gpt-5.5-fast",
                "elo": 1500.0,
                "eq": 1000.0,
                "speed": 100.0
            }
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["code"], "invalid_request");
    assert!(store
        .get_fact_by_route_key("openai/gpt-5.5-fast")
        .unwrap()
        .is_none());
}
