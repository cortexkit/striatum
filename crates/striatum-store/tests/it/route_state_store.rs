//! Test-vectors for the sqlite route-state store. Ported from the TS
//! `route-state-store.test.ts` facts-domain cases, plus storage-lib integration
//! checks (seed-via-migration runs once, single-writer lease) that the TS version
//! couldn't have (those were the host's job; now they're ours via cortexkit-store).

use std::sync::atomic::{AtomicU64, Ordering};

use striatum_core::model_facts::{
    Confidence, ModelFact, ModelPromptDefaults, ModelScores, RoutePricing, ScoreProvenance,
};
use striatum_core::{
    admit_routes, derive_provider_key, make_capacity_key, make_route_key, relax_earliest_reset,
    select_model_route, BreakGlassCandidate, CatalogFactField, DecisionInput,
    DecisionOutcomeUpdate, ExclusionPolicy, FactSource, FailureBackoffConfig, ModelRequirements,
    RouteFailureReason, RouteOutcome, RouteStateStore, RouteStateUpdate, RouteWallClass,
    RoutingConfig, SelectModelRouteInput, UsageObservation, FAILURE_BACKOFF_INITIAL_MS,
};
use striatum_store::{
    dev_descriptor, DecisionListFilter, FusiformCatalogCursor, FusiformFactUpdate,
    SqliteRouteStateStore,
};

static N: AtomicU64 = AtomicU64::new(0);

/// A unique temp rig: a fresh sqlite path + lease dir per test (no cross-test
/// collision, and a fresh DB so the migration/seed runs from scratch).
struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let n = N.fetch_add(1, Ordering::Relaxed);
        // nanos: cross-run uniqueness under pid reuse
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-store-{label}-{}-{nanos}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }
    fn db_path(&self) -> String {
        self.dir.join("store.db").to_string_lossy().into_owned()
    }
    fn open(&self) -> SqliteRouteStateStore {
        let d = dev_descriptor("striatum", self.db_path());
        SqliteRouteStateStore::open(&d).expect("open store")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn fact(provider: &str, model: &str) -> ModelFact {
    ModelFact {
        billing_plane: None,
        auth_methods: None,
        speed_is_fleet_median: false,
        provider_id: provider.into(),
        model_id: model.into(),
        variant: None,
        family: None,
        scores: ModelScores {
            elo: 55.0,
            eq: 1700.0,
            speed: 90.0,
        },
        context_window: Some(123_456.0),
        max_output_tokens: None,
        cost_tier: Some(2.0),
        cost_per_m_tok_out: Some(4.5),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(true),
        supports_tool_use: Some(true),
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
        score_provenance: ScoreProvenance::Curated,
        aa_index: None,
        review_by_ms: None,
        eq_source: None,
        class_vector: None,
        class_vector_is_overlay: false,
        upstream_status: None, model_type: None, model_kind: None,
    }
}

/// Catalog size after all migrations: 33 (001) + 9 gpt-5.6 (004) + 1 grok-4.5 (006)
/// − 12 opencode-go + 12 ollama-cloud (007) + 1 fable-5 (008) = 44.
// 43 rebased rows (007) + fable-5 (008) + kimi k3 (009) + qwen3.8-max (010)
// + 14 alibaba-token-plan text models (013) - 11 pruned to the real
// entitlement (014: the token plan serves the qwen family only)
// + gemini 3.6 flash (017) + opus-5 and its -fast sibling (018).
// 54 seeded + migration 023's ollama-cloud/kimi-k3 (K3 became servable on
// ollama-cloud; an unseeded route made a roster seat silently unseatable)
// + grok-4.6 (026) + gemini 3.7 flash (027, succession seed carrying
// 3.6-flash scores; 3.6 retires via the config exclude, not the store).
// Migration 041 removes eleven model IDs ending in "-fast" because the request service rejects them at admission.
const CATALOG_LEN: usize = 46;

#[test]
fn migration_seeds_the_curated_catalog_on_first_open() {
    let rig = Rig::new("seed");
    let store = rig.open();
    let facts = store.get_facts().unwrap();
    assert_eq!(
        facts.len(),
        CATALOG_LEN,
        "migrations must seed the full curated catalog"
    );
    let sol = facts
        .iter()
        .find(|f| f.provider_id == "openai" && f.model_id == "gpt-5.6-sol")
        .expect("gpt-5.6-sol seeded by migration 4");
    // Post-007 AA v4.1 rebase.
    assert_eq!(sol.scores.elo, 1_838.0);
    assert_eq!(sol.cost_per_m_tok_out, Some(30.0));
    // Spot-check a known seed row round-trips with correct typed fields.
    let opus = facts
        .iter()
        .find(|f| f.provider_id == "anthropic" && f.model_id == "claude-opus-4-8")
        .expect("opus-4-8 seeded");
    assert_eq!(opus.scores.elo, 1_728.0);
    assert_eq!(opus.cost_per_m_tok_out, Some(25.0));
    assert_eq!(opus.supports_vision, Some(true));
    // ollama-cloud entitlement is present; lapsed opencode-go is gone.
    assert!(facts
        .iter()
        .any(|f| f.provider_id == "ollama-cloud" && f.model_id == "glm-5.2"));
    assert!(!facts.iter().any(|f| f.provider_id == "opencode-go"));
}

#[test]
fn catalog_fingerprint_changes_for_every_scored_fact_field() {
    use rusqlite::Connection;

    let rig = Rig::new("catalog-fingerprint-fields");
    let store = rig.open();
    let baseline = store.catalog_fingerprint().unwrap();

    let mut override_fact = fact("custom", "catalog-probe");
    store
        .upsert_fact(&override_fact, FactSource::Override)
        .unwrap();
    let after_route_key = store.catalog_fingerprint().unwrap();
    assert_ne!(
        baseline, after_route_key,
        "route identity must affect the fingerprint"
    );

    override_fact.scores.elo = 56.0;
    store
        .upsert_fact(&override_fact, FactSource::Override)
        .unwrap();
    let after_elo = store.catalog_fingerprint().unwrap();
    assert_ne!(
        after_route_key, after_elo,
        "Elo must affect the fingerprint"
    );

    override_fact.scores.eq = 1701.0;
    store
        .upsert_fact(&override_fact, FactSource::Override)
        .unwrap();
    let after_eq = store.catalog_fingerprint().unwrap();
    assert_ne!(after_elo, after_eq, "EQ must affect the fingerprint");

    override_fact.speed_multiplier = Some(0.5);
    store
        .upsert_fact(&override_fact, FactSource::Override)
        .unwrap();
    let after_multiplier = store.catalog_fingerprint().unwrap();
    assert_ne!(
        after_eq, after_multiplier,
        "speed multiplier must affect the fingerprint"
    );

    let connection = Connection::open(rig.db_path()).unwrap();
    connection
        .execute(
            "UPDATE model_facts SET excluded = 1 WHERE route_key = 'custom/catalog-probe'",
            [],
        )
        .unwrap();
    let after_exclusion = store.catalog_fingerprint().unwrap();
    assert_ne!(
        after_multiplier, after_exclusion,
        "exclusion must affect the fingerprint"
    );
}

#[test]
fn catalog_fingerprint_changes_when_only_price_changes() {
    let rig = Rig::new("catalog-fingerprint-price");
    let store = rig.open();
    let mut override_fact = fact("custom", "catalog-price-probe");
    store
        .upsert_fact(&override_fact, FactSource::Override)
        .unwrap();
    let before_price_change = store.catalog_fingerprint().unwrap();

    override_fact.cost_per_m_tok_out = Some(5.5);
    store
        .upsert_fact(&override_fact, FactSource::Override)
        .unwrap();

    assert_ne!(
        before_price_change,
        store.catalog_fingerprint().unwrap(),
        "a price-only change must affect the catalog fingerprint"
    );
}

#[test]
fn catalog_fingerprint_is_stable_across_a_shuffled_no_op_reseed() {
    let rig = Rig::new("catalog-fingerprint-reseed");
    let store = rig.open();
    let fingerprint = store.catalog_fingerprint().unwrap();
    let mut facts = store.get_facts().unwrap();
    for fact in &facts {
        let route_key = make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref());
        store.delete_fact(&route_key).unwrap();
    }
    facts.reverse();
    for fact in facts {
        store.upsert_fact(&fact, FactSource::Seed).unwrap();
    }

    assert_eq!(store.catalog_fingerprint().unwrap(), fingerprint);
}

#[test]
fn decision_rows_carry_the_live_catalog_fingerprint() {
    use rusqlite::Connection;

    let rig = Rig::new("catalog-fingerprint-decision");
    let store = rig.open();
    let fingerprint = store.catalog_fingerprint().unwrap();
    let id = store
        .record_decision(&DecisionInput {
            task_id: Some("catalog-task".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();

    let decision = store
        .get_decisions_for_task("catalog-task")
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        decision.catalog_fingerprint.as_deref(),
        Some(fingerprint.as_str())
    );
    let connection = Connection::open(rig.db_path()).unwrap();
    let persisted: Option<String> = connection
        .query_row(
            "SELECT catalog_fingerprint FROM model_route_decisions WHERE id = ?1",
            [&id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(persisted.as_deref(), Some(fingerprint.as_str()));
}

#[test]
fn migration_025_marks_known_vision_families_and_leaves_qwen_unverified() {
    let rig = Rig::new("known-vision-capabilities");
    let facts = rig.open().get_facts().unwrap();
    let vision = |provider: &str, model: &str| {
        facts
            .iter()
            .find(|fact| fact.provider_id == provider && fact.model_id == model)
            .and_then(|fact| fact.supports_vision)
    };

    assert_eq!(vision("anthropic", "claude-fable-5"), Some(true));
    assert_eq!(vision("anthropic", "claude-opus-5"), Some(true));
    assert_eq!(vision("openai", "gpt-5.6-sol"), Some(true));
    assert_eq!(vision("xai", "grok-4.5"), Some(true));
    assert_eq!(vision("alibaba-token-plan", "qwen3.8-max"), None);
}

#[test]
fn migration_012_seeds_output_caps_and_leaves_unknown_routes_uncapped() {
    let rig = Rig::new("max-output-tokens");
    let facts = rig.open().get_facts().unwrap();
    let cap = |route_key: &str| {
        facts
            .iter()
            .find(|fact| {
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    == route_key
            })
            .and_then(|fact| fact.max_output_tokens)
    };

    assert_eq!(cap("anthropic/claude-opus-4-8"), Some(64_000));
    assert_eq!(cap("openai/gpt-5.6-sol"), Some(128_000));
    assert_eq!(cap("xai/grok-4.5"), Some(32_768));
    assert_eq!(cap("kimi-for-coding/k3"), Some(131_072));
    assert_eq!(cap("alibaba-token-plan/qwen3.8-max-preview"), Some(65_536));
    assert_eq!(cap("ollama-cloud/glm-5.2"), None);
    assert_eq!(cap("google/antigravity-gemini-3.1-pro"), None);
}

#[test]
fn seed_runs_once_not_re_inserted_on_reopen() {
    let rig = Rig::new("once");
    {
        let store = rig.open();
        assert_eq!(store.get_facts().unwrap().len(), CATALOG_LEN);
    } // drop releases the single-writer lease
      // Reopen the same DB: the migrations are recorded as applied, so the seeds do
      // NOT re-run (no duplicate-key crash, count unchanged).
    let store = rig.open();
    assert_eq!(
        store.get_facts().unwrap().len(),
        CATALOG_LEN,
        "seed must not re-run on reopen"
    );
}

#[test]
fn migration_004_preserves_a_preseeded_override() {
    use cortexkit_store::{open_sqlite, Migration};
    use rusqlite::Connection;

    let rig = Rig::new("v3-preseeded-gpt56-override");
    let descriptor = dev_descriptor("striatum", rig.db_path());
    {
        let v3 = open_sqlite(&descriptor).unwrap();
        v3.migrate(
            "route_state",
            &[
                Migration {
                    version: 1,
                    statements: include_str!("../../migrations/001_route_state.sql"),
                },
                Migration {
                    version: 2,
                    statements: include_str!("../../migrations/002_usage.sql"),
                },
                Migration {
                    version: 3,
                    statements: include_str!("../../migrations/003_drop_reservations.sql"),
                },
            ],
        )
        .unwrap();
        v3.with_conn(|connection: &Connection| {
            connection.execute(
                "INSERT INTO model_facts (
                   route_key, capacity_key, provider_id, model_id, variant, scores_json,
                   context_window, cost_tier, cost_per_mtok_out, supports_vision,
                   supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at
                 ) VALUES (
                   'openai/gpt-5.6-sol', 'openai/gpt-5.6-sol', 'openai', 'gpt-5.6-sol',
                   NULL, '{\"iq\":12.34,\"eq\":1500,\"speed\":99}', 123456, 1, 2,
                   1, 1, NULL, 'curated', 'local override', 'override', 7
                 )",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    }

    let store = SqliteRouteStateStore::open(&descriptor).expect("remaining migrations apply");
    let sol = store
        .get_fact_by_route_key("openai/gpt-5.6-sol")
        .unwrap()
        .expect("preseeded route remains");
    assert_eq!(sol.scores.elo, 1_838.0);
    assert_eq!(store.get_facts().unwrap().len(), CATALOG_LEN);
    drop(store);

    let connection = Connection::open(rig.db_path()).unwrap();
    let source: String = connection
        .query_row(
            "SELECT source FROM model_facts WHERE route_key='openai/gpt-5.6-sol'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(source, "override");
}

#[test]
fn upsert_override_then_read_back() {
    let rig = Rig::new("override");
    let store = rig.open();
    let f = fact("custom", "model-x");
    store.upsert_fact(&f, FactSource::Override).unwrap();

    let read = store
        .get_fact_by_route_key("custom/model-x")
        .unwrap()
        .expect("present");
    assert_eq!(read.scores.elo, 55.0);
    assert_eq!(read.cost_per_m_tok_out, Some(4.5));
    assert_eq!(read.supports_vision, Some(true));

    let overrides = store.list_override_route_keys().unwrap();
    assert!(overrides.contains(&"custom/model-x".to_string()));
    // A seed fact is NOT an override.
    assert!(!overrides.iter().any(|k| k.starts_with("anthropic/")));
}

#[test]
fn upsert_is_idempotent_by_route_key() {
    let rig = Rig::new("idem");
    let store = rig.open();
    let mut f = fact("custom", "m");
    store.upsert_fact(&f, FactSource::Override).unwrap();
    f.scores.elo = 42.0; // change a field; same route key
    store.upsert_fact(&f, FactSource::Override).unwrap();
    let read = store.get_fact_by_route_key("custom/m").unwrap().unwrap();
    assert_eq!(
        read.scores.elo, 42.0,
        "second upsert updates, not duplicates"
    );
    // still exactly one custom row.
    let n_custom = store
        .get_facts()
        .unwrap()
        .iter()
        .filter(|x| x.provider_id == "custom")
        .count();
    assert_eq!(n_custom, 1);
}

#[test]
fn delete_fact_removes_it() {
    let rig = Rig::new("delete");
    let store = rig.open();
    store
        .upsert_fact(&fact("custom", "gone"), FactSource::Override)
        .unwrap();
    assert!(store
        .get_fact_by_route_key("custom/gone")
        .unwrap()
        .is_some());
    store.delete_fact("custom/gone").unwrap();
    assert!(store
        .get_fact_by_route_key("custom/gone")
        .unwrap()
        .is_none());
}

#[test]
fn capacity_keys_for_provider() {
    let rig = Rig::new("capkeys");
    let store = rig.open();
    // Seeded anthropic has several models → several capacity keys.
    let keys = store.get_capacity_keys_for_provider("anthropic").unwrap();
    assert!(keys.contains(&"anthropic/claude-opus-4-8".to_string()));
    assert!(keys.len() >= 2);
}

#[test]
fn single_writer_lease_rejects_a_second_live_open() {
    let rig = Rig::new("lease");
    let _held = rig.open(); // holds the single-writer lease (same db path)
    let d = dev_descriptor("striatum", rig.db_path());
    let second = SqliteRouteStateStore::open(&d);
    assert!(
        second.is_err(),
        "a second live writer on the same db must be rejected by the lease"
    );
}

// ── route-state telemetry ───────────────────────────────────────────────────

#[test]
fn update_route_state_preserves_unset_fields_and_increments_counters() {
    let rig = Rig::new("telemetry");
    let store = rig.open();
    store
        .update_route_state(
            "p/m",
            "p/m",
            &RouteStateUpdate {
                cooldown_until: Some(5000.0),
                increment_failure: true,
                ..Default::default()
            },
        )
        .unwrap();
    // Second write sets quota_pressure only: cooldown must SURVIVE (unset = preserve).
    store
        .update_route_state(
            "p/m",
            "p/m",
            &RouteStateUpdate {
                quota_pressure: Some(0.7),
                increment_failure: true,
                ..Default::default()
            },
        )
        .unwrap();

    let snap = store.build_snapshot(10_000.0).unwrap();
    let st = snap.by_route.get("p/m").expect("route state present");
    assert_eq!(
        st.cooldown_until,
        Some(5000.0),
        "unset field survives the second update"
    );
    assert_eq!(
        snap.by_capacity.get("p/m").and_then(|s| s.quota_pressure),
        Some(0.7),
        "soft pressure should live on the capacity key so a fresh overlay can replace it"
    );
}

// ── decisions ───────────────────────────────────────────────────────────────

#[test]
fn record_decision_and_set_outcome() {
    let rig = Rig::new("decisions");
    let store = rig.open();
    let id = store
        .record_decision(&DecisionInput {
            task_id: Some("t1".into()),
            target_agent: "mason".into(),
            requirements_json: "{\"iq\":80,\"eq\":20}".into(),
            selected_route_key: Some("anthropic/claude-opus-4-8".into()),
            selected_provider_id: Some("anthropic".into()),
            selected_model_id: Some("claude-opus-4-8".into()),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();
    store
        .set_decision_outcome(
            &id,
            &DecisionOutcomeUpdate {
                provider_outcome: None,
                outcome: RouteOutcome::Completed,
                last_error: None,
                observed_at_ms: 1_000,
                failure_backoff: FailureBackoffConfig::default(),
            },
        )
        .unwrap();

    let decisions = store.get_decisions_for_task("t1").unwrap();
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].id, id);
    assert_eq!(
        decisions[0].selected_route_key.as_deref(),
        Some("anthropic/claude-opus-4-8")
    );
    assert_eq!(decisions[0].outcome.as_deref(), Some("completed"));
}

#[test]
fn quota_outcome_retry_after_cools_route_until_the_reported_reset_mutation_proof() {
    const ROUTE: &str = "alibaba-token-plan/qwen3.8-max-preview";
    const OBSERVED_AT: i64 = 1_000;
    const RETRY_AFTER_SECS: u64 = 9 * 60 + 40;
    let rig = Rig::new("quota-outcome-retry-after");
    let store = rig.open();
    let decision_id = selected_decision(&store, "dec-quota-outcome", ROUTE);

    set_outcome(
        &store,
        &decision_id,
        RouteOutcome::QuotaExhausted {
            retry_after_secs: RETRY_AFTER_SECS,
        },
        Some("host account pool rate-limited"),
        OBSERVED_AT,
    );

    let expected_reset = (OBSERVED_AT + RETRY_AFTER_SECS as i64 * 1_000) as f64;
    let snapshot = store.build_snapshot(OBSERVED_AT as f64 + 1.0).unwrap();
    let fact = store
        .get_fact_by_route_key(ROUTE)
        .unwrap()
        .expect("qwen fact");
    let available: std::collections::HashSet<String> = [ROUTE.to_string()].into_iter().collect();
    let blocked = admit_routes(
        std::slice::from_ref(&fact),
        Some(&snapshot),
        ExclusionPolicy::default(),
        None,
        &available,
        &ModelRequirements { iq: 80.0, eq: 20.0 },
        &RoutingConfig::default(),
    );
    assert_eq!(
        blocked.rejected.len(),
        1,
        "ignoring retry_after_secs leaves the exhausted route selectable"
    );
    assert_eq!(blocked.rejected[0].reason, "quota_exhausted");
    assert_eq!(
        snapshot
            .by_route
            .get(ROUTE)
            .and_then(|state| state.quota_exhausted_until),
        Some(expected_reset),
        "retry_after_secs must write the exact route quota horizon"
    );

    let after_reset = store.build_snapshot(expected_reset + 1.0).unwrap();
    let selectable = admit_routes(
        &[fact],
        Some(&after_reset),
        ExclusionPolicy::default(),
        None,
        &available,
        &ModelRequirements { iq: 80.0, eq: 20.0 },
        &RoutingConfig::default(),
    );
    assert!(
        selectable
            .rejected
            .iter()
            .all(|route| route.route_key != ROUTE),
        "the route becomes selectable after the reported reset"
    );
}

fn selected_decision(store: &impl RouteStateStore, id: &str, route_key: &str) -> String {
    let (provider_id, model_id) = route_key.split_once('/').unwrap();
    store
        .record_decision(&DecisionInput {
            decision_id: Some(id.to_string()),
            task_id: Some(id.to_string()),
            target_agent: "mason".into(),
            requirements_json: "{\"iq\":80,\"eq\":20}".into(),
            selected_route_key: Some(route_key.into()),
            selected_provider_id: Some(provider_id.into()),
            selected_model_id: Some(model_id.into()),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            anchor_version: 5,
            ..Default::default()
        })
        .unwrap()
}

fn set_outcome(
    store: &impl RouteStateStore,
    decision_id: &str,
    outcome: RouteOutcome,
    last_error: Option<&str>,
    observed_at_ms: i64,
) {
    store
        .set_decision_outcome(
            decision_id,
            &DecisionOutcomeUpdate {
                provider_outcome: None,
                outcome,
                last_error: last_error.map(str::to_owned),
                observed_at_ms,
                failure_backoff: FailureBackoffConfig::default(),
            },
        )
        .unwrap();
}

#[test]
fn contract_errors_preserve_route_failures_and_record_contract_quality() {
    const ROUTE: &str = "openai/contract-test";
    const ERROR: &str = "reply_contract_error: prose verdict lacks a verbatim single-line quote from the judged sentence";
    let rig = Rig::new("contract-preserves-streak");
    let store = rig.open();
    store
        .update_route_state(ROUTE, ROUTE, &RouteStateUpdate::default())
        .unwrap();
    let conn = rusqlite::Connection::open(rig.db_path()).unwrap();
    let route_state = || {
        conn.query_row(
        "SELECT consecutive_terminal_failures, failure_count, failure_backoff_until, failure_backoff_reason FROM model_route_state WHERE route_key=?1",
        [ROUTE], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<f64>>(2)?, row.get::<_, Option<String>>(3)?))
    ).unwrap()
    };
    for attempt in 0..=3 {
        if attempt > 0 {
            let id = selected_decision(&store, &format!("dec-provider-{attempt}"), ROUTE);
            set_outcome(
                &store,
                &id,
                RouteOutcome::Error,
                Some("HTTP 503 provider unavailable"),
                attempt * 2,
            );
        }
        let before = route_state();
        assert_eq!(before.0, attempt);
        assert_eq!(before.1, attempt);
        assert_eq!(before.2.is_some(), attempt == 3);
        let id = selected_decision(&store, &format!("dec-contract-{attempt}"), ROUTE);
        set_outcome(
            &store,
            &id,
            RouteOutcome::ContractError,
            Some(ERROR),
            attempt * 2 + 1,
        );
        assert_eq!(route_state(), before);
        assert_eq!(
            store
                .get_decision_by_id(&id)
                .unwrap()
                .unwrap()
                .outcome
                .as_deref(),
            Some("contract_error")
        );
        let recorded: String = conn
            .query_row(
                "SELECT outcome_error FROM model_route_decisions WHERE id=?1",
                [&id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(recorded, ERROR);
    }
}

#[test]
fn ten_request_invalid_outcomes_leave_the_route_admissible_and_audited() {
    const ROUTE: &str = "openai/request-invalid-test";
    let rig = Rig::new("request-invalid-outcomes");
    let store = rig.open();
    let fact = fact("openai", "request-invalid-test");
    store.upsert_fact(&fact, FactSource::Override).unwrap();
    store
        .update_route_state(ROUTE, ROUTE, &RouteStateUpdate::default())
        .unwrap();
    let wire: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../fixtures/run_status.request_invalid_openai.json"
    ))
    .unwrap();
    let error = &wire["error"];
    let diagnostic = format!(
        "class {}; provider code {}; provider subcode {}; HTTP {}; Broca said: {}",
        error["class"].as_str().unwrap(),
        error["provider_code"].as_str().unwrap(),
        error["provider_subcode"].as_str().unwrap(),
        error["status"],
        error["message"].as_str().unwrap()
    );
    let conn = rusqlite::Connection::open(rig.db_path()).unwrap();
    for attempt in 1..=10 {
        let id = selected_decision(&store, &format!("dec-request-invalid-{attempt}"), ROUTE);
        set_outcome(
            &store,
            &id,
            RouteOutcome::SessionError,
            Some(&diagnostic),
            attempt,
        );
        let row = store.get_decision_by_id(&id).unwrap().unwrap();
        assert_eq!(row.outcome.as_deref(), Some("session_error"));
        let recorded_error: String = conn
            .query_row(
                "SELECT outcome_error FROM model_route_decisions WHERE id=?1",
                [&id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(recorded_error, diagnostic);
        assert!(recorded_error
            .contains("provider code request_invalid; provider subcode invalid_json_schema;"));
    }
    let counters: (i64, i64) = conn.query_row(
        "SELECT consecutive_terminal_failures, failure_count FROM model_route_state WHERE route_key=?1",
        [ROUTE], |row| Ok((row.get(0)?, row.get(1)?))
    ).unwrap();
    assert_eq!(counters, (0, 0));
    let snapshot = store.build_snapshot(11.0).unwrap();
    let state = &snapshot.by_route[ROUTE];
    assert_eq!(state.failure_backoff_until, None);
    assert_eq!(state.failure_backoff_reason, None);
    assert_eq!(state.quota_exhausted_until, None);
    let admission = admit_routes(
        &[fact],
        Some(&snapshot),
        ExclusionPolicy::default(),
        None,
        &[ROUTE.to_string()].into_iter().collect(),
        &ModelRequirements { iq: 0.0, eq: 0.0 },
        &RoutingConfig::default(),
    );
    assert!(admission.rejected.is_empty(), "{:?}", admission.rejected);
    assert_eq!(admission.admitted.len(), 1);
}

#[test]
fn request_invalid_does_not_reset_genuine_failures_or_existing_backoff() {
    const ROUTE: &str = "openai/request-invalid-test";
    let rig = Rig::new("request-invalid-preserves-streak");
    let store = rig.open();
    let conn = rusqlite::Connection::open(rig.db_path()).unwrap();
    for attempt in 1..=3 {
        let id = selected_decision(&store, &format!("dec-provider-error-{attempt}"), ROUTE);
        set_outcome(
            &store,
            &id,
            RouteOutcome::Error,
            Some("HTTP 503 provider unavailable"),
            attempt * 2,
        );
        let before: (i64, i64, Option<f64>, Option<String>) = conn.query_row(
            "SELECT consecutive_terminal_failures, failure_count, failure_backoff_until, failure_backoff_reason FROM model_route_state WHERE route_key=?1",
            [ROUTE], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        ).unwrap();
        assert_eq!(before.0, attempt);
        assert_eq!(before.1, attempt);
        assert_eq!(before.2.is_some(), attempt == 3);
        let id = selected_decision(
            &store,
            &format!("dec-invalid-between-failures-{attempt}"),
            ROUTE,
        );
        set_outcome(
            &store,
            &id,
            RouteOutcome::Error,
            Some("request_invalid: invalid schema"),
            attempt * 2 + 1,
        );
        let after = conn.query_row(
            "SELECT consecutive_terminal_failures, failure_count, failure_backoff_until, failure_backoff_reason FROM model_route_state WHERE route_key=?1",
            [ROUTE], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<f64>>(2)?, row.get::<_, Option<String>>(3)?))
        ).unwrap();
        assert_eq!(
            before, after,
            "request errors must neither reset nor increment route evidence"
        );
    }
}

#[test]
fn three_consecutive_session_failures_cool_qwen_by_name_and_preserve_quota_wall() {
    const ROUTE: &str = "alibaba-token-plan/qwen3.8-max-preview";
    const ERROR: &str =
        "Access to model denied. Please make sure you are eligible for using the model.";
    let rig = Rig::new("consecutive-failure-backoff");
    let store = rig.open();
    store
        .update_route_state(
            ROUTE,
            ROUTE,
            &RouteStateUpdate {
                quota_exhausted_until: Some(9_000_000.0),
                ..Default::default()
            },
        )
        .unwrap();

    for failure in 1..=3 {
        let id = selected_decision(&store, &format!("dec-qwen-{failure}"), ROUTE);
        set_outcome(
            &store,
            &id,
            RouteOutcome::SessionError,
            Some(ERROR),
            1_000 + failure,
        );
    }

    let snapshot = store.build_snapshot(2_000.0).unwrap();
    let state = snapshot.by_route.get(ROUTE).expect("qwen state");
    assert_eq!(
        state.failure_backoff_until,
        Some((1_003 + FAILURE_BACKOFF_INITIAL_MS) as f64)
    );
    assert_eq!(state.quota_exhausted_until, Some(9_000_000.0));
    assert_eq!(
        state.failure_backoff_reason.as_deref(),
        Some(
            "consecutive terminal send failures: 3; last error: Access to model denied. Please make sure you are eligible for using the model."
        )
    );

    let fact = store
        .get_fact_by_route_key(ROUTE)
        .unwrap()
        .expect("qwen fact");
    let quota_admission = admit_routes(
        std::slice::from_ref(&fact),
        Some(&snapshot),
        ExclusionPolicy::default(),
        None,
        &[ROUTE.to_string()].into_iter().collect(),
        &ModelRequirements { iq: 80.0, eq: 20.0 },
        &RoutingConfig::default(),
    );
    assert_eq!(quota_admission.rejected.len(), 1);
    assert_eq!(quota_admission.rejected[0].route_key, ROUTE);
    assert_eq!(
        quota_admission.rejected[0].reason, "quota_exhausted",
        "the existing longer quota wall remains authoritative over the shorter safety net"
    );

    let mut backoff_only = snapshot;
    backoff_only
        .by_route
        .get_mut(ROUTE)
        .unwrap()
        .quota_exhausted_until = None;
    backoff_only
        .by_capacity
        .get_mut(ROUTE)
        .unwrap()
        .quota_exhausted_until = None;
    let backoff_admission = admit_routes(
        &[fact],
        Some(&backoff_only),
        ExclusionPolicy::default(),
        None,
        &[ROUTE.to_string()].into_iter().collect(),
        &ModelRequirements { iq: 80.0, eq: 20.0 },
        &RoutingConfig::default(),
    );
    assert_eq!(backoff_admission.rejected[0].route_key, ROUTE);
    assert_eq!(backoff_admission.rejected[0].reason, "backoff_active");
    assert!(backoff_admission.rejected[0]
        .detail
        .as_deref()
        .unwrap()
        .contains(ERROR));
}

#[test]
fn success_after_two_terminal_failures_resets_before_the_third_failure() {
    const ROUTE: &str = "alibaba-token-plan/qwen3.8-max-preview";
    let rig = Rig::new("consecutive-failure-reset");
    let store = rig.open();
    for failure in 1..=2 {
        let id = selected_decision(&store, &format!("dec-failure-{failure}"), ROUTE);
        set_outcome(
            &store,
            &id,
            RouteOutcome::SessionError,
            Some("send failed"),
            failure,
        );
    }
    let success = selected_decision(&store, "dec-success", ROUTE);
    set_outcome(&store, &success, RouteOutcome::Completed, None, 3);
    let failure = selected_decision(&store, "dec-after-success", ROUTE);
    set_outcome(
        &store,
        &failure,
        RouteOutcome::SessionError,
        Some("send failed again"),
        4,
    );

    let snapshot = store.build_snapshot(5.0).unwrap();
    let state = snapshot.by_route.get(ROUTE).expect("qwen state");
    assert_eq!(state.failure_backoff_until, None);
    assert_eq!(state.failure_backoff_reason, None);
}

#[test]
fn project_root_population_round_trips_through_decision_write_path() {
    use rusqlite::Connection;

    let rig = Rig::new("decision-join-identity");
    let store = rig.open();
    let with_identity = store
        .record_decision(&DecisionInput {
            task_id: Some("run-with-identity".into()),
            parent_session_id: Some("ses-reused".into()),
            harness: Some("broca".into()),
            project_root: Some("/repos/one".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();
    let without_identity = store
        .record_decision(&DecisionInput {
            task_id: Some("run-without-identity".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            score_json: "{}".into(),
            rejected_json: "[]".into(),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();

    let connection = Connection::open(rig.db_path()).unwrap();
    let stored_identity: (Option<String>, Option<String>) = connection
        .query_row(
            "SELECT harness, project_root FROM model_route_decisions WHERE id=?1",
            [&with_identity],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let absent_identity: (Option<String>, Option<String>) = connection
        .query_row(
            "SELECT harness, project_root FROM model_route_decisions WHERE id=?1",
            [&without_identity],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(
        stored_identity,
        (Some("broca".into()), Some("/repos/one".into()))
    );
    assert_eq!(absent_identity, (None, None));
}

#[test]
fn migration_030_preserves_real_028_era_decision_rows_without_join_identity() {
    use cortexkit_store::{open_sqlite, Migration};
    use rusqlite::Connection;

    macro_rules! migration {
        ($version:literal, $file:literal) => {
            Migration {
                version: $version,
                statements: include_str!(concat!("../../migrations/", $file)),
            }
        };
    }

    let rig = Rig::new("migration-030-028-era-row");
    let descriptor = dev_descriptor("striatum", rig.db_path());
    let migrations = [
        migration!(1, "001_route_state.sql"),
        migration!(2, "002_usage.sql"),
        migration!(3, "003_drop_reservations.sql"),
        migration!(4, "004_gpt56_family.sql"),
        migration!(5, "005_panel_selection.sql"),
        migration!(6, "006_grok_45.sql"),
        migration!(7, "007_aa_v41_rebase.sql"),
        migration!(8, "008_fable_5.sql"),
        migration!(9, "009_kimi_k3.sql"),
        migration!(10, "010_qwen38_max.sql"),
        migration!(11, "011_qwen38_rescore.sql"),
        migration!(12, "012_max_output_tokens.sql"),
        migration!(13, "013_alibaba_token_plan_expansion.sql"),
        migration!(14, "014_alibaba_entitlement_prune.sql"),
        migration!(15, "015_provider_cooldown_inheritance.sql"),
        migration!(16, "016_opus_default_variant_xhigh.sql"),
        migration!(17, "017_gemini_36_flash.sql"),
        migration!(18, "018_opus_5.sql"),
        migration!(19, "019_anthropic_real_prices.sql"),
        migration!(20, "020_decision_retention_index.sql"),
        migration!(21, "021_new_sibling_models.sql"),
        migration!(22, "022_elo_rescore.sql"),
        migration!(23, "023_ollama_kimi_k3.sql"),
        migration!(24, "024_restore_override_reverted_facts.sql"),
        migration!(25, "025_known_vision_capabilities.sql"),
        migration!(26, "026_grok_46.sql"),
        migration!(27, "027_gemini_37_flash.sql"),
        migration!(28, "028_decision_ledger_provenance.sql"),
    ];
    let v28 = open_sqlite(&descriptor).unwrap();
    v28.migrate("route_state", &migrations).unwrap();
    v28.with_conn(|connection: &Connection| {
        connection.execute(
            "INSERT INTO model_route_decisions (
                id, decision_kind, task_id, parent_session_id, send_id, target_agent,
                requirements_json, selected_route_key, selected_provider_id, selected_model_id,
                selected_variant, score_json, rejected_json, explanation_json,
                quota_signal_json, policy_revision, anchor_version, created_at
              ) VALUES (
                'decision-v28', 'single', 'run-42', 'ses-reused', 'send-42', 'mason',
                '{}', 'openai/gpt-5.6', 'openai', 'gpt-5.6', NULL, '{}', '[]', '{}',
                '{\"state\":\"none\"}', 'revision-1', 2, 42
              )",
            [],
        )?;
        Ok(())
    })
    .unwrap();
    drop(v28);

    let _current = SqliteRouteStateStore::open(&descriptor).unwrap();
    let connection = Connection::open(rig.db_path()).unwrap();
    let stored: (String, String, String, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT task_id, parent_session_id, send_id, harness, project_root
             FROM model_route_decisions WHERE id='decision-v28'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        stored,
        (
            "run-42".into(),
            "ses-reused".into(),
            "send-42".into(),
            None,
            None,
        )
    );
}

#[test]
fn decision_scope_attribution_round_trips_and_stays_null_when_absent() {
    let rig = Rig::new("decision-scope-attribution");
    let store = rig.open();
    let scoped = DecisionInput {
        task_id: Some("scoped-task".into()),
        requirements_json: "{}".into(),
        score_json: "{}".into(),
        rejected_json: "[]".into(),
        project_root: Some("/work/projects/example".into()),
        project_id: Some("entorhinal-project-7".into()),
        workdir: Some("/work/projects/example".into()),
        ..Default::default()
    };
    let scoped_id = store.record_decision(&scoped).unwrap();
    // An unscoped decision keeps both columns NULL: the project is never
    // derived from the folder or anything else the row carries.
    let unscoped = DecisionInput {
        task_id: Some("unscoped-task".into()),
        requirements_json: "{}".into(),
        score_json: "{}".into(),
        rejected_json: "[]".into(),
        project_root: Some("/work/projects/example".into()),
        ..Default::default()
    };
    let unscoped_id = store.record_decision(&unscoped).unwrap();
    drop(store);

    let store = rig.open();
    let row = store.get_decision_by_id(&scoped_id).unwrap().unwrap();
    assert_eq!(row.project_id.as_deref(), Some("entorhinal-project-7"));
    assert_eq!(row.workdir.as_deref(), Some("/work/projects/example"));
    assert_eq!(row.project_root.as_deref(), Some("/work/projects/example"));
    let row = store.get_decision_by_id(&unscoped_id).unwrap().unwrap();
    assert_eq!(row.project_id, None);
    assert_eq!(row.workdir, None);
    assert_eq!(row.project_root.as_deref(), Some("/work/projects/example"));
}

#[test]
fn selected_execution_identity_round_trips_independently_of_caller_provenance() {
    let rig = Rig::new("selected-execution-identity");
    let store = rig.open();
    let input = DecisionInput {
        harness: Some("opencode".into()),
        send_id: Some("identity-send".into()),
        selected_route_key: Some("provider/model".into()),
        requirements_json: "{}".into(),
        score_json: "{}".into(),
        rejected_json: "[]".into(),
        ..Default::default()
    };
    let id = store
        .record_decision_with_selection(&input, Some("broca"), Some("sidekick.explore"))
        .unwrap();
    drop(store);
    let store = rig.open();
    let row = store.get_decision_by_id(&id).unwrap().unwrap();
    assert_eq!(row.harness.as_deref(), Some("opencode"));
    assert_eq!(row.selected_harness.as_deref(), Some("broca"));
    assert_eq!(row.selected_workload.as_deref(), Some("sidekick.explore"));
    assert_eq!(
        store.get_decision_by_send_id("identity-send").unwrap(),
        Some(row)
    );
}

#[test]
fn decision_read_queries_render_full_explanations_and_compact_lists() {
    let rig = Rig::new("decision-read-queries");
    let store = rig.open();
    let rejections = serde_json::json!([
        { "routeKey": "p/slow", "reason": "too_slow" },
        { "routeKey": "p/expensive", "reason": "too_expensive" },
        { "routeKey": "p/also-slow", "reason": "too_slow" },
    ]);
    store
        .record_decision(&DecisionInput {
            decision_id: Some("dec-read".into()),
            decision_kind: "single".into(),
            task_id: Some("task-read".into()),
            parent_session_id: Some("ses-read".into()),
            harness: Some("broca".into()),
            project_root: Some("/repos/read".into()),
            send_id: Some("send-read".into()),
            target_agent: "mason".into(),
            requirements_json: r#"{"iq":80,"eq":20}"#.into(),
            selected_route_key: Some("p/selected".into()),
            selected_provider_id: Some("p".into()),
            selected_model_id: Some("selected".into()),
            score_json: r#"{"adequacy":0.98,"selectionCost":2,"metrics":{"speed":7}}"#.into(),
            rejected_json: serde_json::to_string(&rejections).unwrap(),
            explanation_json: Some(r#"["cost-primary"]"#.into()),
            quota_signal_json: Some(r#"{"state":"present"}"#.into()),
            policy_revision: Some("policy-read".into()),
            anchor_version: 9,
            ..Default::default()
        })
        .unwrap();

    let explanation = store
        .get_decision_by_send_id("send-read")
        .unwrap()
        .expect("send id resolves its decision");
    assert_eq!(explanation.id, "dec-read");
    assert_eq!(explanation.parent_session_id.as_deref(), Some("ses-read"));
    assert_eq!(
        explanation.rejected_json,
        serde_json::to_string(&rejections).unwrap()
    );
    assert_eq!(
        store
            .get_decision_by_id("dec-read")
            .unwrap()
            .unwrap()
            .score_json,
        r#"{"adequacy":0.98,"selectionCost":2,"metrics":{"speed":7}}"#
    );

    let rows = store
        .list_decisions(&DecisionListFilter {
            target_agent: Some("mason".into()),
            limit: 20,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "dec-read");
    assert_eq!(rows[0].selected_route_key.as_deref(), Some("p/selected"));
    assert_eq!(rows[0].top_rejection_reason.as_deref(), Some("too_slow"));
    assert_eq!(rows[0].top_rejection_count, 2);
}

#[test]
fn decision_retention_deletes_old_rows_and_keeps_young_rows() {
    use rusqlite::{params, Connection};

    let rig = Rig::new("decision-retention");
    let store = rig.open();
    let input = |id: &str| DecisionInput {
        decision_id: Some(id.into()),
        task_id: Some("retention-task".into()),
        target_agent: "mason".into(),
        requirements_json: "{}".into(),
        score_json: "{}".into(),
        rejected_json: "[]".into(),
        anchor_version: 2,
        ..Default::default()
    };
    for id in ["old", "cutoff", "young"] {
        store.record_decision(&input(id)).unwrap();
    }

    let day_ms: i64 = 24 * 60 * 60_000;
    let cutoff = 31 * day_ms;
    {
        let connection = Connection::open(rig.db_path()).unwrap();
        for (id, created_at) in [
            ("old", cutoff - 1),
            ("cutoff", cutoff),
            ("young", cutoff + 1),
        ] {
            connection
                .execute(
                    "UPDATE model_route_decisions SET created_at=?1 WHERE id=?2",
                    params![created_at, id],
                )
                .unwrap();
        }
    }

    assert_eq!(store.delete_decisions_older_than(cutoff, 100).unwrap(), 1);
    let kept: Vec<String> = store
        .get_decisions_for_task("retention-task")
        .unwrap()
        .into_iter()
        .map(|decision| decision.id)
        .collect();
    assert_eq!(kept, vec!["cutoff".to_string(), "young".to_string()]);
}

#[test]
fn decision_rejections_keep_twelve_entries_and_summarize_the_rest() {
    use rusqlite::Connection;

    let rig = Rig::new("decision-rejection-truncation");
    let store = rig.open();
    // Twelve catalog-tail rows arrive first (the ledger stores them in producer
    // order); the three runtime walls arrive last. The bound must keep the walls,
    // which is what an operator reads when asking why a provider was skipped.
    let mut rejected = (0..12)
        .map(|i| serde_json::json!({ "routeKey": format!("tail-{i}"), "reason": "unscored_awaiting_placement" }))
        .collect::<Vec<_>>();
    let walls = [
        serde_json::json!({ "routeKey": "wall-0", "reason": "quota_exhausted", "detail": "wall for 60s more" }),
        serde_json::json!({ "routeKey": "wall-1", "reason": "cooldown_active", "detail": "wall for 61s more" }),
        serde_json::json!({ "routeKey": "wall-2", "reason": "backoff_active", "detail": "3 failures" }),
    ];
    rejected.extend(walls.iter().cloned());
    let raw = serde_json::to_string(&rejected).unwrap();
    store
        .record_decision(&DecisionInput {
            decision_id: Some("truncated-rejections".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            score_json: "{}".into(),
            rejected_json: raw,
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();

    let connection = Connection::open(rig.db_path()).unwrap();
    let stored: String = connection
        .query_row(
            "SELECT rejected_json FROM model_route_decisions WHERE id='truncated-rejections'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored: Vec<serde_json::Value> = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored.len(), 13);
    assert_eq!(
        &stored[..3],
        &walls[..],
        "runtime walls must survive the bound ahead of the catalog tail"
    );
    assert_eq!(&stored[3..12], &rejected[..9]);
    assert_eq!(
        stored[12],
        serde_json::json!({
            "truncated": 3,
            "reasons": { "unscored_awaiting_placement": 3 },
        })
    );
}

#[test]
fn decision_rejections_name_every_omitted_unavailable_route() {
    use rusqlite::Connection;

    let rig = Rig::new("decision-rejection-not-available");
    let store = rig.open();
    // Operator fences fill the inline bound first, exactly as they did when a
    // host that did not serve openai/gpt-6-luna recorded a decision that never
    // named the route at all.
    let mut rejected = (0..12)
        .map(|i| serde_json::json!({ "routeKey": format!("fenced-{i}"), "reason": "excluded_by_operator" }))
        .collect::<Vec<_>>();
    rejected
        .push(serde_json::json!({ "routeKey": "openai/gpt-6-luna", "reason": "not_available" }));
    rejected
        .push(serde_json::json!({ "routeKey": "tail-0", "reason": "unscored_awaiting_placement" }));
    rejected.push(serde_json::json!({ "routeKey": "openai/gpt-6-sol", "reason": "not_available" }));
    store
        .record_decision(&DecisionInput {
            decision_id: Some("not-available-rejections".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            score_json: "{}".into(),
            rejected_json: serde_json::to_string(&rejected).unwrap(),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();
    let connection = Connection::open(rig.db_path()).unwrap();
    let stored: String = connection
        .query_row(
            "SELECT rejected_json FROM model_route_decisions WHERE id='not-available-rejections'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored: Vec<serde_json::Value> = serde_json::from_str(&stored).unwrap();
    assert_eq!(
        stored[12],
        serde_json::json!({
            "truncated": 3,
            "reasons": { "not_available": 2, "unscored_awaiting_placement": 1 },
            "notAvailable": ["openai/gpt-6-luna", "openai/gpt-6-sol"],
        })
    );
}

#[test]
fn decision_rejections_retain_subscription_reserve_numbers_ahead_of_catalog_tail() {
    use striatum_core::subscription::{QuotaAccount, SubscriptionPlan, SubscriptionPool};
    let rig = Rig::new("decision-subscription-reserve");
    let store = rig.open();
    let pool = SubscriptionPool {
        provider: "openai".into(),
        keep_free_percent: 20.0,
        plans: vec![SubscriptionPlan {
            tier: Some("pro_100".into()),
            plan: None,
            measured: None,
            fee_usd: 100.0,
            accounts: vec![QuotaAccount {
                weekly_used: 81.0,
                weekly_remaining_fraction: 0.99,
                hours_to_reset: 166.74457583333333,
                short_walled: false,
                short_windows: Vec::new(),
                banked_resets: 0,
                banked_expires_at_ms: None,
                prepaid_credits: false,
            }],
        }],
        ..Default::default()
    };
    let detail = pool.reserve_diagnostic(20.0, false);
    let numbers: serde_json::Value = serde_json::from_str(&detail).unwrap();
    assert_eq!(numbers["pool"], "openai");
    assert_eq!(numbers["unit"], "weekly_percentage_points_per_account");
    assert_eq!(numbers["plans"][0]["headroomPoints"], 19.0);
    assert_eq!(numbers["plans"][0]["keepFreePoints"], 19.0);
    assert_eq!(numbers["plans"][0]["availablePoints"], 0.0);
    let reserve = serde_json::json!({"routeKey":"openai/gpt-6.1-sol", "reason":"subscription_reserve", "detail":detail});
    let mut rejected: Vec<_> = (0..12)
        .map(|i| {
            serde_json::json!({
                "routeKey":format!("fenced-{i}"), "reason":"excluded_by_operator"
            })
        })
        .collect();
    rejected.push(reserve.clone());
    store
        .record_decision(&DecisionInput {
            decision_id: Some("reserve-numbers".into()),
            target_agent: "mason".into(),
            requirements_json: "{}".into(),
            score_json: "null".into(),
            rejected_json: serde_json::to_string(&rejected).unwrap(),
            anchor_version: 5,
            ..Default::default()
        })
        .unwrap();
    let stored: String = rusqlite::Connection::open(rig.db_path())
        .unwrap()
        .query_row(
            "SELECT rejected_json FROM model_route_decisions WHERE id='reserve-numbers'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored: serde_json::Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored[0], reserve);
    assert_eq!(stored.as_array().unwrap().len(), 13);
    assert_eq!(
        stored[12],
        serde_json::json!({"truncated":1,"reasons":{"excluded_by_operator":1}})
    );
}

// ── build_snapshot capacity propagation ─────────────────────────────────────

#[test]
fn build_snapshot_propagates_capacity_shared_signals() {
    let rig = Rig::new("snapshot");
    let store = rig.open();
    // Two variants share a capacity key; one carries quota-exhaustion, the other a
    // cooldown. The capacity-shared signal takes the later/worst of each.
    store
        .update_route_state(
            "p/m/high",
            "p/m",
            &RouteStateUpdate {
                quota_exhausted_until: Some(9000.0),
                ..Default::default()
            },
        )
        .unwrap();
    store
        .update_route_state(
            "p/m/low",
            "p/m",
            &RouteStateUpdate {
                cooldown_until: Some(7000.0),
                ..Default::default()
            },
        )
        .unwrap();
    let snap = store.build_snapshot(1000.0).unwrap();
    let cap = snap.by_capacity.get("p/m").expect("capacity present");
    assert_eq!(cap.quota_exhausted_until, Some(9000.0));
    assert_eq!(cap.cooldown_until, Some(7000.0));
    assert_eq!(
        snap.by_route
            .get("p/m/high")
            .and_then(|s| s.quota_exhausted_until),
        Some(9000.0)
    );
}

#[test]
fn wall_classes_round_trip_and_a_null_legacy_class_keeps_break_glass_behavior() {
    let rig = Rig::new("wall-class-round-trip");
    let store = rig.open();
    let billing_error = "personal-team-blocked:spending-limit: You have run out of credits or need a Grok subscription";
    store
        .record_provider_cooldown(
            "xai",
            "xai/grok-4.6",
            "xai/grok-4.6",
            &RouteStateUpdate {
                cooldown_until: Some(5_000.0),
                quota_exhausted_until: Some(5_000.0),
                observed_at_ms: Some(1_000),
                wall_class: Some(RouteWallClass::Billing),
                last_error: Some(billing_error.into()),
                ..Default::default()
            },
        )
        .unwrap();
    store
        .update_route_state(
            "p/quota",
            "p/quota",
            &RouteStateUpdate {
                quota_exhausted_until: Some(6_000.0),
                observed_at_ms: Some(1_000),
                wall_class: Some(RouteWallClass::Quota),
                last_error: Some("you have reached your usage limit".into()),
                ..Default::default()
            },
        )
        .unwrap();
    store
        .update_route_state(
            "legacy/pre-migration",
            "legacy/pre-migration",
            &RouteStateUpdate {
                quota_exhausted_until: Some(7_000.0),
                observed_at_ms: Some(1_000),
                ..Default::default()
            },
        )
        .unwrap();

    let snapshot = store.build_snapshot(1_000.0).unwrap();
    assert_eq!(
        snapshot.by_route["xai/grok-4.6"].wall_class,
        Some(RouteWallClass::Billing)
    );
    assert_eq!(
        snapshot.by_route["xai/grok-4.6"].last_error.as_deref(),
        Some(billing_error)
    );
    // Identity-free quota is a capped cooldown, not account quota. Keep the
    // caller's class in original evidence while excluding it from inference.
    assert_eq!(snapshot.by_route["p/quota"].wall_class, None);
    let raw: String = rusqlite::Connection::open(rig.db_path())
        .unwrap()
        .query_row(
            "SELECT wall_class FROM model_route_state WHERE route_key='p/quota'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(raw, "quota");
    assert_eq!(snapshot.by_route["legacy/pre-migration"].wall_class, None);

    assert!(
        relax_earliest_reset(
            &snapshot,
            &[BreakGlassCandidate {
                route_key: "xai/grok-4.6".into(),
                capacity_key: "xai/grok-4.6".into(),
            }]
        )
        .is_none(),
        "a live billing wall is not relaxable"
    );
    assert!(
        relax_earliest_reset(
            &snapshot,
            &[BreakGlassCandidate {
                route_key: "legacy/pre-migration".into(),
                capacity_key: "legacy/pre-migration".into(),
            }]
        )
        .is_some(),
        "a NULL class preserves the pre-migration break-glass rule"
    );
}

#[test]
fn persisted_double_mirror_break_glass_clears_only_the_rejected_candidate() {
    let rig = Rig::new("break-glass-double-mirror");
    let store = rig.open();
    let route = fact("p", "candidate");
    let route_key = make_route_key("p", "candidate", None);
    let capacity_key = make_capacity_key("p", "candidate");
    store.upsert_fact(&route, FactSource::Override).unwrap();
    store
        .update_route_state(
            &route_key,
            &capacity_key,
            &RouteStateUpdate {
                quota_exhausted_until: Some(5_000.0),
                ..Default::default()
            },
        )
        .unwrap();
    store
        .update_route_state(
            "p/not-a-candidate",
            "p/not-a-candidate",
            &RouteStateUpdate {
                quota_exhausted_until: Some(2_000.0),
                ..Default::default()
            },
        )
        .unwrap();

    let snapshot = store.build_snapshot(1_000.0).unwrap();
    assert_eq!(
        snapshot.by_route[&route_key].quota_exhausted_until,
        Some(5_000.0)
    );
    assert_eq!(
        snapshot.by_capacity[&capacity_key].quota_exhausted_until,
        Some(5_000.0)
    );
    let available_models = std::collections::HashSet::from([route_key.clone()]);
    let blocked = select_model_route(SelectModelRouteInput {
        target_agent: "mason".into(),
        target_is_head: false,
        requirements: ModelRequirements { iq: 10.0, eq: 0.0 },
        available_models: available_models.clone(),
        facts: vec![route.clone()],
        runtime_snapshot: Some(snapshot.clone()),
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),

        required_capabilities: None,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: std::collections::HashSet::new(),
        providers_without_cache_write_reports: std::collections::HashSet::new(),
    });
    assert_eq!(
        blocked.unwrap_err().reason,
        RouteFailureReason::QuotaOrCooldownExhausted
    );

    let relaxed = relax_earliest_reset(
        &snapshot,
        &[BreakGlassCandidate {
            route_key: route_key.clone(),
            capacity_key: capacity_key.clone(),
        }],
    )
    .expect("rejected candidate is exhausted");
    assert_eq!(relaxed.route_key, route_key);
    assert_eq!(
        relaxed.snapshot.by_route[&route_key].quota_exhausted_until,
        None
    );
    assert_eq!(
        relaxed.snapshot.by_capacity[&capacity_key].quota_exhausted_until,
        None
    );
    assert_eq!(
        relaxed.snapshot.by_route["p/not-a-candidate"].quota_exhausted_until,
        Some(2_000.0)
    );

    let selected = select_model_route(SelectModelRouteInput {
        target_agent: "mason".into(),
        target_is_head: false,
        requirements: ModelRequirements { iq: 10.0, eq: 0.0 },
        available_models,
        facts: vec![route],
        runtime_snapshot: Some(relaxed.snapshot),
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),

        required_capabilities: None,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: std::collections::HashSet::new(),
        providers_without_cache_write_reports: std::collections::HashSet::new(),
    })
    .expect("clearing both mirrors admits the candidate");
    assert_eq!(selected.route_key, route_key);
}

// ── usage observations (observed TPS) ──────────────────────────────────

fn obs(id: &str, route: &str, out: f64, dur: f64, at: f64) -> UsageObservation {
    UsageObservation {
        id: id.into(),
        route_key: route.into(),
        capacity_key: "p/m".into(),
        provider_id: "p".into(),
        input_tokens: None,
        output_tokens: out,
        reasoning_tokens: None,
        cached_read_tokens: None,
        cache_write_tokens: None,
        duration_ms: dur,
        workload_class: None,
        account_key: None,
        observed_at: at,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct StoredUsageRow {
    route_key: String,
    capacity_key: String,
    provider_id: String,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    reasoning_tokens: Option<i64>,
    cached_read_tokens: Option<i64>,
    cache_write_tokens: Option<i64>,
    duration_ms: Option<i64>,
    workload_class: Option<String>,
    account_key: Option<String>,
    observed_at: i64,
}

fn stored_usage_row(rig: &Rig, id: &str) -> StoredUsageRow {
    rusqlite::Connection::open(rig.db_path())
        .unwrap()
        .query_row(
            "SELECT route_key, capacity_key, provider_id, input_tokens, output_tokens,
                    reasoning_tokens, cached_read_tokens, cache_write_tokens, duration_ms,
                    workload_class, account_key, observed_at
             FROM model_route_usage
             WHERE id = ?1",
            [id],
            |row| {
                Ok(StoredUsageRow {
                    route_key: row.get(0)?,
                    capacity_key: row.get(1)?,
                    provider_id: row.get(2)?,
                    input_tokens: row.get(3)?,
                    output_tokens: row.get(4)?,
                    reasoning_tokens: row.get(5)?,
                    cached_read_tokens: row.get(6)?,
                    cache_write_tokens: row.get(7)?,
                    duration_ms: row.get(8)?,
                    workload_class: row.get(9)?,
                    account_key: row.get(10)?,
                    observed_at: row.get(11)?,
                })
            },
        )
        .unwrap()
}

fn usage_row_counts(rig: &Rig) -> (i64, i64) {
    rusqlite::Connection::open(rig.db_path())
        .unwrap()
        .query_row(
            "SELECT COUNT(*), COUNT(DISTINCT id) FROM model_route_usage",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

fn complete_usage(id: &str) -> UsageObservation {
    let mut observation = obs(id, "p/m/high", 500.0, 5_000.0, 1_000.0);
    observation.input_tokens = Some(100.0);
    observation.reasoning_tokens = Some(25.0);
    observation.cached_read_tokens = Some(300.0);
    observation.cache_write_tokens = Some(0.0);
    observation.workload_class = Some("ephemeral".into());
    observation.account_key = Some("p/primary".into());
    observation
}

#[test]
fn migration_036_extends_the_single_usage_table_and_adds_the_class_time_index() {
    let rig = Rig::new("usage-profile-migration");
    let store = rig.open();
    let connection = rusqlite::Connection::open(rig.db_path()).unwrap();
    let columns = connection
        .prepare("PRAGMA table_info(model_route_usage)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for column in [
        "reasoning_tokens",
        "cached_read_tokens",
        "cache_write_tokens",
        "workload_class",
        "account_key",
    ] {
        assert!(
            columns.iter().any(|actual| actual == column),
            "missing {column}"
        );
    }
    let usage_tables = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%usage%'")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(usage_tables, vec!["model_route_usage"]);
    let index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index'
               AND name = 'model_route_usage_workload_class_observed_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(index_count, 1);
    drop(connection);
    drop(store);
}

#[test]
fn usage_records_rows_below_speed_eligibility_but_rejects_empty_ids() {
    let rig = Rig::new("usage-gates");
    let store = rig.open();
    assert!(!store
        .record_usage_observation(&obs("a", "p/m", 50.0, 5_000.0, 1_000.0))
        .unwrap());
    assert!(!store
        .record_usage_observation(&obs("b", "p/m", 500.0, 500.0, 1_000.0))
        .unwrap());
    assert!(!store
        .record_usage_observation(&obs("", "p/m", 500.0, 5_000.0, 1_000.0))
        .unwrap());
    assert!(store
        .record_usage_observation(&obs("c", "p/m", 500.0, 5_000.0, 1_000.0))
        .unwrap());
    assert_eq!(usage_row_counts(&rig), (3, 3));
}

#[test]
fn later_usable_delivery_fills_null_speed_columns_without_adding_a_row() {
    let rig = Rig::new("usage-upgrade");
    let store = rig.open();
    let mut unusable = complete_usage("upgrade");
    unusable.output_tokens = f64::NAN;
    unusable.duration_ms = f64::INFINITY;
    assert!(!store.record_usage_observation(&unusable).unwrap());
    let before = stored_usage_row(&rig, "upgrade");
    assert_eq!(before.output_tokens, None);
    assert_eq!(before.duration_ms, None);

    let mut later = complete_usage("upgrade");
    later.route_key = "other/route".into();
    later.capacity_key = "other/capacity".into();
    later.provider_id = "other".into();
    later.input_tokens = Some(999.0);
    later.output_tokens = 250.0;
    later.reasoning_tokens = Some(99.0);
    later.cached_read_tokens = Some(999.0);
    later.cache_write_tokens = Some(999.0);
    later.duration_ms = 2_000.0;
    later.workload_class = Some("head".into());
    later.account_key = Some("other/account".into());
    later.observed_at = 2_000.0;
    assert!(store.record_usage_observation(&later).unwrap());

    let after = stored_usage_row(&rig, "upgrade");
    assert_eq!(after.output_tokens, Some(250));
    assert_eq!(after.duration_ms, Some(2_000));
    assert_eq!(after.input_tokens, before.input_tokens);
    assert_eq!(after.reasoning_tokens, before.reasoning_tokens);
    assert_eq!(after.cached_read_tokens, before.cached_read_tokens);
    assert_eq!(after.cache_write_tokens, before.cache_write_tokens);
    assert_eq!(after.workload_class, before.workload_class);
    assert_eq!(after.account_key, before.account_key);
    assert_eq!(after.route_key, before.route_key);
    assert_eq!(after.capacity_key, before.capacity_key);
    assert_eq!(after.provider_id, before.provider_id);
    assert_eq!(after.observed_at, before.observed_at);
    assert_eq!(usage_row_counts(&rig), (1, 1));
}

#[test]
fn complete_usage_row_is_immutable_on_redelivery_and_message_ids_stay_unique() {
    let rig = Rig::new("usage-complete-redelivery");
    let store = rig.open();
    let original = complete_usage("complete");
    assert!(store.record_usage_observation(&original).unwrap());
    let before = stored_usage_row(&rig, "complete");

    let mut redelivery = complete_usage("complete");
    redelivery.route_key = "other/route".into();
    redelivery.capacity_key = "other/capacity".into();
    redelivery.provider_id = "other".into();
    redelivery.input_tokens = Some(200.0);
    redelivery.output_tokens = 900.0;
    redelivery.reasoning_tokens = Some(200.0);
    redelivery.cached_read_tokens = Some(900.0);
    redelivery.cache_write_tokens = Some(50.0);
    redelivery.duration_ms = 9_000.0;
    redelivery.workload_class = Some("head".into());
    redelivery.account_key = Some("other/account".into());
    redelivery.observed_at = 9_000.0;
    assert!(!store.record_usage_observation(&redelivery).unwrap());
    assert_eq!(stored_usage_row(&rig, "complete"), before);

    let mut incomplete = complete_usage("upgrade");
    incomplete.output_tokens = 0.0;
    incomplete.duration_ms = -1.0;
    assert!(!store.record_usage_observation(&incomplete).unwrap());
    assert!(store
        .record_usage_observation(&obs("upgrade", "p/m/high", 100.0, 1_000.0, 2_000.0))
        .unwrap());
    assert_eq!(usage_row_counts(&rig), (2, 2));
}

#[test]
fn invalid_output_and_duration_are_stored_as_null() {
    let rig = Rig::new("usage-invalid-output-and-duration");
    let store = rig.open();
    for (id, output) in [
        ("output-nan", f64::NAN),
        ("output-positive-infinity", f64::INFINITY),
        ("output-negative-infinity", f64::NEG_INFINITY),
        ("output-negative", -1.0),
        ("output-zero", 0.0),
        ("output-floor-zero", 0.99),
    ] {
        assert!(!store
            .record_usage_observation(&obs(id, "p/m", output, 1_000.0, 1_000.0))
            .unwrap());
        assert_eq!(stored_usage_row(&rig, id).output_tokens, None, "{id}");
    }

    for (id, duration) in [
        ("nan", f64::NAN),
        ("positive-infinity", f64::INFINITY),
        ("negative-infinity", f64::NEG_INFINITY),
        ("negative", -0.1),
    ] {
        assert!(!store
            .record_usage_observation(&obs(id, "p/m", 100.0, duration, 1_000.0))
            .unwrap());
        assert_eq!(stored_usage_row(&rig, id).duration_ms, None);
    }

    assert!(!store
        .record_usage_observation(&obs("zero", "p/m", 100.0, 0.0, 1_000.0))
        .unwrap());
    assert_eq!(stored_usage_row(&rig, "zero").duration_ms, Some(0));
}

#[test]
fn observed_speed_needs_min_samples_then_takes_the_median_sample_speed() {
    let rig = Rig::new("usage-speed");
    let store = rig.open();
    // 4 eligible samples < MIN_SAMPLES (5) → not enough trusted signal.
    for i in 0..4 {
        store
            .record_usage_observation(&obs(
                &format!("m{i}"),
                "p/m",
                1000.0,
                1000.0,
                1000.0 + i as f64,
            ))
            .unwrap();
    }
    assert_eq!(store.get_observed_speed("p/m", 10_000.0).unwrap(), None);
    // 5th sample crosses MIN_SAMPLES: every sample runs 1000 tok over 1000 ms,
    // so the median sample speed is 1000 tok/s.
    store
        .record_usage_observation(&obs("m4", "p/m", 1000.0, 1000.0, 1005.0))
        .unwrap();
    let speed = store.get_observed_speed("p/m", 10_000.0).unwrap().unwrap();
    assert!((speed - 1000.0).abs() < 1e-9, "got {speed}");
}

#[test]
fn observed_speed_ignores_rows_outside_the_trailing_window() {
    let rig = Rig::new("usage-window");
    let store = rig.open();
    // 5 eligible samples, but all observed_at far in the past relative to `now`.
    let ancient = 1000.0;
    for i in 0..5 {
        store
            .record_usage_observation(&obs(
                &format!("old{i}"),
                "p/m",
                1000.0,
                1000.0,
                ancient + i as f64,
            ))
            .unwrap();
    }
    // now is well beyond ancient + WINDOW_MS (7 days), so all rows fall out.
    let now = ancient + 8.0 * 24.0 * 60.0 * 60_000.0;
    assert_eq!(store.get_observed_speed("p/m", now).unwrap(), None);
}

#[test]
fn aggregated_observed_speeds_match_the_former_per_fact_overlay() {
    let rig = Rig::new("usage-aggregate-overlay");
    let store = rig.open();

    let mut prompt_default = fact("p", "prompt-default");
    prompt_default.prompt_defaults = Some(ModelPromptDefaults {
        variant: Some("high".into()),
        ..Default::default()
    });
    store
        .upsert_fact(&prompt_default, FactSource::Override)
        .unwrap();

    let mut own_variant = fact("p", "own-variant");
    own_variant.variant = Some("low".into());
    store
        .upsert_fact(&own_variant, FactSource::Override)
        .unwrap();
    store
        .upsert_fact(&fact("p", "insufficient-samples"), FactSource::Override)
        .unwrap();

    // Use more than the per-route cap so the oracle also verifies that the grouped
    // query retains only the same most-recent twenty samples as the old loop.
    for i in 0..26 {
        store
            .record_usage_observation(&obs(
                &format!("prompt-{i}"),
                "p/prompt-default/high",
                1_000.0 + (i * 10) as f64,
                1_000.0,
                9_000.0 + i as f64,
            ))
            .unwrap();
    }
    for i in 0..5 {
        store
            .record_usage_observation(&obs(
                &format!("variant-{i}"),
                "p/own-variant/low",
                2_000.0,
                1_000.0,
                9_500.0 + i as f64,
            ))
            .unwrap();
    }
    for i in 0..4 {
        store
            .record_usage_observation(&obs(
                &format!("insufficient-{i}"),
                "p/insufficient-samples",
                1_000.0,
                1_000.0,
                9_700.0 + i as f64,
            ))
            .unwrap();
    }

    let now = 10_000.0;
    // This is the former per-fact path kept as the test oracle: derive the same
    // prompt-default route key, call the singular query, and fall back to catalog speed.
    let expected: std::collections::HashMap<String, f64> = store
        .get_facts()
        .unwrap()
        .into_iter()
        .filter(|fact| fact.provider_id == "p")
        .map(|fact| {
            let observed_variant = fact
                .prompt_defaults
                .as_ref()
                .and_then(|defaults| defaults.variant.clone())
                .or_else(|| fact.variant.clone());
            let observed_route_key = make_route_key(
                &fact.provider_id,
                &fact.model_id,
                observed_variant.as_deref(),
            );
            let route_key =
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref());
            let observed_speed = store
                .get_observed_speed(&observed_route_key, now)
                .unwrap()
                .unwrap_or(fact.scores.speed);
            (route_key, observed_speed)
        })
        .collect();

    let aggregated = store
        .get_facts_with_observed_speed(now, &RoutePricing::default())
        .unwrap();
    for fact in aggregated.iter().filter(|fact| fact.provider_id == "p") {
        let route_key = make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref());
        assert_eq!(
            fact.scores.speed, expected[&route_key],
            "aggregated overlay drifted from the former per-fact lookup for {route_key}"
        );
    }
}

#[test]
fn facts_with_observed_speed_overlays_only_routes_with_trusted_signal() {
    let rig = Rig::new("usage-overlay");
    let store = rig.open();
    store
        .upsert_fact(&fact("p", "m"), FactSource::Override)
        .unwrap();
    // Without observations, the seed speed (90) survives.
    let before = store
        .get_facts_with_observed_speed(10_000.0, &RoutePricing::default())
        .unwrap();
    let pm_before = before
        .iter()
        .find(|f| f.provider_id == "p" && f.model_id == "m")
        .unwrap();
    assert_eq!(pm_before.scores.speed, 90.0);
    // Feed 5 eligible samples at 2000 tok/s (2000 tok over 1000 ms each).
    for i in 0..5 {
        store
            .record_usage_observation(&obs(
                &format!("s{i}"),
                "p/m",
                2000.0,
                1000.0,
                1000.0 + i as f64,
            ))
            .unwrap();
    }
    let after = store
        .get_facts_with_observed_speed(10_000.0, &RoutePricing::default())
        .unwrap();
    let pm_after = after
        .iter()
        .find(|f| f.provider_id == "p" && f.model_id == "m")
        .unwrap();
    assert!(
        (pm_after.scores.speed - 2000.0).abs() < 1e-9,
        "overlaid speed got {}",
        pm_after.scores.speed
    );
}

/// One runaway generation that ran to the output-token limit must not set a
/// route's observed speed. Nineteen ordinary samples at about 40 tok/s sit
/// beside one 128,000-token / 2,305,748 ms sample (about 55.5 tok/s). Dividing
/// the window's total tokens by its total duration lets that one sample carry
/// most of the weight and reports about 52.9 tok/s; the median per-sample speed
/// stays with the nineteen. Both the singular lookup and the batched catalog
/// overlay must agree.
#[test]
fn one_runaway_sample_does_not_set_the_observed_speed() {
    let rig = Rig::new("usage-speed-runaway");
    let store = rig.open();
    store
        .upsert_fact(&fact("p", "m"), FactSource::Override)
        .unwrap();
    // Ordinary samples: 1000 tokens over 24,700..25,240 ms, i.e. 39.6..40.5 tok/s.
    for i in 0..19 {
        store
            .record_usage_observation(&obs(
                &format!("ordinary-{i}"),
                "p/m",
                1_000.0,
                24_700.0 + (i * 30) as f64,
                1_000.0 + i as f64,
            ))
            .unwrap();
    }
    store
        .record_usage_observation(&obs("runaway", "p/m", 128_000.0, 2_305_748.0, 1_100.0))
        .unwrap();

    let now = 10_000.0;
    let speed = store.get_observed_speed("p/m", now).unwrap().unwrap();
    assert!(
        (39.5..=40.6).contains(&speed),
        "observed speed should follow the ordinary samples, got {speed}"
    );
    let overlaid = store
        .get_facts_with_observed_speed(now, &RoutePricing::default())
        .unwrap()
        .into_iter()
        .find(|f| f.provider_id == "p" && f.model_id == "m")
        .unwrap();
    assert_eq!(overlaid.scores.speed, speed);
}

/// A route with no observation of its own must enter at the fleet MEDIAN, not at
/// zero. Zero is the worst value the speed tie-break can hold, and that tie-break
/// decides the whole effectively-free bucket, so an unmeasured route would lose
/// every comparison it enters - while winning one is the only way it is ever run
/// and measured. The stand-in breaks that deadlock; a route with its own samples
/// must still use them, and a hand-seeded catalog speed must still outrank the
/// median.
#[test]
fn an_unmeasured_route_enters_at_the_fleet_median_rather_than_at_zero() {
    let rig = Rig::new("usage-median-standin");
    let store = rig.open();

    // Two measured routes at 100 and 300 tok/s, and one never measured with no
    // catalog speed of its own.
    for (model, tps) in [("fast", 3000.0), ("slow", 1000.0)] {
        let mut measured = fact("p", model);
        measured.scores.speed = 0.0;
        store.upsert_fact(&measured, FactSource::Override).unwrap();
        for i in 0..5 {
            store
                .record_usage_observation(&obs(
                    &format!("{model}{i}"),
                    &format!("p/{model}"),
                    tps,
                    1000.0,
                    1000.0 + i as f64,
                ))
                .unwrap();
        }
    }
    let mut unmeasured = fact("p", "unmeasured");
    unmeasured.scores.speed = 0.0;
    store
        .upsert_fact(&unmeasured, FactSource::Override)
        .unwrap();

    let facts = store
        .get_facts_with_observed_speed(10_000.0, &RoutePricing::default())
        .unwrap();
    let speed_of = |model: &str| {
        facts
            .iter()
            .find(|f| f.provider_id == "p" && f.model_id == model)
            .unwrap_or_else(|| panic!("{model} missing"))
    };

    let standin = speed_of("unmeasured");
    assert!(
        (standin.scores.speed - 2000.0).abs() < 1e-9,
        "unmeasured route should enter at the median of 1000 and 3000, got {}",
        standin.scores.speed
    );
    assert!(
        standin.speed_is_fleet_median,
        "a stand-in must be marked so a decision reader can tell it from a measurement"
    );

    // The measured routes keep their own observations, and are not marked.
    assert!((speed_of("fast").scores.speed - 3000.0).abs() < 1e-9);
    assert!(!speed_of("fast").speed_is_fleet_median);
    assert!((speed_of("slow").scores.speed - 1000.0).abs() < 1e-9);

    // A hand-seeded catalog speed is authoritative over the fleet median: the seed
    // is somebody's measurement, the median is nobody's.
    let seeded = fact("p", "seeded");
    assert!(seeded.scores.speed > 0.0, "fixture must carry a seed speed");
    store.upsert_fact(&seeded, FactSource::Override).unwrap();
    let facts = store
        .get_facts_with_observed_speed(10_000.0, &RoutePricing::default())
        .unwrap();
    let seeded_after = facts
        .iter()
        .find(|f| f.provider_id == "p" && f.model_id == "seeded")
        .unwrap();
    assert!((seeded_after.scores.speed - 90.0).abs() < 1e-9);
    assert!(!seeded_after.speed_is_fleet_median);
}

#[test]
fn panel_decision_kind_and_migration() {
    use cortexkit_store::{open_sqlite, Migration};
    use rusqlite::Connection;

    let rig = Rig::new("v4-panel-migration");
    let descriptor = dev_descriptor("striatum", rig.db_path());
    {
        let v4 = open_sqlite(&descriptor).unwrap();
        v4.migrate(
            "route_state",
            &[
                Migration {
                    version: 1,
                    statements: include_str!("../../migrations/001_route_state.sql"),
                },
                Migration {
                    version: 2,
                    statements: include_str!("../../migrations/002_usage.sql"),
                },
                Migration {
                    version: 3,
                    statements: include_str!("../../migrations/003_drop_reservations.sql"),
                },
                Migration {
                    version: 4,
                    statements: include_str!("../../migrations/004_gpt56_family.sql"),
                },
            ],
        )
        .unwrap();
        v4.with_conn(|connection: &Connection| {
            connection.execute(
                "INSERT INTO model_route_decisions (
                   id, task_id, target_agent, requirements_json, selected_route_key,
                   selected_provider_id, selected_model_id, score_json, rejected_json,
                   anchor_version, created_at
                 ) VALUES ('v4-decision','legacy-task','mason','{}','openai/gpt-5.5',
                           'openai','gpt-5.5','{}','[]',2,1)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    }

    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let facts = store.get_facts().unwrap();
    assert_eq!(facts.len(), CATALOG_LEN);
    let consciously_absent: [&str; 0] = [];
    assert!(facts.iter().all(|fact| {
        fact.family.is_some()
            || consciously_absent.contains(
                &make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    .as_str(),
            )
    }));
    let hosted_claude = facts
        .iter()
        .find(|fact| fact.model_id == "antigravity-claude-opus-4-6-thinking")
        .unwrap();
    assert_eq!(hosted_claude.family.as_deref(), Some("claude"));
    let kimi = facts
        .iter()
        .find(|fact| fact.model_id == "kimi-k2.6")
        .unwrap();
    assert_eq!(kimi.family.as_deref(), Some("kimi"));
    assert_eq!(kimi.provider_id, "ollama-cloud");

    let legacy = store.get_decisions_for_task("legacy-task").unwrap();
    assert_eq!(legacy[0].decision_kind, "single");
    assert_eq!(
        legacy[0].selected_route_key.as_deref(),
        Some("openai/gpt-5.5")
    );
    assert_eq!(
        legacy[0].catalog_fingerprint, None,
        "migration must not fabricate catalog provenance for historical decisions"
    );

    let panel_id = store
        .record_decision(&DecisionInput {
            decision_kind: "panel".into(),
            task_id: Some("panel-task".into()),
            target_agent: "athena-panel".into(),
            requirements_json: "{\"size\":2}".into(),
            score_json: "null".into(),
            rejected_json: "[]".into(),
            explanation_json: Some("{\"members\":[\"openai/gpt-5.6-sol\"],\"causes\":[]}".into()),
            anchor_version: 2,
            ..Default::default()
        })
        .unwrap();
    let panel = store.get_decisions_for_task("panel-task").unwrap();
    assert_eq!(panel[0].id, panel_id);
    assert_eq!(panel[0].decision_kind, "panel");
    assert_eq!(panel[0].selected_route_key, None);

    let connection = Connection::open(rig.db_path()).unwrap();
    let (provider, model, variant, explanation): (
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    ) = connection
        .query_row(
            "SELECT selected_provider_id, selected_model_id, selected_variant, explanation_json
             FROM model_route_decisions WHERE id=?1",
            [&panel_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!((provider, model, variant), (None, None, None));
    assert!(explanation.contains("openai/gpt-5.6-sol"));
}

/// Migration 007: drop opencode-go, seed ollama-cloud, rebase iq, but only when
/// scores_json still matches the frozen pre-007 value (deliberate overrides survive).
#[test]
fn migration_007_rebases_catalog_and_preserves_overrides() {
    use cortexkit_store::{open_sqlite, Migration};
    use rusqlite::Connection;

    let rig = Rig::new("v7-aa-v41-rebase");
    let descriptor = dev_descriptor("striatum", rig.db_path());
    {
        // Apply through migration 6 only, then plant a deliberate override on a
        // route that 007 would otherwise rewrite.
        let pre = open_sqlite(&descriptor).unwrap();
        pre.migrate(
            "route_state",
            &[
                Migration {
                    version: 1,
                    statements: include_str!("../../migrations/001_route_state.sql"),
                },
                Migration {
                    version: 2,
                    statements: include_str!("../../migrations/002_usage.sql"),
                },
                Migration {
                    version: 3,
                    statements: include_str!("../../migrations/003_drop_reservations.sql"),
                },
                Migration {
                    version: 4,
                    statements: include_str!("../../migrations/004_gpt56_family.sql"),
                },
                Migration {
                    version: 5,
                    statements: include_str!("../../migrations/005_panel_selection.sql"),
                },
                Migration {
                    version: 6,
                    statements: include_str!("../../migrations/006_grok_45.sql"),
                },
            ],
        )
        .unwrap();
        pre.with_conn(|connection: &Connection| {
            // Confirm pre-007 catalog still has opencode-go and lacks ollama-cloud/glm-5.2.
            let opencode_n: i64 = connection.query_row(
                "SELECT COUNT(*) FROM model_facts WHERE route_key LIKE 'opencode-go/%'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(opencode_n, 12);
            let glm52: i64 = connection.query_row(
                "SELECT COUNT(*) FROM model_facts WHERE route_key = 'ollama-cloud/glm-5.2'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(glm52, 0);

            // Deliberate override: change scores_json away from the frozen seed so
            // the guarded UPDATE must leave this row alone.
            connection.execute(
                "UPDATE model_facts
                 SET scores_json = '{\"elo\":1234,\"eq\":1500,\"speed\":99}',
                     source = 'override',
                     notes = 'local override survives 007'
                 WHERE route_key = 'openai/gpt-5.5'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    }

    // Full open applies migration 007.
    let store = SqliteRouteStateStore::open(&descriptor).expect("migration 007 applies");
    let facts = store.get_facts().unwrap();
    assert_eq!(facts.len(), CATALOG_LEN);

    // opencode-go rows are gone.
    assert!(!facts.iter().any(|f| f.provider_id == "opencode-go"));
    assert!(store
        .get_fact_by_route_key("opencode-go/minimax-m3")
        .unwrap()
        .is_none());

    // ollama-cloud/glm-5.2 carries its resolved Elo and family.
    let glm = store
        .get_fact_by_route_key("ollama-cloud/glm-5.2")
        .unwrap()
        .expect("ollama-cloud/glm-5.2 seeded by 007");
    assert_eq!(glm.scores.elo, 1_660.0);
    assert_eq!(glm.scores.eq, 1565.0);
    assert_eq!(glm.family.as_deref(), Some("glm"));

    // Un-overridden seed row was rebased (sol 69.5 → 58).
    let sol = store
        .get_fact_by_route_key("openai/gpt-5.6-sol")
        .unwrap()
        .expect("sol remains");
    assert_eq!(sol.scores.elo, 1_838.0);

    // Non-capability override fields survive the Elo migration, while the migration
    // assigns every catalog route its resolved Elo value.
    let overridden = store
        .get_fact_by_route_key("openai/gpt-5.5")
        .unwrap()
        .expect("overridden route remains");
    assert_eq!(overridden.scores.elo, 1_690.0);
    assert_eq!(overridden.scores.eq, 1500.0);
    assert_eq!(overridden.scores.speed, 99.0);

    drop(store);
    let connection = Connection::open(rig.db_path()).unwrap();
    let source: String = connection
        .query_row(
            "SELECT source FROM model_facts WHERE route_key='openai/gpt-5.5'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(source, "override");
    let mistral_family: String = connection
        .query_row(
            "SELECT family FROM model_facts WHERE route_key='ollama-cloud/mistral-large-3'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mistral_family, "mistral");
}

#[test]
fn speed_multiplier_applies_after_observed_speed_overlay() {
    let rig = Rig::new("speed-multiplier-overlay");
    let store = rig.open();
    store
        .upsert_fact(&fact("pmult", "mmult"), FactSource::Override)
        .unwrap();

    let pricing = RoutePricing {
        speed_multipliers: [("pmult/mmult".to_string(), 0.5)].into(),
        ..Default::default()
    };

    for i in 0..5 {
        store
            .record_usage_observation(&obs(
                &format!("s{i}"),
                "pmult/mmult",
                2000.0,
                1000.0,
                1000.0 + i as f64,
            ))
            .unwrap();
    }

    let after = store
        .get_facts_with_observed_speed(10_000.0, &pricing)
        .unwrap();
    let pm_after = after
        .iter()
        .find(|f| f.provider_id == "pmult" && f.model_id == "mmult")
        .unwrap();
    assert!(
        (pm_after.scores.speed - 1000.0).abs() < 1e-9,
        "overlaid speed got {}, expected 1000.0 (2000.0 * 0.5)",
        pm_after.scores.speed
    );
    assert_eq!(pm_after.speed_multiplier, Some(0.5));
}

#[test]
fn migration_008_seeds_fable_for_roster_seating_without_entering_routing() {
    let rig = Rig::new("fable-seed");
    let store = rig.open();
    let facts = store.get_facts().unwrap();
    let fable = facts
        .iter()
        .find(|fact| fact.model_id == "claude-fable-5")
        .expect("fable-5 fact row exists after migration 008");
    assert_eq!(fable.provider_id, "anthropic");
    assert_eq!(fable.family.as_deref(), Some("claude"));
    assert_eq!(fable.scores.elo, 1_911.0);
    assert!(fable.scores.eq > 2115.5, "eq curated above opus-4-8");
    // Migration 019 corrected this to the real list price ($10 in / $50 out);
    // the row seeded here at 25 understated it by half, which only became
    // load-bearing once cost stopped being zeroed for subscription routes.
    assert_eq!(fable.cost_per_m_tok_out, Some(50.0));
}

#[test]
fn migration_009_seeds_kimi_k3_one_notch_below_opus() {
    let rig = Rig::new("kimi-k3-seed");
    let store = rig.open();
    let facts = store.get_facts().unwrap();
    let opus = facts
        .iter()
        .find(|fact| fact.provider_id == "anthropic" && fact.model_id == "claude-opus-4-8")
        .expect("opus-4-8 fact row");
    let kimi = facts
        .iter()
        .find(|fact| fact.provider_id == "kimi-for-coding" && fact.model_id == "k3")
        .expect("kimi k3 fact row exists after migration 009");
    assert_eq!(kimi.provider_id, "kimi-for-coding");
    assert_eq!(kimi.family.as_deref(), Some("kimi"));
    assert_eq!(kimi.scores.elo, 1_742.0);
    assert_eq!(opus.scores.elo, 1_728.0);
    assert_eq!(kimi.scores.eq, opus.scores.eq - 200.0);
    assert_eq!(kimi.context_window, Some(256_000.0));
}

#[test]
fn migrations_seed_and_rescore_qwen38_max_into_the_k3_band() {
    let rig = Rig::new("qwen38-max-seed");
    let store = rig.open();
    let facts = store.get_facts().unwrap();
    let k3 = facts
        .iter()
        .find(|fact| fact.provider_id == "kimi-for-coding" && fact.model_id == "k3")
        .expect("k3 fact row");
    let qwen = facts
        .iter()
        .find(|fact| {
            fact.provider_id == "alibaba-token-plan" && fact.model_id == "qwen3.8-max-preview"
        })
        .expect("qwen3.8-max-preview fact row exists after migrations 010+011");
    assert_eq!(qwen.scores.elo, 1_721.0);
    assert_eq!(k3.scores.elo, 1_742.0);
    assert_eq!(qwen.scores.eq, 1895.0);
    assert_eq!(qwen.family.as_deref(), Some("qwen"));
    assert_eq!(qwen.context_window, Some(256_000.0));
}

#[test]
fn provider_cooldown_is_durable_non_shortening_and_inherited_by_new_routes() {
    let rig = Rig::new("provider-cooldown-inheritance");
    let store = rig.open();
    store
        .upsert_fact(&fact("p", "a"), FactSource::Override)
        .unwrap();
    store
        .upsert_fact(&fact("p", "b"), FactSource::Override)
        .unwrap();

    // Route-specific state is longer than the first provider horizon and must
    // survive the provider fanout unchanged.
    store
        .update_route_state(
            "p/a/high",
            "p/a",
            &RouteStateUpdate {
                cooldown_until: Some(20_000.0),
                observed_at_ms: Some(300),
                ..Default::default()
            },
        )
        .unwrap();
    store
        .record_provider_cooldown(
            "p",
            "p/a/low",
            "p/a",
            &RouteStateUpdate {
                cooldown_until: Some(10_000.0),
                quota_exhausted_until: Some(30_000.0),
                increment_quota_error: true,
                observed_at_ms: Some(200),
                ..Default::default()
            },
        )
        .unwrap();

    let provider = store
        .get_provider_cooldown("p")
        .unwrap()
        .expect("provider state persisted");
    assert_eq!(provider.provider_key, "p");
    assert_eq!(provider.cooldown_until, Some(10_000.0));
    assert_eq!(provider.quota_exhausted_until, Some(30_000.0));

    let snapshot = store.build_snapshot(1_000.0).unwrap();
    assert_eq!(snapshot.by_route["p/a/high"].cooldown_until, Some(20_000.0));
    assert_eq!(snapshot.by_route["p/a/high"].quota_exhausted_until, None);
    assert_eq!(snapshot.by_route["p/b"].cooldown_until, Some(10_000.0));
    assert_eq!(
        snapshot.by_route["p/b"].quota_exhausted_until,
        Some(30_000.0)
    );

    // An out-of-order signal cannot replace newer provider evidence. The
    // independent model wall does not inherit a provider evidence generation.
    store
        .record_provider_cooldown(
            "p",
            "p/a/low",
            "p/a",
            &RouteStateUpdate {
                cooldown_until: Some(5_000.0),
                quota_exhausted_until: Some(25_000.0),
                observed_at_ms: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
    let provider = store
        .get_provider_cooldown("p")
        .unwrap()
        .expect("provider state remains");
    assert_eq!(provider.cooldown_until, Some(10_000.0));
    assert_eq!(provider.quota_exhausted_until, Some(30_000.0));
    let snapshot = store.build_snapshot(1_000.0).unwrap();
    assert_eq!(snapshot.by_route["p/a/high"].cooldown_until, Some(20_000.0));
    assert_eq!(snapshot.by_route["p/a/high"].quota_exhausted_until, None);

    // Minting after the provider write inherits the live provider horizons.
    store
        .upsert_fact(&fact("p", "c"), FactSource::Override)
        .unwrap();
    // The direct route-state minting path inherits the same provider horizons.
    store
        .update_route_state("p/d/high", "p/d", &RouteStateUpdate::default())
        .unwrap();
    let snapshot = store.build_snapshot(1_000.0).unwrap();
    assert_eq!(snapshot.by_route["p/c"].cooldown_until, Some(10_000.0));
    assert_eq!(
        snapshot.by_route["p/c"].quota_exhausted_until,
        Some(30_000.0)
    );
    assert_eq!(snapshot.by_route["p/d/high"].cooldown_until, Some(10_000.0));
    assert_eq!(
        snapshot.by_route["p/d/high"].quota_exhausted_until,
        Some(30_000.0)
    );
}

#[test]
fn provider_key_derivation_is_shared_for_route_and_capacity_keys() {
    assert_eq!(derive_provider_key(" openai/gpt-5.5/high "), Some("openai"));
    assert_eq!(derive_provider_key("openai/gpt-5.5"), Some("openai"));
    assert_eq!(derive_provider_key("openai"), Some("openai"));
    assert_eq!(derive_provider_key("/gpt-5.5"), None);
    assert_eq!(derive_provider_key("   "), None);
}

#[test]
fn provider_record_and_route_insertion_are_serialized_without_a_missed_horizon() {
    use std::sync::Arc;
    use std::thread;

    let rig = Rig::new("provider-route-concurrency");
    let store = Arc::new(rig.open());
    store
        .upsert_fact(&fact("p", "base"), FactSource::Override)
        .unwrap();
    let recorder = Arc::clone(&store);
    let inserter = Arc::clone(&store);

    let provider_thread = thread::spawn(move || {
        recorder
            .record_provider_cooldown(
                "p",
                "p/base/high",
                "p/base",
                &RouteStateUpdate {
                    cooldown_until: Some(50_000.0),
                    quota_exhausted_until: Some(60_000.0),
                    ..Default::default()
                },
            )
            .unwrap();
    });
    let insert_thread = thread::spawn(move || {
        inserter
            .upsert_fact(&fact("p", "minted"), FactSource::Override)
            .unwrap();
    });
    provider_thread.join().unwrap();
    insert_thread.join().unwrap();

    let snapshot = store.build_snapshot(1_000.0).unwrap();
    let minted = snapshot
        .by_route
        .get("p/minted")
        .expect("minted route state");
    assert_eq!(minted.cooldown_until, Some(50_000.0));
    assert_eq!(minted.quota_exhausted_until, Some(60_000.0));
}

#[test]
fn provider_fanout_failure_rolls_back_provider_state_and_route_changes() {
    use rusqlite::Connection;

    let rig = Rig::new("provider-cooldown-rollback");
    let store = rig.open();
    store
        .upsert_fact(&fact("p", "a"), FactSource::Override)
        .unwrap();

    // Inject a failure only into the fanout statement. The provider row is
    // written first, so observing no provider row afterward proves the whole
    // provider write and fanout share one transaction.
    let connection = Connection::open(rig.db_path()).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_provider_fanout
             BEFORE UPDATE ON model_route_state
             WHEN OLD.provider_id = 'p'
             BEGIN SELECT RAISE(ABORT, 'injected provider fanout failure'); END;",
        )
        .unwrap();
    let result = store.record_provider_cooldown(
        "p",
        "p/a",
        "p/a",
        &RouteStateUpdate {
            cooldown_until: Some(90_000.0),
            ..Default::default()
        },
    );
    assert!(result.is_err(), "fault injection must fail the fanout");
    assert!(
        store.get_provider_cooldown("p").unwrap().is_none(),
        "provider state must roll back with the fanout"
    );
    let snapshot = store.build_snapshot(1_000.0).unwrap();
    assert_eq!(snapshot.by_route["p/a"].cooldown_until, None);
}

#[test]
fn migration_022_renames_every_capability_key_and_applies_resolved_elos() {
    let rig = Rig::new("elo-rescore");
    let store = rig.open();
    let facts = store.get_facts().unwrap();
    assert_eq!(facts.len(), CATALOG_LEN);
    let elo = |route_key: &str| {
        facts
            .iter()
            .find(|fact| {
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
                    == route_key
            })
            .map(|fact| fact.scores.elo)
            .unwrap()
    };
    let resolved = [
        ("alibaba-token-plan/qwen3.7-max", 1254.0),
        ("alibaba-token-plan/qwen3.7-plus", 715.0),
        ("alibaba-token-plan/qwen3.8-max", 1721.0),
        ("alibaba-token-plan/qwen3.8-max-preview", 1721.0),
        ("anthropic/claude-fable-5", 1911.0),
        ("anthropic/claude-opus-4-5", 698.0),
        ("anthropic/claude-opus-4-6", 1130.0),
        ("anthropic/claude-opus-4-7", 1635.0),
        ("anthropic/claude-opus-4-8", 1728.0),
        ("anthropic/claude-opus-5", 1905.0),
        ("anthropic/claude-sonnet-4-6", 637.0),
        ("google/antigravity-gemini-3.1-pro", 1280.0),
        ("google/antigravity-gemini-3.5-flash", 1345.0),
        ("google/antigravity-gemini-3.6-flash", 1410.0),
        ("kimi-for-coding/k3", 1742.0),
        ("ollama-cloud/deepseek-v4-flash", 1180.0),
        ("ollama-cloud/deepseek-v4-flash:0731", 1610.0),
        ("ollama-cloud/deepseek-v4-pro", 1284.0),
        ("ollama-cloud/glm-5.1", 988.0),
        ("ollama-cloud/glm-5.2", 1660.0),
        ("ollama-cloud/kimi-k2.5", 599.0),
        ("ollama-cloud/kimi-k2.6", 1215.0),
        ("ollama-cloud/kimi-k2.7-code", 1102.0),
        ("ollama-cloud/minimax-m2.5", 520.0),
        ("ollama-cloud/minimax-m2.7", 984.0),
        ("ollama-cloud/minimax-m3", 1311.0),
        ("ollama-cloud/mistral-large-3", 520.0),
        ("openai/gpt-5.4", 1589.0),
        ("openai/gpt-5.4-mini", 956.0),
        ("openai/gpt-5.5", 1690.0),
        ("openai/gpt-5.6-luna", 1621.0),
        ("openai/gpt-5.6-sol", 1838.0),
        ("openai/gpt-5.6-terra", 1685.0),
        ("xai/grok-4.3", 659.0),
        ("xai/grok-4.5", 1649.0),
    ];
    for (route_key, expected) in resolved {
        assert_eq!(elo(route_key), expected, "resolved Elo for {route_key}");
    }
    assert_eq!(elo("openai/gpt-5.6-sol-pro"), 1_838.0);
    assert_eq!(elo("xai/grok-build-0.1"), 40.0 * 33.0);
    drop(store);

    let connection = rusqlite::Connection::open(rig.db_path()).unwrap();
    let (elo_rows, legacy_rows): (i64, i64) = connection
        .query_row(
            "SELECT
               sum(json_type(scores_json, '$.elo') IS NOT NULL),
               sum(json_type(scores_json, '$.iq') IS NOT NULL)
             FROM model_facts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(elo_rows as usize, CATALOG_LEN);
    assert_eq!(legacy_rows, 0);
}

#[test]
fn aa_inferred_does_not_overwrite_override_and_override_wins() {
    let rig = Rig::new("aa-inferred-precedence");
    let store = rig.open();
    let mut override_fact = fact("custom", "override-probe");
    override_fact.scores.elo = 1_500.0;
    store
        .upsert_fact(&override_fact, FactSource::Override)
        .unwrap();

    let mut inferred = override_fact.clone();
    inferred.scores.elo = 900.0;
    inferred.score_provenance = ScoreProvenance::AaInferred;
    inferred.aa_index = Some(40.0);
    inferred.review_by_ms = Some(1);
    let err = store
        .upsert_fact(&inferred, FactSource::AaInferred)
        .expect_err("aa_inferred must refuse to clobber an override");
    assert!(
        err.0.contains("aa_inferred does not overwrite"),
        "got {err:?}"
    );
    let kept = store
        .get_fact_by_route_key("custom/override-probe")
        .unwrap()
        .unwrap();
    assert_eq!(kept.scores.elo, 1_500.0);
    assert_eq!(kept.score_provenance, ScoreProvenance::Override);
    assert_eq!(kept.aa_index, None);

    // Other direction: an aa_inferred row is replaced by an override.
    let mut inferred_only = fact("custom", "inferred-probe");
    inferred_only.scores.elo = 800.0;
    inferred_only.score_provenance = ScoreProvenance::AaInferred;
    inferred_only.aa_index = Some(30.0);
    inferred_only.review_by_ms = Some(2);
    store
        .upsert_fact(&inferred_only, FactSource::AaInferred)
        .unwrap();
    let mut as_override = inferred_only.clone();
    as_override.scores.elo = 1_600.0;
    as_override.score_provenance = ScoreProvenance::Override;
    store
        .upsert_fact(&as_override, FactSource::Override)
        .unwrap();
    let won = store
        .get_fact_by_route_key("custom/inferred-probe")
        .unwrap()
        .unwrap();
    assert_eq!(won.scores.elo, 1_600.0);
    assert_eq!(won.score_provenance, ScoreProvenance::Override);
    assert_eq!(won.aa_index, None);
}

#[test]
fn aa_inferred_does_not_overwrite_curated_seed() {
    let rig = Rig::new("aa-inferred-seed-precedence");
    let store = rig.open();
    let mut inferred = store
        .get_fact_by_route_key("openai/gpt-5.6-sol")
        .unwrap()
        .unwrap();
    let original_elo = inferred.scores.elo;
    inferred.scores.elo = 12.0;
    inferred.score_provenance = ScoreProvenance::AaInferred;
    inferred.aa_index = Some(1.0);
    store
        .upsert_fact(&inferred, FactSource::AaInferred)
        .expect_err("aa_inferred must refuse to clobber a curated seed row");
    let kept = store
        .get_fact_by_route_key("openai/gpt-5.6-sol")
        .unwrap()
        .unwrap();
    assert_eq!(kept.scores.elo, original_elo);
    assert_eq!(kept.score_provenance, ScoreProvenance::Curated);
}

#[test]
fn list_inferred_flags_a_row_past_review_by() {
    let rig = Rig::new("aa-inferred-review-by");
    let store = rig.open();
    let mut inferred = fact("custom", "stale-inferred");
    inferred.scores.elo = 400.0;
    inferred.score_provenance = ScoreProvenance::AaInferred;
    inferred.aa_index = Some(20.0);
    inferred.review_by_ms = Some(1_000);
    store
        .upsert_fact(&inferred, FactSource::AaInferred)
        .unwrap();
    let rows = store.list_inferred_rows(2_000).unwrap();
    let row = rows
        .iter()
        .find(|row| row.route_key == "custom/stale-inferred")
        .expect("inferred row listed");
    assert_eq!(row.aa_index, Some(20.0));
    assert!((row.elo - 400.0).abs() < 1e-9);
    assert_eq!(row.review_by_ms, Some(1_000));
    assert!(row.review_by_passed, "deadline 1000 is past now=2000");

    let fresh = store.list_inferred_rows(500).unwrap();
    let fresh_row = fresh
        .iter()
        .find(|row| row.route_key == "custom/stale-inferred")
        .unwrap();
    assert!(!fresh_row.review_by_passed);
}

#[test]
fn unscored_rows_retain_provenance_in_the_fact_loader() {
    let rig = Rig::new("unscored-hidden");
    let store = rig.open();
    let mut unscored = fact("custom", "unscored-probe");
    unscored.score_provenance = ScoreProvenance::Unscored;
    store.upsert_fact(&unscored, FactSource::Unscored).unwrap();
    let loaded = store
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|fact| fact.provider_id == "custom" && fact.model_id == "unscored-probe")
        .expect("the admission layer needs the unscored provenance");
    assert_eq!(loaded.score_provenance, ScoreProvenance::Unscored);
    assert_eq!(
        store.list_unscored_rows().unwrap()[0].route_key,
        "custom/unscored-probe"
    );
    let by_key = store
        .get_fact_by_route_key("custom/unscored-probe")
        .unwrap()
        .unwrap();
    assert_eq!(by_key.score_provenance, ScoreProvenance::Unscored);
}

#[test]
fn fusiform_precedence_updates_seed_facts_but_preserves_scores_and_same_field_overrides() {
    let rig = Rig::new("fusiform-precedence");
    let store = rig.open();
    let route_key = make_route_key("anthropic", "claude-opus-4-5", None);
    let initial = store.get_fact_by_route_key(&route_key).unwrap().unwrap();
    let initial_elo = initial.scores.elo;
    assert_eq!(initial.supports_vision, Some(true));

    let update = |vision| FusiformFactUpdate {
        auth_methods: None,
        provider_id: "anthropic".to_string(),
        model_id: "claude-opus-4-5".to_string(),
        present: true,
        context_window: Some(250_000.0),
        max_output_tokens: Some(32_000),
        cost_tier: Some(3.0),
        cost_per_mtok_out: Some(12.0),
        input_rate: Some(2.5),
        cache_read_rate: Some(0.25),
        cache_write_rate: Some(3.125),
        reasoning_rate: Some(12.0),
        supports_vision: Some(vision),
        supports_tool_use: Some(true),
        reasoning_efforts: None,
        canonical_id: None,
        upstream_status: None, model_type: None, model_kind: None,
        lineage: None,
    };
    store
        .apply_fusiform_catalog(
            &[update(false)],
            FusiformCatalogCursor {
                catalog_version: 1,
                resolved_at_ms: 100,
            },
        )
        .unwrap();
    let fusiform = store.get_fact_by_route_key(&route_key).unwrap().unwrap();
    assert_eq!(fusiform.supports_vision, Some(false));
    assert_eq!(fusiform.input_rate, Some(2.5));
    assert_eq!(fusiform.cache_read_rate, Some(0.25));
    assert_eq!(fusiform.cache_write_rate, Some(3.125));
    assert_eq!(fusiform.reasoning_rate, Some(12.0));
    assert_eq!(
        fusiform.scores.elo, initial_elo,
        "Fusiform cannot alter Elo"
    );

    let mut operator = fusiform;
    operator.scores.elo = initial_elo + 321.0;
    operator.supports_vision = Some(false);
    store.upsert_fact(&operator, FactSource::Override).unwrap();
    store
        .mark_operator_fact_fields(&route_key, &[CatalogFactField::SupportsVision])
        .unwrap();
    store
        .apply_fusiform_catalog(
            &[update(true)],
            FusiformCatalogCursor {
                catalog_version: 2,
                resolved_at_ms: 200,
            },
        )
        .unwrap();
    let replayed = store.get_fact_by_route_key(&route_key).unwrap().unwrap();
    assert_eq!(
        replayed.scores.elo,
        initial_elo + 321.0,
        "operator Elo must survive replay"
    );
    assert_eq!(
        replayed.supports_vision,
        Some(false),
        "an override of the same factual field has precedence"
    );
    let catalog = store
        .get_catalog_facts()
        .unwrap()
        .into_iter()
        .find(|row| row.fact.provider_id == "anthropic" && row.fact.model_id == "claude-opus-4-5")
        .unwrap();
    assert_eq!(catalog.facts_source.as_deref(), Some("fusiform"));
    assert_eq!(catalog.fusiform_version, Some(2));

    assert_eq!(
        store
            .clear_operator_fact_fields(&route_key, &[CatalogFactField::SupportsVision])
            .unwrap(),
        vec![CatalogFactField::SupportsVision]
    );
    store
        .apply_fusiform_catalog(
            &[update(true)],
            FusiformCatalogCursor {
                catalog_version: 3,
                resolved_at_ms: 300,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .get_fact_by_route_key(&route_key)
            .unwrap()
            .unwrap()
            .supports_vision,
        Some(true),
        "Fusiform must resume ownership after the operator stamp is cleared"
    );
}

#[test]
fn fusiform_catalog_keys_on_provider_and_model_never_bare_model_id() {
    // Two operator-known providers (both seeded in the catalog) serve the same
    // model spelling: two rows, keyed on the pair, never a bare-id merge.
    let rig = Rig::new("fusiform-pair-key");
    let store = rig.open();
    let fact = |provider: &str| FusiformFactUpdate {
        auth_methods: None,
        provider_id: provider.to_string(),
        model_id: "shared-spelling".to_string(),
        present: true,
        context_window: Some(128_000.0),
        max_output_tokens: Some(8_192),
        cost_tier: Some(1.0),
        cost_per_mtok_out: Some(2.0),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(provider == "anthropic"),
        supports_tool_use: Some(true),
        reasoning_efforts: None,
        canonical_id: None,
        upstream_status: None, model_type: None, model_kind: None,
        lineage: None,
    };
    store
        .apply_fusiform_catalog(
            &[fact("anthropic"), fact("openai")],
            FusiformCatalogCursor {
                catalog_version: 7,
                resolved_at_ms: 700,
            },
        )
        .unwrap();

    let rows = store
        .get_catalog_facts()
        .unwrap()
        .into_iter()
        .filter(|row| row.fact.model_id == "shared-spelling")
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].fact.provider_id, "anthropic");
    assert_eq!(rows[0].fact.supports_vision, Some(true));
    assert_eq!(rows[1].fact.provider_id, "openai");
    assert_eq!(rows[1].fact.supports_vision, Some(false));
}

#[test]
fn fusiform_first_sight_rows_are_minted_only_under_operator_known_providers() {
    // Live catalog: ~7,500 models across ~200 providers. A first-sight row exists
    // only under a provider that already has any fact row; the rest are not stored.
    let rig = Rig::new("fusiform-provider-scope");
    let store = rig.open();
    let fact = |provider: &str, model: &str| FusiformFactUpdate {
        auth_methods: None,
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        present: true,
        context_window: Some(64_000.0),
        max_output_tokens: Some(4_096),
        cost_tier: Some(1.0),
        cost_per_mtok_out: Some(1.0),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(false),
        supports_tool_use: Some(true),
        reasoning_efforts: None,
        canonical_id: None,
        upstream_status: None, model_type: None, model_kind: None,
        lineage: None,
    };
    store
        .apply_fusiform_catalog(
            &[
                fact("302ai", "MiniMax-M2.7"),
                fact("anthropic", "claude-never-seen"),
                fact("anthropic", "claude-opus-5-fast"),
            ],
            FusiformCatalogCursor {
                catalog_version: 9,
                resolved_at_ms: 900,
            },
        )
        .unwrap();
    let rows = store.get_catalog_facts().unwrap();
    assert!(
        !rows.iter().any(|row| row.fact.provider_id == "302ai"),
        "an unknown provider's model must not be stored"
    );
    let minted = rows
        .iter()
        .find(|row| row.fact.model_id == "claude-never-seen")
        .expect("a known provider's new model is minted");
    assert_eq!(minted.fact.provider_id, "anthropic");
    assert!(
        !rows
            .iter()
            .any(|row| row.fact.model_id == "claude-opus-5-fast"),
        "Fusiform -fast IDs must never be added to routing"
    );
}

/// A model the upstream withdrew is served as absent when retired models are
/// requested; its fact row leaves the routable set on that write, and a model
/// first seen already withdrawn is not minted at all.
#[test]
fn withdrawn_model_leaves_the_routable_set_and_is_never_minted_on_first_sight() {
    let rig = Rig::new("fusiform-withdrawn");
    let store = rig.open();
    let route_key = make_route_key("anthropic", "claude-opus-4-5", None);
    let withdrawn = |provider: &str, model: &str| FusiformFactUpdate {
        auth_methods: None,
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        present: false,
        context_window: Some(200_000.0),
        max_output_tokens: Some(32_000),
        cost_tier: Some(3.0),
        cost_per_mtok_out: Some(12.0),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(true),
        supports_tool_use: Some(true),
        reasoning_efforts: None,
        canonical_id: None,
        upstream_status: None, model_type: None, model_kind: None,
        lineage: None,
    };
    let routable_before = store.get_facts().unwrap().len();
    assert!(store.get_facts().unwrap().iter().any(|fact| make_route_key(
        &fact.provider_id,
        &fact.model_id,
        fact.variant.as_deref()
    ) == route_key));

    store
        .apply_fusiform_catalog(
            &[
                withdrawn("anthropic", "claude-opus-4-5"),
                withdrawn("anthropic", "never-seen-before"),
            ],
            FusiformCatalogCursor {
                catalog_version: 5,
                resolved_at_ms: 500,
            },
        )
        .unwrap();

    let routable = store.get_facts().unwrap();
    assert!(
        routable.iter().all(|fact| make_route_key(
            &fact.provider_id,
            &fact.model_id,
            fact.variant.as_deref()
        ) != route_key),
        "a withdrawn model is not offered to selection"
    );
    assert_eq!(routable.len(), routable_before - 1);
    let stored = store
        .get_catalog_facts()
        .unwrap()
        .into_iter()
        .find(|row| {
            make_route_key(
                &row.fact.provider_id,
                &row.fact.model_id,
                row.fact.variant.as_deref(),
            ) == route_key
        })
        .expect("the row survives with its scores and records the absence");
    assert_eq!(stored.fusiform_present, Some(false));
    assert!(
        store
            .get_fact_by_route_key(&make_route_key("anthropic", "never-seen-before", None))
            .unwrap()
            .is_none(),
        "a model first seen withdrawn mints no row"
    );
}

/// The apply reads the stored (provider, model) pairs once per batch, so a row
/// minted early in a batch must count as stored for the rest of it: a second
/// entry for the same pair updates that row instead of minting it again, and
/// unknown providers stay out.
#[test]
fn fusiform_apply_treats_a_pair_minted_earlier_in_the_batch_as_stored() {
    let rig = Rig::new("fusiform-batch-repeat");
    let store = rig.open();
    let entry = |provider: &str, model: &str, context_window: f64| FusiformFactUpdate {
        auth_methods: None,
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        present: true,
        context_window: Some(context_window),
        max_output_tokens: Some(4_096),
        cost_tier: Some(1.0),
        cost_per_mtok_out: Some(1.0),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(false),
        supports_tool_use: Some(true),
        reasoning_efforts: None,
        canonical_id: None,
        upstream_status: None, model_type: None, model_kind: None,
        lineage: None,
    };
    let before = store.get_catalog_facts().unwrap().len();

    store
        .apply_fusiform_catalog_for_providers(
            &[
                entry("mapped-provider", "model-a", 1_000.0),
                entry("mapped-provider", "model-a", 2_000.0),
                entry("mapped-provider", "model-b", 3_000.0),
                entry("anthropic", "claude-batch-repeat", 4_000.0),
                entry("anthropic", "claude-batch-repeat", 5_000.0),
                entry("unmapped-provider", "model-c", 6_000.0),
            ],
            FusiformCatalogCursor {
                catalog_version: 11,
                resolved_at_ms: 1_100,
            },
            &std::collections::HashSet::from(["mapped-provider".to_string()]),
        )
        .unwrap();

    let rows = store.get_catalog_facts().unwrap();
    assert_eq!(rows.len(), before + 3, "three distinct pairs are minted");
    let context = |provider: &str, model: &str| {
        let matching = rows
            .iter()
            .filter(|row| row.fact.provider_id == provider && row.fact.model_id == model)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "{provider}/{model} has one row");
        matching[0].fact.context_window
    };
    assert_eq!(context("mapped-provider", "model-a"), Some(2_000.0));
    assert_eq!(context("mapped-provider", "model-b"), Some(3_000.0));
    assert_eq!(context("anthropic", "claude-batch-repeat"), Some(5_000.0));
    assert!(!rows
        .iter()
        .any(|row| row.fact.provider_id == "unmapped-provider"));
}

#[test]
fn fusiform_cursor_is_committed_with_the_applied_snapshot() {
    let rig = Rig::new("fusiform-cursor");
    let store = rig.open();
    assert_eq!(store.fusiform_catalog_cursor().unwrap(), None);
    let cursor = FusiformCatalogCursor {
        catalog_version: 11,
        resolved_at_ms: 1_234,
    };
    store.apply_fusiform_catalog(&[], cursor).unwrap();
    assert_eq!(store.fusiform_catalog_cursor().unwrap(), Some(cursor));
}

fn class_obs(
    id: &str,
    class: Option<&str>,
    input: Option<f64>,
    out: Option<f64>,
    reasoning: Option<f64>,
    cached_read: Option<f64>,
    cache_write: Option<f64>,
    dur: Option<f64>,
    at: f64,
) -> UsageObservation {
    UsageObservation {
        id: id.into(),
        route_key: "p/m".into(),
        capacity_key: "p/m".into(),
        provider_id: "p".into(),
        input_tokens: input,
        output_tokens: out.unwrap_or(0.0),
        reasoning_tokens: reasoning,
        cached_read_tokens: cached_read,
        cache_write_tokens: cache_write,
        duration_ms: dur.unwrap_or(-1.0),
        workload_class: class.map(str::to_string),
        account_key: None,
        observed_at: at,
    }
}

#[test]
fn get_facts_with_observed_speed_computes_class_vector_arithmetic_mean_over_trailing_window() {
    let rig = Rig::new("class-vector-mean");
    let store = rig.open();
    let now = 10_000_000.0;

    let mut total_fresh_input = 0.0;
    let mut total_output_excl = 0.0;
    let mut total_reasoning = 0.0;
    let mut total_cached_read = 0.0;
    let mut total_cache_write = 0.0;

    for i in 0..50 {
        let input = 1000.0 + (i * 10) as f64;
        let reasoning = 100.0 + (i * 5) as f64;
        let output_excl = 500.0 + (i * 20) as f64;
        let output = output_excl + reasoning;
        let cached_read = 2000.0 + (i * 50) as f64;
        let cache_write = 50.0 + i as f64;
        // Last row has NULL duration, but all 5 token counters non-NULL
        let dur = if i == 49 { None } else { Some(1000.0) };

        total_fresh_input += input;
        total_output_excl += output_excl;
        total_reasoning += reasoning;
        total_cached_read += cached_read;
        total_cache_write += cache_write;

        store
            .record_usage_delivery(&class_obs(
                &format!("msg-{i}"),
                Some("ephemeral"),
                Some(input),
                Some(output),
                Some(reasoning),
                Some(cached_read),
                Some(cache_write),
                dur,
                now - 1000.0 - (i * 10) as f64,
            ))
            .unwrap();
    }

    let facts = store
        .get_facts_with_observed_speed(now, &RoutePricing::default())
        .unwrap();
    assert!(!facts.is_empty());
    for fact in &facts {
        let vec = fact
            .class_vector
            .as_ref()
            .expect("ephemeral vector must be present on overlaid facts");
        assert_eq!(vec.workload_class, "ephemeral");
        assert_eq!(vec.sample_count, 50);
        assert!(vec.is_usable());
        assert!(fact.class_vector_is_overlay);
        assert!((vec.fresh_input - total_fresh_input / 50.0).abs() < 1e-9);
        assert!((vec.output_excluding_reasoning - total_output_excl / 50.0).abs() < 1e-9);
        assert!((vec.reasoning - total_reasoning / 50.0).abs() < 1e-9);
        assert!((vec.cached_read - total_cached_read / 50.0).abs() < 1e-9);
        assert!((vec.cache_write - total_cache_write / 50.0).abs() < 1e-9);
    }
}

#[test]
fn complete_ephemeral_row_trailing_window_boundary_pinned_to_now_argument() {
    let rig = Rig::new("class-vector-boundary");
    let store = rig.open();
    let now = 1_000_000_000.0;
    let seven_days_ms = 7.0 * 24.0 * 60.0 * 60.0 * 1000.0;

    // now - 7d exactly: NOT in (now - 7d, now]
    store
        .record_usage_delivery(&class_obs(
            "at-boundary-old",
            Some("ephemeral"),
            Some(100.0),
            Some(100.0),
            Some(0.0),
            Some(100.0),
            Some(10.0),
            Some(1000.0),
            now - seven_days_ms,
        ))
        .unwrap();

    // now - 7d - 1000.0: NOT in
    store
        .record_usage_delivery(&class_obs(
            "older-than-boundary",
            Some("ephemeral"),
            Some(100.0),
            Some(100.0),
            Some(0.0),
            Some(100.0),
            Some(10.0),
            Some(1000.0),
            now - seven_days_ms - 1000.0,
        ))
        .unwrap();

    // now - 7d + 1.0 (eps): IN
    store
        .record_usage_delivery(&class_obs(
            "inside-lower-eps",
            Some("ephemeral"),
            Some(200.0),
            Some(100.0),
            Some(0.0),
            Some(100.0),
            Some(10.0),
            Some(1000.0),
            now - seven_days_ms + 1.0,
        ))
        .unwrap();

    // now exactly: IN
    store
        .record_usage_delivery(&class_obs(
            "inside-upper",
            Some("ephemeral"),
            Some(300.0),
            Some(100.0),
            Some(0.0),
            Some(100.0),
            Some(10.0),
            Some(1000.0),
            now,
        ))
        .unwrap();

    // now + 1.0 (future relative to now): NOT in
    store
        .record_usage_delivery(&class_obs(
            "future",
            Some("ephemeral"),
            Some(400.0),
            Some(100.0),
            Some(0.0),
            Some(100.0),
            Some(10.0),
            Some(1000.0),
            now + 1.0,
        ))
        .unwrap();

    let vec = store
        .get_class_vector("ephemeral", now)
        .unwrap()
        .expect("vector formed from rows in window");
    assert_eq!(
        vec.sample_count, 2,
        "exactly now-7d+eps and now are in population"
    );
    assert!((vec.fresh_input - 250.0).abs() < 1e-9);
}

// A Broca-reported run reaches the class vector with its reasoning counted once.
// Source: a real Codex mason run's Broca usage block (below). Broca's canonical
// output_tokens includes reasoning (broca-protocol crates/broca-protocol/src/
// usage.rs:22 in Broca checkout 6c45a375: reasoning is "ALWAYS A SUBSET of
// `output_tokens`"). The vector's output_excluding_reasoning must be the 28,338
// visible tokens; 39,620 would price the 11,282 reasoning tokens twice.
#[test]
fn broca_codex_run_usage_reaches_the_class_vector_with_reasoning_counted_once() {
    use striatum_core::model_resolution::build_usage_observation;
    use striatum_core::{BrocaUsageFacts, NativeUsageFacts};

    let rig = Rig::new("class-vector-broca-reasoning");
    let store = rig.open();
    let now = 10_000_000.0;
    let usage: BrocaUsageFacts = serde_json::from_str(
        r#"{"input_tokens":339380,"cached_input_tokens":20647936,"cache_write_tokens":0,"output_tokens":39620,"reasoning_tokens":11282}"#,
    )
    .unwrap();
    let mut facts = NativeUsageFacts {
        message_id: "codex-mason-run".into(),
        provider_id: "openai".into(),
        model_id: "gpt-6.1-sol".into(),
        variant: Some("xhigh".into()),
        agent: Some("mason".into()),
        created_ms: now - 2_000.0,
        completed_ms: now - 1_000.0,
        ..Default::default()
    };
    facts.apply_broca_usage(Some(&usage));
    let observation = build_usage_observation(&facts, &[]).unwrap();
    store.record_usage_delivery(&observation).unwrap();

    let vector = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    assert_eq!(vector.sample_count, 1);
    assert!((vector.output_excluding_reasoning - 28_338.0).abs() < 1e-9);
    assert!((vector.reasoning - 11_282.0).abs() < 1e-9);
    assert!(
        (vector.output_excluding_reasoning + vector.reasoning - 39_620.0).abs() < 1e-9,
        "visible + reasoning must equal Broca's output total"
    );
    assert!((vector.fresh_input - 339_380.0).abs() < 1e-9);
    assert!((vector.cached_read - 20_647_936.0).abs() < 1e-9);
    assert_eq!(vector.cache_write, 0.0);
}

#[test]
fn class_vector_floor_50_requires_complete_rows_and_null_duration_reaches_floor() {
    let rig = Rig::new("class-vector-floor-50");
    let store = rig.open();
    let now = 10_000_000.0;

    // 49 incomplete rows (NULL cached read)
    for i in 0..49 {
        store
            .record_usage_delivery(&class_obs(
                &format!("incomplete-{i}"),
                Some("ephemeral"),
                Some(0.0),
                Some(100.0),
                Some(0.0),
                None,
                Some(0.0),
                Some(1000.0),
                now - 1000.0 - i as f64,
            ))
            .unwrap();
    }
    // 1 complete row with cached_read = 50.0
    store
        .record_usage_delivery(&class_obs(
            "complete-1",
            Some("ephemeral"),
            Some(100.0),
            Some(100.0),
            Some(0.0),
            Some(50.0),
            Some(10.0),
            Some(1000.0),
            now - 500.0,
        ))
        .unwrap();

    let vec = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    assert_eq!(
        vec.sample_count, 1,
        "only complete-case rows count toward floor"
    );
    assert!(!vec.is_usable(), "1 row is unusable (< 50)");
    assert!((vec.cached_read - 50.0).abs() < 1e-9);

    // Plant 48 more complete rows (total 49 complete)
    for i in 2..50 {
        store
            .record_usage_delivery(&class_obs(
                &format!("complete-{i}"),
                Some("ephemeral"),
                Some(100.0),
                Some(100.0),
                Some(0.0),
                Some(50.0),
                Some(10.0),
                Some(1000.0),
                now - 100.0 - i as f64,
            ))
            .unwrap();
    }
    let vec49 = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    assert_eq!(vec49.sample_count, 49);
    assert!(!vec49.is_usable(), "49 complete rows is unusable");

    // 50th complete row: all five counters present, but duration is NULL
    store
        .record_usage_delivery(&class_obs(
            "complete-50-null-duration",
            Some("ephemeral"),
            Some(100.0),
            Some(100.0),
            Some(0.0),
            Some(50.0),
            Some(10.0),
            None,
            now - 10.0,
        ))
        .unwrap();

    let vec50 = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    assert_eq!(
        vec50.sample_count, 50,
        "duration is not part of complete-case predicate"
    );
    assert!(vec50.is_usable(), "50 complete rows makes vector usable");
}

#[test]
fn planted_rows_of_each_enumerated_class_produce_four_separate_vectors_and_selection_consults_ephemeral(
) {
    let rig = Rig::new("class-vectors-four-classes");
    let store = rig.open();
    let now = 10_000_000.0;

    for (class, val) in [
        ("ephemeral", 100.0),
        ("head", 200.0),
        ("gather", 300.0),
        ("hire", 400.0),
    ] {
        for i in 0..50 {
            store
                .record_usage_delivery(&class_obs(
                    &format!("{class}-{i}"),
                    Some(class),
                    Some(val),
                    Some(val),
                    Some(0.0),
                    Some(val),
                    Some(val),
                    Some(1000.0),
                    now - 100.0 - i as f64,
                ))
                .unwrap();
        }
    }

    // Plant 50 unclassified (NULL) and 50 unrecognized class rows
    for i in 0..50 {
        store
            .record_usage_delivery(&class_obs(
                &format!("none-class-{i}"),
                None,
                Some(999.0),
                Some(999.0),
                Some(0.0),
                Some(999.0),
                Some(999.0),
                Some(1000.0),
                now - 100.0 - i as f64,
            ))
            .unwrap();
        store
            .record_usage_delivery(&class_obs(
                &format!("unrec-class-{i}"),
                Some("unrecognized_class"),
                Some(888.0),
                Some(888.0),
                Some(0.0),
                Some(888.0),
                Some(888.0),
                Some(1000.0),
                now - 100.0 - i as f64,
            ))
            .unwrap();
    }

    // Four separate vectors
    let v_eph = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    let v_head = store.get_class_vector("head", now).unwrap().unwrap();
    let v_gather = store.get_class_vector("gather", now).unwrap().unwrap();
    let v_hire = store.get_class_vector("hire", now).unwrap().unwrap();

    assert_eq!(v_eph.workload_class, "ephemeral");
    assert!((v_eph.fresh_input - 100.0).abs() < 1e-9);
    assert_eq!(
        v_eph.sample_count, 50,
        "unrecognized rows never pooled into ephemeral"
    );

    assert_eq!(v_head.workload_class, "head");
    assert!((v_head.fresh_input - 200.0).abs() < 1e-9);

    assert_eq!(v_gather.workload_class, "gather");
    assert!((v_gather.fresh_input - 300.0).abs() < 1e-9);

    assert_eq!(v_hire.workload_class, "hire");
    assert!((v_hire.fresh_input - 400.0).abs() < 1e-9);

    assert!(store
        .get_class_vector("unrecognized_class", now)
        .unwrap()
        .is_none());
    assert!(store.get_class_vector("", now).unwrap().is_none());

    let facts = store
        .get_facts_with_observed_speed(now, &RoutePricing::default())
        .unwrap();
    for fact in facts {
        let cv = fact.class_vector.as_ref().unwrap();
        assert_eq!(cv.workload_class, "ephemeral");
        assert!((cv.fresh_input - 100.0).abs() < 1e-9);
    }
}

#[test]
fn class_vector_lands_as_overlay_value_and_is_never_written_back_to_stored_fact() {
    let rig = Rig::new("class-vector-overlay-persisted");
    let store = rig.open();
    let now = 10_000_000.0;

    let mut custom = fact("p", "custom-model");
    custom.cost_per_m_tok_out = Some(10.0);
    custom.input_rate = Some(2.0);
    custom.cache_read_rate = Some(0.5);
    custom.cache_write_rate = Some(1.5);
    custom.reasoning_rate = Some(10.0);
    store.upsert_fact(&custom, FactSource::Override).unwrap();

    let base_fact = store
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|f| f.provider_id == "p" && f.model_id == "custom-model")
        .unwrap();
    assert!(base_fact.class_vector.is_none());
    assert!(!base_fact.class_vector_is_overlay);
    let baseline_cost = striatum_core::selection_cost(&base_fact, false, true);

    for i in 0..50 {
        store
            .record_usage_delivery(&class_obs(
                &format!("msg-{i}"),
                Some("ephemeral"),
                Some(1000.0),
                Some(1200.0),
                Some(200.0),
                Some(5000.0),
                Some(100.0),
                Some(1000.0),
                now - 100.0 - i as f64,
            ))
            .unwrap();
    }

    // BEFORE overlay: stored fact has NO vector and selection_cost is UNCHANGED
    let raw_fact = store
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|f| f.provider_id == "p" && f.model_id == "custom-model")
        .unwrap();
    assert!(
        raw_fact.class_vector.is_none(),
        "stored fact must have no class_vector"
    );
    assert!(!raw_fact.class_vector_is_overlay);
    let pre_overlay_cost = striatum_core::selection_cost(&raw_fact, false, true);
    assert_eq!(
        pre_overlay_cost, baseline_cost,
        "planted usage rows must NOT change selection_cost before overlay runs"
    );

    // Run overlay: now selection_cost changes
    let overlaid = store
        .get_facts_with_observed_speed(now, &RoutePricing::default())
        .unwrap();
    let overlaid_fact = overlaid
        .into_iter()
        .find(|f| f.provider_id == "p" && f.model_id == "custom-model")
        .unwrap();
    assert!(overlaid_fact.class_vector.is_some());
    assert!(overlaid_fact.class_vector_is_overlay);
    let overlaid_cost = striatum_core::selection_cost(&overlaid_fact, false, true);
    assert_ne!(
        overlaid_cost, baseline_cost,
        "planted usage rows must change selection_cost after overlay runs"
    );

    // Attempt to persist overlaid fact back to store: it must NOT survive into stored fact
    store
        .upsert_fact(&overlaid_fact, FactSource::Override)
        .unwrap();
    let reloaded = store
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|f| f.provider_id == "p" && f.model_id == "custom-model")
        .unwrap();
    assert!(
        reloaded.class_vector.is_none(),
        "vector must never be persisted on a stored fact"
    );
    assert!(!reloaded.class_vector_is_overlay);
}

fn route_input<'a>(facts: Vec<ModelFact>) -> SelectModelRouteInput<'a> {
    let mut available_models = std::collections::HashSet::new();
    for f in &facts {
        available_models.insert(make_route_key(
            &f.provider_id,
            &f.model_id,
            f.variant.as_deref(),
        ));
        available_models.insert(make_capacity_key(&f.provider_id, &f.model_id));
    }
    SelectModelRouteInput {
        target_agent: "mason".to_string(),
        target_is_head: false,
        requirements: ModelRequirements { iq: 50.0, eq: 50.0 },
        available_models,
        facts,
        runtime_snapshot: None,
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),

        required_capabilities: None,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: std::collections::HashSet::new(),
        providers_without_cache_write_reports: std::collections::HashSet::new(),
    }
}

#[test]
fn vector_absence_is_whole_selection_fallback_and_a_missing_rate_degrades_only_its_route() {
    let mut f1 = fact("p1", "m1");
    f1.cost_per_m_tok_out = Some(10.0);
    f1.input_rate = Some(2.0);
    f1.cache_read_rate = Some(0.5);
    f1.cache_write_rate = Some(1.5);
    f1.scores.elo = 1800.0;

    let mut f2 = fact("p2", "m2");
    f2.cost_per_m_tok_out = Some(15.0);
    f2.input_rate = Some(3.0);
    f2.cache_read_rate = Some(0.8);
    f2.cache_write_rate = Some(2.0);
    f2.scores.elo = 1800.0;

    let res_none = select_model_route(route_input(vec![f1.clone(), f2.clone()])).unwrap();
    assert_eq!(res_none.score.objective.cost, 10.0 / 30.0);

    let vec = striatum_core::ClassVector {
        workload_class: "ephemeral".into(),
        fresh_input: 1000.0,
        output_excluding_reasoning: 500.0,
        reasoning: 100.0,
        cached_read: 2000.0,
        cache_write: 50.0,
        sample_count: 50,
        trailing_window_days: 7.0,
        statistic: "arithmetic_mean".into(),
        is_overlay: true,
    };
    f1.class_vector = Some(vec.clone());
    f1.class_vector_is_overlay = true;
    f2.class_vector = Some(vec);
    f2.class_vector_is_overlay = true;

    let res_basket = select_model_route(route_input(vec![f1.clone(), f2.clone()])).unwrap();
    assert_ne!(res_basket.score.objective.cost, 10.0 / 30.0);
    assert_eq!(res_basket.score.objective.cost, f1.basket_cost().unwrap());

    // A missing rate on a used dimension degrades only the route that lacks it
    // (priced at the maximum); f1 keeps its basket cost. Vector absence above is
    // the only thing that still moves the whole selection.
    let mut f2_incomplete = f2.clone();
    f2_incomplete.input_rate = None;
    let res_degraded = select_model_route(route_input(vec![f1.clone(), f2_incomplete])).unwrap();
    assert_eq!(res_degraded.route_key, "p1/m1");
    assert_eq!(res_degraded.score.objective.cost, f1.basket_cost().unwrap());
}
/// A class vector is served from the store's cache until usage is written
/// through the store: a row that appears behind its back is not seen, a row
/// recorded through record_usage_delivery is (Loop R184: before the cache
/// every selection re-read the whole seven-day population).
#[test]
fn class_vector_is_cached_until_usage_is_recorded() {
    let rig = Rig::new("class-vector-cache");
    let store = rig.open();
    let now = 1_800_000_000_000.0;
    store
        .record_usage_delivery(&class_obs(
            "cached-1",
            Some("ephemeral"),
            Some(100.0),
            Some(100.0),
            Some(0.0),
            Some(50.0),
            Some(10.0),
            Some(1000.0),
            now - 500.0,
        ))
        .unwrap();
    let first = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    assert_eq!(first.sample_count, 1);

    // Behind the store's back: a second complete profile in the window.
    let raw = rusqlite::Connection::open(rig.db_path()).unwrap();
    raw.execute(
        "INSERT INTO model_route_usage (id,route_key,capacity_key,provider_id,input_tokens,output_tokens,duration_ms,observed_at,reasoning_tokens,cached_read_tokens,cache_write_tokens,workload_class)
         VALUES ('behind-back','p/m','p/m','p',300,100,1000,?1,0,50,10,'ephemeral')",
        rusqlite::params![(now - 400.0) as i64],
    )
    .unwrap();
    let cached = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    assert_eq!(
        cached.sample_count, 1,
        "the cached vector is served without a re-read"
    );

    store
        .record_usage_delivery(&class_obs(
            "cached-2",
            Some("ephemeral"),
            Some(500.0),
            Some(100.0),
            Some(0.0),
            Some(50.0),
            Some(10.0),
            Some(1000.0),
            now - 300.0,
        ))
        .unwrap();
    let recomputed = store.get_class_vector("ephemeral", now).unwrap().unwrap();
    assert_eq!(
        recomputed.sample_count, 3,
        "a write through the store recomputes over everything"
    );
    assert_eq!(recomputed.fresh_input, 300.0);
}

#[test]
fn migration_041_removes_fast_facts_and_route_state_without_touching_history() {
    use rusqlite::Connection;

    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE model_facts(route_key TEXT PRIMARY KEY);
         CREATE TABLE model_route_state(route_key TEXT PRIMARY KEY);
         CREATE TABLE model_route_decisions(selected_route_key TEXT);
         CREATE TABLE model_route_usage(route_key TEXT);",
        )
        .unwrap();
    connection
        .execute_batch(
            "INSERT INTO model_facts VALUES ('openai/gpt-5.5-fast'), ('openai/gpt-5.5');
         INSERT INTO model_route_state VALUES ('openai/gpt-5.5-fast');
         INSERT INTO model_route_decisions VALUES ('openai/gpt-5.5-fast');
         INSERT INTO model_route_usage VALUES ('openai/gpt-5.5-fast');",
        )
        .unwrap();
    connection
        .execute_batch(include_str!("../../migrations/041_retire_fast_routes.sql"))
        .unwrap();
    let facts: Vec<String> = connection
        .prepare("SELECT route_key FROM model_facts ORDER BY route_key")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(facts, ["openai/gpt-5.5"]);
    let states: i64 = connection
        .query_row("SELECT COUNT(*) FROM model_route_state", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(states, 0);
    let decisions: i64 = connection
        .query_row("SELECT COUNT(*) FROM model_route_decisions", [], |row| {
            row.get(0)
        })
        .unwrap();
    let usage: i64 = connection
        .query_row("SELECT COUNT(*) FROM model_route_usage", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!((decisions, usage), (1, 1));
    let violations = connection
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map([], |_| Ok(()))
        .unwrap()
        .count();
    assert_eq!(violations, 0);
}

#[test]
fn migration_057_prunes_unserved_unpicked_facts_but_preserves_overrides_and_decisions() {
    use rusqlite::{params, Connection};

    let rig = Rig::new("migration-057-fusiform-prune");
    let store = rig.open();
    for (provider, model, source) in [
        ("test", "old-unpicked", FactSource::Seed),
        ("test", "recent-picked", FactSource::Seed),
        ("test", "old-pick", FactSource::Seed),
        ("test", "override-unserved", FactSource::Override),
        ("test", "unknown-presence", FactSource::Seed),
        (
            "google",
            "antigravity-gemini-4-argon-high",
            FactSource::Override,
        ),
        (
            "google",
            "antigravity-gemini-4-argon-low",
            FactSource::Override,
        ),
    ] {
        store.upsert_fact(&fact(provider, model), source).unwrap();
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let ten_days_ago = now_ms - 10 * 24 * 60 * 60 * 1000;
    let thirty_one_days_ago = now_ms - 31 * 24 * 60 * 60 * 1000;
    let connection = Connection::open(rig.db_path()).unwrap();
    connection
        .execute(
            "UPDATE model_facts SET fusiform_present = 0, facts_source = 'fusiform'
             WHERE route_key IN (
               'test/old-unpicked', 'test/recent-picked', 'test/old-pick',
               'test/override-unserved',
               'google/antigravity-gemini-4-argon-high',
               'google/antigravity-gemini-4-argon-low'
             )",
            [],
        )
        .unwrap();
    let add_decision = |id: &str, route_key: &str, created_at: i64| {
        connection
            .execute(
                "INSERT INTO model_route_decisions (
                   id, target_agent, requirements_json, selected_route_key, score_json,
                   rejected_json, anchor_version, created_at
                 ) VALUES (?1, 'explore', '{}', ?2, '{}', '[]', 1, ?3)",
                params![id, route_key, created_at],
            )
            .unwrap();
    };
    add_decision("recent-picked", "test/recent-picked", ten_days_ago);
    add_decision("old-pick", "test/old-pick", thirty_one_days_ago);
    add_decision(
        "argon-high-pick",
        "google/antigravity-gemini-4-argon-high",
        ten_days_ago,
    );
    add_decision(
        "argon-low-pick",
        "google/antigravity-gemini-4-argon-low",
        ten_days_ago,
    );

    connection
        .execute_batch(include_str!(
            "../../migrations/057_prune_absent_fusiform_facts.sql"
        ))
        .unwrap();

    let row_count = |route_key: &str| {
        connection
            .query_row(
                "SELECT COUNT(*) FROM model_facts WHERE route_key = ?1",
                [route_key],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    assert_eq!(row_count("test/old-unpicked"), 0);
    assert_eq!(row_count("test/old-pick"), 0);
    assert_eq!(row_count("test/recent-picked"), 1);
    assert_eq!(row_count("test/override-unserved"), 1);
    assert_eq!(row_count("test/unknown-presence"), 1);
    assert_eq!(row_count("google/antigravity-gemini-4-argon-high"), 0);
    assert_eq!(row_count("google/antigravity-gemini-4-argon-low"), 0);

    let decisions: i64 = connection
        .query_row("SELECT COUNT(*) FROM model_route_decisions", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        decisions, 4,
        "cleanup must not erase score or decision history"
    );
    let unserved_overrides: Vec<String> = connection
        .prepare(
            "SELECT route_key FROM model_facts
             WHERE source = 'override' AND COALESCE(fusiform_present, 0) != 1
             ORDER BY route_key",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(unserved_overrides, ["test/override-unserved"]);
}

#[test]
fn complete_catalog_sync_stamps_missing_rows_and_prunes_unpicked_absence() {
    use rusqlite::{params, Connection};
    use std::collections::HashSet;

    let rig = Rig::new("complete-catalog-sync-prune");
    let store = rig.open();
    for (model, source) in [
        ("catalog-served", FactSource::Seed),
        ("recently-retired", FactSource::Seed),
        ("unpicked-retired", FactSource::Seed),
        ("operator-retired", FactSource::Override),
    ] {
        store.upsert_fact(&fact("test", model), source).unwrap();
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    Connection::open(rig.db_path())
        .unwrap()
        .execute(
            "INSERT INTO model_route_decisions (
               id, target_agent, requirements_json, selected_route_key, score_json,
               rejected_json, anchor_version, created_at
             ) VALUES ('recent-retired-pick', 'explore', '{}', 'test/recently-retired', '{}', '[]', 1, ?1)",
            params![now_ms - 10 * 24 * 60 * 60 * 1000],
        )
        .unwrap();

    let served = FusiformFactUpdate {
        auth_methods: None,
        provider_id: "test".into(),
        model_id: "catalog-served".into(),
        present: true,
        context_window: Some(16_384.0),
        max_output_tokens: Some(2_048),
        cost_tier: Some(1.0),
        cost_per_mtok_out: Some(1.0),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(false),
        supports_tool_use: Some(true),
        reasoning_efforts: None,
        canonical_id: None,
        upstream_status: None, model_type: None, model_kind: None,
        lineage: None,
    };
    store
        .apply_fusiform_catalog_snapshot_for_providers(
            &[served],
            FusiformCatalogCursor {
                catalog_version: 100,
                resolved_at_ms: now_ms,
            },
            &HashSet::new(),
            &HashSet::new(),
        )
        .unwrap();
    let absent_before_cleanup = store.prune_unserved_unpicked_facts().unwrap();

    assert!(absent_before_cleanup.contains(&"test/recently-retired".to_string()));
    assert!(absent_before_cleanup.contains(&"test/unpicked-retired".to_string()));
    assert!(absent_before_cleanup.contains(&"test/operator-retired".to_string()));
    assert_eq!(
        store
            .get_fact_by_route_key("test/catalog-served")
            .unwrap()
            .unwrap()
            .supports_vision,
        Some(false)
    );
    assert!(store
        .get_fact_by_route_key("test/unpicked-retired")
        .unwrap()
        .is_none());
    assert!(store
        .get_fact_by_route_key("test/recently-retired")
        .unwrap()
        .is_some());
    let routable = store.get_facts().unwrap();
    assert!(routable
        .iter()
        .any(|fact| fact.model_id == "catalog-served"));
    assert!(routable
        .iter()
        .any(|fact| fact.model_id == "operator-retired"));
    assert!(routable
        .iter()
        .all(|fact| fact.model_id != "recently-retired"));
    assert_eq!(
        store.unserved_override_route_keys().unwrap(),
        ["test/operator-retired"]
    );
    let history_count: i64 = Connection::open(rig.db_path())
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM model_route_decisions
             WHERE selected_route_key = 'test/recently-retired'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        history_count, 1,
        "score history remains in the decision log"
    );
}

/// Read a route-state row's stored provider, model and variant.
fn stored_route_identity(rig: &Rig, route_key: &str) -> (String, String, Option<String>) {
    rusqlite::Connection::open(rig.db_path())
        .unwrap()
        .query_row(
            "SELECT provider_id, model_id, variant FROM model_route_state WHERE route_key = ?1",
            [route_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

#[test]
fn a_route_state_write_keeps_the_catalog_identity_of_a_slash_bearing_model() {
    // Aggregator providers serve model ids that contain "/". The route key
    // `openrouter/anthropic/claude-x` is that model with no variant, which only
    // the stored fact can say; splitting the key reads it as model `anthropic`
    // with variant `claude-x`.
    const ROUTE: &str = "openrouter/anthropic/claude-x";
    let rig = Rig::new("slash-model-route-state");
    let store = rig.open();
    store
        .upsert_fact(
            &fact("openrouter", "anthropic/claude-x"),
            FactSource::Override,
        )
        .unwrap();
    store
        .update_route_state(
            ROUTE,
            "openrouter/anthropic/claude-x",
            &RouteStateUpdate {
                cooldown_until: Some(5_000.0),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        stored_route_identity(&rig, ROUTE),
        ("openrouter".into(), "anthropic/claude-x".into(), None)
    );
}

#[test]
fn a_decision_outcome_keeps_the_decision_identity_of_a_slash_bearing_model() {
    // No catalog fact: the decision's recorded provider and model decide the
    // identity, and the key adds no variant after them.
    const ROUTE: &str = "openrouter/anthropic/claude-y";
    let rig = Rig::new("slash-model-decision-outcome");
    let store = rig.open();
    let id = selected_decision(&store, "dec-slash-model", ROUTE);
    set_outcome(
        &store,
        &id,
        RouteOutcome::Error,
        Some("HTTP 503 provider unavailable"),
        1_000,
    );
    assert_eq!(
        stored_route_identity(&rig, ROUTE),
        ("openrouter".into(), "anthropic/claude-y".into(), None)
    );
}

#[test]
fn an_undecodable_score_row_reads_as_unscored_and_does_not_block_decisions() {
    // Routing loads a `scores_json` that does not decode as zero scores. The
    // catalog fingerprint stamped on every decision, and the operator listings,
    // must read the same row the same way instead of failing on it.
    const ROUTE: &str = "p/corrupt-scores";
    let rig = Rig::new("corrupt-score-row");
    let store = rig.open();
    store
        .upsert_fact(&fact("p", "corrupt-scores"), FactSource::Override)
        .unwrap();
    let conn = rusqlite::Connection::open(rig.db_path()).unwrap();
    let set_scores = |scores_json: &str| {
        conn.execute(
            "UPDATE model_facts SET scores_json = ?1 WHERE route_key = ?2",
            [scores_json, ROUTE],
        )
        .unwrap();
    };

    set_scores(r#"{"elo":0,"eq":0,"speed":0}"#);
    let zero_scores_fingerprint = store.catalog_fingerprint().unwrap();

    set_scores("{not json");
    let loaded = store.get_fact_by_route_key(ROUTE).unwrap().unwrap();
    assert_eq!(loaded.scores, ModelScores::default());
    assert_eq!(
        store.catalog_fingerprint().unwrap(),
        zero_scores_fingerprint,
        "the fingerprint hashes the scores routing reads"
    );
    selected_decision(&store, "dec-after-corrupt-scores", "openai/gpt-5.5");
    assert!(store
        .list_canonical_gaps()
        .unwrap()
        .iter()
        .all(|gap| gap.route_key != ROUTE));
}

#[test]
fn new_routes_inherit_the_provider_wall_cause_through_the_insert_trigger() {
    // The model_facts insert trigger is the one place a new route inherits a
    // live provider wall. Both fact writers rely on it, cause included.
    let rig = Rig::new("provider-wall-cause-inheritance");
    let store = rig.open();
    store
        .upsert_fact(&fact("p", "base"), FactSource::Override)
        .unwrap();
    store
        .record_provider_cooldown(
            "p",
            "p/base",
            "p/base",
            &RouteStateUpdate {
                quota_exhausted_until: Some(50_000.0),
                wall_class: Some(RouteWallClass::Billing),
                last_error: Some("out of credits".into()),
                ..Default::default()
            },
        )
        .unwrap();
    store
        .upsert_fact(&fact("p", "upserted"), FactSource::Override)
        .unwrap();
    store
        .apply_fusiform_catalog(
            &[FusiformFactUpdate {
                auth_methods: None,
                provider_id: "p".into(),
                model_id: "discovered".into(),
                present: true,
                context_window: Some(64_000.0),
                max_output_tokens: Some(4_096),
                cost_tier: Some(1.0),
                cost_per_mtok_out: Some(1.0),
                input_rate: None,
                cache_read_rate: None,
                cache_write_rate: None,
                reasoning_rate: None,
                supports_vision: Some(false),
                supports_tool_use: Some(true),
                reasoning_efforts: None,
                canonical_id: None,
                upstream_status: None, model_type: None, model_kind: None,
                lineage: None,
            }],
            FusiformCatalogCursor {
                catalog_version: 1,
                resolved_at_ms: 100,
            },
        )
        .unwrap();

    let snapshot = store.build_snapshot(1_000.0).unwrap();
    for route in ["p/upserted", "p/discovered"] {
        let state = &snapshot.by_route[route];
        assert_eq!(state.quota_exhausted_until, Some(50_000.0), "{route}");
        assert_eq!(state.wall_class, Some(RouteWallClass::Billing), "{route}");
    }
}

#[test]
fn migration_83_preserves_unknown_classification_and_catalog_refreshes_all_variants() {
    let rig = Rig::new("model-classification");
    let descriptor = dev_descriptor("striatum", rig.db_path());
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 82).unwrap();
    drop(old);
    let store = rig.open();
    let key = "anthropic/claude-opus-4-5";
    let initial = store.get_fact_by_route_key(key).unwrap().unwrap();
    assert_eq!(initial.model_type, None);
    assert_eq!(initial.model_kind, None);
    let initial_fingerprint = store.catalog_fingerprint().unwrap();
    let update = |model_type: Option<&str>, model_kind: Option<&str>| FusiformFactUpdate {
        provider_id: initial.provider_id.clone(),
        model_id: initial.model_id.clone(),
        present: true,
        auth_methods: None,
        context_window: initial.context_window,
        max_output_tokens: initial.max_output_tokens,
        cost_tier: initial.cost_tier,
        cost_per_mtok_out: initial.cost_per_m_tok_out,
        input_rate: initial.input_rate,
        cache_read_rate: initial.cache_read_rate,
        cache_write_rate: initial.cache_write_rate,
        reasoning_rate: initial.reasoning_rate,
        supports_vision: initial.supports_vision,
        supports_tool_use: initial.supports_tool_use,
        reasoning_efforts: None,
        canonical_id: None,
        upstream_status: None,
        lineage: initial.family.clone(),
        model_type: model_type.map(str::to_owned),
        model_kind: model_kind.map(str::to_owned),
    };
    let cursor = FusiformCatalogCursor { catalog_version: 1, resolved_at_ms: 1 };
    store.apply_fusiform_catalog(&[update(Some("transcription"), None)], cursor).unwrap();
    let typed_fingerprint = store.catalog_fingerprint().unwrap();
    assert_ne!(initial_fingerprint, typed_fingerprint);
    store.apply_fusiform_catalog(&[update(Some("transcription"), Some("decision"))], cursor).unwrap();
    assert_ne!(typed_fingerprint, store.catalog_fingerprint().unwrap());
    let mut variant = initial.clone();
    variant.variant = Some("high".into());
    store.upsert_fact(&variant, FactSource::Override).unwrap();
    for key in [key, "anthropic/claude-opus-4-5/high"] {
        let fact = store.get_fact_by_route_key(key).unwrap().unwrap();
        assert_eq!(fact.model_type.as_deref(), Some("transcription"));
        assert_eq!(fact.model_kind.as_deref(), Some("decision"));
    }
    let mut discovered = update(Some("image"), Some("extraction"));
    discovered.model_id = "new-image-engine".into();
    store.apply_fusiform_catalog(&[discovered], cursor).unwrap();
    let fact = store.get_fact_by_route_key("anthropic/new-image-engine").unwrap().unwrap();
    assert_eq!(fact.model_type.as_deref(), Some("image"));
    assert_eq!(fact.model_kind.as_deref(), Some("extraction"));
    store.apply_fusiform_catalog(&[update(None, None)], cursor).unwrap();
    // An older score-edit copy must not reintroduce a classification the catalog removed.
    variant.variant = Some("low".into());
    variant.model_type = Some("image".into());
    variant.model_kind = Some("extraction".into());
    store.upsert_fact(&variant, FactSource::Override).unwrap();
    for key in [key, "anthropic/claude-opus-4-5/high", "anthropic/claude-opus-4-5/low"] {
        let fact = store.get_fact_by_route_key(key).unwrap().unwrap();
        assert_eq!(fact.model_type, None);
        assert_eq!(fact.model_kind, None);
    }
    let fingerprint = store.catalog_fingerprint().unwrap();
    drop(store);
    let reopened = rig.open();
    assert_eq!(reopened.catalog_fingerprint().unwrap(), fingerprint);
    drop(reopened);
    let error = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 82).err().unwrap();
    assert!(error.to_string().contains("store_ahead"));
    assert!(error.to_string().contains("84"));
}
