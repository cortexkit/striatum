//! Test-vectors for the sqlite route-state store. Ported from the TS
//! `route-state-store.test.ts` facts-domain cases, plus storage-lib integration
//! checks (seed-via-migration runs once, single-writer lease) that the TS version
//! couldn't have (those were the host's job; now they're ours via cortexkit-store).

use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::model_facts::{
    Confidence, ModelFact, ModelPromptDefaults, ModelScores, ScoreProvenance,
};
use prefrontal_routing_core::{
    admit_routes, derive_provider_key, make_capacity_key, make_route_key, relax_earliest_reset,
    select_model_route, BreakGlassCandidate, CatalogFactField, DecisionInput,
    DecisionOutcomeUpdate, ExclusionPolicy, FactSource, FailureBackoffConfig, ModelRequirements,
    RouteFailureReason, RouteOutcome, RouteStateStore, RouteStateUpdate, RoutingConfig,
    SelectModelRouteInput, UsageObservation, FAILURE_BACKOFF_INITIAL_MS,
};
use prefrontal_routing_store::{
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
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-store-{label}-{}-{nanos}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }
    fn db_path(&self) -> String {
        self.dir.join("store.db").to_string_lossy().into_owned()
    }
    fn open(&self) -> SqliteRouteStateStore {
        let d = dev_descriptor("prefrontal-routing", self.db_path());
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
const CATALOG_LEN: usize = 57;

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
    let descriptor = dev_descriptor("prefrontal-routing", rig.db_path());
    {
        let v3 = open_sqlite(&descriptor).unwrap();
        v3.migrate(
            "route_state",
            &[
                Migration {
                    version: 1,
                    statements: include_str!("../migrations/001_route_state.sql"),
                },
                Migration {
                    version: 2,
                    statements: include_str!("../migrations/002_usage.sql"),
                },
                Migration {
                    version: 3,
                    statements: include_str!("../migrations/003_drop_reservations.sql"),
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
    let d = dev_descriptor("prefrontal-routing", rig.db_path());
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
                outcome,
                last_error: last_error.map(str::to_owned),
                observed_at_ms,
                failure_backoff: FailureBackoffConfig::default(),
            },
        )
        .unwrap();
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
        &[],
        &[],
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
        &[],
        &[],
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
                statements: include_str!(concat!("../migrations/", $file)),
            }
        };
    }

    let rig = Rig::new("migration-030-028-era-row");
    let descriptor = dev_descriptor("prefrontal-routing", rig.db_path());
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

    assert_eq!(store.delete_decisions_older_than(cutoff).unwrap(), 1);
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
    let mut rejected = (0..12)
        .map(|i| serde_json::json!({ "routeKey": format!("kept-{i}"), "reason": "admission" }))
        .collect::<Vec<_>>();
    rejected.extend([
        serde_json::json!({ "routeKey": "omitted-0", "reason": "quota_exhausted" }),
        serde_json::json!({ "routeKey": "omitted-1", "reason": "quota_exhausted" }),
        serde_json::json!({ "routeKey": "omitted-2", "reason": "context_too_small" }),
    ]);
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
    assert_eq!(&stored[..12], &rejected[..12]);
    assert_eq!(
        stored[12],
        serde_json::json!({
            "truncated": 3,
            "reasons": {
                "context_too_small": 1,
                "quota_exhausted": 2,
            },
        })
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
        requirements: ModelRequirements { iq: 10.0, eq: 0.0 },
        available_models: available_models.clone(),
        facts: vec![route.clone()],
        runtime_snapshot: Some(snapshot.clone()),
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),
        prefer_fast_specs: Vec::new(),
        required_capabilities: None,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: std::collections::HashSet::new(),
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
        requirements: ModelRequirements { iq: 10.0, eq: 0.0 },
        available_models,
        facts: vec![route],
        runtime_snapshot: Some(relaxed.snapshot),
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),
        prefer_fast_specs: Vec::new(),
        required_capabilities: None,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: std::collections::HashSet::new(),
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
        output_tokens: out,
        duration_ms: dur,
        observed_at: at,
    }
}

#[test]
fn usage_rejects_samples_below_the_eligibility_gates() {
    let rig = Rig::new("usage-gates");
    let store = rig.open();
    // Below MIN_OUTPUT_TOKENS (100).
    assert!(!store
        .record_usage_observation(&obs("a", "p/m", 50.0, 5000.0, 1000.0))
        .unwrap());
    // Below MIN_DURATION_MS (1000).
    assert!(!store
        .record_usage_observation(&obs("b", "p/m", 500.0, 500.0, 1000.0))
        .unwrap());
    // Empty id.
    assert!(!store
        .record_usage_observation(&obs("", "p/m", 500.0, 5000.0, 1000.0))
        .unwrap());
    // An eligible one inserts.
    assert!(store
        .record_usage_observation(&obs("c", "p/m", 500.0, 5000.0, 1000.0))
        .unwrap());
}

#[test]
fn usage_is_idempotent_by_message_id() {
    let rig = Rig::new("usage-idem");
    let store = rig.open();
    assert!(store
        .record_usage_observation(&obs("msg1", "p/m", 500.0, 5000.0, 1000.0))
        .unwrap());
    // Same id again → no new row (a redrive must not double-count).
    assert!(!store
        .record_usage_observation(&obs("msg1", "p/m", 999.0, 9999.0, 2000.0))
        .unwrap());
}

#[test]
fn observed_speed_needs_min_samples_then_aggregates_tokens_over_duration() {
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
    // 5th sample crosses MIN_SAMPLES: 5 * 1000 tok over 5 * 1000 ms = 1000 tok/s.
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

    let aggregated = store.get_facts_with_observed_speed(now).unwrap();
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
    let before = store.get_facts_with_observed_speed(10_000.0).unwrap();
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
    let after = store.get_facts_with_observed_speed(10_000.0).unwrap();
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

#[test]
fn panel_decision_kind_and_migration() {
    use cortexkit_store::{open_sqlite, Migration};
    use rusqlite::Connection;

    let rig = Rig::new("v4-panel-migration");
    let descriptor = dev_descriptor("prefrontal-routing", rig.db_path());
    {
        let v4 = open_sqlite(&descriptor).unwrap();
        v4.migrate(
            "route_state",
            &[
                Migration {
                    version: 1,
                    statements: include_str!("../migrations/001_route_state.sql"),
                },
                Migration {
                    version: 2,
                    statements: include_str!("../migrations/002_usage.sql"),
                },
                Migration {
                    version: 3,
                    statements: include_str!("../migrations/003_drop_reservations.sql"),
                },
                Migration {
                    version: 4,
                    statements: include_str!("../migrations/004_gpt56_family.sql"),
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
    assert_eq!(hosted_claude.family.as_deref(), Some("anthropic"));
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
    let descriptor = dev_descriptor("prefrontal-routing", rig.db_path());
    {
        // Apply through migration 6 only, then plant a deliberate override on a
        // route that 007 would otherwise rewrite.
        let pre = open_sqlite(&descriptor).unwrap();
        pre.migrate(
            "route_state",
            &[
                Migration {
                    version: 1,
                    statements: include_str!("../migrations/001_route_state.sql"),
                },
                Migration {
                    version: 2,
                    statements: include_str!("../migrations/002_usage.sql"),
                },
                Migration {
                    version: 3,
                    statements: include_str!("../migrations/003_drop_reservations.sql"),
                },
                Migration {
                    version: 4,
                    statements: include_str!("../migrations/004_gpt56_family.sql"),
                },
                Migration {
                    version: 5,
                    statements: include_str!("../migrations/005_panel_selection.sql"),
                },
                Migration {
                    version: 6,
                    statements: include_str!("../migrations/006_grok_45.sql"),
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
    // set_speed_multipliers replaces a PROCESS-GLOBAL map with no restore, and
    // tests share the process: keying this test on the fixture-standard "p/m"
    // halved every later reader's p/m speed (90 -> 45) whenever the scheduler
    // ran this test first - green locally, red on CI's core count. A route key
    // no other test reads removes the collision domain instead of racing it.
    store
        .upsert_fact(&fact("pmult", "mmult"), FactSource::Override)
        .unwrap();

    let mut multipliers = std::collections::HashMap::new();
    multipliers.insert("pmult/mmult".to_string(), 0.5);
    prefrontal_routing_core::model_facts::set_speed_multipliers(multipliers);

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

    let after = store.get_facts_with_observed_speed(10_000.0).unwrap();
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
    assert_eq!(fable.family.as_deref(), Some("anthropic"));
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
    assert_eq!(
        snapshot.by_route["p/a/high"].quota_exhausted_until,
        Some(30_000.0)
    );
    assert_eq!(snapshot.by_route["p/b"].cooldown_until, Some(10_000.0));
    assert_eq!(
        snapshot.by_route["p/b"].quota_exhausted_until,
        Some(30_000.0)
    );

    // A later, older signal cannot shorten either provider or route state.
    store
        .record_provider_cooldown(
            "p",
            "p/a/low",
            "p/a",
            &RouteStateUpdate {
                cooldown_until: Some(5_000.0),
                quota_exhausted_until: Some(25_000.0),
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
    assert_eq!(
        snapshot.by_route["p/a/high"].quota_exhausted_until,
        Some(30_000.0)
    );

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
    assert_eq!(elo("openai/gpt-5.6-sol-fast"), 1_838.0);
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
fn aa_index_cache_replace_is_wholesale() {
    let rig = Rig::new("aa-index-cache");
    let store = rig.open();
    store
        .replace_aa_index_table(&[("slug-a".into(), 40.0), ("slug-b".into(), 50.0)], 10)
        .unwrap();
    assert_eq!(store.get_aa_index("slug-a").unwrap(), Some((40.0, 10)));
    store
        .replace_aa_index_table(&[("slug-c".into(), 12.0)], 20)
        .unwrap();
    assert_eq!(store.get_aa_index("slug-a").unwrap(), None);
    assert_eq!(store.get_aa_index("slug-c").unwrap(), Some((12.0, 20)));
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
        provider_id: "anthropic".to_string(),
        model_id: "claude-opus-4-5".to_string(),
        present: true,
        context_window: Some(250_000.0),
        max_output_tokens: Some(32_000),
        cost_tier: Some(3.0),
        cost_per_mtok_out: Some(12.0),
        supports_vision: Some(vision),
        supports_tool_use: Some(true),
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
}

#[test]
fn fusiform_catalog_keys_on_provider_and_model_never_bare_model_id() {
    // Two operator-known providers (both seeded in the catalog) serve the same
    // model spelling: two rows, keyed on the pair, never a bare-id merge.
    let rig = Rig::new("fusiform-pair-key");
    let store = rig.open();
    let fact = |provider: &str| FusiformFactUpdate {
        provider_id: provider.to_string(),
        model_id: "shared-spelling".to_string(),
        present: true,
        context_window: Some(128_000.0),
        max_output_tokens: Some(8_192),
        cost_tier: Some(1.0),
        cost_per_mtok_out: Some(2.0),
        supports_vision: Some(provider == "anthropic"),
        supports_tool_use: Some(true),
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
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        present: true,
        context_window: Some(64_000.0),
        max_output_tokens: Some(4_096),
        cost_tier: Some(1.0),
        cost_per_mtok_out: Some(1.0),
        supports_vision: Some(false),
        supports_tool_use: Some(true),
    };
    store
        .apply_fusiform_catalog(
            &[
                fact("302ai", "MiniMax-M2.7"),
                fact("anthropic", "claude-never-seen"),
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
