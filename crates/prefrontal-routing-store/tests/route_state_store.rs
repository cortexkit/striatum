//! Test-vectors for the sqlite route-state store. Ported from the TS
//! `route-state-store.test.ts` facts-domain cases, plus storage-lib integration
//! checks (seed-via-migration runs once, single-writer lease) that the TS version
//! couldn't have (those were the host's job; now they're ours via cortexkit-store).

use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::model_facts::{
    Confidence, ModelFact, ModelPromptDefaults, ModelScores,
};
use prefrontal_routing_core::{
    derive_provider_key, make_capacity_key, make_route_key, relax_earliest_reset,
    select_model_route, BreakGlassCandidate, DecisionInput, FactSource, ModelRequirements,
    RouteFailureReason, RouteOutcome, RouteStateStore, RouteStateUpdate, SelectModelRouteInput,
    UsageObservation,
};
use prefrontal_routing_store::{dev_descriptor, SqliteRouteStateStore};

static N: AtomicU64 = AtomicU64::new(0);

/// A unique temp rig: a fresh sqlite path + lease dir per test (no cross-test
/// collision, and a fresh DB so the migration/seed runs from scratch).
struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "prefrontal-routing-store-{label}-{}-{n}",
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
    }
}

/// Catalog size after all migrations: 33 (001) + 9 gpt-5.6 (004) + 1 grok-4.5 (006)
/// − 12 opencode-go + 12 ollama-cloud (007) + 1 fable-5 (008) = 44.
// 43 rebased rows (007) + fable-5 (008) + kimi k3 (009) + qwen3.8-max (010)
// + 14 alibaba-token-plan text models (013) - 11 pruned to the real
// entitlement (014: the token plan serves the qwen family only)
// + gemini 3.6 flash (017) + opus-5 and its -fast sibling (018).
// 54 seeded + migration 023's ollama-cloud/kimi-k3 (K3 became servable on
// ollama-cloud; an unseeded route made a roster seat silently unseatable).
const CATALOG_LEN: usize = 56;

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
        .set_decision_outcome(&id, RouteOutcome::Completed)
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
    store
        .upsert_fact(&fact("p", "m"), FactSource::Override)
        .unwrap();

    let mut multipliers = std::collections::HashMap::new();
    multipliers.insert("p/m".to_string(), 0.5);
    prefrontal_routing_core::model_facts::set_speed_multipliers(multipliers);

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
