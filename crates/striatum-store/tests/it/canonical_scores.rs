//! One quality score per model: routes that Fusiform's `model.canonical_id`
//! names as the same model share one operator Elo/EQ placement when routing
//! reads the catalog. The route ids and canonical ids are the ones measured on
//! the live catalog for the Muse and Gemini Flash rows.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use striatum_core::model_facts::{ModelFact, ScoreProvenance};
use striatum_core::score_alias::append_alias_rows;
use striatum_core::{make_route_key, FactSource, RouteStateStore};
use striatum_store::{
    dev_descriptor, FusiformCatalogCursor, FusiformFactUpdate, SqliteRouteStateStore,
};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

const MUSE: &str = "meta/muse-spark-1.3";

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "striatum-canonical-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn open(&self) -> SqliteRouteStateStore {
        SqliteRouteStateStore::open(&dev_descriptor(
            "striatum",
            self.dir.join("store.db").to_string_lossy().into_owned(),
        ))
        .expect("open store")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// One served catalog row; `route` is Fusiform's `provider/model` key.
fn served(route: &str, canonical_id: Option<&str>) -> FusiformFactUpdate {
    let (provider, model) = route.split_once('/').unwrap();
    FusiformFactUpdate {
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        auth_methods: None,
        login_methods_documented: false,
        present: true,
        context_window: Some(200_000.0),
        max_output_tokens: Some(32_000),
        cost_tier: Some(2.0),
        cost_per_mtok_out: Some(3.0),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(false),
        supports_tool_use: Some(true),
        reasoning_efforts: None,
        catalog_efforts: None,
        canonical_id: canonical_id.map(str::to_string),
        upstream_status: None,
        model_type: None,
        model_kind: None,
        lineage: None,
    }
}

/// The measured live rows: three Muse routes share one canonical id, two have
/// none; the Antigravity alias shares its target's id; both 3.5 Flash rows have none.
fn live_catalog() -> Vec<FusiformFactUpdate> {
    vec![
        served("meta/muse-spark-1.3", Some(MUSE)),
        served("meta/muse-spark-1.3-contributor", Some(MUSE)),
        served("openrouter/meta/muse-spark-1.3", Some(MUSE)),
        served("opencode/muse-spark-1.3-contributor-free", None),
        served("openrouter/meta/muse-spark-1.3-contributor", None),
        served("google/gemini-3.5-flash", None),
        served("opencode/gemini-3.5-flash", None),
        served(
            "google/antigravity-gemini-3.8-flash",
            Some("google/gemini-3.8-flash"),
        ),
        served("google/gemini-3.8-flash", Some("google/gemini-3.8-flash")),
    ]
}

fn ingest(store: &SqliteRouteStateStore, catalog: &[FusiformFactUpdate]) {
    let providers = ["meta", "openrouter", "opencode", "google"]
        .into_iter()
        .map(str::to_string)
        .collect::<HashSet<_>>();
    store
        .apply_fusiform_catalog_for_providers(
            catalog,
            FusiformCatalogCursor {
                catalog_version: 7,
                resolved_at_ms: 700,
            },
            &providers,
        )
        .unwrap();
}

/// What `route.model_upsert` does for one route key: merge both judgment
/// scores over the stored row and write it as an operator override.
fn place(store: &SqliteRouteStateStore, route: &str, elo: f64, eq: f64) {
    let mut fact = store
        .get_fact_by_route_key(route)
        .unwrap()
        .unwrap_or_else(|| panic!("{route} is stored"));
    fact.scores.elo = elo;
    fact.scores.eq = eq;
    fact.confidence = striatum_core::model_facts::Confidence::Curated;
    fact.score_provenance = ScoreProvenance::Curated;
    store.upsert_fact(&fact, FactSource::Override).unwrap();
}

fn routed(store: &SqliteRouteStateStore) -> BTreeMap<String, ModelFact> {
    store
        .get_facts()
        .unwrap()
        .into_iter()
        .map(|fact| {
            (
                make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()),
                fact,
            )
        })
        .collect()
}

#[test]
fn one_upsert_scores_every_route_of_the_model() {
    let rig = Rig::new("muse-share");
    let store = rig.open();
    ingest(&store, &live_catalog());
    place(&store, "meta/muse-spark-1.3", 1810.0, 1520.0);

    let facts = routed(&store);
    for route in [
        "meta/muse-spark-1.3",
        "meta/muse-spark-1.3-contributor",
        "openrouter/meta/muse-spark-1.3",
    ] {
        let fact = &facts[route];
        assert_eq!(
            (fact.scores.elo, fact.scores.eq),
            (1810.0, 1520.0),
            "{route} reads the model's one score"
        );
        assert_ne!(
            fact.score_provenance,
            ScoreProvenance::Unscored,
            "{route} is routable"
        );
    }
    // Model-intrinsic only: each route keeps its own served facts.
    assert_eq!(
        facts["openrouter/meta/muse-spark-1.3"].context_window,
        Some(200_000.0)
    );

    // The raw row is untouched, so a later per-route write cannot persist a
    // borrowed score as the route's own.
    let raw = store
        .get_fact_by_route_key("openrouter/meta/muse-spark-1.3")
        .unwrap()
        .unwrap();
    assert_eq!(raw.score_provenance, ScoreProvenance::Unscored);
    assert_eq!(raw.scores.elo, 0.0);

    let inventory = store.get_catalog_facts().unwrap();
    let reseller = inventory
        .iter()
        .find(|row| {
            row.fact.provider_id == "openrouter" && row.fact.model_id == "meta/muse-spark-1.3"
        })
        .unwrap();
    assert_eq!(reseller.fact.scores.elo, 1810.0);
    assert_eq!(reseller.canonical_id.as_deref(), Some(MUSE));
    assert_eq!(
        reseller.score_borrowed_from.as_deref(),
        Some("meta/muse-spark-1.3")
    );

    let unscored = store
        .list_unscored_rows()
        .unwrap()
        .into_iter()
        .map(|row| row.route_key)
        .collect::<Vec<_>>();
    assert!(
        !unscored.contains(&"meta/muse-spark-1.3-contributor".to_string()),
        "a route that reads its model's score is not awaiting a placement: {unscored:?}"
    );
}

#[test]
fn a_route_without_a_canonical_id_does_not_borrow() {
    let rig = Rig::new("muse-null");
    let store = rig.open();
    ingest(&store, &live_catalog());
    place(&store, "meta/muse-spark-1.3", 1810.0, 1520.0);
    place(&store, "google/gemini-3.5-flash", 1700.0, 1400.0);

    let facts = routed(&store);
    for route in [
        "opencode/muse-spark-1.3-contributor-free",
        "openrouter/meta/muse-spark-1.3-contributor",
        "opencode/gemini-3.5-flash",
    ] {
        let fact = &facts[route];
        assert_eq!(fact.scores.elo, 0.0, "{route} has no stated identity");
        assert_eq!(fact.score_provenance, ScoreProvenance::Unscored, "{route}");
    }
    let unscored = store
        .list_unscored_rows()
        .unwrap()
        .into_iter()
        .map(|row| row.route_key)
        .collect::<Vec<_>>();
    assert!(unscored.contains(&"opencode/gemini-3.5-flash".to_string()));
}

#[test]
fn a_routes_own_score_overrides_its_models_score() {
    let rig = Rig::new("muse-own");
    let store = rig.open();
    ingest(&store, &live_catalog());
    place(&store, "meta/muse-spark-1.3", 1810.0, 1520.0);
    place(&store, "meta/muse-spark-1.3-contributor", 1750.0, 1480.0);

    let facts = routed(&store);
    assert_eq!(
        (
            facts["meta/muse-spark-1.3-contributor"].scores.elo,
            facts["meta/muse-spark-1.3-contributor"].scores.eq
        ),
        (1750.0, 1480.0),
        "the route's own placement wins over the model's"
    );
    assert_eq!(facts["meta/muse-spark-1.3"].scores.elo, 1810.0);
    assert_eq!(
        facts["openrouter/meta/muse-spark-1.3"].scores.elo, 1810.0,
        "an unscored route borrows the maker's own listing when placements disagree"
    );
}

#[test]
fn an_alias_listing_shares_its_target_models_score() {
    let rig = Rig::new("gemini-alias");
    let store = rig.open();
    ingest(&store, &live_catalog());
    place(&store, "google/gemini-3.8-flash", 1760.0, 1450.0);

    let facts = routed(&store);
    let alias = &facts["google/antigravity-gemini-3.8-flash"];
    assert_eq!((alias.scores.elo, alias.scores.eq), (1760.0, 1450.0));
    assert_ne!(alias.score_provenance, ScoreProvenance::Unscored);
}

#[test]
fn a_score_alias_provider_borrows_the_shared_score() {
    // score_alias copies a provider's rows under another provider name after
    // the store read, so the borrowed rows carry the shared score too.
    let rig = Rig::new("score-alias");
    let store = rig.open();
    ingest(&store, &live_catalog());
    place(&store, "meta/muse-spark-1.3", 1810.0, 1520.0);
    let mut facts = store.get_facts().unwrap();
    append_alias_rows(
        &mut facts,
        &BTreeMap::from([("openrouter-byok".to_string(), "openrouter".to_string())]),
    );
    let aliased = facts
        .iter()
        .find(|fact| {
            fact.provider_id == "openrouter-byok" && fact.model_id == "meta/muse-spark-1.3"
        })
        .expect("the alias provider borrows the reseller row");
    assert_eq!(aliased.scores.elo, 1810.0);
    assert_ne!(aliased.score_provenance, ScoreProvenance::Unscored);
}

#[test]
fn a_new_variant_row_inherits_its_models_canonical_id() {
    let rig = Rig::new("variant-identity");
    let store = rig.open();
    ingest(&store, &live_catalog());
    let mut high = store
        .get_fact_by_route_key("meta/muse-spark-1.3")
        .unwrap()
        .unwrap();
    high.variant = Some("high".into());
    high.scores.elo = 1850.0;
    high.scores.eq = 1530.0;
    store.upsert_fact(&high, FactSource::Override).unwrap();
    assert_eq!(
        store
            .route_keys_for_canonical_id(MUSE, Some("high"))
            .unwrap(),
        vec!["meta/muse-spark-1.3/high".to_string()]
    );
    assert_eq!(
        store.route_keys_for_canonical_id(MUSE, None).unwrap(),
        vec![
            "meta/muse-spark-1.3".to_string(),
            "meta/muse-spark-1.3-contributor".to_string(),
            "openrouter/meta/muse-spark-1.3".to_string(),
        ]
    );
}

#[test]
fn canonical_gaps_list_scored_routes_with_no_stated_identity() {
    let rig = Rig::new("gaps");
    let store = rig.open();
    ingest(&store, &live_catalog());
    place(&store, "meta/muse-spark-1.3", 1810.0, 1520.0);
    place(&store, "google/gemini-3.5-flash", 1700.0, 1400.0);
    let gaps = store.list_canonical_gaps().unwrap();
    let keys = gaps
        .iter()
        .map(|row| row.route_key.as_str())
        .collect::<Vec<_>>();
    assert!(keys.contains(&"google/gemini-3.5-flash"), "{keys:?}");
    assert!(
        !keys.contains(&"meta/muse-spark-1.3"),
        "has an identity: {keys:?}"
    );
    assert!(
        !keys.contains(&"opencode/gemini-3.5-flash"),
        "unscored rows are not gaps: {keys:?}"
    );
    let flash = gaps
        .iter()
        .find(|row| row.route_key == "google/gemini-3.5-flash")
        .unwrap();
    assert!(flash.fusiform_served);
    assert_eq!(flash.source, "override");
    // Seeded rows Fusiform has never served are gaps as well, marked unserved.
    assert!(gaps.iter().any(|row| !row.fusiform_served));
}

#[test]
fn reingest_carries_a_changed_canonical_id_and_null_clears_it() {
    let rig = Rig::new("reingest");
    let store = rig.open();
    ingest(&store, &live_catalog());
    place(&store, "meta/muse-spark-1.3", 1810.0, 1520.0);
    let mut catalog = live_catalog();
    for row in &mut catalog {
        if row.provider_id == "openrouter" && row.model_id == "meta/muse-spark-1.3" {
            row.canonical_id = None;
        }
        if row.provider_id == "opencode" && row.model_id == "muse-spark-1.3-contributor-free" {
            row.canonical_id = Some(MUSE.to_string());
        }
    }
    ingest(&store, &catalog);
    let facts = routed(&store);
    assert_eq!(facts["openrouter/meta/muse-spark-1.3"].scores.elo, 0.0);
    assert_eq!(
        facts["opencode/muse-spark-1.3-contributor-free"].scores.elo,
        1810.0
    );
}

#[test]
fn upstream_status_survives_a_score_placement_and_reaches_new_variants() {
    let rig = Rig::new("upstream-status");
    let store = rig.open();
    let mut catalog = live_catalog();
    for row in &mut catalog {
        if row.provider_id == "opencode" && row.model_id == "muse-spark-1.3-contributor-free" {
            row.upstream_status = Some("deprecated".into());
        }
    }
    ingest(&store, &catalog);
    place(
        &store,
        "opencode/muse-spark-1.3-contributor-free",
        1800.0,
        1500.0,
    );
    let mut high = store
        .get_fact_by_route_key("opencode/muse-spark-1.3-contributor-free")
        .unwrap()
        .unwrap();
    high.variant = Some("high".into());
    store.upsert_fact(&high, FactSource::Override).unwrap();
    let facts = routed(&store);
    for route in [
        "opencode/muse-spark-1.3-contributor-free",
        "opencode/muse-spark-1.3-contributor-free/high",
    ] {
        assert!(facts[route].is_deprecated_upstream(), "{route}");
    }
    assert!(!facts["meta/muse-spark-1.3"].is_deprecated_upstream());
    assert!(!store.list_canonical_gaps().unwrap().iter().any(|row| row
        .route_key
        .starts_with("opencode/muse-spark-1.3-contributor-free")));
}

/// The catalog inventory records each model's published reasoning efforts, even
/// when the operator configured only one effort as a route (Gemini 4 Argon is
/// configured only at `high`), so a consumer can lower a model's effort to one
/// the catalog publishes.
#[test]
fn the_catalog_inventory_serves_the_published_reasoning_efforts() {
    let rig = Rig::new("reasoning-efforts");
    let store = rig.open();
    let mut argon = served("google/antigravity-gemini-4-argon", None);
    argon.reasoning_efforts = Some(vec!["low".into(), "medium".into(), "high".into()]);
    let unread = served("google/gemini-3.5-flash", None);
    ingest(&store, &[argon, unread]);

    let inventory = store.get_catalog_facts().unwrap();
    let row = |model: &str| {
        inventory
            .iter()
            .find(|row| row.fact.provider_id == "google" && row.fact.model_id == model)
            .unwrap_or_else(|| panic!("{model} is served"))
    };
    let argon = serde_json::to_value(row("antigravity-gemini-4-argon")).unwrap();
    assert_eq!(
        argon["reasoningEfforts"],
        serde_json::json!(["low", "medium", "high"])
    );
    let unread = serde_json::to_value(row("gemini-3.5-flash")).unwrap();
    assert!(
        unread.get("reasoningEfforts").is_none(),
        "a model the catalog stated no efforts for serves none"
    );
}
