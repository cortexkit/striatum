//! Winner ladder tests compare operator notices with what live selection would choose.

use super::*;

/// A store holding two routes of one test-only provider, and ladder sources
/// whose observed availability only this rig writes, so the seed catalog
/// never competes. `incumbent` wins the top tier; `small` is cheap and weak.
struct LadderRig {
    /// Held so the scratch directory (and the store file) outlive the test.
    rig: Rig,
    store: SharedStore,
    sources: winner_ladder::LadderSources,
    provider: String,
}

impl LadderRig {
    fn new(label: &str) -> Self {
        let rig = Rig::new(label);
        let store: SharedStore = Arc::new(rig.open_store());
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let provider = format!("ladder-{label}-{}-{n}", std::process::id());
        let mut incumbent = fact(&provider, "incumbent");
        incumbent.scores.elo = 1_500.0;
        incumbent.scores.eq = 1_500.0;
        incumbent.cost_per_m_tok_out = Some(10.0);
        let mut small = fact(&provider, "small");
        small.scores.elo = 1_200.0;
        small.scores.eq = 1_500.0;
        small.cost_per_m_tok_out = Some(0.5);
        for row in [&incumbent, &small] {
            store.upsert_fact(row, FactSource::Override).unwrap();
        }
        let sources = winner_ladder::LadderSources {
            routing_config: rig.dir.join("striatum.jsonc"),
            operator_config: rig.dir.join("striatum.jsonc"),
            observed: Arc::new(winner_ladder::ObservedSelections::default()),
        };
        Self {
            rig,
            store,
            sources,
            provider,
        }
    }

    fn route(&self, model: &str) -> String {
        make_route_key(&self.provider, model, None)
    }

    /// Stand in for a host route.select: record the availability it was handed
    /// and the runtime snapshot as the store holds it now.
    fn observe(&self, models: &[&str]) {
        self.observe_as("mason", models);
    }

    /// A host route.select for `target_agent`, which is the caller class.
    fn observe_as(&self, target_agent: &str, models: &[&str]) {
        self.sources.observed.record(
            winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, target_agent),
            models.iter().map(|model| self.route(model)),
            Vec::new(),
            self.store.build_snapshot(now_ms() as f64).unwrap(),
        );
    }

    /// A process restart: the store is closed and reopened from its file, and
    /// the in-memory record of observed selections starts empty.
    fn restart(self) -> Self {
        let Self {
            rig,
            store,
            sources,
            provider,
        } = self;
        drop(store);
        let store: SharedStore = Arc::new(rig.open_store());
        Self {
            rig,
            store,
            sources: winner_ladder::LadderSources {
                observed: Arc::new(winner_ladder::ObservedSelections::default()),
                ..sources
            },
            provider,
        }
    }

    fn ladder(&self) -> winner_ladder::WinnerLadder {
        winner_ladder::compute_ladder(self.store.as_ref(), &self.sources)
            .unwrap()
            .expect("a selection was observed")
    }

    fn announce(&self, trigger: &str) -> Option<winner_ladder::WinnerChangeNotice> {
        winner_ladder::announce(self.store.as_ref(), &self.sources, trigger)
    }

    /// Notice lines this rig's provider appears in, in emission order.
    fn notices(&self) -> Vec<serde_json::Value> {
        winner_ladder::emitted_lines()
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.contains(&self.provider))
            .map(|line| {
                let body = line
                    .strip_prefix(winner_ladder::NOTICE_PREFIX)
                    .expect("notice line carries the fixed prefix");
                serde_json::from_str(body).unwrap()
            })
            .collect()
    }

    async fn upsert(&self, params: serde_json::Value) -> Frame {
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({
                "method": ROUTE_MODEL_UPSERT_OP,
                "params": params,
            }))
            .unwrap(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        handle_model_upsert(
            frame,
            &tx,
            &self.store,
            &self.sources,
            &empty_broca_credentials(),
        )
        .await
        .unwrap();
        rx.recv().await.unwrap()
    }

    fn challenger_params(&self) -> serde_json::Value {
        json!({
            "providerID": self.provider,
            "modelID": "challenger",
            "elo": 2_000.0,
            "eq": 1_500.0,
            "costPerMTokOut": 30.0,
        })
    }

    /// Place the challenger straight into the store, as a placement applied
    /// while no ladder was being computed would.
    fn place_challenger_directly(&self) {
        let mut challenger = fact(&self.provider, "challenger");
        challenger.scores.elo = 2_000.0;
        challenger.scores.eq = 1_500.0;
        challenger.cost_per_m_tok_out = Some(30.0);
        self.store
            .upsert_fact(&challenger, FactSource::Override)
            .unwrap();
    }
}

fn host_tier(ladder: &winner_ladder::WinnerLadder, tier: u8) -> winner_ladder::TierWinners {
    ladder.ladders[&host_mason().label()].tiers[&tier].clone()
}

fn host_mason() -> winner_ladder::LadderKey {
    winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, "mason")
}

/// What the router itself returns at `tier` for the recorded host selection,
/// computed directly rather than through the ladder.
fn direct_live_selection(rig: &LadderRig, tier: u8) -> Option<String> {
    let observed = rig.sources.observed.current();
    let observation = &observed[&host_mason()];
    let resolved = resolve_launch_requirements(
        "mason",
        Some(&RequestedModelRequirements {
            iq: Some(f64::from(tier)),
            eq: None,
            caps: None,
        }),
    )
    .unwrap();
    select_model_route_with_config(
        SelectModelRouteInput {
            target_agent: "mason".to_string(),
            target_is_head: false,
            requirements: resolved.requirements,
            available_models: observation.available.iter().cloned().collect(),
            facts: rig
                .store
                .get_facts_with_observed_speed(
                    now_ms() as f64,
                    config_overrides::live_config(&rig.sources.routing_config).pricing(),
                )
                .unwrap(),
            runtime_snapshot: Some(observation.snapshot.clone()),
            exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(),
            required_capabilities: resolved.required_capabilities,
            is_subscription_route: None,
            min_best_effort_adequacy: None,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: HashSet::new(),
            providers_without_cache_write_reports: HashSet::new(),
        },
        &DEFAULT_ROUTING_CONFIG,
    )
    .ok()
    .map(|selected| selected.route_key)
}

#[test]
fn no_ladder_exists_before_a_selection_has_been_observed() {
    let rig = LadderRig::new("unobserved");
    assert_eq!(
        winner_ladder::compute_ladder(rig.store.as_ref(), &rig.sources).unwrap(),
        None
    );
    assert_eq!(rig.announce("boot"), None);
    assert_eq!(
        rig.store.route_winner_baseline().unwrap(),
        None,
        "absence of a selection is not a ladder and must not become the baseline"
    );
}

#[test]
fn the_first_ever_ladder_is_stored_silently() {
    let rig = LadderRig::new("first");
    rig.observe(&["incumbent", "small"]);
    assert_eq!(rig.announce("first selection"), None);
    let stored = rig
        .store
        .route_winner_baseline()
        .unwrap()
        .expect("baseline written");
    let stored: winner_ladder::WinnerLadder = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored, rig.ladder());
    assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
}

#[tokio::test]
async fn placement_moving_the_live_winner_emits_one_notice_naming_what_selection_returns() {
    let rig = LadderRig::new("raise");
    rig.observe(&["incumbent", "small", "challenger"]);
    assert_eq!(rig.announce("first selection"), None);
    assert_eq!(
        host_tier(&rig.ladder(), 100).live,
        Some(rig.route("incumbent")),
        "precondition: the incumbent holds iq 100"
    );

    let response = rig.upsert(rig.challenger_params()).await;
    assert_eq!(response.header.ty, FrameType::Response);

    let notices = rig.notices();
    assert_eq!(
        notices.len(),
        1,
        "exactly one notice per placement: {notices:?}"
    );
    let notice = &notices[0];
    assert_eq!(
        notice["trigger"],
        format!("route.model_upsert {}", rig.route("challenger"))
    );
    assert_eq!(notice["rule"], winner_ladder::NOTICE_RULE);
    let changes = notice["changes"].as_array().unwrap();
    let top = changes
        .iter()
        .find(|change| change["tiers"].as_array().unwrap().contains(&json!(100)))
        .expect("a change naming iq 100");
    assert_eq!(top["substrate"], winner_ladder::SUBSTRATE_HOST);
    assert_eq!(top["oldRoute"], json!(rig.route("incumbent")));
    assert_eq!(top["newRoute"], json!(rig.route("challenger")));
    for change in changes {
        for tier in change["tiers"].as_array().unwrap() {
            let tier = tier.as_u64().unwrap() as u8;
            assert_eq!(
                change["newRoute"],
                json!(direct_live_selection(&rig, tier)),
                "the notice names what route selection returns at iq {tier}"
            );
        }
    }
    // The announced ladder is the new baseline: nothing more to announce.
    assert_eq!(rig.announce("again"), None);
}

#[tokio::test]
async fn a_route_outside_the_recorded_availability_is_never_announced() {
    let rig = LadderRig::new("fence");
    rig.observe(&["incumbent", "small"]);
    assert_eq!(rig.announce("first selection"), None);

    let response = rig.upsert(rig.challenger_params()).await;
    assert_eq!(response.header.ty, FrameType::Response);
    assert!(
        rig.store
            .get_fact_by_route_key(&rig.route("challenger"))
            .unwrap()
            .is_some(),
        "the placement must land"
    );
    assert_eq!(
        rig.notices(),
        Vec::<serde_json::Value>::new(),
        "the challenger is not in what live selection can reach"
    );
    let ladder = rig.ladder();
    for winners in ladder.ladders[&host_mason().label()].tiers.values() {
        for route in [&winners.neutral, &winners.live].into_iter().flatten() {
            assert!(
                [rig.route("incumbent"), rig.route("small")].contains(route),
                "ladder names {route}, outside the recorded availability"
            );
        }
    }

    // Once live selection reports the challenger as reachable, the move is real.
    let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
    rig.observe(&["incumbent", "small", "challenger"]);
    let notice = watch
        .check(rig.store.as_ref(), &rig.sources)
        .expect("availability now includes the stronger route");
    assert!(notice
        .trigger
        .contains("opencode-host / mason availability"));
    assert!(notice.changes.iter().any(|change| {
        change.new_route.as_deref() == Some(rig.route("challenger").as_str())
            && change.tiers.contains(&100)
    }));
}

#[tokio::test]
async fn placement_that_changes_a_score_but_no_winner_emits_no_notice() {
    let rig = LadderRig::new("twin");
    rig.observe(&["incumbent", "small"]);
    assert_eq!(rig.announce("first selection"), None);
    let before = rig.ladder();

    let response = rig
        .upsert(json!({
            "providerID": rig.provider,
            "modelID": "small",
            "elo": 1_201.0,
        }))
        .await;
    assert_eq!(response.header.ty, FrameType::Response);
    let stored = rig
        .store
        .get_fact_by_route_key(&rig.route("small"))
        .unwrap()
        .unwrap();
    assert_eq!(stored.scores.elo, 1_201.0, "the placement must land");
    assert_eq!(rig.ladder(), before, "precondition: no tier's winner moved");

    assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
}

#[test]
fn a_quota_wall_that_moves_only_the_live_winner_is_not_announced() {
    let rig = LadderRig::new("quota");
    rig.observe(&["incumbent", "small"]);
    assert_eq!(rig.announce("first selection"), None);
    let incumbent = rig.route("incumbent");

    // Wall the iq-100 winner hard: live selection routes around it.
    let far_future = now_ms() as f64 + 3_600_000.0;
    record_cooldown_update(
        rig.store.as_ref(),
        &RecordCooldownParams {
            route_key: incumbent.clone(),
            capacity_key: incumbent.clone(),
            credential_id: None,
            cooldown_until: Some(far_future),
            quota_exhausted_until: Some(far_future),
            quota_pressure: Some(1.0),
            wall_class: Some(RouteWallClass::Quota),
            provider_wide: false,
            increment_quota_error: true,
            increment_failure: false,
            last_error: Some("usage limit".into()),
        },
    )
    .unwrap();
    rig.observe(&["incumbent", "small"]);
    let walled = host_tier(&rig.ladder(), 100);
    assert_eq!(walled.neutral, Some(incumbent.clone()));
    assert_ne!(
        walled.live,
        Some(incumbent),
        "precondition: the wall moves the live winner"
    );

    assert_eq!(rig.announce("quota"), None);
    assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
}

#[test]
fn a_restart_between_two_differing_ladders_announces_once() {
    let rig = LadderRig::new("restart-moved");
    rig.observe(&["incumbent", "small", "challenger"]);
    assert_eq!(rig.announce("first selection"), None);

    rig.place_challenger_directly();
    let rig = rig.restart();
    let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
    assert_eq!(
        rig.announce("boot"),
        None,
        "nothing is compared before a selection arrives"
    );
    rig.observe(&["incumbent", "small", "challenger"]);
    let notice = watch
        .check(rig.store.as_ref(), &rig.sources)
        .expect("the first selection after the restart reveals the move");
    assert!(notice.changes.iter().any(|change| {
        change.old_route.as_deref() == Some(rig.route("incumbent").as_str())
            && change.new_route.as_deref() == Some(rig.route("challenger").as_str())
            && change.tiers.contains(&100)
    }));
    assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
    assert_eq!(rig.announce("later"), None);
    assert_eq!(rig.notices().len(), 1);
}

#[test]
fn a_restart_with_no_change_announces_nothing() {
    let rig = LadderRig::new("restart-same");
    rig.observe(&["incumbent", "small"]);
    assert_eq!(rig.announce("first selection"), None);

    let rig = rig.restart();
    let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
    rig.observe(&["incumbent", "small"]);
    assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
    assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
}

#[test]
fn config_reload_excluding_the_winner_announces_once_and_an_unchanged_config_is_silent() {
    let rig = LadderRig::new("reload");
    rig.observe(&["incumbent", "small"]);
    assert_eq!(rig.announce("first selection"), None);
    let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
    assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);

    crate::test_config::write(
        &rig.sources.routing_config,
        serde_json::to_vec(&json!({
            "model_routing": { "exclude": {"all": [rig.route("incumbent")]} }
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        watch.check(rig.store.as_ref(), &rig.sources),
        None,
        "a pending edit has not changed the active config"
    );
    config_overrides::settle_live_config(&rig.sources.routing_config);
    let notice = watch
        .check(rig.store.as_ref(), &rig.sources)
        .expect("excluding the iq-100 winner moves it");
    assert!(notice.trigger.contains("routing config "));
    assert!(notice.changes.iter().any(|change| {
        change.old_route.as_deref() == Some(rig.route("incumbent").as_str())
            && change.tiers.contains(&100)
    }));
    assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
}

/// A baseline row written before ladders were keyed by caller class is
/// replaced silently rather than compared against the new keys.
#[test]
fn a_stored_baseline_of_the_old_shape_is_replaced_without_announcing() {
    let rig = LadderRig::new("old-shape");
    let old = json!({
        "substrates": {
            winner_ladder::SUBSTRATE_HOST: {
                "available": [rig.route("small")],
                "tiers": { "100": { "neutral": rig.route("small"), "live": rig.route("small") } }
            }
        },
        "eq": 50.0
    });
    rig.store
        .put_route_winner_baseline(&old.to_string(), "before the upgrade", 1)
        .unwrap();
    rig.observe(&["incumbent", "small"]);
    assert_eq!(rig.announce("first selection after upgrade"), None);
    assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());
    let stored: winner_ladder::WinnerLadder =
        serde_json::from_str(&rig.store.route_winner_baseline().unwrap().unwrap()).unwrap();
    assert_eq!(stored.version, winner_ladder::LADDER_FORMAT_VERSION);
    assert_eq!(stored, rig.ladder());
}

/// The live flap: the plugin's mason launches and core's seed-catalog explore
/// selections hand the host different lists, whose iq-100 winners differ.
/// Once each class has a baseline, their alternation announces nothing, and
/// each class's ladder replays its own list rather than a shared one.
#[test]
fn two_caller_classes_alternating_their_lists_announce_nothing_once_both_have_baselines() {
    let rig = LadderRig::new("classes");
    rig.place_challenger_directly();
    let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
    rig.observe_as("mason", &["incumbent", "small", "challenger"]);
    assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
    rig.observe_as("explore", &["incumbent", "small"]);
    assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);

    for round in 0..3 {
        rig.observe_as("mason", &["incumbent", "small", "challenger"]);
        assert_eq!(
            watch.check(rig.store.as_ref(), &rig.sources),
            None,
            "round {round}: mason's list arriving again is not a change"
        );
        assert_eq!(rig.announce("alternation"), None);
        rig.observe_as("explore", &["incumbent", "small"]);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
        assert_eq!(rig.announce("alternation"), None);
    }
    assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());

    let ladder = rig.ladder();
    let explore = winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, "explore");
    assert_eq!(
        ladder.ladders[&host_mason().label()].tiers[&100].live,
        Some(rig.route("challenger")),
        "mason's ladder replays mason's list"
    );
    assert_eq!(
        ladder.ladders[&explore.label()].tiers[&100].live,
        Some(rig.route("incumbent")),
        "explore's ladder replays explore's list, not mason's"
    );
}

#[tokio::test]
async fn a_placement_moving_one_class_winner_announces_for_that_class_only() {
    let rig = LadderRig::new("one-class");
    rig.observe_as("mason", &["incumbent", "small", "challenger"]);
    rig.observe_as("explore", &["incumbent", "small"]);
    assert_eq!(rig.announce("first selections"), None);

    let response = rig.upsert(rig.challenger_params()).await;
    assert_eq!(response.header.ty, FrameType::Response);

    let notices = rig.notices();
    assert_eq!(notices.len(), 1, "one notice: {notices:?}");
    let changes = notices[0]["changes"].as_array().unwrap();
    assert!(!changes.is_empty());
    for change in changes {
        assert_eq!(change["key"], "opencode-host / mason", "{change}");
        assert_eq!(change["callerClass"], "mason");
        assert_eq!(change["substrate"], winner_ladder::SUBSTRATE_HOST);
    }
    assert!(changes.iter().any(|change| {
        change["newRoute"] == json!(rig.route("challenger"))
            && change["tiers"].as_array().unwrap().contains(&json!(100))
    }));
    assert_eq!(rig.announce("again"), None);
}

/// Plugin explore launches and core's seed-catalog explore selections share
/// a class but not a list. Within the window the class replays the union, so
/// their alternation is not an availability change.
#[test]
fn one_class_alternating_two_lists_within_the_window_uses_their_union_and_announces_nothing() {
    let rig = LadderRig::new("union");
    rig.place_challenger_directly();
    let mut watch = winner_ladder::ConfigReloadWatch::new(&rig.sources);
    rig.observe_as("explore", &["incumbent", "small", "challenger"]);
    assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);

    for round in 0..3 {
        rig.observe_as("explore", &["incumbent", "small"]);
        assert_eq!(
            watch.check(rig.store.as_ref(), &rig.sources),
            None,
            "round {round}: the narrower list adds nothing to the union"
        );
        rig.observe_as("explore", &["incumbent", "small", "challenger"]);
        assert_eq!(watch.check(rig.store.as_ref(), &rig.sources), None);
        assert_eq!(rig.announce("alternation"), None);
    }
    assert_eq!(rig.notices(), Vec::<serde_json::Value>::new());

    rig.observe_as("explore", &["incumbent", "small"]);
    let explore = winner_ladder::LadderKey::new(winner_ladder::SUBSTRATE_HOST, "explore");
    let ladder = rig.ladder();
    let replayed = &ladder.ladders[&explore.label()];
    assert!(
        replayed.available.contains(&rig.route("challenger")),
        "the union keeps the wider list after the narrower one arrives"
    );
    assert_eq!(replayed.tiers[&100].live, Some(rig.route("challenger")));
}

/// The ladder prices facts with the multipliers of the routing config it
/// reads. Its first computation after boot must already use them; pricing
/// subscription routes at list would disagree with every later ladder.
#[test]
fn the_first_ladder_after_boot_prices_routes_with_the_configured_multipliers() {
    let rig = LadderRig::new("multiplier");
    let listed_provider = format!("{}-listed", rig.provider);
    let listed_route = make_route_key(&listed_provider, "listed", None);
    let mut listed = fact(&listed_provider, "listed");
    listed.scores.elo = 1_500.0;
    listed.scores.eq = 1_500.0;
    listed.cost_per_m_tok_out = Some(8.0);
    rig.store
        .upsert_fact(&listed, FactSource::Override)
        .unwrap();
    // At list price `listed` (8) undercuts `incumbent` (10). A provider-wide
    // multiplier makes the incumbent cheaper without subsidizing its rival.
    let mut providers = serde_json::Map::new();
    providers.insert(
        rig.provider.clone(),
        json!({ "subscription_multiplier": 100 }),
    );
    crate::test_config::write(
        &rig.sources.routing_config,
        serde_json::to_vec(&json!({ "providers": providers })).unwrap(),
    )
    .unwrap();
    rig.sources.observed.record(
        host_mason(),
        [rig.route("incumbent"), listed_route.clone()],
        Vec::new(),
        rig.store.build_snapshot(now_ms() as f64).unwrap(),
    );

    let first = rig.ladder();
    assert_eq!(
        host_tier(&first, 30).neutral,
        Some(rig.route("incumbent")),
        "the first ladder must already see the multiplier"
    );
    assert_eq!(
        first,
        rig.ladder(),
        "identical inputs give identical ladders"
    );
    crate::test_config::write(&rig.sources.routing_config, "{}").unwrap();
    config_overrides::settle_live_config(&rig.sources.routing_config);
    assert_eq!(
        host_tier(&rig.ladder(), 30).neutral,
        Some(listed_route),
        "without the provider multiplier the lower list price must win"
    );
}
