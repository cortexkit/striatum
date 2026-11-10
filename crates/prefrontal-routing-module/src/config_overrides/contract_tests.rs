use super::*;
use cortexkit_role_routing::service_tier::ServiceTier;
use std::sync::atomic::{AtomicU64, Ordering};

struct Rig {
    dir: PathBuf,
    path: PathBuf,
    revision: u64,
}

impl Rig {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "routing-schema-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self {
            path: dir.join("prefrontal-routing.jsonc"),
            dir,
            revision: 0,
        }
    }

    fn write(&mut self, document: &str) {
        self.revision += 1;
        crate::test_config::write(&self.path, document).unwrap();
        std::fs::File::open(&self.path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(
                SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000 + self.revision),
            ))
            .unwrap();
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        live_configs().lock().unwrap().remove(&self.path);
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

const GOOD: &str = r#"{"model_routing":{"exclude":{"all":["anthropic"]},"panels":{"audit":{"topK":1,"by":"iq"}},"pins":{"synthesizer":{"model":"openai/gpt-6","variant":"high"}}}}"#;

#[test]
fn striatum_global_config_prefers_primary_falls_back_and_never_merges() {
    for (primary_exists, previous_exists) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let dir = cortexkit_test_support::ScratchDir::new("striatum-global-config");
        let primary = dir.join("striatum.jsonc");
        let previous = dir.join("prefrontal-routing.jsonc");
        if primary_exists {
            crate::test_config::write_raw(
                &primary,
                r#"{"consumers":{"head":{"service_tier":"fast"}}}"#,
            )
            .unwrap();
        }
        if previous_exists {
            crate::test_config::write_raw(
                &previous,
                r#"{"consumers":{"sidekick":{"service_tier":"ultrafast"}}}"#,
            )
            .unwrap();
        }
        let mut reports = Vec::new();
        let chosen = config_path_in_with_reporter(primary.parent().unwrap(), |message| {
            reports.push(message.to_owned())
        });
        assert_eq!(
            chosen,
            if primary_exists || !previous_exists {
                primary.clone()
            } else {
                previous.clone()
            }
        );
        let config = live_config_at(&chosen, Instant::now()).unwrap();
        assert_eq!(
            config.service_tier("head", "p", "m"),
            primary_exists.then_some(ServiceTier::Fast)
        );
        assert_eq!(
            config.service_tier("sidekick", "p", "m"),
            (!primary_exists && previous_exists).then_some(ServiceTier::Ultrafast)
        );
        assert_eq!(reports.len(), usize::from(previous_exists));
        if previous_exists {
            let report = &reports[0];
            assert!(report.starts_with("[striatum] config_renamed:"), "{report}");
            assert!(
                report.contains(&previous.display().to_string())
                    && report.contains(&primary.display().to_string()),
                "{report}"
            );
            assert!(
                report.contains(if primary_exists {
                    "is ignored"
                } else {
                    "rename it to"
                }),
                "{report}"
            );
        }
        live_configs().lock().unwrap().remove(&chosen);
    }
}

#[test]
fn striatum_fallback_reload_reads_selected_file() {
    let mut rig = Rig::new();
    rig.write(GOOD);
    let chosen = config_path_in(&rig.dir);
    assert_eq!(chosen, rig.path);
    let now = Instant::now();
    live_config_at(&chosen, now).unwrap();
    crate::test_config::write_raw(rig.dir.join("striatum.jsonc"), "not json").unwrap();
    rig.write(r#"{"model_routing":{"exclude":{"all":["xai"]}}}"#);
    live_config_at(&chosen, now).unwrap();
    let updated = live_config_at(&chosen, now + RELOAD_DEBOUNCE).unwrap();
    assert_eq!(updated.operator_policy().exclude, ["xai"]);
}

#[test]
fn striatum_still_refuses_alfonso_legacy_file() {
    for primary_exists in [false, true] {
        let dir = cortexkit_test_support::ScratchDir::new("striatum-alfonso-legacy");
        let primary = dir.join("striatum.jsonc");
        let legacy = dir.join("alfonso-routing.jsonc");
        if primary_exists {
            crate::test_config::write_raw(&primary, "{}").unwrap();
        }
        crate::test_config::write_raw(&legacy, "not json").unwrap();
        let error = initialize_config(&primary).unwrap_err();
        assert!(
            error.contains("config_renamed")
                && error.contains(&legacy.display().to_string())
                && error.contains(&primary.display().to_string()),
            "{error}"
        );
        assert!(
            error.contains(if primary_exists { "delete" } else { "move" }),
            "{error}"
        );
    }
}

fn first_load_refuses(document: &str, message: &str) {
    let mut rig = Rig::new();
    rig.write(document);
    let error = initialize_config(&rig.path)
        .expect_err("invalid first load must not become an empty policy");
    assert!(error.contains(message), "{error}");
    assert!(!live_configs().lock().unwrap().contains_key(&rig.path));
    rig.write(GOOD);
    initialize_config(&rig.path).unwrap();
    assert_eq!(
        live_config(&rig.path).operator_policy().exclude,
        ["anthropic"]
    );
}

fn reload_refuses(document: &str, message: &str) {
    let mut rig = Rig::new();
    rig.write(GOOD);
    let now = Instant::now();
    let held = live_config_at(&rig.path, now).unwrap();
    rig.write(document);
    let mut reports = Vec::new();
    for elapsed in [
        Duration::ZERO,
        RELOAD_DEBOUNCE / 2,
        RELOAD_DEBOUNCE,
        RELOAD_DEBOUNCE * 2,
    ] {
        let next = live_config_at_with_reporter(&rig.path, now + elapsed, |error| {
            reports.push(error.to_owned())
        })
        .unwrap();
        assert!(
            Arc::ptr_eq(&held, &next),
            "a refusal must keep the entire last good snapshot"
        );
    }
    assert_eq!(reports.len(), 1, "one refusal report per changed file");
    assert!(reports[0].contains(message), "{}", reports[0]);
    rig.write(document);
    live_config_at_with_reporter(&rig.path, now + RELOAD_DEBOUNCE * 3, |error| {
        reports.push(error.to_owned())
    })
    .unwrap();
    live_config_at_with_reporter(&rig.path, now + RELOAD_DEBOUNCE * 4, |error| {
        reports.push(error.to_owned())
    })
    .unwrap();
    assert_eq!(
        reports.len(),
        2,
        "a second edit must produce a second refusal report"
    );
    rig.write(r#"{"model_routing":{"exclude":{"all":["xai"]}}}"#);
    let repaired = settle_live_config(&rig.path);
    assert_eq!(repaired.operator_policy().exclude, ["xai"]);
    assert!(
        repaired.panels().audit.is_none(),
        "a class is replaced whole, not deep-merged"
    );
    assert!(repaired.slot_pin("synthesizer").is_none());
    assert_eq!(held.operator_policy().exclude, ["anthropic"]);
}

macro_rules! refusal_case {
    ($name:ident, $document:expr, $message:expr) => {
        mod $name {
            use super::*;
            #[test]
            fn first_load() {
                first_load_refuses($document, $message);
            }
            #[test]
            fn reload() {
                reload_refuses($document, $message);
            }
        }
    };
}

refusal_case!(
    floor_null,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","floor":null}}}}"#,
    "invalid type"
);
refusal_case!(
    policy_null,
    r#"{"model_routing":{"panels":{"audit":null}}}"#,
    "invalid type"
);
refusal_case!(
    pin_null,
    r#"{"model_routing":{"pins":{"synthesizer":null}}}"#,
    "invalid type"
);
refusal_case!(
    variant_null,
    r#"{"model_routing":{"pins":{"synthesizer":{"model":"p/m","variant":null}}}}"#,
    "invalid type"
);

refusal_case!(
    exclude_shape,
    r#"{"providers":{"p":{"not_for":true}}}"#,
    "invalid type"
);
// A pin the config's own rules refuse for the consumer the pin serves could
// never be served: routing applies `not_for` to explicit pins too.
refusal_case!(
    synthesizer_pin_refused_by_model_not_for,
    r#"{"consumers":{"synthesizer":{"pin":{"model":"anthropic/claude-opus-5-5","variant":"medium"}}},
        "models":{"anthropic/claude-opus-5-5":{"not_for":["athena","lab"]}}}"#,
    r#"consumers.synthesizer.pin pins anthropic/claude-opus-5-5 but this config refuses route anthropic/claude-opus-5-5 for consumer "athena": models["anthropic/claude-opus-5-5"].not_for lists "athena""#
);
refusal_case!(
    lab_planner_pin_refused_by_provider_not_for,
    r#"{"consumers":{"lab_planner":{"pin":{"model":"anthropic/claude-opus-5-5"}}},
        "providers":{"anthropic":{"not_for":["lab"]}}}"#,
    r#"consumers.lab_planner.pin pins anthropic/claude-opus-5-5 but this config refuses route anthropic/claude-opus-5-5 for consumer "lab": providers.anthropic.not_for lists "lab""#
);
refusal_case!(
    spec_designer_pin_refused_by_provider_models_list,
    r#"{"consumers":{"spec_designer":{"pin":{"model":"anthropic/claude-opus-5-5"}}},
        "providers":{"anthropic":{"models":["claude-sonnet-5"]}}}"#,
    r#"consumers.spec_designer.pin pins anthropic/claude-opus-5-5 but this config refuses route anthropic/claude-opus-5-5 for consumer "athena": providers.anthropic.models ["claude-sonnet-5"] does not list the model"#
);
refusal_case!(
    project_pin_refused_by_model_only_for,
    r#"{"consumers":{"mason":{"projects":{"proj":{"pin":"p/m:high"}}}},
        "models":{"p/m":{"only_for":["sidekick"]}}}"#,
    r#"consumers.mason.projects.proj.pin pins p/m but this config refuses route p/m for consumer "mason": models["p/m"].only_for ["sidekick"] does not name "mason""#
);
refusal_case!(
    removed_aa,
    r#"{"aa":{"api_key":"never-read"},"unknown":true}"#,
    "config_key_removed: aa"
);
refusal_case!(removed_aa_null, r#"{"aa":null}"#, "config_key_removed: aa");
refusal_case!(
    removed_explicit_only_providers,
    r#"{"model_routing":{"explicit_only_providers":["anthropic-platform"],"exclude":{"all":["xai"]}}}"#,
    "unknown field `explicit_only_providers`"
);
refusal_case!(
    removed_explicit_only_providers_empty,
    r#"{"model_routing":{"explicit_only_providers":[]}}"#,
    "unknown field `explicit_only_providers`"
);
refusal_case!(
    removed_account_selection_shadow,
    r#"{"account_selection":{"enabled":true,"shadow":false}}"#,
    "unknown field `shadow`"
);
refusal_case!(
    removed_account_selection_shadow_null,
    r#"{"account_selection":{"shadow":null}}"#,
    "unknown field `shadow`"
);
refusal_case!(
    unknown_root,
    r#"{"surprise":true}"#,
    "unknown field `surprise`"
);
refusal_case!(
    unknown_routing_key,
    r#"{"model_routing":{"surprise":true}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    unknown_exclude_group,
    r#"{"providers":{"p":{"surprise":[]}}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    non_array_exclude_group,
    r#"{"providers":{"p":{"not_for":"mason"}}}"#,
    "expected a sequence"
);
refusal_case!(
    unknown_catalog_key,
    r#"{"catalog":{"surprise":1}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    unknown_account_key,
    r#"{"account_selection":{"surprise":true}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    unknown_model_key,
    r#"{"model_routing":{"models":{"p/m":{"surprise":1}}}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    unknown_overqualification_key,
    r#"{"model_routing":{"overqualification":{"surprise":1}}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    unknown_class,
    r#"{"model_routing":{"panels":{"surprise":{"topK":1,"by":"iq"}}}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    unknown_policy_key,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","surprise":1}}}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    top_k_required,
    r#"{"model_routing":{"panels":{"audit":{"by":"iq"}}}}"#,
    "missing field `topK`"
);
refusal_case!(
    top_k_zero,
    r#"{"model_routing":{"panels":{"audit":{"topK":0,"by":"iq"}}}}"#,
    "topK"
);
refusal_case!(
    top_k_negative,
    r#"{"model_routing":{"panels":{"audit":{"topK":-1,"by":"iq"}}}}"#,
    "invalid value"
);
refusal_case!(
    top_k_fraction,
    r#"{"model_routing":{"panels":{"audit":{"topK":1.5,"by":"iq"}}}}"#,
    "invalid type"
);
refusal_case!(
    by_required,
    r#"{"model_routing":{"panels":{"audit":{"topK":1}}}}"#,
    "missing field `by`"
);
refusal_case!(
    by_enum,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"random"}}}}"#,
    "unknown variant"
);
refusal_case!(
    extra_negative,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","extra":-1}}}}"#,
    "invalid value"
);
refusal_case!(
    extra_fraction,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","extra":0.5}}}}"#,
    "invalid type"
);
refusal_case!(
    extra_by_required,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","extra":1}}}}"#,
    "extraBy is required"
);
refusal_case!(
    extra_by_null,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","extra":1,"extraBy":null}}}}"#,
    "invalid routing config"
);
refusal_case!(
    extra_by_enum,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","extraBy":"shuffle"}}}}"#,
    "unknown variant"
);
refusal_case!(
    width_cap,
    r#"{"model_routing":{"panels":{"audit":{"topK":7,"by":"iq","extra":2,"extraBy":"random"}}}}"#,
    "topK + extra"
);
refusal_case!(
    floor_negative,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","floor":-1}}}}"#,
    "invalid value"
);
refusal_case!(
    floor_over_100,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","floor":101}}}}"#,
    "floor must be"
);
refusal_case!(
    floor_fraction,
    r#"{"model_routing":{"panels":{"audit":{"topK":1,"by":"iq","floor":80.5}}}}"#,
    "invalid type"
);
refusal_case!(
    unknown_pin,
    r#"{"consumers":{"synthesizer":{"pin":{"model":"p/m","surprise":true}}}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    synthesizer_unknown_key,
    r#"{"model_routing":{"pins":{"synthesizer":{"model":"p/m","temperature":1}}}}"#,
    "unknown field `temperature`"
);
refusal_case!(
    synthesizer_model_required,
    r#"{"model_routing":{"pins":{"synthesizer":{"variant":"high"}}}}"#,
    "missing field `model`"
);
refusal_case!(
    synthesizer_model_shape,
    r#"{"model_routing":{"pins":{"synthesizer":{"model":"bare"}}}}"#,
    "must be provider/model"
);
refusal_case!(
    synthesizer_variant_type,
    r#"{"model_routing":{"pins":{"synthesizer":{"model":"p/m","variant":1}}}}"#,
    "invalid type"
);
refusal_case!(
    synthesizer_variant_empty,
    r#"{"model_routing":{"pins":{"synthesizer":{"model":"p/m","variant":" "}}}}"#,
    "variant must be non-empty"
);
refusal_case!(
    planner_model_required,
    r#"{"model_routing":{"pins":{"lab_planner":{}}}}"#,
    "missing field `model`"
);
refusal_case!(
    planner_unknown_key_refused,
    r#"{"consumers":{"lab_planner":{"pin":{"model":"p/m","unknown":"high"}}}}"#,
    "unknown field `unknown`"
);
// The designer pin is checked exactly as the synthesizer pin is.
refusal_case!(
    spec_designer_null,
    r#"{"model_routing":{"pins":{"spec_designer":null}}}"#,
    "invalid type"
);
refusal_case!(
    spec_designer_variant_null,
    r#"{"model_routing":{"pins":{"spec_designer":{"model":"p/m","variant":null}}}}"#,
    "invalid type"
);
refusal_case!(
    spec_designer_unknown_key,
    r#"{"model_routing":{"pins":{"spec_designer":{"model":"p/m","temperature":1}}}}"#,
    "unknown field `temperature`"
);
refusal_case!(
    spec_designer_model_required,
    r#"{"model_routing":{"pins":{"spec_designer":{"variant":"high"}}}}"#,
    "missing field `model`"
);
refusal_case!(
    spec_designer_model_shape,
    r#"{"model_routing":{"pins":{"spec_designer":{"model":"bare"}}}}"#,
    "consumers.spec_designer.pin.model must be provider/model"
);
refusal_case!(
    spec_designer_variant_type,
    r#"{"model_routing":{"pins":{"spec_designer":{"model":"p/m","variant":1}}}}"#,
    "invalid type"
);
refusal_case!(
    spec_designer_variant_empty,
    r#"{"model_routing":{"pins":{"spec_designer":{"model":"p/m","variant":" "}}}}"#,
    "consumers.spec_designer.pin.variant must be non-empty"
);
refusal_case!(
    unknown_variant_group,
    r#"{"consumers":{"mason":{"surprise":{}}}}"#,
    "unknown field `surprise`"
);
refusal_case!(
    negated_variant_key,
    r#"{"model_routing":{"variants":{"all":{"-openai/gpt-6":"high"}}}}"#,
    "model_routing.variants.all: key \"-openai/gpt-6\" must be provider, provider/model or provider/model/variant"
);
refusal_case!(
    blank_variant_value,
    r#"{"model_routing":{"variants":{"mason":{"openai/gpt-6":" "}}}}"#,
    "must be one non-empty word"
);
// Pricing and reserve values are checked when a document is published, so an
// invalid multiplier or reserve never reaches a selection's pricing snapshot.
refusal_case!(
    speed_multiplier_above_two,
    r#"{"model_routing":{"models":{"openai/gpt-6":{"speed_multiplier":2.5}}}}"#,
    "invalid cost/speed override for openai/gpt-6"
);
refusal_case!(
    speed_multiplier_zero,
    r#"{"model_routing":{"models":{"openai/gpt-6":{"speed_multiplier":0}}}}"#,
    "invalid cost/speed override for openai/gpt-6"
);
#[test]
fn route_subscription_multiplier_zero_loads_with_a_named_warning() {
    let mut rig = Rig::new();
    rig.write(r#"{"models":{"openai/gpt-6":{"subscription_multiplier":0}}}"#);
    crate::take_operator_warnings();
    initialize_config(&rig.path).unwrap();
    let config = live_config(&rig.path);
    let warnings = crate::take_operator_warnings();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("models.openai/gpt-6.subscription_multiplier"));
    assert_eq!(
        config.config.model_routing.models["openai/gpt-6"].subscription_multiplier,
        None
    );
}
refusal_case!(
    provider_subscription_multiplier_negative,
    r#"{"model_routing":{"subscription_multipliers":{"openai":-1}}}"#,
    "invalid subscription_multiplier for openai"
);
refusal_case!(
    keep_free_percent_above_hundred,
    r#"{"model_routing":{"keep_free_percent":{"anthropic":101}}}"#,
    "invalid keep_free_percent for anthropic"
);

fn reasoning_catalog(entries: &[(&str, &[&str])]) -> ReasoningCatalogSource {
    let catalog = ReasoningCatalog {
        models: entries
            .iter()
            .map(|(model, efforts)| {
                (
                    (*model).to_string(),
                    prefrontal_routing_store::PublishedEfforts::Published(
                        efforts.iter().map(|effort| (*effort).to_string()).collect(),
                    ),
                )
            })
            .collect(),
    };
    Arc::new(move || Ok(catalog.clone()))
}

#[test]
fn variant_overrides_are_checked_against_the_catalog_at_load_and_on_reload() {
    let mut rig = Rig::new();
    let accepted = r#"{"model_routing":{"variants":{
        "all":{"openai/gpt-6.1-sol":"high"},
        "athena":{"openai/gpt-6.1-sol":"xhigh","anthropic/claude-opus-5-5":"xhigh"},
        "mason":{"anthropic/claude-opus-5-5":"high"}
    }}}"#;
    rig.write(accepted);
    initialize_config(&rig.path).unwrap();
    let catalog = reasoning_catalog(&[
        ("openai/gpt-6.1-sol", &["low", "medium", "high", "xhigh"]),
        (
            "anthropic/claude-opus-5-5",
            &["low", "medium", "high", "xhigh", "max"],
        ),
    ]);
    attach_reasoning_catalog(&rig.path, catalog).unwrap();
    let held = live_config(&rig.path);
    assert_eq!(
        held.variant_groups().athena["anthropic/claude-opus-5-5"],
        "xhigh"
    );

    // A reload naming a variant the route does not publish keeps the last good
    // config and names the route, the variant and the published list.
    rig.write(r#"{"model_routing":{"variants":{"athena":{"openai/gpt-6.1-sol":"max"}}}}"#);
    let now = Instant::now();
    let mut reports = Vec::new();
    for elapsed in [Duration::ZERO, RELOAD_DEBOUNCE] {
        let next = live_config_at_with_reporter(&rig.path, now + elapsed, |error| {
            reports.push(error.to_owned())
        })
        .unwrap();
        assert!(Arc::ptr_eq(&held, &next));
    }
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert!(
        reports[0].contains(
            "variant_not_published: model_routing.variants.athena.openai/gpt-6.1-sol: route openai/gpt-6.1-sol does not publish variant max; published: low, medium, high, xhigh"
        ),
        "{}",
        reports[0]
    );

    // When the routing store opens, the module attaches its catalog and checks
    // the config that is already active. A catalog that no longer publishes an
    // active override (xhigh here) fails that check, and the module refuses to
    // start.
    let mut fresh = Rig::new();
    fresh.write(accepted);
    initialize_config(&fresh.path).unwrap();
    let error = attach_reasoning_catalog(
        &fresh.path,
        reasoning_catalog(&[
            ("openai/gpt-6.1-sol", &["low", "medium", "high"]),
            (
                "anthropic/claude-opus-5-5",
                &["low", "medium", "high", "max"],
            ),
        ]),
    )
    .unwrap_err();
    assert!(error.contains("does not publish variant xhigh"), "{error}");
    catalog_sources().lock().unwrap().remove(&rig.path);
    catalog_sources().lock().unwrap().remove(&fresh.path);
}

#[test]
fn check_config_offline_reads_the_store_or_says_why_it_skipped() {
    let mut rig = Rig::new();
    rig.write(r#"{"model_routing":{"variants":{"all":{"openai/gpt-6.1-sol":"high"}}}}"#);
    let missing = rig.dir.join("no-such-store.db");
    let report = check_config_offline(&rig.path, Some(&missing)).unwrap();
    assert!(
        report.starts_with("catalog check skipped: ") && report.contains("does not exist"),
        "{report}"
    );
    let report = check_config_offline(&rig.path, None).unwrap();
    assert!(report.starts_with("catalog check skipped: "), "{report}");

    // A store whose catalog publishes only low and medium for the route refuses
    // the configured variant high.
    let store_path = rig.dir.join("store.db");
    let connection = rusqlite::Connection::open(&store_path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE model_facts (route_key TEXT, provider_id TEXT, model_id TEXT, reasoning_efforts_json TEXT);
             INSERT INTO model_facts VALUES ('openai/gpt-6.1-sol', 'openai', 'gpt-6.1-sol', '[\"low\",\"medium\"]');",
        )
        .unwrap();
    drop(connection);
    let error = check_config_offline(&rig.path, Some(&store_path)).unwrap_err();
    assert!(
        error.contains(
            "route openai/gpt-6.1-sol does not publish variant high; published: low, medium"
        ),
        "{error}"
    );
    let connection = rusqlite::Connection::open(&store_path).unwrap();
    connection
        .execute(
            "UPDATE model_facts SET reasoning_efforts_json = '[\"low\",\"high\"]'",
            [],
        )
        .unwrap();
    drop(connection);
    let report = check_config_offline(&rig.path, Some(&store_path)).unwrap();
    assert!(
        report.starts_with("catalog check passed against "),
        "{report}"
    );
}

#[test]
fn converted_keys_load_and_camel_case_spellings_are_refused() {
    let document = include_str!("../../fixtures/preserved-routing.jsonc");
    for camel in [
        document.replace(
            "consecutive_terminal_failure_threshold",
            "consecutiveTerminalFailureThreshold",
        ),
        document.replace(
            "providers_without_prompt_cache",
            "providersWithoutPromptCache",
        ),
    ] {
        let mut rig = Rig::new();
        rig.write(&camel);
        assert!(initialize_config(&rig.path).is_err(), "{camel}");
    }
    {
        let document = document.to_owned();
        let mut rig = Rig::new();
        rig.write(&document);
        initialize_config(&rig.path).unwrap();
        let active = live_config(&rig.path);
        assert_eq!(
            active.document.as_deref(),
            Some(crate::test_config::document(&document).as_str())
        );
        let section = &active.config.model_routing;
        assert_eq!(
            active.exclude_groups().all,
            ["anthropic", "-anthropic/claude-5", "openai/gpt-6/high"]
        );
        assert_eq!(
            active.exclude_groups().mason,
            ["kimi-code-plan-global/k3", "openai/gpt-6-astra"]
        );
        assert_eq!(active.exclude_groups().sidekick, ["xai", "-xai/grok-5"]);
        assert_eq!(active.exclude_groups().athena, ["local/model"]);
        assert_eq!(active.exclude_groups().lab, ["google/gemini-4"]);
        assert_eq!(active.exclude_groups().core, ["deepseek"]);
        assert_eq!(
            section.subscription,
            ["ollama-cloud", "openai", "opencode-go", "-openai/metered"]
        );
        assert!(active.operator_policy().score_alias.is_empty());
        let model = &section.models["openai/gpt-6/high"];
        assert_eq!(
            (model.elo, model.eq, model.speed),
            (Some(1900.0), Some(1750.0), Some(90.0))
        );
        assert_eq!(
            (model.cost_per_mtok_out, model.context_window),
            (Some(4.0), Some(100000.0))
        );
        assert_eq!(
            (
                model.vision,
                model.speed_multiplier,
                model.subscription_multiplier
            ),
            (Some(true), Some(1.5), None)
        );
        assert_eq!(section.demand_exponent, Some(1.5));
        assert_eq!(
            (
                section.overqualification.threshold,
                section.overqualification.doubling,
                section.overqualification.base
            ),
            (Some(10.0), Some(20.0), Some(0.5))
        );
        assert_eq!(active.min_band(), Some(0.7));
        assert_eq!(
            active
                .failure_backoff()
                .consecutive_terminal_failure_threshold,
            4
        );
        assert_eq!(active.providers_without_prompt_cache(), ["local", "xai"]);
        assert_eq!(section.subscription_multipliers["openai"], 40.0);
        assert_eq!(section.keep_free_percent["openai"], 15.0);
        assert_eq!(active.catalog_poll_ms(), 12345);
        assert_eq!(
            active.account_flags(),
            AccountSelectionFlags { enabled: true }
        );
    }
}

#[test]
fn absent_file_and_missing_groups_are_empty() {
    let mut rig = Rig::new();
    initialize_config(&rig.path).unwrap();
    assert_eq!(
        live_config(&rig.path).operator_policy(),
        ModelRoutingPolicyInput::default()
    );
    assert_eq!(
        live_config(&rig.path).exclude_groups(),
        &ExcludeGroups::default()
    );
    rig.write(r#"{"model_routing":{"exclude":{"mason":["anthropic"]}}}"#);
    let next = settle_live_config(&rig.path);
    assert_eq!(
        next.exclude_groups(),
        &ExcludeGroups {
            mason: vec!["anthropic".into()],
            ..Default::default()
        }
    );
}

#[test]
fn valid_panel_boundaries_all_classes_and_strict_pins() {
    let mut rig = Rig::new();
    rig.write(
        r#"{"model_routing":{"panels":{
        "audit":{"topK":1,"by":"iq"},
        "design_review":{"topK":8,"by":"eq","extra":0},
        "diagnose":{"topK":1,"by":"cost","extra":7,"extraBy":"random","floor":0},
        "evaluate":{"topK":2,"by":"iq","extra":1,"extraBy":"eq","floor":100},
        "explain":{"topK":1,"by":"cost","extraBy":"cost"},
        "spec":{"topK":3,"by":"iq","extra":1,"extraBy":"iq"},
        "lab":{"topK":4,"by":"iq"}},
        "pins":{"synthesizer":{"model":"p/m","variant":"high"},"lab_planner":{"model":"q/n"},
            "spec_designer":{"model":"r/o","variant":"xhigh"}}}}"#,
    );
    initialize_config(&rig.path).unwrap();
    let active = live_config(&rig.path);
    let audit = active.panels().audit.as_ref().unwrap();
    assert_eq!(
        (audit.top_k, audit.extra, audit.extra_by, audit.floor),
        (1, 0, None, None)
    );
    assert_eq!(active.panels().diagnose.as_ref().unwrap().floor, Some(0));
    assert_eq!(active.panels().evaluate.as_ref().unwrap().floor, Some(100));
    assert_eq!(active.slot_pin("lab_planner").unwrap().model, "q/n");
    let designer = active.slot_pin("spec_designer").unwrap();
    assert_eq!(
        (designer.model.as_str(), designer.variant.as_deref()),
        ("r/o", Some("xhigh"))
    );
    assert_eq!(
        active.slot_pin("synthesizer").unwrap().variant.as_deref(),
        Some("high")
    );
}

#[test]
fn renamed_first_load_refuses_old_only_and_both_files() {
    for both in [false, true] {
        let mut rig = Rig::new();
        if both {
            rig.write(GOOD);
        }
        let old = rig.path.with_file_name(OLD_CONFIG_BASENAME);
        crate::test_config::write(&old, "invalid settings are never read").unwrap();
        let error = initialize_config(&rig.path).unwrap_err();
        assert!(error.contains("config_renamed"), "{error}");
        assert!(
            error.contains(&old.display().to_string())
                && error.contains(&rig.path.display().to_string()),
            "{error}"
        );
        assert!(
            error.contains(if both { "delete" } else { "move" }),
            "{error}"
        );
    }
}

#[test]
fn renamed_reload_keeps_last_good_and_reports_once_per_old_name_change() {
    let mut rig = Rig::new();
    rig.write(GOOD);
    let held = live_config(&rig.path);
    let old = rig.path.with_file_name(OLD_CONFIG_BASENAME);
    let mut reports = Vec::new();
    let now = Instant::now();
    for edit in 0..2 {
        crate::test_config::write(&old, "x".repeat(edit + 1)).unwrap();
        for tick in 0..3 {
            let next = live_config_at_with_reporter(
                &rig.path,
                now + RELOAD_DEBOUNCE * (edit as u32 * 3 + tick),
                |error| reports.push(error.to_owned()),
            )
            .unwrap();
            assert!(Arc::ptr_eq(&held, &next));
        }
        assert_eq!(reports.len(), edit + 1);
        assert!(reports[edit].contains("config_renamed"));
    }
    std::fs::remove_file(old).unwrap();
    assert_eq!(
        settle_live_config(&rig.path).operator_policy().exclude,
        ["anthropic"]
    );
}

#[test]
fn retired_account_model_families_load_with_named_warnings() {
    let mut warnings = Vec::new();
    let config = super::parse_config_with_reporter(
        r#"{
        "accounts": {"model_families": [{"model":"claude-fable-5", "family":"opus"}]},
        "providers": {"anthropic": {"accounts": {"model_families": null}}}
    }"#,
        |warning| warnings.push(warning.to_string()),
    )
    .unwrap();
    assert_eq!(warnings.len(), 2);
    assert!(warnings[0].contains("`accounts.model_families`"));
    assert!(warnings[1].contains("`providers.anthropic.accounts.model_families`"));
    assert!(warnings
        .iter()
        .all(|warning| warning.contains("ignored") && warning.contains("delete this key")));
    let policy = super::allocator_policy_from(&config.accounts, None).unwrap();
    assert_eq!(
        policy.family_for_model("claude-fable-5"),
        prefrontal_routing_core::accounts::ModelFamily::Fable
    );
    assert!(super::parse_config_with_reporter(
        r#"{"accounts":{"model_families":false, "typo":true}}"#,
        |_| {}
    )
    .unwrap_err()
    .contains("typo"));
}

#[test]
fn generated_schema_is_current_and_closed() {
    let generated = routing_config_schema();
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("routing-config.schema.json");
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        generated,
        "regenerate using ck-striatum --print-config-schema"
    );
    let schema: serde_json::Value = serde_json::from_str(&generated).unwrap();
    assert_eq!(schema["additionalProperties"], false);
    for name in [
        "ConsumerPolicy",
        "ProviderPolicy",
        "ProjectPolicy",
        "RunnerPolicy",
        "PanelPolicies",
        "PanelPolicy",
        "SynthesizerPin",
        "CatalogSection",
        "AccountPoolFloor",
        "ProviderAccountsPolicy",
        "ModelFactOverride",
        "OverqualificationSection",
        "SubstrateGroups",
    ] {
        assert_eq!(
            schema["$defs"][name]["additionalProperties"], false,
            "{name}"
        );
    }
    assert!(schema["$defs"].get("ModelFamilyRules").is_none());
    assert!(schema["properties"].get("model_families").is_none());
    let properties = &schema["properties"];
    assert!(schema["$defs"].get("AccountSelectionFlags").is_none());
    assert!(schema["$defs"].get("AccountModelFamily").is_none());
    assert!(schema["$defs"]["ProviderAccountsPolicy"]["properties"]
        .get("model_families")
        .is_none());
    let retired = super::validate_document(r#"{"accounts":{"enabled":false}}"#).unwrap_err();
    assert!(retired.contains("accounts.enabled"));
    assert!(retired.contains(crate::config_convert::COMMAND));
    assert!(schema["$defs"]["AccountDuration"]["anyOf"]
        .as_array()
        .unwrap()
        .iter()
        .all(|variant| variant["type"] != "object"));
    assert!(schema["$defs"]["RemovedAccountFlags"]
        .get("properties")
        .is_none());
    assert_eq!(
        schema["$defs"]["ProviderAccountsPolicy"]["properties"]["metered_fallback"]["default"],
        false
    );
    assert!(properties
        .get("consecutiveTerminalFailureThreshold")
        .is_none());
    assert!(properties.get("providersWithoutPromptCache").is_none());
}

#[test]
fn substrate_mapping_loads_all_groups_and_matches_own_provider_not_score_alias() {
    use crate::substrate_policy::{resolve, Harness};

    let mut rig = Rig::new();
    rig.write(
        r#"{
        "model_routing": {
            "score_alias": {"alias": "vendor"},
            "substrates": {
                "all": {"default": "broca", "vendor": "opencode"},
                "mason": {"vendor/special": "pi"},
                "sidekick": {}, "athena": {}, "core": {},
                "lab": {"default": "pi"},
                "sidekick.explore": {},
                "sidekick.librarian": {"default": "opencode"},
                "sidekick.gather": {}, "sidekick.comments": {},
                "hire": {}, "persona_seat": {}
            }
        }
    }"#,
    );
    initialize_config(&rig.path).unwrap();
    let config = live_config(&rig.path);
    assert!(config.operator_policy().score_alias.is_empty());
    assert_eq!(
        resolve(config.substrate_groups(), "alias/model/high", None, None),
        Harness::Broca
    );
    assert_eq!(
        resolve(config.substrate_groups(), "vendor/model/high", None, None),
        Harness::Opencode
    );
    assert_eq!(
        resolve(
            config.substrate_groups(),
            "unknown/future-model",
            Some(crate::select_policy::Consumer::Lab),
            None,
        ),
        Harness::Pi
    );
}

#[test]
fn substrate_mapping_invalid_keys_and_values_refuse_first_load_and_reload() {
    for (substrates, diagnostic) in [
        (
            serde_json::json!({"unknown": {}}),
            "model_routing.substrates.unknown",
        ),
        (
            serde_json::json!({"lab": []}),
            "model_routing.substrates.lab",
        ),
        (
            serde_json::json!({"lab": null}),
            "model_routing.substrates.lab",
        ),
        (
            serde_json::json!({"all": {"vendor/model": "other"}}),
            "key \"vendor/model\"",
        ),
        (
            serde_json::json!({"all": {"vendor/model": 7}}),
            "key \"vendor/model\"",
        ),
        (
            serde_json::json!({"all": {"vendor/model": null}}),
            "key \"vendor/model\"",
        ),
        (
            serde_json::json!({"all": {"-vendor": "broca"}}),
            "key \"-vendor\"",
        ),
        (serde_json::json!({"all": {"": "broca"}}), "key \"\""),
        (
            serde_json::json!({"all": {"/model": "broca"}}),
            "key \"/model\"",
        ),
        (
            serde_json::json!({"all": {"vendor/": "broca"}}),
            "key \"vendor/\"",
        ),
        (
            serde_json::json!({"all": {" vendor": "broca"}}),
            "key \" vendor\"",
        ),
        (
            serde_json::json!({"all": {"vendor ": "broca"}}),
            "key \"vendor \"",
        ),
    ] {
        let document = serde_json::json!({"model_routing": {"substrates": substrates}}).to_string();
        first_load_refuses(&document, diagnostic);
        reload_refuses(&document, diagnostic);
    }
}

#[test]
fn retired_sidekick_lead_is_an_unknown_substrate_group() {
    let document = r#"{"model_routing":{"substrates":{"sidekick.lead":{"default":"broca"}}}}"#;
    first_load_refuses(document, "model_routing.substrates.sidekick.lead");
    reload_refuses(document, "model_routing.substrates.sidekick.lead");
    assert!(
        serde_json::from_str::<crate::selection_harness::Workload>(r#""sidekick.lead""#).is_err()
    );
}

#[test]
fn absent_substrate_mapping_and_withdrawn_groups_resolve_to_broca() {
    use crate::substrate_policy::{resolve, Harness};

    let mut rig = Rig::new();
    initialize_config(&rig.path).unwrap();
    let absent = live_config(&rig.path);
    rig.write(r#"{"model_routing":{"substrates":{"all":{"default":"pi"}}}}"#);
    let configured = settle_live_config(&rig.path);
    assert_eq!(
        resolve(configured.substrate_groups(), "vendor/model", None, None),
        Harness::Pi
    );
    rig.write(r#"{"model_routing":{}}"#);
    let withdrawn = settle_live_config(&rig.path);
    for config in [absent, withdrawn] {
        assert_eq!(
            resolve(config.substrate_groups(), "vendor/model", None, None),
            Harness::Broca
        );
    }
    assert_eq!(
        resolve(configured.substrate_groups(), "vendor/model", None, None),
        Harness::Pi
    );
}

#[test]
fn substrate_schema_closes_groups_harnesses_and_route_key_grammar() {
    let schema: serde_json::Value = serde_json::from_str(&routing_config_schema()).unwrap();
    let groups = &schema["$defs"]["SubstrateGroups"];
    assert_eq!(groups["type"], "object");
    assert_eq!(groups["additionalProperties"], false);
    let names = [
        "all",
        "mason",
        "sidekick",
        "athena",
        "lab",
        "core",
        "codemode",
        "flow",
        "sidekick.explore",
        "sidekick.librarian",
        "sidekick.gather",
        "sidekick.comments",
        "sidekick.planner",
        "sidekick.reader",
        "sidekick.map",
        "sidekick.reviewer",
        "hire",
        "persona_seat",
    ];
    assert_eq!(groups["properties"].as_object().unwrap().len(), names.len());
    for name in names {
        let group = &groups["properties"][name];
        assert_eq!(group["type"], "object", "{name}");
        assert_eq!(group["additionalProperties"]["$ref"], "#/$defs/Harness");
        assert_eq!(
            group["propertyNames"],
            serde_json::json!({"allOf": [
                {"pattern": "^[^/]+(/[\\s\\S]+)?$"},
                {"not": {"pattern": "^[-\\s]|\\s$"}}
            ]})
        );
    }
    assert_eq!(schema["$defs"]["Harness"]["type"], "string");
    assert_eq!(
        schema["$defs"]["Harness"]["enum"],
        serde_json::json!(["broca", "opencode", "pi"])
    );
}

#[test]
fn conversion_materializes_consumers_and_a_listing_replaces_them() {
    let defaults = LiveConfig::new(
        parse_config(&crate::config_convert::convert("{}").unwrap()).unwrap(),
        None,
    )
    .v1_consumers();
    let mut expected = vec![
        "athena",
        "codemode",
        "core",
        "flow",
        "lab",
        "mason",
        "mc_dream",
        "mc_dream_verify",
        "mc_dream_map",
        "mc_dream_curate",
        "mc_dream_classify",
        "mc_dream_review_user",
        "mc_dream_retrospective",
        "mc_dream_primers",
        "mc_dream_compress_cues",
        "mc_dream_docs",
        "mc_summary",
        "sidekick",
    ];
    expected.sort();
    assert_eq!(defaults, expected);

    let mut rig = Rig::new();
    rig.write(r#"{"model_routing":{}}"#);
    rig.write(r#"{"model_routing":{"consumers":["summary","codemode"]}}"#);
    initialize_config(&rig.path).unwrap();
    assert_eq!(
        live_config(&rig.path).v1_consumers(),
        ["codemode", "summary"]
    );
}

#[test]
fn conversion_materializes_runner_and_exclusions_follow_the_consumer_group() {
    let config = LiveConfig::new(
        parse_config(&crate::config_convert::convert("{}").unwrap()).unwrap(),
        None,
    );
    let runners = config.v1_runners();
    assert_eq!(runners.len(), 1);
    assert_eq!(runners[0].module, "broca");
    assert!(runners[0].routes.is_none());

    let mut rig = Rig::new();
    rig.write(r#"{"model_routing":{}}"#);
    rig.write(r#"{"model_routing":{"exclude":{"all":["a"],"mason":["b"]}}}"#);
    initialize_config(&rig.path).unwrap();
    let groups = live_config(&rig.path).exclude_groups().clone();
    assert_eq!(groups.named_group("mason"), ["b"]);
    // A consumer with no group of its own reads only `all`, which the
    // selection applies separately.
    for consumer in crate::config_policy::MAGIC_CONTEXT_CONSUMERS {
        assert!(groups.named_group(consumer).is_empty(), "{consumer}");
    }
    assert!(groups.named_group("all").is_empty());
}

#[test]
fn routing_v1_sections_refuse_malformed_entries_by_path() {
    for (document, message) in [
        (
            r#"{"model_routing":{"consumers":["two words"]}}"#,
            "consumer id",
        ),
        (r#"{"consumers":{"a":{"mode":"other"}}}"#, "unknown variant"),
        (
            r#"{"model_routing":{"runners":[{"module":"r"},{"module":"r"}]}}"#,
            "listed once",
        ),
        (
            r#"{"model_routing":{"runners":[{"module":"r","routes":["-p"]}]}}"#,
            "model_routing.runners.r",
        ),
        (
            r#"{"consumers":{"nobody":{"projects":{"p":{"pin":"bad"}}}}}"#,
            "must be provider/model[:variant]",
        ),
        (
            r#"{"model_routing":{"project_pins":{"p":{"mason":"model-only"}}}}"#,
            "must be provider/model[:variant]",
        ),
    ] {
        first_load_refuses(document, message);
    }
    let mut rig = Rig::new();
    rig.write(r#"{"model_routing":{"project_pins":{"p1":{"mc_dream":"a/b:high"}}}}"#);
    initialize_config(&rig.path).unwrap();
    assert_eq!(
        live_config(&rig.path).project_pin("p1", "mc_dream"),
        Some("a/b:high")
    );
    assert_eq!(live_config(&rig.path).project_pin("p2", "mc_dream"), None);
}
