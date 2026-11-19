use super::*;
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
            path: dir.join("striatum.jsonc"),
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
        "models":{"anthropic/claude-opus-5-5":{"not_for":["prefrontal-core.athena","prefrontal-core.lab"]}}}"#,
    r#"consumers.synthesizer.pin pins anthropic/claude-opus-5-5 but this config refuses route anthropic/claude-opus-5-5 for consumer "prefrontal-core.athena": models["anthropic/claude-opus-5-5"].not_for lists "prefrontal-core.athena""#
);
refusal_case!(
    lab_planner_pin_refused_by_provider_not_for,
    r#"{"consumers":{"lab_planner":{"pin":{"model":"anthropic/claude-opus-5-5"}}},
        "providers":{"anthropic":{"not_for":["prefrontal-core.lab"]}}}"#,
    r#"consumers.lab_planner.pin pins anthropic/claude-opus-5-5 but this config refuses route anthropic/claude-opus-5-5 for consumer "prefrontal-core.lab": providers.anthropic.not_for lists "prefrontal-core.lab""#
);
refusal_case!(
    spec_designer_pin_refused_by_provider_models_list,
    r#"{"consumers":{"spec_designer":{"pin":{"model":"anthropic/claude-opus-5-5"}}},
        "providers":{"anthropic":{"models":["claude-sonnet-5"]}}}"#,
    r#"consumers.spec_designer.pin pins anthropic/claude-opus-5-5 but this config refuses route anthropic/claude-opus-5-5 for consumer "prefrontal-core.athena": providers.anthropic.models ["claude-sonnet-5"] does not list the model"#
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

#[test]
fn retired_variant_keys_are_refused_at_startup_and_reload_with_exact_paths() {
    let cases = [
        (r#"{"variants":{"openai/gpt-6":"high"}}"#, "variants"),
        (
            r#"{"model_routing":{"variants":null}}"#,
            "model_routing.variants",
        ),
        (
            r#"{"consumers":{"prefrontal-core.athena":{"variants":{}}}}"#,
            r#"consumers."prefrontal-core.athena".variants"#,
        ),
    ];
    for (document, path) in cases {
        let expected = format!("config_key_removed: {path}; delete this key");
        let mut fresh = Rig::new();
        fresh.write(document);
        let error = initialize_config(&fresh.path).unwrap_err();
        assert!(error.contains(&expected), "{error}");
        let error = crate::config_convert::convert_value(serde_json::from_str(document).unwrap())
            .unwrap_err();
        assert!(error.contains(&expected), "{error}");

        let mut rig = Rig::new();
        rig.write(r#"{"consumers":{"prefrontal-core.athena":{"workload":"reason"}}}"#);
        initialize_config(&rig.path).unwrap();
        let held = live_config(&rig.path);
        rig.write(document);
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
        assert!(reports[0].contains(&expected), "{reports:?}");
    }
}
#[test]
fn check_config_counts_are_anonymous_and_available_without_variants_or_store() {
    let mut rig = Rig::new();
    rig.write(r#"{
        "providers":{"private-provider":{"subscription":true,"pin_only":false}},
        "models":{"private-provider/private-model":{"subscription":false,"pin_only":true}},
        "runners":[{"module":"private-runner","routes":["private-provider","private-provider/private-model"]}]
    }"#);
    let lines = |report: &str| {
        report
            .lines()
            .filter(|line| line.contains('='))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    let expected_rules = [
        "subscription_rules=2",
        "exclusion_rules=2",
        "runner_route_rules=2",
    ];
    let missing = rig.dir.join("missing.db");
    let corrupt = rig.dir.join("corrupt.db");
    std::fs::write(&corrupt, b"not a sqlite database").unwrap();
    for path in [None, Some(missing.as_path()), Some(corrupt.as_path())] {
        let report = check_config_offline(&rig.path, path).unwrap();
        let counts = lines(&report);
        assert_eq!(&counts[..3], expected_rules);
        assert_eq!(counts.len(), 5);
        for line in &counts[3..] {
            assert!(line.contains("=unavailable ("), "{line}");
            assert!(
                !line.ends_with("()"),
                "unavailability needs a reason: {line}"
            );
        }
        assert!(!counts.join("\n").contains("private"));
    }
    let store_path = rig.dir.join("counts.db");
    let connection = rusqlite::Connection::open(&store_path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE model_facts (name TEXT);
        INSERT INTO model_facts VALUES ('private-model'), ('private-model-variant');
        CREATE TABLE account_vault_roster (singleton INTEGER, roster_json TEXT);",
        )
        .unwrap();
    connection.execute("INSERT INTO account_vault_roster VALUES (1, ?1)", [r#"{"identities":[{"credential_ids":["private-credential-a","private-credential-b"],"active_credential_ids":["private-credential-a"]},{"credential_ids":["private-credential-c"]}]}"#]).unwrap();
    drop(connection);
    let settings_before = std::fs::read(&rig.path).unwrap();
    let store_before = std::fs::read(&store_path).unwrap();
    let counts = lines(&check_config_offline(&rig.path, Some(&store_path)).unwrap());
    assert_eq!(&counts[..3], expected_rules);
    assert_eq!(
        &counts[3..],
        ["catalog_facts=2", "vault_roster_credentials=3"]
    );
    assert!(!counts.join("\n").contains("private"));
    assert_eq!(std::fs::read(&rig.path).unwrap(), settings_before);
    assert_eq!(std::fs::read(&store_path).unwrap(), store_before);
    let mut invalid = Rig::new();
    invalid.write(r#"{"unknown-setting":true}"#);
    assert!(check_config_offline(&invalid.path, Some(&store_path)).is_err());
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
        // The explicit converter keeps old consumer keys literally: a rule
        // for mason does not become a rule for prefrontal-core.mason.
        for (consumer, route) in [
            ("mason", "kimi-code-plan-global/k3"),
            ("mason", "openai/gpt-6-astra"),
            ("sidekick", "xai/other"),
            ("athena", "local/model"),
            ("lab", "google/gemini-4"),
            ("core", "deepseek/other"),
        ] {
            assert!(
                !active.route_allowed(consumer, route, true),
                "{consumer}: {route}"
            );
        }
        assert!(active.route_allowed("sidekick", "xai/grok-5", true));
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
    rig.write(r#"{"providers":{"anthropic":{"not_for":["prefrontal-core.mason"]}}}"#);
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
fn vanished_file_keeps_last_good_pin_only_policy_and_reports_once() {
    let mut rig = Rig::new();
    rig.write(r#"{"providers":{"p":{"pin_only":true}}}"#);
    let now = Instant::now();
    let held = live_config_at(&rig.path, now).unwrap();
    assert!(!held.route_allowed("mason", "p/model", false));
    assert!(held.route_allowed("mason", "p/model", true));
    std::fs::remove_file(&rig.path).unwrap();

    let mut reports = Vec::new();
    for elapsed in [
        Duration::ZERO,
        RELOAD_DEBOUNCE / 2,
        RELOAD_DEBOUNCE,
        RELOAD_DEBOUNCE * 2,
    ] {
        let next = live_config_at_with_reporter(&rig.path, now + elapsed, |error| {
            reports.push(error.to_owned());
        })
        .unwrap();
        assert!(
            Arc::ptr_eq(&held, &next),
            "a missing file must keep the last good snapshot"
        );
        assert!(!next.route_allowed("mason", "p/model", false));
        assert!(next.route_allowed("mason", "p/model", true));
    }
    assert_eq!(reports, ["missing"]);

    rig.write(r#"{"providers":{"p":{"not_for":["mason"]}}}"#);
    live_config_at(&rig.path, now + RELOAD_DEBOUNCE * 3).unwrap();
    let repaired = live_config_at(&rig.path, now + RELOAD_DEBOUNCE * 4).unwrap();
    assert!(!Arc::ptr_eq(&held, &repaired));
    assert!(!repaired.route_allowed("mason", "p/model", true));
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
        striatum_core::accounts::ModelFamily::Fable
    );
    assert!(super::parse_config_with_reporter(
        r#"{"accounts":{"model_families":false, "typo":true}}"#,
        |_| {}
    )
    .unwrap_err()
    .contains("typo"));
}

#[test]
fn pin_origin_enrollment_defaults_to_the_full_core_principal() {
    let defaults = LiveConfig::default();
    assert!(defaults.honours_pin_origin("reserved:prefrontal-core"));
    for principal in [
        "prefrontal-core",
        "reserved:prefrontal-core-other",
        "reserved:prefrontal-core.child",
    ] {
        assert!(!defaults.honours_pin_origin(principal));
    }
    let parsed = LiveConfig::new(parse_config("{}").unwrap(), None);
    assert!(parsed.honours_pin_origin("reserved:prefrontal-core"));
    let disabled = LiveConfig::new(parse_config(r#"{"pin_origin_callers":[]}"#).unwrap(), None);
    assert!(!disabled.honours_pin_origin("reserved:prefrontal-core"));
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
    ] {
        assert_eq!(
            schema["$defs"][name]["additionalProperties"], false,
            "{name}"
        );
    }
    assert!(schema["$defs"].get("ModelFamilyRules").is_none());
    assert!(schema["$defs"].get("SubstrateGroups").is_none());
    assert!(schema["properties"].get("substrates").is_none());
    assert!(schema["properties"].get("model_families").is_none());
    let properties = &schema["properties"];
    assert_eq!(properties["pin_origin_callers"]["type"], "array");
    assert_eq!(properties["pin_origin_callers"]["items"]["type"], "string");
    assert_eq!(
        properties["pin_origin_callers"]["default"],
        serde_json::json!(["reserved:prefrontal-core", "reserved:prefrontal"])
    );
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
    assert!(!live_config(&rig.path).route_allowed("mason", "b/model", true));
    // A consumer with no group of its own reads only `all`, which the
    // selection applies separately.
    for consumer in [
        "magic-context.summary",
        "magic-context.dream",
        "magic-context.dream.verify",
        "magic-context.dream.verify-broad",
    ] {
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
