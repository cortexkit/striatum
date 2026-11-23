use super::{install_catalog, install_panel_families, reserved, Harness};
use crate::managed_settings::test_daemon::ID;
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use subc_protocol::FrameType;

const MASON: &str = "prefrontal.mason";
const AGENT: &str = "thalamus-harness";
const MODEL: &str = "anthropic/model-a";
const HARNESSES: [&str; 3] = ["opencode2", "claude-code", "pi"];

fn fixture_with_runners(hosted: &[&str]) -> Arc<Harness> {
    let harness = Harness::new("project-runners");
    let mut catalog = fixture::catalog();
    catalog.accounts.clear();
    for model in &mut catalog.models {
        model.provider = "anthropic".into();
        model.priced_per_token = true;
    }
    catalog.runners[0].module = "broca".into();
    catalog.runners[0].harnesses = None;
    catalog.runners[0].default_harness = None;
    catalog.runners[1].module = AGENT.into();
    catalog.runners[1].harnesses = Some(hosted.iter().map(|name| (*name).into()).collect());
    // An explicit placement is not the runner's default-harness lookup.
    catalog.runners[1].default_harness = hosted.last().map(|name| (*name).into());
    for runner in &mut catalog.runners {
        runner.route_keys = catalog
            .models
            .iter()
            .map(|model| model.route_key())
            .collect();
    }
    install_catalog(&harness, &catalog);
    install_panel_families(&harness, &catalog);
    harness.edit_config(|config| {
        for consumer in [MASON, "prefrontal.sidekick", "prefrontal.sidekick.review"] {
            config["consumers"][consumer]["owner"] =
                json!(fixture::CONSUMER.trim_start_matches("reserved:"));
        }
        config["panels"]["audit"] = json!({"top_k":1,"by":"iq"});
    });
    harness
}

fn runner_layer(consumer: &str, runners: Value) -> Value {
    json!({"consumers":{consumer:{"runners":runners}}})
}

fn agent(harness: &str) -> Value {
    json!({"module":AGENT,"harness":harness})
}

fn request(consumer: &str) -> Value {
    json!({"consumer":consumer,"requirements":{"iq":0},"pin":MODEL})
}

fn assert_agent(answer: &Value, harness: &str) {
    assert_eq!(answer["runner"]["module"], AGENT, "{answer}");
    assert_eq!(answer["runner"]["harness"], harness, "{answer}");
    assert!(
        answer["explanation"].as_str().unwrap().contains(AGENT),
        "{answer}"
    );
}

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

#[tokio::test]
async fn mason_uses_project_harness_order_and_other_scopes_keep_broca() {
    for (index, expected) in HARNESSES.into_iter().enumerate() {
        let harness = fixture_with_runners(&HARNESSES[index..]);
        let daemon = harness.project_daemon().await;
        daemon.set(ID, runner_layer(MASON, json!(HARNESSES.map(agent))));
        daemon.no_overrides("pj-other");
        let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
        let params = request(MASON);
        let reply = caller.call("route.select", &params).await;
        if index == 0 {
            println!(
                "scratch project decision: {}",
                json!({
                    "scope_project_id": ID, "request": {"method":"route.select","params":params}, "answer":reply.1
                })
            );
        }
        assert_agent(&result(reply), expected);
        for other in [Some("pj-other"), None] {
            let caller = harness.bind(reserved(fixture::CONSUMER), other).await;
            let answer = result(caller.call("route.select", &params).await);
            assert_eq!(answer["runner"]["module"], "broca");
        }
        let unscoped = harness.bind(reserved(fixture::CONSUMER), None).await;
        let mut tiers_only = params.clone();
        tiers_only["settings"] = json!({"project_id":ID});
        let answer = result(unscoped.call("route.select", &tiers_only).await);
        assert_eq!(answer["project_settings"], "applied");
        assert_eq!(answer["runner"]["module"], "broca");
    }
}

#[tokio::test]
async fn more_specific_user_runner_list_beats_project_parent() {
    let harness = fixture_with_runners(&HARNESSES);
    harness.edit_config(|config| {
        config["consumers"]["prefrontal.sidekick.review"]["runners"] = json!([{"module":"broca"}]);
    });
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        runner_layer("prefrontal.sidekick", json!([agent("claude-code")])),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let answer = result(
        caller
            .call("route.select", &request("prefrontal.sidekick.review"))
            .await,
    );
    assert_eq!(answer["runner"]["module"], "broca", "{answer}");
}

#[tokio::test]
async fn request_runner_filter_cannot_add_an_enrolled_runner_left_out_of_the_project() {
    let harness = fixture_with_runners(&HARNESSES);
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        runner_layer(MASON, json!([agent("opencode2"), agent("claude-code")])),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let mut params = request(MASON);
    params["runners"] = json!(["broca"]);
    let reply = caller.call("route.select", &params).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["detail"]["reason"], "harness_unavailable");
    params["runners"] = json!([AGENT]);
    params["harness"] = json!("claude-code");
    let answer = result(caller.call("route.select", &params).await);
    assert_agent(&answer, "claude-code");
}

fn mason_declared_on_broca() -> Arc<Harness> {
    let harness = fixture_with_runners(&HARNESSES);
    super::install_head_snapshot(&harness);
    super::super::consumers::replace_catalog(&json!([{
        "module_id":"prefrontal", "nonce_verified":true,
        "role_declarations":{"routing/v1":{"consumers":[{
            "name":MASON, "iq":70, "eq":0, "placement":{"module":"broca"},
            "priority":"worker", "tools":"required", "workload":"code"
        }]}}
    }]));
    let full = [
        "plans",
        "fork_answer",
        "submit_tool",
        "final_text",
        "output_cap",
        "one_off",
    ];
    let declaration = super::super::runners::parse_declaration(&json!({"runner":{
        "pairs":[{"wire_api":"anthropic.messages", "auth_method":"apikey"}],
        "harnesses":["opencode2", "pi", "claude-code"],
        "default_harness":"opencode2", "starts_sessions":true,
        "harness_limits":{
            "opencode2":{"capabilities":full}, "claude-code":{"capabilities":full},
            "pi":{"capabilities":&full[..4]}
        }
    }}))
    .unwrap()
    .unwrap();
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .insert(AGENT.into(), declaration);
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":"broca","via_vault":true}, {"module":AGENT,"via_vault":true}
        ]);
        config["consumers"][MASON] = json!({"below_bar":"refuse"});
    });
    harness
}

fn mason_request() -> Value {
    json!({"consumer":MASON, "requirements":{"harness_capabilities":[]}})
}

#[tokio::test]
async fn declared_mason_placement_falls_back_when_project_list_is_disjoint() {
    let harness = mason_declared_on_broca();
    let project = "mason-disjoint";
    let daemon = harness.project_daemon().await;
    daemon.set(project, runner_layer(MASON, json!(HARNESSES.map(agent))));
    let caller = harness.bind(reserved("prefrontal"), Some(project)).await;
    crate::logging::take_operator_warnings();
    for _ in 0..2 {
        assert_eq!(
            result(caller.call("route.select", &mason_request()).await)["runner"]["module"],
            "broca"
        );
    }
    let warnings = crate::logging::take_operator_warnings();
    let warnings: Vec<_> = warnings
        .iter()
        .filter(|warning| warning.contains(MASON))
        .collect();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    for token in [project, "broca", AGENT, "opencode2", "claude-code", "pi"] {
        assert!(warnings[0].contains(token), "{}", warnings[0]);
    }
}

#[tokio::test]
async fn declared_mason_placement_keeps_partial_overlap_and_honors_caller_filter() {
    let harness = mason_declared_on_broca();
    let project = "mason-partial";
    let daemon = harness.project_daemon().await;
    daemon.set(
        project,
        runner_layer(MASON, json!([agent("opencode2"), {"module":"broca"}])),
    );
    let caller = harness.bind(reserved("prefrontal"), Some(project)).await;
    let mut params = mason_request();
    assert_eq!(
        result(caller.call("route.select", &params).await)["runner"]["module"],
        "broca"
    );
    params["runners"] = json!([AGENT]);
    let (frame, reply) = caller.call("route.select", &params).await;
    assert_eq!(frame, FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "no_route", "{reply}");
    assert_eq!(
        reply["detail"],
        json!({
            "reason":"harness_unavailable", "missing":{},
            "runners":[
                {"module":AGENT, "harness":"opencode2", "reason":"excluded_by_policy",
                 "filter":"declared_placement", "declared_module":"broca"},
                {"module":"broca", "reason":"excluded_by_policy", "filter":"request_runners"}
            ]
        })
    );
}

#[tokio::test]
async fn declared_mason_placement_never_widens_global_enrollment() {
    let harness = mason_declared_on_broca();
    let project = "mason-unenrolled";
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":AGENT,"via_vault":true}]);
    });
    let daemon = harness.project_daemon().await;
    daemon.set(project, runner_layer(MASON, json!(HARNESSES.map(agent))));
    let caller = harness.bind(reserved("prefrontal"), Some(project)).await;
    crate::logging::take_operator_warnings();
    let (frame, reply) = caller.call("route.select", &mason_request()).await;
    assert_eq!(frame, FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "no_route", "{reply}");
    assert_eq!(
        reply["detail"],
        json!({
            "reason":"harness_unavailable", "missing":{},
            "runners":HARNESSES.map(|harness| json!({
                "module":AGENT, "harness":harness, "reason":"excluded_by_policy",
                "filter":"global_enrollment", "declared_module":"broca"
            }))
        })
    );
    assert!(crate::logging::take_operator_warnings()
        .iter()
        .any(|line| line.contains(project)
            && line.contains("declared runner is not globally enrolled")));
}

#[tokio::test]
async fn project_runner_change_refreshes_only_the_notified_project() {
    let harness = fixture_with_runners(&HARNESSES);
    let daemon = harness.project_daemon().await;
    daemon.set(ID, runner_layer(MASON, json!([agent("opencode2")])));
    daemon.set("pj-other", runner_layer(MASON, json!([agent("pi")])));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let other = harness
        .bind(reserved(fixture::CONSUMER), Some("pj-other"))
        .await;
    let params = request(MASON);
    for _ in 0..2 {
        assert_agent(
            &result(caller.call("route.select", &params).await),
            "opencode2",
        );
        assert_agent(&result(other.call("route.select", &params).await), "pi");
    }
    assert_eq!((daemon.gets(ID), daemon.gets("pj-other")), (1, 1));
    daemon.set(ID, runner_layer(MASON, json!([agent("claude-code")])));
    daemon.changed(ID).await;
    assert_agent(
        &result(caller.call("route.select", &params).await),
        "claude-code",
    );
    assert_agent(&result(other.call("route.select", &params).await), "pi");
    assert_eq!((daemon.gets(ID), daemon.gets("pj-other")), (2, 1));
    daemon.no_overrides(ID);
    daemon.changed(ID).await;
    assert_eq!(
        result(caller.call("route.select", &params).await)["runner"]["module"],
        "broca"
    );
}

#[tokio::test]
async fn panel_seat_follows_project_runners_in_live_and_estimate_paths() {
    let harness = fixture_with_runners(&HARNESSES);
    let daemon = harness.project_daemon().await;
    daemon.set(ID, runner_layer(MASON, json!([agent("claude-code")])));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    for estimate in [false, true] {
        let answer = result(caller.call("route.select_panel", &json!({
            "class":"audit", "consumer":MASON, "available_routes":[MODEL], "estimate":estimate
        })).await);
        let seat = &answer["seats"][0]["answer"];
        assert_eq!(seat["runner"]["module"], AGENT, "{answer}");
        assert_eq!(seat["runner"]["harness"], "claude-code", "{answer}");
    }
}

#[derive(Clone, Default)]
struct WarningLog(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for WarningLog {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                write!(self.0, "{value:?} ").unwrap();
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn unenrolled_runner_is_skipped_and_the_rest_applies() {
    let harness = fixture_with_runners(&HARNESSES);
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        json!({"consumers":{
            MASON:{"runners":[{"module":"not-enrolled"}, agent("opencode2")]},
            "prefrontal.sidekick":{"runners":[agent("pi")]}
        }}),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let mut params = request(MASON);
    params["settings"] = json!({"project_id":ID});
    let answer = result(caller.call("route.select", &params).await);
    assert_agent(&answer, "opencode2");
    assert_eq!(answer["project_settings"], "applied");
    assert_agent(
        &result(
            caller
                .call("route.select", &request("prefrontal.sidekick"))
                .await,
        ),
        "pi",
    );
}

#[tokio::test]
async fn daemon_project_ignores_user_only_fields_and_warns_once_per_unknown_name() {
    let harness = fixture_with_runners(&HARNESSES);
    harness.edit_config(|config| {
        config["consumers"]
            .as_object_mut()
            .unwrap()
            .remove("sidekick");
    });
    let daemon = harness.project_daemon().await;
    daemon.set(ID, json!({
        "runners": [{"module":"not-enrolled"}],
        "consumers": {
            MASON: {"pin":{"model":"anthropic/model-b"}, "runners":[{"module":AGENT,"harness":"opencode2","via_vault":true}]},
            "sidekick":{"service_tier":"fast"},
            "prefrontal.mason.child":{"service_tier":"fast"},
            "prefrontal.sidekick.review":{},
        }
    }));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let mut params = request(MASON);
    params.as_object_mut().unwrap().remove("pin");
    let warnings = WarningLog::default();
    let _subscriber = tracing::subscriber::set_default(warnings.clone());
    for _ in 0..2 {
        let frame = caller.request_frame("route.select", &params);
        assert_agent(&result(harness.finish_dispatched(frame).await), "opencode2");
    }
    let log = warnings.0.lock().unwrap().join("\n");
    assert_eq!(log.matches("ignored user-only fields").count(), 1, "{log}");
    assert_eq!(
        log.matches("unknown consumer in project routing settings")
            .count(),
        1,
        "{log}"
    );
    assert!(log.contains("\"sidekick\""), "{log}");
}

#[tokio::test]
async fn project_routes_and_harnesses_cannot_widen_a_global_entry() {
    let harness = fixture_with_runners(&HARNESSES);
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":"broca"},
            {"module":AGENT,"harness":"claude-code","routes":["anthropic/model-b"]},
            {"module":AGENT,"harness":"pi","routes":[MODEL]}
        ]);
    });
    let daemon = harness.project_daemon().await;
    daemon.set(
        ID,
        runner_layer(
            MASON,
            json!([
                agent("opencode2"),
                {"module":AGENT,"harness":"claude-code","routes":[MODEL]},
                {"module":AGENT,"harness":"pi","routes":["anthropic"]}
            ]),
        ),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    assert_agent(
        &result(caller.call("route.select", &request(MASON)).await),
        "pi",
    );
    let mut excluded = request(MASON);
    excluded["pin"] = json!("anthropic/model-b");
    assert_eq!(
        caller.call("route.select", &excluded).await.0,
        FrameType::Error
    );
    // The bare entry inherits claude-code too, but its route restrictions
    // cannot hide a later entry for the same resolved pair.
    daemon.set(
        ID,
        runner_layer(
            MASON,
            json!([
                {"module":AGENT,"routes":[MODEL]}, agent("claude-code")
            ]),
        ),
    );
    daemon.changed(ID).await;
    assert_agent(
        &result(caller.call("route.select", &excluded).await),
        "claude-code",
    );
}

#[tokio::test]
async fn project_may_choose_an_auto_enrolled_first_party_runner() {
    let harness = fixture_with_runners(&HARNESSES);
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca"}]));
    let config = harness.operator_config();
    super::super::runners::DescribeCache::default().catalog(
        &json!([{"module_id":AGENT,"first_party":true,"incarnation":"daemon-a","spawn":7,
        "role_declarations":{"routing/v1":{"runner":{
            "pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}],
            "harnesses":HARNESSES
        }}}}]),
        &config.v1_runners(),
        &mut harness.credentials().lock().unwrap(),
        crate::now_ms() as i64,
    );
    let daemon = harness.project_daemon().await;
    daemon.set(ID, runner_layer(MASON, json!([agent("pi")])));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    assert_agent(
        &result(caller.call("route.select", &request(MASON)).await),
        "pi",
    );
}

#[tokio::test]
async fn slow_or_refused_project_get_uses_user_settings_and_retries_after_change() {
    let harness = fixture_with_runners(&HARNESSES);
    let daemon = harness.project_daemon().await;
    daemon.delay("slow");
    daemon.refuse("refused");
    let warnings = WarningLog::default();
    let _subscriber = tracing::subscriber::set_default(warnings.clone());
    for project in ["slow", "refused"] {
        let caller = harness
            .bind(reserved(fixture::CONSUMER), Some(project))
            .await;
        let mut params = request(MASON);
        params["settings"] = json!({"project_id":project});
        for _ in 0..2 {
            let answer = result(
                harness
                    .finish_dispatched(caller.request_frame("route.select", &params))
                    .await,
            );
            assert_eq!(answer["runner"]["module"], "broca", "{answer}");
            assert_eq!(answer["project_settings"], "unresolved");
        }
        assert_eq!(daemon.gets(project), 1);
        daemon.set(project, runner_layer(MASON, json!([agent("pi")])));
        daemon.changed(project).await;
        assert_agent(&result(caller.call("route.select", &params).await), "pi");
        assert_eq!(daemon.gets(project), 2);
    }
    let log = warnings.0.lock().unwrap().join("\n");
    assert_eq!(
        log.matches("project settings unresolved").count(),
        2,
        "{log}"
    );
    assert!(log.contains("layer_forbidden"), "{log}");
}

#[tokio::test]
async fn unknown_project_is_a_no_layer_result_and_can_be_retried() {
    let harness = fixture_with_runners(&HARNESSES);
    let daemon = harness.project_daemon().await;
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let mut params = request(MASON);
    params["settings"] = json!({"project_id":ID});
    let warnings = WarningLog::default();
    let _subscriber = tracing::subscriber::set_default(warnings.clone());
    for _ in 0..2 {
        let answer = result(
            harness
                .finish_dispatched(caller.request_frame("route.select", &params))
                .await,
        );
        assert_eq!(answer["runner"]["module"], "broca", "{answer}");
        assert_eq!(answer["project_settings"], "none", "{answer}");
    }
    assert_eq!(daemon.gets(ID), 1);
    let log = warnings.0.lock().unwrap().join("\n");
    assert_eq!(
        log.matches("unknown_project: using user routing settings")
            .count(),
        1,
        "{log}"
    );
    assert!(!log.contains("project settings unresolved"), "{log}");
    daemon.set(ID, runner_layer(MASON, json!([agent("pi")])));
    daemon.changed(ID).await;
    assert_agent(&result(caller.call("route.select", &params).await), "pi");
}

#[tokio::test]
async fn empty_project_child_preserves_user_runner_pin_and_exclusions() {
    let harness = fixture_with_runners(&HARNESSES);
    let parent = "prefrontal.sidekick";
    let child = "prefrontal.sidekick.explore";
    harness.edit_config(|config| {
        config["consumers"][parent]["runners"] = json!([agent("pi")]);
        config["consumers"][parent]["pin"] = json!({"model":MODEL});
        config["consumers"][parent]["service_tier"] = json!("fast");
        config["models"]["anthropic/model-b"]["not_for"] = json!([parent]);
    });
    let daemon = harness.project_daemon().await;
    daemon.set(ID, json!({"consumers":{child:{}}}));
    let route = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let mut params = request(child);
    params.as_object_mut().unwrap().remove("pin");
    params["settings"] = json!({"project_id":ID});
    let answer = result(route.call("route.select", &params).await);
    assert_agent(&answer, "pi");
    assert_eq!(
        answer["route"],
        json!({"provider":"anthropic", "model":"model-a"})
    );
    assert!(
        answer["explanation"]
            .as_str()
            .unwrap()
            .contains("consumer's operator pin"),
        "{answer}"
    );
    assert_eq!(answer["project_settings"], "applied");
    let scoped = crate::managed_settings::project_view(&harness.config_path_for_test(), ID)
        .await
        .unwrap();
    let layers = crate::tier_settings::Layers::new(scoped.consumers.clone(), None);
    assert_eq!(
        layers.resolve(
            &harness.operator_config(),
            child,
            "anthropic",
            "model-a",
            None
        ),
        Some(crate::tier_settings::Tier::Fast.value())
    );
    params["pin"] = json!("anthropic/model-b");
    let refused = route.call("route.select", &params).await;
    assert_eq!(refused.1["detail"]["reason"], "excluded", "{refused:?}");
}
