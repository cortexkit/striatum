use super::{install_catalog, install_panel_families, reserved, Harness};
use crate::project_settings::tests::{census, write, Fake, ID};
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use std::path::Path;
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

fn project_file(project: &Path, consumer: &str, runners: Value) {
    write(
        project,
        &json!({"consumers":{consumer:{"runners":runners}}}).to_string(),
    );
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
        let scratch = cortexkit_test_support::ScratchDir::new("project-runner-order");
        let project = scratch.join("project");
        project_file(&project, MASON, json!(HARNESSES.map(agent)));
        let mut registry = census(&[project], None);
        let mut other = registry["result"]["projects"][0].clone();
        other["projectId"] = json!("pj-other");
        other["roots"] = json!([]);
        other["rootRecords"] = json!([]);
        registry["result"]["projects"]
            .as_array_mut()
            .unwrap()
            .push(other);
        harness.settings_source(Fake::new(registry));
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
    }
}

#[tokio::test]
async fn project_parent_runner_list_beats_more_specific_user_setting() {
    let harness = fixture_with_runners(&HARNESSES);
    harness.edit_config(|config| {
        config["consumers"]["prefrontal.sidekick.review"]["runners"] = json!([{"module":"broca"}]);
    });
    let scratch = cortexkit_test_support::ScratchDir::new("project-runner-sidekick");
    let project = scratch.join("project");
    let workspace = scratch.join("workspace");
    project_file(
        &workspace,
        "prefrontal.sidekick.review",
        json!([agent("pi")]),
    );
    project_file(
        &project,
        "prefrontal.sidekick",
        json!([agent("claude-code")]),
    );
    harness.settings_source(Fake::new(census(&[project], Some(&workspace))));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let answer = result(
        caller
            .call("route.select", &request("prefrontal.sidekick.review"))
            .await,
    );
    assert_agent(&answer, "claude-code");
}

#[tokio::test]
async fn request_runner_filter_cannot_add_an_enrolled_runner_left_out_of_the_project() {
    let harness = fixture_with_runners(&HARNESSES);
    let scratch = cortexkit_test_support::ScratchDir::new("project-runner-filter");
    let project = scratch.join("project");
    project_file(
        &project,
        MASON,
        json!([agent("opencode2"), agent("claude-code")]),
    );
    harness.settings_source(Fake::new(census(&[project], None)));
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

#[tokio::test]
async fn project_runner_edit_and_removal_apply_on_the_next_request() {
    let harness = fixture_with_runners(&HARNESSES);
    let scratch = cortexkit_test_support::ScratchDir::new("project-runner-reload");
    let project = scratch.join("project");
    let workspace = scratch.join("workspace");
    project_file(&workspace, MASON, json!([agent("pi")]));
    harness.settings_source(Fake::new(census(&[project.clone()], Some(&workspace))));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let params = request(MASON);
    project_file(&project, MASON, json!([agent("opencode2")]));
    assert_agent(
        &result(caller.call("route.select", &params).await),
        "opencode2",
    );
    project_file(&project, MASON, json!([agent("claude-code")]));
    assert_agent(
        &result(caller.call("route.select", &params).await),
        "claude-code",
    );
    std::fs::remove_file(project.join(".cortexkit/striatum.jsonc")).unwrap();
    assert_agent(&result(caller.call("route.select", &params).await), "pi");
    std::fs::remove_file(workspace.join(".cortexkit/striatum.jsonc")).unwrap();
    assert_eq!(
        result(caller.call("route.select", &params).await)["runner"]["module"],
        "broca"
    );
}

#[tokio::test]
async fn panel_seat_follows_project_runners_in_live_and_estimate_paths() {
    let harness = fixture_with_runners(&HARNESSES);
    let scratch = cortexkit_test_support::ScratchDir::new("project-runner-panel");
    let project = scratch.join("project");
    project_file(&project, MASON, json!([agent("claude-code")]));
    harness.settings_source(Fake::new(census(&[project], None)));
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

async fn invalid_project_runner_entry(entry: Value, reason: &str) {
    let harness = fixture_with_runners(&HARNESSES);
    let scratch = cortexkit_test_support::ScratchDir::new("invalid-project-runner");
    let project = scratch.join("project");
    let workspace = scratch.join("workspace");
    project_file(&workspace, MASON, json!([agent("pi")]));
    write(
        &project,
        &json!({"consumers":{
            MASON:{"runners":[agent("opencode2")]},
            "unrelated": {"runners":[entry]}
        }})
        .to_string(),
    );
    harness.settings_source(Fake::new(census(&[project], Some(&workspace))));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    let mut params = request(MASON);
    params["settings"] = json!({"project_id":ID});
    let warnings = WarningLog::default();
    let _subscriber = tracing::subscriber::set_default(warnings.clone());
    for _ in 0..2 {
        // Dispatch on this thread so the test subscriber sees handler warnings.
        let frame = caller.request_frame("route.select", &params);
        let answer = result(harness.finish_dispatched(frame).await);
        assert_eq!(answer["runner"]["module"], "broca", "{answer}");
        assert_eq!(answer["project_settings"], "unresolved", "{answer}");
    }
    let log = warnings.0.lock().unwrap().join("\n");
    assert_eq!(
        log.matches("project settings unresolved").count(),
        1,
        "{log}"
    );
    assert!(log.contains(reason), "expected {reason:?} in {log}");
}

#[tokio::test]
async fn unenrolled_runner_rejects_the_whole_project_once() {
    invalid_project_runner_entry(json!({"module":"not-enrolled"}), "is not enrolled").await;
}

#[tokio::test]
async fn project_runner_rejects_via_vault_as_an_unknown_field() {
    invalid_project_runner_entry(
        json!({"module":AGENT,"via_vault":true}),
        "unknown field `via_vault`",
    )
    .await;
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
    let scratch = cortexkit_test_support::ScratchDir::new("narrow-project-runner");
    let project = scratch.join("project");
    project_file(
        &project,
        MASON,
        json!([
            agent("opencode2"),
            {"module":AGENT,"harness":"claude-code","routes":[MODEL]},
            {"module":AGENT,"harness":"pi","routes":["anthropic"]}
        ]),
    );
    harness.settings_source(Fake::new(census(&[project.clone()], None)));
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
    project_file(
        &project,
        MASON,
        json!([
            {"module":AGENT,"routes":[MODEL]},
            agent("claude-code")
        ]),
    );
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
    let scratch = cortexkit_test_support::ScratchDir::new("auto-project-runner");
    let project = scratch.join("project");
    project_file(&project, MASON, json!([agent("pi")]));
    harness.settings_source(Fake::new(census(&[project], None)));
    let caller = harness.bind(reserved(fixture::CONSUMER), Some(ID)).await;
    assert_agent(
        &result(caller.call("route.select", &request(MASON)).await),
        "pi",
    );
}
