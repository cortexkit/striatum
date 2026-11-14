//! Consumer admission through the same catalog and route-bind seams as production.

use std::sync::Arc;

use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

use super::conformance_tests::{install_catalog, reserved, Harness, Route};
use super::{consumers, Caller};

fn harness() -> Arc<Harness> {
    let harness = Harness::new("consumer-admission");
    harness.edit_config(|config| {
        config["consumers"] = json!({
            "codemode": { "owner": "prefrontal-core", "pay_per_token": true },
            "mc_dream_curate": { "owner": "magic-context" }
        });
    });
    install_catalog(&harness, &fixture::catalog());
    harness
}

fn entry(module: &str, verified: bool, names: &[&str]) -> Value {
    json!({
        "module_id": module, "nonce_verified": verified,
        "role_declarations": { "routing/v1": { "consumers": names.iter().map(|name| json!({"name": name})).collect::<Vec<_>>() } }
    })
}

fn select(consumer: &str) -> Value {
    json!({"consumer": consumer, "requirements": {"iq": 30}})
}

fn selected_input(h: &Harness, reply: &(FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let id = reply.1["result"]["decision_id"].as_str().unwrap();
    let row = h.store().get_decision_by_id(id).unwrap().unwrap();
    serde_json::from_str(&row.requirements_json).unwrap()
}

fn placement_catalog(h: &Harness, declarations: Value) {
    consumers::replace_catalog(&json!([{
        "module_id":"basal", "nonce_verified":true,
        "role_declarations":{"routing/v1":{"consumers":declarations}}
    }]));
    let models: serde_json::Map<_, _> = h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .map(|mut fact| {
            fact.supports_tool_use = Some(true);
            h.store()
                .upsert_fact(&fact, striatum_core::FactSource::Override)
                .unwrap();
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                json!({"existence":"present","capability.tool_call":true}),
            )
        })
        .collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({"catalog_version":41,"resolved_at_ms":123,"models":models}))
            .unwrap(),
    )
    .unwrap();
    let shared = h.credentials();
    let mut credentials = shared.lock().unwrap();
    credentials.catalog_snapshot = Some(Arc::new(snapshot));
    let runner = credentials.runner_declarations[fixture::RUNNER_ONE].clone();
    credentials
        .runner_declarations
        .insert("broca".into(), runner);
}

fn no_route(reply: &(FrameType, Value), reason: &str) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    assert_eq!(reply.1["detail"]["reason"], reason, "{}", reply.1);
    assert!(
        reply.1["detail"].get("reset_at_ms").is_none(),
        "{}",
        reply.1
    );
}

fn on_runner(reply: &(FrameType, Value), module: &str) {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["runner"]["module"], module, "{}", reply.1);
}

#[tokio::test]
async fn production_consumer_declarations_serve_defaults_admission_placement_and_kind() {
    let h = harness();
    h.edit_config(|config| {
        config["runners"] = json!([{"module":fixture::RUNNER_ONE}, {"module":"broca"}]);
    });
    placement_catalog(
        &h,
        json!([
            {"name":"basal.task", "iq":40, "ladder":["example-provider/model-a"],
             "selectors":["selector"], "placement":{"module":"broca"}, "kind":"llm"},
            {"name":"basal.media", "iq":40, "kind":"transcribe"},
            {"name":"foreign.task", "iq":40}
        ]),
    );
    let owner = h.bind(reserved("basal"), None).await;
    let selector = h.bind(reserved("selector"), None).await;
    let request = json!({"consumer":"basal.task.child", "requirements":{}});
    for route in [&owner, &selector] {
        let reply = route.call("route.select", &request).await;
        assert_eq!(selected_input(&h, &reply)["iq"], 40.0);
        assert_eq!(reply.1["result"]["route"]["model"], "model-a");
        on_runner(&reply, "broca");
    }
    let stranger = h.bind(reserved("stranger"), None).await;
    admission_refused(
        stranger.call("route.select", &request).await,
        "consumer_not_owned",
        "basal.task.child",
    );
    admission_refused(
        owner.call("route.select", &select("foreign.task")).await,
        "unknown_consumer",
        "foreign.task",
    );
    // A router withholding media support must retain its compatibility refusal.
    super::describe::without_capability(
        "select_media",
        super::describe::without_capability("select_kind", async {
            let media = owner
                .call(
                    "route.select",
                    &json!({
                        "consumer":"basal.media", "requirements":{}
                    }),
                )
                .await;
            no_route(&media, "no_available_model");
            assert!(media.1["message"]
                .as_str()
                .unwrap()
                .contains("route.select_engine"));
            let describe = owner.call("role.describe", &json!({})).await;
            assert_eq!(describe.0, FrameType::Response, "{describe:?}");
            let groups = describe.1["result"]["capabilities"].as_array().unwrap();
            assert_eq!(
                groups
                    .iter()
                    .filter(|group| **group == json!("consumer_declarations"))
                    .count(),
                1
            );
            assert!(!groups.contains(&json!("select_kind")));
        }),
    )
    .await;
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        assert_eq!(
            super::describe::capabilities().as_ref(),
            super::describe::CAPABILITIES
        );
    })
    .await;
}

#[tokio::test]
async fn declared_placement_module_narrows_settings_and_caller_filters() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        placement_catalog(
            &h,
            json!([{"name":"basal.task", "placement":{"module":"broca"}}]),
        );
        h.edit_config(|config| {
            config["runners"] = json!([
                {"module":fixture::RUNNER_ONE}, {"module":"broca"}
            ])
        });
        let owner = h.bind(reserved("basal"), None).await;
        on_runner(
            &owner.call("route.select", &select("basal.task")).await,
            "broca",
        );
        let mut request = select("basal.task");
        request["runners"] = json!([fixture::RUNNER_ONE]);
        no_route(
            &owner.call("route.select", &request).await,
            "harness_unavailable",
        );
        h.edit_config(|config| {
            config["consumers"]["basal.task"]["runner"] = json!(fixture::RUNNER_ONE)
        });
        no_route(
            &owner.call("route.select", &select("basal.task")).await,
            "harness_unavailable",
        );
        h.edit_config(|config| {
            config["consumers"]
                .as_object_mut()
                .unwrap()
                .remove("basal.task");
            config["runners"] = json!([{"module":fixture::RUNNER_ONE}]);
        });
        no_route(
            &owner.call("route.select", &select("basal.task")).await,
            "harness_unavailable",
        );
        // Empty settings fail before declared placement, preserving the existing reason.
        h.edit_config(|config| config["runners"] = json!([]));
        no_route(
            &owner.call("route.select", &select("basal.task")).await,
            "no_available_model",
        );
    })
    .await;
}

#[tokio::test]
async fn declared_placement_inherits_only_same_owner_and_nearest_member() {
    // Placement inheritance is independent of the media kind dispatcher. Keep
    // the older kind refusal here and exercise advertised search separately.
    super::describe::without_capability("select_media", super::describe::without_capability("select_kind", async {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        placement_catalog(&h, json!([
            {"name":"basal.task", "placement":{"module":"broca"}, "kind":"llm"},
            {"name":"basal.task.child"},
            {"name":"basal.task.child.own", "placement":{"module":fixture::RUNNER_ONE}, "kind":"search"}
        ]));
        h.edit_config(|config| {
            config["runners"] = json!([{"module":fixture::RUNNER_ONE}, {"module":"broca"}]);
            config["consumers"]["basal.task.settings"] = json!({"owner":"basal"});
            config["consumers"]["basal.task.ownerless"] = json!({});
            config["consumers"]["basal.task.foreign"] = json!({"owner":"other"});
            config["consumers"]["basal.task_underscore"] = json!({});
        });
        let owner = h.bind(reserved("basal"), None).await;
        for name in ["basal.task.child", "basal.task.settings", "basal.task.ownerless", "basal.task.unlisted"] {
            on_runner(&owner.call("route.select", &select(name)).await, "broca");
        }
        let registry = consumers::effective(&h.operator_config());
        let inherited = registry.defaults_for("basal.task.child.own.unlisted");
        assert_eq!(serde_json::to_value(inherited.placement).unwrap(), json!({"module":fixture::RUNNER_ONE}));
        assert_eq!(inherited.kind.unwrap().as_str(), "search");
        no_route(&owner.call("route.select", &select("basal.task.child.own.unlisted")).await, "no_available_model");
        let other = h.bind(reserved("other"), None).await;
        for name in ["basal.task.foreign", "basal.task_underscore"] {
            assert!(registry.defaults_for(name).placement.is_none());
            assert!(registry.defaults_for(name).kind.is_none());
            on_runner(&other.call("route.select", &select(name)).await, fixture::RUNNER_ONE);
        }
    }).await;
    })).await;
}

#[tokio::test]
async fn production_declared_search_kind_is_invalid_even_when_inherited() {
    super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = harness();
        placement_catalog(
            &h,
            json!([
                {"name":"basal.task", "kind":"llm"},
                {"name":"basal.task.child", "kind":"search"}
            ]),
        );
        let route = h.bind(reserved("basal"), None).await;
        let (frame, reply) = route
            .call("route.select", &select("basal.task.child.unlisted"))
            .await;
        assert_eq!(frame, FrameType::Error, "{reply}");
        assert_eq!(reply["code"], "invalid_request", "{reply}");
        assert_eq!(reply["detail"]["field"], "kind", "{reply}");
    })
    .await;
}

#[tokio::test]
async fn declared_placement_caller_runner_resolution_and_catalog_validation() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        placement_catalog(&h, json!([{
            "name":"basal.task", "placement":"caller_runner", "selectors":["broca",fixture::RUNNER_ONE]
        }]));
        h.edit_config(|config| config["runners"] = json!([
            {"module":fixture::RUNNER_ONE}, {"module":"broca"}, {"module":"settings-only"}
        ]));
        let owner = h.bind(reserved("basal"), None).await;
        for runners in [None, Some(json!(["broca",fixture::RUNNER_ONE])), Some(json!(["settings-only"])), Some(json!(["unknown"]))] {
            let mut request = select("basal.task");
            if let Some(runners) = runners { request["runners"] = runners; }
            no_route(&owner.call("route.select", &request).await, "caller_runner_unresolved");
        }
        for module in ["broca", fixture::RUNNER_ONE] {
            let mut request = select("basal.task");
            request["runners"] = json!([module]);
            let reply = owner.call("route.select", &request).await;
            on_runner(&reply, module);
            assert!(reply.1["result"]["runner"]["harness"].is_string(), "{}", reply.1);
            let runner = h.bind(reserved(module), None).await;
            on_runner(&runner.call("route.select", &select("basal.task")).await, module);
            let mut other_harness = select("basal.task");
            other_harness["harness"] = json!("beta");
            let reply = runner.call("route.select", &other_harness).await;
            on_runner(&reply, module);
            assert_eq!(reply.1["result"]["runner"]["harness"], "beta", "{}", reply.1);
            let mut contradicted = select("basal.task");
            contradicted["runners"] = json!([if module == "broca" {fixture::RUNNER_ONE} else {"broca"}]);
            no_route(&runner.call("route.select", &contradicted).await, "harness_unavailable");
        }
        h.edit_config(|config| config["runners"] = json!([{"module":fixture::RUNNER_ONE}]));
        let mut request = select("basal.task");
        request["runners"] = json!(["broca"]);
        no_route(&owner.call("route.select", &request).await, "harness_unavailable");
    }).await;
}

#[tokio::test]
async fn declared_placement_what_if_uses_unchanged_broca_parameter() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        placement_catalog(
            &h,
            json!([{"name":"basal.task", "placement":"caller_runner"}]),
        );
        h.edit_config(|config| {
            config["runners"] = json!([
                {"module":fixture::RUNNER_ONE,"routes":["example-provider/model-a"]},
                {"module":"broca","routes":["example-provider/model-b"]}
            ])
        });
        let operator = h.bind(reserved("callosum"), None).await;
        let mut request = json!({"consumer":"basal.task", "iqs":[30]});
        let unresolved = operator.call("route.what_if", &request).await;
        assert_eq!(unresolved.0, FrameType::Response, "{}", unresolved.1);
        assert_eq!(
            unresolved.1["result"]["results"][0]["refusal"]["detail"]["reason"],
            "caller_runner_unresolved",
            "{}",
            unresolved.1
        );
        request["runner"] = json!("broca");
        let selected = operator.call("route.what_if", &request).await;
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        assert_eq!(
            selected.1["result"]["results"][0]["pick"]["route"], "example-provider/model-b",
            "{}",
            selected.1
        );
        request["runner"] = json!(fixture::RUNNER_ONE);
        let invalid = operator.call("route.what_if", &request).await;
        assert_eq!(invalid.1["code"], "invalid_request", "{}", invalid.1);
        assert_eq!(invalid.1["detail"]["field"], "runner", "{}", invalid.1);
    })
    .await;
}

#[tokio::test]
async fn declared_kind_select_retains_media_and_unknown_kinds_and_production_hold() {
    let h = harness();
    h.edit_config(|config| {
        config["runners"] = json!([{"module":fixture::RUNNER_ONE}, {"module":"broca"}]);
        config["consumers"]["basal"] = json!({"owner":"basal"});
    });
    let owner = h.bind(reserved("basal"), None).await;
    let declarations = json!([
        {"name":"basal.media", "kind":"transcribe", "placement":{"module":"broca"}},
        {"name":"basal.media.child"},
        {"name":"basal.future", "kind":"future-engine"}
    ]);
    // Consumer declarations alone do not enable the media select protocol.
    super::describe::without_capability(
        "select_media",
        super::describe::without_capability(
            "select_kind",
            super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
                placement_catalog(&h, declarations.clone());
                for (name, kind) in [
                    ("basal.media", "transcribe"),
                    ("basal.media.child.unlisted", "transcribe"),
                    ("basal.future", "future-engine"),
                ] {
                    let reply = owner.call("route.select", &select(name)).await;
                    no_route(&reply, "no_available_model");
                    let message = reply.1["message"].as_str().unwrap();
                    assert!(
                        message.contains("route.select_engine") && message.contains(kind),
                        "{}",
                        reply.1
                    );
                }
                let mut request = select("basal.media");
                request["kind"] = json!("transcribe");
                let invalid = owner.call("route.select", &request).await;
                assert_eq!(invalid.1["code"], "invalid_request", "{}", invalid.1);
                assert_eq!(invalid.1["detail"]["field"], "kind", "{}", invalid.1);
                assert!(!super::describe::capabilities().contains(&"select_kind"));
            }),
        ),
    )
    .await;
    // Withholding the group must still disable cached and newly read declarations.
    super::describe::without_capability("consumer_declarations", async {
        on_runner(
            &owner.call("route.select", &select("basal.media")).await,
            fixture::RUNNER_ONE,
        );
        consumers::replace_catalog(&json!([{"module_id":"basal","nonce_verified":true,
        "role_declarations":{"routing/v1":{"consumers":declarations}}}]));
        on_runner(
            &owner.call("route.select", &select("basal.media")).await,
            fixture::RUNNER_ONE,
        );
        assert!(!super::describe::capabilities().contains(&"consumer_declarations"));
    })
    .await;
}

#[tokio::test]
async fn declared_kind_engine_matches_or_refuses_crossing() {
    let h = harness();
    let owner = h.bind(reserved("basal"), None).await;
    let mut request = json!({"consumer":"basal.transcribe", "kind":"transcribe",
    "requirements":{}, "privacy":"local_only", "caller_local":[{
        "kind":"transcribe", "provider":"media-vendor", "model":"local-engine",
        "local":true, "formats":["audio/wav"], "languages":["en"], "latency_class":"interactive"
    }]});
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        placement_catalog(
            &h,
            json!([
                {"name":"basal.transcribe","kind":"transcribe"},
                {"name":"basal.synthesize","kind":"synthesize"},
                {"name":"basal.llm","kind":"llm"},
                {"name":"basal.undeclared"}
            ]),
        );
        for name in [
            "basal.transcribe",
            "basal.transcribe.child",
            "basal.undeclared",
        ] {
            request["consumer"] = json!(name);
            let reply = owner.call("route.select_engine", &request).await;
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            assert_eq!(
                reply.1["result"]["engine"]["model"], "local-engine",
                "{}",
                reply.1
            );
        }
        for name in ["basal.synthesize", "basal.llm"] {
            request["consumer"] = json!(name);
            let reply = owner.call("route.select_engine", &request).await;
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
            assert_eq!(reply.1["detail"]["field"], "kind", "{}", reply.1);
        }
    })
    .await;
    // An already decoded declaration still cannot restrict engines when the group is off.
    super::describe::without_capability("consumer_declarations", async {
        request["consumer"] = json!("basal.synthesize");
        let reply = owner.call("route.select_engine", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    })
    .await;
}

#[tokio::test]
async fn declared_defaults_iq_precedence_refusals_and_production_hold() {
    let h = harness();
    let modules = json!([{
        "module_id": "magic-context", "nonce_verified": true,
        "role_declarations": {"routing/v1": {"consumers": [
            {"name": "magic-context.summary", "iq": 40},
            {"name": "magic-context.no-default"}
        ]}}
    }]);
    let owner = h.bind(reserved("magic-context"), None).await;
    let stranger = h.bind(reserved("stranger"), None).await;
    let omitted = json!({"consumer":"magic-context.summary", "requirements":{}});
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        consumers::replace_catalog(&modules);
        let parsed = consumers::declared::parse_count();
        for (iq, expected) in [(None, 40), (Some(0), 0), (Some(25), 25)] {
            let mut request = omitted.clone();
            if let Some(iq) = iq {
                request["requirements"]["iq"] = json!(iq);
            }
            for _ in 0..2 {
                let reply = owner.call("route.select", &request).await;
                assert_eq!(
                    selected_input(&h, &reply)["iq"].as_f64(),
                    Some(f64::from(expected)),
                    "{}",
                    reply.1
                );
            }
            admission_refused(
                stranger.call("route.select", &request).await,
                "consumer_not_owned",
                "magic-context.summary",
            );
        }
        assert_eq!(
            consumers::declared::parse_count(),
            parsed,
            "repeated selects must not parse declarations"
        );
        h.edit_config(|config| config["consumers"]["magic-context"]["iq"] = json!(55.5));
        for request in [
            omitted.clone(),
            json!({"consumer":"magic-context.summary", "requirements":{"iq":0}}),
        ] {
            let reply = owner.call("route.select", &request).await;
            assert_eq!(selected_input(&h, &reply)["iq"], 55.5);
        }
        for (request, field) in [
            (
                json!({"consumer":"magic-context.no-default", "requirements":{}}),
                "iq",
            ),
            (json!({"consumer":"unknown", "requirements":{}}), "iq"),
            (json!({"consumer":"magic-context.summary"}), "requirements"),
        ] {
            let reply = owner.call("route.select", &request).await;
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], "invalid_requirements", "{}", reply.1);
            assert_eq!(reply.1["detail"]["field"], field, "{}", reply.1);
        }
    })
    .await;
    // Withholding consumer_declarations disables defaults even after selects used them.
    super::describe::without_capability("consumer_declarations", async {
        let reply = owner.call("route.select", &omitted).await;
        assert_eq!(reply.1["code"], "invalid_requirements", "{}", reply.1);
        consumers::replace_catalog(&modules);
        let reply = owner.call("route.select", &omitted).await;
        assert_eq!(reply.1["code"], "invalid_requirements", "{}", reply.1);
    })
    .await;
}

#[tokio::test]
async fn declared_defaults_inherit_by_member_only_for_the_same_owner() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        h.edit_config(|config| {
            config["consumers"]["magic-context.summary.settings"] =
                json!({"owner":"magic-context"});
            config["consumers"]["magic-context.summary.ownerless"] = json!({});
            config["consumers"]["magic-context.summary.foreign"] = json!({"owner":"other"});
            config["consumers"]["legacy.settings"] = json!({});
        });
        consumers::replace_catalog(&json!([{
            "module_id":"magic-context", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":"magic-context.summary", "iq":40, "eq":10, "selectors":["selector"],
                 "ladder":["example-provider/model-a"], "max_output_tokens":4096},
                {"name":"magic-context.summary.child"},
                {"name":"magic-context.summary.child.own", "iq":20, "eq":0,
                 "ladder":["example-provider/model-b"]},
                {"name":"legacy", "eq":17}
            ]}}
        }]));
        let owner = h.bind(reserved("magic-context"), None).await;
        let selector = h.bind(reserved("selector"), None).await;
        let config = h.operator_config();
        let registry = consumers::effective(&config);
        for (name, iq, eq, model) in [
            ("magic-context.summary.child", 40, 10, "model-a"),
            ("magic-context.summary.settings", 40, 10, "model-a"),
            ("magic-context.summary.ownerless", 40, 10, "model-a"),
            ("magic-context.summary.child.unlisted", 40, 10, "model-a"),
            ("magic-context.summary.child.own", 20, 0, "model-b"),
            ("magic-context.summary.child.own.unlisted", 20, 0, "model-b"),
        ] {
            for route in [&owner, &selector] {
                let reply = route
                    .call("route.select", &json!({"consumer":name,"requirements":{}}))
                    .await;
                let input = selected_input(&h, &reply);
                assert_eq!(input["iq"].as_f64(), Some(f64::from(iq)), "{name}: {input}");
                assert_eq!(input["eq"].as_f64(), Some(f64::from(eq)), "{name}: {input}");
                assert_eq!(
                    registry.with_defaults(name, &json!({"requirements":{}}), &config)
                        ["requirements"]["max_output_tokens"],
                    4096,
                    "{name}"
                );
                assert_eq!(reply.1["result"]["route"]["model"], model, "{}", reply.1);
            }
        }
        let other = h.bind(reserved("other"), None).await;
        for (name, route) in [
            ("magic-context.summary.foreign", &other),
            ("legacy.settings", &other),
        ] {
            let missing = route
                .call("route.select", &json!({"consumer":name,"requirements":{}}))
                .await;
            assert_eq!(missing.1["code"], "invalid_requirements", "{}", missing.1);
            let reply = route.call("route.select", &select(name)).await;
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            assert_eq!(
                reply.1["result"]["route"]["model"], "model-b",
                "{}",
                reply.1
            );
            let input = registry.with_defaults(name, &select(name), &config);
            assert!(
                input["requirements"]
                    .get("max_output_tokens")
                    .is_none_or(Value::is_null),
                "{input}"
            );
        }
        let legacy = owner.call("route.select", &select("legacy.unlisted")).await;
        assert_eq!(selected_input(&h, &legacy)["eq"].as_f64(), Some(17.0));
        let underscore = owner
            .call("route.select", &select("magic-context.summary_child"))
            .await;
        assert_eq!(underscore.1["code"], "unknown_consumer");
    })
    .await;
}

#[tokio::test]
async fn declared_defaults_ladder_mode_operator_overrides_and_empty_ladder_refusal() {
    let h = harness();
    let owner = h.bind(reserved("magic-context"), None).await;
    let request = select("magic-context.summary");
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        consumers::replace_catalog(&json!([{
            "module_id":"magic-context", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":"magic-context.summary", "ladder":["example-provider/model-a"]},
                {"name":"magic-context.empty"}
            ]}}
        }]));
        let reply = owner.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["route"]["model"], "model-a");
        h.edit_config(|config| {
            config["consumers"]["magic-context.summary"] = json!({"mode":"demand"})
        });
        let reply = owner.call("route.select", &request).await;
        assert_eq!(
            reply.1["result"]["route"]["model"], "model-b",
            "{}",
            reply.1
        );
        h.edit_config(|config| {
            config["consumers"]["magic-context.summary"] = json!({"mode":"ladder"})
        });
        let reply = owner.call("route.select", &request).await;
        assert_eq!(
            reply.1["result"]["route"]["model"], "model-a",
            "{}",
            reply.1
        );
        h.edit_config(|config| {
            config["consumers"]["magic-context.summary"]["ladder"] =
                json!(["example-provider/model-b"])
        });
        let reply = owner.call("route.select", &request).await;
        assert_eq!(
            reply.1["result"]["route"]["model"], "model-b",
            "{}",
            reply.1
        );
        h.edit_config(|config| {
            config["consumers"]["magic-context.empty"] = json!({"mode":"ladder"})
        });
        let reply = owner
            .call("route.select", &select("magic-context.empty"))
            .await;
        assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
        assert_eq!(
            reply.1["detail"]["reason"], "no_available_model",
            "{}",
            reply.1
        );
        h.edit_config(|config| {
            config["consumers"]
                .as_object_mut()
                .unwrap()
                .remove("magic-context.summary");
        });
    })
    .await;
    super::describe::without_capability("consumer_declarations", async {
        let reply = owner.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(
            reply.1["result"]["route"]["model"], "model-b",
            "{}",
            reply.1
        );
    })
    .await;
}

#[tokio::test]
async fn declared_defaults_ladder_never_widens_operator_model_provider_key_or_runner_policy() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        consumers::replace_catalog(&json!([{
            "module_id":"magic-context", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":"magic-context.summary", "iq":0, "ladder":[
                    "example-provider/model-a", "example-provider/model-b",
                    "example-subscription/model-c", "example-subscription/model-d"
                ]}
            ]}}
        }]));
        let owner = h.bind(reserved("magic-context"), None).await;
        let request = json!({"consumer":"magic-context.summary", "requirements":{}});
        let reply = owner.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["route"]["model"], "model-a");
        for restriction in ["pin_only", "not_for"] {
            h.edit_config(|config| {
                config["models"]["example-provider/model-a"] = json!({});
                match restriction {
                    "pin_only" => {
                        config["models"]["example-provider/model-a"]["pin_only"] = json!(true)
                    }
                    "not_for" => {
                        config["models"]["example-provider/model-a"]["not_for"] =
                            json!(["magic-context"])
                    }
                    _ => unreachable!(),
                }
            });
            let reply = owner.call("route.select", &request).await;
            assert_eq!(reply.0, FrameType::Response, "{restriction}: {}", reply.1);
            assert_eq!(
                reply.1["result"]["route"]["model"], "model-b",
                "{restriction}: {}",
                reply.1
            );
        }
        h.edit_config(|config| {
            config["consumers"]["magic-context.summary"]["pay_per_token"] = json!(false);
        });
        let reply = owner.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let id = reply.1["result"]["decision_id"].as_str().unwrap();
        assert_eq!(
            h.store()
                .v1_decision(id)
                .unwrap()
                .unwrap()
                .allowed_account_class,
            striatum_core::accounts::AllowedAccountClass::NotPayPerToken
        );
        h.edit_config(|config| {
            config["providers"]["example-provider"]["not_for"] = json!(["magic-context"]);
        });
        let reply = owner.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(
            reply.1["result"]["route"]["provider"], "example-subscription",
            "{}",
            reply.1
        );
        h.edit_config(|config| {
            config["runners"] = json!([
                {"module":"runner-one", "routes":["example-provider"]}
            ])
        });
        let reply = owner.call("route.select", &request).await;
        assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
        assert_eq!(
            reply.1["detail"]["reason"], "no_available_model",
            "{}",
            reply.1
        );
    })
    .await;
}

const SETTINGS_LADDER: [&str; 3] = [
    "example-provider/model-a",
    "example-subscription/model-c",
    "example-subscription/model-d",
];
const LADDER_NOW: u64 = 1_800_000_000_000;

fn settings_ladder_harness() -> Arc<Harness> {
    let h = harness();
    h.edit_config(|config| {
        config["consumers"]["magic-context"] = json!({
            "owner":"magic-context", "mode":"ladder", "ladder":SETTINGS_LADDER
        });
        config["providers"]["example-provider"]["pin_only"] = json!(true);
        config["models"][SETTINGS_LADDER[1]]["pin_only_for"] = json!(["magic-context"]);
        // Leave an automatic candidate outside the ladder to detect accidental fallback.
        config["models"]["example-provider/model-b"]["pin_only"] = json!(false);
    });
    h
}

fn settings_ladder_wall(h: &Harness, key: &str, reset: u64) {
    h.store().update_route_state(key, key, &striatum_core::RouteStateUpdate {
        cooldown_until: Some(reset as f64),
        observed_at_ms: Some(LADDER_NOW as i64 + 1),
        wall_class: Some(striatum_core::RouteWallClass::Quota),
        last_error: Some("quota exhausted".into()),
        ..Default::default()
    }).unwrap();
}

async fn settings_ladder_preview(h: &Arc<Harness>, consumer: &str) -> Value {
    let route = h.bind(json!({"kind":"direct"}), None).await;
    let reply = route.call("route.what_if", &json!({"consumer":consumer,"iqs":[0]})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"]["results"][0].clone()
}

#[tokio::test]
async fn settings_ladder_admits_pin_only_routes_in_order_and_preserves_waits() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = settings_ladder_harness();
    let route = h.bind(reserved("magic-context"), None).await;
    for consumer in ["magic-context", "magic-context.summary"] {
        let request = json!({"consumer":consumer,"requirements":{"iq":0},"hold_wait_max_s":0});
        let reply = route.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["route"]["model"], "model-a", "{}", reply.1);
        let preview = settings_ladder_preview(&h, consumer).await;
        assert_eq!(preview["pick"]["route"], SETTINGS_LADDER[0], "{preview}");
        let mut excluded = request.clone();
        excluded["exclude"] = json!([SETTINGS_LADDER[0]]);
        let reply = route.call("route.select", &excluded).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["route"]["model"], "model-c", "{}", reply.1);
    }
    let request = json!({"consumer":"magic-context.summary","requirements":{"iq":0},"hold_wait_max_s":0});
    for (key, reset) in SETTINGS_LADDER[..2].iter().zip([LADDER_NOW + 20_000, LADDER_NOW + 30_000]) {
        settings_ladder_wall(&h, key, reset);
    }
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["route"]["model"], "model-d", "{}", reply.1);
    let preview = settings_ladder_preview(&h, "magic-context.summary").await;
    assert_eq!(preview["pick"]["route"], SETTINGS_LADDER[2], "{preview}");
    settings_ladder_wall(&h, SETTINGS_LADDER[2], LADDER_NOW + 40_000);
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    assert_eq!(reply.1["detail"]["reason"], "quota_or_cooldown", "{}", reply.1);
    assert_eq!(reply.1["detail"]["reset_at_ms"], LADDER_NOW + 20_000, "{}", reply.1);
    let preview = settings_ladder_preview(&h, "magic-context.summary").await;
    assert_eq!(preview["refusal"]["code"], reply.1["code"], "{preview}");
    assert_eq!(preview["refusal"]["detail"]["reason"], reply.1["detail"]["reason"], "{preview}");
    assert_eq!(preview["refusal"]["detail"]["reset_at_ms"], reply.1["detail"]["reset_at_ms"], "{preview}");
}

#[tokio::test]
async fn settings_ladder_pin_only_admission_still_obeys_not_for_and_only_for() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = settings_ladder_harness();
    let route = h.bind(reserved("magic-context"), None).await;
    let request = json!({"consumer":"magic-context.summary","requirements":{"iq":0}});
    // The second route is pin-only too: reaching it proves the ladder's pin
    // admission does not bypass the first route's consumer restriction.
    for restriction in [json!({"not_for":["magic-context"]}), json!({"only_for":["other"]})] {
        h.edit_config(|config| config["models"][SETTINGS_LADDER[0]] = restriction);
        let reply = route.call("route.select", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["route"]["model"], "model-c", "{}", reply.1);
        let preview = settings_ladder_preview(&h, "magic-context.summary").await;
        assert_eq!(preview["pick"]["route"], SETTINGS_LADDER[1], "{preview}");
    }
}

#[tokio::test]
async fn settings_ladder_all_accounts_blocked_without_reset_is_typed() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = settings_ladder_harness();
    let mut third = h.store().get_facts().unwrap().into_iter().find(|fact| {
        fact.provider_id == "example-subscription" && fact.model_id == "model-c"
    }).unwrap();
    third.model_id = "model-e".into();
    h.store().upsert_fact(&third, striatum_core::FactSource::Override).unwrap();
    h.edit_config(|config| {
        config["consumers"]["magic-context"]["ladder"] = json!([
            "example-subscription/model-e", SETTINGS_LADDER[1], SETTINGS_LADDER[2]
        ]);
        config["providers"]["example-subscription"]["pin_only"] = json!(true);
    });
    let route = h.bind(reserved("magic-context"), None).await;
    {
        let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
        for reading in quota.readings.readings.values_mut() {
            reading.received_at_ms = LADDER_NOW as i64;
            reading.entry["fetchedAt"] = json!(crate::account_selection::rfc3339(LADDER_NOW as i64));
            for name in ["primary", "secondary"] {
                if let Some(window) = reading.entry["usage"][name].as_object_mut() {
                    window.insert("usedPercent".into(), json!(100.0));
                    window.remove("resetsAt");
                }
            }
        }
    }
    let request = json!({"consumer":"magic-context.summary","requirements":{"iq":0},"hold_wait_max_s":0});
    let reply = route.call("route.select", &request).await;
    no_route(&reply, "accounts_blocked");
    let preview = settings_ladder_preview(&h, "magic-context.summary").await;
    assert_eq!(preview["refusal"]["code"], "no_route", "{preview}");
    assert_eq!(preview["refusal"]["detail"]["reason"], "accounts_blocked", "{preview}");
    assert!(preview["refusal"]["detail"].get("reset_at_ms").is_none(), "{preview}");
}

#[tokio::test]
async fn settings_ladder_what_if_admits_operator_pin_only_choices() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = settings_ladder_harness();
    let route = h.bind(reserved("magic-context"), None).await;
    for (index, expected) in SETTINGS_LADDER.iter().enumerate() {
        let preview = settings_ladder_preview(&h, "magic-context.summary").await;
        assert_eq!(preview["pick"]["route"], *expected, "{preview}");
        let live = route.call("route.select", &json!({
            "consumer":"magic-context.summary", "requirements":{"iq":0}, "hold_wait_max_s":0
        })).await;
        assert_eq!(live.0, FrameType::Response, "{}", live.1);
        let live = &live.1["result"]["route"];
        assert_eq!(format!("{}/{}", live["provider"].as_str().unwrap(), live["model"].as_str().unwrap()), *expected);
        settings_ladder_wall(&h, expected, LADDER_NOW + 20_000 + index as u64 * 10_000);
    }
    let preview = settings_ladder_preview(&h, "magic-context.summary").await;
    assert_eq!(preview["refusal"]["code"], "no_route", "{preview}");
    assert_eq!(preview["refusal"]["detail"]["reason"], "quota_or_cooldown", "{preview}");
    assert_eq!(preview["refusal"]["detail"]["reset_at_ms"], LADDER_NOW + 20_000, "{preview}");
}

#[tokio::test]
async fn settings_ladder_legacy_selection_admits_pin_only_but_not_consumer_restrictions() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = settings_ladder_harness();
    h.edit_config(|config| {
        config["consumers"]["codemode"]["mode"] = json!("ladder");
        config["consumers"]["codemode"]["ladder"] = json!(SETTINGS_LADDER);
        config["consumers"]["codemode"]["pay_per_token"] = json!(false);
        config["models"][SETTINGS_LADDER[1]]["pin_only_for"] = json!(["codemode"]);
    });
    let route = h.bind(reserved("magic-context"), None).await;
    let request = json!({"consumer":"codemode", "targetAgent":"codemode",
        "executor":"broca", "substrate":"broca", "requirements":{"iq":30,"eq":0}});
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["selected"]["routeKey"], SETTINGS_LADDER[0], "{}", reply.1);
    h.edit_config(|config| config["models"][SETTINGS_LADDER[0]]["not_for"] = json!(["codemode"]));
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["selected"]["routeKey"], SETTINGS_LADDER[1], "{}", reply.1);
    for key in &SETTINGS_LADDER[1..] {
        settings_ladder_wall(&h, key, LADDER_NOW + 30_000);
    }
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
}

const LADDER_VARIANTS: [&str; 3] = ["high", "high", "medium"];

fn variant_ladder_harness(first_variant: &str, legacy: bool) -> Arc<Harness> {
    let h = settings_ladder_harness();
    let entries: Vec<_> = SETTINGS_LADDER.iter().zip([first_variant, "high", "medium"])
        .map(|(route, variant)| format!("{route}:{variant}")).collect();
    h.edit_config(|config| {
        let consumer = if legacy { "codemode" } else { "magic-context" };
        config["consumers"][consumer]["mode"] = json!("ladder");
        config["consumers"][consumer]["ladder"] = json!(entries);
        if legacy {
            config["consumers"][consumer]["pay_per_token"] = json!(false);
        }
        config["consumers"][consumer]["variants"] = json!({
            (SETTINGS_LADDER[0]): "low", (SETTINGS_LADDER[1]): "low", (SETTINGS_LADDER[2]): "low"
        });
        // Disable pin-only restrictions to test suffix preservation independently.
        config["providers"]["example-provider"]["pin_only"] = json!(false);
        config["models"][SETTINGS_LADDER[1]]["pin_only_for"] = json!([]);
    });
    for mut fact in h.store().get_facts().unwrap() {
        if SETTINGS_LADDER.contains(&format!("{}/{}", fact.provider_id, fact.model_id).as_str()) {
            fact.prompt_defaults.get_or_insert_with(Default::default).variant = Some("low".into());
            h.store().upsert_fact(&fact, striatum_core::FactSource::Override).unwrap();
        }
    }
    h.attach_reasoning_catalog(striatum_store::ReasoningCatalog {
        models: SETTINGS_LADDER.iter().map(|route| ((*route).to_owned(),
            striatum_store::PublishedEfforts::Published(vec!["low".into(),"medium".into(),"high".into()]))).collect(),
    });
    h
}

#[tokio::test]
async fn settings_ladder_preserves_each_explicit_variant_in_decisions_and_what_if() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = variant_ladder_harness("high", false);
    let route = h.bind(reserved("magic-context"), None).await;
    for index in 0..SETTINGS_LADDER.len() {
        let excluded: Vec<_> = SETTINGS_LADDER[..index].iter().zip(&LADDER_VARIANTS[..index])
            .map(|(key, variant)| format!("{key}/{variant}")).collect();
        let reply = route.call("route.select", &json!({"consumer":"magic-context.summary",
            "requirements":{"iq":0}, "exclude":excluded, "hold_wait_max_s":0})).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let result = &reply.1["result"];
        assert_eq!(format!("{}/{}", result["route"]["provider"].as_str().unwrap(), result["route"]["model"].as_str().unwrap()), SETTINGS_LADDER[index], "{}", reply.1);
        assert_eq!(result["route"]["variant"], LADDER_VARIANTS[index], "{}", reply.1);
        let decision = h.store().v1_decision(result["decision_id"].as_str().unwrap()).unwrap().unwrap();
        assert_eq!(decision.variant.as_deref(), Some(LADDER_VARIANTS[index]));
    }
    for index in 0..SETTINGS_LADDER.len() {
        let preview = settings_ladder_preview(&h, "magic-context.summary").await;
        assert_eq!(preview["pick"]["variant"], LADDER_VARIANTS[index], "{preview}");
        assert_eq!(preview["pick"]["route"], format!("{}/{}", SETTINGS_LADDER[index], LADDER_VARIANTS[index]), "{preview}");
        settings_ladder_wall(&h, SETTINGS_LADDER[index], LADDER_NOW + 20_000 + index as u64 * 10_000);
    }
}

#[tokio::test]
async fn settings_ladder_unpublished_variant_refuses_like_an_explicit_pin() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = variant_ladder_harness("unsupported", false);
    let route = h.bind(reserved("magic-context"), None).await;
    let request = json!({"consumer":"magic-context.summary","requirements":{"iq":0}});
    let reply = route.call("route.select", &request).await;
    let mut pinned = request;
    pinned["pin"] = json!(format!("{}:unsupported", SETTINGS_LADDER[0]));
    let pin_reply = route.call("route.select", &pinned).await;
    assert_eq!(pin_reply.1["code"], "pin_unavailable", "{}", pin_reply.1);
    assert_eq!(pin_reply.1["detail"]["reason"], "invalid_variant", "{}", pin_reply.1);
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], pin_reply.1["code"], "{}", reply.1);
    assert_eq!(reply.1["detail"]["reason"], pin_reply.1["detail"]["reason"], "{}", reply.1);
    let preview = settings_ladder_preview(&h, "magic-context.summary").await;
    assert_eq!(preview["refusal"]["code"], reply.1["code"], "{preview}");
    assert_eq!(preview["refusal"]["detail"]["reason"], reply.1["detail"]["reason"], "{preview}");
}

#[tokio::test]
async fn settings_ladder_explicit_variant_walls_keep_the_earliest_reset() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = variant_ladder_harness("high", false);
    let route = h.bind(reserved("magic-context"), None).await;
    for (index, (key, variant)) in SETTINGS_LADDER.iter().zip(LADDER_VARIANTS).enumerate() {
        settings_ladder_wall(&h, &format!("{key}/{variant}"), LADDER_NOW + 20_000 + index as u64 * 10_000);
    }
    let reply = route.call("route.select", &json!({"consumer":"magic-context.summary",
        "requirements":{"iq":0}, "hold_wait_max_s":0})).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["detail"]["reason"], "quota_or_cooldown", "{}", reply.1);
    assert_eq!(reply.1["detail"]["reset_at_ms"], LADDER_NOW + 20_000, "{}", reply.1);
    let preview = settings_ladder_preview(&h, "magic-context.summary").await;
    assert_eq!(preview["refusal"]["detail"]["reason"], reply.1["detail"]["reason"], "{preview}");
    assert_eq!(preview["refusal"]["detail"]["reset_at_ms"], reply.1["detail"]["reset_at_ms"], "{preview}");
}

#[tokio::test]
async fn settings_ladder_legacy_selection_preserves_explicit_variant() {
    let _clock = crate::InjectedClock::set(LADDER_NOW);
    let h = variant_ladder_harness("high", true);
    let route = h.bind(reserved("magic-context"), None).await;
    let reply = route.call("route.select", &json!({"consumer":"codemode", "targetAgent":"codemode",
        "executor":"broca", "substrate":"broca", "requirements":{"iq":30,"eq":0}})).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["selected"]["promptParams"]["variant"], "high", "{}", reply.1);
    assert_eq!(reply.1["result"]["selected"]["routeKey"], format!("{}/high", SETTINGS_LADDER[0]), "{}", reply.1);
}

fn admission_refused(reply: (FrameType, Value), code: &str, consumer: &str) {
    refused(reply.clone(), code, consumer);
    assert_eq!(reply.1["detail"], json!({"consumer": consumer}));
    for member in ["reset_at_ms", "retry_after_ms", "retry_at_ms"] {
        assert!(reply.1.get(member).is_none(), "{}", reply.1);
    }
}

#[tokio::test]
async fn declared_admission_selectors_use_attested_identity_not_their_scope() {
    let h = harness();
    let modules = json!([{
        "module_id": "admission-owner", "nonce_verified": true,
        "role_declarations": {"routing/v1": {"consumers": [
            {"name": "admission-owner.task", "iq": 40, "selectors": ["admission-selector"]}
        ]}}
    }]);
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        consumers::replace_catalog(&modules);
        let owner = h.bind(reserved("admission-owner"), None).await;
        let selector = h.bind(reserved("admission-selector"), None).await;
        let delegate = scoped(&h, "admission-owner", true, h.reserve_channel(), 1).await;
        for route in [&selector, &owner, &delegate] {
            let reply = route
                .call("route.select", &select("admission-owner.task"))
                .await;
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        }
        let stranger = h.bind(reserved("stranger"), None).await;
        let selector_scope = scoped(&h, "admission-selector", true, h.reserve_channel(), 1).await;
        let unauthorized = scoped(&h, "admission-owner", false, h.reserve_channel(), 1).await;
        for route in [&stranger, &selector_scope, &unauthorized] {
            admission_refused(
                route
                    .call("route.select", &select("admission-owner.task"))
                    .await,
                "consumer_not_owned",
                "admission-owner.task",
            );
        }
    })
    .await;

    // Withholding consumer_declarations disables selectors even after selects used them.
    super::describe::without_capability("consumer_declarations", async {
        consumers::replace_catalog(&modules);
        let owner = h.bind(reserved("admission-owner"), None).await;
        let selector = h.bind(reserved("admission-selector"), None).await;
        for route in [&owner, &selector] {
            admission_refused(
                route
                    .call("route.select", &select("admission-owner.task"))
                    .await,
                "unknown_consumer",
                "admission-owner.task",
            );
        }
    })
    .await;
}

#[tokio::test]
async fn declared_admission_unlisted_dotted_children_inherit_legacy_and_settings_admission() {
    let h = harness();
    h.edit_config(|config| {
        config["consumers"]["settings-parent"] = json!({
            "mode": "ladder", "ladder": ["example-provider/model-b"]
        });
        config["consumers"]["settings-parent.owned"] = json!({"owner": "settings-owner"});
    });
    consumers::replace_catalog(&json!([{
        "module_id": "legacy-owner", "nonce_verified": true,
        "role_declarations": {"routing/v1": {"consumers": [
            {"name": "legacy-owner.task", "eq": 17},
            {"name": "legacy-owner.task.nested", "eq": 19},
            {"name": "transitional-task", "eq": 23}
        ]}}
    }]));
    let owner = h.bind(reserved("legacy-owner"), None).await;
    let stranger = h.bind(reserved("stranger"), None).await;
    let config = h.operator_config();
    let registry = consumers::effective(&config);
    for (consumer, eq) in [
        ("legacy-owner.task.child", 17),
        ("legacy-owner.task.nested.child", 19),
        ("transitional-task.child", 23),
    ] {
        assert!(registry.knows(consumer));
        assert_eq!(
            registry.with_defaults(consumer, &select(consumer), &config)["requirements"]["eq"],
            eq
        );
        let reply = owner.call("route.select", &select(consumer)).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let id = reply.1["result"]["decision_id"].as_str().unwrap();
        assert_eq!(
            h.store().v1_decision(id).unwrap().unwrap().consumer,
            consumer
        );
        admission_refused(
            stranger.call("route.select", &select(consumer)).await,
            "consumer_not_owned",
            consumer,
        );
    }
    let direct = h.bind(json!({"kind": "direct"}), None).await;
    for route in [&owner, &stranger, &direct] {
        let reply = route
            .call("route.select", &select("settings-parent.child"))
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["route"]["model"], "model-b");
    }
    let settings_owner = h.bind(reserved("settings-owner"), None).await;
    let reply = settings_owner
        .call("route.select", &select("settings-parent.owned.child"))
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["route"]["model"], "model-b");
    admission_refused(
        stranger
            .call("route.select", &select("settings-parent.owned.child"))
            .await,
        "consumer_not_owned",
        "settings-parent.owned.child",
    );
    for consumer in [
        "legacy-owner.task_child",
        "transitional-task_child",
        "no-ancestor.child",
    ] {
        assert!(!registry.knows(consumer));
        admission_refused(
            owner.call("route.select", &select(consumer)).await,
            "unknown_consumer",
            consumer,
        );
    }
}

#[tokio::test]
async fn declared_admission_selector_selects_an_engine_and_reports_its_own_decisions() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        consumers::replace_catalog(&json!([{
            "module_id": "engine-owner", "nonce_verified": true,
            "role_declarations": {"routing/v1": {"consumers": [
                {"name": "engine-owner.task", "iq": 40, "selectors": ["engine-selector"]}
            ]}}
        }]));
        let selector = h.bind(reserved("engine-selector"), None).await;
        let request = json!({"consumer": "engine-owner.task", "kind": "transcribe",
        "requirements": {}, "privacy": "local_only", "caller_local": [{
            "kind": "transcribe", "provider": "media-vendor", "model": "local-engine",
            "local": true, "formats": ["audio/wav"], "languages": ["en"],
            "latency_class": "interactive"
        }]});
        let stranger = h.bind(reserved("stranger"), None).await;
        admission_refused(
            stranger.call("route.select_engine", &request).await,
            "consumer_not_owned",
            "engine-owner.task",
        );
        let selected = selector.call("route.select_engine", &request).await;
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        assert_eq!(selected.1["result"]["engine"]["model"], "local-engine");
        let llm = selector
            .call("route.select", &select("engine-owner.task"))
            .await;
        assert_eq!(llm.0, FrameType::Response, "{}", llm.1);
        for decision in [selected, llm] {
            let report =
                json!({"decision_id": decision.1["result"]["decision_id"], "outcome": "completed"});
            let reply = selector.call("route.report", &report).await;
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        }
    })
    .await;
}

#[tokio::test]
async fn declared_admission_inherits_selectors_and_locks_only_extended_settings_children() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        h.edit_config(|config| {
            for name in [
                "inherit-owner.task.settings",
                "inherit-owner.task.branch.settings",
                "inherit-owner.legacy.settings",
            ] {
                config["consumers"][name] = json!({});
            }
            config["consumers"]["inherit-owner.task.owned"] = json!({"owner": "inherit-owner"});
            config["consumers"]["inherit-owner.task.foreign"] = json!({"owner": "settings-owner"});
        });
        let modules = json!([{
            "module_id": "inherit-owner", "nonce_verified": true,
            "role_declarations": {"routing/v1": {"consumers": [
                {"name": "inherit-owner.task", "iq": 40, "selectors": ["parent-selector"]},
                {"name": "inherit-owner.task.branch"},
                {"name": "inherit-owner.task.closed", "selectors": []},
                {"name": "inherit-owner.task.changed", "selectors": ["child-selector"]},
                {"name": "inherit-owner.task_underscore"},
                {"name": "inherit-owner.legacy", "eq": 17}
            ]}}
        }]);
        consumers::replace_catalog(&modules);
        let owner = h.bind(reserved("inherit-owner"), None).await;
        let parent_selector = h.bind(reserved("parent-selector"), None).await;
        let child_selector = h.bind(reserved("child-selector"), None).await;
        let stranger = h.bind(reserved("stranger"), None).await;
        for consumer in [
            "inherit-owner.task.child",
            "inherit-owner.task.branch",
            "inherit-owner.task.branch.child",
            "inherit-owner.task.settings",
            "inherit-owner.task.branch.settings",
            "inherit-owner.task.owned",
        ] {
            for route in [&parent_selector, &owner] {
                let reply = route.call("route.select", &select(consumer)).await;
                assert_eq!(reply.0, FrameType::Response, "{consumer}: {}", reply.1);
            }
            for route in [&child_selector, &stranger] {
                admission_refused(
                    route.call("route.select", &select(consumer)).await,
                    "consumer_not_owned",
                    consumer,
                );
            }
        }
        for consumer in [
            "inherit-owner.task.closed",
            "inherit-owner.task.closed.child",
            "inherit-owner.task_underscore",
        ] {
            assert_eq!(
                owner.call("route.select", &select(consumer)).await.0,
                FrameType::Response
            );
            admission_refused(
                parent_selector
                    .call("route.select", &select(consumer))
                    .await,
                "consumer_not_owned",
                consumer,
            );
        }
        for consumer in [
            "inherit-owner.task.changed",
            "inherit-owner.task.changed.child",
        ] {
            assert_eq!(
                child_selector
                    .call("route.select", &select(consumer))
                    .await
                    .0,
                FrameType::Response
            );
            admission_refused(
                parent_selector
                    .call("route.select", &select(consumer))
                    .await,
                "consumer_not_owned",
                consumer,
            );
        }
        let settings_owner = h.bind(reserved("settings-owner"), None).await;
        assert_eq!(
            settings_owner
                .call("route.select", &select("inherit-owner.task.foreign"))
                .await
                .0,
            FrameType::Response
        );
        for route in [&owner, &parent_selector] {
            admission_refused(
                route
                    .call("route.select", &select("inherit-owner.task.foreign"))
                    .await,
                "consumer_not_owned",
                "inherit-owner.task.foreign",
            );
        }
        for route in [&owner, &parent_selector, &stranger] {
            assert_eq!(
                route
                    .call("route.select", &select("inherit-owner.legacy.settings"))
                    .await
                    .0,
                FrameType::Response
            );
        }
        let config = h.operator_config();
        assert!(consumers::effective(&config).with_defaults(
            "inherit-owner.legacy.settings",
            &select("inherit-owner.legacy.settings"),
            &config
        )["requirements"]
            .get("eq")
            .is_none());
        let mut refreshed = modules;
        refreshed[0]["role_declarations"]["routing/v1"]["consumers"][0]["selectors"] = json!([]);
        consumers::replace_catalog(&refreshed);
        admission_refused(
            parent_selector
                .call(
                    "route.select",
                    &select("inherit-owner.task.branch.settings"),
                )
                .await,
            "consumer_not_owned",
            "inherit-owner.task.branch.settings",
        );
    })
    .await;
}

#[tokio::test]
async fn declared_admission_ignored_claims_do_not_conflict_but_legacy_and_settings_claims_do() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        let owner_entry = json!({"module_id": "conflict-owner", "nonce_verified": true,
        "role_declarations": {"routing/v1": {"consumers": [{
            "name": "conflict-owner.task", "iq": 40, "eq": 17,
            "selectors": ["conflict-selector"]
        }]}}});
        let ignored_entry = json!({"module_id": "other-owner", "nonce_verified": true,
        "role_declarations": {"routing/v1": {"consumers": [{
            "name": "conflict-owner.task", "iq": 60, "selectors": ["other-selector"]
        }]}}});
        let owner = h.bind(reserved("conflict-owner"), None).await;
        let selector = h.bind(reserved("conflict-selector"), None).await;
        let other = h.bind(reserved("other-owner"), None).await;
        let other_selector = h.bind(reserved("other-selector"), None).await;
        for modules in [
            json!([owner_entry, ignored_entry]),
            json!([ignored_entry, owner_entry]),
        ] {
            consumers::replace_catalog(&modules);
            for route in [&selector, &owner] {
                let reply = route
                    .call("route.select", &select("conflict-owner.task"))
                    .await;
                assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            }
            for route in [&other, &other_selector] {
                admission_refused(
                    route
                        .call("route.select", &select("conflict-owner.task"))
                        .await,
                    "consumer_not_owned",
                    "conflict-owner.task",
                );
            }
        }
        let legacy_claim = entry("other-owner", true, &["conflict-owner.task"]);
        for modules in [
            json!([owner_entry, legacy_claim]),
            json!([legacy_claim, owner_entry]),
            json!([owner_entry]),
        ] {
            if modules.as_array().unwrap().len() == 1 {
                h.edit_config(|config| {
                    config["consumers"]["conflict-owner.task"] = json!({"owner": "other-owner"})
                });
            }
            consumers::replace_catalog(&modules);
            let config = h.operator_config();
            let registry = consumers::effective(&config);
            assert!(!registry.policies.contains_key("conflict-owner.task"));
            assert!(!registry
                .names()
                .iter()
                .any(|name| name == "conflict-owner.task"));
            let defaults = registry.with_defaults(
                "conflict-owner.task",
                &select("conflict-owner.task"),
                &config,
            );
            assert_eq!(defaults["requirements"], json!({"iq": 30}));
            for route in [&owner, &selector, &other, &other_selector] {
                for consumer in ["conflict-owner.task", "conflict-owner.task.child"] {
                    admission_refused(
                        route.call("route.select", &select(consumer)).await,
                        "consumer_not_owned",
                        consumer,
                    );
                }
            }
        }
    })
    .await;
}

async fn consumer_health_report(harness: &Harness) -> Value {
    let frame = subc_protocol::Frame::build(
        FrameType::Request,
        crate::control_flags(),
        0,
        0,
        1,
        serde_json::to_vec(&json!({"op": subc_protocol::session::MODULE_CONTROL_OP_HEALTH_CHECK}))
            .unwrap(),
    )
    .unwrap();
    serde_json::from_slice(&harness.handle(frame).await.body).unwrap()
}

#[tokio::test]
async fn declared_names_ignore_foreign_extensions_keep_legacy_admission_and_refresh() {
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        let h = harness();
        let before = consumer_health_report(&h).await;
        let modules = json!([{
            "module_id": "name-owner", "nonce_verified": true,
            "role_declarations": {"routing/v1": {"consumers": [
                {"name": "outside-new", "iq": 40, "eq": 70},
                {"name": "name-owner.task", "iq": 40},
                {"name": "outside-legacy", "eq": 23},
                {"name": "outside-null", "eq": 24, "iq": null, "ladder": null,
                 "placement": null, "selectors": null, "kind": null}
            ]}}
        }]);
        crate::take_operator_warnings();
        consumers::replace_catalog(&modules);
        let health = consumers::health();
        let after = consumer_health_report(&h).await;
        assert!(before.get("status").is_some(), "{before}");
        assert_eq!(after["status"], before["status"]);
        assert_eq!(after["detail"], before["detail"]);
        assert_eq!(after["metrics"]["consumerDeclarations"], health);
        let issues = health["issues"].as_array().unwrap();
        assert_eq!(issues.len(), 3, "{health}");
        let lines = crate::take_operator_warnings();
        for issue in issues {
            assert_eq!(
                lines
                    .iter()
                    .filter(|line| line.contains(issue.as_str().unwrap()))
                    .count(),
                1
            );
        }
        consumers::replace_catalog(&modules);
        assert_eq!(consumers::health(), health);
        assert!(crate::take_operator_warnings().is_empty());
        let owner = h.bind(reserved("name-owner"), None).await;
        let stranger = h.bind(reserved("other-module"), None).await;
        let config = h.operator_config();
        let registry = consumers::effective(&config);
        assert!(!registry.knows("outside-new"));
        assert!(!registry.policies.contains_key("outside-new"));
        assert_eq!(
            registry.with_defaults("outside-new", &select("outside-new"), &config)["requirements"]
                .get("eq"),
            None
        );
        refused(
            owner.call("route.select", &select("outside-new")).await,
            "unknown_consumer",
            "outside-new",
        );
        for (name, eq) in [
            ("outside-legacy", 23),
            ("outside-null", 24),
            ("name-owner.task", 0),
        ] {
            assert_eq!(
                owner.call("route.select", &select(name)).await.0,
                FrameType::Response
            );
            refused(
                stranger.call("route.select", &select(name)).await,
                "consumer_not_owned",
                name,
            );
            if eq != 0 {
                let defaults = registry.with_defaults(name, &select(name), &config);
                assert_eq!(defaults["requirements"]["eq"], eq);
                let missing_iq = registry.with_defaults(
                    name,
                    &json!({"consumer":name, "requirements":{}}),
                    &config,
                );
                assert!(missing_iq["requirements"].get("iq").is_none());
            }
        }
        let parsed = consumers::declared::parse_count();
        for _ in 0..3 {
            assert_eq!(
                owner
                    .call("route.select", &select("name-owner.task"))
                    .await
                    .0,
                FrameType::Response
            );
        }
        assert_eq!(
            consumers::declared::parse_count(),
            parsed,
            "select must not parse declarations"
        );
        assert!(crate::take_operator_warnings().is_empty());
        consumers::replace_catalog(&json!([entry("replacement", true, &["replacement.task"])]));
        assert!(consumers::declared::parse_count() > parsed);
        refused(
            owner.call("route.select", &select("name-owner.task")).await,
            "unknown_consumer",
            "name-owner.task",
        );
        let replacement = h.bind(reserved("replacement"), None).await;
        assert_eq!(
            replacement
                .call("route.select", &select("replacement.task"))
                .await
                .0,
            FrameType::Response
        );
        assert_eq!(consumers::health()["issues"], json!([]));
    })
    .await;
}

#[tokio::test]
async fn settings_only_names_log_info_at_load_without_health_warnings_or_request_logs() {
    let h = harness();
    consumers::replace_catalog(&json!([entry(
        "declared-owner",
        true,
        &["declared-owner.task"]
    )]));
    crate::take_operator_warnings();
    crate::take_operator_infos();
    h.edit_config(|config| {
        config["consumers"] =
            json!({"settings-only": {}, "declared-owner.task": {}, "head": {}, "classifier": {}});
    });
    assert_eq!(
        crate::take_operator_infos(),
        vec!["[striatum] settings consumer \"settings-only\" has no module declaration"]
    );
    assert!(crate::take_operator_warnings().is_empty());
    assert_eq!(consumers::health()["issues"], json!([]));
    let caller = h.bind(reserved("any-bound-module"), None).await;
    for _ in 0..3 {
        h.operator_config();
        assert_eq!(
            caller
                .call("route.select", &select("settings-only"))
                .await
                .0,
            FrameType::Response
        );
    }
    // Per-call telemetry is expected; declaration diagnostics still belong
    // only to settings load, not to each request using a settings-only name.
    assert_eq!(
        crate::take_operator_infos(),
        vec![format!(
            "routing select call {}",
            json!({"surface":"routing/v1","consumer":"settings-only", "class":"worker","head_request":false})
        ); 3]
    );
    assert!(crate::take_operator_warnings().is_empty());
    assert_eq!(consumers::health()["issues"], json!([]));
}

#[tokio::test]
async fn declared_names_mixed_runner_slot_and_preview_follow_serving_capabilities() {
    let h = harness();
    let models: serde_json::Map<_, _> = h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .map(|mut fact| {
            fact.supports_tool_use = Some(true);
            h.store()
                .upsert_fact(&fact, striatum_core::FactSource::Override)
                .unwrap();
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                json!({"existence":"present", "capability.tool_call":true}),
            )
        })
        .collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({"catalog_version":41, "resolved_at_ms":123, "models":models}))
            .unwrap(),
    )
    .unwrap();
    h.credentials().lock().unwrap().catalog_snapshot = Some(Arc::new(snapshot));
    h.edit_config(|config| {
        config["consumers"] = json!({"preview": {}});
        config["runners"] = json!([{"module":"broca"}]);
    });
    let slot = json!({
        "consumers": [{"name":"broca.task", "iq":40}, {"name":"foreign-task", "iq":40}],
        "runner": {
            "pairs":[{"wire_api":"anthropic.messages", "auth_method":"apikey"},
                     {"wire_api":"anthropic.messages", "auth_method":"oauth"}],
            "harnesses":["name-test"], "default_harness":"name-test"
        }
    });
    let modules = json!([{"module_id":"broca", "nonce_verified":true,
        "role_declarations":{"routing/v1":slot}}]);
    let operator = h.bind(reserved("callosum"), None).await;
    let preview_request = json!({"consumer":"preview", "iqs":[0], "runner":"broca",
        "runner_declarations":{"broca":slot},
        "head_choices":{"runner_module":"broca", "harness":"name-test"}});
    super::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        consumers::replace_catalog(&modules);
        let registry = consumers::effective(&h.operator_config());
        assert!(registry.knows("broca.task"));
        assert!(!registry.knows("foreign-task"));
        let runners = super::runners::declarations_from_catalog(&modules);
        assert_eq!(
            runners["broca"].harnesses(),
            Some(["name-test".to_string()].as_slice())
        );
        h.credentials().lock().unwrap().runner_declarations = runners;
        let owner = h.bind(reserved("broca"), None).await;
        let live = owner.call("route.select", &select("broca.task")).await;
        assert_eq!(live.0, FrameType::Response, "{live:?}");
        assert_eq!(live.1["result"]["runner"]["module"], "broca");
        // The proposed runner must be read from the overlay, not the live copy.
        h.credentials().lock().unwrap().runner_declarations.clear();
        let preview = operator.call("route.what_if", &preview_request).await;
        assert_eq!(preview.0, FrameType::Response, "{preview:?}");
        assert!(
            preview.1["result"]["results"][0]["pick"]["route"].is_string(),
            "{preview:?}"
        );
        assert!(
            !preview.1["result"]["head_choices"]["models"]
                .as_array()
                .unwrap()
                .is_empty(),
            "{preview:?}"
        );
        let unchanged = owner.call("route.select", &select("broca.task")).await;
        assert_eq!(unchanged.0, FrameType::Response, "{unchanged:?}");
        assert!(
            unchanged.1["result"]["runner"].get("harness").is_none(),
            "{unchanged:?}"
        );
        assert!(h
            .credentials()
            .lock()
            .unwrap()
            .runner_declarations
            .is_empty());
    })
    .await;
    super::describe::without_capability("consumer_declarations", async {
        assert!(!super::describe::capabilities().contains(&"consumer_declarations"));
        consumers::replace_catalog(&modules);
        assert!(!consumers::effective(&h.operator_config()).knows("broca.task"));
        assert!(super::runners::declarations_from_catalog(&modules).is_empty());
        let owner = h.bind(reserved("broca"), None).await;
        refused(
            owner.call("route.select", &select("broca.task")).await,
            "unknown_consumer",
            "broca.task",
        );
        let preview = operator.call("route.what_if", &preview_request).await;
        assert_eq!(preview.0, FrameType::Error, "{preview:?}");
        assert_eq!(preview.1["code"], "invalid_request");
        assert_eq!(preview.1["detail"]["field"], "runner_declarations.broca");
    })
    .await;
}

#[tokio::test]
async fn dotted_not_for_excludes_gather_but_keeps_plain_athena_and_records_the_full_name() {
    let harness = harness();
    harness.edit_config(|config| {
        config["consumers"]["athena"] = json!({});
        config["consumers"]["athena.gather"] = json!({});
        config["consumers"]["athena.gather.regather"] = json!({});
        config["models"]["example-provider/model-a"]["not_for"] = json!(["athena.gather"]);
    });
    let caller = harness.bind(reserved("prefrontal-core"), None).await;
    for consumer in ["athena", "athena.gather", "athena.gather.regather"] {
        let reply = caller
            .call(
                "route.select",
                &json!({
                    "consumer": consumer, "requirements": {"iq": 0}, "detail": true,
                    "available_routes": ["example-provider/model-a", "example-provider/model-b"],
                    "prefer": ["example-provider/model-a"]
                }),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let result = &reply.1["result"];
        assert_eq!(
            result["route"]["model"],
            if consumer == "athena" {
                "model-a"
            } else {
                "model-b"
            },
            "{}",
            reply.1
        );
        let id = result["decision_id"].as_str().unwrap();
        assert_eq!(
            harness.store().v1_decision(id).unwrap().unwrap().consumer,
            consumer
        );
        if consumer != "athena" {
            let rejected = result["detail"]["rejected"]
                .as_array()
                .unwrap()
                .iter()
                .find(|rejected| rejected["routeKey"] == "example-provider/model-a")
                .unwrap();
            assert_eq!(rejected["reason"], "excluded_by_operator");
            assert_eq!(
                rejected["detail"],
                "models[\"example-provider/model-a\"].not_for lists \"athena.gather\""
            );
        }
    }
}

#[tokio::test]
async fn dotted_only_for_admits_gather_and_refuses_plain_athena() {
    let harness = harness();
    harness.edit_config(|config| {
        config["consumers"]["athena"] = json!({});
        config["consumers"]["athena.gather"] = json!({});
        config["models"]["example-provider/model-a"]["only_for"] = json!(["athena.gather"]);
    });
    let caller = harness.bind(reserved("prefrontal-core"), None).await;
    for consumer in ["athena", "athena.gather"] {
        let reply = caller
            .call(
                "route.select",
                &json!({
                    "consumer": consumer, "requirements": {"iq": 0}, "detail": true,
                    "available_routes": ["example-provider/model-a"]
                }),
            )
            .await;
        if consumer == "athena.gather" {
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            assert_eq!(reply.1["result"]["route"]["model"], "model-a");
        } else {
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], "no_route");
            let rejected = reply.1["detail"]["rejected"]
                .as_array()
                .unwrap()
                .iter()
                .find(|rejected| rejected["routeKey"] == "example-provider/model-a")
                .unwrap();
            assert_eq!(rejected["reason"], "excluded_by_operator");
            assert_eq!(rejected["detail"],
                "models[\"example-provider/model-a\"].only_for [\"athena.gather\"] does not name \"athena\"");
        }
    }
}

#[tokio::test]
async fn dotted_unknown_root_keeps_unknown_consumer_selection_behavior() {
    let harness = harness();
    harness.edit_config(|config| {
        config["consumers"]["nosuch"] = json!({});
        config["consumers"]["nosuch.gather"] = json!({});
        config["models"]["example-provider/model-a"]["not_for"] = json!(["athena"]);
    });
    let caller = harness.bind(reserved("prefrontal-core"), None).await;
    for consumer in ["nosuch", "nosuch.gather"] {
        let reply = caller
            .call(
                "route.select",
                &json!({
                    "consumer": consumer, "requirements": {"iq": 0},
                    "available_routes": ["example-provider/model-a"]
                }),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["route"]["model"], "model-a");
    }
}

#[tokio::test]
async fn consumer_models_checks_declaring_delegated_configured_and_foreign_owners() {
    let harness = harness();
    consumers::replace_catalog(&json!([entry(
        "declaring-module",
        true,
        &["example.worker"]
    )]));
    for (module, consumer) in [
        ("declaring-module", "example.worker"),
        ("magic-context", "mc_dream_curate"),
    ] {
        let route = harness.bind(reserved(module), None).await;
        let reply = route
            .call("route.consumer_models", &json!({"consumer":consumer}))
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        let answer = serde_json::from_value(reply.1["result"].clone()).unwrap();
        cortexkit_role_routing::consumer_models::check_consumer_models(
            &cortexkit_role_routing::consumer_models::ConsumerModelsRequest::new(consumer),
            &answer,
        )
        .unwrap();
    }
    let delegate = scoped(
        &harness,
        "declaring-module",
        true,
        harness.reserve_channel(),
        1,
    )
    .await;
    assert_eq!(
        delegate
            .call(
                "route.consumer_models",
                &json!({"consumer":"example.worker"})
            )
            .await
            .0,
        FrameType::Response
    );
    let stranger = scoped(&harness, "stranger", true, harness.reserve_channel(), 1).await;
    refused(
        stranger
            .call(
                "route.consumer_models",
                &json!({"consumer":"example.worker"}),
            )
            .await,
        "consumer_not_owned",
        "example.worker",
    );
    refused(
        stranger
            .call("route.consumer_models", &json!({"consumer":"missing"}))
            .await,
        "unknown_consumer",
        "missing",
    );
    let extra = delegate
        .call(
            "route.consumer_models",
            &json!({"consumer":"example.worker","extra":1}),
        )
        .await;
    assert_eq!(extra.1["code"], "invalid_request");
    assert_eq!(extra.1["detail"]["field"], "extra");
}

#[tokio::test]
async fn consumer_models_preserves_frozen_fractional_scores_exclusions_and_subscription_inventory()
{
    let h = harness();
    let frozen: Value = serde_json::from_str(include_str!(
        "../../fixtures/replay/available-models.golden.json"
    ))
    .unwrap();
    let facts: Vec<striatum_core::ModelFact> =
        serde_json::from_value(frozen["catalog"].clone()).unwrap();
    h.install_facts(&facts);
    h.edit_config(|config| {
        config["models"]["p/excluded"] = json!({"pin_only":true});
        config["models"]["p/unscored"] = json!({"subscription":true});
    });
    let route = h.bind(reserved("prefrontal-core"), None).await;
    let reply = route
        .call("route.consumer_models", &json!({"consumer":"codemode"}))
        .await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let entries = reply.1["result"]["models"].as_array().unwrap();
    for name in ["fractional", "excluded", "unscored", "deprecated"] {
        let entry = entries
            .iter()
            .find(|m| m["provider"] == "p" && m["model"] == name)
            .unwrap();
        let fact = facts
            .iter()
            .find(|f| f.provider_id == "p" && f.model_id == name)
            .unwrap();
        assert_eq!(entry["elo"], json!(fact.scores.elo));
        assert_eq!(entry["eq"], json!(fact.scores.eq));
        assert_eq!(entry["excluded"], name == "excluded");
        assert_eq!(
            entry["billing"],
            if name == "unscored" {
                "subscription"
            } else {
                "per_token"
            }
        );
    }
    let stranger = h.bind(reserved("stranger"), None).await;
    assert_eq!(
        stranger
            .call(
                "route.consumer_models",
                &json!({"consumer":"codemode","project_root":"/injected"})
            )
            .await
            .1["detail"]["field"],
        "project_root"
    );
}

#[tokio::test]
async fn ownerless_config_consumer_is_selectable_by_unrelated_callers() {
    let harness = harness();
    let consumer = "example.public";
    harness.edit_config(|config| config["consumers"][consumer] = json!({}));
    for principal in [
        reserved("first-unrelated"),
        reserved("second-unrelated"),
        Value::Null,
    ] {
        let route = harness.bind(principal, None).await;
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    }

    let channel = harness.reserve_channel();
    let old_route = scoped(&harness, "unrelated-owner", false, channel, 1).await;
    let captured = Caller::for_route(channel, 1);
    let dispatched = old_route.request_frame("route.select", &select(consumer));
    scoped(&harness, "unrelated-owner", false, channel, 2).await;
    refused(
        harness.finish_dispatched(dispatched).await,
        "consumer_not_owned",
        consumer,
    );
    assert_eq!(
        consumers::effective(&harness.operator_config())
            .admit(consumer, &captured)
            .unwrap_err()
            .code
            .as_str(),
        "consumer_not_owned"
    );
}

fn refused(reply: (FrameType, Value), code: &str, consumer: &str) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], code, "{}", reply.1);
    assert_eq!(reply.1["detail"]["consumer"], consumer, "{}", reply.1);
}

async fn scoped(
    harness: &Arc<Harness>,
    owner: &str,
    authorized: bool,
    channel: u16,
    epoch: u32,
) -> Route {
    harness.bind_frame(json!({
        "op": "route.bind", "route_channel": channel, "epoch": epoch,
        "target": { "kind": "management_surface", "module_id": crate::DEFAULT_MODULE_ID },
        "identity": { "project_root": cortexkit_test_support::scratch_root(), "harness": "consumer-admission", "session": "session-1" },
        "principal": reserved("broca"),
        "scope": { "owner": reserved(owner), "ref": "codemode-run", "kind": "ephemeral", "scope_epoch": 7, "project_id": "project-1", "owner_authorized": authorized }
    }), channel, epoch).await
}

#[tokio::test]
async fn verified_declarer_and_configured_owner_are_admitted_without_a_project() {
    let harness = harness();
    consumers::replace_catalog(&json!([entry(
        "declaring-module",
        true,
        &["example.worker"]
    )]));
    for (module, consumer) in [
        ("declaring-module", "example.worker"),
        ("magic-context", "mc_dream_curate"),
    ] {
        let route = harness.bind(reserved(module), None).await;
        let reply = route.call("route.select", &select(consumer)).await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    }
    let stranger = harness.bind(reserved("unrelated"), Some("project-1")).await;
    refused(
        stranger
            .call("route.select", &select("mc_dream_curate"))
            .await,
        "consumer_not_owned",
        "mc_dream_curate",
    );
}

#[tokio::test]
async fn only_verified_catalog_declarations_create_ownership() {
    let harness = harness();
    let route = harness.bind(reserved("declaring-module"), None).await;
    consumers::replace_catalog(&json!([entry(
        "declaring-module",
        true,
        &["example.worker"]
    )]));
    assert_eq!(
        route
            .call("route.select", &select("example.worker"))
            .await
            .0,
        FrameType::Response
    );
    consumers::replace_catalog(&json!([entry(
        "declaring-module",
        false,
        &["example.worker"]
    )]));
    refused(
        route.call("route.select", &select("example.worker")).await,
        "unknown_consumer",
        "example.worker",
    );
    let description = route.call("role.describe", &json!({})).await;
    assert!(!description.1["result"]["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["id"] == "example.worker"));
    consumers::replace_catalog(&json!([]));
    refused(
        route.call("route.select", &select("example.worker")).await,
        "unknown_consumer",
        "example.worker",
    );
}

#[tokio::test]
async fn different_verified_owners_lock_the_name_in_both_catalog_orders() {
    let harness = harness();
    let first = entry("first-module", true, &["example.worker"]);
    let second = entry("second-module", true, &["example.worker"]);
    for catalog in [json!([first, second]), json!([second, first])] {
        consumers::replace_catalog(&catalog);
        for module in ["first-module", "second-module"] {
            let route = harness.bind(reserved(module), None).await;
            refused(
                route.call("route.select", &select("example.worker")).await,
                "consumer_not_owned",
                "example.worker",
            );
            let description = route.call("role.describe", &json!({})).await;
            assert!(!description.1["result"]["consumers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["id"] == "example.worker"));
        }
    }
}

#[tokio::test]
async fn configured_owners_join_the_declaration_lock_without_overriding_it() {
    let harness = harness();
    let owner = harness.bind(reserved("magic-context"), None).await;
    consumers::replace_catalog(&json!([entry("magic-context", true, &["mc_dream_curate"])]));
    assert_eq!(
        owner
            .call("route.select", &select("mc_dream_curate"))
            .await
            .0,
        FrameType::Response
    );
    consumers::replace_catalog(&json!([entry("other-module", true, &["mc_dream_curate"])]));
    refused(
        owner.call("route.select", &select("mc_dream_curate")).await,
        "consumer_not_owned",
        "mc_dream_curate",
    );
    let other = harness.bind(reserved("other-module"), None).await;
    refused(
        other.call("route.select", &select("mc_dream_curate")).await,
        "consumer_not_owned",
        "mc_dream_curate",
    );
}

#[tokio::test]
async fn delegated_select_requires_current_owner_authorized_scope_not_project_equality() {
    let harness = harness();
    let channel = harness.reserve_channel();
    let route = scoped(&harness, "prefrontal-core", true, channel, 1).await;
    let authority = Caller::for_route(channel, 1).scope_authority.unwrap();
    assert_eq!(authority.owner, "reserved:prefrontal-core");
    assert_eq!(authority.reference, "codemode-run");
    assert_eq!(serde_json::to_value(authority.kind).unwrap(), "ephemeral");
    assert_eq!(authority.scope_epoch, 7);
    assert_eq!(authority.project_id.as_deref(), Some("project-1"));
    assert!(authority.owner_authorized);
    assert_eq!(
        route.call("route.select", &select("codemode")).await.0,
        FrameType::Response
    );
    for (owner, authorized, epoch) in [("prefrontal-core", false, 2), ("other-module", true, 3)] {
        let route = scoped(&harness, owner, authorized, channel, epoch).await;
        refused(
            route.call("route.select", &select("codemode")).await,
            "consumer_not_owned",
            "codemode",
        );
    }
    let unscoped = harness.bind(reserved("broca"), None).await;
    refused(
        unscoped.call("route.select", &select("codemode")).await,
        "consumer_not_owned",
        "codemode",
    );
    // The transport drops stale frames; the ownership boundary must also deny
    // a stale epoch if a request was already dispatched when the channel rebound.
    let config = harness.operator_config();
    let registry = consumers::effective(&config);
    let stale = Caller::for_route(channel, 1);
    assert!(stale.scope_authority.is_none());
    assert_eq!(
        registry
            .admit("codemode", &stale)
            .unwrap_err()
            .code
            .as_str(),
        "consumer_not_owned"
    );
    crate::clear_route_scope(channel, 1);
    assert!(Caller::for_route(channel, 3).scope_authority.is_some());
    crate::clear_route_scope(channel, 3);
    assert!(Caller::for_route(channel, 3).scope_authority.is_none());
}

#[tokio::test]
async fn legacy_core_selection_stays_open_but_non_core_cannot_borrow_target_agent_authority() {
    let harness = harness();
    let route = harness.bind(reserved("unrelated"), None).await;
    let core = route
        .call(
            "route.select",
            &json!({"targetAgent": "codemode", "executor":"broca", "substrate":"broca", "requirements": {"iq": 30, "eq": 0}, "availableModels": ["example-provider/model-a"]}),
        )
        .await;
    assert_eq!(core.0, FrameType::Response, "{}", core.1);
    for params in [
        json!({"targetAgent": "mc_dream_curate", "requirements": {"iq": 30, "eq": 0}}),
        json!({"targetAgent": "mason", "consumer": "mc_dream_curate", "requirements": {"iq": 30, "eq": 0}, "owner": "magic-context", "project_id": "project-1"}),
    ] {
        refused(
            route.call("route.select", &params).await,
            "consumer_not_owned",
            "mc_dream_curate",
        );
    }
}

#[tokio::test]
async fn legacy_explore_admission_preserves_the_selected_route_without_a_consumer_group() {
    let harness = harness();
    let caller = harness.bind(reserved("unrelated"), None).await;
    let request = json!({"targetAgent": "explore", "requirements": {"iq": 30, "eq": 0}, "availableModels": ["example-provider/model-a"]});
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        reply.1["result"]["selected"]["routeKey"],
        "example-provider/model-a"
    );
    let mut canonical = request;
    canonical["targetAgent"] = json!("mason");
    let baseline = caller.call("route.select", &canonical).await;
    assert_eq!(baseline.0, FrameType::Response, "{}", baseline.1);
    assert_eq!(
        baseline.1["result"]["selected"]["routeKey"],
        reply.1["result"]["selected"]["routeKey"]
    );
}

#[tokio::test]
async fn an_unregistered_legacy_target_label_has_no_core_admission_exception() {
    let harness = harness();
    let caller = harness.bind(reserved("unrelated"), None).await;
    let request = json!({"targetAgent": "unregistered-label", "requirements": {"iq": 30, "eq": 0}, "availableModels": ["example-provider/model-a"]});
    refused(
        caller.call("route.select", &request).await,
        "unknown_consumer",
        "unregistered-label",
    );
    harness.edit_config(|config| {
        config["consumers"]["unregistered-label"] = json!({"owner": "consumer-owner"})
    });
    refused(
        caller.call("route.select", &request).await,
        "consumer_not_owned",
        "unregistered-label",
    );
    let owner = harness.bind(reserved("consumer-owner"), None).await;
    let reply = owner.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

#[tokio::test]
async fn a_report_cannot_reuse_a_delegates_decision_without_owner_authorization() {
    let harness = harness();
    let channel = harness.reserve_channel();
    let route = scoped(&harness, "prefrontal-core", true, channel, 1).await;
    let selected = route.call("route.select", &select("codemode")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let report =
        json!({"decision_id": selected.1["result"]["decision_id"], "outcome": "completed"});
    let unscoped = harness.bind(reserved("broca"), None).await;
    refused(
        unscoped.call("route.report", &report).await,
        "consumer_not_owned",
        "codemode",
    );
    let reply = route.call("route.report", &report).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

#[tokio::test]
async fn a_dispatched_select_loses_authority_when_the_route_rebinds_before_admission() {
    let harness = harness();
    let channel = harness.reserve_channel();
    let old_route = scoped(&harness, "prefrontal-core", true, channel, 1).await;
    let captured_caller = Caller::for_route(channel, 1);
    let dispatched = old_route.request_frame("route.select", &select("codemode"));
    let current_route = scoped(&harness, "prefrontal-core", true, channel, 2).await;
    assert_eq!(
        consumers::effective(&harness.operator_config())
            .admit("codemode", &captured_caller)
            .unwrap_err()
            .code
            .as_str(),
        "consumer_not_owned"
    );
    refused(
        harness.finish_dispatched(dispatched).await,
        "consumer_not_owned",
        "codemode",
    );
    assert_eq!(
        current_route
            .call("route.select", &select("codemode"))
            .await
            .0,
        FrameType::Response
    );
}

#[test]
fn body_claims_and_unattested_principals_never_grant_configured_ownership() {
    let harness = harness();
    let registry = consumers::effective(&harness.operator_config());
    for principal in [
        "direct",
        "unverified",
        "unattested",
        "reserved:",
        "reserved:unrelated",
    ] {
        let caller = Caller {
            binding: None,
            principal: principal.into(),
            scope: None,
            scope_authority: None,
        };
        assert_eq!(
            registry
                .admit("mc_dream_curate", &caller)
                .unwrap_err()
                .code
                .as_str(),
            "consumer_not_owned"
        );
    }
}

#[tokio::test]
async fn fractional_operator_score_targets_do_not_cross_the_integer_wire_validator() {
    let harness = harness();
    harness.edit_config(|config| {
        config["consumers"]["codemode"]["iq"] = json!(30.5);
        config["consumers"]["codemode"]["eq"] = json!(20.5);
    });
    let route = harness.bind(reserved("prefrontal-core"), None).await;
    let reply = route.call("route.select", &select("codemode")).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

#[test]
fn declaration_defaults_inherit_within_the_declaring_module_and_requests_override_them() {
    let catalog = consumers::Catalog::from_catalog(&json!([
        {"module_id": "module-a", "nonce_verified": true, "role_declarations": {"routing/v1": {"consumers": [
             {"name": "example", "eq": 40, "context_window": 32000, "max_output_tokens":4096},
             {"name": "example.worker", "eq": 50, "tools": "required", "max_output_tokens":8192}
        ]}}},
        entry("module-b", true, &["example.foreign"])
    ]));
    let harness = harness();
    let config = harness.operator_config();
    let registry = catalog.effective(&config);
    let demands = registry.with_defaults(
        "example.worker",
        &json!({"requirements": {"iq": 30, "eq": 0, "max_output_tokens":2048}}),
        &config,
    );
    assert_eq!(
        demands["requirements"],
        json!({"iq": 30, "eq": 0, "context_window": 32000, "tools": "required", "max_output_tokens":2048})
    );
    let foreign = registry.with_defaults(
        "example.foreign",
        &json!({"requirements": {"iq": 30}}),
        &config,
    );
    assert_eq!(foreign["requirements"], json!({"iq": 30}));
    let declared = registry.with_defaults(
        "example.worker",
        &json!({"requirements":{"iq":30}}),
        &config,
    );
    assert_eq!(declared["requirements"]["max_output_tokens"], 8192);
    harness.edit_config(|config| config["consumers"]["example"]["max_output_tokens"] = json!(1024));
    let operator = registry.with_defaults(
        "example.worker",
        &json!({"requirements":{"iq":30,"max_output_tokens":2048}}),
        &harness.operator_config(),
    );
    assert_eq!(operator["requirements"]["max_output_tokens"], 1024);
}

#[test]
fn catalog_wire_decodes_routing_declarations_with_the_pinned_protocol() {
    use serde::de::DeserializeOwned;
    use std::future::Future;
    use subc_client_rs::SubcConsumer;

    // Infer the actual SDK result type rather than mirroring its catalog schema.
    // The callback is never invoked: this test decodes the wire without a daemon.
    fn decode<T: DeserializeOwned, E, Fut: Future<Output = Result<T, E>>>(
        _catalog_list: impl FnOnce(SubcConsumer) -> Fut,
        bytes: &[u8],
    ) -> T {
        serde_json::from_slice(bytes).unwrap()
    }
    let mut module = entry("declaring-module", true, &["example.worker"]);
    module["roles"] = json!([]);
    module["control_ops"] = json!([]);
    module["generation"] = json!(1);
    let wire = json!({"modules": [module], "generation": 1});
    let catalog = decode(
        |consumer| async move { consumer.catalog_list().await },
        &serde_json::to_vec(&wire).unwrap(),
    );
    let modules = serde_json::to_value(catalog.modules).unwrap();
    let slot = &modules[0]["role_declarations"]["routing/v1"];
    let declarations =
        cortexkit_role_routing::declarations::parse_consumer_declarations(slot).unwrap();
    assert_eq!(declarations.consumers[0].name, "example.worker");
    assert_eq!(modules[0]["nonce_verified"], true);
    assert_eq!(modules[0]["module_id"], "declaring-module");
}

#[tokio::test]
async fn consumer_alias_admission_does_not_bypass_module_ownership() {
    let h = harness();
    consumers::replace_catalog(&json!([entry(
        "prefrontal-core",
        true,
        &["prefrontal-core.sidekick"]
    )]));
    h.edit_config(|config| {
        config["consumers"] = json!({"sidekick": {}});
    });
    let owner = h.bind(reserved("prefrontal-core"), None).await;
    let stranger = h.bind(reserved("another-module"), None).await;
    for consumer in [
        "sidekick",
        "prefrontal-core.sidekick",
        "sidekick.explore",
        "prefrontal-core.sidekick.explore",
    ] {
        assert_eq!(
            owner.call("route.select", &select(consumer)).await.0,
            FrameType::Response
        );
        let (ty, reply) = stranger.call("route.select", &select(consumer)).await;
        assert_eq!(ty, FrameType::Error, "{reply}");
        assert_eq!(reply["code"], "consumer_not_owned", "{reply}");
    }
}

#[test]
fn consumer_alias_ownership_conflict_warns_once_at_catalog_load() {
    let _h = harness();
    let catalog = json!([
        entry("legacy-alias-warning-owner", true, &["flow"]),
        entry("basal", true, &["basal.flow"]),
    ]);
    let expected = "consumer alias ownership conflict: \"flow\" declared by \"legacy-alias-warning-owner\" and \"basal.flow\" declared by \"basal\"";
    crate::take_operator_warnings();
    consumers::replace_catalog(&catalog);
    let issues = consumers::health()["issues"].as_array().unwrap().clone();
    assert_eq!(
        issues
            .iter()
            .filter(|issue| issue.as_str() == Some(expected))
            .count(),
        1
    );
    let warnings = crate::take_operator_warnings();
    assert_eq!(
        warnings
            .iter()
            .filter(|warning| warning.contains(expected))
            .count(),
        1
    );
    consumers::replace_catalog(&catalog);
    assert!(crate::take_operator_warnings().is_empty());

    let same_owner = json!([entry("basal", true, &["flow", "basal.flow"])]);
    consumers::replace_catalog(&same_owner);
    assert!(!consumers::health()["issues"]
        .as_array()
        .unwrap()
        .iter()
        .any(|issue| issue
            .as_str()
            .is_some_and(|issue| issue.contains("consumer alias ownership conflict"))));
}

#[tokio::test]
async fn consumer_priority_inherits_single_owner_and_operator_overrides_by_segment() {
    use cortexkit_role_routing::declarations::ConsumerPriority::*;
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let h = harness();
    let modules = json!([
        {"module_id":"basal","nonce_verified":true,"role_declarations":{"routing/v1":{"consumers":[
            {"name":"basal","priority":"interactive"},
            {"name":"basal.block","priority":"blocking"},
            {"name":"basal.block.deep","priority":"background"},
            {"name":"basal.worker","priority":"worker"},
            {"name":"legacy-task","priority":"blocking","eq":23},
            {"name":"shared","priority":"interactive"}
        ]}}},
        {"module_id":"other","nonce_verified":true,"role_declarations":{"routing/v1":{"consumers":[
            {"name":"shared","priority":"background"},
            {"name":"basal.foreign"}
        ]}}}
    ]);
    let catalog = consumers::Catalog::from_catalog(&modules);
    let registry = catalog.effective(&h.operator_config());
    for (name, expected) in [
        ("basal", Interactive),
        ("basal.child", Interactive),
        ("basal.block.child", Blocking),
        ("basal.block.deep.child", Background),
        ("basal.worker", Worker),
        ("basal.foreign.child", Worker),
        ("shared", Worker),
        ("shared.child", Worker),
        ("unknown", Worker),
        ("basal_block", Worker),
        ("legacy-task.child", Blocking),
    ] {
        assert_eq!(
            registry.defaults_for(name).priority,
            Some(expected),
            "{name}"
        );
    }
    // Scheduling metadata must not discard a transitional name's demand or
    // ownership: those names remain usable until their callers are renamed.
    assert_eq!(registry.defaults_for("legacy-task").eq, Some(23));
    consumers::replace_catalog(&modules);
    let owner = h.bind(reserved("basal"), None).await;
    let stranger = h.bind(reserved("other"), None).await;
    assert_eq!(
        owner.call("route.select", &select("legacy-task")).await.0,
        FrameType::Response
    );
    admission_refused(
        stranger.call("route.select", &select("legacy-task")).await,
        "consumer_not_owned",
        "legacy-task",
    );
    h.edit_config(|config| {
        config["consumer_priorities"] = json!({
            "basal":"background", "basal.block":"interactive",
            "shared":"blocking", "unknown":"interactive"
        });
    });
    let registry = catalog.effective(&h.operator_config());
    for (name, expected) in [
        ("basal", Background),
        ("basal.worker", Background),
        ("basal.block.deep.child", Interactive),
        ("basal.blocked", Background),
        ("basal_block", Worker),
        ("shared.child", Blocking),
        ("unknown.child", Interactive),
        ("unknownish", Worker),
    ] {
        assert_eq!(
            registry.defaults_for(name).priority,
            Some(expected),
            "{name}"
        );
    }
}
