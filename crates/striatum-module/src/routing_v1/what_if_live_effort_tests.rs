use super::*;

const LUNA: &str = "example-provider/gpt-6-luna";
const SOL: &str = "example-provider/gpt-6.2-sol";
const TERRA: &str = "example-provider/gpt-6.1-terra";
const ASTRA: &str = "example-provider/gpt-6.2-astra";
const KIMI: &str = "example-provider/kimi-k3.1-pro";
const CARBON: &str = "example-provider/gemini-4-carbon";
const GROK: &str = "example-provider/grok-4.7";
const OPUS: &str = "example-provider/claude-opus-5-5";
const SIDEKICK: &str = "prefrontal.sidekick";
const ATHENA: &str = "prefrontal.athena";
const GATHER: &str = "prefrontal.athena.gather";
const CHAIR: &str = "prefrontal.athena.synthesizer";
const MASON: &str = "prefrontal.mason";
const CODEMODE: &str = "basal.codemode";
const FLOW: &str = "basal.flow";

async fn approved() -> (std::sync::Arc<Harness>, Route) {
    let (harness, route) = configured().await;
    let owner = fixture::CONSUMER.trim_start_matches("reserved:");
    let facts = harness.store().get_facts().unwrap();
    let template = facts
        .iter()
        .find(|fact| fact.model_id == "model-a")
        .unwrap();
    // Use the harness's existing provider/account; the distinctions under test
    // are the model's catalog facts, not credential availability.
    for model in [LUNA, SOL, TERRA, ASTRA, KIMI, CARBON, GROK, OPUS] {
        let mut fact = template.clone();
        fact.model_id = model.split_once('/').unwrap().1.into();
        fact.family = Some(fact.model_id.clone());
        harness
            .store()
            .upsert_fact(&fact, striatum_core::FactSource::Override)
            .unwrap();
    }
    serve_efforts(&harness, |model, row| {
        let graded = match model {
            LUNA | SOL | TERRA | ASTRA | GROK => "xhigh",
            CARBON | OPUS => "high",
            KIMI => "max",
            _ => return,
        };
        row["quality.graded_effort"] = json!(graded);
        match model {
            LUNA => row["workload.lookup.effort_default"] = json!("high"),
            SOL | TERRA | CARBON => row["workload.lookup.excluded"] = json!(true),
            ASTRA | KIMI => row["workload.reason.effort_default"] = json!("max"),
            GROK => row["workload.lookup.effort_default"] = json!("low"),
            _ => {}
        }
        if model == ASTRA {
            row["quality.elo.effort.max"] = json!(2400);
        }
        if model == KIMI {
            row["quality.elo.effort.max"] = json!(2300);
        }
        if matches!(model, TERRA | SOL | OPUS | GROK) {
            row["workload.code.effort_default"] = json!(graded);
        }
    });
    let offered: Vec<_> = harness
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .filter(|fact| {
            [fixture::PRICED_PROVIDER, fixture::SUBSCRIPTION_PROVIDER]
                .contains(&fact.provider_id.as_str())
        })
        .collect();
    crate::routing_v1::conformance_tests::publish_fixture_models(
        &mut harness.credentials().lock().unwrap(),
        "broca",
        &offered,
        true,
    );
    harness.edit_config(|c| {
        for (id, workload) in [
            (SIDEKICK, "lookup"),
            (ATHENA, "reason"),
            (GATHER, "lookup"),
            (CHAIR, "synthesize"),
            (MASON, "code"),
            (CODEMODE, "quick"),
            (FLOW, "quick"),
        ] {
            c["consumers"][id] = json!({"owner":owner,"workload":workload});
        }
        c["consumers"][CHAIR]["pin"] = json!({"model":OPUS,"variant":"medium"});
        c["consumers"][fixture::SUMMARY]["mode"] = json!("ladder");
        c["consumers"][fixture::SUMMARY]["ladder"] = json!([format!("{OPUS}:medium")]);
        for (model, elo) in [
            (LUNA, 1800),
            (SOL, 2200),
            (TERRA, 2100),
            (ASTRA, 1850),
            (KIMI, 1850),
            (CARBON, 2000),
            (GROK, 1900),
            (OPUS, 1900),
        ] {
            c["models"][model] = json!({"elo":elo,"eq":elo,"pin_only":false});
        }
    });
    (harness, route)
}

async fn live(route: &Route, operation: &str, body: Value) -> Value {
    let (ty, response) = route.call(operation, &body).await;
    assert_eq!(ty, FrameType::Response, "{response}");
    response["result"].clone()
}

async fn pick(route: &Route, consumer: &str, iq: u8, available: &[&str]) -> Value {
    live(
        route,
        "route.select",
        json!({"consumer":consumer,"requirements":{"iq":iq},
        "available_routes":available,"detail":true}),
    )
    .await
}

fn assert_route(answer: &Value, model: &str, variant: &str, source: &str) {
    let (provider, model) = model.split_once('/').unwrap();
    assert_eq!(
        answer["route"],
        json!({"provider":provider,"model":model,"variant":variant}),
        "{answer}"
    );
    assert_eq!(
        answer["detail"]["reasoning"]["effort_source"], source,
        "{answer}"
    );
}

#[tokio::test]
async fn approved_lookup_rows_use_luna_and_exclude_sol_terra_and_carbon() {
    let (_harness, route) = approved().await;
    for (consumer, iq, available) in [
        (SIDEKICK, 30, vec![LUNA]),
        (SIDEKICK, 60, vec![LUNA, TERRA]),
        (SIDEKICK, 90, vec![LUNA, SOL]),
        (GATHER, 30, vec![LUNA]),
        (GATHER, 50, vec![LUNA, TERRA]),
        (GATHER, 80, vec![LUNA, CARBON]),
    ] {
        let selected = pick(&route, consumer, iq, &available).await;
        assert_route(&selected, LUNA, "high", "workload_default");
        let explained = live(
            &route,
            "route.explain",
            json!({"decision_id":selected["decision_id"]}),
        )
        .await;
        assert_eq!(
            explained["effort_source"], "workload_default",
            "{explained}"
        );
        assert_eq!(explained["grade"]["elo_effort"], "xhigh", "{explained}");
        if let Some(excluded) = available.get(1) {
            assert!(
                explained["workload_exclusions"]
                    .as_array()
                    .unwrap()
                    .contains(
                        &json!({"route":excluded,"reason":"workload_excluded","workload":"lookup"})
                    ),
                "{explained}"
            );
        }
    }
    let selected = pick(&route, GATHER, 100, &[CARBON, GROK, LUNA]).await;
    assert_route(&selected, GROK, "low", "workload_default");
    assert_eq!(
        selected["detail"]["reasoning"]["grade"]["elo_effort"],
        "xhigh"
    );
}

#[tokio::test]
async fn approved_quick_rows_use_terras_graded_effort() {
    let (_harness, route) = approved().await;
    for consumer in [CODEMODE, FLOW] {
        let selected = pick(&route, consumer, 65, &[TERRA]).await;
        assert_route(&selected, TERRA, "xhigh", "graded");
        assert_eq!(
            selected["detail"]["reasoning"]["grade"]["elo_effort"],
            "xhigh"
        );
    }
}

#[tokio::test]
async fn approved_reason_panels_use_astra_and_kimi_at_max() {
    let (harness, route) = approved().await;
    harness.edit_config(|c| {
        for class in [
            "audit",
            "spec",
            "diagnose",
            "design_review",
            "evaluate",
            "explain",
        ] {
            let top = match class {
                "explain" => 1,
                "design_review" | "evaluate" => 3,
                _ => 2,
            };
            c["panels"][class] = json!({"top_k":top,"by":"iq","extra":0,"extra_by":"random"});
        }
    });
    for class in [
        "audit",
        "spec",
        "diagnose",
        "design_review",
        "evaluate",
        "explain",
    ] {
        let routes = match class {
            "explain" => vec![CARBON],
            "design_review" | "evaluate" => vec![ASTRA, CARBON, KIMI],
            _ => vec![ASTRA, CARBON],
        };
        let answer = live(
            &route,
            "route.select_panel",
            json!({"class":class,
            "consumer":format!("{ATHENA}.{class}"),"available_routes":routes}),
        )
        .await;
        let seats = answer["seats"].as_array().unwrap();
        assert_eq!(seats.len(), routes.len(), "{class}: {answer}");
        for (index, model) in routes.iter().enumerate() {
            let variant = if *model == CARBON { "high" } else { "max" };
            let (provider, name) = model.split_once('/').unwrap();
            let seat = seats
                .iter()
                .find(|seat| seat["answer"]["route"]["model"] == name)
                .unwrap();
            assert_eq!(
                seat["answer"]["route"],
                json!({"provider":provider,"model":name,"variant":variant}),
                "{class}: {answer}"
            );
            if index == 0 && *model == ASTRA {
                assert_eq!(
                    seats[0]["answer"]["route"]["model"], name,
                    "the measured max grade must outrank Carbon: {answer}"
                );
            }
            let explained = live(
                &route,
                "route.explain",
                json!({"decision_id":seat["answer"]["decision_id"]}),
            )
            .await;
            assert_eq!(
                explained["effort_source"],
                if *model == CARBON {
                    "graded"
                } else {
                    "workload_default"
                }
            );
            assert_eq!(explained["grade"]["elo_effort"], variant);
        }
    }
}

#[tokio::test]
async fn replacement_excludes_the_served_effort_of_a_base_catalog_row() {
    let (harness, route) = approved().await;
    harness.edit_config(|c| {
        c["panels"]["audit"] = json!({"top_k":1,"by":"iq","extra":0,"extra_by":"random"});
    });
    let body = json!({"class":"audit","consumer":format!("{ATHENA}.audit"),
        "available_routes":[ASTRA, CARBON]});
    let original = live(&route, "route.select_panel", body.clone()).await;
    let seated = &original["seats"][0]["answer"]["route"];
    assert_eq!(seated["model"], "gpt-6.2-astra");
    assert_eq!(seated["variant"], "max");
    let replace = json!({"replace":{"panel_id":original["panel_id"],"slot":0}});
    let first = live(&route, "route.select_panel", replace.clone()).await;
    assert_eq!(
        first["seat"]["answer"]["route"]["model"], "gemini-4-carbon",
        "{first}"
    );
    let second = live(&route, "route.select_panel", replace).await;
    assert!(second["seat"].is_null(), "{second}");

    harness.edit_config(|c| {
        c["consumers"][format!("{ATHENA}.audit")]["workload"] = json!("code");
    });
    let different = live(
        &route,
        "route.select_panel",
        json!({"replace":{
            "panel_id":original["panel_id"],"slot":0
        }}),
    )
    .await;
    assert_eq!(
        different["seat"]["answer"]["route"]["model"], "gpt-6.2-astra",
        "{different}"
    );
    assert_eq!(different["seat"]["answer"]["route"]["variant"], "xhigh");

    let mut excluded = body;
    excluded["exclude"] = json!([ASTRA]);
    let answer = live(&route, "route.select_panel", excluded).await;
    assert_eq!(
        answer["seats"][0]["answer"]["route"]["model"],
        "gemini-4-carbon"
    );
}

#[tokio::test]
async fn approved_masons_chair_and_summary_keep_their_efforts() {
    let (_harness, route) = approved().await;
    for (model, variant) in [
        (TERRA, "xhigh"),
        (SOL, "xhigh"),
        (OPUS, "high"),
        (GROK, "xhigh"),
    ] {
        let selected = pick(&route, MASON, 40, &[model]).await;
        assert_route(&selected, model, variant, "workload_default");
    }
    for consumer in [CHAIR, fixture::SUMMARY] {
        let selected = pick(&route, consumer, 40, &[OPUS]).await;
        assert_route(&selected, OPUS, "medium", "pin");
    }
}

#[tokio::test]
async fn approved_head_picker_uses_graded_not_workload_effort() {
    let (_harness, route) = approved().await;
    let answer = live(
        &route,
        "route.head_choices",
        json!({"runner_module":"broca"}),
    )
    .await;
    let luna = answer["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["model"] == "gpt-6-luna")
        .unwrap();
    assert_eq!(luna["default_variant"], "xhigh", "{luna}");
}

#[tokio::test]
async fn removed_effort_source_is_not_a_what_if_member() {
    let (harness, _) = configured().await;
    let direct = operator(&harness).await;
    let (ty, answer) = direct
        .call(
            "route.what_if",
            &json!({"consumer":fixture::SUMMARY,
        "iqs":[0],"effort_source":"catalog"}),
        )
        .await;
    assert_eq!(ty, FrameType::Error, "{answer}");
    assert_eq!(answer["code"], "invalid_request", "{answer}");
    assert!(
        answer["message"]
            .as_str()
            .unwrap()
            .contains("unknown field `effort_source`"),
        "{answer}"
    );
}
