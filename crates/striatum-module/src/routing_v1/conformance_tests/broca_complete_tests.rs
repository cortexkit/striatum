use super::*;
use cortexkit_role_routing::price::RatesPerMtok;
use cortexkit_role_routing_conformance::SelectionFacts;

// Broca's llm_complete sends this body with a dollar cap even when subscriptions are allowed.
const BODY: &[u8] = br#"{"consumer":"broca.complete.basal.flow","requirements":{"iq":60,"eq":20,"max_output_tokens":500,"input_tokens":535,"max_cost_usd":0.5},"runners":["broca"]}"#;

const NOW: i64 = 1_800_000_000_000;
const METERED: &str = "example-provider/model-b";
const SUBSCRIPTION: &str = "example-subscription/model-c";
const UNPRICEABLE: [&str; 3] = [
    "example-provider/model-a",
    SUBSCRIPTION,
    "example-subscription/model-d",
];

fn setup() -> (Arc<Harness>, Catalog, SelectionFacts) {
    let harness = Harness::new("broca-complete");
    let mut catalog = fixture::catalog();
    catalog.runners.truncate(1);
    catalog.runners[0].module = "broca".into();
    catalog.runners[0].harnesses = Some(vec!["broca".into()]);
    catalog.runners[0].default_harness = Some("broca".into());
    catalog.runners[0].route_keys = catalog
        .models
        .iter()
        .map(|model| model.route_key())
        .collect();
    assert!(catalog.models.iter().any(|model| !model.priced_per_token));
    assert!(catalog.models.iter().any(|model| model.priced_per_token));
    assert!(catalog.models.iter().any(|model| !model.priced_per_token
        && catalog
            .accounts
            .iter()
            .any(|account| account.provider == model.provider && account.method == "oauth")));
    install_catalog(&harness, &catalog);
    harness.edit_config(|config| {
        config["consumers"] = json!({"broca.complete":{"owner":"broca"}});
    });
    let mut facts = SelectionFacts::new();
    for model in &catalog.models {
        facts
            .list_rates
            .insert(model.route_key(), RatesPerMtok::new(4.into(), 20.into()));
    }
    selection_facts(&harness, &facts);
    let models: serde_json::Map<String, Value> = catalog
        .models
        .iter()
        .map(|model| {
            (
                model.route_key(),
                json!({
                    "existence":"present",
                    "rate.input":{"state":"priced","currency":"USD","units":4,"exponent":0},
                    "rate.output":{"state":"priced","currency":"USD","units":20,"exponent":0}
                }),
            )
        })
        .collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({
            "catalog_version":1,"resolved_at_ms":1,"models":models
        }))
        .unwrap(),
    )
    .unwrap();
    harness.credentials().lock().unwrap().catalog_snapshot = Some(snapshot);
    (harness, catalog, facts)
}

// Pin the pre-filter uncapped reply bytes. The test clock fixes expiry and
// replay supplies the sequential decision IDs.
fn uncapped_bytes(id: u8, key: &str, context: u32, output: u32) -> String {
    let (provider, model) = key.split_once('/').unwrap();
    let price = if key == METERED {
        r#","price":{"basis":"metered","rates_per_mtok":{"input":4,"output":20}}"#
    } else {
        ""
    };
    format!(
        concat!(
            r#"{{"result":{{"context_window":{context},"decision_id":"dec_{id:012}","expires_at_ms":1800000600000,"#,
            r#""explanation":"{key} on broca for consumer broca.complete.basal.flow, chosen by the lowest-cost route that meets the requirements.","#,
            r#""max_output_tokens":{output}{price},"route":{{"model":"{model}","provider":"{provider}"}},"#,
            r#""runner":{{"harness":"broca","model":{{"model":"{model}","provider":"{provider}"}},"module":"broca"}}}}}}"#,
        ),
        context = context,
        id = id,
        key = key,
        output = output,
        price = price,
        model = model,
        provider = provider
    )
}

#[tokio::test]
async fn broca_complete_child_request_reaches_selection() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _ids = striatum_store::replay::Inputs::new(NOW).install();
    let (harness, catalog, mut facts) = setup();
    let route = harness.bind(reserved("broca"), None).await;
    let mut uncapped: Value = serde_json::from_slice(BODY).unwrap();
    uncapped["requirements"]
        .as_object_mut()
        .unwrap()
        .remove("max_cost_usd");
    let (ty, mixed) = route.call_raw("route.select", &uncapped).await;
    assert_eq!(ty, FrameType::Response);
    assert_eq!(
        String::from_utf8(mixed).unwrap(),
        uncapped_bytes(0, METERED, 128000, 16000)
    );
    for (id, key, context, output) in [
        (1, UNPRICEABLE[0], 200000, 32000),
        (2, METERED, 128000, 16000),
        (3, SUBSCRIPTION, 200000, 64000),
        (4, UNPRICEABLE[2], 100000, 8000),
    ] {
        harness.edit_config(|config| config["runners"][0]["routes"] = json!([key]));
        let (ty, baseline) = route.call_raw("route.select", &uncapped).await;
        assert_eq!(ty, FrameType::Response, "uncapped {key}");
        assert_eq!(
            String::from_utf8(baseline).unwrap(),
            uncapped_bytes(id, key, context, output)
        );
    }
    harness.edit_config(|config| {
        config["runners"][0]["routes"] = json!(catalog.runners[0].route_keys)
    });

    let (ty, bytes) = route.call_raw_params("route.select", BODY).await;
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(ty, FrameType::Response, "{reply}");
    let answer = &reply["result"];
    assert_eq!(answer["runner"]["module"], "broca", "{reply}");
    assert!(
        answer["decision_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "{reply}"
    );
    assert!(answer.get("price").is_some(), "{reply}");
    assert_eq!(
        answer["route"],
        json!({"provider":"example-provider", "model":"model-b"}),
        "{reply}"
    );
    assert_eq!(answer["price"]["basis"], "metered", "{reply}");

    let mut structured: Value = serde_json::from_slice(BODY).unwrap();
    structured["requirements"]["structured_output"] = json!(true);
    let structured = route.call("route.select", &structured).await;
    if structured.0 == FrameType::Error {
        assert_eq!(structured.1["code"], "no_route", "{:?}", structured);
        assert_eq!(
            structured.1["detail"]["reason"], "missing_capability",
            "{:?}",
            structured
        );
    } else {
        assert_eq!(structured.0, FrameType::Response, "{:?}", structured);
        assert!(
            structured.1["result"].get("price").is_some(),
            "{:?}",
            structured
        );
    }

    let description = route.call("role.describe", &json!({})).await;
    assert_eq!(description.0, FrameType::Response, "{:?}", description);
    let consumer = description.1["result"]["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|consumer| consumer["id"] == "broca.complete")
        .unwrap();
    assert_eq!(consumer["priced_only"], false, "{consumer}");

    let mut pinned: Value = serde_json::from_slice(BODY).unwrap();
    pinned["pin"] = json!(SUBSCRIPTION);
    let (ty, refusal) = route.call("route.select", &pinned).await;
    assert_eq!(ty, FrameType::Error, "{refusal}");
    assert_eq!(refusal["code"], "no_route", "{refusal}");
    assert_eq!(refusal["detail"]["reason"], "price_unknown", "{refusal}");
    pinned["pin_fallback"] = json!("route");
    let (ty, fallback) = route.call("route.select", &pinned).await;
    assert_eq!(ty, FrameType::Response, "{fallback}");
    assert_eq!(
        fallback["result"]["detail"]["pin_reason"], "price_unknown",
        "{fallback}"
    );
    assert_eq!(
        fallback["result"]["price"]["basis"], "metered",
        "{fallback}"
    );

    for model in &catalog.models {
        let key = model.route_key();
        harness.edit_config(|config| config["runners"][0]["routes"] = json!([key]));
        let capped = route.call_raw_params("route.select", BODY).await;
        let reply: Value = serde_json::from_slice(&capped.1).unwrap();
        if key == METERED {
            assert_eq!(capped.0, FrameType::Response, "capped {key}: {reply}");
            assert_eq!(reply["result"]["price"]["basis"], "metered", "{reply}");
        } else {
            assert_eq!(capped.0, FrameType::Error, "capped {key}: {reply}");
            assert_eq!(reply["code"], "no_route", "{reply}");
            assert_eq!(reply["detail"]["reason"], "price_unknown", "{reply}");

            assert!(
                reply["detail"]["rejected"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|row| { row["routeKey"] == key && row["reason"] == "price_unknown" }),
                "{reply}"
            );
            continue;
        }
        assert_eq!(
            reply["result"]["route"]["provider"], model.provider,
            "{reply}"
        );
        assert_eq!(reply["result"]["route"]["model"], model.model, "{reply}");
    }
    facts
        .list_rates
        .insert(METERED.into(), RatesPerMtok::new(4.into(), 2000.into()));
    selection_facts(&harness, &facts);
    harness.edit_config(|config| config["runners"][0]["routes"] = json!([METERED]));
    let capped = route.call_raw_params("route.select", BODY).await;
    assert_eq!(
        capped.0,
        FrameType::Response,
        "high output rate: {}",
        String::from_utf8_lossy(&capped.1)
    );
}

#[tokio::test]
async fn broca_complete_capped_retries_each_unpriceable_route_once() {
    use std::time::Instant;

    let _clock = crate::InjectedClock::set(NOW as u64);
    let (harness, _, _) = setup();
    let route = harness.bind(reserved("broca"), None).await;
    let mut timings = [Vec::new(), Vec::new()];
    for pair in 0..6 {
        for retrying in [pair % 2 == 1, pair % 2 == 0] {
            let mut ladder = UNPRICEABLE.to_vec();
            ladder.insert(if retrying { ladder.len() } else { 0 }, METERED);
            harness.edit_config(|config| {
                config["consumers"]["broca.complete"] =
                    json!({"owner":"broca", "mode":"ladder", "ladder":ladder});
            });
            super::super::select::take_input_builds(&harness);
            let start = Instant::now();
            let (ty, raw) = route.call_raw_params("route.select", BODY).await;
            let elapsed = start.elapsed();
            let reply: Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(ty, FrameType::Response, "{reply}");
            assert_eq!(
                reply["result"]["route"],
                json!({"provider":"example-provider", "model":"model-b"})
            );
            assert_eq!(reply["result"]["price"]["basis"], "metered", "{reply}");
            assert_eq!(
                super::super::select::take_input_builds(&harness),
                [1, 1, if retrying { 4 } else { 1 }]
            );
            if pair > 0 {
                timings[usize::from(retrying)].push(elapsed);
            }
        }
    }
    for samples in &mut timings {
        samples.sort_unstable();
    }
    eprintln!("capped select wall time, median of five paired samples: one attempt {:?}; four attempts {:?}", timings[0][2], timings[1][2]);
}
