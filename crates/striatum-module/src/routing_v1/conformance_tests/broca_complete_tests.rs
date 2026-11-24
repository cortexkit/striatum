use super::*;
use cortexkit_role_routing::price::RatesPerMtok;
use cortexkit_role_routing_conformance::SelectionFacts;

// Captured from Broca's llm_complete test, 24 Nov.
const BODY: &[u8] = br#"{"consumer":"broca.complete.basal.flow","requirements":{"iq":60,"eq":20,"max_output_tokens":500,"input_tokens":535,"max_cost_usd":0.5},"runners":["broca"]}"#;

#[tokio::test]
async fn broca_complete_child_request_reaches_selection() {
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
    let route = harness.bind(reserved("broca"), None).await;
    let (ty, bytes) = route.call_raw_params("route.select", BODY).await;
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    println!("BROCA_COMPLETE_REPLY {}", String::from_utf8(bytes).unwrap());
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
    let selected = catalog
        .models
        .iter()
        .find(|model| {
            answer["route"]["provider"] == model.provider && answer["route"]["model"] == model.model
        })
        .unwrap();
    println!(
        "BROCA_COMPLETE_SELECTED subscription={} price={} subscription_member={}",
        !selected.priced_per_token,
        answer["price"],
        answer.get("subscription").unwrap_or(&Value::Null)
    );

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

    for model in &catalog.models {
        let key = model.route_key();
        harness.edit_config(|config| config["runners"][0]["routes"] = json!([key]));
        let mut uncapped: Value = serde_json::from_slice(BODY).unwrap();
        uncapped["requirements"]
            .as_object_mut()
            .unwrap()
            .remove("max_cost_usd");
        let baseline = route.call("route.select", &uncapped).await;
        assert_eq!(
            baseline.0,
            FrameType::Response,
            "baseline {key}: {baseline:?}"
        );
        let capped = route.call_raw_params("route.select", BODY).await;
        let reply: Value = serde_json::from_slice(&capped.1).unwrap();
        println!(
            "BROCA_COMPLETE_ISOLATED {key} subscription={} reply={reply}",
            !model.priced_per_token
        );
        assert_eq!(capped.0, FrameType::Response, "capped {key}: {reply}");
        assert_eq!(
            reply["result"]["route"]["provider"], model.provider,
            "{reply}"
        );
        assert_eq!(reply["result"]["route"]["model"], model.model, "{reply}");
    }
    for model in &catalog.models {
        facts
            .list_rates
            .insert(model.route_key(), RatesPerMtok::new(4.into(), 2000.into()));
    }
    selection_facts(&harness, &facts);
    let capped = route.call_raw_params("route.select", BODY).await;
    assert_eq!(
        capped.0,
        FrameType::Response,
        "high output rate: {}",
        String::from_utf8_lossy(&capped.1)
    );
}
