//! Wire samples for callers switching to declared consumers, prices and harnesses.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use cortexkit_role_routing::declarations::DemandDefaults;
use cortexkit_role_routing::price::RatesPerMtok;
use cortexkit_role_routing::select::{
    check_answer_serving, parse_select_request_with_defaults, SelectAnswer,
};
use cortexkit_role_routing_conformance::{fixture, SelectionFacts};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

use super::super::conformance_tests::{reserved, selection_facts};
use super::super::dispatch_tests::configured;
use super::super::{consumers, describe, runners};

const NOW: u64 = 1_800_000_000_000;
const METERED: &str = "example-provider/model-a";
const SUBSCRIPTION: &str = "example-subscription/model-c";
const STEMS: &[&str] = &[
    "basal-flow",
    "magic-context-summary",
    "price-metered",
    "price-subscription-5m-1h",
    "price-subscription-30m-12h",
    "price-unpriced",
    "decision-harness",
    "broca-harness",
];

fn require_production_capabilities() {
    assert_eq!(&*describe::capabilities(), describe::CAPABILITIES);
    for group in [
        "consumer_declarations",
        "decision_harness",
        "select_eligible_routes",
        "select_price",
    ] {
        assert!(
            describe::CAPABILITIES.contains(&group),
            "sample capture requires advertised group: {group}"
        );
    }
}

async fn capture(stem: &str) -> [(String, Vec<u8>); 2] {
    let _clock = crate::InjectedClock::set(NOW);
    let _ids = striatum_store::replay::Inputs::new(NOW as i64).install();
    // Each pair starts with independent decisions, account state and replay IDs.
    let (harness, _) = configured().await;
    let mut facts = SelectionFacts::new();
    let long_lifetimes = stem == "price-subscription-30m-12h";
    let ttl = if long_lifetimes {
        [("30m".into(), 55.into()), ("12h".into(), 88.into())]
    } else {
        [("5m".into(), 5.into()), ("1h".into(), 8.into())]
    };
    let rates = if long_lifetimes {
        RatesPerMtok::new(22.into(), 110.into())
            .with_cache_read(11.into())
            .with_cache_write(44.into())
    } else {
        RatesPerMtok::new(4.into(), 20.into())
            .with_cache_read(1.into())
            .with_cache_write(5.into())
    }
    .with_cache_write_ttl(ttl.into());
    facts.list_rates.insert(METERED.into(), rates.clone());
    facts.list_rates.insert(SUBSCRIPTION.into(), rates);
    facts.multipliers.insert(SUBSCRIPTION.into(), 22.into());
    selection_facts(&harness, &facts);

    let slot: Value =
        serde_json::from_str(include_str!("../runners/broca-runner-slot.after.json")).unwrap();
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        let mut broca = slot.clone();
        if stem != "broca-harness" {
            // Today's Broca has no harness facts. Keep that wire shape alongside
            // the unchanged real declaration that Broca will publish next.
            broca["runner"].as_object_mut().unwrap().remove("harnesses");
            broca["runner"]
                .as_object_mut()
                .unwrap()
                .remove("harness_limits");
        }
        credentials.runner_declarations.insert(
            "broca".into(),
            runners::parse_declaration(&broca).unwrap().unwrap(),
        );
        credentials.runner_declarations.insert(
            "sample-harness-runner".into(),
            runners::parse_declaration(&json!({"runner":{
                "pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}],
                "harnesses":["sample-harness"],"default_harness":"sample-harness"
            }}))
            .unwrap()
            .unwrap(),
        );
        // Broca's real slot requires a curated wire endpoint for API-key routes.
        // Supply that catalog fact without weakening the runner declaration.
        let rate = |units| json!({"state":"priced","currency":"USD","units":units,"exponent":0});
        let catalog = credentials.billing_catalog.clone().with_test_plane(
            &serde_json::to_vec(&json!({
                "plane":{"provider_id":"example-provider","auth_method":"apikey","kind":"upstream"},
                "models":{METERED:{
                    "wire.family":"anthropic.messages",
                    "wire.api":{"value":"https://api.example.test/messages","from":"curated"},
                    "rate.input":rate(4),"rate.output":rate(20),"rate.cache_read":rate(1),"rate.cache_write":rate(5),
                    "rate.cache_write.ttl.5m":rate(5),"rate.cache_write.ttl.1h":rate(8)
                }}
            })).unwrap(),
        );
        credentials.publish_billing_catalog(catalog);
    }
    consumers::replace_catalog(&json!([
        {"module_id":"basal","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":"basal.flow","placement":{"module":"broca"}}
            ]}}},
        {"module_id":"magic-context","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":"magic-context.summary","iq":40,"placement":"caller_runner"}
            ]}}}
    ]));
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":fixture::RUNNER_ONE},
            {"module":"broca"},
            {"module":"sample-harness-runner"}
        ]);
    });

    if stem == "price-unpriced" {
        let mut catalog = harness.store().get_facts().unwrap();
        for fact in &mut catalog {
            fact.input_rate = None;
            fact.cost_per_m_tok_out = None;
            fact.cache_read_rate = None;
            fact.cache_write_rate = None;
            fact.billing_plane = None;
            harness
                .store()
                .upsert_fact(fact, striatum_core::FactSource::Override)
                .unwrap();
        }
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        let catalog = crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&catalog)
            .with_wire_apis(&credentials.wire_apis);
        credentials.publish_billing_catalog(catalog);
    }

    let mut params = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"pin":METERED});
    let (caller, expected_runner) = match stem {
        "basal-flow" | "broca-harness" => {
            params = json!({"consumer":"basal.flow","requirements":{"iq":50,"tools":"none"},
                "eligible_routes":[{"provider":"example-provider","model":"model-a"}]});
            ("basal", "broca")
        }
        "magic-context-summary" => {
            params = json!({"consumer":"magic-context.summary","requirements":{"tools":"none"},
                "runners":["broca"],
                "eligible_routes":[{"provider":"example-provider","model":"model-a"}]});
            ("magic-context", "broca")
        }
        "price-subscription-5m-1h" | "price-subscription-30m-12h" => {
            params["pin"] = json!(SUBSCRIPTION);
            (fixture::CONSUMER, fixture::RUNNER_ONE)
        }
        "decision-harness" => {
            params["harness"] = json!("sample-harness");
            (fixture::CONSUMER, "sample-harness-runner")
        }
        "price-metered" | "price-unpriced" => (fixture::CONSUMER, fixture::RUNNER_ONE),
        _ => panic!("unknown sample: {stem}"),
    };
    let route = harness.bind(reserved(caller), None).await;
    let frame = route.request_frame("route.select", &params);
    let request = frame.body.clone();
    let reply = harness.handle(frame).await;
    assert_eq!(
        reply.header.ty,
        FrameType::Response,
        "{stem}: {}",
        String::from_utf8_lossy(&reply.body)
    );
    let body: Value = serde_json::from_slice(&reply.body).unwrap();
    let answer = &body["result"];
    let mut defaults = DemandDefaults::default();
    if stem == "magic-context-summary" {
        defaults.iq = Some(40);
    }
    let parsed =
        parse_select_request_with_defaults(&params, describe::CAPABILITIES, &defaults).unwrap();
    let typed: SelectAnswer = serde_json::from_value(answer.clone()).unwrap();
    check_answer_serving(&parsed, &typed, describe::CAPABILITIES).unwrap();
    assert_eq!(
        answer["runner"]["module"], expected_runner,
        "{stem}: {body}"
    );
    if stem == "broca-harness" {
        assert_eq!(answer["runner"]["harness"], "broca", "{body}");
    } else if expected_runner == "broca" {
        assert!(answer["runner"].get("harness").is_none(), "{stem}: {body}");
    } else if stem == "decision-harness" {
        assert_eq!(answer["runner"]["harness"], "sample-harness", "{body}");
    }
    if stem == "price-unpriced" {
        assert!(answer.get("price").is_none(), "{body}");
    } else {
        let subscription = stem.starts_with("price-subscription-");
        assert_eq!(
            answer["price"]["basis"],
            if subscription {
                "subscription_equivalent"
            } else {
                "metered"
            },
            "{stem}: {body}"
        );
        if subscription {
            assert_eq!(answer["price"]["multiplier"], 22, "{body}");
            let expected = if long_lifetimes {
                json!({"input":1.0,"output":5.0,"cache_read":0.5,"cache_write":2.0,
                    "cache_write_ttl":{"30m":2.5,"12h":4.0}})
            } else {
                json!({"input":4.0/22.0,"output":20.0/22.0,"cache_read":1.0/22.0,
                    "cache_write":5.0/22.0,"cache_write_ttl":{"5m":5.0/22.0,"1h":8.0/22.0}})
            };
            assert_eq!(answer["price"]["rates_per_mtok"], expected, "{body}");
        } else {
            assert!(answer["price"].get("multiplier").is_none(), "{body}");
            assert_eq!(
                answer["price"]["rates_per_mtok"],
                json!({
                    "input":4,"output":20,"cache_read":1,"cache_write":5,
                    "cache_write_ttl":{"5m":5,"1h":8}
                }),
                "{stem}: {body}"
            );
        }
    }
    if stem == "magic-context-summary" {
        let decision = harness
            .store()
            .get_decision_by_id(typed.decision_id.as_str())
            .unwrap()
            .unwrap();
        let requirements: Value = serde_json::from_str(&decision.requirements_json).unwrap();
        assert_eq!(requirements["iq"].as_f64(), Some(40.0));
    }
    // Preserve handler bytes, including member order and generated fields.
    [
        (format!("{stem}.request.json"), request),
        (format!("{stem}.reply.json"), reply.body),
    ]
}

fn verify(samples: BTreeMap<String, Vec<u8>>) {
    let directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/route-select-declared-v1");
    let update = std::env::var_os("UPDATE_ROUTE_SELECT_DECLARED_VECTORS").is_some();
    let mut expected: BTreeSet<_> = samples.keys().cloned().collect();
    expected.extend(["README.md".into(), "SHA256SUMS".into()]);
    for entry in std::fs::read_dir(&directory).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        assert!(expected.contains(&name), "unlisted sample file: {name}");
    }
    for (name, bytes) in &samples {
        let path = directory.join(name);
        if update {
            std::fs::write(&path, bytes).unwrap();
        }
        let actual = std::fs::read(&path).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(&actual, bytes, "sample byte difference: {name}");
    }
    let mut files = samples;
    files.insert(
        "README.md".into(),
        std::fs::read(directory.join("README.md")).unwrap(),
    );
    let sums: String = files
        .iter()
        .map(|(name, bytes)| format!("{:x}  {name}\n", Sha256::digest(bytes)))
        .collect();
    let path = directory.join("SHA256SUMS");
    if update {
        std::fs::write(&path, &sums).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(&path).expect("SHA256SUMS must exist"),
        sums,
        "SHA256SUMS: byte or digest difference"
    );
}

#[tokio::test]
async fn serving_bytes_match_tagged_vectors() {
    require_production_capabilities();
    let mut samples = BTreeMap::new();
    for stem in STEMS {
        for (name, bytes) in capture(stem).await {
            assert!(samples.insert(name, bytes).is_none());
        }
    }
    verify(samples);
}
