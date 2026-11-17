//! Search uses the real dispatcher with module-owned credentials and catalog rows.

use super::conformance_tests::{reserved, Harness, Route};
use serde_json::{json, Value};
use std::sync::Arc;
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

const CONSUMER: &str = "plexus.web_search";
const NOW: u64 = 1_750_000_000_000;

fn declarations() -> Value {
    json!([{"module_id":"plexus","nonce_verified":true,"role_declarations":{
        "routing/v1":{"consumers":[{"name":CONSUMER,"kind":"search","selectors":["plexus"]}]},
        "search-provider/v1":{"routes":[
            {"provider":"search-fixture","model":"allowance"},
            {"provider":"search-fixture","model":"paid"},
            {"provider":"search-fixture","model":"unpriced"}
        ]}
    }}])
}

fn catalog(allowance_grade: u64) -> Value {
    json!({"catalog_version":1,"resolved_at_ms":NOW,
    "quality_scores":{"graded_by":"operator","graded_at_ms":NOW},"models":{
        "search-fixture/allowance":{"existence":"present","model.type":"search","quality.grade":allowance_grade,
                "rate.request.per_request":{"state":"priced","currency":"USD","units":10,"exponent":3,"per":1}},
        "search-fixture/paid":{"existence":"present","model.type":"search","quality.grade":400,
                "rate.output.per_result":{"state":"priced","currency":"USD","units":1,"exponent":3,"per":1}},
        "search-fixture/unpriced":{"existence":"present","model.type":"search","quality.grade":400}
    }})
}

fn install(h: &Harness, catalog: &Value) {
    let snapshot =
        crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(catalog).unwrap()).unwrap();
    h.credentials().lock().unwrap().catalog_snapshot = Some(snapshot);
}

fn setup(grade: u64) -> Arc<Harness> {
    let h = Harness::new("search");
    super::consumers::replace_catalog(&declarations());
    install(&h, &catalog(grade));
    h
}

fn request() -> Value {
    json!({"consumer":CONSUMER,"kind":"search","privacy":"remote_allowed","requirements":{"iq":75},
        "eligible_routes":[{"provider":"search-fixture","model":"allowance"},{"provider":"search-fixture","model":"paid"}],
        "allowance_left":[{"provider":"search-fixture","model":"allowance"}]})
}

async fn selected(route: &Route, params: &Value) -> Value {
    let (ty, value) = route.call("route.select", params).await;
    assert_eq!(ty, FrameType::Response, "{value}");
    let answer: cortexkit_role_routing::select_media::SelectMediaAnswer =
        serde_json::from_value(value["result"].clone()).unwrap();
    let parsed = cortexkit_role_routing::select_search::parse_select_search_request(
        params,
        &super::describe::capabilities(),
        &["search"],
    )
    .unwrap();
    cortexkit_role_routing::select_search::check_answer(&parsed, &answer).unwrap();
    value["result"].clone()
}

async fn refused(route: &Route, params: &Value, code: &str, field: Option<&str>) {
    let (ty, value) = route.call("route.select", params).await;
    assert_eq!(ty, FrameType::Error, "{value}");
    assert_eq!(value["code"], code, "{value}");
    if let Some(field) = field {
        assert_eq!(value["detail"]["field"], field);
    }
}

#[tokio::test]
async fn allowance_wins_only_after_the_quality_level_and_keeps_catalog_price() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    let answer = selected(&route, &request()).await;
    assert_eq!(
        answer["engine"],
        json!({"provider":"search-fixture","model":"allowance","local":false,"caller_local":false,"module":"plexus"})
    );
    assert_eq!(
        answer["price"],
        json!({"basis":"metered","rates":{"request.per_request":{"rate":0.01}}})
    );
    install(&h, &catalog(100));
    let answer = selected(&route, &request()).await;
    assert_eq!(answer["engine"]["model"], "paid");
    assert_eq!(
        answer["price"],
        json!({"basis":"metered","rates":{"output.per_result":{"rate":0.001}}})
    );
    let mut params = request();
    params["requirements"] = json!({});
    assert_eq!(selected(&route, &params).await["engine"]["model"], "paid");
}

#[tokio::test]
async fn unpriced_is_unknown_not_free_and_allowance_does_not_invent_a_quote() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    let mut params = request();
    params["eligible_routes"][0]["model"] = json!("unpriced");
    params["allowance_left"] = json!([]);
    assert_eq!(selected(&route, &params).await["engine"]["model"], "paid");
    params["allowance_left"] = json!([{"provider":"search-fixture","model":"unpriced"}]);
    let answer = selected(&route, &params).await;
    assert_eq!(answer["engine"]["model"], "unpriced");
    assert!(answer.get("price").is_none());
}

#[tokio::test]
async fn search_policy_and_eligible_subset_are_hard_filters() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    for policy in [json!({"not_for":[CONSUMER]}), json!({"pin_only":true})] {
        h.edit_config(|config| config["models"]["search-fixture/allowance"] = policy);
        assert_eq!(
            selected(&route, &request()).await["engine"]["model"],
            "paid"
        );
    }
    h.edit_config(|config| config["models"]["search-fixture/allowance"] = json!({}));
    let mut params = request();
    params["exclude"] = json!(["search-fixture/allowance"]);
    assert_eq!(selected(&route, &params).await["engine"]["model"], "paid");
    params["eligible_routes"] = json!([]);
    params["allowance_left"] = json!([]);
    refused(&route, &params, "no_route", None).await;
    params["allowance_left"] = json!([{"provider":"search-fixture","model":"allowance"}]);
    refused(
        &route,
        &params,
        "invalid_request",
        Some("allowance_left[0]"),
    )
    .await;
}

#[tokio::test]
async fn search_requirement_guard_refuses_every_unknown_member_by_name() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    for field in ["format", "tools", "context_tokens", "future_requirement"] {
        let mut params = request();
        params["requirements"] = json!({field:null});
        refused(&route, &params, "invalid_requirements", Some(field)).await;
    }
    for iq in [json!(101), json!(-1), json!(1.5), json!("50")] {
        let mut params = request();
        params["requirements"] = json!({"iq":iq});
        refused(&route, &params, "invalid_requirements", Some("iq")).await;
    }
}

#[tokio::test]
async fn search_privacy_guard_requires_explicit_remote_permission_and_refuses_caller_local() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    let mut params = request();
    params.as_object_mut().unwrap().remove("privacy");
    refused(&route, &params, "invalid_request", Some("privacy")).await;
    params["privacy"] = json!("local_only");
    refused(&route, &params, "no_route", None).await;
    params["privacy"] = json!("remote_allowed");
    params["caller_local"] = json!(null);
    refused(&route, &params, "invalid_request", Some("caller_local")).await;
}

#[tokio::test]
async fn search_allowance_guard_requires_subset_and_refuses_other_kinds() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    let mut params = request();
    params.as_object_mut().unwrap().remove("eligible_routes");
    refused(
        &route,
        &params,
        "invalid_request",
        Some("allowance_left[0]"),
    )
    .await;
    params = request();
    params["allowance_left"][0]["model"] = json!("outside");
    refused(
        &route,
        &params,
        "invalid_request",
        Some("allowance_left[0]"),
    )
    .await;
    for kind in ["llm", "transcribe", "image"] {
        params = request();
        params["consumer"] = json!("plexus.other");
        params["kind"] = json!(kind);
        params["requirements"] = json!({"iq":0});
        params["allowance_left"] = json!([]);
        if kind == "llm" {
            params.as_object_mut().unwrap().remove("privacy");
        }
        refused(&route, &params, "invalid_request", Some("allowance_left")).await;
    }
}

#[tokio::test]
async fn omitted_kind_uses_declared_search_and_otherwise_keeps_chat_decoding() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    let mut params = request();
    params.as_object_mut().unwrap().remove("kind");
    let (ty, reply) = route.call("route.select", &params).await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    assert_eq!(reply["result"]["engine"]["model"], "allowance");
    params.as_object_mut().unwrap().remove("privacy");
    refused(&route, &params, "invalid_request", Some("privacy")).await;
    let mut entries = declarations();
    entries[0]["role_declarations"]["routing/v1"]["consumers"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"plexus.chat","kind":"llm"}));
    super::consumers::replace_catalog(&entries);
    for consumer in ["plexus.chat", "undeclared.chat"] {
        // The chat parser still requires iq. Search would instead require privacy.
        refused(
            &route,
            &json!({"consumer":consumer,"requirements":{}}),
            "invalid_requirements",
            Some("iq"),
        )
        .await;
    }
}

#[tokio::test]
async fn search_declarations_are_atomic_verified_and_join_only_catalog_search_rows() {
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    let mut entries = declarations();
    let good = entries[0]["role_declarations"]["search-provider/v1"]["routes"][0].clone();
    entries[0]["role_declarations"]["search-provider/v1"]["routes"] = json!([good.clone(), good]);
    super::consumers::replace_catalog(&entries);
    refused(&route, &request(), "no_route", None).await;
    assert!(super::consumers::health()
        .to_string()
        .contains("duplicate_route"));
    entries = declarations();
    entries[0]["role_declarations"]["search-provider/v1"]["routes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"provider":"bad","model":"","account":"hidden"}));
    super::consumers::replace_catalog(&entries);
    refused(&route, &request(), "no_route", None).await;
    assert!(super::consumers::health()
        .to_string()
        .contains("invalid_declaration"));
    entries = declarations();
    entries[0]["nonce_verified"] = json!(false);
    // A separate verified entry declares the consumer, so its ownership does
    // not rely on the unverified search-provider declaration.
    entries.as_array_mut().unwrap().push(json!({"module_id":"plexus","nonce_verified":true,"role_declarations":{"routing/v1":{"consumers":[{"name":CONSUMER}]}}}));
    super::consumers::replace_catalog(&entries);
    refused(&route, &request(), "no_route", None).await;
    assert!(super::consumers::health()
        .to_string()
        .contains("unverified"));
    super::consumers::replace_catalog(&declarations());
    let mut missing = catalog(400);
    missing["models"]
        .as_object_mut()
        .unwrap()
        .remove("search-fixture/allowance");
    missing["models"]["search-fixture/paid"]["model.type"] = json!("speech");
    install(&h, &missing);
    refused(&route, &request(), "no_route", None).await;
    let health = super::consumers::health().to_string();
    assert!(health.contains("search-fixture/allowance excluded: missing catalog search row"));
    assert!(health.contains("search-fixture/paid excluded: missing catalog search row"));
}

#[tokio::test]
async fn search_report_outcomes_are_route_scoped_bounded_and_idempotent() {
    let _clock = crate::InjectedClock::set(NOW);
    for (status, legacy, wait, cooldown, success) in [
        ("served", "completed", None, None, true),
        ("served_empty", "completed", None, None, true),
        ("throttled", "failed", Some(1234), Some(1234), false),
        (
            "throttled",
            "failed",
            Some(7_200_000),
            Some(3_600_000),
            false,
        ),
        ("throttled", "failed", None, Some(5_000), false),
        ("quota", "failed", None, Some(3_600_000), false),
        ("credential", "failed", None, Some(3_600_000), false),
        ("unavailable", "failed", None, Some(5_000), false),
        ("rejected", "cancelled", None, None, false),
    ] {
        let h = setup(400);
        let route = h.bind(reserved("plexus"), None).await;
        let answer = selected(&route, &request()).await;
        let mut report = json!({"decision_id":answer["decision_id"],"outcome":legacy,"attempt_outcome":{"status":status}});
        if let Some(wait) = wait {
            report["attempt_outcome"]["retry_after_ms"] = json!(wait);
        }
        let (ty, reply) = route.call("route.report", &report).await;
        assert_eq!(ty, FrameType::Response, "{status}: {reply}");
        let key = "search-fixture/allowance";
        let snapshot = h.store().build_snapshot(NOW as f64).unwrap();
        let state = snapshot.by_route.get(key);
        assert_eq!(
            state.and_then(|state| state.cooldown_until),
            cooldown.map(|wait| (NOW + wait) as f64),
            "{status}"
        );
        assert_eq!(
            state.and_then(|state| state.success_ema),
            success.then_some(1.0),
            "{status}"
        );
        assert!(
            snapshot.by_route.get("search-fixture/paid").is_none(),
            "{status}: sibling route was affected"
        );
        let duplicate = route.call("route.report", &report).await;
        assert_eq!(duplicate, (ty, reply));
        assert_eq!(
            h.store()
                .build_snapshot(NOW as f64)
                .unwrap()
                .by_route
                .get(key)
                .and_then(|state| state.cooldown_until),
            state.and_then(|state| state.cooldown_until)
        );
        if let Some(wait) = cooldown {
            assert_eq!(
                selected(&route, &request()).await["engine"]["model"],
                "paid",
                "{status}"
            );
            h.credentials().lock().unwrap().engine_clock_offset_ms = wait + 1;
            assert_eq!(
                selected(&route, &request()).await["engine"]["model"],
                "allowance",
                "{status}: next select is the recovery test"
            );
        } else {
            assert_eq!(
                selected(&route, &request()).await["engine"]["model"],
                "allowance",
                "{status}"
            );
        }
    }
}

#[tokio::test]
async fn serving_bytes_match_search_vectors() {
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, BTreeSet};
    let _clock = crate::InjectedClock::set(NOW);
    let _ids = striatum_store::replay::Inputs::new(NOW as i64).install();
    let h = setup(400);
    let route = h.bind(reserved("plexus"), None).await;
    let mut samples = BTreeMap::new();
    for (name, grade, params, model, error) in [
        ("01-allowance-wins", 400, request(), Some("allowance"), None),
        ("02-paid-clears-level", 100, request(), Some("paid"), None),
        (
            "03-empty-subset",
            400,
            {
                let mut params = request();
                params["eligible_routes"] = json!([]);
                params["allowance_left"] = json!([]);
                params
            },
            None,
            Some("no_route"),
        ),
        (
            "04-refused-requirement",
            400,
            {
                let mut params = request();
                params["requirements"]["format"] = json!("text/plain");
                params
            },
            None,
            Some("invalid_requirements"),
        ),
    ] {
        install(&h, &catalog(grade));
        let frame = route.request_frame("route.select", &params);
        let request = frame.body.clone();
        let reply = h.handle(frame).await;
        let value: Value = serde_json::from_slice(&reply.body).unwrap();
        if let Some(model) = model {
            assert_eq!(reply.header.ty, FrameType::Response, "{name}: {value}");
            assert_eq!(value["result"]["engine"]["model"], model);
            assert_eq!(value["result"]["expires_at_ms"], NOW + 600_000);
            let answer = serde_json::from_value(value["result"].clone()).unwrap();
            let parsed = cortexkit_role_routing::select_search::parse_select_search_request(
                &params,
                &super::describe::capabilities(),
                &["search"],
            )
            .unwrap();
            cortexkit_role_routing::select_search::check_answer(&parsed, &answer).unwrap();
            assert_eq!(serde_json::to_value(answer).unwrap(), value["result"]);
        } else {
            assert_eq!(reply.header.ty, FrameType::Error);
            assert_eq!(value["code"], error.unwrap());
            if name == "04-refused-requirement" {
                assert_eq!(value["detail"]["field"], "format");
            }
        }
        // Preserve dispatcher bytes, including member order and refusal detail.
        let mut pair = b"{\"request\":".to_vec();
        pair.extend_from_slice(&request);
        pair.extend_from_slice(b",\"reply\":");
        pair.extend_from_slice(&reply.body);
        pair.push(b'}');
        samples.insert(format!("{name}.json"), pair);
    }
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/route-select-search-v1");
    let update = std::env::var_os("UPDATE_ROUTE_SELECT_SEARCH_VECTORS").is_some();
    let mut sums = String::new();
    for (name, bytes) in &samples {
        let path = dir.join(name);
        if update {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&path, bytes).unwrap();
        }
        assert_eq!(std::fs::read(&path).expect(name), *bytes, "{name}");
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes)));
    }
    if update {
        std::fs::write(dir.join("SHA256SUMS"), &sums).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(dir.join("SHA256SUMS")).unwrap(),
        sums
    );
    let files: BTreeSet<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(
        files,
        samples
            .keys()
            .cloned()
            .chain(["README.md".into(), "SHA256SUMS".into()])
            .collect()
    );
}
