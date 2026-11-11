use super::*;

fn eval(kind: &str, model: &str, metric: &str, set: &str, score: f64, time: u64) -> Value {
    json!({"kind":kind,"engine":{"provider":"media-vendor","model":model},
        "metric":metric,"score":score,"set":{"id":set,"items":20},"measured_at_ms":time})
}

async fn accepted(route: &Route, op: &str, body: &Value) {
    let (kind, value) = route.call(op, body).await;
    assert_eq!(kind, FrameType::Response, "{value}");
    assert_eq!(value["result"], json!({}));
}

async fn refused(route: &Route, op: &str, body: &Value, code: &str) -> Value {
    let (kind, value) = route.call(op, body).await;
    assert_eq!(kind, FrameType::Error, "{value}");
    assert_eq!(value["code"], code, "{value}");
    value
}

fn speed(decision: &Value, kind: &str, outcome: &str, duration: Option<u64>, wall: u64) -> Value {
    json!({"decision_id":decision["decision_id"],"kind":kind,
        "engine":{"provider":decision["engine"]["provider"],"model":decision["engine"]["model"]},
        "outcome":outcome,"input":{"duration_ms":duration,"bytes":100,"chars":20},
        "wall_ms":wall,"first_output_ms":10})
}

#[tokio::test]
async fn engine_eval_requires_current_attested_exact_declaration_and_valid_shape() {
    let harness = setup(vec![engine("a", false)]);
    let owner = harness.bind(reserved("colliculus"), None).await;
    let foreign = harness.bind(reserved("stranger"), None).await;
    let body = eval("transcribe", "a", "wer", "reference", 0.9, 100);
    accepted(&owner, "route.engine_eval", &body).await;
    refused(
        &foreign,
        "route.engine_eval",
        &body,
        "engine_not_declared_by_caller",
    )
    .await;
    for (field, value) in [
        ("kind", json!("image")),
        ("engine", json!({"provider":"media-vendor","model":"other"})),
    ] {
        let mut invalid = body.clone();
        invalid[field] = value;
        refused(
            &owner,
            "route.engine_eval",
            &invalid,
            "engine_not_declared_by_caller",
        )
        .await;
    }
    for (pointer, value, field) in [
        ("/score", json!(-0.01), "score"),
        ("/score", json!(1.01), "score"),
        ("/score", json!("NaN"), "score"),
        ("/set/items", json!(-1), "set.items"),
        ("/set", json!({"id":"set","items":1,"extra":1}), "set.extra"),
        (
            "/engine",
            json!({"provider":1,"model":"a"}),
            "engine.provider",
        ),
        ("/measured_at_ms", json!(1.5), "measured_at_ms"),
    ] {
        let mut invalid = body.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        assert_eq!(
            refused(&owner, "route.engine_eval", &invalid, "invalid_request").await["detail"]
                ["field"],
            field
        );
    }
    let direct = harness.bind(json!({"kind":"direct"}), None).await;
    refused(
        &direct,
        "route.engine_eval",
        &body,
        "engine_not_declared_by_caller",
    )
    .await;
    super::super::consumers::replace_catalog(&json!([
        {"module_id":"colliculus","nonce_verified":false,"role_declarations":{"media-provider/v1":{"engines":[engine("a", false)]}}}
    ]));
    refused(
        &owner,
        "route.engine_eval",
        &body,
        "engine_not_declared_by_caller",
    )
    .await;
}

#[tokio::test]
async fn engine_eval_ranks_only_newest_comparable_set_with_metric_defaults() {
    let mut image_a = engine("a", false);
    image_a["kind"] = json!("image");
    let mut image_b = image_a.clone();
    image_b["model"] = json!("b");
    let harness = setup(vec![
        engine("a", false),
        engine("b", false),
        engine("c", false),
        image_a,
        image_b,
    ]);
    let route = harness.bind(reserved("colliculus"), None).await;
    facts(
        &harness,
        &[
            ("a", "transcribe", None, Some(0.0)),
            ("c", "transcribe", None, Some(1.0)),
        ],
    );
    for body in [
        eval("transcribe", "a", "named_term_recall", "old", 1.0, 100),
        eval("transcribe", "b", "named_term_recall", "old", 0.1, 100),
        eval("transcribe", "a", "wer", "old", 0.9, 100),
        eval("transcribe", "b", "wer", "old", 0.8, 100),
    ] {
        accepted(&route, "route.engine_eval", &body).await;
    }
    assert_eq!(selected(&route, &request()).await["engine"]["model"], "a");
    accepted(
        &route,
        "route.engine_eval",
        &eval("transcribe", "b", "wer", "new", 0.0, 200),
    )
    .await;
    // A measured zero still beats cheaper engines measured only on the old set.
    assert_eq!(selected(&route, &request()).await["engine"]["model"], "b");
    accepted(
        &route,
        "route.engine_eval",
        &eval("transcribe", "b", "wer", "new", 0.1, 150),
    )
    .await;
    accepted(
        &route,
        "route.engine_eval",
        &eval("transcribe", "a", "wer", "aaa", 0.1, 200),
    )
    .await;
    assert_eq!(selected(&route, &request()).await["engine"]["model"], "a");
    accepted(
        &route,
        "route.engine_eval",
        &eval("transcribe", "a", "wer", "aaa", 1.0, 200),
    )
    .await;
    let mut explicit = request();
    explicit["requirements"]["metric"] = json!("named_term_recall");
    assert_eq!(selected(&route, &explicit).await["engine"]["model"], "a");
    explicit["requirements"]["metric"] = json!("missing");
    assert_eq!(selected(&route, &explicit).await["engine"]["model"], "a");
    accepted(
        &route,
        "route.engine_eval",
        &eval("image", "b", "wer", "reference", 1.0, 300),
    )
    .await;
    let mut image = request();
    image["kind"] = json!("image");
    assert_eq!(selected(&route, &image).await["engine"]["model"], "a");
    image["requirements"]["metric"] = json!("wer");
    assert_eq!(selected(&route, &image).await["engine"]["model"], "b");
}

#[tokio::test]
async fn engine_report_is_declarer_only_decision_bound_and_idempotent() {
    let mut image = engine("a", false);
    image["kind"] = json!("image");
    let harness = setup(vec![engine("a", false), engine("b", false), image]);
    let provider = harness.bind(reserved("colliculus"), None).await;
    let selecting = harness.bind(reserved("callosum"), None).await;
    let mut phone = request();
    phone["consumer"] = json!("phone.media");
    let decision = selected(&selecting, &phone).await;
    let body = speed(&decision, "transcribe", "done", Some(1000), 250);
    accepted(&provider, "route.engine_report", &body).await;
    accepted(&provider, "route.engine_report", &body).await;
    refused(
        &selecting,
        "route.engine_report",
        &body,
        "engine_not_declared_by_caller",
    )
    .await;
    for (field, value) in [
        ("kind", json!("image")),
        ("decision_id", json!("missing")),
        ("engine", json!({"provider":"media-vendor","model":"b"})),
    ] {
        let mut invalid = body.clone();
        invalid[field] = value;
        refused(
            &provider,
            "route.engine_report",
            &invalid,
            "unknown_decision",
        )
        .await;
    }
    let mut local = phone;
    local["privacy"] = json!("local_only");
    local["caller_local"] = json!([engine("a", true)]);
    let local = selected(&selecting, &local).await;
    refused(
        &provider,
        "route.engine_report",
        &speed(&local, "transcribe", "done", Some(1), 1),
        "unknown_decision",
    )
    .await;
    for (pointer, value, field) in [
        ("/input/duration_ms", json!(-1), "input.duration_ms"),
        ("/input", json!({"extra":1}), "input.extra"),
        ("/wall_ms", json!("1"), "wall_ms"),
        ("/first_output_ms", json!(1.5), "first_output_ms"),
    ] {
        let mut invalid = body.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        assert_eq!(
            refused(
                &provider,
                "route.engine_report",
                &invalid,
                "invalid_request"
            )
            .await["detail"]["field"],
            field
        );
    }
    let (_, samples) = harness
        .store()
        .engine_measurements("transcribe", None, &[("media-vendor", "a")])
        .unwrap();
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].successful_realtime_factor(), Some(0.25));
    assert_eq!(samples[0].first_output_ms, Some(10));
}

#[tokio::test]
async fn engine_speed_filters_realtime_but_never_ranks_or_uses_failed_jobs() {
    let mut a = engine("a", false);
    a["latency_class"] = json!("realtime");
    let harness = setup(vec![a]);
    let route = harness.bind(reserved("colliculus"), None).await;
    let mut realtime = request();
    realtime["requirements"]["latency_class"] = json!("realtime");
    for (outcome, duration) in [
        ("failed", Some(1000)),
        ("cancelled", Some(1000)),
        ("future_outcome", Some(1000)),
        ("done", None),
        ("ended", Some(0)),
    ] {
        let decision = selected(&route, &realtime).await;
        accepted(
            &route,
            "route.engine_report",
            &speed(&decision, "transcribe", outcome, duration, 2000),
        )
        .await;
    }
    let decision = selected(&route, &realtime).await;
    accepted(
        &route,
        "route.engine_report",
        &speed(&decision, "transcribe", "done", Some(1000), 2000),
    )
    .await;
    let refusal = refused(&route, "route.select_engine", &realtime, "no_engine").await;
    assert_eq!(refusal["detail"]["filter"], "requirements");
    assert!(refusal["detail"].get("retry_at_ms").is_none());
    let decision = selected(&route, &request()).await;
    let row = harness
        .store()
        .get_decision_by_id(decision["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&row.score_json).unwrap()["accuracy"],
        Value::Null
    );
}

#[tokio::test]
async fn engine_newer_eval_on_ineligible_engine_cannot_choose_comparison_set() {
    let mut excluded = engine("c", false);
    excluded["languages"] = json!(["fr"]);
    let harness = setup(vec![engine("a", false), engine("b", false), excluded]);
    let route = harness.bind(reserved("colliculus"), None).await;
    for body in [
        eval("transcribe", "a", "wer", "old", 0.1, 100),
        eval("transcribe", "b", "wer", "old", 0.9, 100),
        eval("transcribe", "a", "wer", "new", 1.0, 10),
        eval("transcribe", "c", "wer", "new", 1.0, 200),
    ] {
        accepted(&route, "route.engine_eval", &body).await;
    }
    let mut params = request();
    params["requirements"]["language"] = json!("en");
    assert_eq!(selected(&route, &params).await["engine"]["model"], "b");
}
