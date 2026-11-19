use super::*;
use cortexkit_test_support::ScratchDir;
use serde_json::json;
use subc_client_rs::config::{ConfigLayer, ConfigLayerSnapshot};

fn snapshot(document: &str) -> ConfigLayerSnapshot {
    ConfigLayerSnapshot::file(
        "striatum",
        ConfigLayer::User,
        "/isolated/striatum.jsonc",
        Some(document.as_bytes().to_vec()),
    )
}

fn review_schema() -> Schema {
    Schema::new(emitted_schema().clone()).expect("accept the full routing settings declaration")
}

#[test]
fn review_accepts_a_jsonc_change_and_reports_value_and_source_changes() {
    let schema = review_schema();
    let base = schema
        .review_tuple(&[], &ValidationContext::new("striatum", None))
        .unwrap();
    let mut base = ConfigView::new(
        3,
        base.effective,
        base.sources,
        vec![],
        ConfigStatus::Active,
    );
    base.sources.insert("/demand_exponent".into(), Layer::User);
    let reply = review(
        &schema,
        "striatum",
        ConfigReviewRequest::new(
            3,
            vec![snapshot("// Terminal failures before avoidance\n{\"consecutive_terminal_failure_threshold\":4,}")],
            Some(base),
        ),
    );
    assert!(reply.refusals.is_empty(), "{:?}", reply.refusals);
    assert_eq!(reply.fields.len(), 2);
    let changed = &reply.fields[0];
    assert_eq!(changed.path, "/consecutive_terminal_failure_threshold");
    assert_eq!(changed.old, RecordedValue::new(json!(3)));
    assert_eq!(changed.new, RecordedValue::new(json!(4)));
    assert_eq!(
        (changed.old_source, changed.new_source, changed.apply),
        (Layer::Default, Layer::User, ApplyKind::Live)
    );
    let source_only = &reply.fields[1];
    assert_eq!(source_only.path, "/demand_exponent");
    assert_eq!(source_only.old, source_only.new);
    assert_eq!(
        (source_only.old_source, source_only.new_source),
        (Layer::User, Layer::Default)
    );
}

fn review_refusal(document: &str) -> ConfigReviewRefusal {
    let reply = review(
        &review_schema(),
        "striatum",
        ConfigReviewRequest::new(0, vec![snapshot(document)], None),
    );
    assert!(reply.fields.is_empty());
    assert_eq!(reply.refusals.len(), 1, "{:?}", reply.refusals);
    reply.refusals.into_iter().next().unwrap()
}

#[test]
fn review_names_an_explicit_null() {
    let refusal = review_refusal(r#"{"catalog":{"poll_ms":null}}"#);
    assert_eq!(refusal.paths, ["/catalog/poll_ms"]);
}

#[test]
fn review_names_an_old_key() {
    let refusal = review_refusal(r#"{"models":{"p/m":{"context_window":4096}}}"#);
    assert_eq!(refusal.paths, ["/models/p~1m/context_window"]);
}

#[test]
fn review_names_the_fields_in_each_panel_cross_field_rule() {
    let ordering = review_refusal(r#"{"panels":{"audit":{"top_k":3,"by":"iq","extra":1}}}"#);
    assert_eq!(
        ordering.paths,
        ["/panels/audit/extra_by", "/panels/audit/extra"]
    );
    assert!(ordering.reasons[0].contains("extra_by is required"));

    let width =
        review_refusal(r#"{"panels":{"audit":{"top_k":8,"by":"iq","extra":1,"extra_by":"iq"}}}"#);
    assert_eq!(width.paths, ["/panels/audit/top_k", "/panels/audit/extra"]);
    assert!(width.reasons[0].contains("top_k + extra must be <= 8"));
}

#[test]
fn review_refuses_a_duplicate_field_even_after_the_tuple_collapses_it() {
    let refusal = review_refusal(r#"{"catalog":{"poll_ms":1000,"poll_ms":2000}}"#);
    assert_eq!(refusal.paths, ["/catalog/poll_ms"]);
    assert!(refusal.reasons[0].contains("duplicate field `poll_ms`"));
}

fn live_settings_source() -> String {
    let sample = r#"{
        // Keep the non-export case executable in ordinary test runs.
        "catalog": {"poll_ms": 3210},
        "retention": {"model_route_decisions_days": 27},
        "consumers": {"mason": {"ladder": ["p/m"]}},
        "models": {"p/m": {"context_window_tokens": 4096, "output_tokens_per_second": 42}},
        "panels": {"audit": {"top_k": 2, "by": "iq"}}
    }"#;
    if let Some(path) = std::env::var_os("STRIATUM_TEST_LIVE_SETTINGS") {
        return std::fs::read_to_string(path).expect("read the isolated live settings export");
    }
    let export = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.cortexkit/internal-docs/research/striatum-live-settings.jsonc");
    match std::fs::read_to_string(export) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => sample.to_owned(),
        Err(error) => panic!("read the isolated live settings export: {error}"),
    }
}

#[test]
fn full_schema_reviews_live_settings_as_the_user_layer() {
    let source = live_settings_source();
    let json = subc_jsonc::jsonc_to_json(&source).unwrap();
    let user = serde_json::from_str(&json).unwrap();
    let tuple = review_schema()
        .review_tuple(
            &[LayerValue::new(Layer::User, user)],
            &ValidationContext::new("striatum", None),
        )
        .unwrap_or_else(|error| panic!("routing user layer refused: {error}"));
    config_overrides::validate_document(&tuple.effective.to_string()).unwrap();
}

#[test]
fn review_keeps_optional_records_absent_and_preserves_supplied_shapes() {
    let schema = review_schema();
    let context = ValidationContext::new("striatum", None);
    let user = json!({
        "concurrency": {"providers": {"p": {"limit": "none"}, "q": {"limit": 1}}},
        "consumers": {"pinned": {"pin": {"model": "p/m"}}, "unbound": {}},
        "panels": {"audit": {"top_k": 2, "by": "iq"}},
    });
    let tuple = schema
        .review_tuple(&[LayerValue::new(Layer::User, user.clone())], &context)
        .unwrap();
    for path in [
        "/concurrency/providers/p/limit",
        "/concurrency/providers/q/limit",
        "/consumers/pinned/pin",
        "/panels/audit",
    ] {
        assert_eq!(tuple.effective.pointer(path), user.pointer(path), "{path}");
        assert_eq!(tuple.sources.get(path), Some(&Layer::User), "{path}");
    }
    for path in [
        "/concurrency/providers/p/scope",
        "/consumers/unbound/pin",
        "/panels/lab",
    ] {
        assert!(tuple.effective.pointer(path).is_none(), "{path}");
    }

    for (invalid, path) in [
        (json!({"consumers":{"x":{"pin":{}}}}), "/consumers/x/pin"),
        (
            json!({"panels":{"audit":{"top_k":9,"by":"iq"}}}),
            "/panels/audit",
        ),
        (
            json!({"panels":{"audit":{"top_k":2,"by":"iq","topK":2}}}),
            "/panels/audit",
        ),
        (
            json!({"concurrency":{"providers":{"p":{"limit":"unlimited"}}}}),
            "/concurrency/providers/p/limit",
        ),
    ] {
        let error = schema
            .review_tuple(&[LayerValue::new(Layer::User, invalid)], &context)
            .unwrap_err();
        assert_eq!(error.path, path, "{error}");
    }
}

#[test]
fn delivered_effective_settings_match_the_file_and_ignore_later_edits() {
    let source = live_settings_source();
    let scratch = ScratchDir::new("routing-managed-versus-file");
    let file = scratch.path().join("file.jsonc");
    let path = scratch.path().join("managed.jsonc");
    std::fs::write(&file, &source).unwrap();
    std::fs::write(
        &path,
        r#"{"catalog":{"poll_ms":42},"retention":{"model_route_decisions_days":999}}"#,
    )
    .unwrap();

    let file_settings = config_overrides::live_config(&file);
    let effective = serde_json::from_str(&subc_jsonc::jsonc_to_json(&source).unwrap()).unwrap();
    let view = ConfigView::new(
        4,
        effective,
        BTreeMap::new(),
        vec![snapshot(&source)],
        ConfigStatus::Active,
    );
    let managed = Settings {
        module_id: "striatum".into(),
        path: path.clone(),
    };
    managed.config_apply(&view).unwrap();
    let applied = config_overrides::live_config(&path);
    assert!(
        file_settings.prepared_settings() == applied.prepared_settings(),
        "managed and file snapshots differ"
    );
    assert_eq!(
        crate::retention::retention_windows(&file),
        crate::retention::retention_windows(&path)
    );

    std::fs::write(&path, r#"{"catalog":{"poll_ms":43}}"#).unwrap();
    assert!(Arc::ptr_eq(&applied, &config_overrides::live_config(&path)));
    let bad = ConfigView::new(
        5,
        json!({"catalog":{"poll_ms":0}}),
        BTreeMap::new(),
        vec![],
        ConfigStatus::Pending,
    );
    assert!(managed.config_apply(&bad).is_err());
    assert!(Arc::ptr_eq(&applied, &config_overrides::live_config(&path)));
}
