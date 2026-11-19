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

// The SDK must accept this schema on its own. It uses only the routing fields
// exercised here, without relying on optional scalar declarations.
fn review_schema() -> Schema {
    Schema::new(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object", "unevaluatedProperties": false,
        "x-ck-layers": ["user"], "x-ck-apply": "live",
        "properties": {
            "catalog": {"type": "object", "unevaluatedProperties": false, "properties": {
                "poll_ms": {"type": "integer", "description": "Catalog poll interval", "default": 600000}
            }},
            "panels": {"type": "object", "unevaluatedProperties": false, "properties": {
                "audit": {"type": "object", "unevaluatedProperties": false, "properties": {
                    "top_k": {"type": "integer", "description": "Leading models", "default": 3},
                    "extra": {"type": "integer", "description": "Additional models", "default": 0},
                    "by": {"type": "string", "description": "Ranking", "default": "iq"}
                }}
            }}
        }
    })).unwrap()
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
    base.sources
        .insert("/panels/audit/extra".into(), Layer::User);
    let reply = review(
        &schema,
        "striatum",
        ConfigReviewRequest::new(
            3,
            vec![snapshot(
                "// Catalog refresh\n{\"catalog\":{\"poll_ms\":2000,},}",
            )],
            Some(base),
        ),
    );
    assert!(reply.refusals.is_empty(), "{:?}", reply.refusals);
    assert_eq!(reply.fields.len(), 2);
    let changed = &reply.fields[0];
    assert_eq!(changed.path, "/catalog/poll_ms");
    assert_eq!(changed.old, RecordedValue::new(json!(600000)));
    assert_eq!(changed.new, RecordedValue::new(json!(2000)));
    assert_eq!(
        (changed.old_source, changed.new_source, changed.apply),
        (Layer::Default, Layer::User, ApplyKind::Live)
    );
    let source_only = &reply.fields[1];
    assert_eq!(source_only.path, "/panels/audit/extra");
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
    let refusal = review_refusal(r#"{"panels":{"audit":{"topK":2}}}"#);
    assert_eq!(refusal.paths, ["/panels/audit/topK"]);
}

#[test]
fn review_names_the_fields_in_each_panel_cross_field_rule() {
    let ordering = review_refusal(r#"{"panels":{"audit":{"top_k":3,"by":"iq","extra":1}}}"#);
    assert_eq!(
        ordering.paths,
        ["/panels/audit/extra_by", "/panels/audit/extra"]
    );
    assert!(ordering.reasons[0].contains("extra_by is required"));

    let width = review_refusal(r#"{"panels":{"audit":{"top_k":8,"by":"iq","extra":1}}}"#);
    assert_eq!(width.paths, ["/panels/audit/top_k", "/panels/audit/extra"]);
    assert!(width.reasons[0].contains("top_k + extra must be <= 8"));
}

#[test]
fn review_refuses_a_duplicate_field_even_after_the_tuple_collapses_it() {
    let refusal = review_refusal(r#"{"catalog":{"poll_ms":1000,"poll_ms":2000}}"#);
    assert_eq!(refusal.paths, ["/catalog/poll_ms"]);
    assert!(refusal.reasons[0].contains("duplicate field `poll_ms`"));
}

#[test]
fn delivered_effective_settings_match_the_file_and_ignore_later_edits() {
    let sample = r#"{
        // Keep the non-export case executable in ordinary test runs.
        "catalog": {"poll_ms": 3210},
        "retention": {"model_route_decisions_days": 27},
        "consumers": {"mason": {"ladder": ["p/m"]}},
        "models": {"p/m": {"context_window_tokens": 4096, "output_tokens_per_second": 42}},
        "panels": {"audit": {"top_k": 2, "by": "iq"}}
    }"#;
    let source = std::env::var_os("STRIATUM_TEST_LIVE_SETTINGS")
        .map(|path| std::fs::read_to_string(path).expect("read the isolated live settings export"))
        .unwrap_or_else(|| sample.to_owned());
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
