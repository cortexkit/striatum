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
    assert_eq!(
        tuple.effective["consumers"]["pinned"]["pin"],
        json!({"model": "p/m"})
    );
    assert_eq!(
        tuple.effective["panels"]["audit"],
        json!({"top_k": 2, "by": "iq", "extra": 0})
    );
    for path in [
        "/concurrency/providers/p/limit",
        "/concurrency/providers/q/limit",
        "/consumers/pinned/pin/model",
        "/panels/audit/top_k",
        "/panels/audit/by",
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
        (
            json!({"consumers":{"x":{"pin":{}}}}),
            "/consumers/x/pin/model",
        ),
        (
            json!({"panels":{"audit":{"top_k":9,"by":"iq"}}}),
            "/panels/audit/top_k",
        ),
        (
            json!({"panels":{"audit":{"top_k":2,"by":"iq","topK":2}}}),
            "/panels/audit/topK",
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

#[test]
fn project_review_accepts_scoped_tiers_and_an_unenrolled_runner() {
    let mut workspace = ConfigLayerSnapshot::file(
        "striatum",
        ConfigLayer::Workspace,
        "/isolated/workspace/striatum.jsonc",
        Some(br#"{"consumers":{"prefrontal.sidekick":{"service_tier":"fast"}}}"#.to_vec()),
    );
    workspace.project_id = Some("pj-review".into());
    workspace.workspace_id = Some("workspace".into());
    let mut project = ConfigLayerSnapshot::file(
        "striatum", ConfigLayer::Project, "/isolated/project/striatum.jsonc",
        Some(br#"{"consumers":{"prefrontal.sidekick":{"runners":[{"module":"not-enrolled","harness":"opencode2","routes":["openai"]}],"service_tiers":{"openai":"ultrafast"}}}}"#.to_vec()),
    );
    project.project_id = workspace.project_id.clone();
    let mut user = snapshot("{}");
    user.project_id = project.project_id.clone();
    let reply = review(
        &review_schema(),
        "striatum",
        ConfigReviewRequest::new(0, vec![user, workspace, project], None),
    );
    assert!(reply.refusals.is_empty(), "{:?}", reply.refusals);
    for (path, layer) in [
        (
            "/consumers/prefrontal.sidekick/service_tier",
            Layer::Workspace,
        ),
        ("/consumers/prefrontal.sidekick/runners", Layer::Project),
        (
            "/consumers/prefrontal.sidekick/service_tiers/openai",
            Layer::Project,
        ),
    ] {
        let field = reply
            .fields
            .iter()
            .find(|field| field.path == path)
            .unwrap();
        assert_eq!(field.project_id.as_deref(), Some("pj-review"));
        assert_eq!(field.new_source, layer);
    }
}

#[tokio::test]
async fn hello_ack_enables_project_settings_without_a_probe() {
    use super::test_daemon::{Advertise, Daemon, ID};
    let dir = ScratchDir::new("project-capability-hello");
    let path = dir.join("routing.jsonc");
    let (daemon, _connection) = Daemon::connect(&path, json!({}), Advertise::Hello).await;
    daemon.set(
        ID,
        json!({"consumers":{"prefrontal.sidekick":{"service_tier":"fast"}}}),
    );
    for _ in 0..2 {
        assert!(project_view(&path, ID).await.is_some());
    }
    assert_eq!(daemon.gets(ID), 1);
    assert_eq!(daemon.module_describes(), 0);
    assert_eq!(daemon.consumer_connections(), 0);
}

#[tokio::test]
async fn consumer_probe_without_a_capability_or_valid_reply_leaves_project_views_off() {
    use super::test_daemon::{Advertise, Daemon, ID};
    for (advertise, reason) in [
        (
            Advertise::Neither,
            "daemon did not advertise project settings",
        ),
        (Advertise::Refused, "consumer capability probe failed"),
        (Advertise::Failed, "consumer capability probe failed"),
    ] {
        crate::logging::take_operator_infos();
        let dir = ScratchDir::new("project-capability");
        let path = dir.join("routing.jsonc");
        let (daemon, _connection) = Daemon::connect(&path, json!({}), advertise).await;
        let mut logs = Vec::new();
        wait_for_probe(|| {
            logs.extend(crate::logging::take_operator_infos());
            disabled_logs(&logs).next().is_some()
        })
        .await;
        assert!(project_view(&path, ID).await.is_none());
        assert_eq!(daemon.gets(ID), 0);
        assert_eq!(daemon.describes(), 1);
        assert_eq!(daemon.module_describes(), 0);
        let reasons: Vec<_> = disabled_logs(&logs).collect();
        assert_eq!(reasons.len(), 1, "{logs:?}");
        assert!(reasons[0].contains(reason), "{logs:?}");
    }
}

#[tokio::test]
async fn unstamped_user_file_reloads_slashed_clef_model_keys() {
    use super::test_daemon::{Advertise, Daemon};
    use crate::DEFAULT_MODULE_ID;
    use std::time::Duration;

    let dir = ScratchDir::new("managed-clef-model-keys");
    let path = dir.join("striatum.jsonc");
    std::fs::write(&path, "{}").unwrap();
    let (daemon, _connection) = Daemon::connect(&path, json!({}), Advertise::Hello).await;
    let initial = config_overrides::live_config(&path);
    let keys = [
        "cloudflare-workers-ai/@cf/cloudflare/clef",
        "cloudflare-workers-ai/@cf/cloudflare/clef-flash",
    ];
    for key in keys {
        assert!(initial.prepared_settings()["models"][key].is_null());
    }
    std::fs::write(
        &path,
        r#"{
            // Restrict these catalog models to classification.
            "models": {
                "cloudflare-workers-ai/@cf/cloudflare/clef": {"only_for": ["broca.classifier"], "pin_only": false},
                "cloudflare-workers-ai/@cf/cloudflare/clef-flash": {"only_for": ["broca.classifier"], "pin_only": false},
            }
        }"#,
    )
    .unwrap();
    daemon.reload_user_file(DEFAULT_MODULE_ID).await.unwrap();
    let applied = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let active = config_overrides::live_config(&path);
            if !Arc::ptr_eq(&initial, &active) {
                break active.prepared_settings();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("routing did not apply the unstamped view");
    for key in keys {
        assert_eq!(
            applied["models"][key]["only_for"],
            json!(["broca.classifier"])
        );
        assert_eq!(applied["models"][key]["pin_only"], false);
    }

    for key in ["provider/model/", "provider//model", "provider/ /model"] {
        std::fs::write(&path, json!({"models": {(key): {}}}).to_string()).unwrap();
        let error = daemon
            .reload_user_file(DEFAULT_MODULE_ID)
            .await
            .unwrap_err();
        assert!(error.contains("/models/"), "{error}");
    }
    assert_eq!(
        daemon.global_gets(),
        2,
        "invalid edits must not send a change hint"
    );
}

#[tokio::test]
async fn slow_consumer_probe_does_not_delay_global_settings_and_recovers_on_retry() {
    use super::test_daemon::{Advertise, Daemon, ID};
    use crate::tier_settings::Tier;
    crate::logging::take_operator_infos();
    let dir = ScratchDir::new("slow-project-capability");
    let path = dir.join("routing.jsonc");
    let user = json!({"consumers":{"prefrontal.sidekick":{"service_tier":"standard"}}});
    let (daemon, _connection) = Daemon::connect(&path, user, Advertise::Slow).await;
    daemon.set(
        ID,
        json!({"consumers":{"prefrontal.sidekick":{"service_tier":"fast"}}}),
    );
    wait_for_probe(|| daemon.describes() == 1).await;
    let startup = crate::logging::take_operator_infos();
    assert!(
        startup
            .iter()
            .any(|line| line.contains("routing settings are ready")),
        "{startup:?}"
    );
    assert!(disabled_logs(&startup).next().is_none(), "{startup:?}");
    assert_eq!(
        config_overrides::live_config(&path).service_tier("prefrontal.sidekick", "openai", "model"),
        Some(Tier::Standard.value())
    );

    wait_for_probe(|| daemon.describes() == 2).await;
    assert!(project_view(&path, ID).await.is_none());
    assert_eq!(daemon.gets(ID), 0);
    daemon.advertise_on_consumer();
    let view = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(view) = project_view(&path, ID).await {
                break view;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("project settings did not become available after retry");
    assert_eq!(
        view.consumers["prefrontal.sidekick"].service_tier,
        Some(Tier::Fast)
    );
    assert_eq!(daemon.describes(), 3);
    assert_eq!(daemon.module_describes(), 0);
    let logs = crate::logging::take_operator_infos();
    let reasons: Vec<_> = disabled_logs(&logs).collect();
    assert_eq!(reasons.len(), 1, "{logs:?}");
    assert!(reasons[0].contains("server.describe timed out"), "{logs:?}");
    assert!(
        logs.iter()
            .any(|line| line.contains("project settings enabled after a retried capability probe")),
        "{logs:?}"
    );
}

fn disabled_logs(logs: &[String]) -> impl Iterator<Item = &str> {
    logs.iter()
        .filter_map(|line| line.strip_prefix("[striatum] project settings disabled: "))
}

async fn wait_for_probe(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("consumer capability probe did not make progress");
}

#[tokio::test]
async fn project_delivery_reports_attempt_scope_and_nack_keeps_only_that_projects_view() {
    use super::test_daemon::{self, Advertise, Daemon, ID};
    use crate::tier_settings::Tier;
    use subc_protocol::config::{ConfigApplyResult, ConfigGetReply};

    let dir = ScratchDir::new("project-delivery");
    let path = dir.join("routing.jsonc");
    let user = json!({"consumers":{"prefrontal.sidekick":{"service_tier":"standard"}}});
    let (daemon, _connection) = Daemon::connect(&path, user.clone(), Advertise::Hello).await;
    let layer = |tier| json!({"consumers":{"prefrontal.sidekick":{"service_tier":tier}}});
    let delivery = |generation, attempt, tier| {
        let mut view = test_daemon::project_view(&user, ID, &layer(tier));
        view.generation = generation;
        view.attempt = Some(attempt);
        view.status = ConfigStatus::Pending;
        ConfigGetReply::new(None, Some(view))
    };
    async fn tier(path: &Path, project: &str) -> Tier {
        project_view(path, project).await.unwrap().consumers["prefrontal.sidekick"]
            .service_tier
            .unwrap()
    }
    daemon.deliver(ID, delivery(3, 5, "fast"));
    daemon.set("other", layer("ultrafast"));
    daemon.no_overrides("plain");
    assert_eq!(tier(&path, ID).await, Tier::Fast);
    assert_eq!(tier(&path, "other").await, Tier::Ultrafast);
    assert!(!project_view(&path, "plain").await.unwrap().applied);
    let reports = daemon.reports();
    assert_eq!(
        reports.len(),
        1,
        "unstamped and unscoped views carry no attempt"
    );
    assert_eq!(reports[0].project_id.as_deref(), Some(ID));
    assert_eq!(
        (
            reports[0].generation,
            reports[0].attempt,
            reports[0].result.clone()
        ),
        (3, 5, ConfigApplyResult::Ack)
    );

    daemon.deliver(ID, delivery(4, 6, "not-a-tier"));
    daemon.changed(ID).await;
    assert_eq!(tier(&path, ID).await, Tier::Fast);
    assert_eq!(tier(&path, "other").await, Tier::Ultrafast);
    assert_eq!(daemon.gets("other"), 1);
    let reports = daemon.reports();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[1].project_id.as_deref(), Some(ID));
    assert_eq!(
        (
            reports[1].generation,
            reports[1].attempt,
            reports[1].result.clone()
        ),
        (4, 6, ConfigApplyResult::Nack)
    );
    assert!(!reports[1].reasons.is_empty());
    assert_eq!(
        config_overrides::live_config(&path).service_tier("prefrontal.sidekick", "openai", "model"),
        Some(Tier::Standard.value())
    );
}

#[tokio::test]
async fn global_settings_notice_refetches_only_the_global_view() {
    use super::test_daemon::{Advertise, Daemon};
    let dir = ScratchDir::new("project-global-notice");
    let path = dir.join("routing.jsonc");
    let (daemon, _connection) = Daemon::connect(&path, json!({}), Advertise::Hello).await;
    for project in ["one", "two"] {
        daemon.no_overrides(project);
        assert!(project_view(&path, project).await.is_some());
    }
    daemon.no_overrides("not-requested");
    daemon.global_changed().await;
    assert_eq!(daemon.global_gets(), 2);
    assert_eq!(daemon.gets("one"), 1);
    assert_eq!(daemon.gets("two"), 1);
    assert_eq!(daemon.gets("not-requested"), 0);
}

macro_rules! captured_project_frame {
    ($file:literal) => {
        include_bytes!(concat!("../../fixtures/subc-project-views-v1/", $file))
    };
}

const CAPTURED_PROJECT: &str = "sample-project";
const CAPTURED_MODULE: &str = "example-router";

#[test]
fn project_wire_capture_checksums_and_scoped_review_reply_are_pinned() {
    use sha2::{Digest, Sha256};
    use subc_protocol::config::ConfigDeclareRequest;
    let sum = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
    let files: [(&str, &[u8]); 9] = [
        ("README.md", captured_project_frame!("README.md")),
        (
            "config-applied-project.stamped.json",
            captured_project_frame!("config-applied-project.stamped.json"),
        ),
        (
            "config-changed-project.stamped.json",
            captured_project_frame!("config-changed-project.stamped.json"),
        ),
        (
            "config-changed-project.unstamped.json",
            captured_project_frame!("config-changed-project.unstamped.json"),
        ),
        (
            "config-declare.json",
            captured_project_frame!("config-declare.json"),
        ),
        (
            "config-get-project-reply.stamped.json",
            captured_project_frame!("config-get-project-reply.stamped.json"),
        ),
        (
            "config-get-project-reply.unstamped.json",
            captured_project_frame!("config-get-project-reply.unstamped.json"),
        ),
        (
            "config-get-unknown-project.json",
            captured_project_frame!("config-get-unknown-project.json"),
        ),
        (
            "config-review-project.stamped.json",
            captured_project_frame!("config-review-project.stamped.json"),
        ),
    ];
    let sums = std::str::from_utf8(captured_project_frame!("SHA256SUMS")).unwrap();
    assert_eq!(sums.lines().count(), files.len());
    for (name, bytes) in files {
        assert!(
            sums.lines()
                .any(|line| line == format!("{}  {name}", sum(bytes))),
            "{name}"
        );
    }
    let declare: ConfigDeclareRequest =
        serde_json::from_slice(captured_project_frame!("config-declare.json")).unwrap();
    let schema = Schema::new(declare.schema).unwrap();
    let fixture: Value = serde_json::from_slice(captured_project_frame!(
        "config-review-project.stamped.json"
    ))
    .unwrap();
    let request: ConfigReviewRequest = serde_json::from_value(fixture["request"].clone()).unwrap();
    assert_eq!(
        request.layers[0].project_id.as_deref(),
        Some(CAPTURED_PROJECT)
    );
    assert_eq!(
        serde_json::to_value(review(&schema, CAPTURED_MODULE, request)).unwrap(),
        fixture["reply"]
    );
}

#[tokio::test]
async fn real_project_get_and_change_frames_drive_the_cache_and_exact_ack() {
    use super::test_daemon::{Advertise, Daemon};
    let dir = ScratchDir::new("captured-project-views");
    let path = dir.join("routing.jsonc");
    let (daemon, _connection) =
        Daemon::connect_as(CAPTURED_MODULE, &path, json!({}), Advertise::Hello).await;
    daemon.captured_reply(
        CAPTURED_PROJECT,
        FrameType::Response,
        captured_project_frame!("config-get-project-reply.unstamped.json"),
    );
    let view = project_view(&path, CAPTURED_PROJECT).await.unwrap();
    assert!(view.applied);
    // The example module's integer setting is not a routing override.
    assert!(view.consumers.is_empty());
    assert!(view.ignored_policy);
    daemon
        .captured_change(captured_project_frame!(
            "config-changed-project.unstamped.json"
        ))
        .await;
    assert!(project_view(&path, CAPTURED_PROJECT).await.unwrap().applied);
    assert_eq!(daemon.gets(CAPTURED_PROJECT), 2);
    assert!(daemon.report_bytes().is_empty());

    let unknown = "unknown-sample-project";
    daemon.captured_reply(
        unknown,
        FrameType::Error,
        captured_project_frame!("config-get-unknown-project.json"),
    );
    assert!(!project_view(&path, unknown).await.unwrap().applied);
    assert_eq!(daemon.gets(unknown), 1);

    daemon.captured_reply(
        CAPTURED_PROJECT,
        FrameType::Response,
        captured_project_frame!("config-get-project-reply.stamped.json"),
    );
    daemon
        .captured_change(captured_project_frame!(
            "config-changed-project.stamped.json"
        ))
        .await;
    assert!(project_view(&path, CAPTURED_PROJECT).await.unwrap().applied);
    assert_eq!(daemon.gets(CAPTURED_PROJECT), 3);
    assert_eq!(daemon.gets(unknown), 1);
    assert_eq!(
        daemon.report_bytes(),
        vec![captured_project_frame!("config-applied-project.stamped.json").to_vec()]
    );
}

#[tokio::test]
async fn failed_project_refresh_keeps_cached_runners_and_tiers_until_a_successful_retry() {
    use super::test_daemon::{Advertise, Daemon, ID};
    use crate::tier_settings::{Layers, Tier};
    use std::time::Duration;

    let dir = ScratchDir::new("project-refresh-failure");
    let path = dir.join("routing.jsonc");
    let consumer = "prefrontal.sidekick";
    let user = json!({
        "runners": [{"module":"project-runner"}],
        "consumers": {consumer: {"service_tier":"standard"}}
    });
    let (daemon, _connection) = Daemon::connect(&path, user, Advertise::Hello).await;
    let config = config_overrides::live_config(&path);
    let layer = |tier, harness| {
        json!({"consumers":{consumer:{
            "service_tier":tier, "runners":[{"module":"project-runner","harness":harness}]
        }}})
    };
    let assert_settings =
        |view: Option<Arc<crate::project_settings::Overrides>>, tier: Tier, harness: &str| {
            let layers = view.map_or_else(Layers::default, |view| {
                Layers::new(view.consumers.clone(), Some(config.v1_runners()))
            });
            let selected_tier = layers.resolve(&config, consumer, "openai", "model", None);
            let runners = layers.runners(consumer, &config, None);
            let chosen_harness = runners
                .as_ref()
                .and_then(|runners| runners.first()?.harness.as_deref());
            assert_eq!(
                (selected_tier, chosen_harness),
                (Some(tier.value()), Some(harness))
            );
        };

    daemon.set(ID, layer("fast", "old-harness"));
    assert_settings(project_view(&path, ID).await, Tier::Fast, "old-harness");
    tokio::time::pause();

    daemon.refuse(ID);
    daemon.changed(ID).await;
    assert_settings(project_view(&path, ID).await, Tier::Fast, "old-harness");
    assert_eq!(daemon.gets(ID), 2);

    daemon.delay(ID);
    tokio::time::advance(Duration::from_secs(2)).await;
    // One lookup retries while the other arrives before the daemon answers.
    let (first, concurrent) = tokio::join!(project_view(&path, ID), project_view(&path, ID));
    assert_settings(first, Tier::Fast, "old-harness");
    assert_settings(concurrent, Tier::Fast, "old-harness");
    assert_eq!(daemon.gets(ID), 3);

    daemon.set(ID, layer("ultrafast", "new-harness"));
    assert_settings(project_view(&path, ID).await, Tier::Fast, "old-harness");
    assert_eq!(daemon.gets(ID), 3);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_settings(
        project_view(&path, ID).await,
        Tier::Ultrafast,
        "new-harness",
    );
    assert_eq!(daemon.gets(ID), 4);
}

async fn wait_for_new_global(path: &Path, before: &Arc<config_overrides::LiveConfig>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while Arc::ptr_eq(before, &config_overrides::live_config(path)) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("routing did not apply the new user settings");
}

fn project_tier(
    path: &Path,
    view: &crate::project_settings::Overrides,
    consumer: &str,
    provider: &str,
) -> Option<cortexkit_role_routing::service_tier::ServiceTier> {
    let config = config_overrides::live_config(path);
    crate::tier_settings::Layers::new(view.consumers.clone(), None)
        .resolve(&config, consumer, provider, "model", None)
}

#[tokio::test]
async fn user_tier_edit_reaches_a_cached_project_that_only_adds_its_own_tier() {
    use super::test_daemon::{Advertise, Daemon, ID};
    use crate::tier_settings::Tier;
    use crate::DEFAULT_MODULE_ID;

    let dir = ScratchDir::new("project-inherited-tiers");
    let path = dir.join("striatum.jsonc");
    let consumer = "prefrontal.sidekick";
    let user = |tier| json!({"consumers":{consumer:{"service_tiers":{"openai":tier}}}});
    std::fs::write(&path, user("fast").to_string()).unwrap();
    let (daemon, _connection) = Daemon::connect(&path, user("fast"), Advertise::Hello).await;
    daemon.set(
        ID,
        json!({"consumers":{consumer:{"service_tiers":{"anthropic":"standard"}}}}),
    );
    let view = project_view(&path, ID).await.unwrap();
    assert_eq!(
        project_tier(&path, &view, consumer, "openai"),
        Some(Tier::Fast.value())
    );
    assert_eq!(
        project_tier(&path, &view, consumer, "anthropic"),
        Some(Tier::Standard.value())
    );

    // The operator changes the user tier; the daemon sends only a global notice.
    let before = config_overrides::live_config(&path);
    std::fs::write(&path, user("standard").to_string()).unwrap();
    daemon.reload_user_file(DEFAULT_MODULE_ID).await.unwrap();
    wait_for_new_global(&path, &before).await;
    let view = project_view(&path, ID).await.unwrap();
    assert_eq!(daemon.gets(ID), 1, "the project view stays cached");
    assert_eq!(
        project_tier(&path, &view, consumer, "openai"),
        Some(Tier::Standard.value()),
        "no scoped layer set the openai tier"
    );
    assert_eq!(
        project_tier(&path, &view, consumer, "anthropic"),
        Some(Tier::Standard.value())
    );
}

#[tokio::test]
async fn project_view_built_on_another_user_layer_keeps_routings_user_values() {
    use super::test_daemon::{self, Advertise, Daemon, ID};
    use crate::tier_settings::Tier;
    use subc_protocol::config::ConfigGetReply;

    let dir = ScratchDir::new("project-user-skew");
    let path = dir.join("striatum.jsonc");
    let applied = json!({"consumers":{"prefrontal.sidekick":{"service_tier":"standard"}}});
    let (daemon, _connection) = Daemon::connect(&path, applied, Advertise::Hello).await;
    // The daemon merged this project over a user layer routing has not applied.
    let skewed = json!({"consumers":{"prefrontal.sidekick":{"service_tier":"fast"}}});
    let layer = json!({"consumers":{"prefrontal.lab":{"service_tier":"ultrafast"}}});
    daemon.deliver(
        ID,
        ConfigGetReply::new(Some(test_daemon::project_view(&skewed, ID, &layer)), None),
    );
    let view = project_view(&path, ID).await.unwrap();
    assert_eq!(
        view.consumers.keys().collect::<Vec<_>>(),
        ["prefrontal.lab"],
        "only the project's own consumer is a project override"
    );
    assert_eq!(
        project_tier(&path, &view, "prefrontal.sidekick", "openai"),
        Some(Tier::Standard.value())
    );
    assert_eq!(
        project_tier(&path, &view, "prefrontal.lab", "openai"),
        Some(Tier::Ultrafast.value())
    );
}

#[tokio::test]
async fn project_tiers_resolve_as_before_when_the_user_layers_agree() {
    use super::test_daemon::{Advertise, Daemon, ID};
    use crate::tier_settings::Tier;

    let dir = ScratchDir::new("project-tiers-unchanged");
    let path = dir.join("striatum.jsonc");
    let user = json!({"consumers":{"prefrontal.sidekick":{
        "service_tier":"standard",
        "service_tiers":{"openai":"standard","openai/model":"fast","google":"standard"}
    }}});
    let (daemon, _connection) = Daemon::connect(&path, user, Advertise::Hello).await;
    daemon.set(
        ID,
        json!({"consumers":{
            "prefrontal.sidekick":{"service_tier":"fast","service_tiers":{"openai":"ultrafast"}},
            "prefrontal.sidekick.review":{"service_tiers":{"google":"fast"}}
        }}),
    );
    let view = project_view(&path, ID).await.unwrap();
    for (consumer, provider, tier) in [
        // The user's model entry outranks the project's provider entry.
        ("prefrontal.sidekick", "openai", Tier::Fast),
        ("prefrontal.sidekick", "google", Tier::Standard),
        ("prefrontal.sidekick", "anthropic", Tier::Fast),
        ("prefrontal.sidekick.review", "openai", Tier::Fast),
        ("prefrontal.sidekick.review", "google", Tier::Fast),
        ("prefrontal.sidekick.review", "anthropic", Tier::Fast),
    ] {
        assert_eq!(
            project_tier(&path, &view, consumer, provider),
            Some(tier.value()),
            "{consumer} on {provider}"
        );
    }
}

#[tokio::test]
async fn queued_project_refreshes_do_not_discard_the_cached_view_for_a_request() {
    use super::test_daemon::{Advertise, Daemon, ID};
    use crate::tier_settings::Tier;

    let dir = ScratchDir::new("project-refresh-queue");
    let path = dir.join("striatum.jsonc");
    let consumer = "prefrontal.sidekick";
    let user = json!({"consumers":{consumer:{"service_tier":"standard"}}});
    let (daemon, _connection) = Daemon::connect(&path, user, Advertise::Hello).await;
    daemon.set(ID, json!({"consumers":{consumer:{"service_tier":"fast"}}}));
    let view = project_view(&path, ID).await.unwrap();
    assert_eq!(
        project_tier(&path, &view, consumer, "openai"),
        Some(Tier::Fast.value())
    );

    tokio::time::pause();
    daemon.delay(ID);
    for _ in 0..3 {
        daemon.notify_changed(ID);
    }
    // Wait until the first refresh is fetching; the other two queue behind it.
    while daemon.gets(ID) < 2 {
        tokio::task::yield_now().await;
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    let view = project_view(&path, ID)
        .await
        .expect("the request falls back to the last delivered project view");
    assert_eq!(
        project_tier(&path, &view, consumer, "openai"),
        Some(Tier::Fast.value())
    );
}

#[test]
fn consumer_iq_and_eq_outside_0_to_100_are_refused_by_review_and_check_config() {
    let scratch = ScratchDir::new("consumer-demand-range");
    let path = scratch.join("striatum.jsonc");
    for (document, field) in [
        (r#"{"consumers":{"work":{"iq":101}}}"#, "/consumers/work/iq"),
        (
            r#"{"consumers":{"work":{"eq":-0.5}}}"#,
            "/consumers/work/eq",
        ),
    ] {
        let refusal = review_refusal(document);
        assert_eq!(refusal.paths, [field], "{document}");
        std::fs::write(&path, document).unwrap();
        let error = config_overrides::check_config_offline(&path, "striatum", None).unwrap_err();
        assert!(error.contains("from 0 to 100"), "{document}: {error}");
    }
    for document in [
        r#"{"consumers":{"work":{"iq":0,"eq":100}}}"#,
        r#"{"consumers":{"work":{"iq":37.5}}}"#,
    ] {
        let reply = review(
            &review_schema(),
            "striatum",
            ConfigReviewRequest::new(0, vec![snapshot(document)], None),
        );
        assert!(
            reply.refusals.is_empty(),
            "{document}: {:?}",
            reply.refusals
        );
        std::fs::write(&path, document).unwrap();
        config_overrides::check_config_offline(&path, "striatum", None).unwrap();
    }
}

#[test]
fn project_pin_schema_matches_what_loading_accepts() {
    let schema = emitted_schema();
    let project = &schema["$defs"]["ProjectPolicy"];
    assert_eq!(project["required"], json!(["pin"]));
    let pin = &project["properties"]["pin"];
    assert!(pin.get("default").is_none(), "{pin}");
    assert_eq!(pin["x-ck-required"], json!(true));
    assert!(
        !pin["description"].as_str().unwrap().contains("Leave empty"),
        "{pin}"
    );
    for document in [
        r#"{"consumers":{"mc":{"projects":{"p1":{}}}}}"#,
        r#"{"consumers":{"mc":{"projects":{"p1":{"pin":""}}}}}"#,
    ] {
        review_refusal(document);
        assert!(
            config_overrides::validate_document(document).is_err(),
            "{document}"
        );
    }
    let pinned = r#"{"consumers":{"mc":{"projects":{"p1":{"pin":"p/m"}}}}}"#;
    config_overrides::validate_document(pinned).unwrap();
}

fn scoped_review(project_layer: &str) -> ConfigReviewReply {
    let mut user = snapshot(r#"{"runners":[{"module":"runner-a"}]}"#);
    user.project_id = Some("pj-review".into());
    let mut project = ConfigLayerSnapshot::file(
        "striatum",
        ConfigLayer::Project,
        "/isolated/project/striatum.jsonc",
        Some(project_layer.as_bytes().to_vec()),
    );
    project.project_id = user.project_id.clone();
    review(
        &review_schema(),
        "striatum",
        ConfigReviewRequest::new(0, vec![user, project], None),
    )
}

#[test]
fn project_runner_lists_follow_the_user_runner_list_rules() {
    for (layer, reason) in [
        (
            r#"{"consumers":{"work":{"runners":[{"module":"runner-a","routes":["p/"]}]}}}"#,
            "route \"p/\"",
        ),
        (
            r#"{"consumers":{"work":{"runners":[{"module":"runner-a","routes":["-p"]}]}}}"#,
            "route \"-p\"",
        ),
        (
            r#"{"consumers":{"work":{"runners":[{"module":"runner-a","harness":" "}]}}}"#,
            "non-empty words",
        ),
        (
            r#"{"consumers":{"work":{"runners":[{"module":"runner-a"},{"module":"runner-a"}]}}}"#,
            "listed once",
        ),
    ] {
        let reply = scoped_review(layer);
        assert!(reply.fields.is_empty(), "{layer}");
        assert_eq!(reply.refusals.len(), 1, "{layer}: {:?}", reply.refusals);
        let refusal = &reply.refusals[0];
        assert_eq!(refusal.paths, ["/consumers/work/runners"], "{layer}");
        assert_eq!(refusal.project_id.as_deref(), Some("pj-review"));
        assert!(
            refusal.reasons[0].contains(reason),
            "{layer}: {:?}",
            refusal.reasons
        );
    }
    // An unenrolled runner is still a well-formed choice; enrollment narrows it per request.
    let reply = scoped_review(
        r#"{"consumers":{"work":{"runners":[{"module":"not-enrolled","harness":"h","routes":["p","p/m"]}]}}}"#,
    );
    assert!(reply.refusals.is_empty(), "{:?}", reply.refusals);
}

#[tokio::test]
async fn delivered_project_runner_list_with_a_malformed_route_is_refused() {
    use super::test_daemon::{self, Advertise, Daemon, ID};
    use subc_protocol::config::{ConfigApplyResult, ConfigGetReply};

    let dir = ScratchDir::new("project-runner-route");
    let path = dir.join("striatum.jsonc");
    let user = json!({"runners":[{"module":"runner-a"}]});
    let (daemon, _connection) = Daemon::connect(&path, user.clone(), Advertise::Hello).await;
    let layer = json!({"consumers":{"work":{"runners":[{"module":"runner-a","routes":["p/"]}]}}});
    let mut view = test_daemon::project_view(&user, ID, &layer);
    view.generation = 2;
    view.attempt = Some(4);
    view.status = ConfigStatus::Pending;
    daemon.deliver(ID, ConfigGetReply::new(None, Some(view)));
    assert!(project_view(&path, ID).await.is_none());
    let reports = daemon.reports();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].result, ConfigApplyResult::Nack);
    assert!(
        reports[0]
            .reasons
            .iter()
            .any(|reason| reason.contains("route \"p/\"")),
        "{:?}",
        reports[0].reasons
    );
}
