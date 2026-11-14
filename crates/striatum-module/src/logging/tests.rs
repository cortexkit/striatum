use std::process::Output;
use std::sync::{Arc, Mutex};

use striatum_core::{DecisionOutcomeUpdate, RouteOutcome, RouteStateStore};

const CHILD: &str = "STRIATUM_TEST_LOGGING_CHILD";

fn child(mode: &str, rust_log: Option<&str>, striatum_log: Option<&str>) -> Output {
    let home = cortexkit_test_support::scratch_root()
        .join(format!("striatum-logging-{}-{mode}", std::process::id()));
    std::fs::create_dir_all(&home).unwrap();
    let mut command = cortexkit_test_support::fenced_command(
        &crate::test_exe::current_test_exe().unwrap(),
        &home,
    );
    command
        .args([
            "--exact",
            "logging::subprocess_tests::startup_logging_child",
            "--nocapture",
        ])
        .env(CHILD, mode)
        .env_remove("RUST_LOG")
        .env_remove("STRIATUM_LOG");
    if let Some(spec) = rust_log {
        command.env("RUST_LOG", spec);
    }
    if let Some(spec) = striatum_log {
        command.env("STRIATUM_LOG", spec);
    }
    let output = command.output().unwrap();
    std::fs::remove_dir_all(home).unwrap();
    assert!(output.status.success(), "{output:?}");
    output
}

#[test]
fn startup_logging_child() {
    let Ok(mode) = std::env::var(CHILD) else {
        return;
    };
    // Use the same initialization hook as main, but keep process-wide state out
    // of the parent test runner and its existing thread-local capture tests.
    crate::initialize_logging();
    if mode == "traffic" {
        crate::routing_v1::exercise_normal_traffic_for_logging();
        return;
    }

    let directory = cortexkit_test_support::scratch_root()
        .join(format!("striatum-log-store-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    {
        let store = striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
            crate::DEFAULT_MODULE_ID,
            directory.join("store.db").to_str().unwrap(),
        ))
        .unwrap();
        store
            .set_decision_outcome(
                "missing decision",
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: RouteOutcome::Completed,
                    last_error: None,
                    observed_at_ms: 100,
                    failure_backoff: Default::default(),
                },
            )
            .unwrap();
    }
    std::fs::remove_dir_all(directory).unwrap();

    crate::account_quota::apply_usage_reply(
        &mut Default::default(),
        br#"{"result":[{"provider":7}]}"#,
        100,
    )
    .unwrap();
    tracing::error!(reason = "two words", debug = ?Some(7), count = 2, "line one\nline two");
    tracing::info!("startup info probe");
    tracing::debug!("startup debug probe");
    tracing::trace!("startup trace probe");

    let captured = Arc::new(Mutex::new(Vec::new()));
    {
        let _guard = tracing::subscriber::set_default(Capture(Arc::clone(&captured)));
        tracing::warn!("local warning probe");
        tracing::info!("local info probe");
    }
    assert_eq!(
        *captured.lock().unwrap(),
        [tracing::Level::WARN, tracing::Level::INFO],
        "thread-local subscribers must override the process-wide fallback"
    );
}

#[test]
fn startup_subscriber_emits_store_and_module_warnings_but_not_info() {
    let output = child("events", None, None);
    let stderr = String::from_utf8(output.stderr).unwrap();
    let lines: Vec<_> = stderr.lines().collect();
    assert_eq!(lines.len(), 3, "{stderr}");
    assert_eq!(
        lines[0],
        "WARN route outcome names a decision the ledger does not hold; route health is left unchanged decision_id=\"missing decision\" outcome=completed"
    );
    assert!(
        lines[1].starts_with(
            "WARN account quota: some Insula entries did not decode and were skipped errors="
        ),
        "{stderr}"
    );
    assert_eq!(
        lines[2],
        r#"ERROR line one\nline two reason="two words" debug=Some(7) count=2"#
    );
    assert!(!stderr.contains("info probe"), "{stderr}");
    assert!(!stderr.contains("local warning probe"), "{stderr}");
}

#[test]
fn environment_can_enable_lower_levels_and_module_setting_wins() {
    let output = child("info", Some("info"), None);
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("INFO startup info probe\n"), "{stderr}");
    assert!(!stderr.contains("DEBUG startup debug probe"), "{stderr}");

    let output = child("trace", Some("off"), Some("ck_striatum=trace"));
    let stderr = String::from_utf8(output.stderr).unwrap();
    for line in [
        "INFO startup info probe\n",
        "DEBUG startup debug probe\n",
        "TRACE startup trace probe\n",
    ] {
        assert!(stderr.contains(line), "{stderr}");
    }
}

#[test]
fn normal_handler_traffic_has_bounded_stderr_volume() {
    let output = child("traffic", None, None);
    let stderr = String::from_utf8(output.stderr).unwrap();
    let lines: Vec<_> = stderr.lines().collect();
    let tracing_lines: Vec<_> = lines
        .iter()
        .copied()
        .filter(|line| line.starts_with("WARN ") || line.starts_with("ERROR "))
        .collect();
    eprintln!(
        "normal handler traffic: {} tracing lines, {} existing operator lines, {} stderr lines total",
        tracing_lines.len(),
        lines.len() - tracing_lines.len(),
        lines.len()
    );
    // The fixtures omit the gateway listing grant in three credential caches.
    // Each cache warns once on listing, not once per operation or report.
    assert_eq!(
        tracing_lines,
        ["WARN reserved:striatum needs list on category llm-gateway to discover company gateways; continuing with the scoped credential listing"; 3],
        "unexpected healthy-handler tracing volume: {stderr}"
    );
}

struct Capture(Arc<Mutex<Vec<tracing::Level>>>);

impl tracing::Subscriber for Capture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        self.0.lock().unwrap().push(*event.metadata().level());
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
