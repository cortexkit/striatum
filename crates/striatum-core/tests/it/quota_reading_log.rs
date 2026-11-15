use serde::Deserialize;
use serde_json::json;
use striatum_core::model_resolution::ACCOUNT_PRODUCER;
use striatum_core::quota::quota_poll::{
    admit_reading_intervals, analyze_reading_log, reading_log_rows_for_poll, render_account_key,
    READING_INTERVAL_MIN_ELAPSED_MS,
};
use striatum_core::quota::ProviderUsage;
use striatum_core::route_state_store::{QuotaReadingLogRow, UsageObservation};

const EPOCH_MS: i64 = 1_893_456_000_000;

fn reading(
    account_key: &str,
    window_key: &str,
    observed_minute: i64,
    fetched_minute: Option<i64>,
    raw_used_percent: Option<f64>,
    banked_credit_count: Option<i64>,
) -> QuotaReadingLogRow {
    QuotaReadingLogRow {
        account_key: account_key.into(),
        window_key: window_key.into(),
        observed_at: EPOCH_MS + observed_minute * 60_000,
        used_percent: raw_used_percent.unwrap_or(25.0),
        raw_used_percent,
        resets_at: Some(EPOCH_MS + 24 * 60 * 60_000),
        window_minutes: Some(300),
        banked_credit_count,
        fetched_at: fetched_minute.map(|minute| EPOCH_MS + minute * 60_000),
        stale: None,
    }
}

fn usage(account_key: &str, observed_at: i64, tokens: f64) -> UsageObservation {
    UsageObservation {
        id: format!("usage-{account_key}-{observed_at}"),
        route_key: "openai/gpt-5.4/high".into(),
        capacity_key: "openai/gpt-5.4".into(),
        provider_id: "openai".into(),
        input_tokens: Some(tokens),
        output_tokens: 1.0,
        reasoning_tokens: Some(0.0),
        cached_read_tokens: Some(0.0),
        cache_write_tokens: Some(0.0),
        duration_ms: 1_000.0,
        workload_class: Some("ephemeral".into()),
        account_key: Some(account_key.into()),
        observed_at: observed_at as f64,
    }
}

#[test]
fn decoder_and_poll_rows_keep_raw_banked_stale_and_distinct_window_keys() {
    let entry: ProviderUsage = serde_json::from_value(json!({
        "provider": "codex",
        "apiProvider": "openai",
        "account": "team-a",
        "fetchedAt": "2030-01-01T00:00:00Z",
        "savedResets": { "availableCount": 3 },
        "stale": { "since": "2029-12-31T23:58:00Z", "class": "upstream_failed" },
        "usage": {
            "primary": {
                "usedPercent": 12.0,
                "rawUsedPercent": 17.0,
                "resetsAt": "2030-01-01T05:00:00Z",
                "windowMinutes": 300
            },
            "secondary": {
                "usedPercent": 40.0,
                "rawUsedPercent": 45.0,
                "resetsAt": "2030-01-08T00:00:00Z",
                "windowMinutes": 10080
            }
        }
    }))
    .unwrap();

    let rows = reading_log_rows_for_poll(&entry, "openai", &[], EPOCH_MS);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].account_key, "openai/team-a");
    assert_eq!(rows[0].window_key, "codexbar:codex:primary");
    assert_eq!(rows[1].window_key, "codexbar:codex:secondary");
    assert_ne!(rows[0].window_key, rows[1].window_key);
    assert_eq!(rows[0].used_percent, 12.0);
    assert_eq!(rows[0].raw_used_percent, Some(17.0));
    assert_eq!(rows[0].banked_credit_count, Some(3));
    assert_eq!(rows[0].fetched_at, Some(EPOCH_MS));
    assert_eq!(rows[0].window_minutes, Some(300));
    assert_eq!(
        rows[0]
            .stale
            .as_ref()
            .and_then(|stale| stale.class.as_deref()),
        Some("upstream_failed")
    );
    assert!(admit_reading_intervals(&rows).is_empty());
}

#[test]
fn antigravity_log_uses_the_extracted_per_model_window_key() {
    let entry: ProviderUsage = serde_json::from_value(json!({
        "provider": "antigravity",
        "apiProvider": "google",
        "account": "seat",
        "fetchedAt": "2030-01-01T00:00:00Z",
        "savedResets": { "availableCount": 1 },
        "usage": {
            "extraRateWindows": [{
                "id": "gemini-flash",
                "title": "Gemini 3.5 Flash (High)",
                "window": {
                    "usedPercent": 20.0,
                    "rawUsedPercent": 21.0,
                    "resetsAt": "2030-01-01T05:00:00Z",
                    "windowMinutes": 300
                }
            }]
        }
    }))
    .unwrap();
    let capacity_keys = vec!["google/antigravity-gemini-3.5-flash".to_string()];

    let rows = reading_log_rows_for_poll(&entry, "google", &capacity_keys, EPOCH_MS);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].window_key, "codexbar:antigravity:gemini-flash");
}

#[test]
fn decoder_absence_is_not_approximated_from_effective_utilization() {
    let entry: ProviderUsage = serde_json::from_value(json!({
        "provider": "codex",
        "apiProvider": "openai",
        "account": "team-a",
        "fetchedAt": "2030-01-01T00:00:00Z",
        "savedResets": {},
        "usage": {
            "primary": {
                "usedPercent": 35.0,
                "resetsAt": "2030-01-01T05:00:00Z",
                "windowMinutes": 300
            }
        }
    }))
    .unwrap();

    let mut rows = reading_log_rows_for_poll(&entry, "openai", &[], EPOCH_MS);
    assert_eq!(rows[0].used_percent, 35.0);
    assert_eq!(rows[0].raw_used_percent, None);
    assert_eq!(rows[0].banked_credit_count, None);
    rows.push(reading(
        "openai/team-a",
        "codexbar:codex:primary",
        10,
        Some(10),
        Some(40.0),
        Some(2),
    ));
    assert!(admit_reading_intervals(&rows).is_empty());
}

#[test]
fn window_key_identity_prevents_cross_window_intervals() {
    let rows = vec![
        reading(
            "openai/team-a",
            "codexbar:codex:primary",
            0,
            Some(0),
            Some(10.0),
            Some(2),
        ),
        reading(
            "openai/team-a",
            "codexbar:codex:secondary",
            10,
            Some(10),
            Some(20.0),
            Some(2),
        ),
    ];

    assert!(admit_reading_intervals(&rows).is_empty());
}

#[test]
fn poll_clock_is_distinct_from_source_clock_for_preserved_readings() {
    let rows = (0..5)
        .map(|minute| {
            reading(
                "openai/team-a",
                "primary",
                minute,
                Some(0),
                Some(10.0),
                Some(2),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows.iter()
            .map(|row| row.observed_at)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        5
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.fetched_at)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1
    );
    assert!(admit_reading_intervals(&rows).is_empty());
}

#[test]
fn source_clock_must_advance_and_clear_the_elapsed_floor() {
    let unchanged_source = vec![
        reading("openai/team-a", "primary", 0, Some(0), Some(10.0), Some(2)),
        reading("openai/team-a", "primary", 20, Some(0), Some(20.0), Some(2)),
    ];
    assert!(admit_reading_intervals(&unchanged_source).is_empty());

    let advancing_source = vec![
        reading("openai/team-a", "primary", 0, Some(0), Some(10.0), Some(2)),
        reading(
            "openai/team-a",
            "primary",
            10,
            Some(10),
            Some(20.0),
            Some(2),
        ),
    ];
    let admitted = admit_reading_intervals(&advancing_source);
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].elapsed_ms, READING_INTERVAL_MIN_ELAPSED_MS);

    let absent_source = vec![
        reading("openai/team-a", "primary", 0, None, Some(10.0), Some(2)),
        reading(
            "openai/team-a",
            "primary",
            20,
            Some(20),
            Some(20.0),
            Some(2),
        ),
    ];
    assert!(admit_reading_intervals(&absent_source).is_empty());
}

#[test]
fn anchored_one_minute_polls_produce_at_least_twenty_intervals() {
    let rows = (0..=210)
        .map(|minute| {
            reading(
                "openai/team-a",
                "primary",
                minute,
                Some(minute),
                Some(10.0 + minute as f64 / 100.0),
                Some(2),
            )
        })
        .collect::<Vec<_>>();

    let admitted = admit_reading_intervals(&rows);
    assert_eq!(admitted.len(), 21);
    assert!(admitted
        .iter()
        .all(|interval| interval.elapsed_ms == READING_INTERVAL_MIN_ELAPSED_MS));
}

#[test]
fn dirty_reading_discards_the_spanning_interval_and_reanchors_after_it() {
    let rows = vec![
        reading("openai/team-a", "primary", 0, Some(0), Some(10.0), Some(2)),
        reading("openai/team-a", "primary", 5, Some(5), None, Some(2)),
        reading(
            "openai/team-a",
            "primary",
            10,
            Some(10),
            Some(11.0),
            Some(2),
        ),
        reading(
            "openai/team-a",
            "primary",
            20,
            Some(20),
            Some(12.0),
            Some(2),
        ),
    ];

    let admitted = admit_reading_intervals(&rows);
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].from_observed_at, EPOCH_MS + 10 * 60_000);
    assert_eq!(admitted[0].to_observed_at, EPOCH_MS + 20 * 60_000);
}

#[test]
fn bank_reset_moved_boundary_nonmonotonic_raw_and_missing_evidence_are_dirty() {
    let base = reading("openai/team-a", "primary", 0, Some(0), Some(10.0), Some(2));
    let mut cases = Vec::new();

    let mut bank_reset = reading(
        "openai/team-a",
        "primary",
        10,
        Some(10),
        Some(20.0),
        Some(1),
    );
    cases.push(bank_reset.clone());

    let mut moved_reset = reading(
        "openai/team-a",
        "primary",
        10,
        Some(10),
        Some(20.0),
        Some(2),
    );
    moved_reset.resets_at = Some(EPOCH_MS + 25 * 60 * 60_000);
    cases.push(moved_reset);

    let nonmonotonic = reading("openai/team-a", "primary", 10, Some(10), Some(9.0), Some(2));
    cases.push(nonmonotonic);

    bank_reset.banked_credit_count = None;
    cases.push(bank_reset);

    let missing_raw = reading("openai/team-a", "primary", 10, Some(10), None, Some(2));
    cases.push(missing_raw);

    for candidate in cases {
        assert!(
            admit_reading_intervals(&[base.clone(), candidate]).is_empty(),
            "dirty interval was admitted"
        );
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayFixture {
    account_key: String,
    window_key: String,
    start_observed_at: i64,
    start_fetched_at: i64,
    step_minutes: i64,
    resets_at: i64,
    window_minutes: i64,
    banked_credit_count: i64,
    raw_used_percents: Vec<f64>,
}

#[test]
fn account_producer_false_replay_admits_time_but_never_invents_tokens_or_fit() {
    assert!(
        !ACCOUNT_PRODUCER,
        "fixture is the named producer-deferral branch"
    );
    let fixture: ReplayFixture =
        serde_json::from_str(include_str!("../fixtures/quota-reading-log-replay-v1.json")).unwrap();
    let readings = fixture
        .raw_used_percents
        .iter()
        .enumerate()
        .map(|(index, raw)| {
            let offset = index as i64 * fixture.step_minutes * 60_000;
            QuotaReadingLogRow {
                account_key: fixture.account_key.clone(),
                window_key: fixture.window_key.clone(),
                observed_at: fixture.start_observed_at + offset,
                used_percent: *raw,
                raw_used_percent: Some(*raw),
                resets_at: Some(fixture.resets_at),
                window_minutes: Some(fixture.window_minutes),
                banked_credit_count: Some(fixture.banked_credit_count),
                fetched_at: Some(fixture.start_fetched_at + offset),
                stale: None,
            }
        })
        .collect::<Vec<_>>();
    let usage_rows = (1..=25)
        .map(|index| {
            let account = if index % 2 == 0 {
                fixture.account_key.as_str()
            } else {
                "openai/sibling"
            };
            usage(
                account,
                fixture.start_observed_at + index * 10 * 60_000,
                20_000.0,
            )
        })
        .collect::<Vec<_>>();

    let analysis = analyze_reading_log(&readings, &usage_rows, ACCOUNT_PRODUCER);

    assert_eq!(analysis.time_admitted_intervals.len(), 20);
    assert!(analysis.usage_admitted_intervals.is_empty());
    assert_eq!(analysis.token_population, None);
    assert!(!analysis.token_floor_applied);
    assert_eq!(analysis.fitted_raw_percent_per_token, None);
}

#[test]
fn enabled_usage_join_scopes_tokens_to_the_burning_account() {
    let readings = (0..=20)
        .map(|index| {
            reading(
                "openai/team-a",
                "primary",
                index * 10,
                Some(index * 10),
                Some(10.0 + index as f64),
                Some(2),
            )
        })
        .collect::<Vec<_>>();
    let mut usage_rows = Vec::new();
    for index in 1..=20 {
        let observed_at = EPOCH_MS + index * 10 * 60_000;
        usage_rows.push(usage("openai/team-a", observed_at, 10_000.0));
        usage_rows.push(usage("openai/sibling", observed_at, 1_000_000.0));
    }

    let analysis = analyze_reading_log(&readings, &usage_rows, true);

    assert_eq!(analysis.time_admitted_intervals.len(), 20);
    assert_eq!(analysis.usage_admitted_intervals.len(), 20);
    assert_eq!(analysis.token_population, Some(20));
    let fitted = analysis.fitted_raw_percent_per_token.unwrap();
    assert!((fitted - 1.0 / 10_001.0).abs() < 1e-12);
}

#[test]
fn account_key_has_one_rendering_including_the_empty_account_segment() {
    assert_eq!(
        render_account_key("openai", Some("team-a")),
        "openai/team-a"
    );
    assert_eq!(render_account_key("openai", None), "openai/");
    assert_eq!(render_account_key("openai", Some("")), "openai/");
}
