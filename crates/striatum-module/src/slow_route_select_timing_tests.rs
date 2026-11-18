//! Timing tests check the diagnostic stages of slow legacy route selection.

use super::*;

#[test]
fn injected_snapshot_delay_is_emitted_in_the_slow_select_breakdown() {
    let timing = RouteSelectTiming::new();
    timing.is_select.set(true);
    let started = Instant::now();
    std::thread::sleep(Duration::from_millis(1_050));
    let snapshot = started.elapsed();
    timing.add_snapshot_closure(snapshot);

    let now = Instant::now();
    let line = timing
        .line_at(now)
        .expect("the injected delay exceeds one second");
    let emitted = rate_limit_route_select_line(line, now, &mut RouteSelectLogLimiter::default())
        .expect("the first slow select is emitted");
    assert!(emitted.contains("slow route.select total_ms="), "{emitted}");
    assert!(emitted.contains("live_config_check_ms="), "{emitted}");
    assert!(
        emitted.contains(&format!("snapshot_closure_ms={}", snapshot.as_millis())),
        "{emitted}"
    );
    assert!(snapshot.as_millis() >= 1_000);
    assert!(emitted.contains("selection_ms="), "{emitted}");
    assert!(emitted.contains("reply_ms="), "{emitted}");
}

#[test]
fn every_stage_bucket_is_named_and_subtracted_from_unclassified() {
    let timing = RouteSelectTiming::new();
    timing.is_select.set(true);
    let stages = [
        ("live_config_check_ms", &timing.live_config_check_ms),
        ("billing_wait_ms", &timing.billing_wait_ms),
        ("resolve_ms", &timing.resolve_ms),
        ("snapshot_closure_ms", &timing.snapshot_closure_ms),
        ("pricing_ms", &timing.pricing_ms),
        ("policy_ms", &timing.policy_ms),
        ("head_history_ms", &timing.head_history_ms),
        ("selection_ms", &timing.selection_ms),
        ("ladder_ms", &timing.ladder_ms),
        ("decision_write_ms", &timing.decision_write_ms),
        ("runner_ms", &timing.runner_ms),
        ("finish_ms", &timing.finish_ms),
        ("reply_ms", &timing.reply_ms),
    ];
    for (index, (_, bucket)) in stages.iter().enumerate() {
        add_stage_ms(bucket, Duration::from_millis(100 + index as u64));
    }
    let line = timing
        .line_at(timing.started + Duration::from_millis(3_000))
        .expect("three seconds is slow");
    let classified: u64 = (0..stages.len() as u64).map(|index| 100 + index).sum();
    for (index, (name, _)) in stages.iter().enumerate() {
        assert!(
            line.contains(&format!(" {name}={} ", 100 + index)),
            "{name}: {line}"
        );
    }
    assert!(
        line.ends_with(&format!("unclassified_ms={}", 3_000 - classified)),
        "{line}"
    );
}

#[test]
fn slow_select_log_rate_limit_counts_suppressed_requests() {
    let start = Instant::now();
    let mut limiter = RouteSelectLogLimiter::default();
    assert_eq!(
        rate_limit_route_select_line("first".into(), start, &mut limiter),
        Some("first".into())
    );
    assert_eq!(
        rate_limit_route_select_line(
            "suppressed".into(),
            start + Duration::from_secs(1),
            &mut limiter,
        ),
        None
    );
    assert_eq!(
        rate_limit_route_select_line("next".into(), start + Duration::from_secs(10), &mut limiter,),
        Some("next (suppressed 1 similar lines)".into())
    );
}
