#[test]
fn reader_never_requests_stream_metadata() {
    for source in [
        include_str!("quota_bus.rs"),
        include_str!("quota_bus/metadata.rs"),
    ] {
        for forbidden in [
            ".get_stream(",
            ".get_raw_message(",
            ".get_first_raw_message_by_subject(",
        ] {
            assert!(
                !source.contains(forbidden),
                "routing reader must not request stream metadata: {forbidden}"
            );
        }
    }
}

#[tokio::test]
async fn failing_connect_uses_jittered_exponential_bind_backoff() {
    let start = std::time::Instant::now();
    let mut backoff = super::reconnect::Backoff::new(start, 17);
    let mut credentials = 0;
    let mut attempts = Vec::new();
    for milliseconds in (0..300_000).step_by(100) {
        let now = start + std::time::Duration::from_millis(milliseconds);
        let result = backoff
            .attempt(now, async {
                credentials += 1;
                attempts.push(milliseconds);
                Err::<(), _>("injected connect failure".into())
            })
            .await;
        if let Some(result) = result {
            assert!(result.is_err());
        }
    }
    assert!(credentials <= 12, "persistent failure minted {credentials} credentials in five simulated minutes: {attempts:?}");
    assert!(
        attempts[1] >= 800 && attempts[1] <= 1200,
        "first retry is jittered around 1s: {attempts:?}"
    );
    assert!(
        attempts
            .windows(2)
            .skip(6)
            .all(|pair| pair[1] - pair[0] >= 48_000),
        "backoff must remain near the 60s cap: {attempts:?}"
    );
}

#[tokio::test]
async fn consumer_not_found_mints_only_on_bind_backoff_schedule() {
    let start = std::time::Instant::now();
    let mut backoff = super::reconnect::Backoff::new(start, 29);
    let mut minted = 0;
    for seconds in 0..600 {
        let result = backoff
            .attempt(start + std::time::Duration::from_secs(seconds), async {
                minted += 1;
                Err::<(), _>("consumer not found (code 404, error code 10014)".into())
            })
            .await;
        if let Some(result) = result {
            assert!(result.is_err());
        }
    }
    assert!(
        minted <= 18,
        "missing durable minted {minted} credentials in ten simulated minutes"
    );
}

#[test]
fn reconnect_jitter_varies_by_seed_and_resets_after_proven_health() {
    let now = std::time::Instant::now();
    let mut delays = std::collections::BTreeSet::new();
    for seed in 1..=16 {
        let mut backoff = super::reconnect::Backoff::new(now, seed);
        backoff.failed(now);
        delays.insert(backoff.deadline().duration_since(now));
        for _ in 0..8 {
            backoff.failed(now);
        }
        assert!(
            (std::time::Duration::from_secs(48)..=std::time::Duration::from_secs(60))
                .contains(&backoff.deadline().duration_since(now))
        );
        backoff.healthy();
        backoff.failed(now);
        assert!(
            (std::time::Duration::from_millis(800)..=std::time::Duration::from_millis(1200))
                .contains(&backoff.deadline().duration_since(now))
        );
    }
    assert!(
        delays.len() > 1,
        "different processes must not all retry at the same instant"
    );
}

#[test]
fn reconnect_backoff_resets_only_after_sixty_continuously_bound_seconds() {
    let start = std::time::Instant::now();
    let mut backoff = super::reconnect::Backoff::new(start, 41);
    for _ in 0..8 {
        backoff.failed(start);
    }
    let before = start + std::time::Duration::from_secs(59);
    backoff.observe_bound(before, Some(start));
    backoff.failed(before);
    assert!(backoff.deadline().duration_since(before) >= std::time::Duration::from_secs(48));
    let settled = start + std::time::Duration::from_secs(60);
    backoff.observe_bound(settled, Some(start));
    backoff.failed(settled);
    assert!(backoff.deadline().duration_since(settled) <= std::time::Duration::from_millis(1200));
    for _ in 0..8 {
        backoff.failed(settled);
    }
    backoff.observe_bound(settled + std::time::Duration::from_secs(600), None);
    backoff.failed(settled);
    assert!(
        backoff.deadline().duration_since(settled) >= std::time::Duration::from_secs(48),
        "an unbound interval never counts as continuous attachment"
    );
}

#[tokio::test]
async fn attach_then_consumer_not_found_flaps_preserve_bind_backoff() {
    let start = std::time::Instant::now();
    let mut backoff = super::reconnect::Backoff::new(start, 37);
    let mut minted = 0;
    for milliseconds in (0..300_000).step_by(100) {
        let now = start + std::time::Duration::from_millis(milliseconds);
        if let Some(result) = backoff
            .attempt(now, async {
                minted += 1;
                Ok(())
            })
            .await
        {
            result.unwrap();
            // The SDK attached, but its next info observation found no durable.
            backoff.failed(now);
        }
    }
    assert!(
        minted <= 12,
        "attach/not-found flaps minted {minted} credentials in five simulated minutes"
    );
}
