use super::super::request_cpu_bench::cpu_us;
use super::*;

#[tokio::test]
#[ignore = "one-minute seam idle and 160 observations/minute CPU measurement"]
async fn fact_transport_cpu_at_live_rate() {
    let (harness, _runner, _) = fixture().await;
    harness.edit_config(|config| config["consumers"]["mason"]["owner"] = json!("prefrontal"));
    let owner = harness.bind(reserved("prefrontal"), None).await;
    let selected = result(
        owner
            .call(
                "route.select",
                &json!({"consumer":"mason", "pin":"synthetic/model", "requirements":{"iq":0}}),
            )
            .await,
    );
    let handler = harness.fact_handler();
    handler.quota_cache.lock().unwrap().consumer.attached = true;
    let mut reconnect = crate::quota_bus::reconnect::Backoff::new(std::time::Instant::now(), 43);
    let (bus, mut cursor) = bus(&handler).await;
    let bytes = publish(&bus, "broca", "finish", &finish(false)).await;
    let store = handler.store.clone();
    let (ready, waiting) = std::sync::mpsc::channel();
    let contention = std::thread::spawn(move || {
        store
            .account_command(striatum_store::AccountCommandKind::Maintenance, move |_| {
                ready.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(100));
                Ok(())
            })
            .unwrap();
    });
    waiting.recv().unwrap();
    let start = std::time::Instant::now();
    consume(&handler, &mut cursor, &bytes).await;
    reconnect.healthy();
    let contention_ms = start.elapsed().as_secs_f64() * 1000.;
    contention.join().unwrap();

    let (_sender, mut receiver) = tokio::sync::mpsc::channel::<()>(1);
    let mut health = tokio::time::interval(Duration::from_secs(10));
    let quiet_start = cpu_us();
    let end = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(end);
    loop {
        tokio::select! {
            _ = &mut end => break,
            _ = receiver.recv() => panic!("quiet reader received an event"),
            _ = health.tick() => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }
        crate::quota_events::observe_binding(
            &handler.quota_cache,
            &mut reconnect,
            std::time::Instant::now(),
        );
    }
    let idle_cpu_us = cpu_us() - quiet_start;
    let mut interval = tokio::time::interval(Duration::from_millis(375));
    let cpu_start = cpu_us();
    let mut worst_ms = 0f64;
    for number in 0..160 {
        interval.tick().await;
        let mut observation = finish(false);
        observation["params"]["event_id"] = json!(format!("live-{number}"));
        let bytes = publish(&bus, "broca", &format!("live-{number}"), &observation).await;
        let start = std::time::Instant::now();
        consume(&handler, &mut cursor, &bytes).await;
        crate::quota_events::observe_binding(
            &handler.quota_cache,
            &mut reconnect,
            std::time::Instant::now(),
        );
        reconnect.healthy();
        worst_ms = worst_ms.max(start.elapsed().as_secs_f64() * 1000.);
        if number % 8 == 0 {
            let report = json!({"op":"route.report", "params":{"decision_id":selected["decision_id"], "outcome":"completed", "usage":{"run_id":format!("report-{number}"), "output_tokens":20}}});
            let bytes = publish(&bus, "prefrontal", &format!("report-{number}"), &report).await;
            let start = std::time::Instant::now();
            consume(&handler, &mut cursor, &bytes).await;
            crate::quota_events::observe_binding(
                &handler.quota_cache,
                &mut reconnect,
                std::time::Instant::now(),
            );
            reconnect.healthy();
            worst_ms = worst_ms.max(start.elapsed().as_secs_f64() * 1000.);
        }
    }
    let active_cpu_us = cpu_us() - cpu_start;
    println!("transport seam: events=180 (160 observations + 20 reports) cpu_us/event={:.3} idle_cpu_us/60s={} worst_apply_ms={:.3} writer_contention_apply_ms={:.3}; seam idle CPU, not SDK/NATS idle", active_cpu_us as f64 / 180., idle_cpu_us, worst_ms, contention_ms);
    assert_eq!(cursor.acks, 181);
}
