//! Opt-in, deterministic request mix; timings exclude fixture creation and warmup.

use super::conformance_tests::{install_catalog, reserved, Harness, Route};
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use striatum_core::route_state_store::CredentialRefusal;
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

// Include the blocking request workers, not only the async caller's thread.
// Both OS interfaces report process CPU at 10 ms resolution; aggregate enough
// operations to make that quantization small relative to the measured work.
fn cpu_us() -> i64 {
    #[cfg(target_os = "linux")]
    {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = stat.rsplit_once(')').unwrap().1.split_whitespace().collect();
    (fields[11].parse::<i64>().unwrap() + fields[12].parse::<i64>().unwrap()) * 10_000
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("/bin/ps")
            .args(["-o", "time=", "-p", &std::process::id().to_string()]).output().unwrap();
        assert!(output.status.success());
        let time = String::from_utf8(output.stdout).unwrap();
        let seconds: f64 = time.trim().split(':').rev().enumerate()
            .map(|(n, part)| part.parse::<f64>().unwrap() * 60_f64.powi(n as i32)).sum();
        (seconds * 1_000_000.0).round() as i64
    }
}

async fn call(route: &Route, op: &str, params: Value, elapsed: &mut i64) -> Value {
    let start = cpu_us();
    let (ty, body) = route.call(op, &params).await;
    *elapsed += cpu_us() - start;
    assert_eq!(ty, FrameType::Response, "{op}: {body}");
    body["result"].clone()
}

fn phase(name: &str, count: usize, mut op: impl FnMut()) {
    let start = cpu_us();
    for _ in 0..count { op(); }
    let total = cpu_us() - start;
    println!("CPU phase {name}: {total} us / {count} operations = {:.1} us/op", total as f64 / count as f64);
}

#[tokio::test]
#[ignore = "CPU measurement: cargo test --release production_sized_request_cpu -- --ignored --nocapture"]
async fn production_sized_request_cpu() {
    let now = 1_900_000_000_000;
    let _clock = crate::InjectedClock::set(now);
    let harness = Harness::new("request-cpu");
    harness.edit_config(|config| {
        config["consumers"] = json!({fixture::SUMMARY: {
            "owner": fixture::CONSUMER.trim_start_matches("reserved:")
        }});
    });
    let mut catalog = fixture::catalog();
    let model = catalog.models[0].clone();
    catalog.models = (0..800).map(|i| {
        let mut model = model.clone();
        model.provider = format!("cpu-provider-{}", i / 80);
        model.model = format!("model-{}", i % 80);
        model.priced_per_token = false;
        model.iq = 100;
        model
    }).collect();
    let account = catalog.accounts[0].clone();
    catalog.accounts = (0..40).map(|i| {
        let mut account = account.clone();
        account.provider = format!("cpu-provider-{}", i / 4);
        account.account_id = format!("account-{i}");
        account.credential_id = format!("oauth:cpu-{i}");
        account.method = "oauth".into();
        account
    }).collect();
    catalog.runners.truncate(1);
    catalog.runners[0].route_keys = catalog.models.iter().map(|m| m.route_key()).collect();
    install_catalog(&harness, &catalog);
    harness.edit_config(|config| config["runners"][0]["via_vault"] = json!(true));
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        let mut rows = credentials.listed_credentials().to_vec();
        for row in &mut rows { row.account_id = None; }
        credentials.apply_listing(&cortexkit_claustrum_client::ScopedCredentialListing {
            view: "cpu-joined".into(), credentials: rows, ..Default::default()
        }, now);
        let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
        for reading in quota.readings.readings.values_mut() {
            let account = catalog.accounts.iter().find(|a| a.account_id == reading.account_id).unwrap();
            reading.entry["credentialRef"] = json!(account.credential_id);
            let index: usize = account.account_id.strip_prefix("account-").unwrap().parse().unwrap();
            if index >= 4 && index % 4 != 3 {
                reading.entry["usage"]["primary"]["usedPercent"] = json!(100.0);
                reading.entry["usage"]["secondary"]["usedPercent"] = json!(100.0);
                harness.store().record_credential_entitlement_fence(&CredentialRefusal {
                    provider_id: account.provider.clone(), credential_id: account.credential_id.clone(),
                    code: "billing_exhausted".into(),
                }, None, now as i64 - 60_000).unwrap();
            }
        }
    }
    let route = harness.bind(reserved(fixture::CONSUMER), None).await;
    let runner = harness.bind(reserved(&catalog.runners[0].module), None).await;
    let select = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},
        "pin":"cpu-provider-0/model-0"});
    let mut warmup = 0;
    call(&route, "route.select", select.clone(), &mut warmup).await;
    if std::env::var_os("STRIATUM_CPU_PROFILE_SELECT").is_some() {
        for _ in 0..300 {
            call(&route, "route.select", select.clone(), &mut warmup).await;
        }
        println!("select-only CPU: {warmup} us");
        return;
    }
    let shared = harness.credentials();
    phase("credential_snapshot", 1000, || {
        std::hint::black_box(shared.lock().unwrap().clone());
    });
    phase("get_facts", 1000, || {
        std::hint::black_box(harness.store().get_facts().unwrap());
    });
    let writes = harness.store().account_write_transaction_count_for_test();
    phase("unchanged_class_context", 100, || {
        std::hint::black_box(crate::account_selection::ClassContext::load(
            harness.store(), &shared, harness.operator_config(), crate::ACCOUNT_QUOTA.get(), now as i64,
        ).unwrap());
    });
    println!("unchanged_class_context write transactions: {}", harness.store().account_write_transaction_count_for_test() - writes);
    let readings = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap().readings.readings.values()
        .map(|r| serde_json::from_value(r.entry.clone()).unwrap()).collect::<Vec<_>>();
    let mut poll = now;
    phase("reading_log_poll_40_accounts", 10, || {
        poll += 1;
        crate::write_quota_readings(harness.store(), &readings, poll).unwrap();
    });
    let batches: usize = std::env::var("STRIATUM_CPU_BATCHES").ok().map(|v| v.parse().unwrap()).unwrap_or(1);
    let mut times = [0; 4];
    for batch in 0..batches {
        let mut decisions = Vec::new();
        for _ in 0..30 {
            decisions.push(call(&route, "route.select", select.clone(), &mut times[1]).await["decision_id"].clone());
        }
        for i in 0..100 {
            let admission = format!("cpu-{batch}-{i}");
            let mut params = json!({"provider":"cpu-provider-0","model":"model-0",
                "conversation_key":admission,"admission_id":admission});
            if i < 30 { params["decision_id"] = decisions[i].clone(); }
            call(&runner, "route.account_for", params, &mut times[0]).await;
        }
        for (i, decision) in decisions.iter().enumerate() {
            let observed = call(&runner, "route.account_observe", json!({
                "event_id":format!("cpu-event-{batch}-{i}"), "kind":"admission_started",
                "admission_id":format!("cpu-{batch}-{i}"), "run_id":format!("cpu-run-{batch}-{i}")
            }), &mut times[2]).await;
            assert_eq!(observed["observation"]["bound"], true);
            call(&route, "route.report", json!({"decision_id":decision,"outcome":"completed",
                "usage":{"run_id":format!("cpu-run-{batch}-{i}"),"output_tokens":20}}), &mut times[3]).await;
        }
    }
    for ((name, total), count) in ["account_for", "select", "observe", "report"].into_iter().zip(times).zip([100,30,30,30]) {
        let count = count * batches;
        println!("CPU {name}: {total} us / {count} operations = {:.1} us/op", total as f64 / count as f64);
    }
}
