//! Opt-in, deterministic request mix; timings exclude fixture creation and warmup.
//!
//! `STRIATUM_CPU_REPRESENTATIVE` accepts all five whitespace-separated `key=count`
//! parameters named by `FixtureCounts`. With no parameters, only the explicitly
//! labelled smoke fixture runs beside production. Set `STRIATUM_CPU_BASELINE=1`
//! only with operator-supplied counts, never to promote a worker's smoke run.
//! `STRIATUM_CPU_OBSERVATION_HISTORY=N` seeds N prior rows in each observation
//! table before timing (default 0). Two principals and four runners model shared
//! history; half the rows belong to the measured runner's principal. The compact
//! event table is intentionally as large as the log, a conservative stress case
//! rather than a claim about the live retention ratio.

use super::conformance_tests::{install_catalog, reserved, Harness, Route};
use cortexkit_role_routing_conformance::{fixture, Catalog};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;
use striatum_core::route_state_store::CredentialRefusal;
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

pub(crate) const FIXTURE_NOW: u64 = 1_900_000_000_000;
const ADMISSION_BURST_REQUESTS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FixtureCounts {
    pub subscription_rules: usize,
    pub exclusion_rules: usize,
    pub runner_route_rules: usize,
    pub catalog_facts: usize,
    pub listed_credentials: usize,
}

impl FixtureCounts {
    pub(crate) fn parse(parameters: &str) -> Result<Self, String> {
        let mut values = std::collections::BTreeMap::new();
        for parameter in parameters.split_whitespace() {
            let (key, value) = parameter.split_once('=').ok_or("expected key=count")?;
            let value = value
                .parse::<usize>()
                .map_err(|_| format!("invalid count for {key}"))?;
            if values.insert(key, value).is_some() {
                return Err(format!("duplicate count {key}"));
            }
        }
        let mut take = |name| {
            values
                .remove(name)
                .ok_or_else(|| format!("missing count {name}"))
        };
        let result = Self {
            subscription_rules: take("subscription_rules")?,
            exclusion_rules: take("exclusion_rules")?,
            runner_route_rules: take("runner_route_rules")?,
            catalog_facts: take("catalog_facts")?,
            listed_credentials: take("listed_credentials")?,
        };
        if !values.is_empty() {
            return Err("unknown count parameter".into());
        }
        Ok(result)
    }
    fn smoke() -> Self {
        Self {
            subscription_rules: 7,
            exclusion_rules: 5,
            runner_route_rules: 3,
            catalog_facts: 12,
            listed_credentials: 6,
        }
    }
}

pub(crate) struct RequestFixture {
    pub(super) harness: Arc<Harness>,
    pub(crate) catalog: Catalog,
}

impl RequestFixture {
    pub(crate) fn edit_config(&self, edit: impl FnOnce(&mut Value)) {
        self.harness.edit_config(edit);
    }
    pub(crate) fn store(&self) -> &crate::SharedStore {
        self.harness.store()
    }
    pub(crate) fn credentials(&self) -> crate::SharedBrocaCredentials {
        self.harness.credentials()
    }
    pub(crate) fn operator_config(&self) -> Arc<crate::config_overrides::LiveConfig> {
        self.harness.operator_config()
    }
}

// Include the blocking request workers, not only the async caller's thread.
// Both OS interfaces report process CPU at 10 ms resolution; aggregate enough
// operations to make that quantization small relative to the measured work.
pub(crate) fn cpu_us() -> i64 {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
        let fields: Vec<_> = stat
            .rsplit_once(')')
            .unwrap()
            .1
            .split_whitespace()
            .collect();
        (fields[11].parse::<i64>().unwrap() + fields[12].parse::<i64>().unwrap()) * 10_000
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("/bin/ps")
            .args(["-o", "time=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        assert!(output.status.success());
        let time = String::from_utf8(output.stdout).unwrap();
        let seconds: f64 = time
            .trim()
            .split(':')
            .rev()
            .enumerate()
            .map(|(n, part)| part.parse::<f64>().unwrap() * 60_f64.powi(n as i32))
            .sum();
        (seconds * 1_000_000.0).round() as i64
    }
}

pub(crate) fn production_fixture() -> RequestFixture {
    build_fixture(800, 40)
}

fn build_fixture(catalog_facts: usize, listed_credentials: usize) -> RequestFixture {
    let now = FIXTURE_NOW;
    let harness = Harness::new("request-cpu");
    // The measured catalog size excludes the store's unserved seed catalog.
    for row in harness.store().get_catalog_facts().unwrap() {
        harness
            .store()
            .delete_fact(&striatum_core::make_route_key(
                &row.fact.provider_id,
                &row.fact.model_id,
                row.fact.variant.as_deref(),
            ))
            .unwrap();
    }
    harness.edit_config(|config| {
        config["consumers"] = json!({fixture::SUMMARY: {
            "owner": fixture::CONSUMER.trim_start_matches("reserved:")
        }});
    });
    let mut catalog = fixture::catalog();
    let model = catalog.models[0].clone();
    catalog.models = (0..catalog_facts)
        .map(|i| {
            let mut model = model.clone();
            model.provider = format!("cpu-provider-{}", i / 80);
            model.model = format!("model-{}", i % 80);
            model.priced_per_token = false;
            model.iq = 100;
            model
        })
        .collect();
    let account = catalog.accounts[0].clone();
    catalog.accounts = (0..listed_credentials)
        .map(|i| {
            let mut account = account.clone();
            account.provider = format!(
                "cpu-provider-{}",
                (i / 4) % catalog_facts.div_ceil(80).max(1)
            );
            account.account_id = format!("account-{i}");
            account.credential_id = format!("oauth:cpu-{i}");
            account.method = "oauth".into();
            account
        })
        .collect();
    catalog.runners.truncate(1);
    catalog.runners[0].route_keys = catalog.models.iter().map(|m| m.route_key()).collect();
    install_catalog(&harness, &catalog);
    harness.edit_config(|config| config["runners"][0]["via_vault"] = json!(true));
    {
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        let mut rows = credentials.listed_credentials().to_vec();
        for row in &mut rows {
            row.account_id = None;
        }
        credentials.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "cpu-joined".into(),
                credentials: rows,
                ..Default::default()
            },
            now,
        );
        let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
        for reading in quota.readings.readings.values_mut() {
            let account = catalog
                .accounts
                .iter()
                .find(|a| a.account_id == reading.account_id)
                .unwrap();
            reading.entry["credentialRef"] = json!(account.credential_id);
            let index: usize = account
                .account_id
                .strip_prefix("account-")
                .unwrap()
                .parse()
                .unwrap();
            if index >= 4 && index % 4 != 3 {
                reading.entry["usage"]["primary"]["usedPercent"] = json!(100.0);
                reading.entry["usage"]["secondary"]["usedPercent"] = json!(100.0);
                harness
                    .store()
                    .record_credential_entitlement_fence(
                        &CredentialRefusal {
                            provider_id: account.provider.clone(),
                            credential_id: account.credential_id.clone(),
                            code: "billing_exhausted".into(),
                        },
                        None,
                        now as i64 - 60_000,
                    )
                    .unwrap();
            }
        }
    }
    RequestFixture { harness, catalog }
}

/// No representative defaults: callers supply all five measured dimensions.
/// Pool credentials name an alias provider while serving a catalog provider.
pub(crate) fn representative_fixture(counts: FixtureCounts) -> RequestFixture {
    let fixture = build_fixture(counts.catalog_facts, counts.listed_credentials);
    fixture.harness.edit_config(|config| {
        for section in ["providers", "models"] {
            if let Some(entries) = config[section].as_object_mut() {
                for policy in entries.values_mut() {
                    if let Some(policy) = policy.as_object_mut() {
                        policy.remove("subscription");
                        policy.remove("pin_only");
                    }
                }
            }
        }
        for i in 0..counts.subscription_rules {
            let key = fixture
                .catalog
                .models
                .get(i)
                .map(|m| m.route_key())
                .unwrap_or_else(|| format!("cpu-rule-provider/model-{i}"));
            config["models"][key]["subscription"] = json!(true);
        }
        for i in 0..counts.exclusion_rules {
            let key = fixture
                .catalog
                .models
                .get(i)
                .map(|m| m.route_key())
                .unwrap_or_else(|| format!("cpu-rule-provider/excluded-{i}"));
            config["models"][key]["pin_only"] = json!(false);
        }
        config["runners"][0]["routes"] = if counts.runner_route_rules == 0 {
            Value::Null
        } else {
            json!((0..counts.runner_route_rules)
                .map(|i| if i == 0 {
                    "cpu-provider-0".into()
                } else {
                    format!("cpu-rule-provider/route-{i}")
                })
                .collect::<Vec<String>>())
        };
    });
    {
        let shared = fixture.credentials();
        let mut credentials = shared.lock().unwrap();
        let mut rows: Vec<_> = credentials
            .listed_credentials()
            .iter()
            .filter(|row| row.id.starts_with("oauth:cpu-"))
            .cloned()
            .collect();
        rows.truncate(counts.listed_credentials);
        if let Some(row) = rows.last_mut() {
            row.provider_ids = vec!["cpu-provider-0".into(), "cpu-pool-0".into()];
            row.serves = vec!["cpu-provider-0".into()];
            row.auth_method = Some("apikey".into());
            row.credential_type = "apikey".into();
        }
        credentials.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "representative-with-pool-targets".into(),
                credentials: rows,
                ..Default::default()
            },
            FIXTURE_NOW,
        );
        let facts = fixture.harness.store().get_facts().unwrap();
        credentials.refresh_serving_methods(&Default::default(), &facts, FIXTURE_NOW as i64, &[]);
    }
    fixture
}

#[derive(Default)]
struct Samples {
    index_builds: [u64; 3],
    cpu: i64,
    wall: Vec<u128>,
    queue_ns: u64,
    sql_ns: u64,
    commit_ns: u64,
    serialization_ns: u64,
    commits: u64,
    acquisitions: u64,
    refusals: usize,
}

struct Before {
    cpu: i64,
    started: Instant,
    stages: striatum_store::StoreStageTiming,
    serialization: u64,
    commits: u64,
    acquisitions: u64,
}
impl Before {
    fn new(store: &striatum_store::SqliteRouteStateStore) -> Self {
        crate::account_selection::take_index_build_counts();
        Self {
            cpu: cpu_us(),
            started: Instant::now(),
            stages: store.stage_timing(),
            serialization: crate::stage_timing::serialization_ns(),
            commits: store.writer_commit_count_for_test(),
            acquisitions: store.writer_acquisition_count_for_test(),
        }
    }
    fn finish(self, store: &striatum_store::SqliteRouteStateStore, samples: &mut Samples) {
        let wall = self.started.elapsed().as_micros();
        self.finish_with_wall_times(store, samples, [wall]);
    }

    fn finish_with_wall_times(
        self,
        store: &striatum_store::SqliteRouteStateStore,
        samples: &mut Samples,
        wall_times: impl IntoIterator<Item = u128>,
    ) {
        for (total, count) in samples
            .index_builds
            .iter_mut()
            .zip(crate::account_selection::take_index_build_counts())
        {
            *total += count;
        }
        samples.wall.extend(wall_times);
        samples.cpu += cpu_us() - self.cpu;
        let stages = store.stage_timing();
        samples.queue_ns += stages.writer_queue_ns - self.stages.writer_queue_ns;
        samples.sql_ns += stages.sql_ns - self.stages.sql_ns;
        samples.commit_ns += stages.commit_ns - self.stages.commit_ns;
        samples.serialization_ns += crate::stage_timing::serialization_ns() - self.serialization;
        samples.commits += store.writer_commit_count_for_test() - self.commits;
        samples.acquisitions += store.writer_acquisition_count_for_test() - self.acquisitions;
    }
}
impl Samples {
    fn print(&mut self, name: &str) {
        self.wall.sort_unstable();
        let count = self.wall.len();
        if count == 0 {
            return;
        }
        let n = count as f64;
        let percentile = |p: usize| self.wall[(count * p).div_ceil(100).saturating_sub(1)];
        // StoreStageTiming measures connection contention, not command-queue wait.
        println!("{name} n={count} refusals={} cpu_us/op={:.2} p50_us={} p99_us={} connection_wait_us/op={:.2} sql_us/op={:.2} commit_us/op={:.2} serialization_us/op={:.2} commits/op={:.3} writer_acquisitions/op={:.3}", self.refusals,
            self.cpu as f64 / n, percentile(50), percentile(99), self.queue_ns as f64 / n / 1_000.0,
            self.sql_ns as f64 / n / 1_000.0, self.commit_ns as f64 / n / 1_000.0,
            self.serialization_ns as f64 / n / 1_000.0, self.commits as f64 / n, self.acquisitions as f64 / n);
        println!(
            "{name} dynamic_index_builds/op blocks={:.3} readings={:.3} rechecks={:.3}",
            self.index_builds[0] as f64 / n,
            self.index_builds[1] as f64 / n,
            self.index_builds[2] as f64 / n
        );
    }
}

async fn call(
    fixture: &RequestFixture,
    route: &Route,
    op: &str,
    params: Value,
    samples: &mut Samples,
) -> Value {
    let before = Before::new(fixture.store());
    let (ty, body) = route.call(op, &params).await;
    before.finish(fixture.harness.store(), samples);
    if ty != FrameType::Response {
        samples.refusals += 1;
        // Empty model or account lists are valid fixtures, but cannot select or
        // admit. Keep reporting their refusal cost rather than inventing rows.
        assert!(
            fixture.catalog.models.is_empty() || fixture.catalog.accounts.is_empty(),
            "{op}: {body}"
        );
    }
    body["result"].clone()
}

async fn admission_burst(
    fixture: &RequestFixture,
    runner: &Route,
    batch: usize,
    samples: &mut Samples,
) {
    let frames: Vec<_> = (0..ADMISSION_BURST_REQUESTS)
        .map(|i| {
            let admission = format!("cpu-admission-burst-{batch}-{i}");
            runner.request_frame(
                "route.account_for",
                &json!({"provider":"cpu-provider-0", "model":"model-0",
                    "conversation_key":admission, "admission_id":admission}),
            )
        })
        .collect();
    let before = Before::new(fixture.store());
    let mut pending = tokio::task::JoinSet::new();
    for frame in frames {
        let harness = Arc::clone(&fixture.harness);
        let enqueued = Instant::now();
        pending.spawn(async move {
            // The normal frame dispatcher drives each synchronous account
            // handler on the production blocking pool. Calling serve directly
            // would serialize the handlers on this current-thread test runtime.
            let reply = harness.handle(frame).await;
            let body: Value = serde_json::from_slice(&reply.body).unwrap();
            (enqueued.elapsed().as_micros(), reply.header.ty, body)
        });
    }
    let mut wall_times = Vec::with_capacity(ADMISSION_BURST_REQUESTS);
    while let Some(reply) = pending.join_next().await {
        let (wall, ty, body) = reply.expect("admission request task panicked");
        wall_times.push(wall);
        if ty != FrameType::Response {
            samples.refusals += 1;
            assert!(
                fixture.catalog.models.is_empty() || fixture.catalog.accounts.is_empty(),
                "route.account_for: {body}"
            );
        }
    }
    assert_eq!(wall_times.len(), ADMISSION_BURST_REQUESTS);
    // These counters and process CPU are shared by all overlapping requests.
    // Sample them once, but retain each request's enqueue-to-reply latency so
    // Samples::print divides totals by completed requests, not by bursts.
    before.finish_with_wall_times(fixture.store(), samples, wall_times);
}

async fn measure(fixture: RequestFixture, batches: usize) {
    let harness = &fixture.harness;
    let history_rows: usize = std::env::var("STRIATUM_CPU_OBSERVATION_HISTORY")
        .map(|n| n.parse().expect("observation history must be a row count"))
        .unwrap_or(0);
    if history_rows > 0 {
        let principal = format!(
            "reserved:{}",
            fixture.catalog.runners[0]
                .module
                .trim_start_matches("reserved:")
        );
        harness
            .store()
            .account_transaction(|tx| {
                for i in 0..history_rows {
                    tx.insert_observation(&striatum_store::AccountObservationRow {
                        observation_id: format!("history-{i}"),
                        principal: if i % 2 == 0 {
                            principal.clone()
                        } else {
                            "reserved:history-other".into()
                        },
                        runner: format!("history-runner-{}", i % 4),
                        event_id: format!("history-event-{i}"),
                        kind: "advisory".into(),
                        admission_id: None,
                        bound: false,
                        observation_json: "{}".into(),
                        effects_json: "[]".into(),
                        received_at: FIXTURE_NOW as i64 - 1,
                    })?;
                }
                Ok(())
            })
            .unwrap();
        println!("observation_history_rows={history_rows} per_table; two principals, four runners (stress fixture)");
    }
    let route = harness.bind(reserved(fixture::CONSUMER), None).await;
    let runner = harness
        .bind(reserved(&fixture.catalog.runners[0].module), None)
        .await;
    let select = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50}, "pin":"cpu-provider-0/model-0"});
    call(
        &fixture,
        &route,
        "route.select",
        select.clone(),
        &mut Samples::default(),
    )
    .await;
    let mut times: [Samples; 7] = std::array::from_fn(|_| Samples::default());
    for batch in 0..batches {
        let mut decisions = Vec::new();
        for _ in 0..30 {
            decisions.push(
                call(
                    &fixture,
                    &route,
                    "route.select",
                    select.clone(),
                    &mut times[0],
                )
                .await["decision_id"]
                    .clone(),
            );
        }
        for i in 0..100 {
            let admission = format!("cpu-{batch}-{i}");
            let mut params = json!({"provider":"cpu-provider-0","model":"model-0", "conversation_key":admission,"admission_id":admission});
            if i < 30 {
                params["decision_id"] = decisions[i].clone();
            }
            call(
                &fixture,
                &runner,
                "route.account_for",
                params,
                &mut times[1],
            )
            .await;
        }
        for (i, decision) in decisions.iter().enumerate() {
            let observed = call(&fixture, &runner, "route.account_observe", json!({
                "event_id":format!("cpu-event-{batch}-{i}"), "kind":"admission_started",
                "admission_id":format!("cpu-{batch}-{i}"), "run_id":format!("cpu-run-{batch}-{i}")
            }), &mut times[2]).await;
            if !decision.is_null() {
                assert_eq!(observed["observation"]["bound"], true);
            }
            call(
                &fixture,
                &route,
                "route.report",
                json!({"decision_id":decision,"outcome":"completed",
                "usage":{"run_id":format!("cpu-run-{batch}-{i}"),"output_tokens":20}}),
                &mut times[3],
            )
            .await;
        }
        for _ in 0..30 {
            let mut params = select.clone();
            params["service_tier"] = json!("ultrafast");
            let chosen = call(&fixture, &route, "route.select", params, &mut times[4]).await;
            if !chosen.is_null() {
                assert_eq!(chosen["service_tier_chosen"], "standard", "{chosen}");
            }
        }
    }
    let events = 30 * batches;
    let mut burst =
        crate::quota_events::bench::QuotaBurst::new(harness.store().clone(), events).await;
    for _ in 0..events {
        let before = Before::new(harness.store());
        burst.next().await;
        before.finish(harness.store(), &mut times[5]);
    }
    println!(
        "quota burst events={events} applied_bodies={} (includes epoch-start full reply)",
        burst.applied_bodies()
    );
    // Run after the sequential mix so its fixture history stays comparable to
    // earlier measurements. Each burst is fully drained before the next one.
    for batch in 0..batches {
        admission_burst(&fixture, &runner, batch, &mut times[6]).await;
    }
    for (name, samples) in [
        "select",
        "account_for",
        "account_observe",
        "report",
        "tier_fallback",
        "quota_event_burst",
        "admission_burst",
    ]
    .into_iter()
    .zip(&mut times)
    {
        samples.print(name);
    }
    if std::env::var_os("STRIATUM_CPU_WAL_TICKS").is_some() {
        println!(
            "reader_cache_cost added_us/checkout=0 added_us/request=0 configuration=open_only"
        );
        // Maintenance follows the unchanged request mix and is timed separately,
        // so opting in cannot attribute a checkpoint to a request's commit.
        for kind in ["passive", "over_cap_truncate"] {
            if kind == "over_cap_truncate" {
                let payload = "x".repeat(striatum_store::WAL_SIZE_CAP_BYTES as usize + 4096);
                harness
                    .store()
                    .put_quota_snapshot(&payload, FIXTURE_NOW as i64)
                    .unwrap();
            }
            let before = harness.store().stage_timing();
            let acquisitions = harness.store().writer_acquisition_count_for_test();
            let started = Instant::now();
            harness.store().checkpoint_wal().unwrap();
            let checkpoint_us = started.elapsed().as_secs_f64() * 1_000_000.0;
            let after = harness.store().stage_timing();
            println!("wal_tick kind={kind} connection=maintenance checkpoint_us={checkpoint_us:.3} checkpoint_writer_acquisitions={} writer_queue_wait_us={:.3} writer_hold_us={:.3} commit_us={:.3} cap_bytes={} reader_cache_kib={} truncate_wait_bound_ms={}",
                harness.store().writer_acquisition_count_for_test() - acquisitions,
                (after.writer_queue_ns - before.writer_queue_ns) as f64 / 1000.0,
                (after.sql_ns - before.sql_ns + after.commit_ns - before.commit_ns) as f64 / 1000.0,
                (after.commit_ns - before.commit_ns) as f64 / 1000.0,
                striatum_store::WAL_SIZE_CAP_BYTES, striatum_store::READER_CACHE_SIZE_KIB,
                striatum_store::WAL_TRUNCATE_WAIT_BOUND.as_millis());
        }
        // Compare a writer-held passive checkpoint with independent maintenance
        // under concurrent commits. This diagnostic runs after the request mix.
        for connection in ["writer_reference", "maintenance"] {
            let payload = "x".repeat(striatum_store::WAL_SIZE_CAP_BYTES as usize + 4096);
            harness.store().put_quota_snapshot(&payload, 0).unwrap();
            harness.store().checkpoint_wal().unwrap();
            harness
                .store()
                .put_quota_snapshot(&"y".repeat(1024 * 1024), 1)
                .unwrap();
            let stop = std::sync::atomic::AtomicBool::new(false);
            let ready = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let writer = scope.spawn(|| {
                    // One commit precedes the tick, and further commits compete
                    // with it until it finishes. Thread-local timers exclude the
                    // reference checkpoint's own writer acquisition.
                    harness.store().put_quota_snapshot("{}", 0).unwrap();
                    ready.wait();
                    let before = striatum_store::SqliteRouteStateStore::thread_stage_timing();
                    let mut n = 0;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        harness.store().put_quota_snapshot("{}", n + 1).unwrap();
                        n += 1;
                    }
                    let after = striatum_store::SqliteRouteStateStore::thread_stage_timing();
                    (
                        n,
                        after.writer_queue_ns - before.writer_queue_ns,
                        after.commit_ns - before.commit_ns,
                    )
                });
                ready.wait();
                let started = Instant::now();
                if connection == "writer_reference" {
                    harness
                        .store()
                        .with_writer_for_test(|conn| {
                            conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")
                        })
                        .unwrap();
                } else {
                    harness.store().checkpoint_wal().unwrap();
                }
                let checkpoint_us = started.elapsed().as_secs_f64() * 1_000_000.0;
                stop.store(true, std::sync::atomic::Ordering::Relaxed);
                let (commits, queue_ns, commit_ns) = writer.join().unwrap();
                println!("wal_tick_concurrent connection={connection} checkpoint_us={checkpoint_us:.3} writer_commits={commits} writer_queue_wait_us/commit={:.3} commit_us/commit={:.3}",
                    queue_ns as f64 / commits.max(1) as f64 / 1000.0,
                    commit_ns as f64 / commits.max(1) as f64 / 1000.0);
            });
        }
    }
}

#[test]
fn burst_sampler_retains_request_latencies_and_counts_shared_commit_once() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = representative_fixture(FixtureCounts::smoke());
    let mut samples = Samples::default();
    let before = Before::new(fixture.store());
    fixture
        .store()
        .put_quota_snapshot("{}", FIXTURE_NOW as i64)
        .unwrap();
    before.finish_with_wall_times(fixture.store(), &mut samples, 1..=64);
    assert_eq!(samples.wall, (1..=64).collect::<Vec<_>>());
    assert_eq!(samples.commits, 1);
    assert_eq!(samples.acquisitions, 1);
}

#[tokio::test]
async fn admission_burst_samples_64_distinct_requests_without_multiplying_shared_counters() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = representative_fixture(FixtureCounts::smoke());
    let runner = fixture
        .harness
        .bind(reserved(&fixture.catalog.runners[0].module), None)
        .await;
    let store = fixture.store();
    let commits = store.writer_commit_count_for_test();
    let acquisitions = store.writer_acquisition_count_for_test();
    let stages = store.stage_timing();
    let cpu_before = cpu_us();
    let mut samples = Samples::default();
    admission_burst(&fixture, &runner, 0, &mut samples).await;
    assert_eq!(samples.wall.len(), 64);
    admission_burst(&fixture, &runner, 1, &mut samples).await;
    assert_eq!(samples.wall.len(), 128);
    assert_eq!(samples.refusals, 0);
    assert!(samples.cpu <= cpu_us() - cpu_before);
    assert!(samples.commits > 0);
    assert_eq!(
        samples.commits,
        store.writer_commit_count_for_test() - commits
    );
    assert_eq!(
        samples.acquisitions,
        store.writer_acquisition_count_for_test() - acquisitions
    );
    let after = store.stage_timing();
    assert_eq!(
        samples.queue_ns,
        after.writer_queue_ns - stages.writer_queue_ns
    );
    assert_eq!(samples.sql_ns, after.sql_ns - stages.sql_ns);
    assert_eq!(samples.commit_ns, after.commit_ns - stages.commit_ns);
    let principal = format!(
        "reserved:{}",
        fixture.catalog.runners[0]
            .module
            .trim_start_matches("reserved:")
    );
    store
        .account_read(|tx| {
            for batch in 0..2 {
                for i in 0..64 {
                    let admission = format!("cpu-admission-burst-{batch}-{i}");
                    assert!(
                        tx.admission(&principal, &admission)?.is_some(),
                        "{admission}"
                    );
                }
            }
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
#[ignore = "release CPU report: cargo test --release production_sized_request_cpu -- --ignored --nocapture"]
async fn production_sized_request_cpu() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let batches = std::env::var("STRIATUM_CPU_BATCHES")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(1);
    assert!(batches > 0);
    println!("production-sized baseline: 800 facts, 40 credentials");
    measure(production_fixture(), batches).await;
    let counts = match std::env::var("STRIATUM_CPU_REPRESENTATIVE") {
        Ok(parameters) => {
            let counts = FixtureCounts::parse(&parameters).expect("supply all five named counts");
            if std::env::var_os("STRIATUM_CPU_BASELINE").is_some() {
                println!("operator-supplied representative baseline: {parameters}");
            } else {
                println!("representative SMOKE RUN, NOT A BASELINE: {parameters}");
            }
            counts
        }
        Err(std::env::VarError::NotPresent) => {
            println!("representative SMOKE RUN, NOT A BASELINE: {:?}; live representative baseline pending", FixtureCounts::smoke());
            FixtureCounts::smoke()
        }
        Err(error) => panic!("invalid representative parameters: {error}"),
    };
    measure(representative_fixture(counts), batches).await;
}

#[test]
fn request_fixture_builders_preserve_named_counts_and_pool_targets() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let production = production_fixture();
    assert_eq!(
        production
            .harness
            .store()
            .get_catalog_facts()
            .unwrap()
            .len(),
        800
    );
    assert_eq!(
        production
            .harness
            .credentials()
            .lock()
            .unwrap()
            .listed_credentials()
            .len(),
        40
    );
    drop(production);
    for parameters in [
        "subscription_rules=7 exclusion_rules=5 runner_route_rules=3 catalog_facts=12 listed_credentials=6",
        "subscription_rules=17 exclusion_rules=9 runner_route_rules=2 catalog_facts=3 listed_credentials=2",
        "subscription_rules=0 exclusion_rules=0 runner_route_rules=0 catalog_facts=0 listed_credentials=0",
    ] {
        let counts = FixtureCounts::parse(parameters).unwrap();
        let fixture = representative_fixture(counts);
        assert_eq!(fixture.operator_config().rule_counts(), (counts.subscription_rules, counts.exclusion_rules, counts.runner_route_rules));
        assert_eq!(fixture.harness.store().get_catalog_facts().unwrap().len(), counts.catalog_facts);
        let shared = fixture.credentials();
        let credentials = shared.lock().unwrap();
        assert_eq!(credentials.listed_credentials().len(), counts.listed_credentials);
        if counts.listed_credentials > 0 {
            let pool = credentials.listed_credentials().last().unwrap();
            assert!(pool.provider_ids.contains(&"cpu-pool-0".into()));
            assert_eq!(pool.serves, ["cpu-provider-0"]);
        }
    }
    assert!(FixtureCounts::parse("catalog_facts=1").is_err());
    assert!(FixtureCounts::parse("subscription_rules=1 exclusion_rules=2 runner_route_rules=3 catalog_facts=4 listed_credentials=5 unexpected=6").is_err());
}

#[tokio::test]
#[ignore = "release picker CPU report: cargo test --release head_choices_request_cpu -- --ignored --nocapture"]
async fn head_choices_request_cpu() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let parameters =
        std::env::var("STRIATUM_CPU_REPRESENTATIVE").expect("supply all five live counts");
    let counts = FixtureCounts::parse(&parameters).unwrap();
    for production in [true, false] {
        let fixture = if production {
            production_fixture()
        } else {
            representative_fixture(counts)
        };
        super::conformance_tests::install_head_snapshot(&fixture.harness);
        let route = fixture
            .harness
            .bind(reserved(fixture::CONSUMER), None)
            .await;
        let params = json!({"runner_module": fixture.catalog.runners[0].module});
        call(
            &fixture,
            &route,
            "route.head_choices",
            params.clone(),
            &mut Samples::default(),
        )
        .await;
        let mut samples = Samples::default();
        for _ in 0..30 {
            call(
                &fixture,
                &route,
                "route.head_choices",
                params.clone(),
                &mut samples,
            )
            .await;
        }
        println!(
            "head_choices fixture={} counts={parameters}",
            if production {
                "production-sized"
            } else {
                "live representative baseline (STRIATUM_CPU_BASELINE=1)"
            }
        );
        samples.print("head_choices");
    }
}

#[tokio::test]
async fn slow_account_calls_emit_one_anonymous_line_per_operation() {
    assert_eq!(
        crate::stage_timing::SLOW_ACCOUNT_CALL,
        std::time::Duration::from_millis(250)
    );
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = representative_fixture(FixtureCounts::smoke());
    let runner = fixture
        .harness
        .bind(reserved(&fixture.catalog.runners[0].module), None)
        .await;
    crate::stage_timing::LOG_LINES.with(|lines| *lines.borrow_mut() = Some(Vec::new()));
    for (index, slow) in [true, false].into_iter().enumerate() {
        if slow {
            crate::stage_timing::ACCOUNT_DELAY.with(|delay| {
                delay.set(
                    crate::stage_timing::SLOW_ACCOUNT_CALL + std::time::Duration::from_millis(50),
                )
            });
        }
        let params = json!({"provider":"cpu-provider-0","model":"model-0","mode":"admit",
            "conversation_key":format!("secret-conversation-{index}"),"admission_id":format!("secret-admission-{index}")});
        let (ty, body) = fixture
            .harness
            .finish_dispatched(runner.request_frame("route.account_for", &params))
            .await;
        assert_eq!(ty, FrameType::Response, "{body}");
    }
    for (index, slow) in [true, false].into_iter().enumerate() {
        if slow {
            crate::stage_timing::ACCOUNT_DELAY.with(|delay| {
                delay.set(
                    crate::stage_timing::SLOW_ACCOUNT_CALL + std::time::Duration::from_millis(50),
                )
            });
        }
        let params = json!({"event_id":format!("secret-event-{index}"),
            "kind":"admission_started","admission_id":format!("secret-admission-{index}"),"run_id":format!("secret-run-{index}")});
        let (ty, body) = fixture
            .harness
            .finish_dispatched(runner.request_frame("route.account_observe", &params))
            .await;
        assert_eq!(ty, FrameType::Response, "{body}");
    }
    let lines = crate::stage_timing::LOG_LINES.with(|lines| lines.borrow_mut().take().unwrap());
    assert_eq!(lines.len(), 2, "{lines:?}");
    for (line, operation) in lines
        .iter()
        .zip(["route.account_for", "route.account_observe"])
    {
        assert!(
            line.starts_with(&format!("[routing] slow {operation} total_ms=")),
            "{line}"
        );
        for field in [
            "writer_queue_wait_ms=",
            "sql_ms=",
            "commit_ms=",
            "serialization_ms=",
            "provider=\"cpu-provider-0\"",
            "principal=\"reserved:",
        ] {
            assert!(line.contains(field), "{field}: {line}");
        }
        assert!(line.contains(&format!("operation={operation}")), "{line}");
        assert!(
            !line.contains("secret-") && !line.contains("oauth:") && !line.contains("account-"),
            "{line}"
        );
        assert_eq!(line.lines().count(), 1);
    }
    assert!(lines[0].contains("mode=\"admit\""));
}

#[tokio::test]
async fn interleaved_account_calls_log_only_their_own_stage_times() {
    use crate::stage_timing::{AccountProbe, ACCOUNT_PROBE, LOG_LINES, SLOW_ACCOUNT_CALL};
    use striatum_store::StoreStageTiming;
    use tokio::sync::oneshot;

    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let fixture = representative_fixture(FixtureCounts::smoke());
    let runner = fixture
        .harness
        .bind(reserved(&fixture.catalog.runners[0].module), None)
        .await;
    let (a_reached, a_wait) = oneshot::channel();
    let (b_reached, b_wait) = oneshot::channel();
    let (a_resume, a_paused) = oneshot::channel();
    let (b_resume, b_paused) = oneshot::channel();
    let (a_expected, a_stages) = oneshot::channel();
    let (b_expected, b_stages) = oneshot::channel();
    let (a_done, a_finished) = oneshot::channel();
    LOG_LINES.with(|lines| *lines.borrow_mut() = Some(Vec::new()));
    let a = async {
        ACCOUNT_PROBE.with(|probe| {
            *probe.borrow_mut() = Some(AccountProbe {
                injected: StoreStageTiming {
                    writer_queue_ns: 10_000_000,
                    sql_ns: 20_000_000,
                    commit_ns: 30_000_000,
                },
                reached: a_reached,
                resume: a_paused,
                expected: a_expected,
            })
        });
        let params = json!({"provider":"cpu-provider-0","model":"model-0","admission_id":"interleaved-a","conversation_key":"interleaved-a"});
        let (ty, body) = fixture
            .harness
            .finish_dispatched(runner.request_frame("route.account_for", &params))
            .await;
        assert_eq!(ty, FrameType::Response, "{body}");
        a_done.send(()).unwrap();
    };
    let b = async {
        a_wait.await.unwrap();
        ACCOUNT_PROBE.with(|probe| {
            *probe.borrow_mut() = Some(AccountProbe {
                injected: StoreStageTiming {
                    writer_queue_ns: 100_000_000,
                    sql_ns: 200_000_000,
                    commit_ns: 300_000_000,
                },
                reached: b_reached,
                resume: b_paused,
                expected: b_expected,
            })
        });
        let params = json!({"provider":"cpu-provider-0","model":"model-0","admission_id":"interleaved-b","conversation_key":"interleaved-b"});
        let (ty, body) = fixture
            .harness
            .finish_dispatched(runner.request_frame("route.account_for", &params))
            .await;
        assert_eq!(ty, FrameType::Response, "{body}");
    };
    let schedule = async {
        b_wait.await.unwrap();
        tokio::time::sleep(SLOW_ACCOUNT_CALL + std::time::Duration::from_millis(50)).await;
        a_resume.send(()).unwrap();
        a_finished.await.unwrap();
        b_resume.send(()).unwrap();
    };
    // Both futures run on one runtime thread. Each yields with different store
    // timings pending, so thread-total deltas necessarily include another call.
    tokio::join!(a, b, schedule);
    let lines = LOG_LINES.with(|lines| lines.borrow_mut().take().unwrap());
    assert_eq!(lines.len(), 2, "{lines:?}");
    for (line, expected) in lines
        .iter()
        .zip([a_stages.await.unwrap(), b_stages.await.unwrap()])
    {
        for (field, ns) in [
            ("writer_queue_wait_ms", expected.writer_queue_ns),
            ("sql_ms", expected.sql_ns),
            ("commit_ms", expected.commit_ns),
        ] {
            let field = format!("{field}={:.3}", ns as f64 / 1_000_000.0);
            assert!(
                line.split_whitespace().any(|part| part == field),
                "expected {field}; {line}"
            );
        }
        assert!(line.contains("serialization_ms="), "{line}");
    }
}

#[tokio::test]
async fn request_cpu_scenarios_run_on_representative_smoke_fixture() {
    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    measure(representative_fixture(FixtureCounts::smoke()), 1).await;
}
