//! Whole-state checks around real writer requests, using the conformance
//! catalogue and account seeds also used by the grouped request properties.

use super::*;
use std::collections::BTreeSet;

#[path = "authorization/tests.rs"]
mod authorization;
#[path = "catalogue/tests.rs"]
mod catalogue;
#[path = "catalogue_prune/tests.rs"]
mod catalogue_prune;
#[path = "claims/tests.rs"]
mod claims;
#[path = "continuation/tests.rs"]
mod continuation;
#[path = "credential/tests.rs"]
mod credential;
#[path = "credential_witnesses/tests.rs"]
mod credential_witnesses;
#[path = "fences/tests.rs"]
mod fences;
#[path = "history/tests.rs"]
mod history;
#[path = "ledger/tests.rs"]
mod ledger;
#[path = "maintenance/tests.rs"]
mod maintenance;
#[path = "metadata/tests.rs"]
mod metadata;
#[path = "outcomes/tests.rs"]
mod outcomes;
#[path = "outside_state/tests.rs"]
mod outside_state;
#[path = "paired_charge/tests.rs"]
mod paired_charge;
#[path = "probes/tests.rs"]
mod probes;
#[path = "producer_catalogue/tests.rs"]
mod producer_catalogue;
#[path = "quota/tests.rs"]
mod quota;
#[path = "read_only/tests.rs"]
mod read_only;
#[path = "report_classes/tests.rs"]
mod report_classes;
#[path = "scope_witnesses/tests.rs"]
mod scope_witnesses;
#[path = "semantic/tests.rs"]
mod semantic;
#[path = "shared_keys/tests.rs"]
mod shared_keys;

const START: u64 = 1_800_000_000_000;
const POOL_PROVIDER: &str = "pool-provider";
const POOL_MODEL: &str = "model-pool";

#[derive(Clone, Copy)]
enum Caller {
    Owner,
    Runner,
    Operator,
}

struct World {
    harness: Arc<Harness>,
    catalog: Catalog,
    consumer: String,
    owner: Route,
    runner: Route,
    operator: Route,
    now: u64,
    sequence: usize,
    frozen: striatum_store::FrozenBlockData,
    outside: u64,
    commands: BTreeSet<String>,
    witnesses: BTreeSet<String>,
    trace: striatum_store::writer_entry_trace::Trace,
    _trace_install: striatum_store::writer_entry_trace::Installed,
    bridge_loads: Arc<std::sync::atomic::AtomicU64>,
    expected_bridge_loads: u64,
    replay: Option<striatum_store::replay::Inputs>,
}

impl World {
    async fn new(label: &str) -> Self {
        let subject = KitSubject {
            observe_blocks: Mutex::new(false),
            media: Mutex::new(Default::default()),
            harness: Mutex::new(Harness::new(label)),
        };
        subject
            .configure_consumers(&fixture::consumers())
            .await
            .unwrap();
        let mut catalog = fixture::catalog();
        // The writer sequences select the subscription model as well as using
        // its allocator, so let that shared model satisfy tool-using consumers.
        catalog
            .models
            .iter_mut()
            .find(|model| {
                model.provider == fixture::SUBSCRIPTION_PROVIDER && model.model == "model-c"
            })
            .unwrap()
            .tool_calls = true;
        // Keep the exact subscription-account seed; add the existing grouped
        // pool test's model shapes and one key serving that borrowed catalogue.
        let pool_models = crate::routing_v1::key_pool_tests::catalog_models();
        let mut pool_key = catalog.accounts.first().unwrap().clone();
        pool_key.provider = POOL_PROVIDER.into();
        pool_key.account_id = "whole-writer-pool".into();
        pool_key.credential_id = "apikey:pool-vendor:whole-writer-pool".into();
        pool_key.method = "apikey".into();
        pool_key.serves = vec!["pool-vendor".into()];
        catalog.accounts.push(pool_key);
        for runner in &mut catalog.runners {
            runner
                .route_keys
                .extend(pool_models.iter().map(|model| model.route_key()));
            runner
                .route_keys
                .push(format!("{POOL_PROVIDER}/{POOL_MODEL}"));
        }
        catalog.models.extend(pool_models);
        subject.configure_catalog(&catalog).await.unwrap();
        let consumer = format!(
            "{}.whole-writer",
            fixture::CONSUMER.trim_start_matches("reserved:")
        );
        subject
            .configure_catalog_declarations(&[CatalogModule {
                principal: fixture::CONSUMER.into(),
                nonce_verified: true,
                role_declarations: [(
                    "routing/v1".into(),
                    json!({"consumers":[{"name":consumer,"iq":30}]}),
                )]
                .into_iter()
                .collect(),
            }])
            .await
            .unwrap();
        let harness = subject.harness();
        let owner = harness.bind(reserved(fixture::CONSUMER), None).await;
        let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
        let operator = harness.bind(json!({"kind":"direct"}), None).await;
        let store = harness.store();
        let trace = striatum_store::writer_entry_trace::Trace::default();
        let trace_install = trace.install();
        let bridge_loads =
            crate::account_state::initialize_counted_at(&store, START as i64).unwrap();
        let expected_bridge_loads = bridge_loads.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            expected_bridge_loads > 0,
            "the load counter is not attached to the installed bridge"
        );
        let frozen = store.frozen_block_data_for_test().unwrap();
        let outside = store.outside_account_writer_count_for_test();
        let world = Self {
            harness,
            catalog,
            consumer,
            owner,
            runner,
            operator,
            now: START,
            sequence: 0,
            frozen,
            outside,
            commands: BTreeSet::new(),
            witnesses: BTreeSet::new(),
            trace,
            _trace_install: trace_install,
            bridge_loads,
            expected_bridge_loads,
            replay: None,
        };
        world.compare("seed");
        world
    }

    fn compare(&self, operation: &str) {
        assert_eq!(
            self.bridge_loads.load(std::sync::atomic::Ordering::Relaxed),
            self.expected_bridge_loads,
            "{operation}: an implicit bridge reload could hide a missing writer mirror"
        );
        let store = self.harness.store();
        let context = format!("step={} now={} op={operation}", self.sequence, self.now);
        // Equality compares the bounded state and cannot see dropped rows.
        // Validate their physical clock metadata separately as maintenance,
        // never as part of a production request's read path.
        store.assert_canonical_block_clocks_for_test().unwrap();
        crate::account_state::assert_fresh_load_for_test(&store, self.now as i64, &context);
        store
            .frozen_block_data_for_test()
            .unwrap()
            .assert_unchanged(&self.frozen);
        assert_eq!(
            store.outside_account_writer_count_for_test(),
            self.outside,
            "{context}: a writer bypassed its batch"
        );
    }

    fn install_replay_inputs(&self) -> Option<striatum_store::replay::Guard> {
        self.replay.as_ref().map(|inputs| {
            let mut step = inputs.clone();
            step.now_ms = self.now as i64;
            step.install()
        })
    }

    async fn request(&mut self, caller: Caller, method: &str, params: Value) -> (FrameType, Value) {
        let _inputs = self.install_replay_inputs();
        let _trace = self.trace.install();
        let _clock = crate::InjectedClock::set(self.now);
        let reply = match caller {
            Caller::Owner => self.owner.call(method, &params).await,
            Caller::Runner => self.runner.call(method, &params).await,
            Caller::Operator => self.operator.call(method, &params).await,
        };
        self.sequence += 1;
        self.commands.insert(method.into());
        self.compare(method);
        reply
    }

    async fn ok(&mut self, caller: Caller, method: &str, params: Value) -> Value {
        let reply = self.request(caller, method, params).await;
        assert_eq!(reply.0, FrameType::Response, "{method}: {}", reply.1);
        reply.1["result"].clone()
    }

    fn command<T: Send + 'static>(
        &mut self,
        name: &str,
        change: impl FnMut(
                &mut crate::account_state::AccountCommand<'_>,
            ) -> striatum_core::RouteStateResult<T>
            + Send
            + 'static,
    ) -> T {
        let _inputs = self.install_replay_inputs();
        let _trace = self.trace.install();
        let _clock = crate::InjectedClock::set(self.now);
        let value = crate::account_writer::execute(
            &self.harness.store(),
            striatum_store::AccountCommandKind::Repair,
            change,
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        self.sequence += 1;
        self.commands.insert(name.into());
        self.compare(name);
        value
    }

    fn advance(&mut self, milliseconds: u64) {
        self.now += milliseconds;
        let now = self.now as i64;
        self.command("tick", move |tx| tx.tick(now));
    }

    fn reload(&mut self) {
        self.harness
            .store()
            .account_state_bridge()
            .unwrap()
            .request_reload();
        self.expected_bridge_loads += 1;
        let now = self.now as i64;
        self.command("reload", move |tx| tx.tick(now));
    }

    fn publish_catalog(&mut self) {
        let _inputs = self.install_replay_inputs();
        let _trace = self.trace.install();
        let _clock = crate::InjectedClock::set(self.now);
        install_catalog(&self.harness, &self.catalog);
        self.sequence += 1;
        self.commands
            .insert("catalogue/listing/quota publication".into());
        self.compare("catalogue/listing/quota publication");
    }

    fn adapter<T>(
        &mut self,
        name: &str,
        operation: impl FnOnce(
            &striatum_store::SqliteRouteStateStore,
        ) -> striatum_core::route_state_store::RouteStateResult<T>,
    ) -> T {
        let _inputs = self.install_replay_inputs();
        let _trace = self.trace.install();
        let _clock = crate::InjectedClock::set(self.now);
        let result =
            operation(self.harness.store()).unwrap_or_else(|error| panic!("{name}: {error}"));
        self.sequence += 1;
        self.commands.insert(name.into());
        self.compare(name);
        result
    }

    async fn request_group(&mut self, draw: u8, nonce: u64) {
        let key = format!("generated-{nonce}-{}", self.sequence);
        let subscription = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        match draw {
            0 => {
                self.ok(
                    Caller::Operator,
                    "route.clear_cooldown",
                    json!({"routeKey":subscription}),
                )
                .await;
                let request = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":key,"admission_id":key});
                let admitted = self
                    .ok(Caller::Runner, "route.account_for", request.clone())
                    .await;
                assert_eq!(
                    self.ok(Caller::Runner, "route.account_for", request).await,
                    admitted,
                    "a repeated admission changed its reply"
                );
                self.witness("admission_replay");
                self.ok(Caller::Runner,"route.account_observe",json!({"kind":"admission_started","event_id":format!("start-{key}"),"admission_id":key,"run_id":key})).await;
                self.ok(Caller::Runner,"route.account_observe",json!({"kind":"run_finished","event_id":format!("finish-{key}"),"admission_id":key,"run_id":key})).await;
            }
            1 | 2 => {
                let route = self
                    .catalog
                    .models
                    .iter()
                    .find(|model| model.route_key().ends_with(&format!("/{POOL_MODEL}")))
                    .unwrap()
                    .route_key();
                self.ok(
                    Caller::Operator,
                    "route.clear_cooldown",
                    json!({"routeKey":route}),
                )
                .await;
                let selected = self.request(Caller::Owner,"route.select",json!({"consumer":self.consumer,"requirements":{"iq":30},"pin":route,"affinity":key})).await;
                if selected.0 == FrameType::Error {
                    assert_eq!(
                        selected.1["code"], "no_route",
                        "unexpected generated select error: {}",
                        selected.1
                    );
                    return;
                }
                let selected = selected.1["result"].clone();
                let report = json!({"decision_id":selected["decision_id"],"outcome":if draw==1 {"completed"} else {"failed"},"usage":{"run_id":key,"output_tokens":nonce%17}});
                let recorded = self.ok(Caller::Owner, "route.report", report.clone()).await;
                assert_eq!(
                    self.ok(Caller::Owner, "route.report", report).await,
                    recorded,
                    "a report replay changed its answer"
                );
            }
            3 => {
                let reply = self
                    .request(
                        Caller::Operator,
                        "route.pool_acquire",
                        json!({"provider":POOL_PROVIDER,"model":POOL_MODEL,"session":key}),
                    )
                    .await;
                if reply.0 == FrameType::Error {
                    assert_eq!(
                        reply.1["code"], "explicit_provider_pool_exhausted",
                        "unexpected generated pool refusal: {}",
                        reply.1
                    );
                    return;
                }
                let lease = reply.1["result"].clone();
                self.ok(Caller::Operator,"route.pool_renew",json!({"provider":POOL_PROVIDER,"session":key,"leaseId":lease["leaseId"],"credentialId":lease["credentialId"]})).await;
                self.ok(Caller::Operator,"route.pool_release",json!({"provider":POOL_PROVIDER,"session":key,"leaseId":lease["leaseId"],"endSession":true})).await;
            }
            4 | 5 => {
                self.advance(1);
                let deadline = self.now + 1_000 + nonce % 100;
                self.ok(Caller::Operator,"route.record_cooldown",json!({"routeKey":subscription,"capacityKey":subscription,"cooldownUntil":deadline,"providerWide":draw==5,"lastError":"HTTP 503 provider maintenance"})).await;
                let standing = self
                    .ok(
                        Caller::Operator,
                        "route.cooldown_status",
                        json!({"routeKey":subscription,"capacityKey":subscription,"now":self.now}),
                    )
                    .await;
                assert_eq!(
                    standing["cooled"], true,
                    "the recorded wall did not enforce: {standing}"
                );
                let kind = if draw == 5 { "provider" } else { "route" };
                let key = if draw == 5 {
                    json!([fixture::SUBSCRIPTION_PROVIDER])
                } else {
                    json!([subscription])
                }
                .to_string();
                let clock_key = key.clone();
                self.command("cooldown/standing source is a live clock", move |tx| {
                    let state = tx.working_blocks()?.unwrap();
                    let (_, cleared) = state
                        .clock(kind, &clock_key)
                        .expect("the recorded source has no clock");
                    assert_eq!(
                        cleared, None,
                        "the recorded source must be active, not an unrelated wall"
                    );
                    Ok(())
                });
                self.witness(if draw == 5 {
                    "provider_wall"
                } else {
                    "route_wall"
                });
                let cleared = self
                    .ok(
                        Caller::Operator,
                        "route.clear_cooldown",
                        json!({"routeKey":subscription}),
                    )
                    .await;
                assert_eq!(
                    cleared["clearedCooldownUntil"].as_f64(),
                    Some(deadline as f64),
                    "the operator must clear the deadline that was actually recorded: {cleared}"
                );
                // A route clear leaves a provider's raw term active. Validate
                // the actual route tombstone or partial provider clear, not
                // an unrelated OR term in the combined status response.
                let (source,clear,models)=self.harness.store().with_reader_for_test(|conn|{
                    conn.query_row("SELECT source_present,cleared_at,cleared_models FROM block WHERE kind=?1 AND origin_key=?2",
                        [kind,&key],|row|Ok((row.get::<_,i64>(0)?,row.get::<_,Option<i64>>(1)?,row.get::<_,String>(2)?)))
                }).unwrap();
                if draw == 5 {
                    assert_eq!((source, clear), (1, None));
                    assert!(
                        serde_json::from_str::<Vec<String>>(&models)
                            .unwrap()
                            .iter()
                            .any(|model| model == "model-c"),
                        "operator clear did not mask this model under the provider: {models}"
                    );
                } else {
                    assert_eq!(source, 0);
                    assert!(clear.is_some(), "operator clear left no route tombstone");
                }
                self.witness("operator_clear");
            }
            6 => {
                self.advance([1, 60_001, 900_001][nonce as usize % 3]);
                self.publish_catalog();
            }
            7 => self.reload(),
            8 => self.publish_catalog(),
            9 => self.maintenance_group(),
            10 => self.credential_group(nonce),
            11 => self.catalogue_group(nonce),
            12 => self.fences_group(nonce),
            13 => self.pool_probe_group(nonce).await,
            14 => self.ledger_group(nonce).await,
            15 => self.quota_group(nonce).await,
            16 => self.authorization_group(nonce).await,
            17 => self.history_group(nonce).await,
            18 => self.claims_group(nonce).await,
            19 => self.continuation_group(nonce).await,
            _ => panic!("unrecognised request group: {draw}"),
        }
    }
}

#[tokio::test]
async fn whole_writer_shared_seed_drives_real_requests_and_matches_a_fresh_load() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("whole-writer-seed").await;
    let consumer = world.consumer.clone();
    let selected=world.ok(Caller::Owner,"route.select",json!({
        "consumer":consumer,"requirements":{"iq":30},"pin":format!("{POOL_PROVIDER}/{POOL_MODEL}"),"affinity":"whole-writer-select"
    })).await;
    let request = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
        "conversation_key":"whole-writer","admission_id":"whole-writer-admission"});
    let admitted = world
        .ok(Caller::Runner, "route.account_for", request.clone())
        .await;
    assert!(admitted["account"]["credential_id"].is_string());
    assert_eq!(
        world.ok(Caller::Runner, "route.account_for", request).await,
        admitted
    );
    world.ok(Caller::Runner,"route.account_observe",json!({"kind":"admission_started","event_id":"whole-writer-start","admission_id":"whole-writer-admission","run_id":"whole-writer-run"})).await;
    world.ok(Caller::Runner,"route.account_observe",json!({"kind":"run_finished","event_id":"whole-writer-finish","admission_id":"whole-writer-admission","run_id":"whole-writer-run"})).await;
    world
        .ok(
            Caller::Owner,
            "route.report",
            json!({"decision_id":selected["decision_id"],"outcome":"completed"}),
        )
        .await;
    let lease = world
        .ok(
            Caller::Operator,
            "route.pool_acquire",
            json!({"provider":POOL_PROVIDER,"model":POOL_MODEL,"session":"whole-writer-pool"}),
        )
        .await;
    world.ok(Caller::Operator,"route.pool_renew",json!({"provider":POOL_PROVIDER,"session":"whole-writer-pool","leaseId":lease["leaseId"],"credentialId":lease["credentialId"]})).await;
    world.ok(Caller::Operator,"route.pool_release",json!({"provider":POOL_PROVIDER,"session":"whole-writer-pool","leaseId":lease["leaseId"],"endSession":true})).await;
    world.advance(60_001);
    world.reload();
    assert_trace_is_declared(&world.trace);
    let entries = world.trace.entries();
    for name in [
        "module_adapter::pool_acquire_for_route",
        "module_adapter::pool_renew",
        "module_adapter::pool_release",
    ] {
        assert!(
            entries.iter().any(|entry| entry.name == name
                && entry.account_rows_written.is_some_and(|rows| rows > 0)),
            "real producer was not traced across the handler/writer threads: {name}"
        );
    }
}

fn declared_adapters() -> BTreeSet<String> {
    public_headers(include_str!("../../../account_writer.rs"))
        .into_keys()
        .map(|name| format!("module_adapter::{name}"))
        .collect()
}

fn assert_trace_is_declared(trace: &striatum_store::writer_entry_trace::Trace) {
    let mut declared = declared_adapters();
    declared.extend(declared_mutable_commands());
    declared.extend(metadata::native_declarations());
    assert!(
        !declared.is_empty(),
        "the production producer declarations were not read"
    );
    for (name, reason) in TRANSIENT_COMMANDS.iter().chain(read_only::entries()) {
        assert!(
            declared.contains(*name),
            "stale non-writer exclusion: {name}: {reason}"
        );
    }
    for entry in trace.entries() {
        assert!(
            declared.contains(entry.name),
            "traced producer has no production declaration: {}",
            entry.name
        );
        let rows = entry
            .account_rows_written
            .expect("a producer scope did not finish before its reply");
        if let Some((_, reason)) = TRANSIENT_COMMANDS
            .iter()
            .chain(read_only::entries())
            .find(|(name, _)| *name == entry.name)
        {
            assert_eq!(
                rows, 0,
                "excluded non-writer wrote durable account state: {}: {reason}",
                entry.name
            );
        }
    }
}

fn assert_all_producers_driven(trace: &striatum_store::writer_entry_trace::Trace) {
    assert_trace_is_declared(trace);
    producer_catalogue::assert_reconciled(trace);
    let mut expected = declared_adapters();
    expected.extend(declared_mutable_commands());
    expected.extend(metadata::native_declarations());
    for (name, _) in TRANSIENT_COMMANDS.iter().chain(read_only::entries()) {
        assert!(
            expected.remove(*name),
            "stale or duplicate non-writer exclusion: {name}"
        );
    }
    let observed: BTreeSet<_> = trace
        .entries()
        .into_iter()
        .map(|entry| entry.name.to_owned())
        .collect();
    let missing = expected.difference(&observed).collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "declared producers not driven by the real generator: {missing:#?}"
    );
}

fn public_headers(source: &str) -> std::collections::BTreeMap<String, String> {
    let mut declarations = std::collections::BTreeMap::new();
    for (start, _) in source.match_indices(" fn ") {
        let line_start = source[..start].rfind('\n').map_or(0, |offset| offset + 1);
        if !source[line_start..start].trim_start().starts_with("pub")
            || source[..line_start].trim_end().ends_with("#[cfg(test)]")
        {
            continue;
        }
        let tail = &source[start + 4..];
        let name: String = tail
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .collect();
        let end = tail
            .find('{')
            .expect("production function declaration has no body");
        assert!(
            declarations
                .insert(name.clone(), tail[..end].to_owned())
                .is_none(),
            "duplicate public producer declaration: {name}"
        );
    }
    declarations
}

fn declared_mutable_commands() -> BTreeSet<String> {
    public_headers(include_str!("../../../account_state.rs"))
        .into_iter()
        .filter(|(_, header)| header.contains("mut self"))
        .map(|(name, _)| format!("account_command::{name}"))
        .collect()
}

// These methods update only the process-local concurrency queue, not durable
// account state. They remain traced so adding a durable account-state write is detected.
const TRANSIENT_COMMANDS: &[(&str, &str)] = &[
    (
        "account_command::record_waiter",
        "records a conversation in the in-memory waiter queue",
    ),
    (
        "account_command::retime_waiter",
        "retimes the in-memory waiter queue",
    ),
    (
        "account_command::took_slot",
        "removes an in-memory waiter after it takes a concurrency slot",
    ),
];

fn shuffled_request_groups(seed: u64) -> Vec<(u8, u64)> {
    let mut random = seed;
    let mut next = || {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        random >> 32
    };
    let mut plan: Vec<_> = (0..20u8).map(|group| (group, next())).collect();
    for index in (1..plan.len()).rev() {
        plan.swap(index, next() as usize % (index + 1));
    }
    plan.extend((0..24).map(|_| ((next() % 20) as u8, next())));
    plan
}

#[tokio::test]
async fn generated_request_groups_compare_all_state_after_every_real_step() {
    let _clock = crate::InjectedClock::set(START);
    for seed in 1..=6 {
        let mut world = World::new(&format!("whole-writer-requests-{seed}")).await;
        for (draw, nonce) in shuffled_request_groups(seed) {
            world.request_group(draw, nonce).await;
        }
        world.finished_charge_witnesses(seed).await;
        world.outside_state_witnesses(seed).await;
        world.admit_scope_witnesses(seed).await;
        world.credential_witnesses(seed).await;
        world.shared_key_witnesses(seed).await;
        world.report_class_witnesses(seed).await;
        assert_trace_is_declared(&world.trace);
        assert_all_producers_driven(&world.trace);
        world.assert_semantic_coverage();
        world.compare("all semantic obligations reached");
    }
}

#[tokio::test]
async fn reviewed_reader_scopes_remain_non_writing_on_real_rows() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("whole-writer-reader-scope").await;
    world.credential_group(1);
    assert_trace_is_declared(&world.trace);
    let entry = world
        .trace
        .entries()
        .into_iter()
        .find(|entry| entry.name == "account_tx::fence_rechecks::credential_quota_classification")
        .expect("the real classification reader was not driven");
    assert_eq!(entry.account_rows_written, Some(0));
}
