//! Whole-state checks around real writer requests, using the conformance
//! catalogue and account seeds also used by the grouped request properties.

use super::*;
use std::collections::BTreeSet;

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
    consumer: String,
    owner: Route,
    runner: Route,
    operator: Route,
    now: u64,
    sequence: usize,
    frozen: striatum_store::FrozenBlockData,
    outside: u64,
    commands: BTreeSet<String>,
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
        crate::account_state::initialize_at(&store, START as i64).unwrap();
        let frozen = store.frozen_block_data_for_test().unwrap();
        let outside = store.outside_account_writer_count_for_test();
        let world = Self {
            harness,
            consumer,
            owner,
            runner,
            operator,
            now: START,
            sequence: 0,
            frozen,
            outside,
            commands: BTreeSet::new(),
        };
        world.compare("seed");
        world
    }

    fn compare(&self, operation: &str) {
        let store = self.harness.store();
        let context = format!("step={} now={} op={operation}", self.sequence, self.now);
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

    async fn request(&mut self, caller: Caller, method: &str, params: Value) -> (FrameType, Value) {
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
        let _clock = crate::InjectedClock::set(self.now);
        let value = crate::account_writer::execute(
            &self.harness.store(),
            striatum_store::AccountCommandKind::Repair,
            change,
        )
        .unwrap();
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
        let now = self.now as i64;
        self.command("reload", move |tx| tx.tick(now));
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
}
