use super::*;
use striatum_core::accounts::AccountStrategy;
use striatum_core::RouteStateStore;
use striatum_store::{AccountAdmissionRow, AccountDemandRow};

#[derive(Clone, Copy, Debug)]
enum ChargeCase {
    TwentyDayRoundRobin,
    ReadingFloor,
}

impl ChargeCase {
    fn strategy(self) -> &'static str {
        match self {
            Self::TwentyDayRoundRobin => "round_robin",
            Self::ReadingFloor => "sticky",
        }
    }

    fn committed_at(self) -> i64 {
        START as i64
            - match self {
                Self::TwentyDayRoundRobin => 20 * 86_400_000,
                Self::ReadingFloor => 1_000,
            }
    }
}

impl World {
    async fn charge_control(case: ChargeCase, charged: bool, seed: u64) -> Self {
        let _clock = crate::InjectedClock::set(START);
        let inputs = striatum_store::replay::Inputs::new(START as i64);
        let _inputs = inputs.install();
        let mut world = Self::new(&format!("charge-{case:?}-{charged}-{seed}")).await;
        world.replay = Some(inputs);
        world.harness.edit_config(|config| {
            config["providers"][fixture::SUBSCRIPTION_PROVIDER]["accounts"]["strategy"] =
                json!(case.strategy());
        });
        world.compare("paired policy publication");
        world.command("paired fixed selection salt", move |tx| {
            tx.salt(START as i64, || format!("paired-charge-{seed}"))
        });
        let first = world
            .catalog
            .accounts
            .iter()
            .filter(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .min_by(|a, b| a.account_id.cmp(&b.account_id))
            .unwrap();
        let (pin, mut admission) = crate::account_state::pinned_admission_for_route(
            &format!("reserved:{}", fixture::RUNNER_ONE),
            "paired-historical-finished",
            fixture::SUBSCRIPTION_PROVIDER,
            "model-c",
            &first.account_id,
            &first.credential_id,
            case.committed_at(),
        );
        admission.charged = charged;
        world.command("paired historical pinned admission", move |tx| {
            tx.insert_pin(&pin)?;
            tx.insert_admission(&admission)
        });
        world.command("paired historical completion", move |tx| {
            assert!(tx.mark_admission_finished(
                &format!("reserved:{}", fixture::RUNNER_ONE),
                "paired-historical-finished",
                case.committed_at() + 1,
            )?);
            Ok(())
        });
        world
    }

    pub(super) async fn finished_charge_witnesses(&mut self, seed: u64) {
        for case in [ChargeCase::TwentyDayRoundRobin, ChargeCase::ReadingFloor] {
            // Fresh twins for each mode keep a previous test Admit's own charge
            // and affinity from becoming a second cause of a later decision.
            for reload in [false, true] {
                let mut control = Self::charge_control(case, false, seed).await;
                let mut charged = Self::charge_control(case, true, seed).await;
                if reload {
                    control.reload();
                    charged.reload();
                }
                let row = assert_only_finished_charge_differs(&mut control, &mut charged, case);
                let mut accounts = control
                    .catalog
                    .accounts
                    .iter()
                    .filter(|account| account.provider == fixture::SUBSCRIPTION_PROVIDER)
                    .map(|account| account.account_id.as_str())
                    .collect::<Vec<_>>();
                accounts.sort();
                assert_eq!(
                    accounts.len(),
                    2,
                    "the borrowed seed's two-account control changed"
                );
                assert_eq!(row.account_id, accounts[0]);
                let second = accounts[1].to_owned();
                let request = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                    "conversation_key":"paired-cold-admit","admission_id":"paired-cold-admit",
                    "initial_input_bytes":4_096,"initial_input_estimated":false});
                let quota_before =
                    format!("{:#?}", crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap());
                let before = control
                    .ok(Caller::Runner, "route.account_for", request.clone())
                    .await;
                assert_eq!(
                    quota_before,
                    format!("{:#?}", crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap()),
                    "the control Admit changed the common quota/listing source"
                );
                let after = charged
                    .ok(Caller::Runner, "route.account_for", request.clone())
                    .await;
                assert_eq!(
                    quota_before,
                    format!("{:#?}", crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap()),
                    "an Admit changed the common quota/listing source between the paired requests"
                );
                assert_eq!(
                    before["account"]["account_id"], row.account_id,
                    "{case:?} reload={reload}: an uncharged finished row changed cold placement"
                );
                assert_eq!(
                    after["account"]["account_id"], second,
                    "{case:?} reload={reload}: the finished charge did not move the actual Admit"
                );
                assert_ne!(
                    before["account"]["account_id"],
                    after["account"]["account_id"]
                );
                control.reload();
                charged.reload();
                assert_eq!(
                    control
                        .ok(Caller::Runner, "route.account_for", request.clone())
                        .await,
                    before
                );
                assert_eq!(
                    charged
                        .ok(Caller::Runner, "route.account_for", request)
                        .await,
                    after
                );
                self.witness(format!(
                    "charge/{}/reload={reload}",
                    match case {
                        ChargeCase::TwentyDayRoundRobin => "round_robin",
                        ChargeCase::ReadingFloor => "reading_floor",
                    }
                ));
            }
        }
        self.publish_catalog();
    }
}

fn historical_row(world: &mut World) -> AccountAdmissionRow {
    world.command("paired keyed historical row", |tx| {
        Ok(tx
            .admission(
                &format!("reserved:{}", fixture::RUNNER_ONE),
                "paired-historical-finished",
            )?
            .unwrap())
    })
}

fn assert_only_finished_charge_differs(
    control: &mut World,
    charged: &mut World,
    case: ChargeCase,
) -> AccountAdmissionRow {
    let mut left = historical_row(control);
    let right = historical_row(charged);
    assert!(!left.charged);
    assert!(right.charged);
    left.charged = true;
    assert_eq!(
        left, right,
        "the paired historical records differ in more than one bit"
    );
    assert_eq!(right.state, "finished");
    assert!(right.started_at.is_none());
    assert!(right.released_at.is_none());
    assert_eq!(right.committed_at, case.committed_at());
    assert_eq!(control.now, charged.now);
    assert_eq!(
        std::fs::read(control.harness.config_path_for_test()).unwrap(),
        std::fs::read(charged.harness.config_path_for_test()).unwrap(),
        "paired operator inputs differ"
    );
    let left_listing = format!(
        "{:#?}",
        control
            .harness
            .credentials()
            .lock()
            .unwrap()
            .listed_credentials(),
    );
    let right_listing = format!(
        "{:#?}",
        charged
            .harness
            .credentials()
            .lock()
            .unwrap()
            .listed_credentials(),
    );
    assert_eq!(
        left_listing, right_listing,
        "paired credential listings differ"
    );
    assert_eq!(
        control.harness.store().get_facts().unwrap(),
        charged.harness.store().get_facts().unwrap(),
        "paired model facts differ"
    );
    let policy = control
        .harness
        .operator_config()
        .allocator_policy_ref(fixture::SUBSCRIPTION_PROVIDER)
        .clone();
    assert_eq!(
        policy,
        charged
            .harness
            .operator_config()
            .allocator_policy_ref(fixture::SUBSCRIPTION_PROVIDER)
            .clone()
    );
    let quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
    let roster = crate::account_selection::roster::allocator_roster(
        &quota,
        fixture::SUBSCRIPTION_PROVIDER,
        control.now as i64,
    );
    let account = roster
        .roster
        .as_ref()
        .unwrap()
        .accounts
        .iter()
        .find(|account| account.account_id == right.account_id)
        .unwrap();
    let observed = account.quota.as_ref().unwrap().observed_at_ms.unwrap();
    match case {
        ChargeCase::TwentyDayRoundRobin => {
            assert_eq!(policy.strategy, AccountStrategy::RoundRobin);
            assert_eq!(control.now as i64 - right.committed_at, 20 * 86_400_000);
            assert!(observed - policy.reflection_lag_ms > right.committed_at);
        }
        ChargeCase::ReadingFloor => {
            assert_eq!(policy.strategy, AccountStrategy::Sticky);
            assert!(
                observed - policy.reflection_lag_ms < right.committed_at,
                "the reading already reflects the historical admission"
            );
        }
    }
    drop(quota);
    let mut expected = (*crate::account_state::snapshot(control.harness.store()).unwrap()).clone();
    let mut actual = (*crate::account_state::snapshot(charged.harness.store()).unwrap()).clone();
    expected.generation = 0;
    actual.generation = 0;
    assert!(expected
        .admissions
        .demand(&right.provider, &right.account_id, None)
        .is_empty());
    assert_eq!(
        actual
            .admissions
            .demand(&right.provider, &right.account_id, None),
        vec![AccountDemandRow {
            account_id: right.account_id.clone(),
            state: "finished".into(),
            committed_at: right.committed_at,
            request_bytes: 1_000_000,
        }]
    );
    let conversation = (
        right.principal.clone().into(),
        right.conversation_hash.clone().into(),
        right.provider.clone().into(),
    );
    let latest = expected.admissions.latest.get(&conversation).unwrap();
    assert_eq!(latest.admission_id, right.admission_id);
    let rowid = latest.rowid;
    // This is the one permitted delta, not a comparator exclusion: inserting
    // the identical finished row with its charge adds one exact demand entry.
    // Full equality still checks every other account, index, block and waiter.
    expected.admissions.insert_admission(&right, rowid);
    assert_eq!(
        expected, actual,
        "{case:?}: allocator state differs beyond the single finished charge"
    );
    right
}

#[tokio::test]
async fn historical_finished_charge_alone_changes_real_admits_before_and_after_reload() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("finished-charge-witness").await;
    world.finished_charge_witnesses(1).await;
}
