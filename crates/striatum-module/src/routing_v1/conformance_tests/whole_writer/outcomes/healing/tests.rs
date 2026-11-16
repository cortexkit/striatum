use super::*;
use striatum_core::accounts::{Fence, PoolKey, PoolScope};
use striatum_store::{AccountAdmissionRow, AccountPoolFenceRow, QuotaSuccessScope};

impl World {
    async fn account_probe_fixture(
        &mut self,
        label: &str,
        recovery_set: Vec<PoolKey>,
        grant: bool,
    ) -> (AccountAdmissionRow, String, String) {
        let id = format!("native-{label}-{}", self.sequence);
        self.ok(
            Caller::Runner,
            "route.account_for",
            json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":id}),
        )
        .await;
        let principal = format!("reserved:{}", fixture::RUNNER_ONE);
        let (p, a) = (principal, id.clone());
        let admission = self
            .command("healing/read real issued admission", move |tx| {
                tx.admission(&p, &a)
            })
            .unwrap();
        let now = self.now as i64;
        let fence_id = format!("native-{label}-fence-{}", self.sequence);
        let mut evidence = self.quota_fence(fence_id.clone(), now - 900_001);
        evidence.provider = admission.provider.clone();
        evidence.account_id = admission.account_id.clone();
        evidence.principal = admission.principal.clone();
        let mut fence: Fence = serde_json::from_str(&evidence.fence_json).unwrap();
        fence.account_id = admission.account_id.clone();
        fence.recovery_set = recovery_set.clone();
        evidence.fence_json = serde_json::to_string(&fence).unwrap();
        let row = AccountPoolFenceRow {
            evidence,
            pool_key: None,
            recovery_set,
        };
        self.command("healing/real whole-account refusal", move |tx| {
            tx.insert_pool_fence(&row)
        });
        let owner = json!([
            "account_admission",
            admission.principal,
            admission.admission_id
        ])
        .to_string();
        if grant {
            let key = json!([fence_id]).to_string();
            let origins = self.command("healing/exact due origin", move |tx| {
                Ok(tx
                    .fence_rechecks()?
                    .into_iter()
                    .filter(|row| row.kind == "account" && row.origin_key == key)
                    .collect::<Vec<_>>())
            });
            assert_eq!(origins.len(), 1, "the real hold must have one origin");
            let held_owner = owner.clone();
            assert!(
                self.command("healing/authorize actual admission", move |tx| {
                    tx.grant_fence_rechecks(&origins, now, Some(&held_owner))
                })
            );
            let held_owner = owner.clone();
            assert!(self.command("healing/authorization exists", move |tx| {
                Ok(tx.probe_authorization_owners()?.contains(&held_owner))
            }));
        }
        (admission, fence_id, owner)
    }

    fn assert_account_probe_settled(&mut self, fence_id: &str, owner: &str, healed: bool) {
        let (id, owner) = (fence_id.to_owned(), owner.to_owned());
        self.command("healing/assert exact source and proof outcome", move |tx| {
            assert!(
                !tx.probe_authorization_owners()?.contains(&owner),
                "a terminal probe retained its authorization"
            );
            let active = tx.working_blocks()?.unwrap().account_fence(&id).is_some();
            assert_eq!(
                active, !healed,
                "the exact whole-account source has the wrong terminal state"
            );
            Ok(())
        });
    }
}

#[tokio::test]
async fn native_authorized_healing_changes_once_then_is_a_durable_noop() {
    let _clock = crate::InjectedClock::set(START);
    for reload in [false, true] {
        let mut world = World::new(&format!("native-authorized-healing-{reload}")).await;
        let (admission, fence_id, owner) = world
            .account_probe_fixture("authorized-heal", Vec::new(), true)
            .await;
        if reload {
            world.reload();
        }
        let now = world.now as i64;
        let before = world.native_block_rows();
        let (o, p, m) = (
            owner.clone(),
            admission.provider.clone(),
            admission.model.clone(),
        );
        world.command("healing/authorized success", move |tx| {
            tx.heal_authorized_probe(&o, &p, &m, now)
        });
        assert_ne!(
            world.native_block_rows(),
            before,
            "authorized healing made no durable change"
        );
        world.assert_account_probe_settled(&fence_id, &owner, true);
        if reload {
            world.reload();
        }
        let before = world.native_block_rows();
        world.command("healing/repeated authorized success", move |tx| {
            tx.heal_authorized_probe(&owner, &admission.provider, &admission.model, now)
        });
        assert_eq!(
            world.native_block_rows(),
            before,
            "repeated authorized healing changed durable rows"
        );
    }
}

#[tokio::test]
async fn native_failed_probe_spends_its_proof_once_without_healing_the_source() {
    let _clock = crate::InjectedClock::set(START);
    for reload in [false, true] {
        let mut world = World::new(&format!("native-failed-probe-{reload}")).await;
        let (_, fence_id, owner) = world
            .account_probe_fixture("failed-probe", Vec::new(), true)
            .await;
        if reload {
            world.reload();
        }
        let now = world.now as i64;
        let before = world.native_block_rows();
        let held_owner = owner.clone();
        world.command("healing/fail actual authorized probe", move |tx| {
            tx.fail_authorized_probe(&held_owner, now, false)
        });
        assert_ne!(
            world.native_block_rows(),
            before,
            "failed probe made no durable change"
        );
        world.assert_account_probe_settled(&fence_id, &owner, false);
        if reload {
            world.reload();
        }
        let before = world.native_block_rows();
        world.command("healing/repeated failed probe", move |tx| {
            tx.fail_authorized_probe(&owner, now, false)
        });
        assert_eq!(
            world.native_block_rows(),
            before,
            "repeated failed probe changed durable rows"
        );
    }
}

#[tokio::test]
async fn native_admission_success_uses_the_authorized_report_healer_once() {
    let _clock = crate::InjectedClock::set(START);
    for reload in [false, true] {
        let mut world = World::new(&format!("native-admission-healing-{reload}")).await;
        let (admission, fence_id, owner) = world
            .account_probe_fixture("admission-heal", Vec::new(), true)
            .await;
        if reload {
            world.reload();
        }
        let now = world.now as i64;
        let before = world.native_block_rows();
        let row = admission.clone();
        // This whole-account refusal names no quota pools whose ordinary
        // use could prove recovery. Only the exact admission's authorized
        // probe can clear it, so ordinary success cannot hide a skipped heal.
        let cleared = world.command("healing/actual admission success", move |tx| {
            tx.heal_admission_success(&row, None, None, None, now)
        });
        let key = json!([fence_id]).to_string();
        assert!(
            cleared
                .iter()
                .any(|row| row.kind == "account" && row.origin_key == key),
            "the authorized report path did not clear its whole-account source"
        );
        assert_ne!(
            world.native_block_rows(),
            before,
            "admission success made no durable change"
        );
        world.assert_account_probe_settled(&fence_id, &owner, true);
        if reload {
            world.reload();
        }
        let before = world.native_block_rows();
        let cleared = world.command("healing/repeated admission success", move |tx| {
            tx.heal_admission_success(&admission, None, None, None, now)
        });
        assert!(
            cleared.is_empty(),
            "an already settled admission cleared another origin"
        );
        assert_eq!(
            world.native_block_rows(),
            before,
            "repeated admission success changed durable rows"
        );
    }
}

#[tokio::test]
async fn native_partial_pool_success_progresses_once_while_the_whole_fence_stands() {
    let _clock = crate::InjectedClock::set(START);
    for reload in [false, true] {
        let mut world = World::new(&format!("native-partial-pool-{reload}")).await;
        let pools = ["first", "second"].map(|name| PoolKey {
            window_minutes: Some(300),
            scope: PoolScope::EveryModel,
            name: Some(name.into()),
        });
        let (admission, fence_id, _) = world
            .account_probe_fixture("partial-pool", pools.to_vec(), false)
            .await;
        if reload {
            world.reload();
        }
        let now = world.now as i64;
        let scope = QuotaSuccessScope {
            pool: Some("first".into()),
            family: striatum_core::accounts::ModelFamily::Other,
        };
        let before = world.native_block_rows();
        let (row, used) = (admission.clone(), scope.clone());
        world.command(
            "healing/serving success covers one recovery pool",
            move |tx| {
                tx.heal_serving_fences_in_scope(
                    &row.provider,
                    &row.account_id,
                    &row.credential_id,
                    &row.model,
                    None,
                    row.committed_at,
                    now,
                    Some(&used),
                )
            },
        );
        assert_ne!(
            world.native_block_rows(),
            before,
            "partial pool success made no durable progress"
        );
        let (provider, id) = (admission.provider.clone(), fence_id.clone());
        let active = world.command(
            "healing/partly recovered fence is still active",
            move |tx| {
                let rows = tx.active_pool_fences(&provider)?;
                let row = rows
                    .into_iter()
                    .find(|row| row.evidence.fence_id == id)
                    .expect("one recovered pool must not clear the whole-account fence");
                assert_eq!(row.recovery_set.len(), 2);
                Ok(row.evidence)
            },
        );
        let id = fence_id.clone();
        let proofs = world.command("healing/exact partial proof", move |tx| {
            Ok(tx
                .working_blocks()?
                .unwrap()
                .success_pools()
                .filter(|row| row.fence_id == id)
                .cloned()
                .collect::<Vec<_>>())
        });
        assert_eq!(
            proofs,
            vec![striatum_store::SuccessPool {
                fence_id,
                pool_key_json: serde_json::to_string(&pools[0]).unwrap(),
                evidence_at: active.observed_at.min(active.created_at),
                healed_at: now,
            }],
            "the partial proof must name the still-active fence's exact evidence and used pool"
        );
        if reload {
            world.reload();
        }
        let before = world.native_block_rows();
        world.command("healing/repeated same-pool serving success", move |tx| {
            tx.heal_serving_fences_in_scope(
                &admission.provider,
                &admission.account_id,
                &admission.credential_id,
                &admission.model,
                None,
                admission.committed_at,
                now,
                Some(&scope),
            )
        });
        assert_eq!(
            world.native_block_rows(),
            before,
            "the same partial pool success changed durable rows twice"
        );
    }
}
