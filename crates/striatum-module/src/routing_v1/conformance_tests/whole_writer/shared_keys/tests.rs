use super::*;

#[path = "size/tests.rs"]
mod size;

impl World {
    pub(super) async fn shared_key_witnesses(&mut self, seed: u64) {
        for reload in [false, true] {
            for reverse in [false, true] {
                let _clock = crate::InjectedClock::set(START);
                let mut world =
                    World::new(&format!("shared-fence-ids-{seed}-{reload}-{reverse}")).await;
                world.isolate_shared_account();
                world.two_fence_ids_witness(reverse, reload).await;
                let _clock = crate::InjectedClock::set(START);
                let mut world =
                    World::new(&format!("shared-size-kinds-{seed}-{reload}-{reverse}")).await;
                world.isolate_shared_account();
                world.two_size_kinds_witness(reverse, reload).await;
            }
        }
    }

    fn isolate_shared_account(&mut self) {
        let target = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap()
            .account_id
            .clone();
        // Unlist the healthy sibling so the account and size refusals under
        // test must refuse a new account_for request, not allow a valid fallback.
        self.catalog.accounts.retain(|row| {
            row.provider != fixture::SUBSCRIPTION_PROVIDER || row.account_id == target
        });
        self.publish_catalog();
    }

    fn shared_key_request(&self, id: &str, tokens: u64) -> Value {
        let account = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        json!({"provider":account.provider,"model":"model-c","conversation_key":id,"admission_id":id,
            "requested_account":{"account_id":account.account_id},
            "initial_input_bytes":tokens*4,"initial_input_estimated":false})
    }

    async fn assert_shared_key_blocked(&mut self, id: &str, tokens: u64) {
        let blocked = self
            .request(
                Caller::Runner,
                "route.account_for",
                self.shared_key_request(id, tokens),
            )
            .await;
        assert_eq!(
            blocked.0,
            FrameType::Error,
            "standing evidence did not change Admit: {}",
            blocked.1
        );
        assert!(
            ![json!("store_error"), json!("account_selection_unavailable")]
                .contains(&blocked.1["code"]),
            "a store failure is not proof of policy enforcement: {}",
            blocked.1
        );
    }

    async fn two_fence_ids_witness(&mut self, reverse: bool, reload: bool) {
        let request = self.shared_key_request("before-two-fences", 4_096);
        let before = self
            .ok(Caller::Runner, "route.account_for", request.clone())
            .await;
        assert_eq!(
            before["account"]["account_id"],
            request["requested_account"]["account_id"]
        );
        let at = self.now as i64;
        let rows = [
            self.quota_fence("shared-account-first".into(), at),
            self.quota_fence("shared-account-second".into(), at),
        ];
        for row in &rows {
            let row = row.clone();
            self.command("shared account/record independent fence id", move |tx| {
                tx.insert_fence(&row)
            });
        }
        if reload {
            self.reload();
        }
        self.assert_active_fence_ids(&["shared-account-first", "shared-account-second"]);
        self.assert_shared_key_blocked("two-account-fences", 4_096)
            .await;
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", request.clone())
                .await,
            before,
            "a previously granted id must replay even when new fences empty its roster"
        );
        let order = if reverse { [1, 0] } else { [0, 1] };
        for (step, index) in order.into_iter().enumerate() {
            self.advance(1);
            let now = self.now as i64;
            let row = rows[index].clone();
            self.command("shared account/clear only the held id", move |tx| {
                tx.clear_held_fence(&row, now, "serving success")
            });
            if reload {
                self.reload();
            }
            if step == 0 {
                self.assert_active_fence_ids(&[rows[1 - index].fence_id.as_str()]);
                self.assert_shared_key_blocked("one-account-fence-remains", 4_096)
                    .await;
            } else {
                self.assert_active_fence_ids(&[]);
            }
        }
        let cleared_request = self.shared_key_request("after-two-fences", 4_096);
        let cleared = self
            .ok(Caller::Runner, "route.account_for", cleared_request.clone())
            .await;
        for row in rows {
            self.command(
                "shared account/delayed retry of cleared fence id",
                move |tx| tx.insert_fence(&row),
            );
        }
        self.assert_active_fence_ids(&[]);
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", request.clone())
                .await,
            before
        );
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", cleared_request.clone())
                .await,
            cleared
        );
        self.reload();
        self.assert_active_fence_ids(&[]);
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", request).await,
            before
        );
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", cleared_request)
                .await,
            cleared
        );
    }

    fn assert_active_fence_ids(&mut self, ids: &[&str]) {
        let expected: BTreeSet<_> = ids.iter().map(|id| id.to_string()).collect();
        self.command(
            "shared account/read exact surviving fence identities",
            move |tx| {
                let actual = tx
                    .active_fences(fixture::SUBSCRIPTION_PROVIDER)?
                    .into_iter()
                    .filter(|row| row.fence_id.starts_with("shared-account-"))
                    .map(|row| row.fence_id)
                    .collect::<BTreeSet<_>>();
                assert_eq!(
                    actual, expected,
                    "one fence id must not clear its sibling on the same account"
                );
                Ok(())
            },
        );
    }
}

#[tokio::test]
async fn two_origins_on_each_shared_key_survive_both_clear_orders_delayed_evidence_and_reloads() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("shared-key-witness-driver").await;
    world.shared_key_witnesses(1).await;
}
