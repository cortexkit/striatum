use super::*;

impl World {
    pub(super) async fn outside_state_witnesses(&mut self, seed: u64) {
        for reload in [false, true] {
            let _clock = crate::InjectedClock::set(START);
            let mut world = World::new(&format!("outside-state-{seed}-{reload}")).await;
            world.expired_and_released_lease_witness(reload).await;
            world.closed_admission_observation_witness(reload).await;
        }
    }

    async fn expired_and_released_lease_witness(&mut self, reload: bool) {
        let session = "outside-state-lease";
        let lease = self
            .ok(
                Caller::Operator,
                "route.pool_acquire",
                json!({"provider":POOL_PROVIDER,"model":POOL_MODEL,"session":session}),
            )
            .await;
        let until = lease["validUntilMs"].as_u64().unwrap();
        assert!(until > self.now, "{lease}");
        self.advance(until - self.now + 1);
        self.publish_catalog();
        if reload {
            self.reload();
        }
        let params = json!({"provider":POOL_PROVIDER,"session":session,
            "leaseId":lease["leaseId"],"credentialId":lease["credentialId"]});
        let renewed = self
            .ok(Caller::Operator, "route.pool_renew", params.clone())
            .await;
        assert_eq!(
            renewed["live"], true,
            "expiry alone must not release a live run: {renewed}"
        );
        assert!(renewed["validUntilMs"].as_u64().unwrap() > self.now);
        self.ok(
            Caller::Operator,
            "route.pool_release",
            json!({"provider":POOL_PROVIDER,
            "session":session,"leaseId":lease["leaseId"],"endSession":true}),
        )
        .await;
        if reload {
            self.reload();
        }
        let released = self.ok(Caller::Operator, "route.pool_renew", params).await;
        assert_eq!(
            released,
            json!({"live":false,"validUntilMs":null}),
            "a credential id cannot recreate a released lease"
        );
    }

    async fn closed_admission_observation_witness(&mut self, reload: bool) {
        let id = "outside-state-closed-admission";
        let admitted = self
            .ok(
                Caller::Runner,
                "route.account_for",
                json!({
            "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
            "conversation_key":id,"admission_id":id}),
            )
            .await;
        self.ok(
            Caller::Runner,
            "route.account_observe",
            json!({"kind":"admission_started",
            "event_id":"outside-state-start","admission_id":id,"run_id":id}),
        )
        .await;
        self.ok(
            Caller::Runner,
            "route.account_observe",
            json!({"kind":"run_finished",
            "event_id":"outside-state-finish","admission_id":id,"run_id":id}),
        )
        .await;
        if reload {
            self.reload();
        }
        assert!(
            crate::account_state::snapshot(self.harness.store())
                .unwrap()
                .admissions
                .open_admission(&format!("reserved:{}", fixture::RUNNER_ONE), id)
                .is_none(),
            "the witness must exercise the keyed closed-row lookup, not an open cached admission"
        );
        let params = json!({"kind":"attempt","event_id":"outside-state-after-finish",
            "admission_id":id,"run_id":id,"served_account_id":admitted["account"]["account_id"],
            "credential_id":admitted["account"]["credential_id"],"attempt_outcome":{"status":"served_empty"}});
        let late = self
            .ok(Caller::Runner, "route.account_observe", params.clone())
            .await;
        assert_eq!(
            late["bound"], true,
            "a closed admission remains attributable: {late}"
        );
        assert_eq!(
            late["duplicate"], false,
            "the observation is a new event: {late}"
        );
        let replay = self
            .ok(Caller::Runner, "route.account_observe", params)
            .await;
        assert_eq!(replay["bound"], true, "{replay}");
        assert_eq!(
            replay["duplicate"], true,
            "a retried closed-row observation is still deduplicated: {replay}"
        );
    }
}

#[tokio::test]
async fn omitted_lease_and_closed_admission_rows_keep_their_wire_contract_on_reload() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("outside-state-witness-driver").await;
    world.outside_state_witnesses(1).await;
}
