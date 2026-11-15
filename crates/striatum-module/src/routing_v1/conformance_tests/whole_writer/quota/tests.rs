use super::*;

impl World {
    pub(super) fn publish_quota_used(&mut self, used: f64) {
        let _clock = crate::InjectedClock::set(self.now);
        let accounts = self
            .catalog
            .accounts
            .iter()
            .filter(|account| account.provider == fixture::SUBSCRIPTION_PROVIDER)
            .map(|account| account.account_id.as_str())
            .collect::<Vec<_>>();
        super::super::warm_credential_tests::quota_used_reset_on(
            &self.harness,
            &accounts,
            used,
            5_000,
        );
        self.sequence += 1;
        self.commands
            .insert("quota publication and durable repair".into());
        self.compare("quota publication and durable repair");
    }

    pub(super) async fn quota_group(&mut self, nonce: u64) {
        let id = format!("quota-hold-{nonce}-{}", self.sequence);
        self.publish_quota_used(0.0);
        self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":format!("before-{id}")})).await;
        self.publish_quota_used(100.0);
        let reply=self.request(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":id,"hold_wait_max_s":120})).await;
        assert_eq!(
            reply.0,
            FrameType::Error,
            "an exhausted reading admitted work: {}",
            reply.1
        );
        assert_eq!(
            reply.1["code"], "account_hold",
            "an exhausted reading was not a quota hold: {}",
            reply.1
        );
        let now = i64::try_from(self.now).unwrap();
        let fence_id = format!("headroom-{nonce}-{}", self.sequence);
        let row = self.quota_fence(fence_id.clone(), now);
        self.command("quota/refusal evidence", move |tx| tx.insert_fence(&row));
        let id = fence_id.clone();
        self.command("quota/standing evidence is present", move |tx| {
            assert!(tx
                .active_fences(fixture::SUBSCRIPTION_PROVIDER)?
                .iter()
                .any(|row| row.fence_id == id));
            Ok(())
        });
        // The quota wire timestamps have whole-second precision. Make the
        // replacement reading actually newer than the millisecond evidence.
        self.advance(1_001);
        self.publish_quota_used(0.0);
        self.command("quota/new headroom healed evidence", move |tx| {
            assert!(
                !tx.active_fences(fixture::SUBSCRIPTION_PROVIDER)?
                    .iter()
                    .any(|row| row.fence_id == fence_id),
                "fresh headroom did not clear the account's hold"
            );
            Ok(())
        });
        let id = format!("headroom-admit-{nonce}-{}", self.sequence);
        self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":id})).await;
    }
}
