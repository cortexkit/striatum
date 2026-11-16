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
        let first=self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":format!("before-{id}")})).await;
        self.publish_quota_used(100.0);
        // A reading alone preserves an established pin; cold work still has
        // to wait for headroom. Confirmed refusals are exercised separately.
        let retained=self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":format!("sticky-{id}")})).await;
        assert_retained_account(&first, &retained);
        let reply=self.request(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":format!("cold-{id}"),"admission_id":id,"hold_wait_max_s":120})).await;
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
        self.witness("fresh_headroom");
        let id = format!("headroom-admit-{nonce}-{}", self.sequence);
        self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":id})).await;
    }
}

fn assert_retained_account(first: &Value, retained: &Value) {
    first["account"]["account_id"]
        .as_str()
        .expect("the initial admission selected an account");
    let pin = first["pin"]["id"]
        .as_str()
        .expect("the initial admission created a real pin");
    let generation = first["pin"]["generation"]
        .as_u64()
        .expect("the initial pin has a generation");
    assert_eq!(
        retained["account"], first["account"],
        "a full reading keeps the established account"
    );
    assert_eq!(
        retained["pin"]["id"].as_str(),
        Some(pin),
        "a full reading keeps the existing pin"
    );
    assert_eq!(
        retained["pin"]["generation"].as_u64(),
        Some(generation),
        "a reading alone does not advance the pin generation"
    );
}
