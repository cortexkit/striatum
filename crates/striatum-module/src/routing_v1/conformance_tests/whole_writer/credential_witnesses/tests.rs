use super::*;

impl World {
    pub(super) async fn credential_witnesses(&mut self, seed: u64) {
        for reload in [false, true] {
            let _clock = crate::InjectedClock::set(START);
            let mut world = World::new(&format!("credential-same-evidence-{seed}-{reload}")).await;
            world.same_credential_evidence_witness(reload).await;
            let _clock = crate::InjectedClock::set(START);
            let mut world = World::new(&format!("credential-listing-cycle-{seed}-{reload}")).await;
            world.fenced_listing_cycle_witness(reload).await;
        }
    }

    fn held_credential_origin(&mut self, credential: &str) -> striatum_store::FenceRecheck {
        let credential = credential.to_owned();
        let now = self.now as i64;
        let held = self.command(
            "credential witness/standing classified evidence",
            move |tx| {
                Ok(tx
                    .working_blocks()?
                    .unwrap()
                    .fence_rechecks(now)?
                    .into_iter()
                    .filter(|origin| {
                        origin.kind == "credential"
                            && origin.credential_id.as_deref() == Some(&credential)
                    })
                    .collect::<Vec<_>>())
            },
        );
        assert_eq!(
            held.len(),
            1,
            "the witness must name one stored credential origin: {held:?}"
        );
        held.into_iter().next().unwrap()
    }

    async fn same_credential_evidence_witness(&mut self, reload: bool) {
        self.publish_quota_used(0.0);
        let credential = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap()
            .credential_id
            .clone();
        let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        let mut decisions = Vec::new();
        for key in ["same-evidence-first", "same-evidence-second"] {
            decisions.push(
                self.ok(
                    Caller::Owner,
                    "route.select",
                    json!({"consumer":self.consumer,
                "requirements":{"iq":30},"pin":route,"affinity":key}),
                )
                .await,
            );
        }
        let refusal = |decision: &Value| {
            json!({"decision_id":decision["decision_id"],"outcome":"refused",
            "credential_id":credential,"runner_code":"billing_exhausted"})
        };
        self.ok(Caller::Owner, "route.report", refusal(&decisions[0]))
            .await;
        let first = self.held_credential_origin(&credential);
        assert!(
            first.classified && !first.quota,
            "current headroom contradicts the first quota interpretation: {first:?}"
        );
        self.publish_quota_used(100.0);
        assert_eq!(
            self.held_credential_origin(&credential),
            first,
            "a reading alone must not reinterpret already classified evidence"
        );
        if reload {
            self.reload();
        }
        // Both reports arrive in the same millisecond and carry the same code.
        // Distinct selected decisions make the second one a new delivery, not
        // a report replay, while its refusal evidence signature is unchanged.
        self.ok(Caller::Owner, "route.report", refusal(&decisions[1]))
            .await;
        let second = self.held_credential_origin(&credential);
        assert_eq!(
            second.evidence, first.evidence,
            "the control must keep the evidence signature identical"
        );
        assert_eq!(second.evidence_at, first.evidence_at);
        assert!(
            second.classified && !second.quota,
            "new quota input must not reclassify an unchanged refusal report: {second:?}"
        );
    }

    async fn fenced_listing_cycle_witness(&mut self, reload: bool) {
        let (index, account) = self
            .catalog
            .accounts
            .iter()
            .enumerate()
            .find(|(_, row)| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        let (index, account) = (index, account.clone());
        let credential = account.credential_id.clone();
        let shared = self.harness.credentials();
        let refusal = striatum_core::route_state_store::CredentialRefusal {
            provider_id: account.provider.clone(),
            credential_id: credential.clone(),
            code: "ENTITLEMENT_ERROR".into(),
        };
        let at = self.now as i64;
        self.adapter("listing witness/actual credential refusal", |store| {
            crate::credential_fences::record(store, &shared, &refusal, at)
                .map_err(striatum_core::RouteStateStoreError)
        });
        let before = self.held_credential_origin(&credential);
        self.catalog.accounts.remove(index);
        self.publish_catalog();
        if reload {
            self.reload();
        }
        assert!(!self
            .harness
            .credentials()
            .lock()
            .unwrap()
            .listed_credentials()
            .iter()
            .any(|row| row.id == credential));
        assert_eq!(
            self.held_credential_origin(&credential),
            before,
            "absence from a listing does not supply success evidence for a refused credential"
        );
        self.catalog.accounts.insert(index, account);
        self.publish_catalog();
        if reload {
            self.reload();
        }
        assert!(self
            .harness
            .credentials()
            .lock()
            .unwrap()
            .listed_credentials()
            .iter()
            .any(|row| row.id == credential));
        assert_eq!(
            self.held_credential_origin(&credential),
            before,
            "restoring the same listed record must retain its standing refusal"
        );
        let reply=self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,
            "model":"model-c","conversation_key":"after-listed-again","admission_id":"after-listed-again"})).await;
        assert_ne!(
            reply["account"]["credential_id"], credential,
            "the restored but still-fenced credential must not serve an ordinary Admit"
        );
    }
}

#[tokio::test]
async fn classified_credential_keeps_its_class_when_same_evidence_is_reported_after_a_reading_change(
) {
    for reload in [false, true] {
        let _clock = crate::InjectedClock::set(START);
        let mut world = World::new(&format!("same-credential-evidence-{reload}")).await;
        world.same_credential_evidence_witness(reload).await;
    }
}

#[tokio::test]
async fn a_fenced_credential_survives_its_listing_drop_and_restore_before_and_after_reload() {
    for reload in [false, true] {
        let _clock = crate::InjectedClock::set(START);
        let mut world = World::new(&format!("same-credential-listing-{reload}")).await;
        world.fenced_listing_cycle_witness(reload).await;
    }
}
