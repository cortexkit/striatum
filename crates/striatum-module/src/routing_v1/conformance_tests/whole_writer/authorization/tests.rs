use super::*;
use crate::account_writer as writer;
use striatum_core::route_state_store::CredentialRefusal;

impl World {
    pub(super) async fn authorization_group(&mut self, nonce: u64) {
        let index = self
            .catalog
            .accounts
            .iter()
            .position(|account| account.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        let original = self.catalog.accounts[index].credential_id.clone();
        let credential = format!("{original}-probe-{nonce}-{}", self.sequence);
        self.catalog.accounts[index].credential_id = credential.clone();
        self.publish_catalog();
        let provider = fixture::SUBSCRIPTION_PROVIDER.to_owned();
        let account = self.catalog.accounts[index].account_id.clone();
        let route = format!("{provider}/model-c");
        self.ok(
            Caller::Operator,
            "route.clear_cooldown",
            json!({"routeKey":route}),
        )
        .await;
        let selected = self
            .ok(
                Caller::Owner,
                "route.select",
                json!({"consumer":self.consumer,"requirements":{"iq":30},"pin":route}),
            )
            .await;
        let decision = selected["decision_id"].as_str().unwrap().to_owned();
        let (d, p, a, c) = (
            decision.clone(),
            provider.clone(),
            account.clone(),
            credential.clone(),
        );
        self.command("authorization/actual served identity", move |tx| {
            tx.bind_model_account(&d, &p, "model-c", &a, &c)
        });
        let d = decision.clone();
        self.command("authorization/served tier", move |tx| {
            tx.record_account_service_tier(&d, None, "standard")
        });
        let now = i64::try_from(self.now).unwrap();
        let refusal = CredentialRefusal {
            provider_id: provider.clone(),
            credential_id: credential.clone(),
            code: "ENTITLEMENT_ERROR".into(),
        };
        self.adapter(
            "authorization/refusal on newly listed credential",
            |store| {
                writer::record_credential_entitlement_fence(store, &refusal, None, now - 900_001)
            },
        );
        let (p, c) = (provider.clone(), credential.clone());
        self.command("authorization/classify held evidence", move |tx| {
            tx.classify_credential_quota(&p, &c, true)
        });
        let c = credential.clone();
        let origins = self.command("authorization/read due origins", move |tx| {
            Ok(tx
                .working_blocks()?
                .unwrap()
                .fence_rechecks(now)?
                .into_iter()
                .filter(|row| row.kind == "credential" && row.credential_id.as_deref() == Some(&c))
                .collect::<Vec<_>>())
        });
        assert_eq!(
            origins.len(),
            1,
            "the newly listed refusal has no claimable clock"
        );
        let from = format!("prepared-probe-{nonce}-{}", self.sequence);
        let owner = from.clone();
        assert!(
            self.command("authorization/grant", move |tx| tx.grant_fence_rechecks(
                &origins,
                now,
                Some(&owner)
            ))
        );
        let temporary = format!("bound-{from}");
        self.adapter("authorization/rebind prepared owner", |store| {
            writer::bind_fence_probe_owner(store, &from, &temporary)
        });
        let (p, c) = (provider.clone(), credential.clone());
        let origins = self.command("authorization/read granted origins", move |tx| {
            Ok(tx
                .working_blocks()?
                .unwrap()
                .fence_rechecks(now)?
                .into_iter()
                .filter(|row| row.provider == p && row.credential_id.as_deref() == Some(&c))
                .collect::<Vec<_>>())
        });
        let (d, a, c) = (decision.clone(), account.clone(), credential.clone());
        assert!(
            self.command("authorization/bind selected decision", move |tx| tx
                .bind_decision_probe(&origins, &temporary, &d, &a, &c, now))
        );
        let (d, c) = (decision.clone(), credential.clone());
        assert!(
            self.command("authorization/selected credential is named", move |tx| tx
                .probe_authorization_names_credential(&d, &c, "model-c"))
        );
        let (d, p) = (decision.clone(), provider.clone());
        self.command("authorization/inconclusive release", move |tx| {
            tx.release_inconclusive_probe(&d, &p, "model-c", now)
        });
        let (p, c) = (provider.clone(), credential.clone());
        let held = self
            .command("authorization/read released refusal", move |tx| {
                tx.credential_entitlement_fence(&p, &c)
            })
            .expect("an inconclusive probe lost its refusal");
        self.command("authorization/clear exact released refusal", move |tx| {
            tx.clear_held_entitlement_fence(&held)
        });
        self.catalog.accounts[index].credential_id = original;
        self.publish_catalog();
    }
}
