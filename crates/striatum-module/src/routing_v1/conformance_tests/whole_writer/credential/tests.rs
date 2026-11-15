use super::*;
use crate::account_writer as writer;
use striatum_core::route_state_store::CredentialRefusal;
use striatum_store::DryRunRepair;

impl World {
    pub(super) fn credential_group(&mut self, nonce: u64) {
        let now = i64::try_from(self.now).unwrap();
        let provider = fixture::SUBSCRIPTION_PROVIDER.to_owned();
        let credential = self
            .catalog
            .accounts
            .iter()
            .find(|account| account.provider == provider)
            .unwrap()
            .credential_id
            .clone();
        let refusal = CredentialRefusal {
            provider_id: provider.clone(),
            credential_id: credential.clone(),
            code: "ENTITLEMENT_ERROR".into(),
        };
        self.adapter("credential/refusal", |store| {
            writer::record_credential_entitlement_fence(
                store,
                &refusal,
                Some("whole-writer-listing"),
                now - 900_001,
            )
        });
        let (p, c) = (provider.clone(), credential.clone());
        self.command("credential/read existing classification", move |tx| {
            striatum_store::AccountTx::credential_quota_classification(tx, &p, &c, nonce % 2 == 0)
        });
        let (p, c) = (provider.clone(), credential.clone());
        self.command("credential/classify", move |tx| {
            writer::apply_block_repair(
                tx,
                DryRunRepair::Classify(p.clone(), c.clone(), nonce % 2 == 0),
                now,
            )
        });
        let (p, c) = (provider.clone(), credential.clone());
        self.command("credential/touch", move |tx| {
            tx.touch_credential_entitlement_fence(&p, &c, now - 900_000)
        });
        let c = credential.clone();
        self.command("credential/fingerprint", move |tx| {
            tx.adopt_credential_entitlement_fingerprint(&c, "whole-writer-listing")
        });
        let c = credential.clone();
        self.command("credential/matching-fingerprint clear", move |tx| {
            tx.clear_credential_entitlement_fences(&c, Some("whole-writer-listing"))
        });
        self.adapter("credential/reinsert", |store| {
            writer::record_credential_entitlement_fence(
                store,
                &refusal,
                Some("whole-writer-listing"),
                now,
            )
        });
        self.adapter("credential/lift all providers", |store| {
            writer::clear_credential_entitlement_fences_for_credential(store, &credential)
        });
        self.adapter("credential/reinsert for counted clear", |store| {
            writer::record_credential_entitlement_fence(store, &refusal, None, now)
        });
        let (p, c) = (provider.clone(), credential.clone());
        self.command("credential/id clear", move |tx| {
            tx.clear_credential_entitlement_fence(&p, &c)
        });
        self.adapter("credential/reinsert for count result", |store| {
            writer::record_credential_entitlement_fence(store, &refusal, None, now + 1)
        });
        let (p, c) = (provider.clone(), credential.clone());
        self.command("credential/counted clear", move |tx| {
            tx.clear_credential_entitlement_fence_count(&p, &c)
        });
        self.advance(2);
        let at = i64::try_from(self.now).unwrap();
        let mut signed_out = refusal;
        signed_out.code = "signed_out".into();
        self.adapter("credential/signed-out evidence", |store| {
            writer::record_credential_entitlement_fence(
                store,
                &signed_out,
                Some("signed-before"),
                at,
            )
        });
        let (p, c) = (provider.clone(), credential.clone());
        let held = self
            .command("credential/read signed-out evidence", move |tx| {
                tx.credential_entitlement_fence(&p, &c)
            })
            .unwrap();
        self.command("credential/signed-out fingerprint CAS", move |tx| {
            tx.update_signed_out_fingerprint(&held, "signed-after")
        });
        self.command("credential/clear signed-out evidence", move |tx| {
            tx.clear_credential_entitlement_fence(&provider, &credential)
        });
    }
}
