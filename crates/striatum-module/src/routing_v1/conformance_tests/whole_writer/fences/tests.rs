use super::*;
use crate::account_writer as writer;
use striatum_core::accounts::{ConfirmationSource, Fence, ResetTime};
use striatum_store::{
    AccountEligibilityFact, AccountFenceRow, AccountPoolFenceRow, AccountRouteFence,
    AccountSizeRefusal,
};

impl World {
    pub(super) fn quota_fence(&self, id: String, now: i64) -> AccountFenceRow {
        let listed = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        AccountFenceRow {
            fence_id: id,
            provider: listed.provider.clone(),
            account_id: listed.account_id.clone(),
            scope_json: "null".into(),
            fence_json: serde_json::to_string(&Fence {
                account_id: listed.account_id.clone(),
                pool: None,
                recovery_set: Vec::new(),
                observed_at_ms: now,
                reset: ResetTime::Missing,
                retry_hint_ms: None,
                source: ConfirmationSource::BrocaAccountLimit,
            })
            .unwrap(),
            source: "broca_limit_exhausted".into(),
            observed_at: now,
            principal: format!("reserved:{}", fixture::RUNNER_ONE),
            admission_id: None,
            observation_id: None,
            created_at: now,
            cleared_at: None,
            cleared_reason: None,
        }
    }

    pub(super) fn fences_group(&mut self, nonce: u64) {
        let now = i64::try_from(self.now).unwrap();
        let listed = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        let (provider, credential) = (listed.provider.clone(), listed.credential_id.clone());
        let mut route = AccountRouteFence {
            provider: provider.clone(),
            credential_id: credential.clone(),
            model: "model-c".into(),
            until_ms: now + 60_000,
            recorded_at: now,
            decision_id: format!("route-source-{nonce}"),
        };
        self.adapter("fence/quota route", |store| {
            writer::record_account_route_fence(store, &route)
        });
        self.adapter("fence/transient record", |store| {
            writer::record_transient_account_route_fence(store, &route)
        });
        self.adapter("fence/billing record", |store| {
            writer::record_billing_account_route_fence(store, &route)
        });
        self.adapter("fence/route reconciliation", |store| {
            writer::reconcile_account_route_fence(store, &route, now + 30_000, now)
        });
        route.recorded_at += 1;
        let r = route.clone();
        self.command("fence/non-quota route", move |tx| {
            tx.record_non_quota_route_fence(&r)
        });
        let (p, c) = (provider.clone(), credential.clone());
        self.command("fence/clear route before reading", move |tx| {
            tx.clear_account_route_fences_before(&p, &c, now + 1)
        });
        self.adapter("fence/clear credential routes", |store| {
            writer::clear_all_account_route_fences_for_credential(store, &credential)
        });
        let mut recovered = route.clone();
        recovered.recorded_at = now + 2;
        self.adapter("fence/model recovery source", |store| {
            writer::record_account_route_fence(store, &recovered)
        });
        let (p, c) = (provider.clone(), credential.clone());
        assert_eq!(
            self.command("fence/clear only the recovered model", move |tx| {
                striatum_store::AccountTx::clear_account_route_fence_before(
                    tx,
                    &p,
                    &c,
                    "model-c",
                    now + 3,
                )
            }),
            1
        );
        let size = AccountSizeRefusal {
            provider: provider.clone(),
            credential_id: credential.clone(),
            model: "model-c".into(),
            size_class: 17,
            expires_at: now + 60_000,
            observed_at: now,
        };
        let s = size.clone();
        self.command("fence/size quota", move |tx| {
            tx.record_account_size_refusal(&s)
        });
        let s = size.clone();
        self.command("fence/size non-quota", move |tx| {
            tx.record_non_quota_size_refusal(&s, now)
        });
        self.adapter("fence/clear size", |store| {
            writer::clear_account_size_refusal(
                store,
                &provider,
                &credential,
                "model-c",
                17,
                now + 1,
            )
        });
        let fact = AccountEligibilityFact {
            provider: provider.clone(),
            credential_id: credential.clone(),
            model: "model-c".into(),
            listing_fingerprint: Some("whole-writer-listing".into()),
            code: "404".into(),
            observed_at: now,
        };
        self.command("eligibility/record and reconcile", move |tx| {
            tx.record_account_eligibility(&fact)?;
            tx.record_eligibility(&fact)?;
            tx.reconcile_account_eligibility(&[])
        });
        let evidence =
            self.quota_fence(format!("whole-writer-fence-{nonce}-{}", self.sequence), now);
        let row = evidence.clone();
        self.command("fence/account hold", move |tx| tx.insert_fence(&row));
        let mut pool_evidence = evidence.clone();
        pool_evidence.fence_id.push_str("-pool");
        let pool_id = pool_evidence.fence_id.clone();
        let row = AccountPoolFenceRow {
            evidence: pool_evidence,
            pool_key: None,
            recovery_set: Vec::new(),
        };
        self.command("fence/explicit pool hold", move |tx| {
            tx.insert_pool_fence(&row)
        });
        let id = evidence.fence_id.clone();
        self.command("fence/recovery set", move |tx| {
            tx.set_fence_recovery_set(&id, &[])
        });
        let tombstone_key = json!([evidence.fence_id]).to_string();
        let tombstone_evidence = evidence.fence_json.clone();
        let clear_held = move |tx: &mut crate::account_state::AccountCommand<'_>| {
            tx.clear_held_fence(&evidence, now + 1, "whole-writer success")
        };
        let clear_id = move |tx: &mut crate::account_state::AccountCommand<'_>| {
            tx.clear_fence(&pool_id, now + 2, "whole-writer success")
        };
        if nonce % 2 == 0 {
            self.command("fence/held clear", clear_held);
            self.command("fence/id clear", clear_id);
        } else {
            self.command("fence/id clear", clear_id);
            self.command("fence/held clear", clear_held);
        }
        // Cleared immutable fence ids leave the bounded state. Inspect this
        // durable tombstone as test validation, not as a request-path read.
        let tombstone=self.harness.store().with_reader_for_test(|conn| {
            conn.query_row("SELECT source_present,cleared_by,cleared_at,evidence FROM block WHERE kind='account' AND origin_key=?1",
                [&tombstone_key],|row|Ok((row.get::<_,i64>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?,row.get::<_,String>(3)?)))
        }).unwrap();
        assert_eq!(
            tombstone,
            (0, "kept".into(), now + 1, tombstone_evidence),
            "the real held-evidence clear must retain its exact audit tombstone"
        );
        self.witness("tombstone");
    }
}
