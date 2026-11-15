use super::*;
use crate::account_writer as writer;
use striatum_core::key_pool::PoolKey;
use striatum_store::{PoolAcquire, PoolAcquireOutcome, PoolWallKind};

impl World {
    pub(super) async fn pool_probe_group(&mut self, nonce: u64) {
        let now = i64::try_from(self.now).unwrap();
        let listed = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == POOL_PROVIDER)
            .unwrap();
        let (credential, account) = (listed.credential_id.clone(), listed.account_id.clone());
        self.adapter("pool/due retry-after", |store| {
            writer::pool_wall(
                store,
                POOL_PROVIDER,
                &credential,
                PoolWallKind::RateLimited,
                now + 5_000,
                "capacity",
                now,
            )
        });
        self.advance(5_001);
        let now = i64::try_from(self.now).unwrap();
        let session = format!("native-pool-probe-{nonce}-{}", self.sequence);
        let serving_account = account.clone();
        let owned_session = session.clone();
        let outcome = self.command("pool/native acquire with due probe", move |tx| {
            let keys = [PoolKey::new(&credential, Some(&account), "active")];
            tx.pool_acquire(&PoolAcquire {
                provider: POOL_PROVIDER,
                session_id: &owned_session,
                keys: &keys,
                now_ms: now,
                lease_ms: 120_000,
                affinity_idle_ms: 120_000,
            })
        });
        let PoolAcquireOutcome::Leased(lease) = outcome else {
            panic!("due pool probe did not get a lease: {outcome:?}");
        };
        self.command("pool/claimed clock is durable", move |tx| {
            let clocks = tx.working_blocks()?.unwrap().blocks(now).unwrap().blocks;
            assert!(
                clocks
                    .iter()
                    .any(|clock| clock.scope.provider == POOL_PROVIDER
                        && clock.last_claimed_at == Some(now)
                        && clock.claim_generation > 0),
                "the granted pool lease holds no probe generation: {clocks:?}"
            );
            Ok(())
        });
        self.ok(Caller::Operator,"route.pool_renew",json!({"provider":POOL_PROVIDER,"session":session,"leaseId":lease.lease_id,"credentialId":lease.credential_id})).await;
        let serving_credential = lease.credential_id.clone();
        self.command("pool/typed serving success", move |tx| {
            tx.heal_serving_fences(
                POOL_PROVIDER,
                &serving_account,
                &serving_credential,
                POOL_MODEL,
                None,
                now,
                now,
            )
        });
        self.adapter("pool/release claimed lease", |store| {
            writer::pool_release(
                store,
                POOL_PROVIDER,
                &session,
                Some(&lease.lease_id),
                true,
                now,
            )
        });
    }
}

#[tokio::test]
async fn native_pool_producer_keeps_the_published_ledger_equal_to_reload() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("whole-writer-native-pool").await;
    world.pool_probe_group(1).await;
}
