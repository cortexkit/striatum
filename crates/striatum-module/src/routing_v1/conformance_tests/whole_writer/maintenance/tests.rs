use super::*;
use crate::account_writer as writer;

impl World {
    pub(super) fn maintenance_group(&mut self) {
        let before = i64::try_from(self.now).unwrap() - 30 * 86_400_000;
        let now = i64::try_from(self.now).unwrap();
        self.adapter("retention/admissions", |store| {
            writer::prune_account_admissions(store, before, 10, now)
        });
        self.adapter("retention/pins", |store| {
            writer::prune_account_pins(store, before, 10)
        });
        self.adapter("retention/affinities", |store| {
            writer::prune_account_affinities(store, Vec::new(), before, before)
        });
        self.adapter("retention/observations", |store| {
            writer::prune_account_observations(store, before, 10)
        });
        self.adapter("retention/observation events", |store| {
            writer::prune_observation_events(store, before, 10)
        });
        self.adapter("retention/v1 decisions", |store| {
            writer::prune_v1_decisions(store, before, 10)
        });
        self.adapter("retention/v1 admissions", |store| {
            writer::prune_v1_admissions(store, before, 10)
        });
        self.adapter("retention/v1 affinities", |store| {
            writer::prune_v1_affinities(store, before, 10)
        });
        self.adapter("retention/continuations", |store| {
            writer::prune_account_continuations(store, before, 10)
        });
        self.adapter("retention/retired evidence", |store| {
            writer::prune_retired_fence_origins(store, before, 10)
        });
        self.adapter("retention/catalogue", writer::prune_unserved_unpicked_facts);
        self.adapter("tick adapter", |store| writer::tick(store, now));
        self.adapter("repair adapter", |store| {
            writer::repair(store, move |tx| tx.tick(now))
        });
        // The public SQL-only maintenance forms do not update the module's
        // admission index. Replay them after the module's Tick and with an
        // old retention cutoff, so deletions use the indexed module paths above.
        self.command("retention/non-returning forms", move |tx| {
            striatum_store::AccountTx::tick(tx, now)?;
            striatum_store::AccountTx::release_expired_admission_probes(tx, now)?;
            striatum_store::AccountTx::prune_account_admissions(tx, before, 10, now)?;
            striatum_store::AccountTx::prune_account_continuations(tx, before, 10)?;
            Ok(())
        });
    }
}
