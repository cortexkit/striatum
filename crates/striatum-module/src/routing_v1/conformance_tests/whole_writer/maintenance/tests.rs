use super::*;
use crate::account_writer as writer;
use crate::retention::{RetentionWindows, ADMISSION_DELETE_BATCH_ROWS};

const DAY_MS: i64 = 86_400_000;

impl World {
    pub(super) fn maintenance_group(&mut self) {
        let now = i64::try_from(self.now).unwrap();
        let before = now
            - i64::try_from(RetentionWindows::default().account_selection_days).unwrap() * DAY_MS;
        self.delete_historical_pins(before, now);
        self.adapter("retention/admissions", |store| {
            writer::prune_account_admissions(store, before, ADMISSION_DELETE_BATCH_ROWS, now)
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
        // Key-pool retention shares this step rather than adding one: every
        // step adds whole-state comparisons, and the generated-world test pins
        // how many it makes.
        self.adapter("retention/continuations", |store| {
            writer::prune_pool_key_history(store, before, 10)?;
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
            striatum_store::AccountTx::prune_account_continuations(tx, before, 10)?;
            // Continuation retention runs its writer command only when the
            // reader plans a due row, so call that command
            // (delete_account_continuations) directly as well.
            tx.delete_account_continuations(before, &[])?;
            Ok(())
        });
    }

    fn delete_historical_pins(&mut self, before: i64, now: i64) {
        let at = before - 2 * DAY_MS;
        let first = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        let (account, credential) = (first.account_id.clone(), first.credential_id.clone());
        self.delete_one_admission(&account, &credential, at, before, now);
        let sequence = self.sequence;
        for index in 0..2 {
            let (pin, row) = crate::account_state::pinned_admission_for_route(
                &format!("reserved:{}", fixture::RUNNER_ONE),
                &format!("retained-pin-{sequence}-{index}"),
                fixture::SUBSCRIPTION_PROVIDER,
                "model-c",
                &account,
                &credential,
                at,
            );
            self.command("retention/old finished pin history", move |tx| {
                tx.insert_pin(&pin)?;
                tx.insert_admission(&row)?;
                assert!(tx.mark_admission_finished(&row.principal, &row.admission_id, at + 1)?);
                Ok(())
            });
        }
        assert_eq!(
            self.adapter("retention/delete two closed admissions", |store| {
                writer::prune_account_admissions(store, before, ADMISSION_DELETE_BATCH_ROWS, now)
            }),
            2
        );
        assert_eq!(
            self.adapter("retention/delete one reader-planned pin", |store| {
                writer::prune_account_pins(store, before, 1)
            }),
            1
        );
        assert_eq!(
            self.command_with_kind(
                "retention/delete the remaining pin through the native producer",
                striatum_store::AccountCommandKind::HistoryMaintenance,
                move |tx| striatum_store::AccountTx::prune_account_pins(tx, before, 10)
            ),
            1
        );
        for name in [
            "account_tx::retention::prune_account_pins",
            "account_tx::retention::delete_account_pins",
        ] {
            assert!(
                self.trace.entries().iter().any(|entry| entry.name == name
                    && entry.account_rows_written.is_some_and(|rows| rows > 0)),
                "the actual deleting producer was not driven: {name}"
            );
        }
        self.witness("retention_delete");
    }

    fn delete_one_admission(
        &mut self,
        account: &str,
        credential: &str,
        at: i64,
        before: i64,
        now: i64,
    ) {
        let (pin, row) = crate::account_state::pinned_admission_for_route(
            &format!("reserved:{}", fixture::RUNNER_ONE),
            &format!("retained-admission-{}", self.sequence),
            fixture::SUBSCRIPTION_PROVIDER,
            "model-c",
            account,
            credential,
            at,
        );
        let principal = row.principal.clone();
        let admission = row.admission_id.clone();
        self.command("retention/old finished admission input", move |tx| {
            tx.insert_pin(&pin)?;
            tx.insert_admission(&row)?;
            assert!(tx.mark_admission_finished(&row.principal, &row.admission_id, at + 1)?);
            Ok(())
        });
        let first_entry = self.trace.entries().len();
        assert_eq!(
            self.adapter("retention/delete one reader-planned admission", |store| {
                writer::prune_account_admissions(store, before, 1, now)
            }),
            1
        );
        self.assert_writes_since(
            first_entry,
            &[
                "module_adapter::prune_account_admissions",
                "account_command::delete_account_admissions",
                "account_tx::retention::delete_account_admissions_returning",
            ],
        );
        self.command("retention/planned admission is gone", move |tx| {
            assert!(tx.admission(&principal, &admission)?.is_none());
            Ok(())
        });
        assert_eq!(
            self.adapter("retention/remove the witness pin", |store| {
                writer::prune_account_pins(store, before, 1)
            }),
            1
        );
    }
}
