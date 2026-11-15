use super::*;
use striatum_store::AccountExclusionRow;

impl World {
    pub(super) async fn ledger_group(&mut self, nonce: u64) {
        let principal = format!("reserved:{}", fixture::RUNNER_ONE);
        let id = format!("ledger-{nonce}-{}", self.sequence);
        let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        self.ok(
            Caller::Operator,
            "route.clear_cooldown",
            json!({"routeKey":route}),
        )
        .await;
        self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":id})).await;
        let (p, a) = (principal.clone(), id.clone());
        let row = self
            .command("ledger/read issued admission", move |tx| {
                tx.admission(&p, &a)
            })
            .expect("account_for did not persist its issued admission");
        let now = i64::try_from(self.now).unwrap();
        let r = row.clone();
        self.command("ledger/touch issued affinity", move |tx| {
            tx.touch_affinity(
                &r.principal,
                &r.conversation_hash,
                &r.provider,
                (&r.pin_id, r.pin_generation),
                now,
                now + 120_000,
                Some(now + 120_000),
            )
        });
        let r = row.clone();
        self.command("ledger/observe issued admission", move |tx| {
            tx.observe_admission(&r, now)
        });
        self.command("ledger/selection salt replay", move |tx| {
            let held = tx.existing_salt()?;
            let replayed =
                striatum_store::AccountTx::salt(tx, now, || "unexpected-new-salt".into())?;
            assert_eq!(held.as_deref(), Some(replayed.as_str()));
            Ok(())
        });
        let (p, a) = (principal.clone(), id.clone());
        let current = self
            .command("ledger/read observed row", move |tx| tx.admission(&p, &a))
            .unwrap();
        assert!(self.command("ledger/keyed lifecycle replay", move |tx| {
            striatum_store::AccountTx::observe_admission(tx, &current, now)?;
            assert!(tx.set_admission_lifecycle(&current, &current.state)?);
            tx.admission_in_states(
                &current.principal,
                &current.admission_id,
                &[current.state.clone()],
            )
        }));
        let (p, a) = (principal.clone(), id.clone());
        self.command("ledger/release issued admission", move |tx| {
            tx.mark_admission_released(&p, &a, now)
        });
        self.ok(Caller::Runner,"route.account_observe",json!({"kind":"run_finished","event_id":format!("closed-{id}"),"admission_id":id,"run_id":format!("run-{id}")})).await;
        let exclusion = AccountExclusionRow {
            principal: principal.clone(),
            admission_id: id.clone(),
            credential_id: row.credential_id,
            runner_view_digest: "whole-writer-runner".into(),
            roster_view_digest: "conformance-vault-view".into(),
            observation_id: None,
            recorded_at: now,
        };
        let first = exclusion.clone();
        self.command("ledger/principal exclusion", move |tx| {
            tx.insert_exclusion(&first)
        });
        self.command("ledger/exclusion recheck", move |tx| {
            tx.insert_exclusion_with_recheck(&exclusion, 60_000)
        });
        let expiring = format!("expiring-{id}");
        self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":expiring,"admission_id":expiring})).await;
        self.now += 60_001;
        let at = i64::try_from(self.now).unwrap();
        self.command("ledger/expire unstarted admission", move |tx| {
            let expired = tx.mark_admission_expired(&principal, &expiring)?;
            // Crossing the clock also lapses other accounts' unstarted holds.
            tx.tick(at)?;
            Ok(expired)
        });
    }
}
