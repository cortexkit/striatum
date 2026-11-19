use super::*;
use striatum_store::V1Recorded;

impl World {
    pub(super) async fn continuation_group(&mut self, nonce: u64) {
        let provider = fixture::SUBSCRIPTION_PROVIDER;
        let accounts = self
            .catalog
            .accounts
            .iter()
            .filter(|row| row.provider == provider)
            .map(|row| row.account_id.clone())
            .collect::<Vec<_>>();
        assert!(
            accounts.len() >= 2,
            "the shared seed needs two subscription accounts for a switch"
        );
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
        let previous = selected["decision_id"].as_str().unwrap().to_owned();
        let principal = format!("reserved:{}", fixture::RUNNER_ONE);
        let original = format!("continued-original-{nonce}-{}", self.sequence);
        let reservation = format!("continued-reservation-{nonce}-{}", self.sequence);
        let run = format!("run-{original}");
        let first_entry = self.trace.entries().len();
        let admitted = self.ok(Caller::Runner,"route.account_for",json!({"provider":provider,"model":"model-c","conversation_key":original,"admission_id":original,"decision_id":previous,"service_tier":"standard","requested_account":{"account_id":accounts[0]}})).await;
        // The trace counts changes to allocator state, not updates to the
        // recorded decisions. Check the calls and read back their values.
        let entries = self.trace.entries();
        let step = &entries[first_entry..];
        for producer in [
            "account_tx::account_selection::bind_model_account",
            "account_tx::account_selection::record_account_service_tier",
        ] {
            assert!(
                step.iter()
                    .any(|entry| entry.name == producer && entry.account_rows_written.is_some()),
                "decision-linked admission did not reach {producer}: {step:?}"
            );
        }
        let store = self.harness.store();
        let identity = store
            .decision_provider_identity(&previous)
            .unwrap()
            .unwrap();
        assert_eq!(
            identity.account_ref,
            admitted["account"]["account_id"].as_str().unwrap()
        );
        assert_eq!(
            identity.credential_id.as_deref(),
            admitted["account"]["credential_id"].as_str()
        );
        let now = i64::try_from(self.now).unwrap();
        let (p, a, r) = (principal.clone(), original.clone(), run.clone());
        let account_decision = self.command("continuation/start original run", move |tx| {
            let admission = tx
                .admission(&p, &a)?
                .expect("the original admission was issued");
            assert!(tx.mark_admission_started(&p, &a, Some(&r), now, false)?);
            Ok(admission.decision_id)
        });
        assert_eq!(
            self.harness
                .store()
                .account_decision_service_tier(&account_decision)
                .unwrap(),
            Some((Some("standard".into()), "standard".into()))
        );
        self.ok(Caller::Runner,"route.account_for",json!({"provider":provider,"model":"model-c","conversation_key":reservation,"admission_id":reservation,"requested_account":{"account_id":accounts[1]}})).await;
        let (p, a) = (principal.clone(), reservation.clone());
        let reserved = self
            .command("continuation/read issued reservation", move |tx| {
                tx.admission(&p, &a)
            })
            .unwrap();
        let decision = format!("continued-model-{nonce}-{}", self.sequence);
        let (old, new) = (previous.clone(), decision.clone());
        self.command("continuation/copy real selection provenance", move |tx| {
            tx.link_continued_decision(&old, &new, now, now + 60_000)
        });
        let (account_decision, admission) = (reserved.decision_id.clone(), original.clone());
        self.command("continuation/bind account-only history", move |tx| {
            tx.bind_continued_account_decision(&account_decision, &admission)
        });
        let selection = format!("offer-{nonce}-{}", self.sequence);
        let record=V1Recorded{body_json:json!({"admission_id":original,"run_id":run,"previous_decision_id":previous}).to_string(),answer_json:json!({"decision_id":decision,"reservation_id":reservation,"valid_until_ms":now+60_000}).to_string()};
        let (p, s, r, a, old, new, res) = (
            principal.clone(),
            selection.clone(),
            run.clone(),
            original.clone(),
            previous.clone(),
            decision.clone(),
            reservation.clone(),
        );
        self.command("continuation/record complete offer", move |tx| {
            tx.record_continuation(
                &p,
                &s,
                &record,
                &r,
                &a,
                &old,
                Some((&new, &res, now + 60_000)),
            )
        });
        let (p, a, r, account, credential) = (
            principal.clone(),
            original.clone(),
            run.clone(),
            reserved.account_id.clone(),
            reserved.credential_id.clone(),
        );
        assert!(
            self.command("continuation/observe account switch", move |tx| tx
                .use_observed_continuation(
                    &p,
                    &a,
                    &r,
                    &account,
                    Some(&credential),
                    now
                ))
        );
        let (p, a, r, new, account, credential) = (
            principal.clone(),
            original.clone(),
            run.clone(),
            decision.clone(),
            reserved.account_id.clone(),
            reserved.credential_id.clone(),
        );
        assert!(
            !self.command("continuation/keyed consumed replay", move |tx| {
                striatum_store::AccountTx::use_continuation(tx, &new, now)?;
                striatum_store::AccountTx::use_observed_continuation(
                    tx,
                    &p,
                    &a,
                    &r,
                    &account,
                    Some(&credential),
                    now,
                )
            })
        );
        let (p, a, r) = (principal.clone(), original.clone(), run.clone());
        self.command("continuation/finish the switched run", move |tx| {
            tx.mark_admission_finished(&p, &a, now)?;
            tx.finish_continuations(&p, &a, &r, now)
        });
        self.command("continuation/finished SQL replay", move |tx| {
            striatum_store::AccountTx::finish_continuations(tx, &principal, &original, &run, now)
        });
    }
}
