use super::*;
use striatum_store::AccountSizeRefusal;

impl World {
    pub(super) async fn two_size_kinds_witness(&mut self, non_quota_first: bool, reload: bool) {
        let account = self
            .catalog
            .accounts
            .iter()
            .find(|row| row.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap()
            .clone();
        let request = self.shared_key_request("before-shared-size", 169_000);
        let before = self
            .ok(Caller::Runner, "route.account_for", request.clone())
            .await;
        assert_eq!(before["account"]["account_id"], account.account_id);
        let at = self.now as i64;
        let launched = self
            .command("shared size/read actual granted admission", move |tx| {
                tx.admission(
                    &format!("reserved:{}", fixture::RUNNER_ONE),
                    "before-shared-size",
                )
            })
            .unwrap();
        assert_eq!(launched.committed_at, at);
        assert_eq!(launched.input_size_class, Some(17));
        let non_quota = AccountSizeRefusal {
            provider: account.provider.clone(),
            credential_id: account.credential_id.clone(),
            model: "model-c".into(),
            size_class: 17,
            observed_at: at - 1,
            expires_at: at + 600_000,
        };
        let row = non_quota.clone();
        self.command("shared size/record non-quota origin", move |tx| {
            tx.record_non_quota_size_refusal(&row, at)
        });
        self.advance(1);
        let quota = AccountSizeRefusal {
            observed_at: self.now as i64,
            ..non_quota.clone()
        };
        let row = quota.clone();
        self.command(
            "shared size/record quota origin on the same tuple",
            move |tx| tx.record_account_size_refusal(&row),
        );
        if reload {
            self.reload();
        }
        self.assert_size_kinds(&account.credential_id, true, true);
        self.assert_shared_key_blocked("both-size-origins", 169_000)
            .await;
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", request.clone())
                .await,
            before
        );
        self.advance(1);
        if non_quota_first {
            // The first report arrived after allocation but names an older
            // evidence time. The actual granted admission's launch time can
            // disprove that evidence, never the later quota report.
            self.heal_shared_size(&launched);
            self.assert_size_kinds(&account.credential_id, true, false);
        } else {
            self.clear_shared_quota_size(&account.credential_id);
            self.assert_size_kinds(&account.credential_id, false, true);
        }
        if reload {
            self.reload();
        }
        self.assert_shared_key_blocked("one-size-origin-remains", 169_000)
            .await;
        self.advance(1);
        if non_quota_first {
            self.clear_shared_quota_size(&account.credential_id);
        } else {
            self.heal_shared_size(&launched);
        }
        self.assert_size_kinds(&account.credential_id, false, false);
        let cleared_request = self.shared_key_request("after-shared-size", 169_000);
        let cleared = self
            .ok(Caller::Runner, "route.account_for", cleared_request.clone())
            .await;
        // Run a real Tick between removal and delayed receipts. Re-recording
        // a removed size source is not replaying a retained tombstone: the
        // existing size-writer contract permits the source to stand again.
        self.advance(1);
        let row = quota.clone();
        self.command("shared size/delayed quota source", move |tx| {
            tx.record_account_size_refusal(&row)
        });
        let now = self.now as i64;
        self.command("shared size/delayed non-quota source", move |tx| {
            tx.record_non_quota_size_refusal(&non_quota, now)
        });
        self.assert_size_kinds(&account.credential_id, true, true);
        self.assert_shared_key_blocked("delayed-size-origins", 169_000)
            .await;
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", request.clone())
                .await,
            before
        );
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", cleared_request.clone())
                .await,
            cleared
        );
        self.reload();
        self.assert_size_kinds(&account.credential_id, true, true);
        self.assert_shared_key_blocked("delayed-size-origins-after-reload", 169_000)
            .await;
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", request).await,
            before
        );
        assert_eq!(
            self.ok(Caller::Runner, "route.account_for", cleared_request)
                .await,
            cleared
        );
    }

    fn assert_size_kinds(&mut self, credential: &str, quota: bool, non_quota: bool) {
        let credential = credential.to_owned();
        self.command("shared size/read both exact origin kinds", move |tx| {
            let blocks = tx.working_blocks()?.unwrap();
            assert_eq!(
                blocks
                    .size_refusal(
                        true,
                        fixture::SUBSCRIPTION_PROVIDER,
                        &credential,
                        "model-c",
                        17
                    )
                    .is_some(),
                quota
            );
            assert_eq!(
                blocks
                    .size_refusal(
                        false,
                        fixture::SUBSCRIPTION_PROVIDER,
                        &credential,
                        "model-c",
                        17
                    )
                    .is_some(),
                non_quota
            );
            Ok(())
        });
    }

    fn clear_shared_quota_size(&mut self, credential: &str) {
        let credential = credential.to_owned();
        let now = self.now as i64;
        self.command(
            "shared size/clear quota without clearing non-quota",
            move |tx| {
                tx.clear_account_size_refusal(
                    fixture::SUBSCRIPTION_PROVIDER,
                    &credential,
                    "model-c",
                    17,
                    now,
                )
            },
        );
    }

    fn heal_shared_size(&mut self, admission: &striatum_store::AccountAdmissionRow) {
        let admission = admission.clone();
        let now = self.now as i64;
        let cleared = self.command(
            "shared size/success for the actual granted admission",
            move |tx| tx.heal_admission_success(&admission, Some(17), None, None, now),
        );
        assert_eq!(
            cleared
                .iter()
                .map(|row| row.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["non_quota_size"],
            "success can disprove only the older origin on the shared tuple"
        );
    }
}
