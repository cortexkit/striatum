use super::*;
use striatum_store::{AccountExclusionRow, AccountSizeRefusal};

impl World {
    pub(super) async fn admit_scope_witnesses(&mut self, seed: u64) {
        for reload in [false, true] {
            let _clock = crate::InjectedClock::set(START);
            let mut size = World::new(&format!("size-admit-{seed}-{reload}")).await;
            size.size_refusal_changes_admit(reload).await;
            let _clock = crate::InjectedClock::set(START);
            let mut principal = World::new(&format!("principal-admit-{seed}-{reload}")).await;
            principal.principal_exclusion_changes_admit(reload).await;
        }
    }

    fn scoped_request(id: &str, tokens: u64) -> Value {
        json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
            "conversation_key":id,"admission_id":id,"initial_input_bytes":tokens*4,
            "initial_input_estimated":false})
    }

    async fn size_refusal_changes_admit(&mut self, reload: bool) {
        self.ok(
            Caller::Runner,
            "route.account_for",
            Self::scoped_request("large-without-wall", 169_000),
        )
        .await;
        let now = self.now as i64;
        let facts: Vec<_> = self
            .catalog
            .accounts
            .iter()
            .filter(|account| account.provider == fixture::SUBSCRIPTION_PROVIDER)
            .map(|account| AccountSizeRefusal {
                provider: account.provider.clone(),
                credential_id: account.credential_id.clone(),
                model: "model-c".into(),
                size_class: 17,
                expires_at: now + 900_000,
                observed_at: now,
            })
            .collect();
        for fact in facts {
            self.command("size witness/real quota refusal", move |tx| {
                tx.record_account_size_refusal(&fact)
            });
        }
        if reload {
            self.reload();
        }
        let blocked = self
            .request(
                Caller::Runner,
                "route.account_for",
                Self::scoped_request("large-with-wall", 169_000),
            )
            .await;
        assert_eq!(
            blocked.0,
            FrameType::Error,
            "a size wall must change the real Admit reply: {}",
            blocked.1
        );
        assert_eq!(blocked.1["code"], "account_not_authorized", "{}", blocked.1);
        let small = self
            .ok(
                Caller::Runner,
                "route.account_for",
                Self::scoped_request("small-with-wall", 4_096),
            )
            .await;
        assert!(
            small["account"]["credential_id"].is_string(),
            "another size band must stay usable: {small}"
        );
        let retry = Self::scoped_request("large-with-wall", 169_000);
        if reload {
            self.reload();
        }
        let retried = self
            .request(Caller::Runner, "route.account_for", retry)
            .await;
        assert_eq!(retried.0, FrameType::Error, "{}", retried.1);
        assert_eq!(
            retried.1["code"], blocked.1["code"],
            "a retried id must not escape its size wall"
        );
    }

    async fn principal_exclusion_changes_admit(&mut self, reload: bool) {
        let admitted = self
            .ok(
                Caller::Runner,
                "route.account_for",
                Self::scoped_request("unexcluded-admission", 4_096),
            )
            .await;
        assert!(admitted["account"]["credential_id"].is_string());
        let now = self.now as i64;
        let roster = crate::ACCOUNT_QUOTA
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .roster
            .as_ref()
            .unwrap()
            .view
            .clone();
        let rows: Vec<_> = self
            .catalog
            .accounts
            .iter()
            .filter(|account| account.provider == fixture::SUBSCRIPTION_PROVIDER)
            .map(|account| AccountExclusionRow {
                principal: format!("reserved:{}", fixture::RUNNER_ONE),
                admission_id: "excluded-admission".into(),
                credential_id: account.credential_id.clone(),
                runner_view_digest: "scope-witness-runner".into(),
                roster_view_digest: roster.clone(),
                observation_id: None,
                recorded_at: now,
            })
            .collect();
        for exclusion in rows {
            assert!(
                self.command("principal witness/real scoped refusal", move |tx| tx
                    .insert_exclusion_with_recheck(&exclusion, 60_000))
            );
        }
        if reload {
            self.reload();
        }
        let excluded = self
            .request(
                Caller::Runner,
                "route.account_for",
                Self::scoped_request("excluded-admission", 4_096),
            )
            .await;
        assert_eq!(
            excluded.0,
            FrameType::Error,
            "principal exclusions must change the real Admit reply: {}",
            excluded.1
        );
        assert_eq!(
            excluded.1["code"], "account_pool_exhausted",
            "{}",
            excluded.1
        );
        self.ok(
            Caller::Runner,
            "route.account_for",
            Self::scoped_request("another-admission", 4_096),
        )
        .await;
        if reload {
            self.reload();
        }
        let retried = self
            .request(
                Caller::Runner,
                "route.account_for",
                Self::scoped_request("excluded-admission", 4_096),
            )
            .await;
        assert_eq!(retried.0, FrameType::Error, "{}", retried.1);
        assert_eq!(
            retried.1["code"], excluded.1["code"],
            "another admission's success cannot lift this scoped refusal"
        );
    }
}

#[tokio::test]
async fn size_and_principal_refusals_each_change_a_real_admit_before_and_after_reload() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("admit-scope-witness-driver").await;
    world.admit_scope_witnesses(1).await;
}
