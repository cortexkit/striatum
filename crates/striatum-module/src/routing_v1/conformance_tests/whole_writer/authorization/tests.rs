use super::*;
use crate::account_writer as writer;
use cortexkit_role_routing_conformance::CatalogAccount;
use striatum_core::route_state_store::CredentialRefusal;
use striatum_store::FenceRecheck;

struct ProbeFixture {
    original_accounts: Vec<CatalogAccount>,
    account: String,
    credential: String,
}

impl World {
    pub(super) async fn authorization_group(&mut self, nonce: u64) {
        let fixture = self.prepare_authorization_credential(nonce, true).await;
        // Fresh quota headroom can lift a billing fence without a probe, but
        // it cannot establish that the provider restored entitlement.
        let origin = self.place_due_credential_fence(
            &fixture,
            self.now as i64 - crate::credential_fences::CREDENTIAL_REFUSED_REPROBE_MS - 1,
            false,
        );
        assert!(!origin.quota);
        assert!(origin.claim_owner.is_none());
        assert!(origin.next_recheck_at <= self.now as i64);

        let first_entry = self.trace.entries().len();
        let decision = self.select_authorization_route().await;
        self.assert_writes_since(
            first_entry,
            &["account_tx::fence_rechecks::bind_decision_probe"],
        );
        self.assert_selected_probe(&fixture, &origin, &decision);
        self.release_authorization_probe(&fixture, &decision, None);
        self.restore_authorization_credential(fixture);
    }

    async fn prepare_authorization_credential(
        &mut self,
        nonce: u64,
        selection_probe: bool,
    ) -> ProbeFixture {
        let original_accounts = self.catalog.accounts.clone();
        let index = self
            .catalog
            .accounts
            .iter()
            .position(|account| account.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        let original = &self.catalog.accounts[index].credential_id;
        let credential = format!("{original}-probe-{nonce}-{}", self.sequence);
        self.catalog.accounts[index].credential_id = credential.clone();
        let account = self.catalog.accounts[index].account_id.clone();
        if selection_probe {
            // The allocator may prefer an unfenced account over a due probe.
            self.catalog.accounts.retain(|row| {
                row.provider != fixture::SUBSCRIPTION_PROVIDER || row.account_id == account
            });
        }
        self.publish_catalog();
        let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        self.ok(
            Caller::Operator,
            "route.clear_cooldown",
            json!({"routeKey":route}),
        )
        .await;
        ProbeFixture {
            original_accounts,
            account,
            credential,
        }
    }

    async fn select_authorization_route(&mut self) -> String {
        let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        let selected = self
            .ok(
                Caller::Owner,
                "route.select",
                json!({"consumer":self.consumer,"requirements":{"iq":30},"pin":route}),
            )
            .await;
        selected["decision_id"].as_str().unwrap().to_owned()
    }

    fn place_due_credential_fence(
        &mut self,
        fixture: &ProbeFixture,
        observed: i64,
        classify_as_quota: bool,
    ) -> FenceRecheck {
        let provider = fixture::SUBSCRIPTION_PROVIDER;
        let credential = &fixture.credential;
        let now = i64::try_from(self.now).unwrap();
        let refusal = CredentialRefusal {
            provider_id: provider.into(),
            credential_id: credential.clone(),
            code: "ENTITLEMENT_ERROR".into(),
        };
        self.adapter(
            "authorization/refusal on newly listed credential",
            |store| writer::record_credential_entitlement_fence(store, &refusal, None, observed),
        );
        if classify_as_quota {
            let c = credential.clone();
            self.command("authorization/classify held evidence", move |tx| {
                tx.classify_credential_quota(provider, &c, true)
            });
        }
        let c = credential.clone();
        let mut origins = self.command("authorization/read due origins", move |tx| {
            Ok(tx
                .working_blocks()?
                .unwrap()
                .fence_rechecks(now)?
                .into_iter()
                .filter(|row| {
                    row.kind == "credential"
                        && row.provider == provider
                        && row.credential_id.as_deref() == Some(&c)
                })
                .collect::<Vec<_>>())
        });
        assert_eq!(
            origins.len(),
            1,
            "the newly listed refusal has no claimable clock"
        );
        origins.pop().unwrap()
    }

    async fn native_authorization_outcomes(&mut self, nonce: u64, reload: bool) {
        let fixture = self.prepare_authorization_credential(nonce, false).await;
        let decision = self.select_authorization_route().await;
        let alternative = self.select_authorization_route().await;
        assert_ne!(
            alternative, decision,
            "a second selection must issue a new decision"
        );
        self.assert_probe_identity(&decision, None);
        let origin = self.place_due_credential_fence(&fixture, self.now as i64 - 900_001, true);
        let now = self.now as i64;
        let from = format!("prepared-probe-{nonce}-{}", self.sequence);
        let owner = from.clone();
        let denied_origin = origin.clone();
        assert!(
            self.command("authorization/grant", move |tx| tx.grant_fence_rechecks(
                std::slice::from_ref(&origin),
                now,
                Some(&owner)
            ))
        );
        if reload {
            self.reload();
        }
        let foreign = alternative.clone();
        assert!(
            !self.command("authorization/foreign grant is refused", move |tx| {
                tx.grant_fence_rechecks(std::slice::from_ref(&denied_origin), now, Some(&foreign))
            }),
            "another owner reused a live probe slot, reload={reload}"
        );
        let (p, c) = (fixture::SUBSCRIPTION_PROVIDER, fixture.credential.clone());
        let origins = self.command("authorization/read granted origins", move |tx| {
            Ok(tx
                .working_blocks()?
                .unwrap()
                .fence_rechecks(now)?
                .into_iter()
                .filter(|row| row.provider == p && row.credential_id.as_deref() == Some(&c))
                .collect::<Vec<_>>())
        });
        assert_eq!(origins.len(), 1);
        assert_eq!(origins[0].claim_owner.as_deref(), Some(from.as_str()));
        let (foreign, a, c, denied) = (
            format!("not-{from}"),
            fixture.account.clone(),
            fixture.credential.clone(),
            origins.clone(),
        );
        let other = alternative.clone();
        assert!(
            !self.command(
                "authorization/foreign decision bind is refused",
                move |tx| { tx.bind_decision_probe(&denied, &foreign, &other, &a, &c, now) }
            ),
            "a different selected decision consumed a live probe slot"
        );
        self.assert_probe_identity(&alternative, None);
        let (d, a, c, old) = (
            decision.clone(),
            fixture.account.clone(),
            fixture.credential.clone(),
            from.clone(),
        );
        let first_entry = self.trace.entries().len();
        assert!(
            self.command("authorization/bind selected decision", move |tx| tx
                .bind_decision_probe(&origins, &old, &d, &a, &c, now))
        );
        producer_catalogue::assert_focused_transfer_wrote(&self.trace.entries()[first_entry..]);
        if reload {
            self.reload();
        }
        let new = decision.clone();
        self.command(
            "authorization/transfer names only its new owner",
            move |tx| {
                let owners = tx.probe_authorization_owners()?;
                assert!(!owners.contains(&from), "a transfer retained its old owner");
                assert!(owners.contains(&new), "a transfer lost its new owner");
                Ok(())
            },
        );
        self.assert_probe_identity(&decision, Some(&fixture));
        self.release_authorization_probe(&fixture, &decision, Some(reload));
        self.restore_authorization_credential(fixture);
    }

    fn assert_probe_identity(&self, decision: &str, fixture: Option<&ProbeFixture>) {
        let identity = self
            .harness
            .store()
            .decision_provider_identity(decision)
            .unwrap();
        if let Some(fixture) = fixture {
            let identity = identity.expect("probe bind did not record the selected identity");
            assert_eq!(identity.account_ref, fixture.account);
            assert_eq!(
                identity.credential_id.as_deref(),
                Some(fixture.credential.as_str())
            );
        } else {
            assert!(
                identity.is_none(),
                "unexpected identity before a successful probe bind"
            );
        }
    }

    fn assert_selected_probe(
        &mut self,
        fixture: &ProbeFixture,
        origin: &FenceRecheck,
        decision: &str,
    ) {
        self.assert_probe_identity(decision, Some(fixture));
        let (kind, key, decision) = (
            origin.kind.clone(),
            origin.origin_key.clone(),
            decision.to_owned(),
        );
        let now = self.now as i64;
        self.command("authorization/decision owns the exact origin", move |tx| {
            let state = tx.working_blocks()?.unwrap();
            let blocks = state.blocks(now)?.blocks;
            let clock = blocks
                .iter()
                .find(|row| row.kind == kind && row.origin_key == key)
                .expect("the selected probe lost its credential origin");
            assert_eq!(clock.claim_owner.as_deref(), Some(decision.as_str()));
            assert_eq!(clock.last_claimed_at, Some(now));
            assert!(clock.claim_generation > 0);
            let proofs = state.authorizations_of(&decision);
            let proof = proofs
                .iter()
                .find(|row| row.kind == kind && row.origin_key == key)
                .expect("the decision has no proof for the selected credential origin");
            assert_eq!(proof.claim_generation, clock.claim_generation);
            assert_eq!(Some(proof.claimed_at), clock.last_claimed_at);
            assert_eq!(proof.evidence, clock.evidence);
            assert_eq!(proof.evidence_at, clock.evidence_at);
            Ok(())
        });
    }

    fn release_authorization_probe(
        &mut self,
        fixture: &ProbeFixture,
        decision: &str,
        reload: Option<bool>,
    ) {
        let provider = fixture::SUBSCRIPTION_PROVIDER;
        let credential = &fixture.credential;
        let now = self.now as i64;
        let (d, c) = (decision.to_owned(), credential.clone());
        assert!(
            self.command("authorization/selected credential is named", move |tx| tx
                .probe_authorization_names_credential(&d, &c, "model-c"))
        );
        if reload.is_some() {
            let (d, c) = (decision.to_owned(), format!("not-{credential}"));
            assert!(
                !self.command("authorization/another credential is not named", move |tx| {
                    tx.probe_authorization_names_credential(&d, &c, "model-c")
                }),
                "a probe proof named a different credential"
            );
            let (d, c) = (decision.to_owned(), credential.clone());
            assert!(
                self.command("authorization/exact report matches", move |tx| {
                    tx.probe_report_matches(&d, Some(&c), false)
                })
            );
            let (d, c) = (decision.to_owned(), format!("not-{credential}"));
            assert!(
                !self.command(
                    "authorization/another report credential is refused",
                    move |tx| { tx.probe_report_matches(&d, Some(&c), false) }
                ),
                "a report with another credential matched the selected probe"
            );
            let (d, c) = (decision.to_owned(), credential.clone());
            assert!(
                !self.command("authorization/preserved report cannot settle", move |tx| {
                    tx.probe_report_matches(&d, Some(&c), true)
                }),
                "a preserved report was allowed to settle a probe"
            );
        }
        let before_release = reload.is_some().then(|| self.native_block_rows());
        let d = decision.to_owned();
        self.command("authorization/inconclusive release", move |tx| {
            tx.release_inconclusive_probe(&d, provider, "model-c", now)
        });
        if let Some(before) = before_release {
            assert_ne!(
                self.native_block_rows(),
                before,
                "an inconclusive release did not change its durable probe proof"
            );
            if reload == Some(true) {
                self.reload();
            }
            let before = self.native_block_rows();
            let d = decision.to_owned();
            self.command("authorization/repeated inconclusive release", move |tx| {
                tx.release_inconclusive_probe(&d, provider, "model-c", now)
            });
            assert_eq!(
                self.native_block_rows(),
                before,
                "an already released probe changed durable native rows"
            );
        }
        let c = credential.clone();
        let held = self
            .command("authorization/read released refusal", move |tx| {
                tx.credential_entitlement_fence(provider, &c)
            })
            .expect("an inconclusive probe lost its refusal");
        self.command("authorization/clear exact released refusal", move |tx| {
            tx.clear_held_entitlement_fence(&held)
        });
    }

    fn restore_authorization_credential(&mut self, fixture: ProbeFixture) {
        self.catalog.accounts = fixture.original_accounts;
        self.publish_catalog();
    }
}

#[tokio::test]
async fn native_probe_authorization_decisions_preserve_exclusive_ownership() {
    let _clock = crate::InjectedClock::set(START);
    for reload in [false, true] {
        let mut world = World::new(&format!("native-authorization-outcomes-{reload}")).await;
        world.native_authorization_outcomes(11, reload).await;
        for name in [
            "grant_fence_rechecks",
            "bind_decision_probe",
            "probe_authorization_names_credential",
            "probe_report_matches",
            "release_inconclusive_probe",
        ] {
            let producer = format!("account_tx::fence_rechecks::{name}");
            assert!(
                world
                    .trace
                    .entries()
                    .iter()
                    .filter(|entry| entry.name == producer)
                    .count()
                    >= 2,
                "both outcomes must reach the real native producer: {producer}, reload={reload}"
            );
        }
        world.compare("all native authorization outcomes checked");
    }
}
