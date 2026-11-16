use super::*;
use crate::account_writer as writer;
use striatum_core::route_state_store::CredentialRefusal;

impl World {
    pub(super) async fn authorization_group(&mut self, nonce: u64) {
        self.authorization_group_with_outcomes(nonce, None).await;
    }

    async fn authorization_group_with_outcomes(&mut self, nonce: u64, reload: Option<bool>) {
        let index = self
            .catalog
            .accounts
            .iter()
            .position(|account| account.provider == fixture::SUBSCRIPTION_PROVIDER)
            .unwrap();
        let original = self.catalog.accounts[index].credential_id.clone();
        let credential = format!("{original}-probe-{nonce}-{}", self.sequence);
        self.catalog.accounts[index].credential_id = credential.clone();
        self.publish_catalog();
        let provider = fixture::SUBSCRIPTION_PROVIDER.to_owned();
        let account = self.catalog.accounts[index].account_id.clone();
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
        let decision = selected["decision_id"].as_str().unwrap().to_owned();
        let alternative = if reload.is_some() {
            let selected = self
                .ok(
                    Caller::Owner,
                    "route.select",
                    json!({"consumer":self.consumer,"requirements":{"iq":30},"pin":format!("{provider}/model-c")}),
                )
                .await;
            let other = selected["decision_id"].as_str().unwrap().to_owned();
            assert_ne!(
                other, decision,
                "a second selection must issue a new decision"
            );
            Some(other)
        } else {
            None
        };
        let (d, p, a, c) = (
            decision.clone(),
            provider.clone(),
            account.clone(),
            credential.clone(),
        );
        self.command("authorization/actual served identity", move |tx| {
            tx.bind_model_account(&d, &p, "model-c", &a, &c)
        });
        let d = decision.clone();
        self.command("authorization/served tier", move |tx| {
            tx.record_account_service_tier(&d, None, "standard")
        });
        let now = i64::try_from(self.now).unwrap();
        let refusal = CredentialRefusal {
            provider_id: provider.clone(),
            credential_id: credential.clone(),
            code: "ENTITLEMENT_ERROR".into(),
        };
        self.adapter(
            "authorization/refusal on newly listed credential",
            |store| {
                writer::record_credential_entitlement_fence(store, &refusal, None, now - 900_001)
            },
        );
        let (p, c) = (provider.clone(), credential.clone());
        self.command("authorization/classify held evidence", move |tx| {
            tx.classify_credential_quota(&p, &c, true)
        });
        let c = credential.clone();
        let origins = self.command("authorization/read due origins", move |tx| {
            Ok(tx
                .working_blocks()?
                .unwrap()
                .fence_rechecks(now)?
                .into_iter()
                .filter(|row| row.kind == "credential" && row.credential_id.as_deref() == Some(&c))
                .collect::<Vec<_>>())
        });
        assert_eq!(
            origins.len(),
            1,
            "the newly listed refusal has no claimable clock"
        );
        let from = format!("prepared-probe-{nonce}-{}", self.sequence);
        let owner = from.clone();
        let denied_origins = origins.clone();
        assert!(
            self.command("authorization/grant", move |tx| tx.grant_fence_rechecks(
                &origins,
                now,
                Some(&owner)
            ))
        );
        if let Some(reload) = reload {
            if reload {
                self.reload();
            }
            let foreign = alternative.clone().unwrap();
            assert!(
                !self.command("authorization/foreign grant is refused", move |tx| {
                    tx.grant_fence_rechecks(&denied_origins, now, Some(&foreign))
                }),
                "another owner reused a live probe slot, reload={reload}"
            );
        }
        let temporary = format!("bound-{from}");
        self.adapter("authorization/rebind prepared owner", |store| {
            writer::bind_fence_probe_owner(store, &from, &temporary)
        });
        if reload.is_some() {
            let (old, new) = (from.clone(), temporary.clone());
            self.command(
                "authorization/transfer names only its new owner",
                move |tx| {
                    let owners = tx.probe_authorization_owners()?;
                    assert!(!owners.contains(&old), "a transfer retained its old owner");
                    assert!(owners.contains(&new), "a transfer lost its new owner");
                    Ok(())
                },
            );
        }
        let (p, c) = (provider.clone(), credential.clone());
        let origins = self.command("authorization/read granted origins", move |tx| {
            Ok(tx
                .working_blocks()?
                .unwrap()
                .fence_rechecks(now)?
                .into_iter()
                .filter(|row| row.provider == p && row.credential_id.as_deref() == Some(&c))
                .collect::<Vec<_>>())
        });
        if let Some(other) = alternative {
            let (from, a, c, denied) = (
                format!("not-{temporary}"),
                account.clone(),
                credential.clone(),
                origins.clone(),
            );
            assert!(
                !self.command(
                    "authorization/foreign decision bind is refused",
                    move |tx| { tx.bind_decision_probe(&denied, &from, &other, &a, &c, now) }
                ),
                "a different selected decision consumed a live probe slot"
            );
        }
        let (d, a, c) = (decision.clone(), account.clone(), credential.clone());
        assert!(
            self.command("authorization/bind selected decision", move |tx| tx
                .bind_decision_probe(&origins, &temporary, &d, &a, &c, now))
        );
        if reload == Some(true) {
            self.reload();
        }
        let (d, c) = (decision.clone(), credential.clone());
        assert!(
            self.command("authorization/selected credential is named", move |tx| tx
                .probe_authorization_names_credential(&d, &c, "model-c"))
        );
        if reload.is_some() {
            let (d, c) = (decision.clone(), format!("not-{credential}"));
            assert!(
                !self.command("authorization/another credential is not named", move |tx| {
                    tx.probe_authorization_names_credential(&d, &c, "model-c")
                }),
                "a probe proof named a different credential"
            );
            let (d, c) = (decision.clone(), credential.clone());
            assert!(
                self.command("authorization/exact report matches", move |tx| {
                    tx.probe_report_matches(&d, Some(&c), false)
                })
            );
            let (d, c) = (decision.clone(), format!("not-{credential}"));
            assert!(
                !self.command(
                    "authorization/another report credential is refused",
                    move |tx| { tx.probe_report_matches(&d, Some(&c), false) }
                ),
                "a report with another credential matched the selected probe"
            );
            let (d, c) = (decision.clone(), credential.clone());
            assert!(
                !self.command("authorization/preserved report cannot settle", move |tx| {
                    tx.probe_report_matches(&d, Some(&c), true)
                }),
                "a preserved report was allowed to settle a probe"
            );
        }
        let before_release = reload.is_some().then(|| self.native_block_rows());
        let (d, p) = (decision.clone(), provider.clone());
        self.command("authorization/inconclusive release", move |tx| {
            tx.release_inconclusive_probe(&d, &p, "model-c", now)
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
            let (d, p) = (decision.clone(), provider.clone());
            self.command("authorization/repeated inconclusive release", move |tx| {
                tx.release_inconclusive_probe(&d, &p, "model-c", now)
            });
            assert_eq!(
                self.native_block_rows(),
                before,
                "an already released probe changed durable native rows"
            );
        }
        let (p, c) = (provider.clone(), credential.clone());
        let held = self
            .command("authorization/read released refusal", move |tx| {
                tx.credential_entitlement_fence(&p, &c)
            })
            .expect("an inconclusive probe lost its refusal");
        self.command("authorization/clear exact released refusal", move |tx| {
            tx.clear_held_entitlement_fence(&held)
        });
        self.catalog.accounts[index].credential_id = original;
        self.publish_catalog();
    }
}

#[tokio::test]
async fn native_probe_authorization_decisions_preserve_exclusive_ownership() {
    let _clock = crate::InjectedClock::set(START);
    for reload in [false, true] {
        let mut world = World::new(&format!("native-authorization-outcomes-{reload}")).await;
        world
            .authorization_group_with_outcomes(11, Some(reload))
            .await;
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
