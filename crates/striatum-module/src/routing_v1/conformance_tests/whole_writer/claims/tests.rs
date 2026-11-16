use super::*;
use crate::account_writer as writer;
use striatum_core::route_state_store::CredentialRefusal;

impl World {
    pub(super) async fn claims_group(&mut self, nonce: u64) {
        for mode in 0..5 {
            let originals = self
                .catalog
                .accounts
                .iter()
                .enumerate()
                .filter(|(_, row)| row.provider == fixture::SUBSCRIPTION_PROVIDER)
                .map(|(index, row)| (index, row.credential_id.clone()))
                .collect::<Vec<_>>();
            for (index, original) in &originals {
                self.catalog.accounts[*index].credential_id =
                    format!("{original}-claim-{nonce}-{mode}-{}", self.sequence);
            }
            self.publish_catalog();
            let id = format!("claimed-admission-{nonce}-{mode}-{}", self.sequence);
            self.ok(Caller::Runner,"route.account_for",json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":id})).await;
            let principal = format!("reserved:{}", fixture::RUNNER_ONE);
            let (p, a) = (principal.clone(), id.clone());
            let admission = self
                .command("claim/read issued admission", move |tx| {
                    tx.admission(&p, &a)
                })
                .unwrap();
            let now = i64::try_from(self.now).unwrap();
            let refusal = CredentialRefusal {
                provider_id: admission.provider.clone(),
                credential_id: admission.credential_id.clone(),
                code: "ENTITLEMENT_ERROR".into(),
            };
            self.adapter("claim/fresh identity with due refusal", |store| {
                writer::record_credential_entitlement_fence(store, &refusal, None, now - 900_001)
            });
            let (p, c) = (admission.provider.clone(), admission.credential_id.clone());
            self.command("claim/classify evidence", move |tx| {
                tx.classify_credential_quota(&p, &c, true)
            });
            let credential = admission.credential_id.clone();
            let origins = self.command("claim/current due evidence", move |tx| {
                Ok(tx
                    .working_blocks()?
                    .unwrap()
                    .fence_rechecks(now)?
                    .into_iter()
                    .filter(|row| {
                        row.kind == "credential"
                            && row.credential_id.as_deref() == Some(&credential)
                    })
                    .collect::<Vec<_>>())
            });
            assert_eq!(origins.len(), 1);
            let owner = json!(["account_admission", principal, id]).to_string();
            match mode {
                0 => {
                    let (p, c) = (admission.provider.clone(), admission.credential_id.clone());
                    let result = self.command("claim/credential alias", move |tx| {
                        striatum_store::AccountTx::claim_credential_probe(
                            tx,
                            &p,
                            &c,
                            now - 900_000,
                            now,
                        )
                    });
                    assert_eq!(result, striatum_store::CredentialProbeClaim::Claimed);
                }
                1 => assert!(self.command("claim/unowned origin", move |tx| {
                    striatum_store::AccountTx::claim_fence_rechecks(tx, &origins, now)
                })),
                2 => assert!(self.command("claim/admission origin", move |tx| {
                    striatum_store::AccountTx::claim_fence_rechecks_for_admission(
                        tx, &origins, now, None,
                    )
                })),
                3 | 4 => {
                    let authorized = owner.clone();
                    assert!(self.command("claim/authorized admission", move |tx| {
                        striatum_store::AccountTx::claim_fence_rechecks_for_authorized_admission(
                            tx,
                            &origins,
                            now,
                            None,
                            &authorized,
                        )
                    }));
                }
                _ => unreachable!(),
            }
            if mode == 3 {
                let (o, p) = (owner.clone(), admission.provider.clone());
                self.command("claim/authorized success", move |tx| {
                    tx.heal_authorized_probe(&o, &p, "model-c", now)
                });
                let o = owner.clone();
                self.command("claim/retire spent authorization", move |tx| {
                    tx.retire_probe_authorizations(&o)
                });
            }
            if mode == 2 {
                let served = admission.clone();
                self.command("claim/admission typed success", move |tx| {
                    tx.heal_admission_success(&served, None, None, None, now)
                });
            }
            if mode == 4 {
                let o = owner.clone();
                assert!(self.command("claim/admission owns a proof", move |tx| Ok(tx
                    .probe_authorization_owners()?
                    .contains(&o))));
                self.advance(60_001);
                let o = owner.clone();
                assert!(!self.command("claim/expiry spent proof", move |tx| Ok(tx
                    .probe_authorization_owners()?
                    .contains(&o))));
                self.witness("probe_expiry");
            }
            let (p, c) = (admission.provider.clone(), admission.credential_id.clone());
            self.command("claim/clear remaining refusal", move |tx| {
                tx.clear_credential_entitlement_fence(&p, &c)
            });
            if mode != 4 {
                let (p, a) = (principal.clone(), id.clone());
                self.command("claim/release issued admission", move |tx| {
                    tx.mark_admission_released(&p, &a, now)
                });
            }
            for (index, credential) in originals {
                self.catalog.accounts[index].credential_id = credential;
            }
            self.publish_catalog();
        }
    }
}
