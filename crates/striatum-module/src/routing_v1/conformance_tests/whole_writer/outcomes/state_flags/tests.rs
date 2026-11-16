use super::*;
use crate::account_writer as writer;
use striatum_core::{accounts::Fence, route_state_store::CredentialRefusal};
use striatum_store::AccountPoolFenceRow;

#[tokio::test]
async fn an_unclassified_credential_infers_quota_only_from_its_standing_hold() {
    let _clock = crate::InjectedClock::set(START);
    for reload in [false, true] {
        for held in [false, true] {
            let mut world =
                World::new(&format!("native-credential-inference-{held}-{reload}")).await;
            let id = "credential-inference-admission";
            world.ok(
                Caller::Runner,
                "route.account_for",
                json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c","conversation_key":id,"admission_id":id}),
            ).await;
            let principal = format!("reserved:{}", fixture::RUNNER_ONE);
            let admission = world
                .command("inference/read actual admission", move |tx| {
                    tx.admission(&principal, id)
                })
                .unwrap();
            let now = world.now as i64;
            if held {
                let fence_id = format!(
                    "credential-quota:{}:native-inference",
                    json!([admission.provider, admission.credential_id])
                );
                let mut evidence = world.quota_fence(fence_id, now - 1);
                evidence.provider = admission.provider.clone();
                evidence.account_id = admission.account_id.clone();
                let mut fence: Fence = serde_json::from_str(&evidence.fence_json).unwrap();
                fence.account_id = admission.account_id.clone();
                evidence.fence_json = serde_json::to_string(&fence).unwrap();
                let row = AccountPoolFenceRow {
                    evidence,
                    pool_key: None,
                    recovery_set: Vec::new(),
                };
                world.command("inference/standing credential quota hold", move |tx| {
                    tx.insert_pool_fence(&row)
                });
            }
            let refusal = CredentialRefusal {
                provider_id: admission.provider.clone(),
                credential_id: admission.credential_id.clone(),
                code: "ENTITLEMENT_ERROR".into(),
            };
            world.adapter("inference/unclassified credential evidence", |store| {
                writer::record_credential_entitlement_fence(store, &refusal, None, now)
            });
            if reload {
                world.reload();
            }
            let (provider, credential) = (admission.provider, admission.credential_id);
            world.command("inference/assert stored class and hold-derived quota", move |tx| {
                let blocks = tx.working_blocks()?.unwrap();
                let origins = blocks.fence_rechecks(now)?;
                let matching = origins.iter().filter(|row| row.kind == "credential"
                    && row.provider == provider
                    && row.credential_id.as_deref() == Some(&credential)).collect::<Vec<_>>();
                assert_eq!(matching.len(), 1);
                assert!(!matching[0].classified, "the witness must not explicitly classify the refusal");
                assert_eq!(matching[0].quota, held,
                    "an unclassified credential must infer quota from its active hold, not its code alone");
                let prefix = format!("credential-quota:{}:", json!([provider, credential]));
                assert_eq!(tx.active_fences(&provider)?.iter().any(|row| row.fence_id.starts_with(&prefix)), held);
                Ok(())
            });
        }
    }
}
