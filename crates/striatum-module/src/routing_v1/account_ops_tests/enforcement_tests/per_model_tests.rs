use super::*;
use crate::account_quota::{RosterIdentity, VaultRoster};
use striatum_core::RouteStateStore as _;

fn catalog_caps(harness: &Harness, enforced: bool) {
    reading(NOW);
    let state = {
        let mut state = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
        state.roster = Some(VaultRoster {
            view: "per-model-roster".into(),
            fetched_at_ms: NOW as i64,
            grants: vec![],
            identities: vec![RosterIdentity {
                refresh_adapter: "test-source".into(),
                account_id: A.into(),
                credential_ids: vec![credential(A).into()],
                active_credential_ids: vec![credential(A).into()],
                first_seen_ms: NOW as i64,
                plan_tier: Some("per-model".into()),
            }],
        });
        state.clone()
    };
    let mut prices = crate::concurrency_caps::tests::per_model_prices();
    prices["prices"][0]["provider_id"] = json!("synthetic");
    prices["prices"][0]["concurrency"]["per_model"] = json!({"synthetic/model":2});
    let mut credentials = harness.credentials().lock().unwrap().clone();
    credentials.publish_plan_prices(&prices);
    credentials.refresh_serving_methods(
        &state,
        &harness.store().get_facts().unwrap(),
        NOW as i64,
        &[],
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(true);
        config["concurrency"] = json!({"enforcement_enabled":enforced});
    });
}

fn model_request(model: &str, id: &str) -> Value {
    let mut body = on(A, id);
    body["model"] = json!(model);
    body
}

fn model_count(harness: &Harness, model: &str) -> usize {
    crate::account_state::snapshot(harness.store())
        .unwrap()
        .inflight
        .count(
            "synthetic",
            A,
            InflightScope::AccountModel(model),
            NOW as i64,
        )
}

#[tokio::test]
async fn model_cap_allows_a_second_admit_for_an_open_run() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    catalog_caps(&harness, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    for id in ["first", "second"] {
        let admitted = admitted_on(&runner, &model_request("model", id)).await;
        assert_eq!(admitted["account"]["account_id"], A);
        start_admission(&runner, id, id).await;
    }
    assert_eq!(model_count(&harness, "model"), 2);

    let mut next = model_request("model", "first-next");
    next["conversation_key"] = json!("first");
    next["mode"] = json!("admit");
    assert_eq!(
        admitted_on(&runner, &next).await["account"]["account_id"],
        A
    );
    start_admission(&runner, "first-next", "first").await;
    assert_eq!(model_count(&harness, "model"), 2);

    assert_concurrency_full(
        &runner
            .call("route.account_for", &model_request("model", "third"))
            .await,
        NOW + 30_000,
    );
}

#[tokio::test]
async fn catalog_per_model_admission_limits_fallback_and_override_without_changing_shadow_replies()
{
    let _clock = crate::InjectedClock::set(NOW);
    for enforced in [true, false] {
        let (harness, _) = setup("synthetic", false);
        caps(&harness, 3, enforced);
        select_ledger_tests::second_model(&harness, false);
        catalog_caps(&harness, enforced);
        let runner = harness.bind(reserved("class-runner"), None).await;
        for i in 0..2 {
            admitted_on(&runner, &model_request("model", &format!("keyed-{i}"))).await;
        }
        let third = runner
            .call("route.account_for", &model_request("model", "keyed-third"))
            .await;
        if enforced {
            assert_concurrency_full(&third, NOW + 30_000);
        } else {
            assert_eq!(response(&third)["account"]["account_id"], A);
        }
        for i in 0..3 {
            admitted_on(&runner, &model_request("other", &format!("fallback-{i}"))).await;
        }
        let fourth = runner
            .call(
                "route.account_for",
                &model_request("other", "fallback-fourth"),
            )
            .await;
        if enforced {
            assert_concurrency_full(&fourth, NOW + 30_000);
            assert_eq!(
                (
                    model_count(&harness, "model"),
                    model_count(&harness, "other")
                ),
                (2, 3)
            );
        } else {
            assert_eq!(response(&fourth)["account"]["account_id"], A);
            assert_eq!(
                (
                    model_count(&harness, "model"),
                    model_count(&harness, "other")
                ),
                (3, 4)
            );
        }
        // Omitting scope inherits account_model, but the winning setting's
        // limit replaces both the plan default and its lower model limit.
        harness.edit_config(|config| {
            config["concurrency"]["plans"] = json!({"synthetic/per-model":{"limit":4}});
        });
        let keyed_count = if enforced { 2 } else { 3 };
        for i in keyed_count..4 {
            let mut body = model_request("model", &format!("override-{i}"));
            // Let the refused conversation retry first so its waiter does not
            // reserve the newly available slot against a different conversation.
            if i == keyed_count {
                body["conversation_key"] = json!("keyed-third");
            }
            admitted_on(&runner, &body).await;
        }
        let fifth = runner
            .call(
                "route.account_for",
                &model_request("model", "override-fifth"),
            )
            .await;
        if enforced {
            assert_concurrency_full(&fifth, NOW + 30_000);
            admitted_on(&runner, &model_request("other", "fallback-fourth")).await;
            assert_concurrency_full(
                &runner
                    .call(
                        "route.account_for",
                        &model_request("other", "fallback-override-fifth"),
                    )
                    .await,
                NOW + 30_000,
            );
            assert_eq!(
                (
                    model_count(&harness, "model"),
                    model_count(&harness, "other")
                ),
                (4, 4)
            );
        } else {
            assert_eq!(response(&fifth)["account"]["account_id"], A);
            assert_eq!(model_count(&harness, "model"), 5);
        }
    }
}
