use super::*;

async fn disjoint(offered: &[String], eligible: &[String]) -> Value {
    let harness = paid_anthropic_harness();
    let shared = harness.credentials();
    let mut credentials = shared.lock().unwrap().clone();
    let template = credentials.listed_credentials()[0].clone();
    let mut rows: Vec<_> = eligible
        .iter()
        .map(|id| ListedCredential {
            id: id.clone(),
            ..template.clone()
        })
        .collect();
    // A vault row for another provider must not appear in eligible diagnostics.
    rows.push(ListedCredential {
        id: "apikey:unrelated".into(),
        provider_ids: vec!["openai".into()],
        serves: vec!["openai".into()],
        ..template
    });
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "disjoint-vault".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    let broca = harness.bind(reserved("broca"), None).await;
    let mut request = credential_only_request("disjoint");
    request["candidates"]["accounts"] = json!(offered
        .iter()
        .map(|id| json!({
            "account_id":id, "credential_id":id,
        }))
        .collect::<Vec<_>>());
    let reply = broca.call("route.account_for", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "account_unservable", "{}", reply.1);
    assert_eq!(
        cortexkit_role_routing::refusals::retry_class("account_unservable", &reply.1["detail"]),
        cortexkit_role_routing::refusals::RetryClass::Permanent
    );
    assert!(harness
        .store()
        .v1_admission("reserved:broca", "disjoint")
        .unwrap()
        .is_none());
    reply.1["detail"].clone()
}

#[tokio::test]
async fn stable_refusal_live_disjoint_candidates_name_actual_eligible_keys() {
    let _clock = crate::InjectedClock::set(NOW);
    let offered = vec!["apikey:muse".into(), "apikey:cline-pass".into()];
    let eligible = (0..5)
        .rev()
        .map(|n| format!("apikey:anthropic:fixture-{n}"))
        .collect::<Vec<_>>();
    let detail = disjoint(&offered, &eligible).await;
    assert_eq!(
        detail,
        json!({"reason":"candidates_disjoint",
        "offered_credential_ids":["apikey:cline-pass","apikey:muse"], "offered_credential_count":2,
        "eligible_credential_ids":["apikey:anthropic:fixture-0","apikey:anthropic:fixture-1",
            "apikey:anthropic:fixture-2","apikey:anthropic:fixture-3","apikey:anthropic:fixture-4"],
        "eligible_credential_count":5})
    );
}

#[tokio::test]
async fn stable_refusal_disjoint_seventy_credentials_are_sorted_and_capped() {
    let _clock = crate::InjectedClock::set(NOW);
    let offered = (0..70)
        .rev()
        .map(|n| format!("apikey:offered-{n:02}"))
        .collect::<Vec<_>>();
    let eligible = (0..70)
        .rev()
        .map(|n| format!("apikey:anthropic:eligible-{n:02}"))
        .collect::<Vec<_>>();
    let detail = disjoint(&offered, &eligible).await;
    assert_eq!(
        detail,
        json!({"reason":"candidates_disjoint",
        "offered_credential_ids":(0..64).map(|n| format!("apikey:offered-{n:02}")).collect::<Vec<_>>(),
        "eligible_credential_ids":(0..64).map(|n| format!("apikey:anthropic:eligible-{n:02}")).collect::<Vec<_>>(),
        "offered_credential_count":70, "eligible_credential_count":70})
    );
}
