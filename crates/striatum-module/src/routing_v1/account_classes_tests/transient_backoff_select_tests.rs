use super::*;
mod tests;

const ACCOUNT: &str = "only-chatgpt-account";
const CREDENTIAL: &str = "oauth:only-chatgpt-login";
const MODELS: [&str; 3] = ["gpt-6-luna", "gpt-6.1-sol", "gpt-6.1-terra"];

fn one_account() -> Arc<Harness> {
    account_with_models(&MODELS)
}

fn one_probe_account() -> Arc<Harness> {
    account_with_models(&MODELS[..1])
}

fn account_with_models(models: &[&str]) -> Arc<Harness> {
    let harness = Harness::new("transient-backoff-select");
    let mut credentials = BrocaCredentials::default();
    // Only these OpenAI routes adequate for this demand exist here, so a
    // refusal can come only from the account test held on them.
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute("DELETE FROM model_facts", [])
        .unwrap();
    let facts: Vec<_> = models
        .iter()
        .map(|model| {
            let mut fact = crate::tests::fact("openai", model);
            fact.auth_methods = Some(vec!["oauth".into()]);
            fact.billing_plane = Some(BillingPlane {
                provider_id: "openai".into(),
                auth_method: "oauth".into(),
                kind: "subscription".into(),
                selection_reason: "single subscription account fixture".into(),
                rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
                    .into_iter()
                    .collect(),
            });
            fact
        })
        .collect();
    harness.install_facts(&facts);
    for fact in &facts {
        catalog_fact(&harness, fact);
    }
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "one-account".into(),
            credentials: vec![ListedCredential {
                id: CREDENTIAL.into(),
                account_id: Some(ACCOUNT.into()),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["openai".into()],
                serves: vec!["openai".into()],
                credential_type: "oauth".into(),
                auth_method: Some("oauth".into()),
                ..Default::default()
            }],
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    publish_fixture_providers(&mut credentials, "broca", &["openai"], true);
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca", "via_vault":true}]);
        config["consumers"] = json!({
            "prefrontal.mason":{"owner":"prefrontal", "mode":"demand"},
            "prefrontal.sidekick.map":{"owner":"prefrontal", "mode":"demand"}
        });
    });
    harness
}

fn admission(id: &str) -> Value {
    json!({"provider":"openai", "model":MODELS[0], "admission_id":id,
        "conversation_key":id, "candidates":{"view_digest":"one-account",
        "accounts":[{"account_id":ACCOUNT, "credential_id":CREDENTIAL}]}})
}
