use super::*;

pub(super) fn configure_harness(
    harness: Arc<Harness>,
    provider: &str,
    converted_live: bool,
    version: Option<u32>,
) -> (Arc<Harness>, BrocaCredentials) {
    let fact = account_fact(provider);
    // Historical fixtures seed only decisions before upgrading. Current fact
    // writers require the current schema, so install those facts after upgrade.
    if version.is_none() {
        harness.install_facts(&[fact.clone()]);
        catalog_fact(&harness, &fact);
    }
    let mut subscription = fact.clone();
    subscription.billing_plane.as_mut().unwrap().auth_method = "oauth".into();
    subscription.billing_plane.as_mut().unwrap().kind = "subscription".into();
    subscription
        .billing_plane
        .as_mut()
        .unwrap()
        .rates
        .insert("rate.output".into(), BillingRate::Priced { rate: 1.0 });
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "mixed-vault".into(),
            credentials: ["oauth", "apikey"]
                .into_iter()
                .map(|method| ListedCredential {
                    id: format!("{method}:test"),
                    account_id: Some(format!("{method}-account")),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec![provider.into()],
                    serves: vec![provider.into()],
                    credential_type: method.into(),
                    auth_method: Some(method.into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact, subscription]),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    super::super::conformance_tests::publish_fixture_providers(
        &mut credentials,
        RUNNER,
        &[provider],
        true,
    );
    // A legacy settings document chooses Broca rather than the fixture runner.
    // Both offer the same test provider so settings conversion doesn't change
    // which account-policy case the test exercises.
    super::super::conformance_tests::publish_fixture_providers(
        &mut credentials,
        "broca",
        &[provider],
        true,
    );
    harness.set_credentials(credentials.clone());
    harness.edit_config(|config| {
        if converted_live {
            *config = crate::test_config::convert_archived(include_str!(
                "../../../fixtures/routing-config-live-2026-10-24.jsonc"
            ));
        }
        config["runners"] = json!([{"module":RUNNER,"via_vault":true}]);
        config["consumers"] = json!({
            "mason":{"owner":OWNER,"mode":"demand"},
            "codemode":{"owner":OWNER,"mode":"demand","pay_per_token":true},
            "flow":{"owner":OWNER,"mode":"demand","pay_per_token":true}
        });
    });
    (harness, credentials)
}
