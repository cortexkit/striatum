use std::sync::{Arc, Mutex};

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use serde_json::json;
use striatum_core::{
    billing::{BillingPlane, BillingRate},
    ModelFact,
};

use super::conformance_tests::{reserved, Harness};
use crate::{
    account_selection::class_context::resolve_class,
    broca_credentials::{credential_serves_fact, BrocaCredentials},
};

const NOW: u64 = 1_800_000_000_000;
const API: &str = "gemini-4-carbon";
const ALIAS: &str = "antigravity-gemini-4-carbon";
const RUNNER: &str = "wire-pair-runner";
const OWNER: &str = "wire-pair-owner";

fn setup() -> (Arc<Harness>, BrocaCredentials, Vec<ModelFact>) {
    let harness = Harness::new("wire-pairs");
    let facts: Vec<_> = [(API, "apikey", 9.0), (ALIAS, "antigravity", 9.0)]
        .into_iter()
        .map(|(model, method, rate)| {
            let mut fact = crate::tests::fact("google", model);
            // Upstream rows do not publish an auth allowlist.
            fact.auth_methods = None;
            fact.cost_per_m_tok_out = Some(rate);
            fact.billing_plane = (method == "apikey").then(|| BillingPlane {
                provider_id: "google".into(),
                auth_method: method.into(),
                kind: "upstream".into(),
                selection_reason: "fixture plane".into(),
                rates: [("rate.output".into(), BillingRate::Priced { rate })].into(),
            });
            fact
        })
        .collect();
    harness.install_facts(&facts);
    let catalog = json!({"catalog_version":1,"resolved_at_ms":NOW,"models":{
        format!("google/{API}"): {"existence":"present","capability.tool_call":true},
        format!("google/{ALIAS}"): {"existence":"present","capability.tool_call":true,
            "rate.output":{"state":"priced","currency":"USD","units":9,"exponent":0,
                "inherited_from":{"basis":"alias","provider_id":"google","model_id":API}}}
    }});
    let snapshot =
        crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&catalog).unwrap())
            .unwrap();
    let (updates, cursor) = snapshot.parts();
    harness
        .store()
        .apply_fusiform_catalog(updates, cursor)
        .unwrap();
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "mixed-google".into(),
            credentials: [
                ("antigravity:google", "antigravity", "oauth"),
                ("apikey:google", "apikey", "apikey"),
            ]
            .into_iter()
            .map(|(id, method, kind)| ListedCredential {
                id: id.into(),
                account_id: Some(id.into()),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["google".into()],
                serves: vec!["google".into()],
                credential_type: kind.into(),
                auth_method: Some(method.into()),
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        },
        NOW,
    );
    credentials.wire_apis = [
        (format!("google/{API}"), "google.generate_content".into()),
        (format!("google/{ALIAS}"), "google.code_assist".into()),
    ]
    .into();
    credentials.wire_api_models = credentials.wire_apis.keys().cloned().collect();
    credentials.runner_declarations.insert(
        RUNNER.into(),
        super::runners::parse_declaration(&json!({
            "runner":{"pairs":[
                {"wire_api":"google.generate_content","auth_method":"apikey"},
                {"wire_api":"google.code_assist","auth_method":"antigravity"}
            ],"follows_account_choice":true}
        }))
        .unwrap()
        .unwrap(),
    );
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("../../fixtures/served-payloads.json")).unwrap();
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts[..1])
            .with_wire_apis(&credentials.wire_apis)
            .with_test_catalog(&serde_json::to_vec(&catalog).unwrap())
            .with_test_refusal(
                "google",
                "antigravity",
                &serde_json::to_vec(&fixtures["refused — undeclared google antigravity"]).unwrap(),
            ),
    );
    credentials.catalog_snapshot = Some(snapshot);
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    harness.set_credentials(credentials.clone());
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":RUNNER,"via_vault":true}]);
        config["consumers"] = json!({"athena":{"owner":OWNER,"mode":"demand"}});
    });
    (harness, credentials, facts)
}

#[test]
fn api_class_excludes_antigravity_but_alias_keeps_its_subscription() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_, credentials, facts) = setup();
    let catalog = facts.iter().collect();
    for (fact, expected, subscription) in [
        (&facts[0], "apikey:google", false),
        (&facts[1], "antigravity:google", true),
    ] {
        let class = resolve_class(
            credentials.listed_credentials(),
            fact,
            &catalog,
            &credentials,
            &[],
            false,
            false,
            false,
            |_| true,
        );
        assert_eq!(class.subscription, subscription);
        assert_eq!(
            class
                .rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            [expected]
        );
        assert_eq!(
            credentials.serving_identities(fact),
            [(expected.into(), Some(expected.into()))]
        );
    }
}

#[test]
fn athena_prices_api_row_on_the_apikey_plane() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials, mut facts) = setup();
    let config = harness.operator_config();
    let targets = Default::default();
    let sources = crate::SelectionPolicySources {
        live_config: &config,
        operator_config: &config,
        operator_config_path: std::path::Path::new("unused"),
        catalog_targets: &targets,
    };
    let priced = crate::price_selection_inputs(
        &mut facts,
        "{}".into(),
        &sources,
        harness.store(),
        &Arc::new(Mutex::new(credentials)),
        true,
        false,
    )
    .unwrap();
    let api = facts.iter().find(|fact| fact.model_id == API).unwrap();
    let class = priced.classes.as_ref().unwrap().class(api, false);
    assert!(!class.subscription);
    assert_eq!(class.rows[0].id, "apikey:google");
    let plane = api
        .billing_plane
        .as_ref()
        .expect("API pricing must not request the Antigravity plane");
    assert_eq!(plane.auth_method, "apikey");
    assert_eq!(
        plane.rates["rate.output"],
        BillingRate::Priced { rate: 9.0 }
    );
    let alias = facts.iter().find(|fact| fact.model_id == ALIAS).unwrap();
    let plane = alias.billing_plane.as_ref().unwrap();
    assert_eq!(plane.auth_method, "antigravity");
    assert_eq!(plane.kind, "no_coverage fallback");
    assert_eq!(plane.rate("rate.output"), Some(9.0));
}

#[tokio::test]
async fn account_for_admits_api_key_and_antigravity_only_on_their_wires() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _, _) = setup();
    let runner = harness.bind(reserved(RUNNER), None).await;
    for (model, expected) in [(API, "apikey:google"), (ALIAS, "antigravity:google")] {
        let reply = runner.call("route.account_for", &json!({
            "provider":"google", "model":model, "conversation_key":model, "admission_id":model,
            "candidates":{"view_digest":"fixture", "accounts":[
                {"account_id":"antigravity:google","credential_id":"antigravity:google"},
                {"account_id":"apikey:google","credential_id":"apikey:google"}
            ]}
        })).await;
        assert_eq!(reply.0, subc_protocol::FrameType::Response, "{}", reply.1);
        assert_eq!(
            reply.1["result"]["account"]["credential_id"], expected,
            "{}",
            reply.1
        );
    }
}

#[tokio::test]
async fn athena_can_select_api_and_antigravity_rows() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _, _) = setup();
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    for (model, expected) in [(API, "apikey:google"), (ALIAS, "antigravity:google")] {
        let selected = caller.call("route.select", &json!({
            "consumer":"athena", "requirements":{"iq":0,"eq":0}, "pin":format!("google/{model}")
        })).await;
        assert_eq!(
            selected.0,
            subc_protocol::FrameType::Response,
            "{}",
            selected.1
        );
        let reply = runner.call("route.account_for", &json!({
            "provider":"google", "model":model, "conversation_key":model, "admission_id":model,
            "decision_id":selected.1["result"]["decision_id"]
        })).await;
        assert_eq!(reply.0, subc_protocol::FrameType::Response, "{}", reply.1);
        assert_eq!(
            reply.1["result"]["account"]["credential_id"], expected,
            "{}",
            reply.1
        );
    }
}

#[test]
fn single_method_and_discovery_gaps_keep_existing_reachability() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_, mut credentials, facts) = setup();
    let catalog = facts.iter().collect();
    let key = credentials.listed_credentials()[1].clone();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "key-only".into(),
            credentials: vec![key.clone()],
            ..Default::default()
        },
        NOW,
    );
    assert!(credential_serves_fact(
        &key,
        &facts[0],
        &catalog,
        &credentials
    ));
    let login = ListedCredential {
        auth_method: Some("antigravity".into()),
        credential_type: "oauth".into(),
        ..key.clone()
    };
    assert!(!credential_serves_fact(
        &login,
        &facts[0],
        &catalog,
        &credentials
    ));
    credentials.runner_declarations.clear();
    assert!(credential_serves_fact(
        &login,
        &facts[0],
        &catalog,
        &credentials
    ));
    let (_, mut credentials, _) = setup();
    credentials.wire_apis.clear();
    credentials.publish_billing_catalog(Default::default());
    assert!(credential_serves_fact(
        &login,
        &facts[0],
        &catalog,
        &credentials
    ));
    let mut restricted = facts[0].clone();
    restricted.auth_methods = Some(vec!["apikey".into()]);
    assert!(!credential_serves_fact(
        &login,
        &restricted,
        &catalog,
        &credentials
    ));
}

#[test]
fn borrowed_rows_use_catalog_wire_family_without_changing_provider_membership() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_, mut credentials, facts) = setup();
    let rows = [
        ListedCredential {
            id: "company-key".into(),
            provider_ids: vec!["company-google".into()],
            categories: vec!["llm-gateway".into()],
            auth_method: Some("apikey".into()),
            ..credentials.listed_credentials()[1].clone()
        },
        ListedCredential {
            id: "company-login".into(),
            provider_ids: vec!["company-google".into()],
            ..credentials.listed_credentials()[0].clone()
        },
    ];
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "company-google".into(),
            credentials: rows.to_vec(),
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    let mut borrowed = facts[0].clone();
    borrowed.provider_id = "company-google".into();
    let catalog = facts.iter().collect();
    assert!(credential_serves_fact(
        &rows[0],
        &borrowed,
        &catalog,
        &credentials
    ));
    assert!(!credential_serves_fact(
        &rows[1],
        &borrowed,
        &catalog,
        &credentials
    ));
    assert!(!credential_serves_fact(
        &rows[0],
        &facts[0],
        &catalog,
        &credentials
    ));
}

#[test]
fn matching_pair_does_not_bypass_the_selected_runners_endpoint_requirement() {
    let _clock = crate::InjectedClock::set(NOW);
    let (_, mut credentials, facts) = setup();
    let catalog = facts.iter().collect();
    credentials.wire_api_models.clear();
    credentials.runner_declarations.insert(
        RUNNER.into(),
        super::runners::parse_declaration(&json!({
            "runner":{"pairs":[{"wire_api":"google.generate_content","auth_method":"apikey"}],
                "follows_account_choice":true,"requires_wire_api":true}
        }))
        .unwrap()
        .unwrap(),
    );
    assert!(credential_serves_fact(
        &credentials.listed_credentials()[1],
        &facts[0],
        &catalog,
        &credentials
    ));
    let registry = super::runners::Registry::build(
        vec![crate::config_policy::RunnerPolicy {
            harness: None,
            module: RUNNER.into(),
            via_vault: true,
            routes: None,
            tool_capabilities: vec![],
        }],
        &credentials,
        &facts,
        NOW,
    );
    assert!(!registry.runs(RUNNER, &format!("google/{API}"), &format!("google/{API}")));
}
