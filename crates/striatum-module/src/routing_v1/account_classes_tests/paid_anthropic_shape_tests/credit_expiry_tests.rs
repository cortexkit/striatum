use super::*;
use crate::account_quota::{self, AccountQuotaState};
use crate::account_selection::rfc3339;
use sha2::{Digest, Sha256};

const VIEW: &str = "paid-anthropic-vault";
const ORGS: [&str; 2] = ["org-first", "org-second"];

fn credit(remaining: i64, expires_in_ms: i64) -> Value {
    json!({
        "id": format!("credit-{expires_in_ms}"), "label": "API credit (Ultra)",
        "remaining": {"minor": remaining, "exponent": 2, "unit": "USD"},
        "total": {"minor": 20000, "exponent": 2, "unit": "USD"},
        "expiresAt": rfc3339(NOW as i64 + expires_in_ms),
        "funding": "subscription", "basis": "stated",
    })
}

fn install_readings(links: [Option<&str>; 2], spend: [Option<Vec<Value>>; 2]) {
    install_readings_with_vendors(links, spend, [Some("anthropic"); 2]);
}

fn install_readings_with_vendors(
    links: [Option<&str>; 2],
    spend: [Option<Vec<Value>>; 2],
    vendors: [Option<&str>; 2],
) {
    let mut quota = AccountQuotaState::default();
    account_quota::apply_roster_listing(
        &mut quota,
        &ScopedCredentialListing {
            view: VIEW.into(),
            credentials: KEYS
                .into_iter()
                .zip(links)
                .map(|(id, org)| ListedCredential {
                    id: id.into(),
                    auth_method: Some("apikey".into()),
                    serves: vec!["anthropic".into()],
                    org_id: org.map(str::to_owned),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW as i64,
    );
    let rows: Vec<_> = ORGS
        .into_iter()
        .zip(spend.into_iter().zip(vendors))
        .filter_map(|(org, (spend, vendor))| {
            let mut row = json!({"provider": "claude-console", "account": org,
                "fetchedAt": rfc3339(NOW as i64), "spend": spend?});
            if let Some(vendor) = vendor {
                row["apiProvider"] = json!(vendor);
            }
            Some(row)
        })
        .collect();
    account_quota::apply_provider_reply(
        &mut quota,
        "claude-console",
        &serde_json::to_vec(&json!({"result":rows})).unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;
}

fn request(id: &str) -> Value {
    let mut request = credential_only_request(id);
    offer_key_account_ids(&mut request);
    request
}

#[tokio::test]
async fn soonest_positive_console_credit_wins_and_is_explained() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    install_readings(
        ORGS.map(Some),
        [
            Some(vec![credit(20000, 240_000)]),
            Some(vec![
                credit(20000, 360_000),
                credit(20000, -60_000),
                credit(0, 60_000),
                credit(19956, 120_000),
            ]),
        ],
    );
    let owner = harness.bind(reserved(OWNER), None).await;
    let broca = harness.bind(reserved("broca"), None).await;
    let selected = result(
        owner
            .call(
                "route.select",
                &json!({
                    "consumer":"codemode", "pin":format!("{POOL}/{MODEL}"), "requirements":{"iq":0}
                }),
            )
            .await,
    );
    let mut first = request("early-credit");
    first["decision_id"] = selected["decision_id"].clone();
    let admitted = result(broca.call("route.account_for", &first).await);
    assert_eq!(admitted["account"]["credential_id"], KEYS[1]);

    let expected_credit = json!({
        "expires_at_ms":NOW + 120_000,
        "remaining":{"minor":19956, "exponent":2, "unit":"USD"}
    });
    let stored = harness
        .store()
        .v1_bound_account_decision(
            selected["decision_id"].as_str().unwrap(),
            admitted["pin"]["id"].as_str().unwrap(),
            "early-credit",
        )
        .unwrap()
        .unwrap();
    let stored: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored["explanation"]["credit_expiry"], expected_credit);

    let explanation = result(
        owner
            .call(
                "route.explain",
                &json!({
                    "decision_id":selected["decision_id"]
                }),
            )
            .await,
    );
    assert_eq!(
        explanation["serving"]["credential_id"], KEYS[1],
        "{explanation}"
    );
    assert_eq!(
        explanation["credit_expiry"], expected_credit,
        "{explanation}"
    );

    let mut next = first;
    next["admission_id"] = json!("keep-credit-key");
    let retained = result(broca.call("route.account_for", &next).await);
    assert_eq!(retained["pin"]["id"], admitted["pin"]["id"]);
    let explanation = result(
        owner
            .call(
                "route.explain",
                &json!({"decision_id":selected["decision_id"]}),
            )
            .await,
    );
    assert_eq!(explanation["serving"]["credential_id"], KEYS[1]);
    assert!(
        explanation.get("credit_expiry").is_none(),
        "retaining a pin did not rank credits again: {explanation}"
    );
}

#[tokio::test]
async fn keys_without_live_console_credit_fall_back() {
    let _clock = crate::InjectedClock::set(NOW);
    for (label, link, spend) in [
        ("exhausted", Some(ORGS[0]), Some(vec![credit(0, 60_000)])),
        ("expired", Some(ORGS[0]), Some(vec![credit(20000, -1_000)])),
        ("no-reading", Some(ORGS[0]), None),
        ("unlinked", None, Some(vec![credit(20000, 60_000)])),
    ] {
        let harness = paid_anthropic_harness();
        install_readings(
            [link, Some(ORGS[1])],
            [spend, Some(vec![credit(100, 120_000)])],
        );
        let broca = harness.bind(reserved("broca"), None).await;
        let admitted = result(broca.call("route.account_for", &request(label)).await);
        assert_eq!(admitted["account"]["credential_id"], KEYS[1], "{label}");
    }
}

#[tokio::test]
async fn console_credit_requires_a_matching_vendor_as_well_as_org() {
    let _clock = crate::InjectedClock::set(NOW);
    for (label, vendor) in [
        ("different-vendor", Some("openai")),
        ("missing-vendor", None),
    ] {
        let harness = paid_anthropic_harness();
        install_readings_with_vendors(
            ORGS.map(Some),
            [
                Some(vec![credit(20000, 60_000)]),
                Some(vec![credit(20000, 120_000)]),
            ],
            [vendor, Some("anthropic")],
        );
        let broca = harness.bind(reserved("broca"), None).await;
        let admitted = result(broca.call("route.account_for", &request(label)).await);
        assert_eq!(admitted["account"]["credential_id"], KEYS[1], "{label}");
    }
}

#[tokio::test]
async fn console_credit_ties_and_unlinked_pool_keep_existing_order() {
    let _clock = crate::InjectedClock::set(NOW);
    for (label, links) in [("no-links", [None; 2]), ("tie", ORGS.map(Some))] {
        let harness = paid_anthropic_harness();
        // The second key offers much more credit; amount is not a tie-breaker.
        install_readings(
            links,
            [
                Some(vec![credit(10, 120_000)]),
                Some(vec![credit(20000, 120_000)]),
            ],
        );
        let broca = harness.bind(reserved("broca"), None).await;
        let admitted = result(broca.call("route.account_for", &request(label)).await);
        assert_eq!(admitted["account"]["credential_id"], KEYS[0], "{label}");
    }
}

#[tokio::test]
async fn console_credit_does_not_move_existing_affinity() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let broca = harness.bind(reserved("broca"), None).await;
    let first = request("initial");
    let initial = result(broca.call("route.account_for", &first).await);
    assert_eq!(initial["account"]["credential_id"], KEYS[0]);

    install_readings(
        ORGS.map(Some),
        [
            Some(vec![credit(20000, 240_000)]),
            Some(vec![credit(20000, 120_000)]),
        ],
    );
    let mut second = request("sticky");
    second["conversation_key"] = first["conversation_key"].clone();
    let sticky = result(broca.call("route.account_for", &second).await);
    assert_eq!(sticky["account"]["credential_id"], KEYS[0]);
    assert_eq!(sticky["pin"]["id"], initial["pin"]["id"]);
}

#[tokio::test]
async fn learned_vault_org_links_console_credit_without_changing_key_identity() {
    const EXPORTED: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/claustrum-list-scoped-v2/list-scoped-v2.json"
    ));
    const ORG: &str = "c57dea1d-dfac-4ad7-9dee-123456789abc";
    const LINKED_KEY: &str = "apikey:anthropic:work";

    let _clock = crate::InjectedClock::set(NOW);
    assert_eq!(
        format!("{:x}", Sha256::digest(EXPORTED)),
        "4ff7e45c2839f71873c1ca12ffe5ea3e15d35897e3610bb884a26a6f0329b691"
    );
    let vectors: Value = serde_json::from_slice(EXPORTED).unwrap();
    let reply = &vectors["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["case"] == "learned_org")
        .unwrap()["reply"];
    let listing = cortexkit_claustrum_client::decode_list_scoped_reply(reply.clone()).unwrap();
    assert!(listing.undecodable_credentials.is_empty());
    assert_eq!(listing.credentials.len(), 1);
    assert_eq!(listing.credentials[0].id, LINKED_KEY);
    assert_eq!(listing.credentials[0].account_id, None);
    assert_eq!(listing.credentials[0].org_id.as_deref(), Some(ORG));
    let harness = paid_anthropic_harness_with_listing(listing.clone());

    let mut quota = AccountQuotaState::default();
    let row = account_quota::apply_roster_listing(&mut quota, &listing, NOW as i64).unwrap();
    let persisted: Value = serde_json::from_str(&row.roster_json).unwrap();
    assert_eq!(persisted["listing"]["credentials"][0]["org_id"], ORG);
    assert_eq!(
        quota.credential_identities[LINKED_KEY].account_id,
        LINKED_KEY
    );
    account_quota::apply_provider_reply(
        &mut quota,
        "claude-console",
        &serde_json::to_vec(&json!({
            "result": [{"provider":"claude-console", "apiProvider": "anthropic", "account": ORG,
                "fetchedAt":rfc3339(NOW as i64), "spend":[credit(19956, 120_000)]}]
        }))
        .unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;

    let broca = harness.bind(reserved("broca"), None).await;
    let mut request = request("learned-console-org");
    request["candidates"] = json!({"view_digest": listing.view, "accounts": [
        {"credential_id": KEYS[0], "account_id": KEYS[0]},
        {"credential_id":LINKED_KEY, "account_id": LINKED_KEY},
    ]});
    let admitted = result(broca.call("route.account_for", &request).await);
    assert_eq!(admitted["account"]["credential_id"], LINKED_KEY);
    assert_eq!(admitted["account"]["account_id"], LINKED_KEY);
}
