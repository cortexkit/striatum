use super::*;
use crate::account_quota::{AccountQuotaState, RosterIdentity, StoredReading, VaultRoster};
use crate::fusiform_catalog::PlanConcurrencyCatalog;
use serde_json::{json, Value};

const T: i64 = 1_790_683_200_000;

fn prices(scope: &str, limit: Value) -> Value {
    json!({"prices":[{"provider_id":"google","tier":"ultra","concurrency":{
        "scope":scope,"limit":limit,"review_by_ms":T+1,
        "per_model":{"google/m1":1}
    }}]})
}

fn roster(tier: Option<&str>) -> AccountQuotaState {
    let mut state = AccountQuotaState::default();
    state.roster = Some(VaultRoster {
        view: "view".into(),
        fetched_at_ms: T,
        grants: vec![],
        identities: vec![RosterIdentity {
            refresh_adapter: "antigravity".into(),
            account_id: "a".into(),
            credential_ids: vec!["cred".into()],
            active_credential_ids: vec!["cred".into()],
            first_seen_ms: T,
            plan_tier: tier.map(str::to_owned),
        }],
    });
    let reading = StoredReading {
        provider: "antigravity".into(),
        account_id: "a".into(),
        entry: json!({"apiProvider":"google"}),
        received_at_ms: T,
        tombstoned_at_ms: None,
    };
    state.readings.readings.insert(
        (reading.provider.clone(), reading.account_id.clone()),
        reading,
    );
    state
}

pub(crate) fn inputs(scope: &str, settings: Value) -> CapInputs {
    let mut input = CapInputs::default();
    input.publish_catalog(&prices(scope, json!(3)), T);
    input.publish_roster(&roster(Some("ultra")));
    input.publish_settings(&serde_json::from_value(settings).unwrap());
    input
}

#[test]
fn cap_catalog_validation_and_duplicate_rows_are_uncapped_once() {
    for invalid in [json!(0), json!(-1), json!(1.5), json!("6")] {
        let catalog = PlanConcurrencyCatalog::load(&prices("account", invalid), T);
        assert_eq!(catalog.cap("google", "ultra"), None);
        assert_eq!(catalog.warnings.len(), 1);
    }
    let catalog = PlanConcurrencyCatalog::load(&prices("unknown", json!(3)), T);
    assert_eq!(catalog.cap("google", "ultra"), None);
    assert_eq!(catalog.warnings.len(), 1);
    let mut duplicate = prices("account", json!(3));
    let row = duplicate["prices"][0].clone();
    duplicate["prices"].as_array_mut().unwrap().push(row);
    let catalog = PlanConcurrencyCatalog::load(&duplicate, T);
    assert_eq!(catalog.cap("google", "ultra"), None);
    assert_eq!(catalog.warnings.len(), 1);
    let mut input = CapInputs::default();
    input.publish_catalog(&duplicate, T);
    input.publish_catalog(&duplicate, T + 1);
    assert_eq!(
        input.warned.len(),
        1,
        "repeated polls must not repeat the plan warning"
    );
}

#[test]
fn cap_review_deadline_warns_once_without_disabling_the_cap() {
    let mut input = inputs("account_model", json!({}));
    input.publish_catalog(&prices("account_model", json!(3)), T + 2);
    input.publish_catalog(&prices("account_model", json!(3)), T + 3);
    assert_eq!(input.warned.len(), 1);
    assert!(input
        .warned
        .iter()
        .next()
        .unwrap()
        .contains("due for review"));
    assert_eq!(
        input.resolved.cap("google", "a", "m1"),
        EffectiveCap {
            limit: Some(3),
            scope: CapScope::AccountModel
        }
    );
}

#[test]
fn cap_roster_plan_join_and_missing_catalog_cases_keep_settings() {
    let mut input = inputs("account_model", json!({}));
    let state = roster(Some("ultra"));
    let reading = state.readings.readings.values().next().unwrap();
    let identity =
        crate::account_quota::identity_for_reading(state.roster.as_ref().unwrap(), reading);
    assert_eq!(
        crate::subscription_economics::plan_price_key(reading, identity),
        Some(("google", Some("ultra")))
    );
    for model in ["m1", "m2", "not-in-catalog"] {
        assert_eq!(input.resolved.cap("google", "a", model).limit, Some(3));
    }
    for missing in [
        json!({"prices":[]}),
        json!({"prices":[{"provider_id":"google","tier":"ultra"}]}),
        prices("account", json!(0)),
    ] {
        input.publish_catalog(&missing, T);
        assert_eq!(
            input.resolved.cap("google", "a", "m1"),
            EffectiveCap::default()
        );
        for (settings, model, limit, scope) in [
            (
                json!({"providers":{"google":{"limit":5}}}),
                "m1",
                5,
                CapScope::Account,
            ),
            (
                json!({"models":{"google/m1":{"limit":2}}}),
                "m1",
                2,
                CapScope::AccountModel,
            ),
            (
                json!({"accounts":{"google/a":{"limit":6}}}),
                "m1",
                6,
                CapScope::Account,
            ),
        ] {
            input.publish_settings(&serde_json::from_value(settings).unwrap());
            assert_eq!(
                input.resolved.cap("google", "a", model),
                EffectiveCap {
                    limit: Some(limit),
                    scope
                }
            );
        }
        input.publish_settings(&CapSettings::default());
    }
    input.publish_catalog(&prices("account_model", json!(3)), T);
    for tier in [None, Some("missing")] {
        input.publish_roster(&roster(tier));
        assert_eq!(
            input.resolved.cap("google", "a", "m1"),
            EffectiveCap::default()
        );
        for (kind, key, expected_scope) in [
            ("providers", "google", CapScope::Account),
            ("models", "google/m1", CapScope::AccountModel),
            ("accounts", "google/a", CapScope::Account),
        ] {
            let mut settings = json!({});
            settings[kind][key] = json!({"limit":5});
            input.publish_settings(&serde_json::from_value(settings).unwrap());
            assert_eq!(
                input.resolved.cap("google", "a", "m1"),
                EffectiveCap {
                    limit: Some(5),
                    scope: expected_scope
                }
            );
        }
        input.publish_settings(&CapSettings::default());
    }
    input.publish_settings(
        &serde_json::from_value(json!({"providers":{"google":{"limit":5}}})).unwrap(),
    );
    assert_eq!(input.resolved.cap("google", "a", "m1").limit, Some(5));
}

#[test]
fn cap_override_precedence_is_whole_for_every_pair_and_scope() {
    let entries = [
        ("providers", "google", 1),
        ("plans", "google/ultra", 4),
        ("models", "google/m1", 2),
        ("accounts", "google/a", 6),
    ];
    for (scope_name, scope) in [
        ("account", CapScope::Account),
        ("account_model", CapScope::AccountModel),
    ] {
        for low in 0..entries.len() {
            for high in low + 1..entries.len() {
                let mut settings = json!({});
                for index in [low, high] {
                    let (kind, key, limit) = entries[index];
                    settings[kind][key] = json!({"limit":limit});
                    // A losing setting's explicit scope must not be inherited.
                    if index == low && kind != "models" {
                        settings[kind][key]["scope"] = json!(if scope_name == "account" {
                            "account_model"
                        } else {
                            "account"
                        });
                    }
                }
                let input = inputs(scope_name, settings);
                assert_eq!(
                    input.resolved.cap("google", "a", "m1"),
                    EffectiveCap {
                        limit: Some(entries[high].2),
                        scope: if high == 2 {
                            CapScope::AccountModel
                        } else {
                            scope
                        },
                    },
                    "overlap {low}/{high} under {scope_name}"
                );
            }
        }
        for kind in ["providers", "plans", "accounts"] {
            let key = match kind {
                "providers" => "google",
                "plans" => "google/ultra",
                _ => "google/a",
            };
            let mut settings = json!({});
            settings[kind][key] = json!({"limit":"none"});
            assert_eq!(
                inputs(scope_name, settings)
                    .resolved
                    .cap("google", "a", "m1"),
                EffectiveCap { limit: None, scope }
            );
        }
    }
}

#[test]
fn cap_scope_defaults_and_input_publication_replace_snapshots() {
    let mut input = inputs(
        "account_model",
        json!({"plans":{"google/ultra":{"limit":4}}}),
    );
    let old = input.resolved.clone();
    for model in ["m1", "m2"] {
        assert_eq!(
            old.cap("google", "a", model),
            EffectiveCap {
                limit: Some(4),
                scope: CapScope::AccountModel
            }
        );
    }
    input.publish_settings(
        &serde_json::from_value(json!({"accounts":{"google/a":{"limit":6,"scope":"account"}}}))
            .unwrap(),
    );
    assert_eq!(
        input.resolved.cap("google", "a", "m1"),
        EffectiveCap {
            limit: Some(6),
            scope: CapScope::Account
        }
    );
    assert_eq!(old.cap("google", "a", "m1").limit, Some(4));
    input.publish_settings(&CapSettings::default());
    input.publish_catalog(&prices("account", json!(7)), T);
    assert_eq!(
        input.resolved.cap("google", "a", "m1"),
        EffectiveCap {
            limit: Some(7),
            scope: CapScope::Account
        }
    );
    input.publish_catalog(&json!({"prices":[]}), T);
    input.publish_settings(
        &serde_json::from_value(json!({"plans":{"google/ultra":{"limit":4}}})).unwrap(),
    );
    assert_eq!(
        input.resolved.cap("google", "a", "m1"),
        EffectiveCap {
            limit: Some(4),
            scope: CapScope::Account
        }
    );
}

#[test]
fn cap_settings_load_rejects_zero_and_model_scope() {
    let validate = |caps: Value| {
        crate::config_overrides::validate_document(&json!({"concurrency":caps}).to_string())
    };
    validate(json!({"models":{"anthropic-platform/m1":{"limit":"none"}}})).unwrap();
    for (kind, key) in [
        ("providers", "google"),
        ("plans", "google/ultra"),
        ("models", "google/m1"),
        ("accounts", "google/a"),
    ] {
        let mut settings = json!({});
        settings[kind][key] = json!({"limit":0});
        let error = validate(settings).unwrap_err();
        assert!(error.contains("exclusion rules"), "{error}");
    }
    assert!(
        validate(json!({"models":{"google/m1":{"limit":2,"scope":"account_model"}}}))
            .unwrap_err()
            .contains("scope")
    );
    for limit in [json!(-1), json!(2.5), json!("unlimited")] {
        assert!(validate(json!({"providers":{"google":{"limit":limit}}})).is_err());
    }
}

#[test]
fn cap_paid_provider_keys_and_admission_normalization_are_exact() {
    let input = inputs(
        "account",
        json!({"models":{
            "anthropic-platform/m1":{"limit":2}, "anthropic/m2":{"limit":1}
        }}),
    );
    assert_eq!(
        input.resolved.cap("anthropic-platform", "key", " M1 "),
        EffectiveCap {
            limit: Some(2),
            scope: CapScope::AccountModel
        }
    );
    for model in ["m2", "anthropic/m1", "m1[variant]", "m1/pool"] {
        assert_eq!(
            input.resolved.cap("anthropic-platform", "key", model),
            EffectiveCap::default()
        );
    }
}

#[test]
fn cap_credential_publications_keep_snapshot_policy() {
    let _clock = crate::InjectedClock::set(T as u64);
    let mut credentials = crate::BrocaCredentials::default();
    credentials.publish_plan_prices(&prices("account_model", json!(3)));
    credentials.refresh_serving_methods(&roster(Some("ultra")), &[], T, &[]);
    let old = credentials.snapshot();
    assert_eq!(
        old.concurrency_caps.resolved.cap("google", "a", "m1").limit,
        Some(3)
    );
    credentials.publish_plan_prices(&prices("account", json!(7)));
    assert_eq!(
        credentials
            .concurrency_caps
            .resolved
            .cap("google", "a", "m1"),
        EffectiveCap {
            limit: Some(7),
            scope: CapScope::Account
        }
    );
    credentials.refresh_serving_methods(&roster(None), &[], T, &[]);
    assert_eq!(
        credentials
            .concurrency_caps
            .resolved
            .cap("google", "a", "m1"),
        EffectiveCap::default()
    );
    assert_eq!(
        old.concurrency_caps.resolved.cap("google", "a", "m1"),
        EffectiveCap {
            limit: Some(3),
            scope: CapScope::AccountModel
        }
    );
}
