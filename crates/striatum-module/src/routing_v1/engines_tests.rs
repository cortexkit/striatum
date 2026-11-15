//! Media selection exercises daemon ownership, ledger and metadata-only accounts.

use super::conformance_tests::{reserved, Harness, Route};
use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
use serde_json::{json, Value};
use std::sync::Arc;
use subc_protocol::FrameType;

#[path = "engine_measurements_tests.rs"]
mod measurements;

#[path = "engine_credentials_tests.rs"]
mod credentials;

#[path = "select_media_tests.rs"]
mod media;

fn engine(model: &str, local: bool) -> Value {
    json!({"kind":"transcribe","provider":"media-vendor","model":model,"local":local,
        "formats":["audio/wav"],"languages":["en"],"latency_class":"interactive"})
}

fn setup(engines: Vec<Value>) -> Arc<Harness> {
    let harness = Harness::new("engines");
    harness.edit_config(|config| {
        config["consumers"] = json!({"colliculus":{"owner":"colliculus","pay_per_token":true,"context_tokens":999999}});
    });
    super::consumers::replace_catalog(&json!([
        {"module_id":"colliculus","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":engines}}},
        {"module_id":"callosum","nonce_verified":true,"role_declarations":{"routing/v1":{"consumers":[{"name":"phone.media"}]}}}
    ]));
    harness.set_credentials(media_credentials(vec![key("opaque-media")]));
    harness
}

fn request() -> Value {
    json!({"consumer":"colliculus","kind":"transcribe","requirements":{},"privacy":"remote_allowed"})
}

#[tokio::test]
async fn media_account_continue_is_a_permanent_invalid_mode() {
    let harness = setup(vec![engine("remote", false)]);
    let route = harness.bind(reserved("colliculus"), None).await;
    let refused = route
        .call("route.account_for", &json!({"mode":"continue"}))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    assert_eq!(refused.1["detail"], json!({"field":"mode"}));
    assert_eq!(
        cortexkit_role_routing::refusals::retry_class("invalid_request", &refused.1["detail"]),
        cortexkit_role_routing::refusals::RetryClass::Permanent,
    );
}

#[tokio::test]
async fn decommissioned_media_engine_is_skipped_even_when_it_is_local() {
    for local in [false, true] {
        let harness = setup(vec![engine("retired", local)]);
        let route = harness.bind(reserved("colliculus"), None).await;
        assert_eq!(
            route.call("route.select_engine", &request()).await.0,
            FrameType::Response
        );
        let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
            "catalog_version":94,"resolved_at_ms":123,"models":{
                "media-vendor/retired":{"existence":"present","quality.decommissioned":true,"model.kind":"transcribe"}
            }
        })).unwrap()).unwrap();
        crate::fusiform_catalog::apply_mapped_catalog(
            &harness.store(),
            &snapshot,
            None,
            Some("media-vendor"),
        )
        .unwrap();
        let refused = route.call("route.select_engine", &request()).await;
        assert_eq!(refused.0, FrameType::Error, "{refused:?}");
        assert_eq!(refused.1["code"], "no_engine");
    }
}

async fn selected(route: &Route, request: &Value) -> Value {
    let (kind, value) = route.call("route.select_engine", request).await;
    assert_eq!(kind, FrameType::Response, "{value}");
    value["result"].clone()
}

#[tokio::test]
async fn non_chat_catalog_classification_does_not_exclude_media_engines() {
    let harness = setup(vec![engine("transcriber", false)]);
    let mut fact = crate::tests::fact("media-vendor", "transcriber");
    fact.model_type = Some("transcription".into());
    fact.model_kind = Some("extraction".into());
    harness.install_facts(&[fact]);
    harness.set_credentials(media_credentials(vec![key("opaque-media")]));
    let route = harness.bind(reserved("colliculus"), None).await;
    let answer = selected(&route, &request()).await;
    assert_eq!(answer["engine"]["model"], "transcriber");
}

#[tokio::test]
async fn daemon_catalog_round_trip_selects_both_colliculus_transcription_engines() {
    use serde::de::DeserializeOwned;
    use std::future::Future;
    use subc_client_rs::SubcConsumer;

    // Infer the SDK reply type used by refresh_loop, including its module schema.
    fn decode<T: DeserializeOwned, E, Fut: Future<Output = Result<T, E>>>(
        _catalog_list: impl FnOnce(SubcConsumer) -> Fut,
        wire: Value,
    ) -> T {
        serde_json::from_value(wire).unwrap()
    }
    let snapshot: Value = serde_json::from_str(include_str!(
        "../../fixtures/2026-11-10-daemon-catalog.json"
    ))
    .unwrap();
    assert_eq!(snapshot["op"], "catalog.list");
    let catalog = decode(
        |consumer| async move { consumer.catalog_list().await },
        snapshot.clone(),
    );
    assert_eq!(catalog.generation, 1);
    assert!(catalog.subc_ops.is_empty());
    let modules = serde_json::to_value(catalog.modules).unwrap();
    let media_slot = |modules: &Value| {
        modules
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["module_id"] == "colliculus")
            .unwrap()["role_declarations"]["media-provider/v1"]
            .clone()
    };
    assert_eq!(modules.as_array().unwrap().len(), 1);
    assert_eq!(media_slot(&modules), media_slot(&snapshot["modules"]));

    let harness = Harness::new("daemon-catalog-engines");
    harness.edit_config(|config| {
        config["consumers"] = json!({"media.smoke":{"pay_per_token":true}});
    });
    super::consumers::replace_catalog(&modules);
    let mut credential = key("opaque-transcription");
    credential.provider_ids = vec!["openai".into(), "google".into()];
    harness.set_credentials(media_credentials(vec![credential]));
    let route = harness.bind(reserved("smoke"), None).await;
    let request = json!({"consumer":"media.smoke","kind":"transcribe",
        "privacy":"remote_allowed","requirements":{}});
    let answer = selected(&route, &request).await;
    assert_eq!(answer["engine"]["module"], "colliculus");
    let declarations = super::consumers::engine_declarations();
    let identities: Vec<_> = declarations["colliculus"]
        .engines
        .iter()
        .map(|engine| {
            (
                engine.provider.as_str(),
                engine.model.as_str(),
                engine.local,
            )
        })
        .collect();
    assert_eq!(
        identities,
        vec![
            ("openai", "gpt-transcribe", false),
            ("google", "gemini-3.8-flash", false),
        ]
    );
    for (requirements, provider, model) in [
        (json!({"format":"audio/mp4"}), "openai", "gpt-transcribe"),
        (json!({"duration_ms":1500001}), "google", "gemini-3.8-flash"),
    ] {
        let mut params = request.clone();
        params["requirements"] = requirements;
        let answer = selected(&route, &params).await;
        assert_eq!(answer["engine"]["module"], "colliculus");
        assert_eq!(answer["engine"]["provider"], provider);
        assert_eq!(answer["engine"]["model"], model);
    }
}

#[test]
fn catalog_engines_ignore_slot_extensions_but_report_each_invalid_engine_issue_once() {
    let mut unknown = engine("unknown-field", false);
    unknown["unexpected"] = json!(true);
    let mut malformed = engine("malformed-local", false);
    malformed["local"] = json!("false");
    let slots = [
        json!({"engines":[engine("valid", false), unknown], "groups":["jobs"]}),
        json!({"engines":[malformed], "jobs_survive_restart":true}),
        json!({"groups":["jobs"]}),
        json!({"engines":{}}),
        json!([]),
        json!({"engines":[engine("valid", false)], "groups":["jobs"], "unknown":true}),
    ];
    let modules: Vec<_> = slots
        .into_iter()
        .enumerate()
        .map(|(index, slot)| {
            json!({"module_id":format!("invalid-projection-{index}"), "nonce_verified":true,
            "role_declarations":{"media-provider/v1":slot}})
        })
        .collect();
    let modules = json!(modules);
    crate::take_operator_warnings();
    super::consumers::replace_catalog(&modules);
    let declarations = super::consumers::engine_declarations();
    assert_eq!(declarations.len(), 1);
    assert_eq!(declarations["invalid-projection-5"].engines.len(), 1);
    let warnings = crate::take_operator_warnings();
    assert_eq!(warnings.len(), 5, "{warnings:?}");
    for index in 0..5 {
        assert!(
            warnings.iter().any(|line| line.contains(&format!(
                "Module invalid-projection-{index} declares media engines routing can't read"
            ))),
            "{warnings:?}"
        );
    }
    assert!(
        warnings[0].contains("unknown_member at engines[1].unexpected"),
        "{warnings:?}"
    );
    super::consumers::replace_catalog(&modules);
    assert!(crate::take_operator_warnings().is_empty());
    let mut changed = modules;
    changed[0]["role_declarations"]["media-provider/v1"]["engines"] = json!({});
    super::consumers::replace_catalog(&changed);
    let warnings = crate::take_operator_warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("Module invalid-projection-0"));
    assert!(warnings[0].contains("invalid_params at engines"));
}

#[tokio::test]
async fn engine_privacy_and_ordered_filters_name_the_emptying_filter() {
    let harness = setup(vec![engine("remote", false)]);
    let route = harness.bind(reserved("colliculus"), None).await;
    for (patch, filter) in [
        (json!({"kind":"image"}), "kind"),
        (
            json!({"requirements":{"language":"fr"},"privacy":"local_only"}),
            "requirements",
        ),
        (json!({"privacy":"local_only"}), "privacy"),
    ] {
        let mut params = request();
        for (key, value) in patch.as_object().unwrap() {
            params[key] = value.clone();
        }
        let (kind, value) = route.call("route.select_engine", &params).await;
        assert_eq!(kind, FrameType::Error, "{value}");
        assert_eq!(value["code"], "no_engine", "{value}");
        assert_eq!(value["detail"]["filter"], filter, "{value}");
        assert!(value["detail"].get("retry_at_ms").is_none());
    }
    let mut local = request();
    local["privacy"] = json!("local_only");
    local["caller_local"] = json!([engine("on-phone", true)]);
    let answer = selected(&route, &local).await;
    assert_eq!(answer["engine"]["model"], "on-phone");
    assert_eq!(answer["engine"]["index"], 0);
    assert!(answer["engine"].get("module").is_none());
}

fn facts(harness: &Harness, measurements: &[(&str, &str, Option<f64>, Option<f64>)]) {
    let mut credentials = media_credentials(vec![key("opaque-media")]);
    credentials.engine_test_facts = measurements
        .iter()
        .map(|(model, kind, accuracy, cost)| {
            (
                (format!("media-vendor/{model}"), (*kind).into()),
                (*accuracy, *cost),
            )
        })
        .collect();
    harness.set_credentials(credentials);
}

#[tokio::test]
async fn engine_ranking_is_deterministic_with_kind_specific_fixture_facts() {
    let harness = setup(vec![
        engine("z", false),
        engine("b", false),
        engine("a", false),
    ]);
    let route = harness.bind(reserved("colliculus"), None).await;
    assert_eq!(selected(&route, &request()).await["engine"]["model"], "a");
    facts(
        &harness,
        &[
            ("b", "transcribe", Some(90.0), Some(4.0)),
            ("z", "transcribe", Some(90.0), Some(2.0)),
        ],
    );
    assert_eq!(selected(&route, &request()).await["engine"]["model"], "z");
    facts(
        &harness,
        &[
            ("b", "image", Some(1000.0), Some(0.0)),
            ("b", "transcribe", None, Some(0.0)),
            ("z", "transcribe", Some(1.0), None),
        ],
    );
    assert_eq!(selected(&route, &request()).await["engine"]["model"], "b");
}

#[tokio::test]
async fn engine_phone_consumer_is_admitted_only_for_declaring_principal() {
    let harness = setup(vec![]);
    let owner = harness.bind(reserved("callosum"), None).await;
    let foreign = harness.bind(reserved("stranger"), None).await;
    let params = json!({"consumer":"phone.media","kind":"transcribe","requirements":{},"privacy":"local_only","caller_local":[engine("phone",true)]});
    assert_eq!(
        selected(&owner, &params).await["engine"]["caller_local"],
        true
    );
    let refused = foreign.call("route.select_engine", &params).await;
    assert_eq!(refused.0, FrameType::Error);
    assert_eq!(refused.1["code"], "consumer_not_owned", "{}", refused.1);
}

#[tokio::test]
async fn engine_decision_uses_shared_ledger_project_stamp_and_report_owner() {
    let harness = setup(vec![engine("remote", false)]);
    let owner = harness
        .bind(reserved("colliculus"), Some("stamped-project"))
        .await;
    let answer = selected(&owner, &request()).await;
    let id = answer["decision_id"].as_str().unwrap();
    let row = harness.store().get_decision_by_id(id).unwrap().unwrap();
    assert_eq!(row.decision_kind, "engine");
    assert_eq!(row.project_id.as_deref(), Some("stamped-project"));
    assert_eq!(row.selected_model_id.as_deref(), Some("remote"));
    let binding = harness.store().v1_decision(id).unwrap().unwrap();
    assert_eq!(binding.consumer, "colliculus");
    assert_eq!(binding.expires_at_ms - binding.created_at_ms, 600000);
    let other = harness.bind(reserved("other"), None).await;
    let report = json!({"decision_id":id,"outcome":"completed"});
    assert_eq!(
        other.call("route.report", &report).await.1["code"],
        "unknown_decision"
    );
    assert_eq!(
        owner.call("route.report", &report).await.0,
        FrameType::Response
    );
    assert_eq!(
        owner
            .call("route.explain", &json!({"decision_id":id}))
            .await
            .1["code"],
        "unknown_decision"
    );
}

fn key(id: &str) -> ListedCredential {
    ListedCredential {
        id: id.into(),
        categories: vec!["media-provider".into(), "llm-provider".into()],
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        state: "active".into(),
        provider_ids: vec!["media-vendor".into()],
        ..Default::default()
    }
}

fn listing(rows: Vec<ListedCredential>, granted: bool) -> ScopedCredentialListing {
    ScopedCredentialListing {
        view: format!(
            "synthetic-{}-{granted}",
            rows.iter()
                .map(|row| &row.id)
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        ),
        credentials: rows,
        grant_tuples: if granted {
            vec![ListedGrant {
                selector_kind: "category".into(),
                selector: "media-provider".into(),
                operation: "list".into(),
            }]
        } else {
            vec![]
        },
        ..Default::default()
    }
}

fn media_credentials(rows: Vec<ListedCredential>) -> crate::broca_credentials::BrocaCredentials {
    let mut credentials = crate::broca_credentials::BrocaCredentials::default();
    credentials.apply_listing(&listing(rows, true), crate::now_ms());
    credentials
}

#[test]
fn engine_media_keys_use_category_method_and_explicit_provider_not_synthetic_ids() {
    let valid = key("opaque-17");
    assert!(crate::account_selection::media_credential_serves(
        &valid,
        "media-vendor"
    ));
    let mut cases = Vec::new();
    let mut row = valid.clone();
    row.categories = vec!["llm-provider".into()];
    cases.push(row);
    let mut row = valid.clone();
    row.auth_method = Some("oauth".into());
    cases.push(row);
    let mut row = valid.clone();
    row.auth_method = None;
    cases.push(row);
    let mut row = valid.clone();
    row.provider_ids.clear();
    row.serves = vec!["media-vendor".into()];
    cases.push(row);
    let mut row = valid.clone();
    row.state = "disabled".into();
    cases.push(row);
    let mut row = valid.clone();
    row.auth_method = Some("oauth".into());
    row.id = "apikey:synthetic".into();
    cases.push(row);
    for row in cases {
        assert!(
            !crate::account_selection::media_credential_serves(&row, "media-vendor"),
            "{row:?}"
        );
    }
}

/// The vault's real listing row for a media key, as `ck auth list --json`
/// showed it on 10 November 2026: auth method `apikey`, no refresh adapter.
/// Claustrum's producer fixture decodes API-key rows to the same auth method
/// (`broca_credentials::producer_fixture_list_only_and_all_auth_method_rows_decode`).
/// An earlier check compared against `api_key`, which no vault row carries,
/// so every media engine was unreachable.
#[test]
fn engine_media_key_matches_the_vault_listing_spelling() {
    let row = ListedCredential {
        id: "apikey:openai:frank".into(),
        categories: vec!["llm-provider".into(), "media-provider".into()],
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        refresh_adapter: None,
        provider_ids: vec!["openai".into()],
        state: "active".into(),
        ..Default::default()
    };
    assert!(crate::account_selection::media_credential_serves(
        &row, "openai"
    ));
}

#[tokio::test]
async fn engine_accounts_fail_closed_without_media_grant_or_eligible_key_and_never_use_llm_only() {
    let harness = setup(vec![engine("remote", false)]);
    let route = harness.bind(reserved("colliculus"), None).await;
    let answer = selected(&route, &request()).await;
    let admission = |id: &str| json!({"admission_id":id,"conversation_key":"media-job","provider":"media-vendor","model":"remote","decision_id":answer["decision_id"],"hold_wait_max_s":0});
    let mut llm = key("opaque-llm");
    llm.categories = vec!["llm-provider".into()];
    for (id, rows, granted, code) in [
        (
            "missing-grant",
            vec![key("opaque-media"), llm.clone()],
            false,
            "account_selection_unavailable",
        ),
        ("no-media", vec![llm.clone()], true, "auth_required"),
        ("media-key", vec![key("opaque-media"), llm], true, ""),
    ] {
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        credentials.apply_listing(&listing(rows, granted), crate::now_ms());
        harness.set_credentials(credentials);
        let reply = route.call("route.account_for", &admission(id)).await;
        if code.is_empty() {
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            assert_eq!(
                reply.1["result"]["account"]["credential_id"], "opaque-media",
                "{}",
                reply.1
            );
        } else {
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            assert_eq!(reply.1["code"], code, "{}", reply.1);
        }
    }
}

#[test]
fn engine_media_listing_failure_revokes_cached_admission_readiness() {
    let mut credentials = crate::broca_credentials::BrocaCredentials::default();
    credentials.apply_listing(&listing(vec![key("opaque-media")], true), 100);
    assert!(credentials.media_listing_ready(100));
    credentials.note_failure("synthetic list refusal");
    assert!(!credentials.media_listing_ready(100));
    credentials.apply_listing(&listing(vec![key("opaque-media")], false), 101);
    assert!(!credentials.media_listing_ready(101));
}

#[tokio::test]
async fn engine_transcription_prices_use_audio_units_and_never_guess_token_conversion() {
    let harness = setup(vec![
        engine("a", false),
        engine("b", false),
        engine("z", false),
    ]);
    let route = harness.bind(reserved("colliculus"), None).await;
    let mut models = json!({
        "media-vendor/a":{"existence":"present","rate.input":{"state":"stated_zero"},"rate.output":{"state":"stated_zero"}},
        "media-vendor/b":{"existence":"present","rate.input_audio":{"state":"priced","currency":"USD","units":1500000000_u64,"exponent":9}},
        "media-vendor/z":{"existence":"present","rate.input_audio.per_second":{"state":"priced","currency":"USD","units":4500000,"exponent":9,"per":60,"increment":{"kind":"unknown"},"minimum_quantity":{"kind":"unknown"}}}
    });
    let snapshot = |models: &Value| {
        crate::fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({"catalog_version":1,"resolved_at_ms":1,"models":models}))
                .unwrap(),
        )
        .unwrap()
    };
    let mut params = request();
    params["requirements"] = json!({"duration_ms":60000});
    let parsed =
        cortexkit_role_routing::select_engine::parse_select_engine_request(&params).unwrap();
    let first = snapshot(&models);
    assert_eq!(
        first.engine_cost(&parsed, "media-vendor", "z"),
        Some(0.0045)
    );
    assert_eq!(first.engine_cost(&parsed, "media-vendor", "b"), None);
    assert_eq!(first.engine_cost(&parsed, "media-vendor", "a"), None);
    let mut credentials = media_credentials(vec![key("opaque-media")]);
    credentials.catalog_snapshot = Some(Arc::new(first));
    harness.set_credentials(credentials);
    // Declarations need to state a duration limit before a duration request can match.
    let mut entries: Vec<_> = ["a", "b", "z"]
        .into_iter()
        .map(|model| engine(model, false))
        .collect();
    for entry in &mut entries {
        entry["max_duration_ms"] = json!(60000);
    }
    super::consumers::replace_catalog(
        &json!([{"module_id":"colliculus","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":entries}}}]),
    );
    assert_eq!(selected(&route, &params).await["engine"]["model"], "z");
    models["media-vendor/b"]["limit.audio_tokens_per_second"] = json!(32);
    let second = snapshot(&models);
    assert!((second.engine_cost(&parsed, "media-vendor", "b").unwrap() - 0.00288).abs() < 1e-12);
    let mut credentials = media_credentials(vec![key("opaque-media")]);
    credentials.catalog_snapshot = Some(Arc::new(second));
    harness.set_credentials(credentials);
    assert_eq!(selected(&route, &params).await["engine"]["model"], "b");
    params["requirements"] = json!({});
    assert_eq!(selected(&route, &params).await["engine"]["model"], "a");
    models["media-vendor/z"]["rate.input_audio.per_second"]["per"] = json!(0);
    assert_eq!(
        snapshot(&models).engine_cost(&parsed, "media-vendor", "z"),
        None
    );
}

#[tokio::test]
async fn engine_admission_binding_expiry_and_replay_use_the_shared_allocator() {
    let harness = setup(vec![engine("remote", false)]);
    let make_credentials = |offset| {
        let mut credentials = crate::broca_credentials::BrocaCredentials::default();
        credentials.apply_listing(&listing(vec![key("opaque-media")], true), crate::now_ms());
        credentials.engine_clock_offset_ms = offset;
        credentials
    };
    harness.set_credentials(make_credentials(0));
    let route = harness.bind(reserved("colliculus"), None).await;
    let answer = selected(&route, &request()).await;
    let mut params = json!({"admission_id":"first","conversation_key":"job","provider":"media-vendor","model":"remote","decision_id":answer["decision_id"],"hold_wait_max_s":0});
    let first = route.call("route.account_for", &params).await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    assert_eq!(first.1["result"]["admission"]["state"], "reserved");
    harness.set_credentials(make_credentials(600000));
    assert_eq!(route.call("route.account_for", &params).await, first);
    params["admission_id"] = json!("expired");
    assert_eq!(
        route.call("route.account_for", &params).await.1["code"],
        "unknown_decision"
    );
    params["model"] = json!("not-in-slot");
    assert_eq!(
        route.call("route.account_for", &params).await.1["code"],
        "account_not_authorized"
    );
    harness.set_credentials(make_credentials(0));
    let mut local = request();
    local["privacy"] = json!("local_only");
    local["caller_local"] = json!([engine("remote", true)]);
    let caller_local = selected(&route, &local).await;
    params["model"] = json!("remote");
    params["decision_id"] = caller_local["decision_id"].clone();
    assert_eq!(
        route.call("route.account_for", &params).await.1["code"],
        "unknown_decision"
    );
    super::consumers::replace_catalog(&json!([
        {"module_id":"colliculus","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":[engine("remote",false)]}}},
        {"module_id":"foreign-media","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":[engine("remote",false)]}}}
    ]));
    let foreign = harness.bind(reserved("foreign-media"), None).await;
    params["decision_id"] = answer["decision_id"].clone();
    assert_eq!(
        foreign.call("route.account_for", &params).await.1["code"],
        "unknown_decision"
    );
}

/// An identical admission can commit between this request's replay check and
/// its decision binding. If the binding then refuses (here the engine decision
/// expired meanwhile), the request must answer as the committed admission did,
/// not with `unknown_decision`.
#[tokio::test]
async fn engine_admission_refused_after_its_twin_committed_replays_the_twin() {
    let harness = setup(vec![engine("remote", false)]);
    let mut credentials = crate::broca_credentials::BrocaCredentials::default();
    credentials.apply_listing(&listing(vec![key("opaque-media")], true), crate::now_ms());
    harness.set_credentials(credentials);
    let route = Arc::new(harness.bind(reserved("colliculus"), None).await);
    let answer = selected(&route, &request()).await;
    let params = json!({"admission_id":"twin","conversation_key":"job","provider":"media-vendor",
        "model":"remote","decision_id":answer["decision_id"],"hold_wait_max_s":0});
    let twin = Arc::new(std::sync::Mutex::new(None));
    {
        let (route, params, twin, credentials) = (
            route.clone(),
            params.clone(),
            twin.clone(),
            harness.credentials(),
        );
        crate::account_selection::after_roster_prepared("v1-replay-checked:twin", move || {
            *twin.lock().unwrap() = Some(route.account_for_direct(&params));
            credentials.lock().unwrap().engine_clock_offset_ms = 600_000;
        });
    }
    let late = route.account_for_direct(&params);
    let twin = twin
        .lock()
        .unwrap()
        .take()
        .expect("the twin ran inside the request");
    let twin = twin.expect("the twin was admitted");
    assert_eq!(twin["admission"]["state"], "reserved");
    assert_eq!(late.map_err(|refusal| refusal.code), Ok(twin));
}

#[tokio::test]
async fn remote_engines_are_automatic_without_llm_spending_opt_in() {
    let harness = setup(vec![engine("remote", false)]);
    harness.edit_config(|config| config["providers"] = json!({}));
    let route = harness.bind(reserved("colliculus"), None).await;
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "remote"
    );
    harness.edit_config(|config| config["providers"]["media-vendor"]["pin_only"] = json!(true));
    let mut local_request = request();
    local_request["caller_local"] = json!([engine("local", true)]);
    assert_eq!(
        selected(&route, &local_request).await["engine"]["model"],
        "local"
    );
}

#[tokio::test]
async fn remote_engine_operator_rules_refuse_by_consumer_but_ignore_pin_only() {
    let harness = setup(vec![engine("remote", false)]);
    let route = harness.bind(reserved("colliculus"), None).await;
    // A consumer-scoped refusal still keeps the engine out.
    harness.edit_config(|config| {
        config["providers"]["media-vendor"] = json!({"not_for":["colliculus"]})
    });
    let refused = route.call("route.select_engine", &request()).await;
    assert_eq!(refused.0, FrameType::Error, "{refused:?}");
    assert_eq!(refused.1["code"], "no_engine");
    // Engines have no pin operation, so a pin-only rule must not make them
    // unusable: a provider marked pin-only for chat models still serves media.
    harness.edit_config(|config| config["providers"]["media-vendor"] = json!({"pin_only":true}));
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "remote"
    );
    harness.edit_config(|config| {
        config["providers"]["media-vendor"] = json!({"pin_only":true});
        config["models"]["media-vendor/remote"] = json!({"pin_only":true});
    });
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "remote"
    );
    // The old exclude grammar converts to pin-only entity rules, which are
    // likewise not a refusal for an engine.
    let excluded = crate::config_convert::convert_value(
        json!({"model_routing":{"exclude":{"all":["media-vendor"]}}}),
    )
    .unwrap();
    harness.edit_config(|config| {
        config["providers"] = excluded["providers"].clone();
        config["models"] = json!({});
    });
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "remote"
    );
}
