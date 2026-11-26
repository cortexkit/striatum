//! Account admission follows route authority, never caller-supplied identity.

use std::sync::Arc;

use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::account_classes_tests::setup;
use super::conformance_tests::{reserved, Harness, Route};

mod enforcement_tests;
mod fork_tests;
mod session_continue_tests;
mod shadow_lines_tests;
mod sticky_full_tests;

const NOW: u64 = 1_800_000_000_000;

fn request(id: &str) -> Value {
    json!({"provider":"synthetic", "model":"model", "conversation_key":"body-key", "admission_id":id})
}

fn hosts(entries: &[(&str, bool, bool)]) {
    super::consumers::replace_catalog(&Value::Array(entries.iter().map(|(module, verified, host)| {
        json!({"module_id":module,"nonce_verified":verified,"provides":if *host {vec!["agent-host/v1"]} else {vec![]}})
    }).collect()));
}

async fn session(harness: &Arc<Harness>, owner: &str, reference: &str, kind: &str) -> Route {
    let channel = harness.reserve_channel();
    harness.bind_frame(json!({
        "op":"route.bind", "route_channel":channel,"epoch":1,
        "target":{"kind":"management_surface","module_id":crate::DEFAULT_MODULE_ID},
        "identity":{"project_root":cortexkit_test_support::scratch_root(),"harness":"account-ops","session":format!("session-{channel}")},
        "principal":reserved("bridge-relay"),
        "scope":{"owner":reserved(owner),"ref":reference,"scope_epoch":7,"kind":kind,"owner_authorized":true}
    }), channel, 1).await
}

fn response(reply: &(FrameType, Value)) -> &Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    &reply.1["result"]
}

#[tokio::test]
async fn runner_not_sent_observe_reader_check() {
    for ordinary_duplicate in [false, true] {
        let _clock = crate::InjectedClock::set(NOW);
        let (harness, _) = setup("synthetic", false);
        harness.edit_config(|config| {
            config["runners"] = json!([{"module":"thalamus-harness","via_vault":true}]);
        });
        let route = harness.bind(reserved("thalamus-harness"), None).await;
        let mut params = request("reader-check");
        params["initial_input_bytes"] = json!(32768);
        params["initial_input_estimated"] = json!(true);
        let admitted_reply = route.call("route.account_for", &params).await;
        let admitted = response(&admitted_reply).clone();
        let account = admitted["account"]["account_id"].as_str().unwrap();
        let demand = || {
            harness
                .store()
                .account_read(|tx| tx.demand("synthetic", account, None))
                .unwrap()
        };
        assert_eq!(
            demand().iter().map(|row| row.request_bytes).sum::<u64>(),
            32768
        );

        let body = json!({"kind":"run_finished", "admission_id":"reader-check",
            "event_id":"reader-check:end:run_finished", "run_id":"reader-run",
            "terminal_reason":"not_sent", "caller":{"runner":"thalamus-harness"}});
        let first = route.call("route.account_observe", &body).await;
        let observation = &response(&first)["observation"];
        assert_eq!(observation["bound"], true);
        assert_eq!(observation["duplicate"], false);
        assert_eq!(
            observation["effects"],
            json!([
                {"effect":"admission_finished"}, {"effect":"lease_ended", "lease_ended":true}
            ])
        );
        let row = || {
            harness
                .store()
                .account_read(|tx| tx.admission("reserved:thalamus-harness", "reader-check"))
                .unwrap()
                .unwrap()
        };
        let finished = row();
        assert_eq!(finished.state, "finished");
        assert!(!finished.charged);
        assert_eq!(finished.run_id, None);
        assert_eq!(finished.started_at, None);
        assert!(demand().is_empty());
        assert_eq!(
            crate::account_state::snapshot(harness.store())
                .unwrap()
                .admissions
                .demand("synthetic", account, None),
            demand()
        );

        let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
        let raw: String = conn
            .query_row(
                "SELECT observation_json FROM account_observation WHERE event_id=?1",
                ["reader-check:end:run_finished"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(serde_json::from_str::<Value>(&raw).unwrap(), body);
        conn.execute_batch("CREATE TABLE reader_check_writes (kind TEXT);
            CREATE TRIGGER reader_check_admission AFTER UPDATE ON account_admission BEGIN INSERT INTO reader_check_writes VALUES ('admission'); END;
            CREATE TRIGGER reader_check_affinity AFTER UPDATE ON account_affinity BEGIN INSERT INTO reader_check_writes VALUES ('affinity'); END;
            CREATE TRIGGER reader_check_observation AFTER INSERT ON account_observation BEGIN INSERT INTO reader_check_writes VALUES ('observation'); END;").unwrap();
        let mut duplicate = body.clone();
        if ordinary_duplicate {
            duplicate.as_object_mut().unwrap().remove("terminal_reason");
        }
        let _later = crate::InjectedClock::set(NOW + 1000);
        let second = route.call("route.account_observe", &duplicate).await;
        let second = &response(&second)["observation"];
        assert_eq!(second["duplicate"], true);
        assert_eq!(second["bound"], true);
        assert_eq!(second["id"], observation["id"]);
        assert_eq!(second["effects"], observation["effects"]);
        assert_eq!(row(), finished);
        assert!(demand().is_empty());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM reader_check_writes", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );

        let next = route
            .call("route.account_for", &request("reader-next"))
            .await;
        let next = response(&next);
        assert_eq!(next["reason"]["code"], "sticky_hit");
        assert_eq!(next["account"], admitted["account"]);
        assert_eq!(next["pin"]["id"], admitted["pin"]["id"]);
    }
}

#[tokio::test]
async fn nonrunners_cannot_admit_or_observe_even_with_a_decision_or_body_identity() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    super::consumers::replace_catalog(&json!([{
        "module_id":"declaring-consumer", "nonce_verified":true,
        "role_declarations":{"routing/v1":{"consumers":[{"name":"declared-consumer","defaults":{}}]}}
    }]));
    for module in ["class-owner", "declaring-consumer"] {
        let route = harness.bind(reserved(module), None).await;
        for decision in [None, Some("existing-or-invented-decision")] {
            let mut body = request("refused");
            if let Some(decision) = decision {
                body["decision_id"] = json!(decision);
            }
            for extra in [None, Some("plugin"), Some("source")] {
                let mut body = body.clone();
                if let Some(extra) = extra {
                    body[extra] = json!("agent-host/v1");
                }
                // A caller that is not an attested runner is refused as
                // `account_not_authorized`; the contract has no
                // `unscoped_caller` code.
                let reply = route.call("route.account_for", &body).await;
                assert_eq!(reply.1["code"], "account_not_authorized", "{}", reply.1);
                assert_eq!(reply.1["detail"], json!({}), "{}", reply.1);
                let mut observed = json!({"event_id":"refused","kind":"account_limit_exhausted","admission_id":"refused"});
                if let Some(extra) = extra {
                    observed[extra] = json!("agent-host/v1");
                }
                // `route.account_observe` is also a runner op: a non-runner is
                // `account_not_authorized` (§9).
                let reply = route.call("route.account_observe", &observed).await;
                assert_eq!(reply.1["code"], "account_not_authorized", "{}", reply.1);
            }
        }
        assert!(harness
            .store()
            .v1_admission(&format!("reserved:{module}"), "refused")
            .unwrap()
            .is_none());
    }
    assert_eq!(harness.store().account_serving_counts().unwrap().total(), 0);
}

#[tokio::test]
async fn session_authority_requires_a_current_head_scope_and_verified_host_capability() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    hosts(&[
        ("replaceable-host", true, true),
        ("non-host", true, false),
        ("unverified-host", false, true),
    ]);
    for (owner, kind) in [
        ("non-host", "head"),
        ("unverified-host", "head"),
        ("replaceable-host", "ephemeral"),
    ] {
        let route = session(&harness, owner, "not-a-session", kind).await;
        for (op, body) in [
            ("route.account_for", request("refused")),
            (
                "route.account_observe",
                json!({"event_id":"refused","kind":"attempt"}),
            ),
        ] {
            // Both runner ops refuse a non-runner as `account_not_authorized`.
            let reply = route.call(op, &body).await;
            assert_eq!(reply.1["code"], "account_not_authorized", "{}", reply.1);
        }
    }
    let route = session(&harness, "replaceable-host", "stale", "head").await;
    let channel = route
        .request_frame("route.account_for", &request("stale"))
        .header
        .channel;
    let mut caller = super::Caller::for_route(channel, 1);
    caller.binding = Some((channel, 2));
    let credentials = Arc::new(std::sync::Mutex::new(
        crate::broca_credentials::BrocaCredentials::default(),
    ));
    let path = std::path::Path::new("unused.jsonc");
    let quota = Arc::new(std::sync::Mutex::new(crate::QuotaCache::default()));
    let deps = super::Deps {
        quota_cache: &quota,
        store: harness.store(),
        routing_config_path: path,
        operator_config_path: path,
        broca_credentials: &credentials,
    };
    assert_eq!(
        super::accounts::account_for(&request("stale"), &caller, &deps)
            .unwrap_err()
            .code
            .as_str(),
        "account_not_authorized"
    );
    assert_eq!(
        super::accounts::account_observe(
            &json!({"event_id":"stale","kind":"attempt"}),
            &caller,
            &deps
        )
        .unwrap_err()
        .code
        .as_str(),
        "account_not_authorized"
    );
    assert_eq!(harness.store().account_serving_counts().unwrap().total(), 0);
}

#[tokio::test]
pub(super) async fn session_ids_and_affinity_are_scoped_by_owner_and_ref_and_parent_use_is_shared_only_within_owner(
) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    hosts(&[("host-one", true, true), ("host-two", true, true)]);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(true);
        config["providers"]["synthetic"]["accounts"] =
            json!({"strategy":"ordered","order":["oauth-account","apikey-account"]});
    });
    let parent = session(&harness, "host-one", "parent", "head").await;
    let first = parent.call("route.account_for", &request("same-id")).await;
    assert_eq!(response(&first)["account"]["account_id"], "oauth-account");
    let used = parent.call("route.account_observe", &json!({"event_id":"same-event","kind":"admission_started","admission_id":"same-id","run_id":"parent-run","served_account_id":"oauth-account"})).await;
    assert_eq!(response(&used)["observation"]["bound"], true);

    let child = session(&harness, "host-one", "child", "head").await;
    let mut body = request("same-id");
    body["parent_conversation_key"] = json!("parent");
    let admitted = child.call("route.account_for", &body).await;
    assert_eq!(
        response(&admitted)["account"]["account_id"],
        "apikey-account"
    );
    assert_ne!(
        response(&admitted)["pin"]["id"],
        response(&first)["pin"]["id"]
    );
    body["conversation_key"] = json!("a-body-cannot-replace-the-stamp");
    assert_eq!(child.call("route.account_for", &body).await, admitted);
    let observed = child.call("route.account_observe", &json!({"event_id":"same-event","kind":"admission_started","admission_id":"same-id","run_id":"child-run"})).await;
    assert_eq!(response(&observed)["observation"]["duplicate"], false);
    assert_eq!(response(&observed)["observation"]["bound"], true);

    let other_owner = session(&harness, "host-two", "child", "head").await;
    let other = other_owner.call("route.account_for", &body).await;
    assert_eq!(response(&other)["account"]["account_id"], "oauth-account");
    assert_ne!(
        response(&other)["pin"]["id"],
        response(&admitted)["pin"]["id"]
    );
    let mut missing_parent = request("missing-parent");
    missing_parent["parent_conversation_key"] = json!("never-reported");
    let orphan = session(&harness, "host-one", "orphan", "head").await;
    assert_eq!(
        response(&orphan.call("route.account_for", &missing_parent).await)["account"]["account_id"],
        "oauth-account"
    );
}

#[tokio::test]
async fn v1_members_are_validated_on_the_live_handler_and_token_size_reaches_the_allocator() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    let selected = consumer
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    let decision = response(&selected)["decision_id"].clone();
    for pool in [String::new(), "a".repeat(65), "bad/name".into()] {
        for (op, mut params) in [
            ("route.account_for", request("invalid-pool")),
            (
                "route.account_observe",
                json!({"event_id":"invalid-pool","kind":"attempt"}),
            ),
            (
                "route.report",
                json!({"decision_id":decision,"outcome":"failed"}),
            ),
        ] {
            params["pool"] = json!(pool);
            let route = if op == "route.report" {
                &consumer
            } else {
                &runner
            };
            let refused = route.call(op, &params).await;
            assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
            assert_eq!(refused.1["detail"]["field"], "pool", "{}", refused.1);
        }
    }
    for extra in [
        json!({"reset_at_ms":NOW+60_000}),
        json!({"runner_code":"account_limit_exhausted"}),
    ] {
        let mut params = json!({"decision_id":decision,"outcome":"refused","eligibility":{"scope":"model","code":"not_on_account"}});
        params
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let refused = consumer.call("route.report", &params).await;
        assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
        assert_eq!(refused.1["detail"]["field"], "eligibility", "{}", refused.1);
    }
    let mut valid = request("unknown-pool");
    valid["pool"] = json!("new-pool_1.valid");
    response(&runner.call("route.account_for", &valid).await);
    let observed = runner.call("route.account_observe", &json!({"event_id":"unknown-pool","kind":"used_account","admission_id":"unknown-pool","pool":"new-pool_1.valid"})).await;
    assert_eq!(response(&observed)["observation"]["bound"], true);
    response(&consumer.call("route.report", &json!({
        "decision_id":decision,"outcome":"refused","runner_code":"size_shaped_429",
        "credential_id":"oauth:test","request_input_tokens":169_000,"reset_at_ms":NOW+600_000,
        "pool":"new-pool_1.valid"
    })).await);
    let mut large = request("large");
    large["request_input_tokens"] = json!(169_000);
    // The size refusal filtered every account in the class for this input.
    let refused = runner.call("route.account_for", &large).await;
    assert_eq!(refused.1["code"], "account_not_authorized", "{}", refused.1);
    assert_eq!(refused.1["detail"], json!({}), "{}", refused.1);
    let mut small = request("small");
    small["request_input_tokens"] = json!(1_000);
    response(&runner.call("route.account_for", &small).await);
}

#[tokio::test]
async fn late_or_missing_observations_never_fail_an_admission() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let unknown = runner.call("route.account_observe", &json!({"event_id":"before-admission","kind":"account_limit_exhausted","admission_id":"missing"})).await;
    assert_eq!(response(&unknown)["observation"]["bound"], false);
    let admitted = runner
        .call("route.account_for", &request("missing-observation"))
        .await;
    response(&admitted);
    let _later = crate::InjectedClock::set(NOW + 3_600_000);
    let late = runner.call("route.account_observe", &json!({"event_id":"late","kind":"admission_started","admission_id":"missing-observation","run_id":"late-run"})).await;
    assert_eq!(response(&late)["observation"]["bound"], true);
    response(
        &runner
            .call("route.account_for", &request("after-late-report"))
            .await,
    );
}

#[tokio::test]
async fn a_scoped_nonrunner_cannot_use_its_own_decision_to_unlock_metered_accounts() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    hosts(&[("replaceable-host", true, true)]);
    harness.edit_config(|config| config["consumers"]["codemode"]["owner"] = json!("bridge-relay"));
    let route = session(&harness, "replaceable-host", "scoped-decision", "head").await;
    let selected = route
        .call(
            "route.select",
            &json!({"consumer":"codemode","requirements":{"iq":30}}),
        )
        .await;
    let mut body = request("nonrunner-decision");
    body["decision_id"] = response(&selected)["decision_id"].clone();
    let refused = route.call("route.account_for", &body).await;
    assert_eq!(refused.1["code"], "unknown_decision", "{}", refused.1);
    assert_eq!(harness.store().account_serving_counts().unwrap().total(), 0);
    body.as_object_mut().unwrap().remove("decision_id");
    let admitted = route.call("route.account_for", &body).await;
    assert_eq!(
        response(&admitted)["account"]["credential_id"],
        "oauth:test"
    );
}

#[tokio::test]
async fn provider_strategy_and_pool_floors_are_readable_without_allocating() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["accounts"] = json!({
            "strategy":"ordered","ordered":"fallback-first","order":["oauth-account","apikey-account"],
            "floors":[{"pool":"named-pool","window_minutes":300,"remaining_percent":8.0}],
            "max_hold":"45s"
        });
        config["providers"]["other"]["accounts"] = json!({"strategy":"round_robin"});
    });
    let route = harness.bind(reserved("class-owner"), None).await;
    let described = route.call("role.describe", &json!({})).await;
    let policies = &response(&described)["account_policies"];
    assert_eq!(policies["synthetic"]["strategy"], "ordered");
    assert_eq!(policies["synthetic"]["fallback_first"], true);
    assert_eq!(policies["synthetic"]["max_hold_s"], 45);
    assert!(policies["synthetic"].get("short_hold_max_ms").is_none());
    assert_eq!(policies["other"]["max_hold_s"], 3600);
    assert_eq!(
        policies["synthetic"]["pool_floors"][0]["remaining_percent"],
        8.0
    );
    assert_eq!(policies["other"]["strategy"], "round_robin");
    assert_eq!(harness.store().account_serving_counts().unwrap().total(), 0);
}

#[tokio::test]
async fn parent_preference_uses_a_live_reservation_and_lapses_at_the_start_or_lease_deadline() {
    for started in [false, true] {
        let _clock = crate::InjectedClock::set(NOW);
        let (harness, _) = setup("synthetic", false);
        harness.edit_config(|config| {
            config["providers"]["synthetic"]["subscription"] = json!(true);
            config["providers"]["synthetic"]["accounts"] =
                json!({"strategy":"ordered","order":["oauth-account","apikey-account"]});
        });
        let runner = harness.bind(reserved("class-runner"), None).await;
        let mut parent = request("parent");
        parent["conversation_key"] = json!("parent-key");
        let parent = runner.call("route.account_for", &parent).await;
        assert_eq!(response(&parent)["account"]["account_id"], "oauth-account");
        let deadline = response(&parent)["pin"][if started {
            "lease_valid_until_ms"
        } else {
            "admission_valid_until_ms"
        }]
        .as_u64()
        .unwrap();
        if started {
            response(&runner.call("route.account_observe", &json!({"event_id":"started","kind":"admission_started","admission_id":"parent","run_id":"run"})).await);
        }
        for (now, id, expected) in [
            (NOW, "child-before-report", "apikey-account"),
            (deadline - 1, "child-before-lapse", "apikey-account"),
            (deadline, "child-at-lapse", "oauth-account"),
        ] {
            let _clock = crate::InjectedClock::set(now);
            let mut child = request(id);
            child["conversation_key"] = json!(id);
            child["parent_conversation_key"] = json!("parent-key");
            let reply = runner.call("route.account_for", &child).await;
            assert_eq!(
                response(&reply)["account"]["account_id"],
                expected,
                "started={started}, now={now}"
            );
        }
    }
}

#[tokio::test]
async fn renewing_the_parent_pin_extends_parent_preference_past_the_old_admission_deadline() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(true);
        config["providers"]["synthetic"]["accounts"] =
            json!({"strategy":"ordered","order":["oauth-account","apikey-account"]});
    });
    let runner = harness.bind(reserved("class-runner"), None).await;
    let mut parent = request("first");
    parent["conversation_key"] = json!("parent-key");
    let first = runner.call("route.account_for", &parent).await;
    let deadline = response(&first)["pin"]["admission_valid_until_ms"]
        .as_u64()
        .unwrap();
    let _clock = crate::InjectedClock::set(deadline - 1);
    parent["admission_id"] = json!("renewed");
    parent["mode"] = json!("renew");
    parent["expected_pin"] = json!({"id":response(&first)["pin"]["id"],"generation":response(&first)["pin"]["generation"]});
    response(&runner.call("route.account_for", &parent).await);
    let _clock = crate::InjectedClock::set(deadline);
    let mut child = request("child");
    child["conversation_key"] = json!("child-key");
    child["parent_conversation_key"] = json!("parent-key");
    let reply = runner.call("route.account_for", &child).await;
    assert_eq!(response(&reply)["account"]["account_id"], "apikey-account");
}
