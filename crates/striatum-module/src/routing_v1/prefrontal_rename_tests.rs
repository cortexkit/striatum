use super::conformance_tests::reserved;
use super::dispatch_tests::configured;
use serde_json::{json, Value};
use subc_protocol::FrameType;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"].clone()
}

#[tokio::test]
async fn prefrontal_rename_settings_only_what_if_inherits_both_directions() {
    for (configured_root, requested_root) in [
        ("prefrontal-core", "prefrontal"),
        ("prefrontal", "prefrontal-core"),
    ] {
        let (harness, _) = configured().await;
        harness.edit_config(|config| {
            config["consumers"] = json!({});
            config["consumers"][format!("{configured_root}.mason")] =
                json!({"pay_per_token":true,"iq":0,"below_bar":"best_effort"});
            config["models"]["example-provider/model-b"]["not_for"] =
                json!([format!("{configured_root}.mason")]);
        });
        let operator = harness.bind(json!({"kind":"direct"}), None).await;
        let mut answers = Vec::new();
        for root in [configured_root, requested_root] {
            let answer = result(
                operator
                    .call(
                        "route.what_if",
                        &json!({"consumer":format!("{root}.mason.review"),"iqs":[0,50]}),
                    )
                    .await,
            );
            answers.push(answer["results"].clone());
        }
        assert_eq!(answers[0], answers[1]);
        assert!(answers[0].as_array().is_some_and(|rows| !rows.is_empty()));
    }
}

#[tokio::test]
async fn prefrontal_rename_old_decision_report_read_and_admit_by_new_principal() {
    let (harness, _) = configured().await;
    harness.edit_config(|config| {
        config["consumers"]["prefrontal-core.mason"] = json!({"owner":"prefrontal-core"});
    });
    let old = harness.bind(reserved("prefrontal-core"), None).await;
    let new = harness.bind(reserved("prefrontal"), None).await;
    let answer = result(old.call("route.select", &json!({"consumer":"prefrontal-core.mason","requirements":{"iq":50},"pin":"example-subscription/model-c","affinity":"rename"})).await);
    let id = answer["decision_id"].as_str().unwrap();
    let runner = harness
        .bind(reserved(answer["runner"]["module"].as_str().unwrap()), None)
        .await;
    result(runner.call("route.account_for", &json!({"decision_id":id,"admission_id":"rename-admission","provider":"example-subscription","model":"model-c","conversation_key":"rename"})).await);
    let observe = json!({"decision_id":id,"admission_id":"rename-admission","event_id":"rename-backoff","kind":"transient_backoff","retry_at_ms":super::now_ms()+10_000});
    let first = result(old.call("route.account_observe", &observe).await);
    assert_eq!(first["observation"]["duplicate"], false);
    assert_eq!(first["observation"]["bound"], true);
    let repeated = result(new.call("route.account_observe", &observe).await);
    assert_eq!(repeated["observation"]["duplicate"], true);
    assert_eq!(repeated["observation"]["id"], first["observation"]["id"]);
    let count: i64 = rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .query_row(
            "SELECT count(*) FROM account_observation WHERE event_id='rename-backoff'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    result(
        new.call(
            "route.report",
            &json!({"decision_id":id,"outcome":"completed"}),
        )
        .await,
    );
    result(new.call("route.explain", &json!({"decision_id":id})).await);
    for caller in [&old, &new] {
        let denied = caller.call("route.account_for", &json!({"decision_id":id,"admission_id":"denied","provider":"example-subscription","model":"model-c","conversation_key":"rename"})).await;
        assert_eq!(denied.1["code"], "account_not_authorized");
        let denied = caller
            .call(
                "route.account_observe",
                &json!({"event_id":"unbound","kind":"attempt"}),
            )
            .await;
        assert_eq!(denied.1["code"], "account_not_authorized");
    }
    let binding = harness.store().v1_decision(id).unwrap().unwrap();
    assert_eq!(binding.principal, "reserved:prefrontal-core");
    assert_eq!(binding.consumer, "prefrontal-core.mason");
    assert_eq!(
        harness
            .store()
            .v1_affinity_decision("reserved:prefrontal", "prefrontal.mason", "rename")
            .unwrap()
            .unwrap()
            .decision_id,
        id
    );
}

#[tokio::test]
async fn prefrontal_rename_new_owner_and_default_user_pin() {
    let (harness, _) = configured().await;
    harness.edit_config(|config| {
        config["consumers"]["prefrontal-core.mason"] = json!({"owner":"prefrontal-core"});
    });
    let new = harness.bind(reserved("prefrontal"), None).await;
    let answer = result(new.call("route.select", &json!({"consumer":"prefrontal.mason","requirements":{"iq":50},"pin":"example-subscription/model-c","pin_origin":"user"})).await);
    assert_eq!(answer["pin_outcome"], "pinned");
}
