use super::select_ledger_tests::{pressure, refused, second_model, select_body};
use super::*;
use striatum_core::RouteStateStore;

fn model(reply: &(FrameType, Value)) -> String {
    response(reply)["route"]["model"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn fixed_routes_ledger_honors_explicit_fallback_and_ordered_modes() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    second_model(&harness, false);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness
        .bind(reserved("class-owner"), Some("fixed-project"))
        .await;
    let mut sticky = select_body(false);
    sticky["pin"] = json!("synthetic/model");
    sticky["affinity"] = json!("continuing");
    assert_eq!(
        model(&consumer.call("route.select", &sticky).await),
        "model"
    );
    sticky.as_object_mut().unwrap().remove("pin");
    fill(&runner, A, "a", 1).await;
    fill(&runner, B, "b", 1).await;

    let mut fallback = select_body(false);
    fallback["pin"] = json!("synthetic/model");
    fallback["pin_fallback"] = json!("route");
    let answer = consumer.call("route.select", &fallback).await;
    assert_eq!(model(&answer), "other", "{}", answer.1);
    assert_eq!(response(&answer)["pin_outcome"], "fell_back");
    assert!(pressure().is_empty());

    // A pin without explicit fallback has no next route, while a continuing
    // model and an operator ladder may use the remaining adequate route.
    fallback.as_object_mut().unwrap().remove("pin_fallback");
    refused(
        &consumer.call("route.select", &fallback).await,
        false,
        NOW + 30_000,
    );
    assert_eq!(pressure().len(), 1);
    assert_eq!(
        model(&consumer.call("route.select", &sticky).await),
        "other"
    );
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!(["synthetic/model", "synthetic/other"]);
    });
    assert_eq!(
        model(&consumer.call("route.select", &select_body(false)).await),
        "other"
    );
    harness.edit_config(|config| {
        config["consumers"]["mason"]["projects"]["fixed-project"] =
            json!({"pin":"synthetic/model"});
    });
    refused(
        &consumer.call("route.select", &select_body(false)).await,
        false,
        NOW + 30_000,
    );

    // Interactive consumers retry the entire fixed-route order without
    // concurrency filtering, rather than refusing or discarding a full pin.
    for enforced in [true, false] {
        harness.edit_config(|config| {
            config["concurrency"]["enforcement_enabled"] = json!(enforced);
            config["consumer_priorities"] = json!({"mason":"interactive"});
        });
        pressure();
        for body in [&fallback, &select_body(false)] {
            assert_eq!(model(&consumer.call("route.select", body).await), "model");
        }
        assert!(pressure().is_empty());
    }
    harness.edit_config(|config| {
        config["consumer_priorities"] = json!({"mason":"worker"});
        config["consumers"]["mason"]
            .as_object_mut()
            .unwrap()
            .remove("projects");
    });
    // With enforcement disabled even an explicitly fallible pin stays put,
    // and the continuing model and ladder pick as without cap settings.
    fallback["pin_fallback"] = json!("route");
    for body in [&fallback, &select_body(false)] {
        assert_eq!(model(&consumer.call("route.select", body).await), "model");
    }
    harness.edit_config(|config| config["concurrency"]["enforcement_enabled"] = json!(true));
    let mut other = on(A, "other-a");
    other["model"] = json!("other");
    admitted_on(&runner, &other).await;
    other = on(B, "other-b");
    other["model"] = json!("other");
    admitted_on(&runner, &other).await;
    for body in [&fallback, &select_body(false), &sticky] {
        refused(
            &consumer.call("route.select", body).await,
            false,
            NOW + 30_000,
        );
    }
    harness.edit_config(|config| config["consumer_priorities"] = json!({"mason":"interactive"}));
    pressure();
    for body in [&fallback, &select_body(false)] {
        assert_eq!(model(&consumer.call("route.select", body).await), "model");
    }
    assert!(pressure().is_empty());
    classifier_order_ledger().await;
}

async fn classifier_order_ledger() {
    let harness = super::super::super::classifier_tests::rig(|_| {}, |_| {});
    const FLASH: &str = "cloudflare-workers-ai/@cf/cloudflare/clef-flash";
    const CLEF: &str = "cloudflare-workers-ai/@cf/cloudflare/clef";
    const JEV: &str = "typesafe/jev-1.13.0";
    harness.edit_config(|config| {
        config["concurrency"] = json!({"enforcement_enabled":true,"providers":{
            "cloudflare-workers-ai":{"limit":1,"scope":"account_model"},"typesafe":{"limit":1}}});
        config["consumers"]["classifier"]["mode"] = json!("ladder");
        config["consumers"]["classifier"]["ladder"] = json!([FLASH, CLEF, JEV]);
    });
    let route = harness.bind(reserved("broca"), None).await;
    let body = json!({"consumer":"classifier","requirements":{"iq":0},"detail":true});
    for (i, key) in [FLASH, CLEF, JEV].into_iter().enumerate() {
        let answer = route.call("route.select", &body).await;
        let answer = response(&answer);
        assert_eq!(
            format!(
                "{}/{}",
                answer["route"]["provider"].as_str().unwrap(),
                answer["route"]["model"].as_str().unwrap()
            ),
            key
        );
        let (provider, model) = key.split_once('/').unwrap();
        let admitted = route.call("route.account_for", &json!({"provider":provider,"model":model,
            "decision_id":answer["decision_id"],"conversation_key":format!("classifier-{i}"),"admission_id":format!("classifier-{i}")})).await;
        response(&admitted);
    }
    refused(
        &route.call("route.select", &body).await,
        false,
        NOW + 30_000,
    );
    harness
        .edit_config(|config| config["consumer_priorities"] = json!({"classifier":"interactive"}));
    pressure();
    assert_eq!(
        model(&route.call("route.select", &body).await),
        "@cf/cloudflare/clef-flash"
    );
    assert!(pressure().is_empty());
    harness.edit_config(|config| {
        config["consumer_priorities"] = json!({"classifier":"worker"});
        config["concurrency"]["enforcement_enabled"] = json!(false);
    });
    assert_eq!(
        model(&route.call("route.select", &body).await),
        "@cf/cloudflare/clef-flash"
    );
    assert_eq!(pressure().len(), 1);
}

fn panel_models(harness: &Harness, only_unusable_spares: bool) {
    let original = harness
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|f| f.provider_id == "synthetic" && f.model_id == "model" && f.variant.is_none())
        .unwrap();
    let mut facts = Vec::new();
    for (name, family, elo, window) in [
        ("model", "top", 3000.0, 200_000.0),
        ("standing", "standing", 2900.0, 200_000.0),
        ("duplicate", "standing", 2899.0, 200_000.0),
        ("alias", "alias", 2898.0, 200_000.0),
        ("small", "small", 2896.0, 1_000.0),
        (
            "spare",
            "spare",
            if only_unusable_spares { 1.0 } else { 2897.0 },
            200_000.0,
        ),
        ("weak", "weak", 1.0, 200_000.0),
    ] {
        let mut fact = original.clone();
        fact.model_id = name.into();
        fact.variant = None;
        fact.family = Some(family.into());
        fact.scores.elo = elo;
        fact.context_window = Some(window);
        fact.max_output_tokens = Some(32_000);
        fact.billing_plane = Some(striatum_core::billing::BillingPlane {
            provider_id: "synthetic".into(),
            auth_method: "apikey".into(),
            kind: "upstream".into(),
            selection_reason: "test metered plane".into(),
            rates: [(
                "rate.output".into(),
                striatum_core::billing::BillingRate::Priced { rate: 9.0 },
            )]
            .into_iter()
            .collect(),
        });
        facts.push(fact);
    }
    let mut credentials = harness.credentials().lock().unwrap().clone();
    harness.install_facts(&facts);
    let models: serde_json::Map<String, Value> = facts.iter().map(|fact| {
        (format!("synthetic/{}", fact.model_id), json!({
            "existence":"present","model.lineage":fact.family,
            "model.canonical_id":matches!(fact.model_id.as_str(), "standing" | "alias").then_some("standing-weights"),
            "capability.tool_call":true,"limit.context":fact.context_window.map(|window| window as u64),"limit.output":fact.max_output_tokens
        }))
    }).collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
        "catalog_version":if only_unusable_spares {901} else {900},"resolved_at_ms":NOW,"models":models
    })).unwrap()).unwrap();
    harness
        .store()
        .apply_fusiform_catalog(snapshot.parts().0, snapshot.parts().1)
        .unwrap();
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    let mut planes = facts.clone();
    for fact in &facts {
        let mut subscription = fact.clone();
        subscription.billing_plane.as_mut().unwrap().auth_method = "oauth".into();
        subscription.billing_plane.as_mut().unwrap().kind = "subscription".into();
        planes.push(subscription);
    }
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&planes),
    );
    harness.set_credentials(credentials);
}

async fn panel_answer(consumer: &Route, request: &Value) -> Value {
    let reply = consumer.call("route.select_panel", request).await;
    let answer = response(&reply).clone();
    let parsed = cortexkit_role_routing::panel::parse_select_panel_request_serving(
        request,
        super::super::super::describe::CAPABILITIES,
    )
    .unwrap();
    cortexkit_role_routing::panel::check_panel_answer(
        &parsed,
        &serde_json::from_value(answer.clone()).unwrap(),
    )
    .unwrap();
    answer
}

fn seated(answer: &Value) -> Vec<&str> {
    answer["seats"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["answer"]["route"]["model"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn panel_ledger_replaces_full_seats_without_relaxing_floor_family_or_package() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    panel_models(&harness, false);
    harness.edit_config(|config| {
        config["concurrency"]["providers"]["synthetic"]["scope"] = json!("account_model");
        config["panels"]["audit"] = json!({"top_k":2,"by":"iq","extra":0,"floor":80});
    });
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    let request =
        json!({"class":"audit","consumer":"mason","seed":"fixed","package_tokens":10_000});
    let baseline = panel_answer(&consumer, &request).await;
    assert_eq!(seated(&baseline), ["model", "standing"], "{baseline}");
    fill(&runner, A, "a", 1).await;
    fill(&runner, B, "b", 1).await;
    pressure();
    let answer = panel_answer(&consumer, &request).await;
    assert_eq!(seated(&answer), ["spare", "standing"], "{answer}");
    assert!(answer.get("shortfall").is_none());
    assert_eq!(pressure().len(), 1);
    harness.edit_config(|config| {
        config["panels"]["audit"] =
            json!({"top_k":1,"by":"iq","extra":1,"extra_by":"eq","floor":80});
    });
    let phased = panel_answer(&consumer, &request).await;
    assert_eq!(seated(&phased), ["spare", "standing"]);
    assert_eq!(phased["seats"][0]["phase"], "top");
    assert_eq!(phased["seats"][1]["phase"], "extra");
    assert_eq!(pressure().len(), 1);
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"top_k":2,"by":"iq","extra":0,"floor":80});
    });

    for account in [A, B] {
        let mut admission = on(account, &format!("spare-{account}"));
        admission["model"] = json!("spare");
        admitted_on(&runner, &admission).await;
    }
    pressure();
    let short = panel_answer(&consumer, &request).await;
    assert_eq!(seated(&short), ["standing"]);
    assert_eq!(pressure().len(), 2);

    // Neither a below-floor family nor a route too small for the package can
    // rescue a missing seat. A spare from the standing family cannot either.
    panel_models(&harness, true);
    pressure();
    let short = panel_answer(&consumer, &request).await;
    assert_eq!(seated(&short), ["standing"], "{short}");
    let causes = short["shortfall"]["detail"]["causes"].as_array().unwrap();
    let full = causes
        .iter()
        .find(|c| c["model"] == "synthetic/model")
        .expect("full member rejection");
    assert_eq!(full["reason"], "no_route");
    assert_eq!(full["reset_at_ms"], NOW + 30_000);
    assert_eq!(full["detail"]["detail"]["reason"], "concurrency_full");
    assert_eq!(pressure().len(), 1);

    for (enforced, priority, would_refuse) in [(true, "interactive", 0), (false, "worker", 1)] {
        harness.edit_config(|config| {
            config["concurrency"]["enforcement_enabled"] = json!(enforced);
            config["consumer_priorities"] = json!({"mason":priority});
        });
        pressure();
        let answer = panel_answer(&consumer, &request).await;
        assert_eq!(seated(&answer), ["model", "standing"]);
        assert!(answer.get("shortfall").is_none());
        let stable = |mut value: Value| {
            value.as_object_mut().unwrap().remove("panel_id");
            for seat in value["seats"].as_array_mut().unwrap() {
                seat["answer"]
                    .as_object_mut()
                    .unwrap()
                    .remove("decision_id");
            }
            value
        };
        assert_eq!(stable(answer), stable(baseline.clone()));
        assert_eq!(pressure().len(), would_refuse);
    }
}
