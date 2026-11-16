use super::*;
use striatum_core::RouteStateStore;

#[tokio::test]
async fn select_ledger_cap_reload_is_immediate_and_uncapped_routes_skip_eligibility() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    fill(&runner, A, "a", 1).await;
    fill(&runner, B, "b", 1).await;
    for legacy in [false] {
        refused(
            &consumer.call("route.select", &select_body(legacy)).await,
            legacy,
            NOW + 30_000,
        );
        if !legacy {
            let mut fallback = select_body(false);
            fallback["pin"] = json!("synthetic/missing");
            fallback["pin_fallback"] = json!("route");
            refused(
                &consumer.call("route.select", &fallback).await,
                false,
                NOW + 30_000,
            );
        }
        harness.edit_config(|config| {
            config["concurrency"]["providers"]["synthetic"]["limit"] = json!(2);
        });
        assert_eq!(
            consumer.call("route.select", &select_body(legacy)).await.0,
            FrameType::Response
        );
        harness.edit_config(|config| {
            config["concurrency"]["providers"]["synthetic"]["limit"] = json!(1);
        });
        refused(
            &consumer.call("route.select", &select_body(legacy)).await,
            legacy,
            NOW + 30_000,
        );
    }
    for providers in [json!({"unrelated":{"limit":1}}), json!({})] {
        harness.edit_config(|config| {
            config["concurrency"]["providers"] = providers;
        });
        crate::account_selection::SELECT_ELIGIBILITY_PROBES.store(0, Ordering::Relaxed);
        for legacy in [false] {
            assert_eq!(
                consumer.call("route.select", &select_body(legacy)).await.0,
                FrameType::Response
            );
        }
        assert_eq!(
            crate::account_selection::SELECT_ELIGIBILITY_PROBES.load(Ordering::Relaxed),
            0
        );
    }
}

pub(super) fn select_body(legacy: bool) -> Value {
    if legacy {
        json!({"targetAgent":"mason","requirements":{"iq":30,"eq":0},"substrate":"broca","executor":"broca","detail":true})
    } else {
        json!({"consumer":"mason","requirements":{"iq":30},"detail":true})
    }
}

pub(super) fn pressure() -> Vec<Value> {
    crate::take_operator_infos()
        .into_iter()
        .filter_map(|line| {
            line.strip_prefix("routing select concurrency refused ")
                .or_else(|| line.strip_prefix("routing select concurrency would refuse "))
                .map(|body| serde_json::from_str(body).unwrap())
        })
        .collect()
}

fn decisions(harness: &Harness) -> usize {
    Connection::open(harness.store_path())
        .unwrap()
        .query_row("SELECT count(*) FROM model_route_decisions", [], |row| {
            row.get(0)
        })
        .unwrap()
}

pub(super) fn refused(reply: &(FrameType, Value), legacy: bool, reset: u64) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    if legacy {
        assert_eq!(reply.1["code"], "concurrency_full", "{}", reply.1);
        assert_eq!(reply.1["earliestReset"].as_f64(), Some(reset as f64));
    } else {
        assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
        assert_eq!(
            reply.1["detail"]["reason"], "concurrency_full",
            "{}",
            reply.1
        );
        assert_eq!(reply.1["detail"]["reset_at_ms"], reset);
    }
}

#[tokio::test]
async fn select_ledger_full_surfaces_reset_audit_logs_and_heads() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    harness.edit_config(|config| {
        config["head_agents"] = json!(["mason"]);
    });
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    fill(&runner, A, "a", 1).await;
    fill(&runner, B, "b", 1).await;
    for legacy in [false] {
        for pin in [false, true] {
            let mut body = select_body(legacy);
            if pin {
                body[if legacy { "modelPin" } else { "pin" }] = json!("synthetic/model:xhigh");
            } else if !legacy {
                body["prefer"] = json!(["synthetic/model"]);
            }
            pressure();
            let before = decisions(&harness);
            refused(
                &consumer.call("route.select", &body).await,
                legacy,
                NOW + 30_000,
            );
            assert_eq!(decisions(&harness), before + 1);
            let lines = pressure();
            assert_eq!(lines.len(), 1, "{lines:?}");
            assert_eq!(lines[0]["consumer"], "mason");
            assert_eq!(lines[0]["class"], "worker");
            harness.edit_config(|config| {
                config["consumer_priorities"] = json!({"mason":"interactive"});
            });
            assert_eq!(
                consumer.call("route.select", &body).await.0,
                FrameType::Response
            );
            assert!(pressure().is_empty());
            harness.edit_config(|config| {
                config["consumer_priorities"] = json!({"mason":"worker"});
            });
        }
    }
}

#[tokio::test]
async fn select_ledger_ignores_affinity_but_excludes_full_with_blocked_sibling() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    fill(&runner, A, "a", 1).await;
    for legacy in [false] {
        let mut body = select_body(legacy);
        if !legacy {
            body["affinity"] = json!("a-0");
        }
        assert_eq!(
            consumer.call("route.select", &body).await.0,
            FrameType::Response
        );
    }
    let mut kept = fresh("kept");
    kept["conversation_key"] = json!("a-0");
    assert_concurrency_full(&runner.call("route.account_for", &kept).await, NOW + 30_000);
    block(&harness, B, (NOW + 100_000) as i64);
    for legacy in [false] {
        refused(
            &consumer.call("route.select", &select_body(legacy)).await,
            legacy,
            NOW + 30_000,
        );
    }
    block(&harness, A, (NOW + 100_000) as i64);
    let all_blocked = consumer.call("route.select", &select_body(false)).await;
    assert_ne!(all_blocked.1["detail"]["reason"], "concurrency_full");
    harness.edit_config(|config| {
        config["concurrency"]["enforcement_enabled"] = json!(false);
    });
    let without_ledger = consumer.call("route.select", &select_body(false)).await;
    assert_eq!(all_blocked, without_ledger);
}

#[tokio::test]
async fn select_ledger_reset_uses_reservation_deadline_in_epoch_milliseconds() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    fill(&runner, A, "a", 1).await;
    fill(&runner, B, "b", 1).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    let _later = at(NOW + 48_000);
    for legacy in [false] {
        refused(
            &consumer.call("route.select", &select_body(legacy)).await,
            legacy,
            NOW + 60_000,
        );
    }
}

pub(super) fn second_model(harness: &Harness, weak: bool) {
    let mut facts = harness.store().get_facts().unwrap();
    let mut second = facts
        .iter()
        .find(|fact| fact.provider_id == "synthetic" && fact.model_id == "model")
        .unwrap()
        .clone();
    second.model_id = "other".into();
    second.variant = None;
    if weak {
        second.scores.elo = 1.0;
        second.scores.eq = 1.0;
    }
    facts.push(second);
    let mut credentials = harness.credentials().lock().unwrap().clone();
    harness.install_facts(&facts);
    super::super::super::account_classes_tests::catalog_fact(harness, facts.last().unwrap());
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    let mut planes: Vec<_> = facts
        .iter()
        .filter(|fact| fact.provider_id == "synthetic")
        .cloned()
        .collect();
    for fact in &mut planes {
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
    }
    for fact in planes.clone() {
        let mut subscription = fact.clone();
        subscription.billing_plane.as_mut().unwrap().auth_method = "oauth".into();
        subscription.billing_plane.as_mut().unwrap().kind = "subscription".into();
        planes.push(subscription);
    }
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&planes),
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["concurrency"]["providers"]["synthetic"]["scope"] = json!("account_model");
    });
}

#[tokio::test]
async fn select_ledger_skips_full_prefer_before_caller_slots_without_widening_quality() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    second_model(&harness, false);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    fill(&runner, A, "a", 1).await;
    fill(&runner, B, "b", 1).await;
    for legacy in [false] {
        let mut body = select_body(legacy);
        body[if legacy {
            "concurrencySlots"
        } else {
            "concurrency_slots"
        }] = json!([{"key":"synthetic/other","running":1,"limit":1}]);
        if !legacy {
            body["prefer"] = json!(["synthetic/model"]);
        }
        let reply = consumer.call("route.select", &body).await;
        let selected = response(&reply);
        let text = selected.to_string();
        assert!(text.contains("other"), "{selected}");
        assert!(text.contains("queued on synthetic/other"), "{selected}");
        assert!(text.contains("concurrency_full"), "{selected}");
        assert!(!text.contains("provider_saturated"), "{selected}");
    }
    // A route outside the quality-gated set must not become best effort just
    // because the stronger model is full.
    let mut facts = harness.store().get_facts().unwrap();
    for fact in &mut facts {
        if fact.model_id == "other" {
            fact.scores.elo = 1.0;
            fact.scores.eq = 1.0;
        }
    }
    let credentials = harness.credentials().lock().unwrap().clone();
    harness.install_facts(&facts);
    harness.set_credentials(credentials);
    refused(
        &consumer.call("route.select", &select_body(false)).await,
        false,
        NOW + 30_000,
    );
    // A ladder's blocked entry is not one of the cap counts that caused the
    // concurrency refusal, so its earlier cooldown cannot shorten the wait.
    harness
        .store()
        .update_route_state(
            "synthetic/other",
            "synthetic/other",
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((NOW + 12_000) as f64),
                observed_at_ms: Some(NOW as i64),
                ..Default::default()
            },
        )
        .unwrap();
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!(["synthetic/model", "synthetic/other"]);
    });
    refused(
        &consumer.call("route.select", &select_body(false)).await,
        false,
        NOW + 30_000,
    );
}

#[tokio::test]
async fn select_ledger_records_no_waiter_before_background_admission() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    harness.edit_config(|config| {
        config["consumer_priorities"] = json!({"mason":"blocking","codemode":"background"});
        config["consumers"]["codemode"]["pay_per_token"] = json!(false);
    });
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    fill(&runner, A, "a", 1).await;
    block(&harness, B, (NOW + 100_000) as i64);
    refused(
        &consumer.call("route.select", &select_body(false)).await,
        false,
        NOW + 30_000,
    );
    assert_eq!(
        crate::account_state::snapshot(harness.store())
            .unwrap()
            .waiters
            .live(NOW as i64)
            .count(),
        0
    );
    let started = runner.call("route.account_observe", &json!({"event_id":"start","kind":"admission_started","admission_id":"a-0","run_id":"a-run"})).await;
    assert_eq!(started.0, FrameType::Response);
    let released = runner.call("route.account_observe", &json!({"event_id":"release","kind":"run_finished","admission_id":"a-0","run_id":"a-run"})).await;
    assert_eq!(released.0, FrameType::Response, "{}", released.1);
    let mut body = select_body(false);
    body["consumer"] = json!("codemode");
    let selected = consumer.call("route.select", &body).await;
    let decision = response(&selected)["decision_id"].clone();
    let mut admission = fresh("background");
    admission["decision_id"] = decision;
    assert_eq!(
        admitted_on(&runner, &admission).await["account"]["account_id"],
        A
    );
    refused(
        &consumer.call("route.select", &select_body(false)).await,
        false,
        NOW + 30_000,
    );
}

#[tokio::test]
async fn select_ledger_shadow_preserves_every_fixture_consumers_live_preview_and_logs_once() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, false);
    harness.edit_config(|config| {
        for name in ["mason", "codemode", "flow"] {
            config["consumers"][name]["pay_per_token"] = json!(false);
        }
    });
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    fill(&runner, A, "a", 1).await;
    fill(&runner, B, "b", 1).await;
    for name in ["mason", "codemode", "flow"] {
        let mut body = select_body(false);
        body["consumer"] = json!(name);
        pressure();
        let live = consumer.call("route.select", &body).await;
        assert_eq!(pressure().len(), 1);
        let preview = operator
            .call("route.what_if", &json!({"consumer":name,"iqs":[30]}))
            .await;
        let live = response(&live);
        let preview = response(&preview);
        assert_eq!(
            preview["results"][0]["pick"]["route"],
            format!(
                "{}/{}",
                live["route"]["provider"].as_str().unwrap(),
                live["route"]["model"].as_str().unwrap()
            )
        );
        assert!(pressure().is_empty());
        harness.edit_config(|config| {
            config["concurrency"]["providers"] = json!({});
        });
        let uncapped = consumer.call("route.select", &body).await;
        let uncapped = response(&uncapped);
        assert_eq!(live["route"], uncapped["route"]);
        assert_eq!(live["detail"], uncapped["detail"]);
        assert!(pressure().is_empty());
        caps(&harness, 1, false);
    }
}
