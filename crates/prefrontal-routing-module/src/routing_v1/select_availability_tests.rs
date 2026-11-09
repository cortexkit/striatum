//! Public select refusals preserve known account reset times so callers can wait.

use cortexkit_role_routing_conformance::{
    fixture,
    wire::{
        errors::Refusal,
        select::{check_answer, parse_select_request_serving, SelectAnswer},
    },
};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::{
    conformance_tests::{reserved, Harness},
    dispatch_tests::configured,
};

const NOW: u64 = 1_800_000_000_000;
const RESET: u64 = NOW + 600_000;
const MODEL: &str = "example-subscription/model-c";

fn fence(harness: &Harness, model: &str, until: u64) {
    for account in fixture::catalog().accounts {
        harness
            .store()
            .record_account_route_fence(&prefrontal_routing_store::AccountRouteFence {
                provider: "example-subscription".into(),
                model: model.into(),
                credential_id: account.credential_id,
                until_ms: until as i64,
                decision_id: "prior-decision".into(),
                recorded_at: NOW as i64,
            })
            .unwrap();
    }
}

fn exhaust_accounts(reset: Option<u64>) {
    let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
    for reading in quota.readings.readings.values_mut() {
        reading.received_at_ms = NOW as i64;
        reading.entry["fetchedAt"] = json!(crate::account_selection::rfc3339(NOW as i64));
        for name in ["primary", "secondary"] {
            if let Some(window) = reading.entry["usage"][name].as_object_mut() {
                window.insert("usedPercent".into(), json!(100.0));
                if let Some(reset) = reset {
                    window.insert(
                        "resetsAt".into(),
                        json!(crate::account_selection::rfc3339(reset as i64)),
                    );
                } else {
                    window.remove("resetsAt");
                }
            }
        }
    }
}

async fn pinned_account_limit(kind: &str, cause: &str, reset: Option<u64>) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = configured().await;
    let mut request =
        json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"hold_wait_max_s":0});
    let pin = match kind {
        "explicit" => {
            request["pin"] = json!(MODEL);
            MODEL
        }
        "slot" => {
            request["pin_slot"] = json!("availability");
            harness.edit_config(|config| {
                config["consumers"]["availability"]["pin"] = json!({"model":MODEL})
            });
            "availability"
        }
        "project" => {
            harness.edit_config(|config| {
                config["consumers"][fixture::SUMMARY]["projects"]["availability-project"] =
                    json!({"pin":MODEL})
            });
            MODEL
        }
        _ => unreachable!(),
    };
    if cause == "fence" {
        fence(&harness, "model-c", reset.unwrap());
    } else {
        exhaust_accounts(reset);
    }
    let route = harness
        .bind(reserved(fixture::CONSUMER), Some("availability-project"))
        .await;
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{kind}/{cause}: {reply:?}");
    let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
    assert_eq!(
        refusal.code.as_str(),
        "pin_unavailable",
        "{kind}/{cause}: {refusal:?}"
    );
    let mut detail = json!({"pin":pin,"reason":"quota_exhausted"});
    if let Some(reset) = reset {
        detail["reset_at_ms"] = json!(reset);
    }
    assert_eq!(
        Value::Object(refusal.detail.clone()),
        detail,
        "{kind}/{cause}"
    );
    assert_eq!(refusal.wait_until_ms(), reset);
}

#[tokio::test]
async fn explicit_pin_preserves_account_limit_and_fence_reset() {
    for cause in ["quota", "fence"] {
        pinned_account_limit("explicit", cause, Some(RESET)).await;
    }
}

#[tokio::test]
async fn slot_pin_preserves_account_limit_and_fence_reset() {
    for cause in ["quota", "fence"] {
        pinned_account_limit("slot", cause, Some(RESET)).await;
    }
}

#[tokio::test]
async fn project_pin_preserves_account_limit_and_fence_reset() {
    for cause in ["quota", "fence"] {
        pinned_account_limit("project", cause, Some(RESET)).await;
    }
}

#[tokio::test]
async fn unknown_account_reset_keeps_each_pin_untimed() {
    for kind in ["explicit", "slot", "project"] {
        pinned_account_limit(kind, "quota", None).await;
    }
}

#[tokio::test]
async fn every_ladder_entry_fenced_reports_the_earliest_reset() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["consumers"][fixture::SUMMARY]["mode"] = json!("ladder");
        config["consumers"][fixture::SUMMARY]["ladder"] =
            json!([MODEL, "example-subscription/model-d"]);
    });
    fence(&harness, "model-c", RESET + 300_000);
    fence(&harness, "model-d", RESET);
    let reply = route
        .call(
            "route.select",
            &json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"hold_wait_max_s":0}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    let refusal: Refusal = serde_json::from_value(reply.1).unwrap();
    assert_eq!(refusal.code.as_str(), "no_route");
    assert_eq!(
        Value::Object(refusal.detail.clone()),
        json!({"reason":"accounts_blocked","reset_at_ms":RESET})
    );
    assert_eq!(refusal.wait_until_ms(), Some(RESET));
}

#[tokio::test]
async fn fenced_pin_fallback_is_a_conforming_public_answer() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, route) = configured().await;
    fence(&harness, "model-c", RESET);
    let request = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"pin":MODEL,"pin_fallback":"route"});
    let reply = route.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let answer: SelectAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
    let request = parse_select_request_serving(&request, super::describe::CAPABILITIES).unwrap();
    check_answer(&request, &answer).unwrap();
    assert_eq!(reply.1["result"]["detail"]["pin_reason"], "quota_exhausted");
}
