//! Consumer settings pins use hard pin admission, not automatic ranking.

use std::sync::Arc;

use cortexkit_role_routing::select::{
    check_answer_serving, parse_select_request_serving, SelectAnswer,
};
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::super::conformance_tests::{
    install_catalog, install_panel_families, reserved, Harness, Route,
};
use super::super::describe;

#[path = "consumer_pin_tests/tests.rs"]
mod samples;

const CHAIR: &str = "prefrontal.athena.synthesizer";
const OPUS: &str = "anthropic/claude-opus-5-5";
const FABLE: &str = "anthropic/claude-fable-5-2";

async fn configured(configured_consumer: &str) -> (Arc<Harness>, Route) {
    let harness = Harness::new("consumer-pin");
    let mut catalog = fixture::catalog();
    let original = catalog.models[0].clone();
    catalog.models = ["claude-opus-5-5", "claude-fable-5-2"]
        .into_iter()
        .map(|name| {
            let mut model = original.clone();
            model.provider = "anthropic".into();
            model.model = name.into();
            model.iq = 90;
            model.family = Some("claude".into());
            model
        })
        .collect();
    for runner in &mut catalog.runners {
        runner.route_keys = vec![OPUS.into(), FABLE.into()];
    }
    install_catalog(&harness, &catalog);
    install_panel_families(&harness, &catalog);
    harness.edit_config(|config| {
        config["consumers"] = json!({"prefrontal.athena":{"owner":"prefrontal"}});
        config["consumers"][configured_consumer] = json!({
            "owner":"prefrontal", "pin":{"model":OPUS,"variant":"medium"}
        });
        config["consumers"]["prefrontal.athena"] = json!({"owner":"prefrontal"});
        config["models"][OPUS]["pin_only_for"] = json!(["prefrontal.athena"]);
        config["models"][OPUS]["subscription"] = json!(true);
        config["models"][FABLE]["subscription"] = json!(true);
        config["panels"]["audit"] = json!({"top_k":1,"by":"iq","extra":0,"floor":0});
    });
    let route = harness.bind(reserved("prefrontal"), None).await;
    (harness, route)
}

fn request(consumer: &str) -> Value {
    json!({"consumer":consumer,"requirements":{"iq":50}})
}

async fn answered(route: &Route, params: &Value) -> Value {
    let reply = route.call("route.select", params).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let answer = reply.1["result"].clone();
    let parsed = parse_select_request_serving(params, describe::CAPABILITIES).unwrap();
    let typed: SelectAnswer = serde_json::from_value(answer.clone()).unwrap();
    check_answer_serving(&parsed, &typed, describe::CAPABILITIES).unwrap();
    answer
}

fn assert_chair(answer: &Value) {
    assert_eq!(
        answer["route"],
        json!({"provider":"anthropic","model":"claude-opus-5-5","variant":"medium"}),
        "{answer}"
    );
    assert_eq!(answer["runner"]["model"], answer["route"], "{answer}");
    // The wire contract reserves pin_outcome for a pin carried by the request.
    assert!(answer.get("pin_outcome").is_none(), "{answer}");
    assert!(
        answer["explanation"]
            .as_str()
            .unwrap()
            .contains("consumer's operator pin"),
        "{answer}"
    );
}

#[tokio::test]
async fn chair_setting_reaches_legacy_panel_policy() {
    let (harness, _) = configured("prefrontal.athena.synthesizer").await;
    let route = harness.bind(reserved("prefrontal"), None).await;
    assert_chair(&answered(&route, &request("prefrontal.athena.synthesizer")).await);
    let reply = route.call("route.select_panel", &json!({"panelSpec":{"class":"AUDIT"},"substrate":"broca","availableModels":[OPUS,FABLE],"packageTokens":1000})).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(
        reply.1["result"]["policy"]["pins"]["synthesizer"],
        json!({"model":OPUS,"variant":"medium"})
    );
}

#[tokio::test]
async fn settings_pin_selects_exact_model_and_variant_through_hard_admission() {
    let (harness, route) = configured(CHAIR).await;
    for pin_only in [false, true] {
        harness.edit_config(|config| config["models"][OPUS]["pin_only"] = json!(pin_only));
        let answer = answered(&route, &request(CHAIR)).await;
        assert_chair(&answer);
        let binding = harness
            .store()
            .v1_decision(answer["decision_id"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(binding.consumer, CHAIR);
        assert_eq!(binding.variant.as_deref(), Some("medium"));
    }
}

#[tokio::test]
async fn settings_pin_inherits_operator_ancestors() {
    let (harness, route) = configured(CHAIR).await;
    harness.edit_config(|config| {
        config["consumers"][CHAIR]
            .as_object_mut()
            .unwrap()
            .remove("pin");
        config["consumers"]["prefrontal"]["pin"] = json!({"model":OPUS,"variant":"medium"});
    });
    assert_chair(&answered(&route, &request(CHAIR)).await);
}

#[tokio::test]
async fn request_pin_overrides_consumer_settings_pin() {
    let (_harness, route) = configured(CHAIR).await;
    let mut params = request(CHAIR);
    params["pin"] = json!(format!("{FABLE}:high"));
    let answer = answered(&route, &params).await;
    assert_eq!(
        answer["route"],
        json!({"provider":"anthropic","model":"claude-fable-5-2","variant":"high"})
    );
    assert_eq!(answer["pin_outcome"], "pinned");
}

#[tokio::test]
async fn not_for_still_refuses_inherited_consumer_settings_pin() {
    let (harness, route) = configured(CHAIR).await;
    let child = format!("{CHAIR}.blocked");
    harness.edit_config(|config| {
        // The parent's pin is valid at load, but its child has a stricter fence.
        config["models"][OPUS]["not_for"] = json!([child]);
    });
    let reply = route.call("route.select", &request(&child)).await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert_eq!(reply.1["code"], "pin_unavailable", "{reply:?}");
    assert_eq!(reply.1["detail"]["reason"], "excluded", "{reply:?}");
}

#[tokio::test]
async fn unservable_settings_pin_refuses_without_substitution() {
    let (harness, route) = configured(CHAIR).await;
    harness.edit_config(|config| {
        config["consumers"][CHAIR]["pin"]["model"] = json!("anthropic/missing");
    });
    let reply = route.call("route.select", &request(CHAIR)).await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert_eq!(reply.1["code"], "pin_unavailable", "{reply:?}");
    assert_eq!(
        reply.1["detail"]["pin"], "anthropic/missing:medium",
        "{reply:?}"
    );
    assert_eq!(reply.1["detail"]["reason"], "unknown_model", "{reply:?}");
}

#[tokio::test]
async fn opus_stays_out_of_automatic_athena_panel_seats() {
    let (_harness, route) = configured(CHAIR).await;
    let params = json!({"class":"audit","consumer":"prefrontal.athena","available_routes":[OPUS,FABLE],"temperatures":[0.2],"seed":"fixed"});
    let reply = route.call("route.select_panel", &params).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let answer: cortexkit_role_routing::panel::SelectPanelAnswer =
        serde_json::from_value(reply.1["result"].clone()).unwrap();
    assert_eq!(answer.seats.len(), 1, "{reply:?}");
    assert_eq!(
        answer.seats[0].answer.route.model, "claude-fable-5-2",
        "{reply:?}"
    );
}

#[tokio::test]
async fn canonical_athena_chair_pin_reaches_panel_and_picker_policy_view() {
    let (harness, panel_caller) = configured(CHAIR).await;
    let picker = harness.bind(reserved("occipital"), None).await;
    // Model pickers read the configured synthesizer pin from the panel policy
    // view; route.head_choices lists models, not configured pins.
    let request = json!({"panelSpec":{"class":"AUDIT"}, "substrate":"broca",
        "availableModels":[OPUS,FABLE], "packageTokens":1000});
    for (caller, principal) in [
        (&panel_caller, "reserved:prefrontal"),
        (&picker, "reserved:occipital"),
    ] {
        crate::take_operator_infos();
        let reply = caller.call("route.select_panel", &request).await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        assert_eq!(
            reply.1["result"]["policy"]["pins"]["synthesizer"],
            json!({"model":"anthropic/claude-opus-5-5","variant":"medium"}),
            "{reply:?}"
        );
        let logs: Vec<_> = crate::take_operator_infos()
            .into_iter()
            .filter(|line| line.starts_with("[routing] legacy panel call "))
            .collect();
        assert_eq!(
            logs,
            [format!("[routing] legacy panel call principal={principal}")]
        );
    }
    let config = harness.operator_config();
    let pin = config
        .slot_pin(crate::config_policy::ATHENA_SYNTHESIZER_CONSUMER)
        .unwrap();
    assert_eq!(pin.model, OPUS);
    assert_eq!(pin.variant.as_deref(), Some("medium"));
    assert!(config.slot_pin("synthesizer").is_none());
}
