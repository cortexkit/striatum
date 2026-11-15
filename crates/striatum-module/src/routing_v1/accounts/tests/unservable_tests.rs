use super::*;
use cortexkit_role_routing::refusals::{retry_class, RetryClass};

#[test]
fn stable_refusal_malformed_allocator_answer_is_unservable() {
    let request = parse_account_for_request_serving(&json!({
        "provider":"openai", "model":"model", "conversation_key":"conversation", "admission_id":"one"
    }), super::super::super::describe::CAPABILITIES).unwrap();
    let valid = json!({
        "admission":{"state":"reserved"},
        "account":{"provider":"openai","account_id":"account-1","credential_id":"oauth:1","method":"oauth"},
        "pin":{"id":"pin-1","generation":1,
            "affinity_expires_at":"1970-01-01T00:00:01Z",
            "admission_valid_until":"1970-01-01T00:00:02Z",
            "lease_valid_until":"1970-01-01T00:00:03Z"},
        "reason":{"code":"headroom","decision_id":"decision-1","policy_version":1}
    });
    assert!(admission_answer(&valid, &request).is_ok());
    for pointer in [
        "/admission/state",
        "/account/provider",
        "/account/account_id",
        "/account/credential_id",
        "/pin/id",
        "/pin/generation",
        "/pin/affinity_expires_at",
        "/pin/admission_valid_until",
        "/pin/lease_valid_until",
        "/reason/decision_id",
    ] {
        let mut malformed = valid.clone();
        *malformed.pointer_mut(pointer).unwrap() = Value::Null;
        let refusal = admission_answer(&malformed, &request).unwrap_err();
        assert_eq!(
            refusal.code,
            RefusalCode::AccountUnservable,
            "{pointer}: {refusal}"
        );
        assert_eq!(
            json!(refusal.detail),
            json!({"reason":"allocator_invariant"})
        );
        assert_eq!(refusal.wait_until_ms(), None);
        assert_eq!(
            retry_class(refusal.code.as_str(), &json!(refusal.detail)),
            RetryClass::Permanent
        );
    }
}

#[test]
fn stable_refusal_legacy_no_eligible_account_is_unservable() {
    let refusal = account_refusal("no_eligible_account", "no eligible account".into(), None);
    assert_eq!(refusal.code, RefusalCode::AccountUnservable);
    assert_eq!(
        json!(refusal.detail),
        json!({"reason":"no_eligible_account"})
    );
    assert_eq!(refusal.wait_until_ms(), None);
    assert_eq!(
        retry_class(refusal.code.as_str(), &json!(refusal.detail)),
        RetryClass::Permanent
    );
    for code in ["store_error", "internal_error", "unscoped_caller"] {
        assert_eq!(
            account_refusal(code, "temporary or unclassified".into(), None).code,
            RefusalCode::AccountSelectionUnavailable
        );
    }
}
