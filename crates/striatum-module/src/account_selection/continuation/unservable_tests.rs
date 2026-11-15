use super::*;

#[test]
fn stable_refusal_malformed_stored_continuation_is_refused() {
    let body = json!({"mode":"continue", "selection_id":"selection", "run_id":"run",
        "admission_id":"admission", "conversation_key":"conversation", "previous_decision_id":"previous",
        "exhausted":{"credential_id":"oauth:1", "account":"account-1",
            "limit_window":"five_hour", "observed_at_ms":1}});
    let request: ContinueRequest = serde_json::from_value(body.clone()).unwrap();
    let answer =
        json!({"kind":"hold", "routing_code":"account_selection_unavailable", "detail":{}});
    let valid = V1Recorded {
        body_json: body.to_string(),
        answer_json: answer.to_string(),
    };
    assert_eq!(standing(valid.clone(), &request).unwrap(), answer);
    for (field, malformed) in [
        ("body", "{"),
        ("body", "{}"),
        ("answer", "{"),
        ("answer", "{}"),
        ("answer", r#"{"kind":"offer","account":42}"#),
    ] {
        let mut record = valid.clone();
        match field {
            "body" => record.body_json = malformed.into(),
            "answer" => record.answer_json = malformed.into(),
            _ => unreachable!(),
        }
        let refused = standing(record, &request).unwrap();
        assert_eq!(
            refused,
            json!({"kind":"refuse", "routing_code":"account_unservable",
            "detail":{"reason":"stored_state_invalid"}}),
            "{field}: {malformed}"
        );
    }
}
