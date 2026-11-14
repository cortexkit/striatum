use super::*;
use crate::concurrency_caps::tests::inputs;
use serde_json::json;

fn google_row(id: &str, model: &str) -> AccountAdmissionRow {
    let mut row = row(id, T);
    row.provider = "google".into();
    row.model = model.into();
    row
}

#[test]
fn cap_model_winner_leaves_shared_count_even_when_unlimited() {
    let (_scratch, store, owner) = fixture("cap-count-split");
    for index in 0..5 {
        let model = if index < 2 { "m1" } else { "m2" };
        reserve(&store, google_row(&index.to_string(), model), T);
    }
    for limit in [json!(2), json!("none")] {
        let caps = inputs("account", json!({"models":{"google/m1":{"limit":limit}}})).resolved;
        let state = owner.published_snapshot().unwrap();
        assert_eq!(
            state
                .inflight
                .count_for_model(&caps, "google", "a", "m1", T),
            2
        );
        assert_eq!(
            state
                .inflight
                .count_for_model(&caps, "google", "a", "m2", T),
            3
        );
        assert_eq!(caps.cap("google", "a", "m1").limit, limit.as_u64());
        assert_eq!(caps.cap("google", "a", "m2").limit, Some(3));
    }
    let caps = inputs("account", json!({"models":{"google/m1":{"limit":2}}})).resolved;
    let persisted = loaded(&store, T);
    assert_eq!(
        persisted
            .inflight
            .count_for_model(&caps, "google", "a", "m2", T),
        3
    );
}

#[test]
fn cap_account_winner_ignores_model_limit_and_assigns_one_count() {
    let (_scratch, store, owner) = fixture("cap-count-account-winner");
    for index in 0..5 {
        reserve(
            &store,
            google_row(&index.to_string(), if index < 3 { "m1" } else { "m2" }),
            T,
        );
    }
    for scope in ["account", "account_model"] {
        let caps = inputs(
            scope,
            json!({
                "models":{"google/m1":{"limit":2}},
                "accounts":{"google/a":{"limit":6}}
            }),
        )
        .resolved;
        let state = owner.published_snapshot().unwrap();
        let count = state
            .inflight
            .count_for_model(&caps, "google", "a", "m1", T);
        assert_eq!(count, if scope == "account" { 5 } else { 3 });
        assert!(
            count < caps.cap("google", "a", "m1").limit.unwrap() as usize,
            "the third run must be below the account override, not the ignored model limit"
        );
        let command_caps = caps.clone();
        assert_eq!(
            run(&store, T, move |tx| Ok(tx.inflight_count_for_model(
                &command_caps,
                "google",
                "a",
                "m1",
                T
            )))
            .unwrap(),
            count
        );
    }
}

#[test]
fn cap_paid_admission_uses_stored_pool_provider_for_its_count() {
    let (_scratch, store, owner) = fixture("cap-count-paid-provider");
    for (index, model) in ["m1", "m1", "m2"].iter().enumerate() {
        let mut row = google_row(&index.to_string(), model);
        row.provider = "anthropic-platform".into();
        reserve(&store, row, T);
    }
    let caps = inputs(
        "account",
        json!({"models":{
            "anthropic-platform/m1":{"limit":2}, "anthropic/m2":{"limit":1}
        }}),
    )
    .resolved;
    let state = owner.published_snapshot().unwrap();
    assert_eq!(
        state
            .inflight
            .count_for_model(&caps, "anthropic-platform", "a", " M1 ", T),
        2
    );
    assert_eq!(
        state
            .inflight
            .count_for_model(&caps, "anthropic-platform", "a", "m2", T),
        1
    );
    assert_eq!(caps.cap("anthropic-platform", "a", "m2").limit, None);
}
