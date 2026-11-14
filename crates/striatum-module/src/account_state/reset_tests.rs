use super::*;
use crate::concurrency_caps::tests::inputs;
use serde_json::json;

fn google(id: &str, now: i64) -> AccountAdmissionRow {
    let mut row = row(id, now);
    row.provider = "google".into();
    row
}

fn next_free(
    owner: &StateOwner,
    caps: &crate::concurrency_caps::ResolvedCaps,
    now: i64,
) -> Option<i64> {
    owner
        .published_snapshot()
        .unwrap()
        .inflight
        .next_free_at(caps, "google", "a", "m1", 1, now)
}

#[test]
fn reset_finds_the_first_fall_below_the_cap_not_the_first_closing_row() {
    let caps = inputs("account", json!({"providers":{"google":{"limit":1}}})).resolved;
    assert_eq!(caps.cap("google", "a", "m1").limit, Some(1));
    let at = T + 48_000;

    // A reservation of its own, and an unused offer's reservation, each free
    // the slot when they expire.
    let (_scratch, store, owner) = fixture("reset-reservation");
    reserve(&store, google("plain", T), T);
    assert_eq!(next_free(&owner, &caps, at), Some(T + MINUTE));
    let (_scratch, store, owner) = fixture("reset-offer");
    let mut original = google("original", T);
    original.account_id = "elsewhere".into();
    reserve(&store, original, T);
    start(&store, "p", "original", "run", T);
    run(&store, T, |tx| {
        let mut reservation = google("offer", T);
        reservation.credential_id = "credential-a".into();
        insert(tx, &reservation)?;
        tx.record_continuation(
            "p",
            "offer",
            &V1Recorded {
                body_json: "{}".into(),
                answer_json: "{}".into(),
            },
            "run",
            "original",
            "decision-original",
            Some((&reservation.decision_id, "offer", T + MINUTE)),
        )
    })
    .unwrap();
    assert_eq!(next_free(&owner, &caps, at), Some(T + MINUTE));

    // Over the cap the first expiry leaves the count at the cap.
    let (_scratch, store, owner) = fixture("reset-overshoot");
    reserve(&store, google("first", T), T);
    reserve(&store, google("second", T + 8_000), T + 8_000);
    assert_eq!(next_free(&owner, &caps, at), Some(T + 8_000 + MINUTE));

    // A renewal closing beside its conversation's started run frees nothing,
    // and a started run observed now holds past any reservation window.
    let (_scratch, store, owner) = fixture("reset-renewal");
    let mut run_row = google("run-row", T);
    run_row.conversation_hash = "conversation".into();
    reserve(&store, run_row, T);
    start(&store, "p", "run-row", "run", T);
    let mut renewal = google("renewal", T);
    renewal.conversation_hash = "conversation".into();
    renewal.request_mode = "renew".into();
    renewal.pin_generation = 2;
    reserve(&store, renewal, T);
    assert_eq!(next_free(&owner, &caps, at), Some(T + 30 * MINUTE));
}
