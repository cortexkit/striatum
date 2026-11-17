use super::*;

const TABLES: &[&str] = &[
    "account_admission",
    "account_affinity",
    "account_pin",
    "account_continue",
    "routing_v1_admission",
    "probe_authorization",
];

fn row_counts(harness: &Harness) -> Vec<i64> {
    harness
        .store()
        .with_reader_for_test(|conn| {
            TABLES
                .iter()
                .map(|table| {
                    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                })
                .collect()
        })
        .unwrap()
}

async fn finish_failure(fault: &str, expected_reason: &str) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup(&["pro_200"], true, true);
    let owner = harness.bind(reserved("tier-owner"), None).await;
    let selected = owner.call("route.select", &select("fast")).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let old = NOW as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS - 1;
    harness
        .store()
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "openai".into(),
                credential_id: "oauth:0".into(),
                code: "billing_exhausted".into(),
            },
            None,
            old,
        )
        .unwrap();
    crate::account_state::initialize(harness.store()).unwrap();
    let before = crate::account_state::snapshot(harness.store()).unwrap();
    let counts = row_counts(&harness);
    let id = format!("finish-{fault}");
    match fault {
        "malformed" => super::super::accounts::inject_finish_answer(&id, |answer| {
            answer["pin"]["generation"] = json!("not-an-integer");
        }),
        "tier" => super::super::accounts::inject_finish_answer(&id, |answer| {
            answer["service_tier_chosen"] = json!("standard");
        }),
        "io" => harness.store().with_writer_for_test(|conn| conn.execute_batch(
            "CREATE TRIGGER fail_public_admission BEFORE INSERT ON routing_v1_admission BEGIN SELECT RAISE(ABORT, 'forced storage failure'); END;"
        )).unwrap(),
        _ => unreachable!(),
    }
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = account_request(&id, None);
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    let expected_code = if fault == "io" {
        "account_selection_unavailable"
    } else {
        "account_unservable"
    };
    assert_eq!(refused.1["code"], expected_code, "{}", refused.1);
    if fault != "io" {
        assert_eq!(refused.1["detail"], json!({"reason":expected_reason}));
    }
    assert_eq!(
        row_counts(&harness),
        counts,
        "no admission, affinity, reservation, pin or probe leaks"
    );
    let after = crate::account_state::snapshot(harness.store()).unwrap();
    assert_eq!(
        after.admissions, before.admissions,
        "no in-memory grant or reservation leaks"
    );
    assert_eq!(after.inflight, before.inflight, "no in-memory slot leaks");
    assert_eq!(after.waiters, before.waiters, "no in-memory waiter leaks");
    assert_eq!(
        after.blocks.authorizations().count(),
        before.blocks.authorizations().count()
    );
    // Preparation classifies the old fence before the admission command. Compare
    // blocks against SQL, not the earlier unclassified publication.
    assert_eq!(
        after.blocks,
        harness
            .store()
            .account_transaction(|tx| tx.load_block_state())
            .unwrap()
    );

    // The same due probe can still be claimed after the failed command.
    if fault == "io" {
        harness
            .store()
            .with_writer_for_test(|conn| conn.execute_batch("DROP TRIGGER fail_public_admission"))
            .unwrap();
    }
    request["admission_id"] = json!(format!("{id}-control"));
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert_eq!(
        admitted.1["result"]["reason"]["code"], "credential_fence_probe",
        "{}",
        admitted.1
    );
    assert!(
        row_counts(&harness)[5] > counts[5],
        "the control must persist a probe authorization"
    );
}

#[tokio::test]
async fn stable_refusal_finish_malformed_answer_rolls_back_sql_and_memory() {
    finish_failure("malformed", "allocator_invariant").await;
}

#[tokio::test]
async fn stable_refusal_finish_bound_tier_mismatch_rolls_back_sql_and_memory() {
    finish_failure("tier", "bound_tier_unavailable").await;
}

#[tokio::test]
async fn finish_store_io_remains_unavailable_and_rolls_back_sql_and_memory() {
    finish_failure("io", "").await;
}
