use super::*;

#[test]
fn compact_rows_leave_high_cardinality_identifiers_owned() {
    let row = AccountAdmissionRow {
        principal: "compact-low-principal".into(),
        admission_id: "compact-high-admission".into(),
        request_fingerprint: "compact-high-fingerprint".into(),
        request_mode: "admit".into(),
        decision_id: "compact-high-decision".into(),
        conversation_hash: "0123456789abcdef".repeat(4),
        provider: "compact-low-provider".into(),
        model: "compact-low-model".into(),
        account_id: "compact-low-account".into(),
        credential_id: "compact-low-credential".into(),
        pin_id: "compact-high-pin".into(),
        pin_generation: 1,
        state: "reserved".into(),
        charged: true,
        request_bytes: 10,
        request_bytes_estimated: false,
        committed_at: 1,
        admission_valid_until: 100,
        lease_valid_until: 200,
        reply_json: "{}".into(),
        run_id: Some("compact-high-run".into()),
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
        last_observed_at: None,
    };
    let mut state = super::super::AdmissionState::default();
    state.insert_admission(&row, 1);
    let stored = &state
        .open_admission(&row.principal, &row.admission_id)
        .unwrap()
        .row;
    assert_eq!(expand_admission(stored), row);
    let mut independent = super::super::AdmissionState::default();
    independent.insert_admission(&row, 1);
    assert_eq!(state, independent);
    for text in [
        &row.principal,
        &row.provider,
        &row.model,
        &row.account_id,
        &row.credential_id,
        &row.state,
        &row.request_mode,
    ] {
        assert!(super::super::shared_text::contains(text));
    }
    for text in [
        &row.admission_id,
        &row.pin_id,
        &row.decision_id,
        row.run_id.as_ref().unwrap(),
        &row.request_fingerprint,
        &row.conversation_hash,
    ] {
        assert!(
            !super::super::shared_text::contains(text),
            "high-cardinality field was interned: {text}"
        );
    }
}
