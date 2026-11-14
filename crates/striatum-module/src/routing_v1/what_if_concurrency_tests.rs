use super::*;
use striatum_store::{AccountAdmissionRow, AccountCommandKind, AccountPinRow, V1Recorded};

const T: i64 = 1_800_000_000_000;
const PROVIDER: &str = "example-provider";

fn reservation(id: &str, account: &str, model: &str) -> AccountAdmissionRow {
    AccountAdmissionRow {
        principal: "p".into(),
        admission_id: id.into(),
        request_fingerprint: "fp".into(),
        request_mode: "admit".into(),
        decision_id: format!("decision-{id}"),
        conversation_hash: format!("conversation-{id}"),
        provider: PROVIDER.into(),
        model: model.into(),
        account_id: account.into(),
        credential_id: format!("credential-{account}"),
        pin_id: format!("pin-{id}"),
        pin_generation: 1,
        state: "reserved".into(),
        charged: true,
        request_bytes: 1,
        request_bytes_estimated: false,
        committed_at: T,
        admission_valid_until: T + 60_000,
        lease_valid_until: T + 120_000,
        reply_json: "{}".into(),
        run_id: None,
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
        last_observed_at: None,
    }
}

fn seed(harness: &Harness) {
    crate::account_writer::execute(&harness.store(), AccountCommandKind::Repair, |tx| {
        for (id, account, model, run) in [
            ("source", "shared", "model-a", Some("moving-run")),
            ("same-run", "shared", "model-b", Some("moving-run")),
            ("pending", "shared", "model-a", None),
            ("model-a-run", "modeled", "model-a", Some("two-model-run")),
            ("model-b-run", "modeled", "model-b", Some("two-model-run")),
            ("model-b-pending", "modeled", "model-b", None),
            ("uncapped-run", "uncapped", "model-a", Some("uncapped-run")),
            ("withdrawn", "uncapped", "withdrawn-model", None),
            ("offer", "modeled", "model-a", None),
        ] {
            let mut row = reservation(id, account, model);
            if id == "offer" {
                row.admission_valid_until = T + 12_000;
            }
            tx.insert_pin(&AccountPinRow {
                pin_id: row.pin_id.clone(),
                principal: row.principal.clone(),
                conversation_hash: row.conversation_hash.clone(),
                provider: row.provider.clone(),
                pin_generation: 1,
                affinity_model: row.model.clone(),
                account_id: row.account_id.clone(),
                credential_id: row.credential_id.clone(),
                pin_mode: "conversation_lease".into(),
                previous_pin_id: None,
                decision_id: row.decision_id.clone(),
                created_at: T,
            })?;
            tx.insert_admission(&row)?;
            if let Some(run) = run {
                tx.mark_admission_started("p", id, run, T, false)?;
            }
        }
        tx.record_continuation(
            "p",
            "offer-selection",
            &V1Recorded {
                body_json: "{}".into(),
                answer_json: "{}".into(),
            },
            "moving-run",
            "source",
            "decision-source",
            Some(("decision-offer", "offer", T + 12_000)),
        )
    })
    .unwrap();
}

fn cap_counts(answer: &Value, account: &str) -> Value {
    assert!(answer["concurrency"]["accounts"].is_array(), "{answer}");
    answer["concurrency"]["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["provider"] == PROVIDER && row["account_id"] == account)
        .unwrap_or_else(|| panic!("missing account {account}: {answer}"))["cap_counts"]
        .clone()
}

#[tokio::test]
async fn what_if_reports_ledger_cap_counts_with_either_switch_without_writes() {
    let _clock = crate::InjectedClock::set(T as u64);
    let (harness, _) = configured().await;
    let mut catalog = fixture::catalog();
    let template = catalog.accounts[0].clone();
    catalog.accounts = ["shared", "modeled", "uncapped"]
        .into_iter()
        .map(|id| {
            let mut account = template.clone();
            account.provider = PROVIDER.into();
            account.account_id = id.into();
            account.credential_id = format!("credential-{id}");
            account.method = "apikey".into();
            account.serves = vec![PROVIDER.into()];
            account
        })
        .collect();
    super::super::conformance_tests::install_catalog(&harness, &catalog);
    harness.edit_config(|config| {
        config["concurrency"] = json!({"accounts":{
            "example-provider/shared":{"limit":1,"scope":"account"},
            "example-provider/modeled":{"limit":1,"scope":"account_model"}
        }})
    });
    seed(&harness);
    let direct = operator(&harness).await;
    let before = rows(&harness);
    let writes = harness.store().account_write_transaction_count_for_test();
    let source_reads = crate::account_state::continuation_source_reads();
    let mut baseline = None;
    for enabled in [None, Some(false), Some(true)] {
        if let Some(enabled) = enabled {
            harness.edit_config(|config| {
                config["concurrency"]["enforcement_enabled"] = json!(enabled)
            });
        }
        let answer = evaluate(&direct, json!({"iqs":[0]})).await;
        assert_eq!(
            answer["concurrency"]["enforcement_enabled"],
            enabled.unwrap_or(false)
        );
        assert_eq!(
            cap_counts(&answer, "shared"),
            json!([
                {"scope":"account","model":null,"effective_cap":1,"holds_in_flight":2}
            ])
        );
        assert_eq!(
            cap_counts(&answer, "modeled"),
            json!([
                {"scope":"account","model":null,"effective_cap":null,"holds_in_flight":0},
                {"scope":"account_model","model":"model-a","effective_cap":1,"holds_in_flight":2},
                {"scope":"account_model","model":"model-b","effective_cap":1,"holds_in_flight":2}
            ])
        );
        assert_eq!(
            cap_counts(&answer, "uncapped"),
            json!([
                {"scope":"account","model":null,"effective_cap":null,"holds_in_flight":2}
            ])
        );
        // A full ledger is diagnostic only until enforcement is connected.
        assert!(answer["results"][0]["pick"].is_object(), "{answer}");
        if let Some(baseline) = &baseline {
            assert_eq!(&answer["results"], baseline);
        } else {
            baseline = Some(answer["results"].clone());
        }
        assert_eq!(rows(&harness), before);
        assert_eq!(
            harness.store().account_write_transaction_count_for_test(),
            writes
        );
        assert_eq!(
            crate::account_state::continuation_source_reads(),
            source_reads
        );
    }
    // The source run stays on its account while an unused offer's reservation
    // closes exactly at its deadline; no writer command is needed for lapse.
    let _later = crate::InjectedClock::set((T + 12_000) as u64);
    let answer = evaluate(&direct, json!({"iqs":[0]})).await;
    assert_eq!(cap_counts(&answer, "shared")[0]["holds_in_flight"], 2);
    assert_eq!(cap_counts(&answer, "modeled")[1]["holds_in_flight"], 1);
    assert_eq!(cap_counts(&answer, "modeled")[2]["holds_in_flight"], 2);
    assert_eq!(rows(&harness), before);
}
