use super::*;
use rusqlite::{types::ValueRef, Connection};

fn samples() -> Value {
    serde_json::from_str(include_str!("../../fixtures/served-payloads.json")).unwrap()
}

pub(crate) fn live_shaped_reply() -> Vec<u8> {
    let samples = samples();
    let mut payload = samples["catalog.get — one model, current"]["response"].clone();
    let row = payload["models"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    payload["models"] = Value::Object(
        (0..12_193)
            .map(|i| {
                (
                    format!(
                        "{}/claude-sonnet-4-5-{i}",
                        if i < 852 {
                            "anthropic"
                        } else {
                            "unmapped-provider"
                        }
                    ),
                    row.clone(),
                )
            })
            .collect(),
    );
    payload["catalog_version"] = json!(0);
    serde_json::to_vec(&payload).unwrap()
}

// Compare the bytes of every SQL value, not only the subset projected into
// ModelFact. Include types and lengths so NULL, empty strings and blobs differ.
fn store_bytes(store: &SqliteRouteStateStore) -> Vec<u8> {
    store
        .with_writer_for_test(|conn| {
            let tables = conn
                .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let mut bytes = Vec::new();
            for table in tables {
                bytes.extend_from_slice(table.as_bytes());
                bytes.push(0);
                let mut query =
                    conn.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
                let columns = query.column_count();
                let mut rows = query
                    .query_map([], |row| {
                        let mut bytes = Vec::new();
                        for column in 0..columns {
                            match row.get_ref(column)? {
                                ValueRef::Null => bytes.push(0),
                                ValueRef::Integer(value) => {
                                    bytes.push(1);
                                    bytes.extend_from_slice(&value.to_le_bytes());
                                }
                                ValueRef::Real(value) => {
                                    bytes.push(2);
                                    bytes.extend_from_slice(&value.to_bits().to_le_bytes());
                                }
                                ValueRef::Text(value) | ValueRef::Blob(value) => {
                                    bytes.push(
                                        if matches!(row.get_ref(column)?, ValueRef::Text(_)) {
                                            3
                                        } else {
                                            4
                                        },
                                    );
                                    bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                                    bytes.extend_from_slice(value);
                                }
                            }
                        }
                        Ok(bytes)
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                rows.sort();
                for row in rows {
                    bytes.extend_from_slice(&(row.len() as u64).to_le_bytes());
                    bytes.extend(row);
                }
            }
            Ok(bytes)
        })
        .unwrap()
}

fn compare_applied(bytes: Vec<u8>, expected_rows: usize) {
    let replay = striatum_store::replay::Inputs::new(1_800_000_000_000);
    let _clock = replay.install();
    let legacy = Arc::new(decode_catalog_get_legacy(&bytes).unwrap());
    let current = decode_catalog_get(bytes).unwrap();
    assert_eq!(
        *current, *legacy,
        "every fact, rate and billing input must match"
    );
    let scratch = cortexkit_test_support::ScratchDir::new("catalog-state-equivalence");
    let open = |name: &str| {
        SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
            &format!("catalog-eq-{expected_rows}"),
            scratch.path().join(name).to_string_lossy(),
        ))
        .unwrap()
    };
    let seed = open("seed.db");
    seed.with_writer_for_test(|conn: &Connection| {
        conn.execute_batch("DELETE FROM model_facts; DELETE FROM model_route_state;")
    })
    .unwrap();
    drop(seed);
    // Copy one initialized store, including migration/seed timestamps, so the
    // comparison has identical inputs and does not mask any changed SQL values.
    for name in ["before.db", "after.db"] {
        std::fs::copy(scratch.path().join("seed.db"), scratch.path().join(name)).unwrap();
    }
    let apply = |name: &str, snapshot: &Arc<CatalogSnapshot>| {
        let store = open(name);
        apply_mapped_catalog(&store, snapshot, None, Some("anthropic")).unwrap();
        assert_eq!(store.get_catalog_facts().unwrap().len(), expected_rows);
        store_bytes(&store)
    };
    let before = apply("before.db", &legacy);
    let after = apply("after.db", &current);
    assert_eq!(after.len(), before.len());
    assert!(
        after == before,
        "all applied store values must be byte-identical; first difference at {:?}",
        after.iter().zip(&before).position(|(a, b)| a != b)
    );
}

#[test]
fn live_shaped_applied_store_is_byte_identical_to_legacy_decode() {
    compare_applied(live_shaped_reply(), 852);
}

#[test]
fn unreadable_rows_among_good_rows_match_legacy_snapshot_and_store() {
    let samples = samples();
    let mut payload = samples["catalog.get — one model, current"]["response"].clone();
    let good = payload["models"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    payload["quality_scores"] = json!({"graded_by":"operator", "graded_at_ms":42});
    payload["models"] = json!({
        "anthropic/good": good,
        "anthropic/bad-existence": {"existence":"unrecognised", "quality.elo":9, "quality.eq":8},
        "anthropic/bad-rate": {"existence":"present", "rate.input":{"state":"priced", "currency":"USD"}},
        "anthropic/bad-shape": [1, 2],
        "not-a-route": {"existence":"present", "quality.decommissioned":true}
    });
    compare_applied(serde_json::to_vec(&payload).unwrap(), 1);
}

#[test]
fn all_golden_catalog_cases_match_legacy_decode() {
    for (name, case) in samples().as_object().unwrap() {
        if !name.starts_with("catalog.get") {
            continue;
        }
        let bytes = serde_json::to_vec(&case["response"]).unwrap();
        match (
            decode_catalog_get_legacy(&bytes),
            decode_catalog_get(&bytes),
        ) {
            (Ok(before), Ok(after)) => assert_eq!(before, *after, "{name}"),
            (Err(_), Err(_)) => {}
            (before, after) => panic!("validation changed for {name}: {before:?} / {after:?}"),
        }
    }
}

#[test]
fn duplicate_top_level_fields_keep_the_last_value_like_legacy_decode() {
    let bytes = br#"{"catalog_version":"bad first value","models":null,"catalog_version":17,"models":{"anthropic/good":{"existence":"present"}},"resolved_at_ms":42}"#;
    assert_eq!(
        decode_catalog_get_legacy(bytes).unwrap(),
        *decode_catalog_get(bytes).unwrap()
    );
}

#[test]
fn bare_decode_ignores_unused_page_fields_like_legacy_decode() {
    let bytes = br#"{"catalog_version":17,"models":{},"resolved_at_ms":42,"source":false,"next_page_token":42,"withheld":{},"uncertain":null,"aliased":false}"#;
    assert_eq!(
        decode_catalog_get_legacy(bytes).unwrap(),
        *decode_catalog_get(bytes).unwrap()
    );
    assert!(decode::CatalogPage::read(bytes, true).is_err());
}

#[test]
fn duplicate_model_keys_keep_only_the_last_row_like_legacy_decode() {
    let bytes = br#"{"catalog_version":17,"models":{"anthropic/good":{"existence":"unreadable"},"anthropic/good":{"existence":"present"}},"resolved_at_ms":42}"#;
    assert_eq!(
        decode_catalog_get_legacy(bytes).unwrap(),
        *decode_catalog_get(bytes).unwrap()
    );
}

#[test]
fn refusal_probe_reads_only_the_error_and_preserves_wire_outcomes() {
    for (bytes, error_frame, expected) in [
        (
            r#"{"result":{"error":{"code":"catalog_changed","message":"restart"}}}"#,
            false,
            Some(("catalog_changed", "restart")),
        ),
        (
            r#"{"result":{"code":"no_coverage"}}"#,
            true,
            Some(("no_coverage", "")),
        ),
        (
            r#"{"code":"no_coverage","message":false}"#,
            true,
            Some(("no_coverage", "")),
        ),
        (r#"{"code":42}"#, true, None),
        (r#"{"error":null,"code":"not-a-refusal"}"#, true, None),
        (
            r#"{"catalog_version":17,"models":{"code":"not-an-error"}}"#,
            false,
            None,
        ),
    ] {
        assert_eq!(
            decode::refusal(bytes.as_bytes(), error_frame),
            expected.map(|(code, message)| (code.to_owned(), message.to_owned()))
        );
    }
}

#[test]
fn publication_shares_snapshot_and_preserves_held_old_reader() {
    let scratch = cortexkit_test_support::ScratchDir::new("catalog-sharing");
    let store = SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
        "catalog-sharing",
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    let credentials = Arc::new(Mutex::new(
        crate::broca_credentials::BrocaCredentials::default(),
    ));
    let bytes =
        serde_json::to_vec(&samples()["catalog.get — one model, current"]["response"]).unwrap();
    let old = decode_catalog_get(&bytes).unwrap();
    apply_mapped_catalog(&store, &old, Some(&credentials), Some("anthropic")).unwrap();
    assert!(Arc::ptr_eq(
        credentials
            .lock()
            .unwrap()
            .catalog_snapshot
            .as_ref()
            .unwrap(),
        &old
    ));
    let reader = credentials.lock().unwrap().snapshot();
    let next = decode_catalog_get(&bytes).unwrap();
    apply_mapped_catalog(&store, &next, Some(&credentials), Some("anthropic")).unwrap();
    assert!(Arc::ptr_eq(
        credentials
            .lock()
            .unwrap()
            .catalog_snapshot
            .as_ref()
            .unwrap(),
        &next
    ));
    assert!(Arc::ptr_eq(reader.catalog_snapshot.as_ref().unwrap(), &old));
    assert_eq!(*old, *next);
    assert!(decode_catalog_get(b"{\"result\":{\"models\":null}}".as_slice()).is_err());
    assert!(Arc::ptr_eq(
        credentials
            .lock()
            .unwrap()
            .catalog_snapshot
            .as_ref()
            .unwrap(),
        &next
    ));
    let base = base_rates_after(BTreeMap::new(), &next);
    for (key, rates) in &next.rates {
        assert!(Arc::ptr_eq(&base[key], rates));
    }
}
