use serde_json::{json, Value};

use super::Harness;

pub(in crate::routing_v1) fn cleared_catalog_fingerprint(harness: &Harness) -> String {
    let sql = rusqlite::Connection::open(harness.store_path()).unwrap();
    let old_defaults: i64 = sql.query_row(
        "SELECT count(*) FROM model_facts WHERE json_type(prompt_defaults_json, '$.variant') IS NOT NULL",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(old_defaults, 0, "the replay must use the migrated catalog");
    harness.store().catalog_fingerprint().unwrap()
}

/// Apply named deltas to the independent historical state, never to the live
/// observation. Only the decision rows listed in the delta file receive these
/// changes; a row the replay captures that isn't on the list is compared
/// unchanged.
pub(super) fn apply(file: &str, expected: &mut Value, fingerprint: &str) {
    let (paths, old_fingerprint, elo, eq) = match file {
        "availability" => (
            include_str!("../../../fixtures/replay/availability.effort-delta.txt"),
            "6d44b9223a879429",
            1800.0,
            1800.0,
        ),
        "report-outcomes" => (
            include_str!("../../../fixtures/replay/report-outcomes.effort-delta.txt"),
            "6d44b9223a879429",
            1800.0,
            1800.0,
        ),
        "tier-consumers" => (
            include_str!("../../../fixtures/replay/tier-consumers.effort-delta.txt"),
            "09e98df5b323dee0",
            55.0,
            1700.0,
        ),
        _ => panic!("no archived effort delta for {file}"),
    };
    assert_ne!(old_fingerprint, fingerprint);
    for line in paths.lines().filter(|line| !line.starts_with('#')) {
        let [pointer, digests, efforts]: [&str; 3] = line
            .split_whitespace()
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let rows = expected
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("missing archived path {pointer}"));
        let indices = |list: &str| {
            list.split(',')
                .filter(|index| *index != "-")
                .map(|index| index.parse::<usize>().unwrap())
                .collect::<Vec<_>>()
        };
        let digests = indices(digests);
        // The digest hashes the stored prompt defaults, so clearing them
        // changes it on both selected and refused decisions.
        for &index in &digests {
            let value = rows[index].get_mut("catalog_fingerprint").unwrap();
            assert_eq!(*value, old_fingerprint, "{pointer}/{index}");
            *value = json!(fingerprint);
        }
        // These fixtures set no workload and have no graded or vendor effort.
        // selected models use the plain fixture grades at an unrecorded effort.
        for index in indices(efforts) {
            assert!(digests.contains(&index), "{pointer}/{index}");
            let requirements = rows[index]["requirements_json"].as_object_mut().unwrap();
            for (key, value) in [
                ("variant", Value::Null),
                ("effort_source", json!("none")),
                (
                    "grade",
                    json!({"elo": elo, "eq": eq, "elo_effort":"unrecorded", "eq_effort":"unrecorded"}),
                ),
            ] {
                assert!(
                    requirements.insert(key.into(), value).is_none(),
                    "{pointer}/{index}: {key}"
                );
            }
        }
    }
}

#[test]
fn an_unlisted_expected_state_mutation_survives_the_effort_delta() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/replay/tier-consumers.native-096-state.golden.json"
    ))
    .unwrap();
    let mut expected = json!({"rows": (0..4).map(|i| json!({"state": fixture["frames"][format!("$.rows[{i}].state")]})).collect::<Vec<_>>()});
    let mut observed = expected.clone();
    apply("tier-consumers", &mut observed, "new-cleared-fingerprint");
    let unrelated = "/rows/0/state/model_route_decisions/0/id";
    *expected.pointer_mut(unrelated).unwrap() = json!("planted-unlisted-id");
    apply("tier-consumers", &mut expected, "new-cleared-fingerprint");
    assert_eq!(super::first_difference("$", &observed, &expected),
        "$.rows[0].state.model_route_decisions[0].id: actual \"dec_000000000000\", expected \"planted-unlisted-id\"");
    assert_ne!(observed, expected);
    *expected.pointer_mut(unrelated).unwrap() = observed.pointer(unrelated).unwrap().clone();
    assert_eq!(observed, expected);
}
