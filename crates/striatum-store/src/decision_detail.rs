//! Full and compact scoring detail in the route-decision ledger.
//!
//! A selection stores the scorer's whole view: `score_json` lists every
//! catalog candidate with its quality inputs (about 134 KB a decision on a
//! live install) and `quota_signal_json` lists every capacity's quota pressure
//! (about 16 KB). Together they were most of the store. After the full-detail
//! window the retention pass rewrites both into a compact summary and stamps
//! `detail_stripped_at`; the rest of the row (requirements, picked route,
//! rejections, explanation, outcome, policy revision) is never touched.
//!
//! The compact `score_json` keeps every member of the full one except the
//! candidate list, which it narrows to the picked route, the routes that also
//! cleared the quality gate (`alsoAdequate`) and, for a panel decision (one
//! that picks several models at once), the route keys its `explanation_json`
//! lists under `members`. So the
//! paths readers use keep resolving on a compacted row: `alsoAdequate`, the
//! picked route's `metrics` (its fit), `objective` (cost, quota, selection
//! cost and `taskPricing` with its `valueMultiplier`), `adequacy` and
//! `pricingMode`. `candidatesOmitted` counts the candidates it dropped. The
//! compact `quota_signal_json` keeps the summary header (state, availability,
//! refresh time) and replaces the per-capacity `signals` list with
//! `signalsOmitted`, its length.
//!
//! A deterministic sample keeps full detail for the whole decision window, so
//! offline calibration can replay it: a decision is in the sample when the
//! FNV-1a 64-bit hash of its id's UTF-8 bytes is a multiple of the configured
//! `one_in`. An offline reader tells full rows from compacted ones by
//! `detail_stripped_at IS NULL`, and can recompute membership from the id
//! alone.

use std::collections::BTreeSet;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::Value;
use striatum_core::RouteStateResult;

use super::{err, SqliteRouteStateStore};

/// FNV-1a 64-bit hash of `decision_id`'s UTF-8 bytes. Stable across builds and
/// platforms, so an offline replay can recompute sample membership.
pub fn decision_id_hash(decision_id: &str) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    decision_id.bytes().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    })
}

/// Whether `decision_id` belongs to the 1-in-`one_in` full-detail sample.
/// Zero disables the sample; one keeps every decision at full detail.
pub fn in_full_detail_sample(decision_id: &str, one_in: u64) -> bool {
    one_in != 0 && decision_id_hash(decision_id) % one_in == 0
}

/// The compact form of a decision's `score_json`. `selected_route_key` and
/// the panel members named in `explanation_json` keep their candidate rows,
/// as do the routes listed in the score's own `alsoAdequate`. A payload that
/// is not a JSON object with a `candidates` array is returned unchanged.
pub fn compact_score_json(
    score_json: &str,
    selected_route_key: Option<&str>,
    explanation_json: Option<&str>,
) -> String {
    let Ok(Value::Object(mut score)) = serde_json::from_str::<Value>(score_json) else {
        return score_json.to_owned();
    };
    let Some(Value::Array(candidates)) = score.remove("candidates") else {
        return score_json.to_owned();
    };
    let mut kept_routes: BTreeSet<String> = selected_route_key
        .map(str::to_owned)
        .into_iter()
        .collect();
    let route_list = |value: Option<&Value>| -> Vec<String> {
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    };
    kept_routes.extend(route_list(score.get("alsoAdequate")));
    if let Some(explanation) =
        explanation_json.and_then(|raw| serde_json::from_str::<Value>(raw).ok())
    {
        kept_routes.extend(route_list(explanation.get("members")));
    }
    let total = candidates.len();
    let kept: Vec<Value> = candidates
        .into_iter()
        .filter(|candidate| {
            candidate
                .get("routeKey")
                .and_then(Value::as_str)
                .is_some_and(|route| kept_routes.contains(route))
        })
        .collect();
    score.insert("candidatesOmitted".into(), Value::from(total - kept.len()));
    score.insert("candidates".into(), Value::Array(kept));
    serde_json::to_string(&Value::Object(score)).unwrap_or_else(|_| score_json.to_owned())
}

/// The compact form of a decision's `quota_signal_json`: the summary header
/// without the per-capacity `signals` list. A payload without that list is
/// returned unchanged.
pub fn compact_quota_signal_json(quota_signal_json: &str) -> String {
    let Ok(Value::Object(mut summary)) = serde_json::from_str::<Value>(quota_signal_json) else {
        return quota_signal_json.to_owned();
    };
    let Some(Value::Array(signals)) = summary.remove("signals") else {
        return quota_signal_json.to_owned();
    };
    summary.insert("signalsOmitted".into(), Value::from(signals.len()));
    serde_json::to_string(&Value::Object(summary)).unwrap_or_else(|_| quota_signal_json.to_owned())
}

/// What one compaction batch did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DetailStripBatch {
    /// Decisions the scan walked past, compacted or not.
    pub examined: usize,
    /// Decisions rewritten to the compact summary.
    pub stripped: usize,
    /// Decisions left at full detail because they are in the sample.
    pub sampled: usize,
}

impl SqliteRouteStateStore {
    /// Compact the scoring detail of at most `limit` decisions created before
    /// `created_before` (Unix milliseconds), oldest first, continuing from
    /// where the previous batch stopped. Decisions in the 1-in-`sample_one_in`
    /// sample are passed over at full detail; a row already compacted (a
    /// continuation copies its predecessor's columns) is left as it is. The
    /// batch runs in one write transaction, which holds the writer from its
    /// first read.
    pub fn strip_decision_detail(
        &self,
        created_before: i64,
        sample_one_in: u64,
        limit: usize,
        now_ms: i64,
    ) -> RouteStateResult<DetailStripBatch> {
        if limit == 0 {
            return Ok(DetailStripBatch::default());
        }
        self.store
            .with_conn(|conn: &Connection| {
                let tx = crate::write_transaction(conn)?;
                let batch = strip_batch(&tx, created_before, sample_one_in, limit, now_ms)?;
                tx.commit()?;
                Ok(batch)
            })
            .map_err(err)
    }
}

fn strip_batch(
    tx: &Transaction<'_>,
    created_before: i64,
    sample_one_in: u64,
    limit: usize,
    now_ms: i64,
) -> rusqlite::Result<DetailStripBatch> {
    let (cursor_at, cursor_id): (i64, String) = tx
        .query_row(
            "SELECT created_at, decision_id FROM model_route_decision_detail_cursor \
             WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .unwrap_or((i64::MIN, String::new()));
    // The created_at index bounds the walk; the id breaks ties so two
    // decisions recorded in the same millisecond are each visited once.
    let due: Vec<(String, i64)> = {
        let mut statement = tx.prepare(
            "SELECT id, created_at FROM model_route_decisions \
               INDEXED BY model_route_decisions_created_at_idx \
             WHERE created_at >= ?1 AND (created_at > ?1 OR id > ?2) AND created_at < ?3 \
             ORDER BY created_at, id LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![
                cursor_at,
                cursor_id,
                created_before,
                i64::try_from(limit).unwrap_or(i64::MAX)
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    let mut batch = DetailStripBatch {
        examined: due.len(),
        ..DetailStripBatch::default()
    };
    {
        let mut read = tx.prepare(
            "SELECT selected_route_key, score_json, quota_signal_json, explanation_json, \
                    detail_stripped_at \
             FROM model_route_decisions WHERE id = ?1",
        )?;
        let mut write = tx.prepare(
            "UPDATE model_route_decisions \
             SET score_json = ?2, quota_signal_json = ?3, detail_stripped_at = ?4 \
             WHERE id = ?1",
        )?;
        for (id, _) in &due {
            if in_full_detail_sample(id, sample_one_in) {
                batch.sampled += 1;
                continue;
            }
            let row: Option<(Option<String>, String, Option<String>, Option<String>, Option<i64>)> =
                read.query_row([id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
                })
                .optional()?;
            let Some((selected, score, quota, explanation, None)) = row else {
                continue;
            };
            let score = compact_score_json(&score, selected.as_deref(), explanation.as_deref());
            let quota = quota.as_deref().map(compact_quota_signal_json);
            write.execute(params![id, score, quota, now_ms])?;
            batch.stripped += 1;
        }
    }
    if let Some((id, created_at)) = due.last() {
        tx.execute(
            "INSERT INTO model_route_decision_detail_cursor (singleton, created_at, decision_id) \
             VALUES (1, ?1, ?2) \
             ON CONFLICT(singleton) DO UPDATE SET \
               created_at = excluded.created_at, decision_id = excluded.decision_id",
            params![created_at, id],
        )?;
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_sample_hash_is_fnv1a_64() {
        // Published FNV-1a 64-bit test vectors.
        assert_eq!(decision_id_hash(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(decision_id_hash("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(decision_id_hash("foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn sample_membership_follows_the_rate() {
        let ids: Vec<String> = (0..2000).map(|index| format!("decision-{index}")).collect();
        let sampled = ids.iter().filter(|id| in_full_detail_sample(id, 20)).count();
        // About one in twenty, from a hash rather than from insertion order.
        assert!((70..=130).contains(&sampled), "{sampled}");
        assert!(ids.iter().all(|id| in_full_detail_sample(id, 1)));
        assert!(!ids.iter().any(|id| in_full_detail_sample(id, 0)));
    }

    #[test]
    fn the_compact_score_keeps_the_picked_and_adequate_candidates_and_pricing() {
        let candidate = |route: &str| json!({"routeKey": route, "model": route, "elo": 1.0, "eq": 1.0,
            "eloSource": "curated", "eqSource": "curated", "eligible": true});
        let full = json!({
            "candidates": [candidate("a/pick"), candidate("b/also"), candidate("c/other"), candidate("d/other")],
            "adequacy": 1.0,
            "selectionCost": 0.2,
            "metrics": {"elo": {"fact": 1300.0, "target": 1250.0}},
            "objective": {"cost": 0.2, "taskPricing": {"valueMultiplier": 6.0, "finalCostUsd": 0.4}},
            "alsoAdequate": ["b/also"],
            "pricingMode": "basket",
        });
        let compact: Value = serde_json::from_str(&compact_score_json(
            &full.to_string(),
            Some("a/pick"),
            Some(r#"["picked a/pick"]"#),
        ))
        .unwrap();
        let kept: Vec<&str> = compact["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|candidate| candidate["routeKey"].as_str().unwrap())
            .collect();
        assert_eq!(kept, ["a/pick", "b/also"]);
        assert_eq!(compact["candidatesOmitted"], 2);
        for member in ["adequacy", "selectionCost", "metrics", "objective", "alsoAdequate", "pricingMode"] {
            assert_eq!(compact[member], full[member], "{member}");
        }
    }

    #[test]
    fn panel_members_keep_their_candidates_and_odd_payloads_pass_through() {
        let panel = json!({"candidates": [{"routeKey": "a/one"}, {"routeKey": "b/two"}, {"routeKey": "c/three"}]});
        let compact: Value = serde_json::from_str(&compact_score_json(
            &panel.to_string(),
            None,
            Some(r#"{"members": ["a/one", "c/three"]}"#),
        ))
        .unwrap();
        assert_eq!(compact["candidates"].as_array().unwrap().len(), 2);
        assert_eq!(compact["candidatesOmitted"], 1);
        for raw in ["null", "{}", r#"{"accuracy":0.9,"cost":1.0}"#, "not json"] {
            assert_eq!(compact_score_json(raw, Some("a/b"), None), raw);
        }
    }

    #[test]
    fn the_compact_quota_summary_drops_only_the_signal_list() {
        let full = json!({"state": "present", "available": true, "refreshedAtMs": 5,
            "signals": [{"capacityKey": "a"}, {"capacityKey": "b"}]});
        let compact: Value =
            serde_json::from_str(&compact_quota_signal_json(&full.to_string())).unwrap();
        assert_eq!(
            compact,
            json!({"state": "present", "available": true, "refreshedAtMs": 5, "signalsOmitted": 2})
        );
        let none = r#"{"state":"none","reason":"quota signal: none (no cached readings)"}"#;
        assert_eq!(compact_quota_signal_json(none), none);
    }

    /// A decision as large as a live one: about 900 candidate rows (about
    /// 134 KB of `score_json`) and 160 quota signals (about 16 KB).
    pub(crate) fn live_sized_decision(id: &str) -> striatum_core::route_state_store::DecisionInput {
        let candidates: Vec<Value> = (0..900)
            .map(|index| {
                let route = format!("provider-{:02}/model-family-name-{index:03}:high", index % 40);
                json!({"routeKey": route, "model": route, "elo": 1234.5, "eq": 1180.25,
                    "eloSource": "operator_grade", "eqSource": "curated", "eligible": false})
            })
            .collect();
        let also: Vec<String> = (0..15)
            .map(|index| format!("provider-{:02}/model-family-name-{index:03}:high", index % 40))
            .collect();
        let score = json!({
            "candidates": candidates,
            "adequacy": 1.0, "rawAdequacy": 1.04, "selectionCost": 0.31,
            "metrics": {"elo": {"fact": 1300.0, "target": 1250.0, "fit": 1.0}},
            "objective": {"cost": 0.3, "quota": 0.01, "speedScore": 0.5, "selectionCost": 0.31,
                "taskPricing": {"valueMultiplier": 6.0, "finalCostUsd": 0.4, "priceUnit": "task"}},
            "alsoAdequate": also,
            "pricingMode": "basket",
        });
        let signals: Vec<Value> = (0..160)
            .map(|index| json!({"capacityKey": format!("provider-{index:03}/pool-weekly"),
                "pressure": 0.125, "windowModel": "weekly", "exhaustedUntil": null}))
            .collect();
        striatum_core::route_state_store::DecisionInput {
            decision_id: Some(id.into()),
            decision_kind: "single".into(),
            target_agent: "mason".into(),
            requirements_json: r#"{"iq":70}"#.into(),
            selected_route_key: Some(also[0].clone()),
            score_json: score.to_string(),
            rejected_json: "[]".into(),
            explanation_json: Some(r#"["cost-primary"]"#.into()),
            quota_signal_json: Some(json!({"state": "present", "signals": signals}).to_string()),
            ..Default::default()
        }
    }

    /// Writer hold time of one compaction batch on live-sized decisions, by
    /// batch size. Run it in release to size `DETAIL_STRIP_BATCH_ROWS`:
    /// `cargo test --release -p striatum-store --lib -- --ignored --nocapture
    /// measure_strip_batch_writer_hold`.
    #[test]
    #[ignore = "measurement, not a check"]
    fn measure_strip_batch_writer_hold() {
        use striatum_core::RouteStateStore;
        let scratch = cortexkit_test_support::ScratchDir::new("strip-batch-hold");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "strip-batch-hold",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let sizes = [1_usize, 2, 4, 8, 16, 32];
        let rounds = 8;
        let total: usize = sizes.iter().sum::<usize>() * rounds;
        let sample = live_sized_decision("probe");
        println!(
            "score_json {} bytes, quota_signal_json {} bytes",
            sample.score_json.len(),
            sample.quota_signal_json.as_ref().unwrap().len()
        );
        for index in 0..total {
            store
                .record_decision(&live_sized_decision(&format!("dec-{index:05}")))
                .unwrap();
        }
        for size in sizes {
            let mut holds: Vec<f64> = (0..rounds)
                .map(|_| {
                    let started = std::time::Instant::now();
                    let batch = store.strip_decision_detail(i64::MAX, 0, size, 1).unwrap();
                    assert_eq!(batch.stripped, size);
                    started.elapsed().as_secs_f64() * 1000.0
                })
                .collect();
            holds.sort_by(f64::total_cmp);
            println!(
                "batch {size:>2} rows: median {:.2} ms, max {:.2} ms, {:.2} ms per row",
                holds[rounds / 2],
                holds[rounds - 1],
                holds[rounds / 2] / size as f64
            );
        }
    }
}
