use std::collections::BTreeMap;

use striatum_core::{ModelFact, ScoreProvenance};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::{err, RouteStateResult, SqliteRouteStateStore};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorQuality {
    pub model: String,
    pub elo: f64,
    pub eq: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnscoredAdmissibleModel {
    pub model: String,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
}

pub(super) fn has_quality_table(c: &Connection) -> rusqlite::Result<bool> {
    c.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = 'routing_operator_quality')",
        [], |row| row.get(0),
    )
}

pub(super) fn fingerprint_rows(
    c: &Connection,
) -> rusqlite::Result<BTreeMap<String, OperatorQuality>> {
    if !has_quality_table(c)? {
        return Ok(BTreeMap::new());
    }
    let mut stmt = c.prepare(
        "SELECT route_key, model, elo, eq FROM routing_operator_quality ORDER BY route_key",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get(0)?,
            OperatorQuality {
                model: row.get(1)?,
                elo: row.get(2)?,
                eq: row.get(3)?,
            },
        ))
    })?;
    rows.collect()
}

pub(super) fn overlay_operator_quality(
    c: &Connection,
    facts: &mut [ModelFact],
    canonical_ids: &[Option<String>],
) -> rusqlite::Result<()> {
    let grades = fingerprint_rows(c)?;
    for (fact, canonical_id) in facts.iter_mut().zip(canonical_ids) {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        let grade = grades.get(&key).or_else(|| {
            canonical_id.as_ref().and_then(|id| grades.values().find(|grade| &grade.model == id))
        });
        if let Some(grade) = grade {
            fact.scores.elo = grade.elo;
            fact.scores.eq = grade.eq;
            fact.score_provenance = ScoreProvenance::OperatorGrade;
            fact.eq_source = Some("operator_grade".into());
        }
    }
    Ok(())
}

impl SqliteRouteStateStore {
    /// Keep catalog grades separate from curated scores so withdrawing a grade
    /// restores the stored score rather than persisting the catalog overlay.
    pub fn replace_operator_quality(
        &self,
        active: bool,
        grades: &BTreeMap<String, OperatorQuality>,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                tx.execute("DELETE FROM routing_operator_quality", [])?;
                for (route, grade) in grades {
                    tx.execute(
                        "INSERT INTO routing_operator_quality VALUES (?1, ?2, ?3, ?4)",
                        params![route, grade.model, grade.elo, grade.eq],
                    )?;
                }
                tx.execute(
                    "INSERT INTO routing_operator_quality_state VALUES (1, ?1)
                     ON CONFLICT(singleton) DO UPDATE SET active = excluded.active",
                    [active],
                )?;
                tx.execute(
                    "DELETE FROM routing_unscored_admissible_models
                 WHERE model IN (SELECT model FROM routing_operator_quality)
                    OR model IN (SELECT route_key FROM routing_operator_quality)",
                    [],
                )?;
                tx.commit()
            })
            .map_err(err)
    }

    pub fn operator_quality(&self) -> RouteStateResult<BTreeMap<String, OperatorQuality>> {
        self.store.with_conn(fingerprint_rows).map_err(err)
    }

    pub fn unscored_admissible_models(
        &self,
        since_ms: i64,
        now_ms: i64,
    ) -> RouteStateResult<Vec<UnscoredAdmissibleModel>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                "SELECT model, first_seen_ms, last_seen_ms FROM routing_unscored_admissible_models
                 WHERE last_seen_ms BETWEEN ?1 AND ?2
                   AND (SELECT active FROM routing_operator_quality_state) = 1
                   AND EXISTS (SELECT 1 FROM routing_operator_quality)
                 ORDER BY model",
            )?;
                let rows = stmt.query_map(params![since_ms, now_ms], |row| {
                    Ok(UnscoredAdmissibleModel {
                        model: row.get(0)?,
                        first_seen_ms: row.get(1)?,
                        last_seen_ms: row.get(2)?,
                    })
                })?;
                rows.collect()
            })
            .map_err(err)
    }
}

pub(super) fn observe_decision(c: &Connection, score: &str, now_ms: i64) -> rusqlite::Result<()> {
    // Remember admission while the grading table is empty too: enabling the
    // finding later must not move the first time routing saw an ungraded model.
    let score: serde_json::Value = serde_json::from_str(score).unwrap_or_default();
    for candidate in score["candidates"].as_array().into_iter().flatten() {
        if candidate["eligible"].as_bool() != Some(true) {
            continue;
        }
        let Some(route) = candidate["routeKey"].as_str() else {
            continue;
        };
        let identity: Option<(String, String, Option<String>)> = c
            .query_row(
                "SELECT provider_id, model_id, canonical_id FROM model_facts WHERE route_key = ?1",
                [route],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let identity = identity.or_else(|| {
            let (provider, model) = candidate["model"].as_str()?.split_once('/')?;
            Some((provider.to_owned(), model.to_owned(), None))
        });
        let Some((provider, model, canonical)) = identity else {
            continue;
        };
        let route = format!("{provider}/{model}");
        let model = canonical
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| format!("{provider}/{model}"));
        let graded: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM routing_operator_quality WHERE model = ?1 OR route_key = ?2)",
            params![model, route], |r| r.get(0),
        )?;
        if !graded {
            c.execute(
                "INSERT INTO routing_unscored_admissible_models VALUES (?1, ?2, ?2)
                 ON CONFLICT(model) DO UPDATE SET last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
                params![model, now_ms],
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use striatum_core::{FactSource, RouteStateStore};
    use serde_json::json;

    #[test]
    fn unscored_admissible_first_seen_survives_passes_and_restarts_after_grading() {
        let dir = cortexkit_test_support::scratch_root()
            .join(format!("operator-quality-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("route.db");
        let descriptor = crate::dev_descriptor(
            "operator-quality-store",
            path.to_string_lossy().into_owned(),
        );
        drop(SqliteRouteStateStore::open_at_version_for_test(&descriptor, 68).unwrap());
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let fact: ModelFact = serde_json::from_value(json!({
            "providerID": "synthetic", "modelID": "ungraded",
            "scores": {"elo": 1000, "eq": 1100, "speed": 0}, "confidence": "curated"
        }))
        .unwrap();
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        store.store.with_conn(|c: &Connection| {
            c.execute("UPDATE model_facts SET canonical_id = 'synthetic-lab/ungraded' WHERE route_key = 'synthetic/ungraded'", [])?;
            Ok(())
        }).unwrap();
        let score = json!({"candidates": [
            {"routeKey": "synthetic/ungraded", "eligible": true},
            {"routeKey": "synthetic/ungraded", "eligible": false}
        ]})
        .to_string();
        let observe = |store: &SqliteRouteStateStore, time| {
            store
                .store
                .with_conn(|c: &Connection| observe_decision(c, &score, time))
                .unwrap();
        };
        observe(&store, 100);
        let curated_fingerprint = store.catalog_fingerprint().unwrap();
        assert!(
            store
                .unscored_admissible_models(0, 1000)
                .unwrap()
                .is_empty(),
            "an empty grading table does not start findings"
        );
        let mut grades = BTreeMap::from([(
            "synthetic/graded".into(),
            OperatorQuality {
                model: "synthetic/graded".into(),
                elo: 1284.0,
                eq: 1599.8,
            },
        )]);
        store.replace_operator_quality(true, &grades).unwrap();
        let graded_fingerprint = store.catalog_fingerprint().unwrap();
        assert_ne!(graded_fingerprint, curated_fingerprint);
        store.replace_operator_quality(true, &grades).unwrap();
        assert_eq!(
            store.catalog_fingerprint().unwrap(),
            graded_fingerprint,
            "a no-op catalog refresh keeps its fingerprint"
        );
        observe(&store, 200);
        observe(&store, 300);
        let rows = store.unscored_admissible_models(250, 1000).unwrap();
        assert_eq!(
            rows,
            [UnscoredAdmissibleModel {
                model: "synthetic-lab/ungraded".into(),
                first_seen_ms: 100,
                last_seen_ms: 300,
            }]
        );
        drop(store);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            store.unscored_admissible_models(0, 1000).unwrap(),
            rows,
            "first seen persists across restart"
        );
        assert!(
            store
                .unscored_admissible_models(301, 1000)
                .unwrap()
                .is_empty(),
            "outside the decision window"
        );
        grades.insert(
            "synthetic/ungraded".into(),
            OperatorQuality {
                model: "synthetic-lab/ungraded".into(),
                elo: 1400.0,
                eq: 1500.0,
            },
        );
        store.replace_operator_quality(true, &grades).unwrap();
        observe(&store, 400);
        assert!(store
            .unscored_admissible_models(0, 1000)
            .unwrap()
            .is_empty());
        grades.remove("synthetic/ungraded");
        store.replace_operator_quality(true, &grades).unwrap();
        observe(&store, 500);
        assert_eq!(
            store.unscored_admissible_models(0, 1000).unwrap()[0].first_seen_ms,
            500
        );
        for cleared_tables in [
            "DELETE FROM routing_operator_quality",
            "DELETE FROM routing_operator_quality_state",
            "DELETE FROM routing_operator_quality; DELETE FROM routing_operator_quality_state",
        ] {
            store
                .store
                .with_conn(|c: &Connection| c.execute_batch(cleared_tables))
                .unwrap();
            assert!(store
                .unscored_admissible_models(0, 1000)
                .unwrap()
                .is_empty());
            store.replace_operator_quality(true, &grades).unwrap();
            assert_eq!(
                store.unscored_admissible_models(0, 1000).unwrap()[0].first_seen_ms,
                500,
                "a catalog refresh rebuilds omitted caches without resetting admission history"
            );
        }
        store
            .replace_operator_quality(false, &BTreeMap::new())
            .unwrap();
        assert_eq!(store.catalog_fingerprint().unwrap(), curated_fingerprint);
        assert!(store
            .unscored_admissible_models(0, 1000)
            .unwrap()
            .is_empty());
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
