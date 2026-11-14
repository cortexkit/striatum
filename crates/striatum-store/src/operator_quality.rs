use std::collections::BTreeMap;

use striatum_core::{Confidence, ModelFact, ScoreProvenance};
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
    let decommissioned = decommissioned_rows(c)?;
    for (fact, canonical_id) in facts.iter_mut().zip(canonical_ids) {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        fact.decommissioned = decommissioned.contains_key(&key)
            || canonical_id
                .as_ref()
                .is_some_and(|id| decommissioned.values().any(|model| model == id));
        let grade = grades.get(&key).or_else(|| {
            canonical_id.as_ref().and_then(|id| grades.values().find(|grade| &grade.model == id))
        });
        if let Some(grade) = grade {
            fact.scores.elo = grade.elo;
            fact.scores.eq = grade.eq;
            fact.score_provenance = ScoreProvenance::OperatorGrade;
            fact.eq_source = Some("operator_grade".into());
            // An operator grade is curated knowledge. The row's stored
            // `unknown` confidence described the catalog row before it was
            // graded (unscored or estimated), and leaving it would keep the
            // router's unknown-fact adequacy penalty on a graded score, so the
            // row could never clear the adequacy floor at any demand. Like the
            // scores, this is a read-time overlay: withdrawing the grade
            // restores the stored confidence.
            fact.confidence = Confidence::Curated;
        }
    }
    Ok(())
}

pub(super) fn decommissioned_rows(c: &Connection) -> rusqlite::Result<BTreeMap<String, String>> {
    let exists: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = 'routing_operator_decommissioned')",
        [], |row| row.get(0),
    )?;
    if !exists {
        return Ok(BTreeMap::new());
    }
    let mut stmt = c.prepare(
        "SELECT route_key, model FROM routing_operator_decommissioned ORDER BY route_key",
    )?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect()
}

impl SqliteRouteStateStore {
    /// Keep catalog grades separate from curated scores so withdrawing a grade
    /// restores the stored score rather than persisting the catalog overlay.
    pub fn replace_operator_quality(
        &self,
        active: bool,
        grades: &BTreeMap<String, OperatorQuality>,
    ) -> RouteStateResult<()> {
        self.replace_operator_quality_snapshot(active, grades, &BTreeMap::new())
    }

    /// Replace both current-state quality facts atomically. A flag-only row must
    /// not manufacture a grade, and a later poll can withdraw either fact.
    pub fn replace_operator_quality_snapshot(
        &self,
        active: bool,
        grades: &BTreeMap<String, OperatorQuality>,
        decommissioned: &BTreeMap<String, String>,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = crate::write_transaction(c)?;
                tx.execute("DELETE FROM routing_operator_quality", [])?;
                for (route, grade) in grades {
                    tx.execute(
                        "INSERT INTO routing_operator_quality VALUES (?1, ?2, ?3, ?4)",
                        params![route, grade.model, grade.elo, grade.eq],
                    )?;
                }
                tx.execute("DELETE FROM routing_operator_decommissioned", [])?;
                for (route, model) in decommissioned {
                    tx.execute(
                        "INSERT INTO routing_operator_decommissioned VALUES (?1, ?2)",
                        params![route, model],
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

    #[test]
    fn decommissioned_migration_cache_snapshot_restart_and_atomic_replacement() {
        let dir = cortexkit_test_support::scratch_root().join(format!("decommissioned-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let descriptor = crate::dev_descriptor("decommissioned-store", dir.join("route.db").to_string_lossy().into_owned());
        drop(SqliteRouteStateStore::open_at_version_for_test(&descriptor, 85).unwrap());
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let fact:ModelFact = serde_json::from_value(json!({
            "providerID":"vendor", "modelID":"retired", "confidence":"curated",
            "scores":{"elo":1000,"eq":1100,"speed":0}
        })).unwrap();
        store.upsert_fact(&fact,FactSource::Override).unwrap();
        let read = |store:&SqliteRouteStateStore|store.get_facts().unwrap().into_iter().find(|f|f.provider_id=="vendor" && f.model_id=="retired").unwrap();
        assert!(!read(&store).decommissioned);
        let before = store.catalog_fingerprint().unwrap();
        let flags = BTreeMap::from([("vendor/retired".into(),"vendor/retired".into())]);
        store.replace_operator_quality_snapshot(false,&BTreeMap::new(),&flags).unwrap();
        assert!(read(&store).decommissioned);
        assert_eq!(read(&store).score_provenance,ScoreProvenance::Override);
        assert_ne!(store.catalog_fingerprint().unwrap(),before);
        assert!(read(&store.private_account_copy_for_test(123).unwrap()).decommissioned);
        assert!(!store.get_fact_by_route_key("vendor/retired").unwrap().unwrap().decommissioned);
        store.store.with_conn(|c|c.execute_batch("CREATE TEMP TRIGGER reject_withdrawal BEFORE INSERT ON routing_operator_decommissioned BEGIN SELECT RAISE(ABORT, 'rejected'); END;")).unwrap();
        let grades = BTreeMap::from([("vendor/retired".into(),OperatorQuality {model:"vendor/retired".into(),elo:1900.0,eq:1900.0})]);
        assert!(store.replace_operator_quality_snapshot(true,&grades,&flags).is_err());
        assert!(store.operator_quality().unwrap().is_empty(),"grade replacement must roll back with withdrawals");
        assert!(read(&store).decommissioned);
        drop(store);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert!(read(&store).decommissioned);
        store.replace_operator_quality_snapshot(false,&BTreeMap::new(),&BTreeMap::new()).unwrap();
        assert!(!read(&store).decommissioned);
        assert_eq!(store.catalog_fingerprint().unwrap(),before);
    }
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

    /// A catalog row without a score is stored with `unknown`
    /// confidence. Once the operator grades it, the grade is the row's score,
    /// so the router must not also charge the unknown-fact penalty: a graded
    /// row well above an easy bar has to clear the adequacy floor. Withdrawing
    /// the grade returns the row to its stored, unknown state.
    #[test]
    fn operator_grade_lifts_unknown_confidence_so_a_graded_row_clears_the_floor() {
        let scratch = cortexkit_test_support::ScratchDir::new("operator-grade-confidence");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "operator-grade-confidence",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let fact: ModelFact = serde_json::from_value(json!({
            "providerID": "synthetic", "modelID": "graded-later",
            "scores": {"elo": 0, "eq": 0, "speed": 0}, "confidence": "unknown"
        }))
        .unwrap();
        store.upsert_fact(&fact, FactSource::Unscored).unwrap();
        let read = || {
            store
                .get_facts()
                .unwrap()
                .into_iter()
                .find(|f| f.provider_id == "synthetic" && f.model_id == "graded-later")
                .unwrap()
        };
        assert_eq!(read().confidence, Confidence::Unknown);

        store
            .replace_operator_quality(
                true,
                &BTreeMap::from([(
                    "synthetic/graded-later".into(),
                    OperatorQuality {
                        model: "synthetic/graded-later".into(),
                        elo: 1678.0,
                        eq: 1629.0,
                    },
                )]),
            )
            .unwrap();
        let graded = read();
        assert_eq!(graded.score_provenance, ScoreProvenance::OperatorGrade);
        assert_eq!(graded.confidence, Confidence::Curated);
        // An iq-10 demand against a 2000-elo field: the bar is far below the
        // grade, so only the unknown-fact penalty could hold the row back.
        let adequacy = striatum_core::score_adequacy(
            &graded,
            &striatum_core::ModelRequirements { iq: 10.0, eq: 0.0 },
            striatum_core::TopRoutableScores { elo: 2000.0, eq: 2000.0 },
            &striatum_core::RoutingConfig::default(),
        );
        assert_eq!(adequacy.raw_adequacy, 1.0);
        assert!(adequacy.clears_adequacy_floor(), "{adequacy:?}");

        store
            .replace_operator_quality(false, &BTreeMap::new())
            .unwrap();
        assert_eq!(read().confidence, Confidence::Unknown);
    }
}
