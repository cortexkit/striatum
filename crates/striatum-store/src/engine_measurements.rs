//! Engine measurements are separate from billed usage and LLM speed estimates.

use striatum_core::RouteStateResult;
use rusqlite::{params, Connection};
use serde::Serialize;

use super::{err, SqliteRouteStateStore};

#[cfg(test)]
#[path = "engine_measurements_tests.rs"]
mod tests;

/// Twenty reports per engine smooth individual jobs without retaining job history.
/// Failed, cancelled and unknown outcomes occupy the window but cannot prove speed.
pub const ENGINE_SPEED_SAMPLE_LIMIT: usize = 20;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EngineEvaluation {
    pub kind: String,
    pub provider: String,
    pub model: String,
    pub metric: String,
    pub set_id: String,
    pub score: f64,
    pub measured_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EngineSpeedSample {
    pub kind: String,
    pub provider: String,
    pub model: String,
    pub outcome: String,
    pub duration_ms: Option<u64>,
    pub wall_ms: u64,
    pub first_output_ms: Option<u64>,
}

impl EngineSpeedSample {
    pub fn successful_realtime_factor(&self) -> Option<f64> {
        if !matches!(self.outcome.as_str(), "done" | "ended") {
            return None;
        }
        self.duration_ms
            .filter(|duration| *duration > 0)
            .map(|duration| self.wall_ms as f64 / duration as f64)
    }
}

fn unsigned(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let text: String = row.get(index)?;
    text.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn speed_sample(row: &rusqlite::Row<'_>) -> rusqlite::Result<EngineSpeedSample> {
    Ok(EngineSpeedSample {
        kind: row.get(0)?,
        provider: row.get(1)?,
        model: row.get(2)?,
        outcome: row.get(3)?,
        duration_ms: row
            .get::<_, Option<String>>(4)?
            .map(|_| unsigned(row, 4))
            .transpose()?,
        wall_ms: unsigned(row, 5)?,
        first_output_ms: row
            .get::<_, Option<String>>(6)?
            .map(|_| unsigned(row, 6))
            .transpose()?,
    })
}

impl SqliteRouteStateStore {
    /// Older evaluations cannot overwrite a newer score. An equal time is last-write-wins.
    pub fn record_engine_evaluation(&self, eval: &EngineEvaluation) -> RouteStateResult<()> {
        self.store
            .with_conn(|conn| {
                conn.execute(
                    "INSERT INTO routing_engine_eval
                 (kind, provider, model, metric, set_id, score, measured_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(kind, provider, model, metric, set_id) DO UPDATE SET
                   score = excluded.score, measured_at_ms = excluded.measured_at_ms
                 WHERE excluded.measured_at_ms >= routing_engine_eval.measured_at_ms",
                    params![
                        eval.kind,
                        eval.provider,
                        eval.model,
                        eval.metric,
                        eval.set_id,
                        eval.score,
                        format!("{:020}", eval.measured_at_ms)
                    ],
                )?;
                Ok(())
            })
            .map_err(err)
    }

    /// The decision marker and bounded sample window commit together. The marker
    /// survives sample eviction, so replaying an older decision cannot add a job.
    pub fn record_engine_speed(
        &self,
        decision_id: &str,
        sample: &EngineSpeedSample,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|conn| {
                let tx = crate::write_transaction(conn)?;
                let first = tx.execute(
                    "UPDATE routing_v1_decision SET engine_reported = 1
                 WHERE decision_id = ?1 AND engine_reported = 0",
                    [decision_id],
                )?;
                if first > 0 {
                    tx.execute(
                        "INSERT INTO routing_engine_speed
                     (kind, provider, model, outcome, duration_ms, wall_ms, first_output_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            sample.kind,
                            sample.provider,
                            sample.model,
                            sample.outcome,
                            sample.duration_ms.map(|n| n.to_string()),
                            sample.wall_ms.to_string(),
                            sample.first_output_ms.map(|n| n.to_string())
                        ],
                    )?;
                    tx.execute(
                        "DELETE FROM routing_engine_speed WHERE sequence IN (
                       SELECT sequence FROM routing_engine_speed
                       WHERE kind = ?1 AND provider = ?2 AND model = ?3
                       ORDER BY sequence DESC LIMIT -1 OFFSET ?4)",
                        params![
                            sample.kind,
                            sample.provider,
                            sample.model,
                            ENGINE_SPEED_SAMPLE_LIMIT as i64
                        ],
                    )?;
                }
                tx.commit()
            })
            .map_err(err)
    }

    /// One reader snapshot, with indexed seeks only for current candidates.
    /// File-backed reads use the read pool, never the store's writer mutex.
    pub fn engine_measurements(
        &self,
        kind: &str,
        metric: Option<&str>,
        engines: &[(&str, &str)],
    ) -> RouteStateResult<(Vec<EngineEvaluation>, Vec<EngineSpeedSample>)> {
        self.store.with_read(|conn: &Connection| {
            let mut eval_query = conn.prepare(
                "SELECT set_id, score, measured_at_ms FROM routing_engine_eval
                 WHERE kind = ?1 AND provider = ?2 AND model = ?3 AND metric = ?4",
            )?;
            let mut speed_query = conn.prepare(
                "SELECT kind, provider, model, outcome, duration_ms, wall_ms, first_output_ms FROM routing_engine_speed
                 WHERE kind = ?1 AND provider = ?2 AND model = ?3 ORDER BY sequence DESC",
            )?;
            let mut evaluations = Vec::new();
            let mut samples = Vec::new();
            for &(provider, model) in engines {
                if let Some(metric) = metric {
                    evaluations.extend(eval_query.query_map(params![kind, provider, model, metric], |row| {
                        Ok(EngineEvaluation {
                            kind: kind.into(), provider: provider.into(), model: model.into(),
                            metric: metric.into(), set_id: row.get(0)?, score: row.get(1)?,
                            measured_at_ms: unsigned(row, 2)?,
                        })
                    })?.collect::<rusqlite::Result<Vec<_>>>()?);
                }
                samples.extend(speed_query.query_map(params![kind, provider, model], speed_sample)?
                    .collect::<rusqlite::Result<Vec<_>>>()?);
            }
            Ok((evaluations, samples))
        }).map_err(err)
    }

    /// Raw, bounded engine windows for the router's catalog inspection interface.
    pub fn engine_speed_samples(&self) -> RouteStateResult<Vec<EngineSpeedSample>> {
        self.store
            .with_read(|conn| {
                let mut query = conn.prepare(
                    "SELECT kind, provider, model, outcome, duration_ms, wall_ms, first_output_ms
                 FROM routing_engine_speed ORDER BY kind, provider, model, sequence DESC",
                )?;
                let samples = query
                    .query_map([], speed_sample)?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(samples)
            })
            .map_err(err)
    }
}
