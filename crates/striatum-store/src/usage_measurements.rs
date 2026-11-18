//! Usage delivery and the observed speed and workload measurements it feeds.
//! Keeping these queries together makes their shared time bounds and cache
//! invalidation rules visible alongside the writes.

use super::*;

/// Fetch one route's bounded observed-speed sample set through the route/time index.
/// The batched lookup prepares this statement once and reuses it for every
/// effective catalog key. The window is `(now - 7d, now]`, the same bounds as the
/// workload-class samples below, so a row exactly seven days old counts in
/// neither.
pub const OBSERVED_SPEED_SAMPLES_SQL: &str = "SELECT output_tokens, duration_ms, observed_at
     FROM model_route_usage INDEXED BY model_route_usage_route_observed_idx
     WHERE route_key = ?1
       AND ?7 = 'standard' AND ?8 = 'standard'
       AND observed_at > ?2
       AND observed_at <= ?3
       AND output_tokens >= ?4
       AND duration_ms >= ?5
     UNION ALL
     SELECT output_tokens, duration_ms, observed_at
     FROM model_route_speed_samples INDEXED BY model_route_speed_samples_route_tier_observed_idx
     WHERE route_key = ?1 AND population_service_tier IS ?7 AND population_speed IS ?8
       AND observed_at > ?2 AND observed_at <= ?3
       AND output_tokens >= ?4 AND duration_ms >= ?5
     ORDER BY observed_at DESC
     LIMIT ?6";

/// Fetch one workload class's complete token-profile population through its class/time index.
pub const WORKLOAD_CLASS_USAGE_SAMPLES_SQL: &str = "SELECT input_tokens, output_tokens,
            reasoning_tokens, cached_read_tokens, cache_write_tokens
     FROM model_route_usage INDEXED BY model_route_usage_workload_class_observed_idx
     WHERE workload_class = ?1
       AND observed_at > ?2
       AND observed_at <= ?3
       AND input_tokens IS NOT NULL
       AND output_tokens IS NOT NULL
       AND reasoning_tokens IS NOT NULL
       AND cached_read_tokens IS NOT NULL
        AND cache_write_tokens IS NOT NULL";

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ObservedSpeedAggregate {
    route_key: String,
    pub(super) sample_count: i64,
    pub(super) output_tokens: i64,
    duration_ms: i64,
    /// Trusted speed for the window, or `None` when the trust gates reject it.
    /// See `observed_speed_from_samples`.
    pub(super) speed: Option<f64>,
}

/// Trusted observed speed, in output tokens per second, for one route's bounded
/// sample window of `(output_tokens, duration_ms)` rows.
///
/// The window must hold at least `OBSERVED_SPEED_MIN_SAMPLES` rows and
/// `OBSERVED_SPEED_MIN_OUTPUT_TOKENS` output tokens in total; otherwise the
/// signal is untrusted and the result is `None`. The speed is the median of the
/// per-sample speeds rather than total tokens over total duration: a single very
/// long generation (for example one that ran to the output-token limit) would
/// otherwise dominate both sums and set the route's speed on its own.
fn observed_speed_from_samples(samples: &[(i64, i64)]) -> Option<f64> {
    if samples.len() < OBSERVED_SPEED_MIN_SAMPLES {
        return None;
    }
    let output_tokens: i64 = samples.iter().map(|(output, _)| output).sum();
    if (output_tokens as f64) < OBSERVED_SPEED_MIN_OUTPUT_TOKENS {
        return None;
    }
    median_of(
        samples
            .iter()
            .filter(|(_, duration)| *duration > 0)
            .map(|(output, duration)| (*output as f64 / *duration as f64) * 1000.0),
    )
}

/// Median of the measured speeds, used as the stand-in for routes with none.
/// Median rather than mean because one very fast or very slow route should not
/// move where an unmeasured route enters the ranking.
fn median_of(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut sorted: Vec<f64> = values
        .filter(|value| value.is_finite() && *value > 0.0)
        .collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Some(if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

fn observed_route_key_for_fact(fact: &ModelFact) -> String {
    let observed_variant = fact
        .prompt_defaults
        .as_ref()
        .and_then(|defaults| defaults.variant.as_deref())
        .or(fact.variant.as_deref());
    striatum_core::make_route_key(&fact.provider_id, &fact.model_id, observed_variant)
}

#[derive(Debug, Clone)]
pub(super) struct CachedClassVector {
    pub(super) usage_generation: u64,
    pub(super) window_minute: i64,
    pub(super) vector: Option<ClassVector>,
}

pub(super) fn floored_i64(value: f64) -> Option<i64> {
    let floored = value.floor();
    (value.is_finite() && floored >= i64::MIN as f64 && floored < i64::MAX as f64)
        .then_some(floored as i64)
}

pub(super) fn stored_token_count(value: f64) -> Option<i64> {
    (value >= 0.0).then(|| floored_i64(value)).flatten()
}

pub(super) fn stored_output_tokens(value: f64) -> Option<i64> {
    stored_token_count(value).filter(|tokens| *tokens > 0)
}

pub(super) fn stored_workload_class(value: Option<&str>) -> Option<&str> {
    value.filter(|class| matches!(*class, "ephemeral" | "head" | "gather" | "hire"))
}

impl SqliteRouteStateStore {
    pub(super) fn overlay_observed_speed(
        &self,
        facts: Vec<ModelFact>,
        now: f64,
        pricing: &striatum_core::RoutePricing,
    ) -> RouteStateResult<Vec<ModelFact>> {
        let ephemeral_vector = self.get_class_vector("ephemeral", now)?;
        // Multiple catalog facts can resolve to the same prompt-default variant.
        // Deduplicating here prevents repeated seeks without consulting usage for keys.
        let observed_route_keys = facts
            .iter()
            .map(observed_route_key_for_fact)
            .collect::<BTreeSet<_>>();
        let observed_speeds = self.get_observed_speeds(&observed_route_keys, now)?;
        // A route with too few samples has NO speed, which is not the same as being
        // slow - but `scores.speed` carries 0.0 for both, and 0.0 is the worst value
        // the tie-break can hold. That tie-break decides the whole effectively-free
        // bucket, so an unmeasured route loses every comparison it enters, and
        // winning one is the only way it is ever run and measured. A model could
        // therefore never earn its own speed (live: 231 of 291 catalog rows sit at
        // 0.0, among them routes with the best elo on their price tier).
        //
        // Standing in for the unknown with the MEDIAN of what is measured breaks the
        // deadlock without inventing an advantage: a new route enters as ordinary
        // rather than as worst, gets picked when its price and quality justify it,
        // and replaces the stand-in with its own observation after the sample floor.
        // Falling back to the catalog number first keeps a hand-seeded speed
        // authoritative over a fleet median.
        let median_observed_speed = median_of(observed_speeds.values().copied());
        let mut overlaid = Vec::with_capacity(facts.len());
        for mut fact in facts {
            fact.class_vector = ephemeral_vector.clone();
            fact.class_vector_is_overlay = ephemeral_vector.is_some();
            let observed_route_key = observed_route_key_for_fact(&fact);
            if let Some(speed) = observed_speeds.get(&observed_route_key) {
                fact.scores.speed = *speed;
            } else if fact.scores.speed <= 0.0 {
                if let Some(median) = median_observed_speed {
                    fact.scores.speed = median;
                    fact.speed_is_fleet_median = true;
                }
            }
            let route_key = striatum_core::make_route_key(
                &fact.provider_id,
                &fact.model_id,
                fact.variant.as_deref(),
            );
            if let Some(multiplier) = pricing.speed_multiplier(&route_key) {
                fact.scores.speed *= multiplier;
                fact.speed_multiplier = Some(multiplier);
            }
            // Subscription subsidy is overlaid rather than persisted, so the
            // objective can price the route at list ÷ multiplier.
            if let Some(multiplier) = pricing.subscription_multiplier(&route_key) {
                fact.subscription_multiplier = Some(multiplier);
            }
            overlaid.push(fact);
        }
        Ok(overlaid)
    }

    pub fn get_facts_with_observed_speed_and_generation(
        &self,
        now: f64,
        pricing: &striatum_core::RoutePricing,
    ) -> RouteStateResult<(Arc<CatalogFactsSnapshot>, Vec<ModelFact>)> {
        let snapshot = self.get_selection_catalog()?;
        let facts = self.overlay_observed_speed(snapshot.facts.clone(), now, pricing)?;
        Ok((snapshot, facts))
    }

    /// Fetch each effective catalog route through its own bounded index seek while
    /// retaining one store call, connection checkout, and prepared statement.
    pub(super) fn get_observed_speed_aggregates(
        &self,
        observed_route_keys: &BTreeSet<String>,
        now: f64,
    ) -> RouteStateResult<Vec<ObservedSpeedAggregate>> {
        if observed_route_keys.is_empty() {
            return Ok(Vec::new());
        }
        self.store
            .with_read(|c: &Connection| {
                let mut stmt = c.prepare(OBSERVED_SPEED_SAMPLES_SQL)?;
                let mut aggregates = Vec::with_capacity(observed_route_keys.len());
                for route_key in observed_route_keys {
                    let samples = stmt
                        .query_map(
                            params![
                                route_key,
                                (now - OBSERVED_SPEED_WINDOW_MS).floor() as i64,
                                now.floor() as i64,
                                USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                                USAGE_MIN_DURATION_MS.floor() as i64,
                                OBSERVED_SPEED_SAMPLE_LIMIT,
                                "standard",
                                "standard",
                            ],
                            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                        )?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    if !samples.is_empty() {
                        aggregates.push(ObservedSpeedAggregate {
                            route_key: route_key.clone(),
                            sample_count: samples.len() as i64,
                            output_tokens: samples.iter().map(|(output, _)| output).sum(),
                            duration_ms: samples.iter().map(|(_, duration)| duration).sum(),
                            speed: observed_speed_from_samples(&samples),
                        });
                    }
                }
                Ok(aggregates)
            })
            .map_err(err)
    }

    pub(super) fn get_observed_speeds(
        &self,
        observed_route_keys: &BTreeSet<String>,
        now: f64,
    ) -> RouteStateResult<HashMap<String, f64>> {
        let mut speeds = HashMap::new();
        for aggregate in self.get_observed_speed_aggregates(observed_route_keys, now)? {
            // Absent or untrusted aggregates leave the catalog speed untouched.
            if let Some(speed) = aggregate.speed {
                speeds.insert(aggregate.route_key, speed);
            }
        }
        Ok(speeds)
    }

    /// Compute the whole-population mean for `get_class_vector`, which caches
    /// the result until a write or a change to the window's minute boundary.
    pub(super) fn compute_class_vector(
        &self,
        workload_class: &str,
        now: f64,
    ) -> RouteStateResult<Option<ClassVector>> {
        let window_start = (now - CLASS_VECTOR_TRAILING_WINDOW_MS).floor() as i64;
        let window_end = now.floor() as i64;
        self.store
            .with_read(|c: &Connection| {
                let mut stmt = c.prepare(WORKLOAD_CLASS_USAGE_SAMPLES_SQL)?;
                let rows =
                    stmt.query_map(params![workload_class, window_start, window_end], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    })?;

                let mut sample_count = 0_u64;
                let mut sum_fresh_input = 0_f64;
                let mut sum_output_excluding_reasoning = 0_f64;
                let mut sum_reasoning = 0_f64;
                let mut sum_cached_read = 0_f64;
                let mut sum_cache_write = 0_f64;

                for row in rows {
                    let (
                        input_tokens,
                        output_tokens,
                        reasoning_tokens,
                        cached_read_tokens,
                        cache_write_tokens,
                    ) = row?;
                    sample_count += 1;
                    sum_fresh_input += input_tokens as f64;
                    let output_excl = (output_tokens - reasoning_tokens).max(0) as f64;
                    sum_output_excluding_reasoning += output_excl;
                    sum_reasoning += reasoning_tokens as f64;
                    sum_cached_read += cached_read_tokens as f64;
                    sum_cache_write += cache_write_tokens as f64;
                }

                if sample_count == 0 {
                    return Ok(None);
                }

                let count = sample_count as f64;
                Ok(Some(ClassVector {
                    workload_class: workload_class.to_string(),
                    fresh_input: sum_fresh_input / count,
                    output_excluding_reasoning: sum_output_excluding_reasoning / count,
                    reasoning: sum_reasoning / count,
                    cached_read: sum_cached_read / count,
                    cache_write: sum_cache_write / count,
                    sample_count,
                    trailing_window_days: CLASS_VECTOR_TRAILING_WINDOW_DAYS,
                    statistic: "arithmetic_mean".to_string(),
                    is_overlay: true,
                }))
            })
            .map_err(err)
    }
}

impl SqliteRouteStateStore {
    /// Insert generation timing without changing usage, task totals or cached workload token profiles.
    /// A repeated sample id is a no-op, even when the redelivery contains different data.
    pub fn record_speed_sample_delivery(
        &self,
        sample: &striatum_core::usage_facts::SpeedSampleObservation,
    ) -> RouteStateResult<UsageRecordResult> {
        let Some(observed_at) = floored_i64(sample.observed_at) else {
            return Ok(UsageRecordResult::default());
        };
        let Some(output) = stored_output_tokens(sample.output_tokens).filter(|n| *n > 0) else {
            return Ok(UsageRecordResult::default());
        };
        let Some(duration) = floored_i64(sample.duration_ms).filter(|n| *n > 0) else {
            return Ok(UsageRecordResult::default());
        };
        if sample.id.trim().is_empty()
            || sample.route_key.trim().is_empty()
            || sample.provider_id.trim().is_empty()
            || sample.model_id.trim().is_empty()
        {
            return Ok(UsageRecordResult::default());
        }
        let (population_tier, population_speed) =
            striatum_core::usage_facts::speed_sample_population(
                sample.service_tier.as_deref(),
                sample.speed.as_deref(),
            );
        let inserted = self
            .store
            .with_conn(|c: &Connection| {
                c.execute(
                    "INSERT INTO model_route_speed_samples
                 (id, route_key, provider_id, model_id, variant, service_tier, speed,
                  output_tokens, reasoning_tokens, duration_ms, observed_at,
                  population_service_tier, population_speed)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(id) DO NOTHING",
                    params![
                        sample.id,
                        sample.route_key,
                        sample.provider_id,
                        sample.model_id,
                        sample.variant,
                        sample.service_tier,
                        sample.speed,
                        output,
                        sample.reasoning_tokens.and_then(stored_token_count),
                        duration,
                        observed_at,
                        population_tier,
                        population_speed
                    ],
                )
            })
            .map_err(err)?;
        Ok(UsageRecordResult {
            recorded: inserted > 0
                && output >= USAGE_MIN_OUTPUT_TOKENS as i64
                && duration >= USAGE_MIN_DURATION_MS as i64,
            usage_recorded: false,
        })
    }

    /// Read one served-tier/speed population. Timed OpenCode assistant usage rows belong only to
    /// standard/standard; selection deliberately continues to use that population.
    pub fn get_observed_speed_for_tier(
        &self,
        route_key: &str,
        service_tier: Option<&str>,
        speed: Option<&str>,
        now: f64,
    ) -> RouteStateResult<Option<f64>> {
        let (population_tier, population_speed) =
            striatum_core::usage_facts::speed_sample_population(service_tier, speed);
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(OBSERVED_SPEED_SAMPLES_SQL)?;
                let samples = stmt
                    .query_map(
                        params![
                            route_key,
                            (now - OBSERVED_SPEED_WINDOW_MS).floor() as i64,
                            now.floor() as i64,
                            USAGE_MIN_OUTPUT_TOKENS as i64,
                            USAGE_MIN_DURATION_MS as i64,
                            OBSERVED_SPEED_SAMPLE_LIMIT,
                            population_tier,
                            population_speed,
                        ],
                        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(observed_speed_from_samples(&samples))
            })
            .map_err(err)
    }

    /// Copy task and workload attribution from the decision, never from the report.
    pub fn record_usage_delivery_for_decision(
        &self,
        obs: &UsageObservation,
        decision_id: &str,
    ) -> RouteStateResult<UsageRecordResult> {
        self.record_usage_delivery_with_decision(obs, Some(decision_id))
    }

    pub(super) fn record_usage_delivery_with_decision(
        &self,
        obs: &UsageObservation,
        decision_id: Option<&str>,
    ) -> RouteStateResult<UsageRecordResult> {
        if obs.id.trim().is_empty()
            || obs.route_key.trim().is_empty()
            || obs.capacity_key.trim().is_empty()
            || obs.provider_id.trim().is_empty()
        {
            return Ok(UsageRecordResult::default());
        }
        let Some(observed_at) = floored_i64(obs.observed_at) else {
            return Ok(UsageRecordResult::default());
        };

        let input_tokens = obs.input_tokens.and_then(stored_token_count);
        let output_tokens = stored_output_tokens(obs.output_tokens);
        let reasoning_tokens = obs.reasoning_tokens.and_then(stored_token_count);
        let cached_read_tokens = obs.cached_read_tokens.and_then(stored_token_count);
        let cache_write_tokens = obs.cache_write_tokens.and_then(stored_token_count);
        let duration_ms = (obs.duration_ms >= 0.0)
            .then(|| floored_i64(obs.duration_ms))
            .flatten();
        let workload_class = stored_workload_class(obs.workload_class.as_deref());

        self.store
            .with_conn(|c: &Connection| {
                let (task_id, decision_workload): (Option<String>, Option<String>) =
                    match decision_id {
                        Some(id) => c.query_row(
                            "SELECT task_id, COALESCE(consumer, selected_workload, target_agent)
                             FROM model_route_decisions WHERE id = ?1",
                            params![id],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )?,
                        None => (None, None),
                    };
                let task_workload = task_id.as_ref().and(decision_workload);
                let held_speed_observation = c
                    .query_row(
                        "SELECT COALESCE(output_tokens >= ?2 AND duration_ms >= ?3, FALSE)
                         FROM model_route_usage
                         WHERE id = ?1",
                        params![
                            obs.id,
                            USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                            USAGE_MIN_DURATION_MS.floor() as i64,
                        ],
                        |row| row.get::<_, bool>(0),
                    )
                    .optional()?;

                let stored_speed_values = c
                    .query_row(
                        "INSERT INTO model_route_usage
                           (id, task_id, route_key, capacity_key, provider_id,
                            input_tokens, output_tokens, duration_ms, observed_at,
                            reasoning_tokens, cached_read_tokens, cache_write_tokens,
                            workload_class, account_key, task_workload)
                         VALUES (?1, ?14, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                                 ?9, ?10, ?11, ?12, ?13, ?15)
                         ON CONFLICT(id) DO UPDATE SET
                           task_id = COALESCE(model_route_usage.task_id, excluded.task_id),
                           input_tokens = COALESCE(model_route_usage.input_tokens,
                                                   excluded.input_tokens),
                           output_tokens = COALESCE(model_route_usage.output_tokens,
                                                    excluded.output_tokens),
                           duration_ms = COALESCE(model_route_usage.duration_ms,
                                                 excluded.duration_ms),
                           reasoning_tokens = COALESCE(model_route_usage.reasoning_tokens,
                                                       excluded.reasoning_tokens),
                           cached_read_tokens = COALESCE(model_route_usage.cached_read_tokens,
                                                         excluded.cached_read_tokens),
                           cache_write_tokens = COALESCE(model_route_usage.cache_write_tokens,
                                                         excluded.cache_write_tokens),
                           workload_class = COALESCE(model_route_usage.workload_class,
                                                     excluded.workload_class),
                           task_workload = CASE
                             WHEN model_route_usage.task_id IS NULL OR model_route_usage.task_id = excluded.task_id
                               THEN COALESCE(model_route_usage.task_workload, excluded.task_workload)
                             ELSE model_route_usage.task_workload
                           END,
                            account_key = COALESCE(model_route_usage.account_key,
                                                   excluded.account_key)
                         WHERE (model_route_usage.input_tokens IS NULL AND excluded.input_tokens IS NOT NULL)
                            OR (model_route_usage.output_tokens IS NULL AND excluded.output_tokens IS NOT NULL)
                            OR (model_route_usage.duration_ms IS NULL AND excluded.duration_ms IS NOT NULL)
                            OR (model_route_usage.reasoning_tokens IS NULL AND excluded.reasoning_tokens IS NOT NULL)
                            OR (model_route_usage.cached_read_tokens IS NULL AND excluded.cached_read_tokens IS NOT NULL)
                            OR (model_route_usage.cache_write_tokens IS NULL AND excluded.cache_write_tokens IS NOT NULL)
                            OR (model_route_usage.workload_class IS NULL AND excluded.workload_class IS NOT NULL)
                             OR (model_route_usage.account_key IS NULL AND excluded.account_key IS NOT NULL)
                              OR (model_route_usage.task_id IS NULL AND excluded.task_id IS NOT NULL)
                              OR (model_route_usage.task_workload IS NULL AND excluded.task_workload IS NOT NULL
                                  AND (model_route_usage.task_id IS NULL OR model_route_usage.task_id = excluded.task_id))
                         RETURNING output_tokens, duration_ms",
                        params![
                            obs.id,
                            obs.route_key,
                            obs.capacity_key,
                            obs.provider_id,
                            input_tokens,
                            output_tokens,
                            duration_ms,
                            observed_at,
                            reasoning_tokens,
                            cached_read_tokens,
                            cache_write_tokens,
                            workload_class,
                            obs.account_key.as_deref(),
                            task_id,
                            task_workload,
                        ],
                        |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
                    )
                    .optional()?;

                let holds_speed_observation =
                    stored_speed_values.is_some_and(|(output, duration)| {
                        output
                            .is_some_and(|tokens| tokens >= USAGE_MIN_OUTPUT_TOKENS.floor() as i64)
                            && duration.is_some_and(|ms| ms >= USAGE_MIN_DURATION_MS.floor() as i64)
                    });
                Ok((UsageRecordResult {
                    recorded: !held_speed_observation.unwrap_or(false) && holds_speed_observation,
                    usage_recorded: held_speed_observation.is_none(),
                }, stored_speed_values.is_some()))
            })
            .map_err(err)
            .map(|(result, changed)| {
                // Retries that supply no new measurements must not invalidate the
                // seven-day population mean. Only inserts/enrichment can move it.
                if changed {
                    self.usage_generation.fetch_add(1, Ordering::AcqRel);
                }
                result
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_open::tests::{observed_speed_test_store, remove_observed_speed_test_store};

    const OLD_OBSERVED_SPEED_AGGREGATE_SQL: &str = "WITH ranked_usage AS (
       SELECT route_key, output_tokens, duration_ms,
              ROW_NUMBER() OVER (
                PARTITION BY route_key ORDER BY observed_at DESC
              ) AS sample_rank
       FROM model_route_usage
       WHERE observed_at > ?1
         AND observed_at <= ?2
         AND output_tokens >= ?3
         AND duration_ms >= ?4
     )
     SELECT route_key, COUNT(*), SUM(output_tokens), SUM(duration_ms)
     FROM ranked_usage
     WHERE sample_rank <= ?5
     GROUP BY route_key";

    fn io_observation(id: &str, at: f64) -> UsageObservation {
        UsageObservation {
            id: id.into(),
            route_key: "io/model".into(),
            capacity_key: "io/model".into(),
            provider_id: "io".into(),
            input_tokens: Some(1000.0),
            output_tokens: 500.0,
            duration_ms: 1000.0,
            observed_at: at,
            reasoning_tokens: Some(100.0),
            cached_read_tokens: Some(200.0),
            cache_write_tokens: Some(300.0),
            workload_class: Some("ephemeral".into()),
            account_key: Some("io/account".into()),
        }
    }

    #[test]
    fn duplicate_usage_delivery_changes_no_database_pages() {
        let (dir, store) = observed_speed_test_store("duplicate-io");
        let obs = io_observation("message", 1_800_000_000_000.0);
        assert!(store.record_usage_delivery(&obs).unwrap().usage_recorded);
        let observer = Connection::open(&dir).unwrap();
        let version = || {
            observer
                .pragma_query_value(None, "data_version", |r| r.get::<_, i64>(0))
                .unwrap()
        };
        let before = version();
        for _ in 0..10 {
            assert_eq!(
                store.record_usage_delivery(&obs).unwrap(),
                UsageRecordResult::default()
            );
        }
        assert_eq!(
            version(),
            before,
            "a duplicate must not write even unchanged values"
        );
        drop(observer);
        drop(store);
        remove_observed_speed_test_store(&dir);
    }

    #[test]
    fn duplicate_usage_delivery_does_not_invalidate_the_population_cache() {
        let (dir, store) = observed_speed_test_store("duplicate-cache");
        let mut obs = io_observation("message", 1_800_000_000_000.0);
        obs.cached_read_tokens = None;
        store.record_usage_delivery(&obs).unwrap();
        let before = store.usage_generation.load(Ordering::Acquire);
        store.record_usage_delivery(&obs).unwrap();
        assert_eq!(store.usage_generation.load(Ordering::Acquire), before);
        // A NULL-only profile upgrade still invalidates, even if speed was
        // already eligible and the reply reports neither new recording effect.
        obs.cached_read_tokens = Some(200.0);
        assert_eq!(
            store.record_usage_delivery(&obs).unwrap(),
            UsageRecordResult::default()
        );
        assert_eq!(store.usage_generation.load(Ordering::Acquire), before + 1);
        drop(store);
        remove_observed_speed_test_store(&dir);
    }

    #[test]
    fn usage_delivery_enriches_each_null_column_and_preserves_first_values() {
        use rusqlite::types::Value;

        struct Case {
            column: &'static str,
            clear: fn(&mut UsageObservation),
            conflict: fn(&mut UsageObservation),
            expected: Value,
        }

        let cases = [
            Case {
                column: "input_tokens",
                clear: |obs| obs.input_tokens = None,
                conflict: |obs| obs.input_tokens = Some(4000.0),
                expected: Value::Integer(1000),
            },
            Case {
                column: "output_tokens",
                clear: |obs| obs.output_tokens = f64::NAN,
                conflict: |obs| obs.output_tokens = 2000.0,
                expected: Value::Integer(500),
            },
            Case {
                column: "duration_ms",
                clear: |obs| obs.duration_ms = f64::NAN,
                conflict: |obs| obs.duration_ms = 2000.0,
                expected: Value::Integer(1000),
            },
            Case {
                column: "reasoning_tokens",
                clear: |obs| obs.reasoning_tokens = None,
                conflict: |obs| obs.reasoning_tokens = Some(300.0),
                expected: Value::Integer(100),
            },
            Case {
                column: "cached_read_tokens",
                clear: |obs| obs.cached_read_tokens = None,
                conflict: |obs| obs.cached_read_tokens = Some(400.0),
                expected: Value::Integer(200),
            },
            Case {
                column: "cache_write_tokens",
                clear: |obs| obs.cache_write_tokens = None,
                conflict: |obs| obs.cache_write_tokens = Some(500.0),
                expected: Value::Integer(300),
            },
            Case {
                column: "workload_class",
                clear: |obs| obs.workload_class = None,
                conflict: |obs| obs.workload_class = Some("head".into()),
                expected: Value::Text("ephemeral".into()),
            },
            Case {
                column: "account_key",
                clear: |obs| obs.account_key = None,
                conflict: |obs| obs.account_key = Some("io/rebound".into()),
                expected: Value::Text("io/account".into()),
            },
        ];
        let (path, store) = observed_speed_test_store("all-enrichment-columns");
        let observer = Connection::open(&path).unwrap();
        let stored = |id: &str, column: &str| {
            observer
                .query_row(
                    &format!("SELECT {column} FROM model_route_usage WHERE id = ?1"),
                    [id],
                    |row| row.get::<_, Value>(0),
                )
                .unwrap()
        };

        for case in cases {
            let complete =
                io_observation(&format!("enrichment-{}", case.column), 1_800_000_000_000.0);
            let mut missing = complete.clone();
            // Output/duration are non-optional in the domain observation; an
            // unavailable measurement is stored as NULL. Verify that setup
            // rather than assuming the sentinel reached the database as NULL.
            (case.clear)(&mut missing);
            store.record_usage_delivery(&missing).unwrap();
            assert_eq!(
                stored(&complete.id, case.column),
                Value::Null,
                "{}: first delivery must leave this column NULL",
                case.column
            );

            let before = store.usage_generation.load(Ordering::Acquire);
            store.record_usage_delivery(&complete).unwrap();
            assert_eq!(
                stored(&complete.id, case.column),
                case.expected,
                "{}: late enrichment must persist",
                case.column
            );
            assert_eq!(
                store.usage_generation.load(Ordering::Acquire),
                before + 1,
                "{}: enrichment must invalidate the population cache",
                case.column
            );

            let mut conflicting = complete.clone();
            (case.conflict)(&mut conflicting);
            let before = store.usage_generation.load(Ordering::Acquire);
            store.record_usage_delivery(&conflicting).unwrap();
            assert_eq!(
                stored(&complete.id, case.column),
                case.expected,
                "{}: an already-set value must not be overwritten",
                case.column
            );
            assert_eq!(
                store.usage_generation.load(Ordering::Acquire),
                before,
                "{}: a conflicting repeat is still a no-op",
                case.column
            );

            // Also exercise COALESCE during a real UPDATE, not just a delivery
            // whose WHERE rejects every assignment. Filling another column
            // must not replace the first value in the column under test.
            let mut first = complete.clone();
            first.id = format!("first-value-{}", case.column);
            let (witness, witness_value) = if case.column == "input_tokens" {
                first.reasoning_tokens = None;
                ("reasoning_tokens", Value::Integer(100))
            } else {
                first.input_tokens = None;
                ("input_tokens", Value::Integer(1000))
            };
            store.record_usage_delivery(&first).unwrap();
            assert_eq!(stored(&first.id, case.column), case.expected);
            assert_eq!(stored(&first.id, witness), Value::Null);
            conflicting.id = first.id.clone();
            let before = store.usage_generation.load(Ordering::Acquire);
            store.record_usage_delivery(&conflicting).unwrap();
            assert_eq!(
                stored(&first.id, witness),
                witness_value,
                "{}: witness enrichment must execute the UPDATE",
                case.column
            );
            assert_eq!(
                stored(&first.id, case.column),
                case.expected,
                "{}: COALESCE must preserve the first value during another column's enrichment",
                case.column
            );
            assert_eq!(store.usage_generation.load(Ordering::Acquire), before + 1);
        }
        drop(observer);
        drop(store);
        remove_observed_speed_test_store(&path);
    }

    fn insert_usage_row(
        store: &SqliteRouteStateStore,
        id: &str,
        route_key: &str,
        output_tokens: i64,
        duration_ms: i64,
        observed_at: i64,
    ) {
        store
            .store
            .with_conn(|connection| {
                connection.execute(
                    "INSERT INTO model_route_usage
                       (id, task_id, route_key, capacity_key, provider_id,
                        input_tokens, output_tokens, duration_ms, observed_at)
                     VALUES (?1, NULL, ?2, ?2, 'test', NULL, ?3, ?4, ?5)",
                    params![id, route_key, output_tokens, duration_ms, observed_at],
                )
            })
            .unwrap();
    }

    fn old_observed_speed_aggregates(
        store: &SqliteRouteStateStore,
        now: f64,
    ) -> Vec<ObservedSpeedAggregate> {
        let mut aggregates = store
            .store
            .with_conn(|connection| {
                let mut statement = connection.prepare(OLD_OBSERVED_SPEED_AGGREGATE_SQL)?;
                let rows = statement.query_map(
                    params![
                        (now - OBSERVED_SPEED_WINDOW_MS).floor() as i64,
                        now.floor() as i64,
                        USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                        USAGE_MIN_DURATION_MS.floor() as i64,
                        OBSERVED_SPEED_SAMPLE_LIMIT,
                    ],
                    |row| {
                        Ok(ObservedSpeedAggregate {
                            route_key: row.get(0)?,
                            sample_count: row.get(1)?,
                            output_tokens: row.get(2)?,
                            duration_ms: row.get(3)?,
                            // The SQL oracle checks row selection only; the speed is
                            // compared separately after `without_speed`.
                            speed: None,
                        })
                    },
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        aggregates.sort_by(|left, right| left.route_key.cmp(&right.route_key));
        aggregates
    }

    /// Drop the computed speed so bounded-seek aggregates can be compared with the
    /// SQL oracle, which reproduces row selection and sums but not the median.
    fn without_speed(aggregates: &[ObservedSpeedAggregate]) -> Vec<ObservedSpeedAggregate> {
        aggregates
            .iter()
            .map(|aggregate| ObservedSpeedAggregate {
                speed: None,
                ..aggregate.clone()
            })
            .collect()
    }

    #[test]
    fn bounded_observed_speed_aggregates_match_the_shipped_window_oracle() {
        let (path, store) = observed_speed_test_store("equivalence");
        let now = OBSERVED_SPEED_WINDOW_MS + 10_000.0;
        let window_start = (now - OBSERVED_SPEED_WINDOW_MS) as i64;
        let route_keys = BTreeSet::from([
            "test/more-than-limit".to_string(),
            "test/exactly-limit".to_string(),
            "test/insufficient".to_string(),
            "test/zero-eligible".to_string(),
        ]);

        for index in 0..25_i64 {
            insert_usage_row(
                &store,
                &format!("more-{index}"),
                "test/more-than-limit",
                1_000 + index,
                1_000,
                window_start + 100 + index,
            );
        }
        for index in 0..OBSERVED_SPEED_SAMPLE_LIMIT {
            insert_usage_row(
                &store,
                &format!("exact-{index}"),
                "test/exactly-limit",
                2_000,
                1_000,
                window_start + 200 + index,
            );
        }
        for index in 0..(OBSERVED_SPEED_MIN_SAMPLES as i64 - 1) {
            insert_usage_row(
                &store,
                &format!("insufficient-{index}"),
                "test/insufficient",
                3_000,
                1_000,
                window_start + 300 + index,
            );
        }

        insert_usage_row(
            &store,
            "more-outside-window",
            "test/more-than-limit",
            50_000,
            1_000,
            window_start - 1,
        );
        insert_usage_row(
            &store,
            "more-below-token-floor",
            "test/more-than-limit",
            USAGE_MIN_OUTPUT_TOKENS as i64 - 1,
            1_000,
            window_start + 1_000,
        );
        insert_usage_row(
            &store,
            "more-below-duration-floor",
            "test/more-than-limit",
            50_000,
            USAGE_MIN_DURATION_MS as i64 - 1,
            window_start + 1_001,
        );
        insert_usage_row(
            &store,
            "zero-outside-window",
            "test/zero-eligible",
            50_000,
            1_000,
            window_start - 1,
        );
        insert_usage_row(
            &store,
            "zero-below-token-floor",
            "test/zero-eligible",
            USAGE_MIN_OUTPUT_TOKENS as i64 - 1,
            1_000,
            window_start + 1_100,
        );
        insert_usage_row(
            &store,
            "zero-below-duration-floor",
            "test/zero-eligible",
            50_000,
            USAGE_MIN_DURATION_MS as i64 - 1,
            window_start + 1_101,
        );

        let expected = old_observed_speed_aggregates(&store, now);
        let actual = store
            .get_observed_speed_aggregates(&route_keys, now)
            .unwrap();
        assert_eq!(without_speed(&actual), expected);
        let sample_count = |route_key: &str| {
            actual
                .iter()
                .find(|aggregate| aggregate.route_key == route_key)
                .unwrap()
                .sample_count
        };
        assert_eq!(
            sample_count("test/more-than-limit"),
            OBSERVED_SPEED_SAMPLE_LIMIT
        );
        assert_eq!(
            sample_count("test/exactly-limit"),
            OBSERVED_SPEED_SAMPLE_LIMIT
        );
        assert_eq!(
            sample_count("test/insufficient"),
            OBSERVED_SPEED_MIN_SAMPLES as i64 - 1
        );

        let trusted = store.get_observed_speeds(&route_keys, now).unwrap();
        assert_eq!(trusted.len(), 2);
        assert!(trusted.contains_key("test/more-than-limit"));
        assert!(trusted.contains_key("test/exactly-limit"));
        assert!(!trusted.contains_key("test/insufficient"));
        assert!(!trusted.contains_key("test/zero-eligible"));

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    fn observed_speed_and_class_vectors_share_the_trailing_window_bounds() {
        // Both read `(now - 7d, now]` from the same usage table: a row exactly
        // seven days old is in neither population. The class-vector side of the
        // bound is pinned by the integration test
        // `complete_ephemeral_row_trailing_window_boundary_pinned_to_now_argument`.
        let (path, store) = observed_speed_test_store("window-bounds");
        let now = OBSERVED_SPEED_WINDOW_MS + 10_000.0;
        let window_start = (now - OBSERVED_SPEED_WINDOW_MS) as i64;
        assert_eq!(OBSERVED_SPEED_WINDOW_MS, CLASS_VECTOR_TRAILING_WINDOW_MS);
        insert_usage_row(
            &store,
            "at-start",
            "test/bounds",
            2_000,
            1_000,
            window_start,
        );
        insert_usage_row(
            &store,
            "after-start",
            "test/bounds",
            2_000,
            1_000,
            window_start + 1,
        );

        let aggregates = store
            .get_observed_speed_aggregates(&BTreeSet::from(["test/bounds".to_string()]), now)
            .unwrap();
        assert_eq!(aggregates.len(), 1);
        assert_eq!(
            aggregates[0].sample_count, 1,
            "the row exactly seven days old is outside the speed window"
        );

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    #[ignore = "prints a local wall-time comparison for the shipped and bounded query shapes"]
    fn observed_speed_bounded_seek_benchmark() {
        let (path, store) = observed_speed_test_store("benchmark");
        let now = OBSERVED_SPEED_WINDOW_MS + 200_000.0;
        let window_start = (now - OBSERVED_SPEED_WINDOW_MS) as i64;
        store
            .store
            .with_conn(|connection| {
                connection.execute(
                    "WITH RECURSIVE sequence(n) AS (
                       VALUES(1)
                       UNION ALL
                       SELECT n + 1 FROM sequence WHERE n < 100000
                     )
                     INSERT INTO model_route_usage
                       (id, task_id, route_key, capacity_key, provider_id,
                        input_tokens, output_tokens, duration_ms, observed_at)
                     SELECT printf('benchmark-%d', n), NULL, 'benchmark/route',
                            'benchmark/route', 'benchmark', NULL, 1000, 1000, ?1 + n
                     FROM sequence",
                    [window_start],
                )
            })
            .unwrap();

        let shipped_started = std::time::Instant::now();
        let expected = old_observed_speed_aggregates(&store, now);
        let shipped_elapsed = shipped_started.elapsed();
        let route_keys = BTreeSet::from(["benchmark/route".to_string()]);
        let bounded_started = std::time::Instant::now();
        let actual = store
            .get_observed_speed_aggregates(&route_keys, now)
            .unwrap();
        let bounded_elapsed = bounded_started.elapsed();
        assert_eq!(without_speed(&actual), expected);
        println!(
            "observed_speed_benchmark rows=100000 shipped_ms={:.3} bounded_ms={:.3}",
            shipped_elapsed.as_secs_f64() * 1_000.0,
            bounded_elapsed.as_secs_f64() * 1_000.0,
        );

        drop(store);
        remove_observed_speed_test_store(&path);
    }
}
