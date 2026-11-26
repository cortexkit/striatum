//! Astrocyte plan joins and the model-specific projection of measured values.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock},
};

use serde::Deserialize;
use serde_json::Value;
use striatum_core::{
    quota::{RateWindow, Usage},
    subscription::{plan_pressure, PlanMeasurements, SubscriptionPool},
};

use super::{measured, parse_rfc3339_ms, price_matches_login};

pub(super) const FRESH_QUOTA_MS: i64 = 15 * 60_000;

#[derive(Default)]
pub(super) struct ReportPlans<'a>(Vec<JoinedPlan<'a>>);

pub(super) struct JoinedPlan<'a> {
    pub(super) name: &'a str,
    // Legacy reports promised one row per plan, not one row per pool.
    pub(super) quota_provider: Option<&'a str>,
    pub(super) rows: Vec<&'a Value>,
}

impl<'a> ReportPlans<'a> {
    pub(super) fn new(report: &'a Value) -> Self {
        let mut scoped = BTreeMap::<(&str, &str), BTreeMap<&str, &Value>>::new();
        let mut legacy = Vec::new();
        for row in report["latest"].as_array().into_iter().flatten() {
            let Some(name) = row["plan"].as_str() else {
                continue;
            };
            if row["pool_scope"].is_null() {
                legacy.push(JoinedPlan {
                    name,
                    quota_provider: None,
                    rows: vec![row],
                });
            } else if let (Some(provider), Some(pool)) =
                (row["quota_provider"].as_str(), row["pool_id"].as_str())
            {
                scoped
                    .entry((provider, name))
                    .or_default()
                    .insert(pool, row);
            }
        }
        // A scoped plan's metadata may be repeated on every pool. That is one
        // login-to-plan match, unlike a login listed under two distinct plans.
        legacy.retain(|plan| {
            plan.rows[0]["quota_provider"]
                .as_str()
                .is_none_or(|provider| !scoped.contains_key(&(provider, plan.name)))
        });
        legacy.extend(
            scoped
                .into_iter()
                .map(|((provider, name), rows)| JoinedPlan {
                    name,
                    quota_provider: Some(provider),
                    rows: rows.into_values().collect(),
                }),
        );
        Self(legacy)
    }

    fn by_price(&self, matches: impl Fn(&Value) -> bool) -> Result<Option<&JoinedPlan<'a>>, ()> {
        let mut plans = self.0.iter().filter(|plan| {
            plan.rows.iter().any(|row| {
                row["plan_prices"]
                    .as_array()
                    .is_some_and(|prices| prices.iter().any(&matches))
            })
        });
        let first = plans.next();
        if plans.next().is_some() {
            Err(())
        } else {
            Ok(first)
        }
    }

    pub(super) fn account(
        &self,
        account_id: &str,
        credential_ref: Option<&str>,
    ) -> Result<Option<&JoinedPlan<'a>>, ()> {
        self.by_price(|price| price_matches_login(price, account_id, credential_ref))
    }

    pub(super) fn credential(&self, id: &str) -> Option<&JoinedPlan<'a>> {
        self.by_price(|price| price["credential_ref"] == id)
            .ok()
            .flatten()
    }
}

#[derive(Deserialize)]
struct CatalogRow {
    provider_id: String,
    pool_id: String,
    models: Vec<String>,
}

#[derive(Default)]
pub(super) struct CatalogPools(BTreeMap<(String, String), BTreeSet<String>>);

impl CatalogPools {
    pub(super) fn new(prices: &Value) -> Self {
        Self(
            prices["quota_pools"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|row| {
                    let row: CatalogRow = serde_json::from_value(row.clone()).ok()?;
                    let models = row
                        .models
                        .iter()
                        .map(|id| model_key(&row.provider_id, id).into_owned())
                        .collect();
                    Some(((row.provider_id, row.pool_id), models))
                })
                .collect(),
        )
    }
}

// A catalog pool names the routing model keys directly. Do not strip a
// provider prefix or guess an alias for a model the list doesn't name.
fn model_key<'a>(provider: &str, id: &'a str) -> Cow<'a, str> {
    if id.contains('/') {
        Cow::Borrowed(id)
    } else {
        format!("{provider}/{id}").into()
    }
}

#[derive(Clone)]
enum Coverage {
    All,
    Models {
        provider: String,
        models: BTreeSet<String>,
    },
}

impl Coverage {
    fn for_row(catalog: &CatalogPools, row: &Value, pool: &str) -> Option<Self> {
        match row["pool_scope"].as_str()? {
            "all" => Some(Self::All),
            "subset" => {
                let provider = row["catalog_provider"].as_str()?;
                let key = (provider.to_owned(), pool.to_owned());
                let models = catalog.0.get(&key)?;
                if let Some(reported) = row["pool_models"].as_array() {
                    let reported: BTreeSet<_> = reported
                        .iter()
                        .filter_map(Value::as_str)
                        .map(|id| model_key(provider, id).into_owned())
                        .collect();
                    if reported != *models {
                        warn_disagreement(key, models, &reported);
                    }
                }
                Some(Self::Models {
                    provider: provider.into(),
                    models: models.clone(),
                })
            }
            _ => None,
        }
    }

    fn covers(&self, model: &str) -> bool {
        match self {
            Self::All => true,
            Self::Models { provider, models } => {
                models.contains(model_key(provider, model).as_ref())
            }
        }
    }
}

fn warn_disagreement(
    key: (String, String),
    catalog: &BTreeSet<String>,
    reported: &BTreeSet<String>,
) {
    static WARNED: OnceLock<Mutex<BTreeSet<(String, String)>>> = OnceLock::new();
    let mut warned = WARNED.get_or_init(Default::default).lock().unwrap();
    if !warned.contains(&key) {
        tracing::warn!(
            catalog_provider = key.0,
            pool_id = key.1,
            ?catalog,
            ?reported,
            "Astrocyte and Fusiform disagree on quota pool coverage; using Fusiform"
        );
        warned.insert(key);
    }
}

#[derive(Clone)]
struct PoolValue {
    id: String,
    minutes: Option<i64>,
    coverage: Coverage,
    measurements: PlanMeasurements,
    readings: Vec<PoolReading>,
}

#[derive(Clone)]
pub(super) struct PlanPools(Vec<PoolValue>);

impl JoinedPlan<'_> {
    pub(super) fn values(
        &self,
        catalog: &CatalogPools,
    ) -> (Option<PlanMeasurements>, Option<PlanPools>) {
        if self.quota_provider.is_none() {
            return (measured(self.rows[0]), None);
        }
        let mut pools: Vec<_> = self
            .rows
            .iter()
            .filter_map(|row| {
                if row["eligibility_reasons"]
                    .as_array()
                    .is_some_and(|reasons| {
                        reasons.iter().any(|reason| reason == "pool_list_missing")
                    })
                {
                    return None;
                }
                let id = row["pool_id"].as_str()?;
                let coverage = Coverage::for_row(catalog, row, id)?;
                Some(PoolValue {
                    id: id.to_owned(),
                    minutes: row["window_minutes"].as_i64(),
                    coverage,
                    measurements: measured(row)?,
                    readings: Vec::new(),
                })
            })
            .collect();
        match pools.as_slice() {
            [] => (None, None),
            [PoolValue {
                coverage: Coverage::All,
                ..
            }] => (Some(pools.remove(0).measurements), None),
            _ => (None, Some(PlanPools(pools))),
        }
    }
}

impl PlanPools {
    pub(super) fn observe(&mut self, usage: &Usage, fetched_at_ms: i64) {
        for pool in &mut self.0 {
            let matches: Vec<_> = [
                ("primary", usage.primary.as_ref()),
                ("secondary", usage.secondary.as_ref()),
                ("tertiary", usage.tertiary.as_ref()),
            ]
            .into_iter()
            .chain(
                usage
                    .extra_rate_windows
                    .iter()
                    .flatten()
                    .filter_map(|extra| Some((extra.id.as_deref()?, extra.window.as_ref()))),
            )
            .filter_map(|(id, window)| (id == pool.id).then_some(window).flatten())
            .filter(|window| {
                pool.minutes
                    .is_none_or(|n| window.window_minutes == Some(n))
            })
            .collect();
            if let [window] = matches.as_slice() {
                if let Some(reading) = PoolReading::new(window, fetched_at_ms) {
                    pool.readings.push(reading);
                }
            }
        }
    }

    fn measured_for(&self, model: &str, now: i64) -> Option<&PlanMeasurements> {
        self.0
            .iter()
            .filter_map(|pool| {
                let value = pool.measurements.multiplier_at(now)?;
                pool.coverage
                    .covers(model)
                    .then(|| (pool, value, pool.pressure(now)))
            })
            .max_by(
                |(_, left_value, left_pressure), (_, right_value, right_pressure)| {
                    // `None` sorts below live pressure. Equal pressure (including two
                    // missing readings) picks the lower and therefore costlier value.
                    left_pressure
                        .partial_cmp(right_pressure)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| right_value.total_cmp(left_value))
                },
            )
            .map(|(pool, _, _)| &pool.measurements)
    }
}

#[derive(Clone)]
struct PoolReading {
    fetched_at_ms: i64,
    resets_at_ms: i64,
    window_minutes: i64,
    headroom_share: f64,
}

impl PoolReading {
    fn new(window: &RateWindow, fetched_at_ms: i64) -> Option<Self> {
        let used = window.raw_used_percent.unwrap_or(window.used_percent);
        let window_minutes = window.window_minutes.filter(|&minutes| minutes > 0)?;
        Some(Self {
            fetched_at_ms,
            resets_at_ms: parse_rfc3339_ms(window.resets_at.as_deref()?)?,
            window_minutes,
            headroom_share: 1.0 - used.clamp(0.0, 100.0) / 100.0,
        })
        .filter(|_| used.is_finite())
    }
}

impl PoolValue {
    fn pressure(&self, now: i64) -> Option<f64> {
        let (mut headroom, mut time, mut count) = (0.0, 0.0_f64, 0);
        for reading in &self.readings {
            if !now
                .checked_sub(reading.fetched_at_ms)
                .is_some_and(|age| (0..=FRESH_QUOTA_MS).contains(&age))
                || reading.resets_at_ms <= now
            {
                continue;
            }
            headroom += reading.headroom_share;
            time = time.max(
                (reading.resets_at_ms - now) as f64 / (reading.window_minutes as f64 * 60_000.0),
            );
            count += 1;
        }
        (count > 0).then(|| plan_pressure(time, headroom / f64::from(count)))
    }
}

// The index is the position in SubscriptionPool::plans. Coverage changes the
// measured value only, not account budgets, pressure or plan pricing weights.
pub(super) type ProviderValues = BTreeMap<usize, PlanPools>;

pub(super) fn project(
    mut pool: Arc<SubscriptionPool>,
    values: &ProviderValues,
    model: &str,
    now: i64,
) -> Arc<SubscriptionPool> {
    for (&index, values) in values {
        let measurement = values.measured_for(model, now);
        if pool.plans[index].measured.as_ref() != measurement {
            Arc::make_mut(&mut pool).plans[index].measured = measurement.cloned();
        }
    }
    pool
}
