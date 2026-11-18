//! The core store trait and the writes that enforce route health.
//! Keeping cooldowns and failure backoff here lets route updates and decision
//! outcomes apply the same fence and inheritance rules.

use super::*;
use crate::block_storage::keyed_updates;
use crate::catalog::row_to_fact;

pub struct ClearedRouteCooldowns {
    pub route_key: String,
    pub routes: Vec<(String, CooldownState)>,
    pub account_fences: Vec<AccountRouteFence>,
}

pub(super) fn row_wall_class(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<RouteWallClass>> {
    row.get::<_, Option<String>>(index)?
        .map(|value| {
            RouteWallClass::parse(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
                )
            })
        })
        .transpose()
}

#[derive(Default)]
struct ExistingProviderState {
    cooldown_until: Option<f64>,
    quota_exhausted_until: Option<f64>,
    quota_pressure: Option<f64>,
    wall_class: Option<RouteWallClass>,
    last_error: Option<String>,
    last_error_at: Option<i64>,
    wall_unscoped: bool,
    wall_observed_at: Option<i64>,
}

#[derive(Default)]
struct ExistingState {
    cooldown_until: Option<f64>,
    quota_exhausted_until: Option<f64>,
    wall_class: Option<RouteWallClass>,
    quota_pressure: Option<f64>,
    latency_ema_ms: Option<f64>,
    success_ema: Option<f64>,
    failure_count: i64,
    quota_error_count: i64,
    last_error: Option<String>,
    last_error_at: Option<i64>,
    wall_unscoped: bool,
}

struct InheritedRouteState {
    route_key: String,
    model_id: String,
    wall_observed_at: Option<i64>,
    last_error: Option<String>,
    last_error_at: Option<i64>,
    wall_class: Option<String>,
    wall_unscoped: bool,
}

/// Whether a stored wall was written from a permanent refusal of one named
/// credential. Such a wall is not provider or route evidence: the credential's
/// fence covers it, so it neither blocks nor lends its horizon to a later write.
pub(crate) fn credential_refusal_wall(last_error: Option<&str>) -> bool {
    last_error
        .is_some_and(|error| striatum_core::account_wall::credential_refusal_in(error).is_some())
}

/// `max` over two optional times, treating `None` as "no value" (the defined one
/// wins; the larger of two defined values wins). Mirrors the TS `maxDefined`.
pub(crate) fn max_defined(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, b) => b,
    }
}

/// Split `provider/model[/variant]` into its parts. A key with three or more
/// segments is ambiguous: `p/a/b` is model `a/b` with no variant or model `a`
/// with variant `b`. This reads it as the latter, so it is only the fallback
/// for a route the catalog does not hold; see `route_identity`.
fn split_route_key(route_key: &str) -> (String, String, Option<String>) {
    let parts: Vec<&str> = route_key.split('/').collect();
    if parts.len() >= 3 {
        (
            parts[0].to_string(),
            parts[1].to_string(),
            Some(parts[2..].join("/")),
        )
    } else {
        (
            parts.first().copied().unwrap_or("").to_string(),
            parts.get(1).copied().unwrap_or("").to_string(),
            None,
        )
    }
}

/// The provider, model and variant behind `route_key`. A catalog route's stored
/// fact is authoritative, since its model id may itself contain `/`. Without a
/// fact, a caller that knows the provider and model (a decision records both)
/// passes them as `known_model`, and the variant is whatever the key adds after
/// them. With no catalog fact, and no `known_model` that the key starts with,
/// the key falls back to `split_route_key`.
fn route_identity(
    connection: &Connection,
    route_key: &str,
    known_model: Option<(&str, &str)>,
) -> rusqlite::Result<(String, String, Option<String>)> {
    if let Some(identity) = connection
        .query_row(
            "SELECT provider_id, model_id, variant FROM model_facts WHERE route_key = ?1",
            [route_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
    {
        return Ok(identity);
    }
    if let Some((provider_id, model_id)) = known_model {
        let capacity_key = striatum_core::make_capacity_key(provider_id, model_id);
        if route_key == capacity_key {
            return Ok((provider_id.to_owned(), model_id.to_owned(), None));
        }
        if let Some(variant) = route_key
            .strip_prefix(capacity_key.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
        {
            return Ok((
                provider_id.to_owned(),
                model_id.to_owned(),
                Some(variant.to_owned()),
            ));
        }
    }
    Ok(split_route_key(route_key))
}

impl SqliteRouteStateStore {
    /// Clear a stored route, or every variant under a base model, atomically.
    /// A known key is used as-is; otherwise its final colon-separated suffix
    /// is parsed as a variant.
    pub fn clear_route_cooldowns(
        &self,
        requested: &str,
    ) -> RouteStateResult<Option<ClearedRouteCooldowns>> {
        let requested = requested.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.clear_route_cooldowns(&requested)
        })
    }

    /// Record that the provider permanently refused `credential_id`.
    ///
    /// A repeated report for the same pair renews the observation time and the
    /// reported code, so a fence that keeps failing stays fresh and its next
    /// recheck probe waits another interval; the fingerprint it was recorded
    /// against is kept, so a replay cannot move the fence onto a newer listing.
    /// A fingerprint is filled in only where none was known.
    pub fn record_credential_entitlement_fence(
        &self,
        refusal: &striatum_core::route_state_store::CredentialRefusal,
        listing_fingerprint: Option<&str>,
        observed_at_ms: i64,
    ) -> RouteStateResult<()> {
        let refusal = refusal.to_owned();
        let listing_fingerprint = listing_fingerprint.map(str::to_owned);
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.record_credential_entitlement_fence(
                &refusal,
                listing_fingerprint.as_deref(),
                observed_at_ms,
            )
        })
    }

    pub fn credential_entitlement_fences(
        &self,
    ) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
        self.account_read(|tx| Ok(tx.current_blocks()?.entitlement_fences()))
    }

    /// Replace the refusal timestamp while preserving its code and listing
    /// fingerprint. Reschedule its next permitted retry in the same transaction;
    /// otherwise an older replacement timestamp can look like an expired fence
    /// imported from a store that never recorded scheduled retries.
    /// Admitting a retry uses the separate claim operation, not this setter.
    pub fn touch_credential_entitlement_fence(
        &self,
        provider_id: &str,
        credential_id: &str,
        observed_at_ms: i64,
    ) -> RouteStateResult<usize> {
        let provider_id = provider_id.to_owned();
        let credential_id = credential_id.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.touch_credential_entitlement_fence(&provider_id, &credential_id, observed_at_ms)
        })
    }

    /// End the fence on one (provider, credential) pair, as a successful run on
    /// that pair proves the credential serves again; returns how many ended.
    pub fn clear_credential_entitlement_fence(
        &self,
        provider_id: &str,
        credential_id: &str,
    ) -> RouteStateResult<usize> {
        let provider_id = provider_id.to_owned();
        let credential_id = credential_id.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.clear_credential_entitlement_fence_count(&provider_id, &credential_id)
        })
    }

    /// End every fence naming `credential_id`, on every provider it is fenced
    /// for, as an operator does right after topping the credential up; returns
    /// the fences that were lifted, ordered by provider, so the reply can name
    /// what the lift removed.
    pub fn clear_credential_entitlement_fences_for_credential(
        &self,
        credential_id: &str,
    ) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
        let credential_id = credential_id.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.clear_credential_entitlement_fences_for_credential(&credential_id)
        })
    }

    /// End every fence on `credential_id` that was recorded against
    /// `fingerprint` (or against no listing at all); returns how many ended.
    pub fn clear_credential_entitlement_fences(
        &self,
        credential_id: &str,
        fingerprint: Option<&str>,
    ) -> RouteStateResult<usize> {
        let credential_id = credential_id.to_owned();
        let fingerprint = fingerprint.map(str::to_owned);
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.clear_credential_entitlement_fences(&credential_id, fingerprint.as_deref())
        })
    }

    /// Record the listing a fence recorded without one was first seen against.
    pub fn adopt_credential_entitlement_fingerprint(
        &self,
        credential_id: &str,
        fingerprint: &str,
    ) -> RouteStateResult<()> {
        let credential_id = credential_id.to_owned();
        let fingerprint = fingerprint.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.adopt_credential_entitlement_fingerprint(&credential_id, &fingerprint)
        })
    }
}

impl RouteStateStore for SqliteRouteStateStore {
    fn upsert_fact(&self, fact: &ModelFact, source: FactSource) -> RouteStateResult<()> {
        let fact = fact.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.upsert_fact(&fact, source)
        })
    }

    fn mark_operator_fact_fields(
        &self,
        route_key: &str,
        fields: &[CatalogFactField],
    ) -> RouteStateResult<()> {
        let marked = |field| {
            if fields.contains(&field) {
                Some("override")
            } else {
                None
            }
        };
        self.store
            .with_conn(|connection: &Connection| {
                connection.execute(
                    "UPDATE model_facts SET
                       context_window_source = COALESCE(?2, context_window_source),
                       max_output_tokens_source = COALESCE(?3, max_output_tokens_source),
                       cost_tier_source = COALESCE(?4, cost_tier_source),
                       cost_per_mtok_out_source = COALESCE(?5, cost_per_mtok_out_source),
                       supports_vision_source = COALESCE(?6, supports_vision_source),
                       supports_tool_use_source = COALESCE(?7, supports_tool_use_source)
                     WHERE route_key = ?1",
                    params![
                        route_key,
                        marked(CatalogFactField::ContextWindow),
                        marked(CatalogFactField::MaxOutputTokens),
                        marked(CatalogFactField::CostTier),
                        marked(CatalogFactField::CostPerMtokOut),
                        marked(CatalogFactField::SupportsVision),
                        marked(CatalogFactField::SupportsToolUse),
                    ],
                )?;
                Ok(())
            })
            .map_err(err)
    }

    fn clear_operator_fact_fields(
        &self,
        route_key: &str,
        fields: &[CatalogFactField],
    ) -> RouteStateResult<Vec<CatalogFactField>> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        self.store
            .with_conn(|connection: &Connection| {
                let tx = write_transaction(connection)?;
                let sources = tx
                    .query_row(
                        "SELECT context_window_source, max_output_tokens_source,
                                cost_tier_source, cost_per_mtok_out_source,
                                supports_vision_source, supports_tool_use_source
                         FROM model_facts WHERE route_key = ?1",
                        [route_key],
                        |row| {
                            Ok((
                                row.get::<_, Option<String>>(0)?,
                                row.get::<_, Option<String>>(1)?,
                                row.get::<_, Option<String>>(2)?,
                                row.get::<_, Option<String>>(3)?,
                                row.get::<_, Option<String>>(4)?,
                                row.get::<_, Option<String>>(5)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some(sources) = sources else {
                    crate::connection_store::timed_commit(tx)?;
                    return Ok(Vec::new());
                };
                let candidates = [
                    (CatalogFactField::ContextWindow, sources.0),
                    (CatalogFactField::MaxOutputTokens, sources.1),
                    (CatalogFactField::CostTier, sources.2),
                    (CatalogFactField::CostPerMtokOut, sources.3),
                    (CatalogFactField::SupportsVision, sources.4),
                    (CatalogFactField::SupportsToolUse, sources.5),
                ];
                let cleared = candidates
                    .into_iter()
                    .filter_map(|(field, source)| {
                        (fields.contains(&field) && source.as_deref() == Some("override"))
                            .then_some(field)
                    })
                    .collect::<Vec<_>>();
                let should_clear = |field| i64::from(cleared.contains(&field));
                tx.execute(
                    "UPDATE model_facts SET
                       context_window_source = CASE
                         WHEN ?2 = 1 AND context_window_source = 'override' THEN NULL
                         ELSE context_window_source END,
                       max_output_tokens_source = CASE
                         WHEN ?3 = 1 AND max_output_tokens_source = 'override' THEN NULL
                         ELSE max_output_tokens_source END,
                       cost_tier_source = CASE
                         WHEN ?4 = 1 AND cost_tier_source = 'override' THEN NULL
                         ELSE cost_tier_source END,
                       cost_per_mtok_out_source = CASE
                         WHEN ?5 = 1 AND cost_per_mtok_out_source = 'override' THEN NULL
                         ELSE cost_per_mtok_out_source END,
                       supports_vision_source = CASE
                         WHEN ?6 = 1 AND supports_vision_source = 'override' THEN NULL
                         ELSE supports_vision_source END,
                       supports_tool_use_source = CASE
                         WHEN ?7 = 1 AND supports_tool_use_source = 'override' THEN NULL
                         ELSE supports_tool_use_source END
                     WHERE route_key = ?1",
                    params![
                        route_key,
                        should_clear(CatalogFactField::ContextWindow),
                        should_clear(CatalogFactField::MaxOutputTokens),
                        should_clear(CatalogFactField::CostTier),
                        should_clear(CatalogFactField::CostPerMtokOut),
                        should_clear(CatalogFactField::SupportsVision),
                        should_clear(CatalogFactField::SupportsToolUse),
                    ],
                )?;
                crate::connection_store::timed_commit(tx)?;
                Ok(cleared)
            })
            .map_err(err)
    }

    fn set_model_family(
        &self,
        route_key: &str,
        family: Option<GrandFamily>,
        source: FamilySource,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|connection: &Connection| {
                connection.execute(
                    "UPDATE model_facts SET family = ?2, family_source = ?3, updated_at = ?4
                     WHERE route_key = ?1",
                    params![
                        route_key,
                        family.map(GrandFamily::as_str),
                        source.as_str(),
                        now_ms()
                    ],
                )?;
                Ok(())
            })
            .map_err(err)
    }

    fn get_model_family(
        &self,
        route_key: &str,
    ) -> RouteStateResult<Option<(Option<GrandFamily>, FamilySource)>> {
        self.store
            .with_conn(|connection: &Connection| {
                connection
                    .query_row(
                        "SELECT family, family_source FROM model_facts WHERE route_key = ?1",
                        [route_key],
                        |row| {
                            let family = row
                                .get::<_, Option<String>>(0)?
                                .as_deref()
                                .and_then(GrandFamily::from_wire);
                            let source_raw = row.get::<_, String>(1)?;
                            let source = FamilySource::from_wire(&source_raw)
                                .unwrap_or(FamilySource::Absent);
                            Ok((family, source))
                        },
                    )
                    .optional()
            })
            .map_err(err)
    }

    fn get_facts(&self) -> RouteStateResult<Vec<ModelFact>> {
        // Routes of one model share its operator score (keyed on Fusiform's
        // canonical id). `get_fact_by_route_key` stays the raw row, so a write
        // that merges over it never persists a borrowed score as the route's own.
        Ok(self
            .read_resolved_fact_rows()?
            .into_iter()
            .filter_map(|(fact, present)| present.then_some(fact))
            .collect())
    }

    fn get_fact_by_route_key(&self, route_key: &str) -> RouteStateResult<Option<ModelFact>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare("SELECT * FROM model_facts WHERE route_key = ?1")?;
                let mut rows = stmt.query_map([route_key], row_to_fact)?;
                match rows.next() {
                    Some(r) => Ok(Some(r?)),
                    None => Ok(None),
                }
            })
            .map_err(err)
    }

    fn list_override_route_keys(&self) -> RouteStateResult<Vec<String>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt =
                    c.prepare("SELECT route_key FROM model_facts WHERE source = 'override'")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    fn delete_fact(&self, route_key: &str) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = write_transaction(c)?;
                tx.execute("DELETE FROM model_facts WHERE route_key = ?1", [route_key])?;
                crate::connection_store::timed_commit(tx)
            })
            .map_err(err)
    }

    /// Withdraw an override without deleting the row: the fact stays in the
    /// catalog under source 'seed' so migration-seeded models survive their
    /// config override's removal. Rows leave the catalog by migration only.
    fn demote_fact_source_to_seed(&self, route_key: &str) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = write_transaction(c)?;
                tx.execute(
                    "UPDATE model_facts SET source = 'seed' WHERE route_key = ?1",
                    [route_key],
                )?;
                crate::connection_store::timed_commit(tx)
            })
            .map_err(err)
    }

    fn get_capacity_keys_for_provider(&self, provider_id: &str) -> RouteStateResult<Vec<String>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT DISTINCT capacity_key FROM model_facts WHERE provider_id = ?1",
                )?;
                let rows = stmt.query_map([provider_id], |r| r.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    fn get_provider_cooldown(
        &self,
        provider_key: &str,
    ) -> RouteStateResult<Option<ProviderCooldownState>> {
        // Cold/raw reads join durable block deadlines to telemetry. Live
        // requests use the published BlockState's copy of the same fields.
        let provider_key = route_snapshot::provider_lookup_key(provider_key)?.to_owned();
        self.store
            .with_conn(|c: &Connection| {
                Ok(route_snapshot::read_provider_cooldown(c, &provider_key))
            })
            .map_err(err)?
    }

    fn record_provider_cooldown(
        &self,
        provider_key: &str,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        let provider_key = provider_key.to_owned();
        let route_key = route_key.to_owned();
        let capacity_key = capacity_key.to_owned();
        let mut update = update.to_owned();
        update.observed_at_ms.get_or_insert_with(now_ms);
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.record_provider_cooldown(&provider_key, &route_key, &capacity_key, &update)
        })
    }

    fn update_route_state(
        &self,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        let route_key = route_key.to_owned();
        let capacity_key = capacity_key.to_owned();
        let mut update = update.to_owned();
        update.observed_at_ms.get_or_insert_with(now_ms);
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.update_route_state(&route_key, &capacity_key, &update)
        })
    }

    fn record_decision(&self, input: &DecisionInput) -> RouteStateResult<String> {
        self.record_decision_with_selection(input, None, None)
    }

    fn set_decision_outcome(
        &self,
        decision_id: &str,
        update: &DecisionOutcomeUpdate,
    ) -> RouteStateResult<()> {
        let decision_id = decision_id.to_owned();
        let update = update.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.set_decision_outcome(&decision_id, &update)
        })
    }

    fn clear_failure_backoff(
        &self,
        route_key: &str,
    ) -> RouteStateResult<Option<FailureBackoffState>> {
        let route_key = route_key.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.clear_failure_backoff(&route_key)
        })
    }

    fn clear_cooldown(&self, route_key: &str) -> RouteStateResult<Option<CooldownState>> {
        Ok(self.clear_route_cooldowns(route_key)?.map(|cleared| {
            cleared
                .routes
                .into_iter()
                .fold(CooldownState::default(), |mut total, (_, row)| {
                    total.cooldown_until = max_defined(total.cooldown_until, row.cooldown_until);
                    total.quota_exhausted_until =
                        max_defined(total.quota_exhausted_until, row.quota_exhausted_until);
                    total
                })
        }))
    }

    fn get_decisions_for_task(&self, task_id: &str) -> RouteStateResult<Vec<DecisionRow>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT id, decision_kind, task_id, send_id, target_agent, selected_route_key,
                              rejected_json, explanation_json, quota_signal_json, policy_revision,
                              catalog_fingerprint, outcome, created_at
                      FROM model_route_decisions WHERE task_id=?1 ORDER BY created_at",
                )?;
                let rows = stmt.query_map([task_id], |r| {
                    Ok(DecisionRow {
                        id: r.get(0)?,
                        decision_kind: r.get(1)?,
                        task_id: r.get(2)?,
                        send_id: r.get(3)?,
                        target_agent: r.get(4)?,
                        selected_route_key: r.get(5)?,
                        rejected_json: r.get(6)?,
                        explanation_json: r.get(7)?,
                        quota_signal_json: r.get(8)?,
                        policy_revision: r.get(9)?,
                        catalog_fingerprint: r.get(10)?,
                        outcome: r.get(11)?,
                        created_at: r.get(12)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    fn delete_decisions_older_than(
        &self,
        created_before_ms: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = write_transaction(c)?;
                let panels = tx.execute(
                    "DELETE FROM routing_v1_panel WHERE panel_id IN (
                        SELECT panel_id FROM routing_v1_panel WHERE created_at_ms < ?1
                        ORDER BY created_at_ms LIMIT ?2)",
                    params![created_before_ms, i64::try_from(limit).unwrap_or(i64::MAX)],
                )?;
                let decisions = tx.execute(
                    "DELETE FROM model_route_decisions WHERE rowid IN ( \
                       SELECT rowid FROM model_route_decisions \
                       WHERE created_at < ?1 ORDER BY created_at LIMIT ?2)",
                    params![created_before_ms, i64::try_from(limit).unwrap_or(i64::MAX)],
                )?;
                crate::connection_store::timed_commit(tx)?;
                Ok(panels + decisions)
            })
            .map_err(err)
    }

    fn record_usage_delivery(&self, obs: &UsageObservation) -> RouteStateResult<UsageRecordResult> {
        self.record_usage_delivery_with_decision(obs, None)
    }
    fn append_quota_readings(&self, rows: &[QuotaReadingLogRow]) -> RouteStateResult<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.apply_quota_delta(rows, &[], &[])
    }

    fn get_quota_readings(&self, account_key: &str) -> RouteStateResult<Vec<QuotaReadingLogRow>> {
        let source = if self.history.is_some() {
            "visible_quota_readings"
        } else {
            "quota_reading_log"
        };
        self.with_history_reader(|connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT * FROM {source} WHERE account_key = ?1 ORDER BY window_key, observed_at",
            ))?;
            let rows = statement
                .query_map([account_key], |row| {
                    let is_stale = row.get::<_, i64>(9)? != 0;
                    let stale = if is_stale {
                        Some(QuotaStaleDisclosure {
                            since: row.get(10)?,
                            class: row.get(11)?,
                        })
                    } else {
                        None
                    };
                    Ok(QuotaReadingLogRow {
                        account_key: row.get(0)?,
                        window_key: row.get(1)?,
                        observed_at: row.get(2)?,
                        used_percent: row.get(3)?,
                        raw_used_percent: row.get(4)?,
                        resets_at: row.get(5)?,
                        window_minutes: row.get(6)?,
                        banked_credit_count: row.get(7)?,
                        fetched_at: row.get(8)?,
                        stale,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .map_err(err)
    }

    fn prune_quota_readings(&self, now_ms: i64) -> RouteStateResult<usize> {
        self.prune_quota_reading_log(reading_log_retention_cutoff(now_ms), usize::MAX)
    }

    fn get_observed_speed(&self, route_key: &str, now: f64) -> RouteStateResult<Option<f64>> {
        if route_key.trim().is_empty() {
            return Ok(None);
        }
        self.get_observed_speed_for_tier(route_key, None, None, now)
    }

    fn get_class_vector(
        &self,
        workload_class: &str,
        now: f64,
    ) -> RouteStateResult<Option<ClassVector>> {
        if !ENUMERATED_WORKLOAD_CLASSES.contains(&workload_class) {
            return Ok(None);
        }
        let generation = self.usage_generation.load(Ordering::Acquire);
        let window_minute = (now / 60_000.0).floor() as i64;
        if let Some(cached) = self
            .class_vectors
            .lock()
            .expect("class vector cache poisoned")
            .get(workload_class)
        {
            if cached.usage_generation == generation && cached.window_minute == window_minute {
                return Ok(cached.vector.clone());
            }
        }
        let vector = self.compute_class_vector(workload_class, now)?;
        self.class_vectors
            .lock()
            .expect("class vector cache poisoned")
            .insert(
                workload_class.to_string(),
                CachedClassVector {
                    usage_generation: generation,
                    window_minute,
                    vector: vector.clone(),
                },
            );
        Ok(vector)
    }

    fn get_facts_with_observed_speed(
        &self,
        now: f64,
        pricing: &striatum_core::RoutePricing,
    ) -> RouteStateResult<Vec<ModelFact>> {
        self.overlay_observed_speed(self.get_facts()?, now, pricing)
    }

    fn build_snapshot(&self, now: f64) -> RouteStateResult<ModelRouteStateSnapshot> {
        // Private stores have no publisher; their snapshot reads the typed
        // sources. Live requests use BlockState::route_state_snapshot.
        self.store
            .with_conn(|c: &Connection| Ok(route_snapshot::read_snapshot(c, now, now_ms())))
            .map_err(err)?
    }
}

impl crate::AccountTx<'_> {
    pub fn clear_route_cooldowns(
        &self,
        requested: &str,
    ) -> RouteStateResult<Option<ClearedRouteCooldowns>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::route_state::clear_route_cooldowns");
        self.command_sql(|conn: &Connection| {
            let tx = conn;
            let known = |key: &str| -> rusqlite::Result<bool> {
                tx.query_row("SELECT EXISTS(SELECT 1 FROM model_facts WHERE route_key=?1 OR capacity_key=?1 UNION ALL SELECT 1 FROM model_route_state WHERE route_key=?1 OR capacity_key=?1 UNION ALL SELECT 1 FROM routing_v1_decision WHERE provider||'/'||model=?1 OR provider||'/'||model||'/'||variant=?1 UNION ALL SELECT 1 FROM account_admission WHERE provider||'/'||model=?1)", [key], |row| row.get(0))
            };
            let requested = requested.trim();
            let route_key = if known(requested)? {
                requested.to_owned()
            } else if let Some((base, variant)) = requested.rsplit_once(':') {
                let key = format!("{base}/{variant}");
                if !known(&key)? { return Ok(None); }
                key
            } else { return Ok(None); };
            let identity: Option<(String, String, Option<String>)> = tx.query_row(
                "SELECT provider_id,model_id,variant FROM model_facts WHERE route_key=?1 OR capacity_key=?1 UNION ALL SELECT provider_id,model_id,variant FROM model_route_state WHERE route_key=?1 OR capacity_key=?1 UNION ALL SELECT provider,model,variant FROM routing_v1_decision WHERE provider||'/'||model=?1 OR provider||'/'||model||'/'||variant=?1 UNION ALL SELECT provider,model,NULL FROM account_admission WHERE provider||'/'||model=?1 LIMIT 1",
                [&route_key], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))
            ).optional()?;
            let Some((provider, model, _variant)) = identity else { return Ok(None); };
            let capacity = striatum_core::make_capacity_key(&provider, &model);
            let base = route_key == capacity;
            let state=self.current_blocks().map_err(fence_rechecks::command_error)?;
            let routes=state.route_cooldowns_for_clear(&route_key,base,&capacity);
            let account_fences=state.route_fences_for_clear(&provider,&model);
            let at=now_ms();
            let keys=serde_json::to_string(&routes.iter().map(|(key,_)|key).collect::<Vec<_>>()).map_err(|error|rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            tx.execute(OPERATOR_CLEAR_ROUTE_BLOCKS,params![keys,at])?;
            tx.execute("UPDATE model_route_state SET updated_at=?4 WHERE route_key=?1 OR (?2 AND capacity_key=?3)",params![route_key,base,capacity,at])?;
            tx.execute(OPERATOR_CLEAR_PROVIDER_MODEL,params![provider,model])?;
            tx.execute(&keyed_updates::operator_account_routes_sql(),params![provider,model,at])?;
            // Clearing one route also lifts the provider's credential-wide
            // billing wall, allowing an early retry as the operator requested.
            tx.execute("UPDATE block SET source_present=0,cleared_at=?2,cleared_by='removed',claim_owner=NULL
                WHERE kind='pool_billing' AND is_origin=1 AND source_present=1 AND provider=?1",params![provider,at])?;
            tx.execute("UPDATE block SET source_present=0,cleared_at=?2,cleared_by='removed',claim_owner=NULL
                WHERE kind='credential' AND is_origin=1 AND source_present=1 AND provider=?1 AND code='credential_refused'",params![provider,at])?;

            Ok(Some(ClearedRouteCooldowns {route_key, routes, account_fences}))
        }).map_err(err)
    }

    pub fn record_credential_entitlement_fence(
        &self,
        refusal: &striatum_core::route_state_store::CredentialRefusal,
        listing_fingerprint: Option<&str>,
        observed_at_ms: i64,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::route_state::record_credential_entitlement_fence",
        );
        if refusal.provider_id.trim().is_empty() || refusal.credential_id.trim().is_empty() {
            return Ok(());
        }
        self.command_scope(|tx| {
            tx.record_credential_refusal(refusal, listing_fingerprint, observed_at_ms)
        })
    }

    pub fn touch_credential_entitlement_fence(
        &self,
        provider_id: &str,
        credential_id: &str,
        observed_at_ms: i64,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::route_state::touch_credential_entitlement_fence",
        );
        self.command_sql(|c: &Connection| {
            let tx = c;
            let state = self
                .current_blocks()
                .map_err(fence_rechecks::command_error)?;
            state.check_source(
                tx,
                "credential",
                &serde_json::json!([provider_id, credential_id]).to_string(),
                provider_id,
            )?;
            let changed =
                if let Some(mut fence) = state.entitlement_fence(provider_id, credential_id) {
                    fence.observed_at_ms = observed_at_ms;
                    block_storage::write_source(tx, &block_storage::credential_source(&fence)?)?
                } else {
                    0
                };
            fence_rechecks::register_on(
                self,
                "credential",
                &serde_json::json!([provider_id, credential_id]).to_string(),
            )?;

            Ok(changed)
        })
        .map_err(err)
    }

    pub fn clear_credential_entitlement_fence_count(
        &self,
        provider_id: &str,
        credential_id: &str,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::route_state::clear_credential_entitlement_fence_count",
        );
        self.command_sql(|c: &Connection| {
            c.execute(
                "UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
                      WHERE kind='credential' AND is_origin=1 AND source_present=1 AND provider=?1 AND credential_id=?2",
                params![provider_id, credential_id,now_ms()],
            )
        })
        .map_err(err)
    }

    pub fn clear_credential_entitlement_fences_for_credential(
        &self,
        credential_id: &str,
    ) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::route_state::clear_credential_entitlement_fences_for_credential",
        );
        self.command_sql(|c: &Connection| {
            keyed_updates::clear_credential_for_id(self, c, credential_id)
        })
        .map_err(err)
    }

    pub fn clear_credential_entitlement_fences(
        &self,
        credential_id: &str,
        fingerprint: Option<&str>,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::route_state::clear_credential_entitlement_fences",
        );
        self.command_sql(|c: &Connection| {
            c.execute(
                &keyed_updates::clear_matching_credential_sql(),
                params![credential_id, fingerprint, now_ms()],
            )
        })
        .map_err(err)
    }

    pub fn adopt_credential_entitlement_fingerprint(
        &self,
        credential_id: &str,
        fingerprint: &str,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::route_state::adopt_credential_entitlement_fingerprint",
        );
        self.command_sql(|c: &Connection| {
            c.execute(
                &keyed_updates::adopt_credential_fingerprint_sql(),
                params![credential_id, fingerprint],
            )
            .map(|_| ())
        })
        .map_err(err)
    }

    pub fn record_provider_cooldown(
        &self,
        provider_key: &str,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::route_state::record_provider_cooldown");
        let observed_at = update.observed_at_ms.unwrap_or_else(now_ms);
        let mut bounded = update.clone();
        {
            // No account is named on this legacy provider-wide signal. Its
            // quota class cannot establish account quota, so cap it as a
            // non-quota fence while retaining the reported class as evidence.
            let cap = observed_at
                .saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS)
                as f64;
            bounded.cooldown_until = update.cooldown_until.map(|until| until.min(cap));
            bounded.quota_exhausted_until =
                update.quota_exhausted_until.map(|until| until.min(cap));
        }
        let update = &bounded;
        // Old callers can still send an account failure as a route cooldown.
        // Keep account evidence in the account allocator, never fan it out to
        // every model, nor swap out an independent model wall's cause.
        if !update.unscoped_wall
            && striatum_core::account_wall::account_scoped_wall(
                update.last_error.as_deref(),
                update.wall_class,
            )
        {
            return Ok(());
        }
        let provider_key = derive_provider_key(provider_key)
            .ok_or_else(|| err("provider cooldown write has no canonical provider key"))?
            .to_owned();
        let route_provider = derive_provider_key(route_key)
            .ok_or_else(|| err("provider cooldown route has no canonical provider key"))?
            .to_owned();
        if route_provider != provider_key
            || derive_provider_key(capacity_key) != Some(provider_key.as_str())
        {
            return Err(err("provider cooldown route/provider keys do not match"));
        }
        self.command_sql(|c: &Connection| {
                // The store connection mutex serializes this transaction with
                // upsert_fact and every other route-state writer. The provider
                // row, inheritance row, and existing-route fanout therefore
                // commit together or roll back together.
                let tx = c;
                let new_wall=update.cooldown_until.is_some() || update.quota_exhausted_until.is_some();
                // Read only to decide this writer's own write of the row (see `route_write_read`).
                let prior_at:Option<i64>=tx.query_row(ROUTE_READ_PROVIDER_PRIOR_OBSERVED,[&provider_key],|row|row.get(0)).optional()?.flatten();
                if new_wall && prior_at.is_some_and(|at|at>observed_at) {return Ok(());}
                // Replace inherited provider evidence only. An independent
                // model refusal keeps its own deadline and evidence generation.
                // Read only to decide this writer's own write of the row (see `route_write_read`).
                let mut inherited=tx.prepare(ROUTE_READ_PROVIDER_INHERITED_ROUTES)?
                    .query_map([&provider_key],|row|Ok(InheritedRouteState {route_key:row.get(0)?,model_id:row.get(1)?,wall_observed_at:row.get(2)?,last_error:row.get(3)?,last_error_at:row.get(4)?,wall_class:row.get(5)?,wall_unscoped:row.get(6)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
                let existing = tx
                    .query_row(
                        // Read only to decide this writer's own write of the row (see `route_write_read`).
                        ROUTE_READ_PROVIDER_EXISTING,
                        [&provider_key],
                        |row| {
                            Ok(ExistingProviderState {
                                cooldown_until: row.get(0)?,
                                quota_exhausted_until: row.get(1)?,
                                quota_pressure: row.get(2)?,
                                last_error: row.get(3)?,
                                last_error_at: row.get(4)?,
                                wall_class: row_wall_class(row, 5)?,
                                wall_unscoped: row.get(6)?,
                                wall_observed_at: row.get(7)?,
                            })
                        },
                    )
                    .optional()?;
                // A stored row from one credential's refusal holds no provider
                // wall, so a new provider-wide signal merges with nothing.
                let existing = existing
                    .filter(|state| !credential_refusal_wall(state.last_error.as_deref()))
                    .unwrap_or_default();
                let cooldown_until = if new_wall {update.cooldown_until} else {existing.cooldown_until};
                let quota_exhausted_until = if new_wall {update.quota_exhausted_until} else {existing.quota_exhausted_until};
                let quota_pressure = update.quota_pressure.or(existing.quota_pressure);
                let wall_class = update.wall_class.or(existing.wall_class);
                let (last_error, last_error_at) = match &update.last_error {
                    Some(error) => (Some(error.clone()), Some(observed_at)),
                    None if (update.cooldown_until.is_some()
                        || update.quota_exhausted_until.is_some())
                        && striatum_core::account_wall::account_scoped_wall(
                            existing.last_error.as_deref(),
                            existing.wall_class,
                        ) =>
                    {
                        (Some("route wall (scope unknown)".into()), Some(observed_at))
                    }
                    None => (existing.last_error, existing.last_error_at),
                };
                let updated_at = observed_at;

                tx.execute(
                    "INSERT INTO provider_cooldown_state(provider_key,quota_pressure,last_error,last_error_at,updated_at)
                     VALUES (?1,?2,?3,?4,?5) ON CONFLICT(provider_key) DO UPDATE SET
                       quota_pressure=excluded.quota_pressure,last_error=excluded.last_error,
                       last_error_at=excluded.last_error_at,updated_at=excluded.updated_at",
                    params![provider_key,quota_pressure,last_error,last_error_at,updated_at],
                )?;

                // A direct cooldown signal can name a route that is not in the
                // catalog yet. Materialize it from the just-merged provider row
                // before the fanout, so it cannot miss this write.
                let (_, model_id, variant) = route_identity(&tx, route_key, None)?;
                let inserted=tx.execute(
                    "INSERT OR IGNORE INTO model_route_state (
                       route_key,capacity_key,provider_id,model_id,variant,quota_pressure,
                       latency_ema_ms,success_ema,failure_count,quota_error_count,last_error,last_error_at,updated_at,wall_observed_at
                     ) VALUES (?1,?2,?3,?4,?5,?6,NULL,NULL,0,0,?7,?8,?9,?10)",
                    params![route_key,capacity_key,route_provider,model_id,variant,quota_pressure,last_error,last_error_at,updated_at,prior_at],
                )?;

                if inserted>0 {inherited.push(InheritedRouteState {route_key:route_key.into(),model_id,wall_observed_at:prior_at,
                    last_error:last_error.clone(),last_error_at,wall_class:wall_class.map(|class|class.as_str().to_owned()),wall_unscoped:existing.wall_unscoped});}
                let unscoped=if new_wall {update.unscoped_wall} else {existing.wall_unscoped};
                let wall_observed=if new_wall {Some(updated_at)} else {prior_at};
                let source_at=wall_observed.or(last_error_at).unwrap_or(updated_at);
                for inherited in &inherited {
                    let wall_observed=if new_wall {Some(updated_at)} else {inherited.wall_observed_at};
                    let error=last_error.as_deref().or(inherited.last_error.as_deref());
                    let error_at=if last_error.is_some() {Some(updated_at)} else {inherited.last_error_at};
                    let class=update.wall_class.map(RouteWallClass::as_str).or(inherited.wall_class.as_deref());
                    let unscoped=if new_wall {update.unscoped_wall} else {inherited.wall_unscoped};
                    if fence_rechecks::quarantined_row(unscoped,error,class) {
                        tx.execute("DELETE FROM block WHERE kind='route' AND is_origin=1 AND origin_key=json_array(?1)",[&inherited.route_key])?;
                    } else if let Some(row)=block_storage::wall_source(tx,block_storage::WallSource {
                        kind:"route",key:&inherited.route_key,provider:&provider_key,model:Some(&inherited.model_id),cooldown:cooldown_until,exhausted:quota_exhausted_until,
                        wall_observed,observed_at:wall_observed.or(error_at).unwrap_or(updated_at),error,class,unscoped,
                    })? {block_storage::write_source(tx,&row)?;}
                }
                if let Some(row)=block_storage::wall_source(tx,block_storage::WallSource {
                    kind:"provider",key:&provider_key,provider:&provider_key,model:None,cooldown:cooldown_until,exhausted:quota_exhausted_until,
                    wall_observed,observed_at:source_at,error:last_error.as_deref(),class:wall_class.map(RouteWallClass::as_str),unscoped,
                })? {block_storage::write_source(tx,&row)?;}
                let inherited=serde_json::to_string(&inherited.iter().map(|row|&row.route_key).collect::<Vec<_>>()).map_err(|e|rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                // A repeated refusal replaces the previous provider deadlines
                // inherited by these routes, even migrated deadlines over an hour.
                tx.execute(
                    UPDATE_INHERITED_ROUTE_WALLS,
                    params![
                        cooldown_until,
                        quota_exhausted_until,
                        update.quota_pressure,
                        update.wall_class.map(RouteWallClass::as_str),
                        if update.increment_failure { 1 } else { 0 },
                        if update.increment_quota_error { 1 } else { 0 },
                        last_error,
                        updated_at,
                        provider_key,
                        inherited,
                    ],
                )?;
                if update.cooldown_until.is_some() || update.quota_exhausted_until.is_some() {
                    tx.execute("UPDATE provider_cooldown_state SET wall_observed_at=?2 WHERE provider_key=?1",params![provider_key,updated_at])?;
                    tx.execute(UPDATE_INHERITED_ROUTE_WALL_SCOPE,params![provider_key,updated_at,update.unscoped_wall,inherited])?;
                    fence_rechecks::register_on(self,"provider",&serde_json::json!([provider_key]).to_string())?;
                }
                Ok(())
            })
            .map_err(err)
    }

    pub fn update_route_state(
        &self,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::route_state::update_route_state");
        let observed_at = update.observed_at_ms.unwrap_or_else(now_ms);
        let mut bounded = update.clone();
        {
            // Identity-free legacy cooldowns are non-quota even when their
            // reported class says quota: no account reading can verify it.
            let cap = observed_at
                .saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS)
                as f64;
            bounded.cooldown_until = update.cooldown_until.map(|until| until.min(cap));
            bounded.quota_exhausted_until =
                update.quota_exhausted_until.map(|until| until.min(cap));
        }
        let update = &bounded;
        if !update.unscoped_wall
            && striatum_core::account_wall::account_scoped_wall(
                update.last_error.as_deref(),
                update.wall_class,
            )
        {
            return Ok(());
        }
        let provider_key = derive_provider_key(route_key)
            .ok_or_else(|| err("route state update has no canonical provider key"))?
            .to_owned();
        self.command_sql(|c: &Connection| {
                // Read-modify-write under one transaction so counter increments,
                // provider inheritance, and field merges see one consistent row.
                // Acquire the writer before reading: in WAL mode, upgrading a
                // deferred read transaction can fail with BUSY without waiting
                // for another writer, even with a busy timeout configured.
                let tx = c;
                let (provider_id, model_id, variant) = route_identity(&tx, route_key, None)?;
                let new_wall=update.cooldown_until.is_some() || update.quota_exhausted_until.is_some();
                // Read only to decide this writer's own write of the row (see `route_write_read`).
                let prior_at:Option<i64>=tx.query_row(ROUTE_READ_ROUTE_PRIOR_OBSERVED,[route_key],|row|row.get(0)).optional()?.flatten();
                if new_wall && prior_at.is_some_and(|at|at>observed_at) {return Ok(());}
                let existing = tx
                    .query_row(
                        // Read only to decide this writer's own write of the row (see `route_write_read`).
                        ROUTE_READ_ROUTE_EXISTING,
                        [route_key],
                        |r| {
                            Ok(ExistingState {
                                cooldown_until: r.get(0)?,
                                quota_exhausted_until: r.get(1)?,
                                wall_class: row_wall_class(r, 9)?,
                                wall_unscoped:r.get(10)?,
                                quota_pressure: r.get(2)?,
                                latency_ema_ms: r.get(3)?,
                                success_ema: r.get(4)?,
                                failure_count: r.get(5)?,
                                quota_error_count: r.get(6)?,
                                last_error: r.get(7)?,
                                last_error_at: r.get(8)?,
                            })
                        },
                    )
                    .optional()?;
                let new_route=existing.is_none();
                let mut ex = existing.unwrap_or_default();
                // The route's own wall from one credential's refusal keeps its
                // evidence (last_error) but lends no horizon to this write.
                if credential_refusal_wall(ex.last_error.as_deref()) {
                    ex.cooldown_until = None;
                    ex.quota_exhausted_until = None;
                    ex.wall_class = None;
                }
                let provider = tx
                    .query_row(
                        // Read only to decide this writer's own write of the row (see `route_write_read`).
                        ROUTE_READ_ROUTE_PROVIDER_ROW,
                        [&provider_key],
                        |r| {
                            Ok(ExistingProviderState {
                                cooldown_until: r.get(0)?,
                                quota_exhausted_until: r.get(1)?,
                                quota_pressure: r.get(2)?,
                                last_error: r.get(3)?,
                                last_error_at: r.get(4)?,
                                wall_class: row_wall_class(r, 5)?,
                                wall_unscoped:r.get(6)?,wall_observed_at:r.get(7)?,
                            })
                        },
                    )
                    .optional()?
                    .unwrap_or_default();
                // A stored provider row from one credential's refusal is not
                // inherited: that refusal never walled the provider.
                let provider = if credential_refusal_wall(provider.last_error.as_deref()) {
                    ExistingProviderState::default()
                } else {
                    provider
                };

                // A model refusal replaces its own horizon. The provider wall
                // remains a separate origin, never a floor for this deadline.
                let cooldown_until = if new_wall {update.cooldown_until} else if new_route {provider.cooldown_until} else {ex.cooldown_until};
                let quota_exhausted_until = if new_wall {update.quota_exhausted_until} else if new_route {provider.quota_exhausted_until} else {ex.quota_exhausted_until};
                let quota_pressure = update
                    .quota_pressure
                    .or(ex.quota_pressure)
                    .or(provider.quota_pressure);
                let wall_class = update
                    .wall_class
                    .or(ex.wall_class)
                    .or(provider.wall_class);
                let latency_ema_ms = update.latency_ema_ms.or(ex.latency_ema_ms);
                let success_ema = update.success_ema.or(ex.success_ema);
                let failure_count = ex.failure_count + if update.increment_failure { 1 } else { 0 };
                let quota_error_count =
                    ex.quota_error_count + if update.increment_quota_error { 1 } else { 0 };
                let (last_error, last_error_at) = match &update.last_error {
                    Some(error) => (Some(error.clone()), Some(observed_at)),
                    None if (update.cooldown_until.is_some() || update.quota_exhausted_until.is_some())
                        && striatum_core::account_wall::account_scoped_wall(ex.last_error.as_deref(), ex.wall_class) => {
                        // A new unscoped wall must not inherit a legacy account
                        // narrative, which would falsely quarantine the new wall.
                        (Some("route wall (scope unknown)".into()), Some(observed_at))
                    }
                    None => (ex
                        .last_error
                        .clone()
                        .or(provider.last_error.clone()), ex.last_error_at.or(provider.last_error_at)),
                };

                tx.execute(
                    "INSERT INTO model_route_state (
                       route_key,capacity_key,provider_id,model_id,variant,quota_pressure,
                       latency_ema_ms,success_ema,failure_count,quota_error_count,last_error,last_error_at,updated_at,wall_observed_at
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
                     ON CONFLICT(route_key) DO UPDATE SET
                       capacity_key=excluded.capacity_key,provider_id=excluded.provider_id,model_id=excluded.model_id,variant=excluded.variant,
                       quota_pressure=excluded.quota_pressure,latency_ema_ms=excluded.latency_ema_ms,success_ema=excluded.success_ema,
                       failure_count=excluded.failure_count,quota_error_count=excluded.quota_error_count,
                       last_error=excluded.last_error,last_error_at=excluded.last_error_at,updated_at=excluded.updated_at",
                    params![route_key,capacity_key,provider_id,model_id,variant,quota_pressure,latency_ema_ms,success_ema,
                        failure_count,quota_error_count,last_error,last_error_at,observed_at,if new_route {provider.wall_observed_at} else {prior_at}],
                )?;
                let wall_observed=if new_wall {Some(observed_at)} else if new_route {provider.wall_observed_at} else {prior_at};
                let unscoped=if new_wall {update.unscoped_wall} else if new_route {provider.wall_unscoped} else {ex.wall_unscoped};
                if let Some(row)=block_storage::wall_source(tx,block_storage::WallSource {
                    kind:"route",key:route_key,provider:&provider_id,model:Some(&model_id),cooldown:cooldown_until,exhausted:quota_exhausted_until,
                    wall_observed,observed_at:wall_observed.or(last_error_at).unwrap_or(observed_at),error:last_error.as_deref(),
                    class:wall_class.map(RouteWallClass::as_str),unscoped,
                })? {block_storage::write_source(tx,&row)?;}
                if new_wall {
                    tx.execute("UPDATE model_route_state SET wall_observed_at=?2 WHERE route_key=?1",params![route_key,observed_at])?;
                    fence_rechecks::register_on(self,"route",&serde_json::json!([route_key]).to_string())?;
                }
                Ok(())
            })
            .map_err(err)
    }

    pub fn set_decision_outcome(
        &self,
        decision_id: &str,
        update: &DecisionOutcomeUpdate,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::route_state::set_decision_outcome");
        // Retain the full error in the audit, but treat an explicitly
        // account-scoped failure as neutral model-health evidence.
        // It must neither record a route-wide quota wall nor change the
        // terminal-failure backoff.
        // A permanent refusal of one credential is the same kind of evidence: the
        // credential is fenced, and the refusal does not by itself block the
        // route's other credentials.
        let mut normalized = update.clone();
        let last_error = update.last_error.as_deref().unwrap_or("");
        if (update.outcome != striatum_core::RouteOutcome::Completed
            && update.provider_outcome.as_ref().is_some_and(|provider| {
                striatum_core::account_wall::transient_provider_error(
                    provider.error_class.as_deref(),
                    provider.provider_code.as_deref(),
                )
            }))
            || striatum_core::account_wall::account_scoped_error(last_error)
            || striatum_core::account_wall::credential_refusal_in(last_error).is_some()
            || update
                .provider_outcome
                .as_ref()
                .is_some_and(|provider| provider.credential_refusal.is_some())
        {
            normalized.outcome = striatum_core::RouteOutcome::Interrupted;
        }
        let update = &normalized;
        self.command_sql(|c: &Connection| {
                let tx = c;
                type DecisionRoute = (
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                );
                let decision: Option<DecisionRoute> = tx
                    .query_row(
                        "SELECT selected_route_key, selected_provider_id, selected_model_id, outcome
                         FROM model_route_decisions WHERE id=?1",
                        [decision_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?;
                // The decision ledger is history a backup may restore empty, and
                // retention deletes its old rows. An outcome for a decision the
                // ledger no longer holds cannot name its route, so it changes
                // nothing; answering an error instead would only make the caller
                // retry an outcome that can never land.
                let Some((route_key, selected_provider_id, selected_model_id, prior_outcome)) =
                    decision
                else {
                    tracing::warn!(
                        decision_id,
                        outcome = update.outcome.as_str(),
                        "route outcome names a decision the ledger does not hold; \
                         route health is left unchanged"
                    );
                    return Ok(());
                };

                // Outcome delivery is soft and may be retried after a lost ack. The first
                // terminal write owns the counter transition; a replay must not count twice.
                if prior_outcome.is_some() {

                    return Ok(());
                }


                tx.execute(
                    "UPDATE model_route_decisions
                     SET outcome=?1, outcome_at=?2, outcome_error=?3
                     WHERE id=?4 AND outcome IS NULL",
                    params![
                        update.outcome.as_str(),
                        update.observed_at_ms,
                        update.last_error,
                        decision_id
                    ],
                )?;

                let Some(route_key) = route_key else {

                    return Ok(());
                };
                let (provider_id, model_id, variant) = route_identity(
                    &tx,
                    &route_key,
                    selected_provider_id.as_deref().zip(selected_model_id.as_deref()),
                )?;
                let capacity_key = format!("{provider_id}/{model_id}");
                if let Some(retry_after_secs) = update.outcome.retry_after_secs() {
                    let quota_exhausted_until = update
                        .observed_at_ms
                        .saturating_add((retry_after_secs.min(i64::MAX as u64) as i64).saturating_mul(1_000)
                            .min(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS))
                        as f64;
                    let surfaced_error = update
                        .last_error
                        .clone()
                        .unwrap_or_else(|| update.outcome.as_str().to_string());
                    let scope=self.current_blocks().map_err(fence_rechecks::command_error)?;
                    let prior_class=scope.block("route",&serde_json::json!([route_key]).to_string()).filter(|row|row.source_present).and_then(|row|row.wall_class.clone());
                    let changed=tx.execute(
                        "INSERT INTO model_route_state (
                           route_key,capacity_key,provider_id,model_id,variant,failure_count,quota_error_count,
                           last_error,last_error_at,updated_at,wall_observed_at
                         ) VALUES (?1,?2,?3,?4,?5,1,1,?6,?7,?7,?7)
                         ON CONFLICT(route_key) DO UPDATE SET
                           capacity_key=excluded.capacity_key,provider_id=excluded.provider_id,model_id=excluded.model_id,variant=excluded.variant,
                           wall_observed_at=excluded.wall_observed_at,failure_count=model_route_state.failure_count+1,
                           quota_error_count=model_route_state.quota_error_count+1,last_error=excluded.last_error,
                           last_error_at=excluded.last_error_at,updated_at=excluded.updated_at
                         WHERE model_route_state.wall_observed_at IS NULL OR excluded.wall_observed_at>=model_route_state.wall_observed_at",
                        params![route_key,capacity_key,provider_id,model_id,variant,surfaced_error,update.observed_at_ms],
                    )?;
                    if changed>0 {
                        if let Some(row)=block_storage::wall_source(tx,block_storage::WallSource {
                            kind:"route",key:&route_key,provider:&provider_id,model:Some(&model_id),cooldown:None,exhausted:Some(quota_exhausted_until),
                            wall_observed:Some(update.observed_at_ms),observed_at:update.observed_at_ms,error:Some(&surfaced_error),class:prior_class.as_deref(),unscoped:true,
                        })? {block_storage::write_source(tx,&row)?;}
                    }
                }
                let existing: Option<(i64, Option<f64>, Option<String>,i64,Option<i64>)> = tx
                    .query_row(
                        // Read only to decide this writer's own write of the row (see `route_write_read`).
                        ROUTE_READ_OUTCOME_FAILURE_STATE,
                        [&route_key],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?,row.get(3)?,row.get(4)?)),
                    )
                    .optional()?;
                let previous_failures = existing
                    .as_ref()
                    .map(|state| state.0.max(0) as u32)
                    .unwrap_or(0);

                let transition=if existing.as_ref().is_some_and(|state|(state.0>0 || state.1.is_some()) && state.3>update.observed_at_ms) {
                    FailureBackoffTransition::Unchanged
                } else {failure_backoff_transition(
                    previous_failures,
                    update.outcome,
                    update.last_error.as_deref(),
                    update.observed_at_ms,
                    update.failure_backoff,
                )};
                match transition {
                    FailureBackoffTransition::Unchanged => {}
                    FailureBackoffTransition::Reset => {
                        // KEYED_EXCEPTION: use the decision's admission index
                        // because closed admissions may no longer be in memory.
                        account_selection::note_keyed_exception();
                        let admitted_at:i64=tx.query_row("SELECT COALESCE(
                            (SELECT MAX(committed_at) FROM account_admission /* keyed exception */ a WHERE a.model_decision_id=d.id
                             AND a.provider=?2 AND a.model=?3 AND a.input_size_class IS NOT NULL),
                            (SELECT created_at_ms FROM routing_v1_decision v WHERE v.decision_id=d.id),d.created_at)
                            FROM model_route_decisions d WHERE d.id=?1",params![decision_id,provider_id,model_id],|row|row.get(0))?;
                        // A late completion from an already-running request
                        // cannot clear failure evidence recorded after it launched.
                        let cleared=tx.execute(
                            "UPDATE model_route_state SET consecutive_terminal_failures=0,updated_at=?1
                             WHERE route_key=?2 AND COALESCE(failure_observed_at,last_error_at,updated_at)<?3",
                            params![update.observed_at_ms, route_key,admitted_at],
                        )?;
                        if cleared>0 {block_storage::remove_source(tx,"failure",&serde_json::json!([route_key]).to_string(),update.observed_at_ms,"completed")?;}
                    }
                    FailureBackoffTransition::Failure {
                        consecutive_failures,
                        cooldown_until,
                        reason,
                    } => {
                        let previous_until = existing.as_ref().and_then(|state| state.1);
                        let previous_reason = existing.as_ref().and_then(|state| state.2.clone());
                        let failure_backoff_until = cooldown_until.map(|value| value as f64).or(previous_until);
                        let failure_backoff_reason = reason.or(previous_reason);
                        let surfaced_error = update
                            .last_error
                            .clone()
                            .unwrap_or_else(|| update.outcome.as_str().to_string());
                        let inherited_scope:Option<(Option<i64>,bool)>=if existing.is_none() {
                            tx.query_row(ROUTE_READ_ROUTE_PROVIDER_ROW,[&provider_id],|row|Ok((row.get(7)?,row.get(6)?))).optional()?
                        } else {None};
                        tx.execute(
                            "INSERT INTO model_route_state (
                               route_key,capacity_key,provider_id,model_id,variant,failure_count,last_error,last_error_at,updated_at,consecutive_terminal_failures,wall_observed_at
                             ) VALUES (?1,?2,?3,?4,?5,1,?6,?7,?7,?8,?9)
                             ON CONFLICT(route_key) DO UPDATE SET
                               capacity_key=excluded.capacity_key,provider_id=excluded.provider_id,model_id=excluded.model_id,variant=excluded.variant,
                               failure_count=model_route_state.failure_count+1,last_error=excluded.last_error,last_error_at=excluded.last_error_at,
                               updated_at=excluded.updated_at,consecutive_terminal_failures=excluded.consecutive_terminal_failures",
                            params![route_key,capacity_key,provider_id,model_id,variant,surfaced_error,update.observed_at_ms,consecutive_failures,inherited_scope.and_then(|scope|scope.0)],
                        )?;
                        tx.execute("UPDATE block SET scope_error=?2,observed_at=CASE WHEN ?3 THEN ?4 ELSE observed_at END
                            WHERE kind='route' AND is_origin=1 AND source_present=1 AND origin_key=json_array(?1)",
                            params![route_key,surfaced_error,existing.as_ref().is_some_and(|state|state.4.is_none()),update.observed_at_ms])?;
                        let scope=self.current_blocks().map_err(fence_rechecks::command_error)?;
                        let key=serde_json::json!([route_key]).to_string();
                        let held=scope.block("route",&key).filter(|row|row.source_present).or_else(||scope.block("failure",&key).filter(|row|row.source_present));
                        if let Some(row)=block_storage::failure_source(tx,block_storage::FailureSource {
                            key:&route_key,provider:&provider_id,model:&model_id,deadline:failure_backoff_until,reason:failure_backoff_reason.as_deref(),
                            observed_at:update.observed_at_ms,error:Some(&surfaced_error),class:held.and_then(|row|row.wall_class.as_deref()),unscoped:held.map_or_else(||inherited_scope.is_some_and(|scope|scope.1),|row|row.wall_unscoped),
                        })? {block_storage::write_source(tx,&row)?;}
                        tx.execute("UPDATE model_route_state SET failure_observed_at=?2 WHERE route_key=?1",params![route_key,update.observed_at_ms])?;
                        if failure_backoff_until.is_some() {
                            fence_rechecks::register_on(self,"failure",&serde_json::json!([route_key]).to_string())?;
                        }
                    }
                }

                fence_rechecks::register_on(self,"route",&serde_json::json!([route_key]).to_string())?;
                Ok(())
            })
            .map_err(err)
    }

    pub fn clear_failure_backoff(
        &self,
        route_key: &str,
    ) -> RouteStateResult<Option<FailureBackoffState>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::route_state::clear_failure_backoff");
        self.command_sql(|c: &Connection| {
            let tx = c;
            let prior = tx
                .query_row(
                    // Read only to decide this writer's own write of the row (see `route_write_read`).
                    ROUTE_READ_CLEAR_FAILURE_STATE,
                    [route_key],
                    |row| {
                        Ok(FailureBackoffState {
                            consecutive_terminal_failures: row.get::<_, i64>(0)?.max(0) as u32,
                            failure_backoff_until: row.get(1)?,
                            failure_backoff_reason: row.get(2)?,
                        })
                    },
                )
                .optional()?;
            if prior.is_some() {
                tx.execute(
                    "UPDATE model_route_state SET consecutive_terminal_failures=0,updated_at=?1 WHERE route_key=?2",
                    params![now_ms(), route_key],
                )?;
                block_storage::remove_source(tx,"failure",&serde_json::json!([route_key]).to_string(),now_ms(),"operator clear")?;
            }

            Ok(prior)
        })
        .map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy_fixture_inputs::{insert_legacy_account_wall, legacy_account_wall_error};

    #[test]
    fn whole_account_recovery_set_survives_restart() {
        use striatum_core::accounts::{ModelFamily, PoolKey, PoolScope};
        let scratch = cortexkit_test_support::ScratchDir::new("routing-pool-recovery");
        let descriptor = dev_descriptor(
            "routing-pool-recovery",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let block = AccountPoolFenceRow {
            evidence: AccountFenceRow {
                fence_id: "whole".into(),
                provider: "p".into(),
                account_id: "a".into(),
                scope_json: "null".into(),
                fence_json: "{}".into(),
                source: "observation".into(),
                observed_at: 1_000,
                principal: "runner".into(),
                admission_id: None,
                observation_id: None,
                created_at: 2_000,
                cleared_at: None,
                cleared_reason: None,
            },
            pool_key: None,
            recovery_set: vec![
                PoolKey {
                    window_minutes: None,
                    scope: PoolScope::Family(ModelFamily::Opus),
                    name: None,
                },
                PoolKey {
                    window_minutes: Some(90),
                    scope: PoolScope::EveryModel,
                    name: Some("pool-p".into()),
                },
                PoolKey {
                    window_minutes: Some(90),
                    scope: PoolScope::EveryModel,
                    name: Some("pool-q".into()),
                },
            ],
        };
        store
            .account_transaction(|tx| tx.insert_pool_fence(&block))
            .unwrap();
        drop(store);
        let restarted = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            restarted
                .account_transaction(|tx| tx.active_pool_fences("p"))
                .unwrap(),
            vec![block]
        );
    }

    #[test]
    fn legacy_account_wall_is_quarantined_without_erasing_evidence_or_real_walls() {
        let scratch = cortexkit_test_support::ScratchDir::new("account-wall-quarantine");
        let descriptor = dev_descriptor(
            "account-wall-quarantine",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 95).unwrap();
        insert_legacy_account_wall(&store, "anthropic/opus/medium", "quota");
        insert_legacy_account_wall(&store, "anthropic/billing", "billing");
        let now = now_ms();
        store.store.with_conn(|conn|conn.execute("UPDATE model_route_state SET cooldown_until=?1,quota_exhausted_until=?1,updated_at=?2 WHERE route_key='anthropic/billing'",params![now+86_400_000,now-1_800_000])).unwrap();
        drop(store);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let snapshot = store.build_snapshot(now as f64).unwrap();
        let scoped = &snapshot.by_route["anthropic/opus/medium"];
        assert_eq!(scoped.cooldown_until, None);
        assert_eq!(scoped.quota_exhausted_until, None);
        assert!(scoped
            .last_error
            .as_ref()
            .unwrap()
            .contains("00000000-0000-4000-8000-0000000000a1"));
        assert_eq!(
            snapshot.by_route["anthropic/billing"].quota_exhausted_until,
            Some((now + 1_800_000) as f64)
        );
        store.store.with_conn(|conn| {
            let stored:(i64,String) = conn.query_row("SELECT quota_exhausted_until,last_error FROM model_route_state WHERE route_key='anthropic/opus/medium'",[],|row| Ok((row.get(0)?,row.get(1)?)))?;
            assert_eq!(stored,(1791493229401,legacy_account_wall_error()),"quarantine is read-only");
            Ok::<_,rusqlite::Error>(())
        }).unwrap();
        // An independent sibling keeps the capacity closed; quarantining one
        // account's row never clears a real pool-wide or billing wall.
        store
            .update_route_state(
                "anthropic/opus/high",
                "anthropic/opus",
                &RouteStateUpdate {
                    quota_exhausted_until: Some((now + 86_400_000) as f64),
                    wall_class: Some(RouteWallClass::Quota),
                    last_error: Some("provider pool exhausted".into()),
                    observed_at_ms: Some(now),
                    unscoped_wall: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            store.build_snapshot(now as f64).unwrap().by_capacity["anthropic/opus"]
                .quota_exhausted_until,
            Some((now + 3_600_000) as f64)
        );
        // A new unscoped wall must replace the old account narrative even if
        // its caller supplies no diagnostic message.
        store
            .update_route_state(
                "anthropic/opus/medium",
                "anthropic/opus",
                &RouteStateUpdate {
                    quota_exhausted_until: Some((now + 86_400_000) as f64),
                    observed_at_ms: Some(now + 10),
                    unscoped_wall: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            store.build_snapshot(now as f64).unwrap().by_route["anthropic/opus/medium"]
                .quota_exhausted_until,
            Some((now + 3_600_010) as f64)
        );
    }

    #[test]
    fn route_state_writers_keep_account_exhaustion_out_of_model_health() {
        let scratch = cortexkit_test_support::ScratchDir::new("account-wall-writers");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "account-wall-writers",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for (index, error) in [
            legacy_account_wall_error(),
            r#"{"provider_code":"account_limit_exhausted","account_ref":"ysample"}"#.into(),
        ]
        .into_iter()
        .enumerate()
        {
            let route = format!("anthropic/writer-{index}");
            let update = RouteStateUpdate {
                cooldown_until: Some(1791493229401.0),
                quota_exhausted_until: Some(1791493229401.0),
                wall_class: Some(RouteWallClass::Quota),
                last_error: Some(error.clone()),
                increment_failure: true,
                increment_quota_error: true,
                ..Default::default()
            };
            store.update_route_state(&route, &route, &update).unwrap();
            store
                .record_provider_cooldown("anthropic", &route, &route, &update)
                .unwrap();
            let decision = store
                .record_decision(&DecisionInput {
                    selected_route_key: Some(route.clone()),
                    selected_provider_id: Some("anthropic".into()),
                    selected_model_id: Some(format!("writer-{index}")),
                    target_agent: "mason".into(),
                    requirements_json: "{}".into(),
                    score_json: "{}".into(),
                    rejected_json: "[]".into(),
                    ..Default::default()
                })
                .unwrap();
            store
                .set_decision_outcome(
                    &decision,
                    &DecisionOutcomeUpdate {
                        provider_outcome: None,
                        outcome: striatum_core::RouteOutcome::QuotaExhausted {
                            retry_after_secs: 279100,
                        },
                        last_error: Some(error),
                        observed_at_ms: 1791214158478,
                        failure_backoff: Default::default(),
                    },
                )
                .unwrap();
            store
                .store
                .with_conn(|conn| {
                    let rows: i64 = conn.query_row(
                        "SELECT count(*) FROM model_route_state WHERE route_key=?1",
                        [&route],
                        |row| row.get(0),
                    )?;
                    assert_eq!(
                        rows, 0,
                        "an account error must not write any model-health row"
                    );
                    let outcome: String = conn.query_row(
                        "SELECT outcome FROM model_route_decisions WHERE id=?1",
                        [&decision],
                        |row| row.get(0),
                    )?;
                    assert_eq!(outcome, "interrupted");
                    Ok::<_, rusqlite::Error>(())
                })
                .unwrap();
        }
        assert!(store.get_provider_cooldown("anthropic").unwrap().is_none());
    }

    #[test]
    fn clear_failure_backoff_preserves_cooldown_and_quota_walls() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = cortexkit_test_support::scratch_root().join(format!(
            "striatum-clear-failure-backoff-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor("striatum-clear-failure-backoff", path.to_string_lossy());
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        legacy_fixture_inputs::seed_failure_clear(&store);

        assert_eq!(
            store
                .clear_failure_backoff("test-provider/test-model")
                .unwrap(),
            Some(FailureBackoffState {
                consecutive_terminal_failures: 3,
                failure_backoff_until: Some(1_000.0),
                failure_backoff_reason: Some("consecutive terminal send failures: 3".into()),
            })
        );
        let after = legacy_fixture_inputs::failure_clear_row(&store);
        assert_eq!(
            after,
            (
                0,
                None,
                None,
                Some(1_500_000_000_000.0),
                Some(1_500_000_000_000.0)
            )
        );
        drop(store);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }
}
