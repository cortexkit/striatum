//! Owned account work submitted to the store's single group-commit engine.
//! Preparing a roster stays on the caller; commands only use transaction data
//! and the immutable inputs captured when the request was prepared.

use striatum_core::route_state_store::RouteStateResult;
use striatum_store::{AccountCommandKind, AccountTx, SqliteRouteStateStore};

pub(crate) fn repair<T: Send + 'static>(
    store: &SqliteRouteStateStore,
    command: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
) -> RouteStateResult<T> {
    execute(store, AccountCommandKind::Repair, command)
}

pub(crate) fn tick(store: &SqliteRouteStateStore, now_ms: i64) -> RouteStateResult<()> {
    execute(store, AccountCommandKind::Tick, move |tx| tx.tick(now_ms))
}

pub(crate) fn pool_wall(
    store: &SqliteRouteStateStore,
    provider: &str,
    credential: &str,
    kind: PoolWallKind,
    until_ms: i64,
    reason: &str,
    now_ms: i64,
) -> RouteStateResult<()> {
    store.pool_wall(provider, credential, kind, until_ms, reason, now_ms)
}

pub(crate) fn pool_key_states(
    store: &SqliteRouteStateStore,
    provider: &str,
    now_ms: i64,
) -> RouteStateResult<BTreeMap<String, striatum_core::key_pool::PoolKeyState>> {
    store.pool_key_states(provider, now_ms)
}

pub(crate) fn prune_unserved_unpicked_facts(
    store: &SqliteRouteStateStore,
) -> RouteStateResult<Vec<String>> {
    store.prune_unserved_unpicked_facts()
}

pub(crate) fn execute<T: Send + 'static>(
    store: &SqliteRouteStateStore,
    kind: AccountCommandKind,
    command: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
) -> RouteStateResult<T> {
    if !store.is_account_evaluation_projection() {
        crate::account_state::initialize(store)?;
    }
    #[cfg(test)]
    let inputs = crate::TestThreadState::capture();
    store.account_command(kind, move |tx| {
        #[cfg(test)]
        let _inputs = inputs.install();
        command(tx)
    })
}

use std::collections::{BTreeMap, HashSet};
use striatum_core::route_state_store::*;
use striatum_core::{FactSource, ModelFact, RouteStateStore};
use striatum_store::*;

pub(crate) fn clear_route_cooldowns(
    store: &SqliteRouteStateStore,
    requested: &str,
) -> RouteStateResult<Option<ClearedRouteCooldowns>> {
    store.clear_route_cooldowns(requested)
}

pub(crate) fn apply_fusiform_catalog_snapshot_for_providers(
    store: &SqliteRouteStateStore,
    facts: &[FusiformFactUpdate],
    cursor: FusiformCatalogCursor,
    mapped_providers: &HashSet<String>,
    retained: &HashSet<(String, String)>,
) -> RouteStateResult<()> {
    store.apply_fusiform_catalog_snapshot_for_providers(facts, cursor, mapped_providers, retained)
}

pub(crate) fn put_account_vault_roster(
    store: &SqliteRouteStateStore,
    roster: &PersistedVaultRoster,
) -> RouteStateResult<()> {
    store.put_account_vault_roster(roster)
}

pub(crate) fn record_credential_entitlement_fence(
    store: &SqliteRouteStateStore,
    refusal: &striatum_core::route_state_store::CredentialRefusal,
    listing_fingerprint: Option<&str>,
    observed_at_ms: i64,
) -> RouteStateResult<()> {
    store.record_credential_entitlement_fence(refusal, listing_fingerprint, observed_at_ms)
}

pub(crate) fn clear_credential_entitlement_fences_for_credential(
    store: &SqliteRouteStateStore,
    credential_id: &str,
) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
    store.clear_credential_entitlement_fences_for_credential(credential_id)
}

pub(crate) fn upsert_fact<S: RouteStateStore>(
    store: &S,
    fact: &ModelFact,
    source: FactSource,
) -> RouteStateResult<()> {
    store.upsert_fact(fact, source)
}

pub(crate) fn record_provider_cooldown(
    store: &SqliteRouteStateStore,
    provider_key: &str,
    route_key: &str,
    capacity_key: &str,
    update: &RouteStateUpdate,
) -> RouteStateResult<()> {
    store.record_provider_cooldown(provider_key, route_key, capacity_key, update)
}

pub(crate) fn update_route_state(
    store: &SqliteRouteStateStore,
    route_key: &str,
    capacity_key: &str,
    update: &RouteStateUpdate,
) -> RouteStateResult<()> {
    store.update_route_state(route_key, capacity_key, update)
}

pub(crate) fn clear_failure_backoff(
    store: &SqliteRouteStateStore,
    route_key: &str,
) -> RouteStateResult<Option<FailureBackoffState>> {
    store.clear_failure_backoff(route_key)
}

pub(crate) fn clear_account_size_refusal(
    store: &SqliteRouteStateStore,
    provider: &str,
    credential: &str,
    model: &str,
    size_class: u32,
    observed_at: i64,
) -> RouteStateResult<()> {
    store.clear_account_size_refusal(provider, credential, model, size_class, observed_at)
}

pub(crate) fn record_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
) -> RouteStateResult<()> {
    store.record_account_route_fence(fence)
}

pub(crate) fn record_transient_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
) -> RouteStateResult<()> {
    store.record_transient_account_route_fence(fence)
}

pub(crate) fn record_billing_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
) -> RouteStateResult<()> {
    store.record_billing_account_route_fence(fence)
}

pub(crate) fn reconcile_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
    until_ms: i64,
    last_reported_at: i64,
) -> RouteStateResult<()> {
    store.reconcile_account_route_fence(fence, until_ms, last_reported_at)
}

pub(crate) fn clear_all_account_route_fences_for_credential(
    store: &SqliteRouteStateStore,
    credential_id: &str,
) -> RouteStateResult<usize> {
    store.clear_all_account_route_fences_for_credential(credential_id)
}

pub(crate) fn pool_acquire_for_route(
    store: &SqliteRouteStateStore,
    request: &PoolAcquire<'_>,
    providers: &[&str],
    model: &str,
    recovered: &[(String, String)],
    accounts: &BTreeMap<String, String>,
) -> RouteStateResult<PoolAcquireOutcome> {
    store.pool_acquire_for_route(request, providers, model, recovered, accounts)
}

pub(crate) fn pool_renew(
    store: &SqliteRouteStateStore,
    provider: &str,
    session_id: &str,
    lease_id: &str,
    credential_id: Option<&str>,
    now_ms: i64,
    lease_ms: i64,
) -> RouteStateResult<Option<i64>> {
    store.pool_renew(
        provider,
        session_id,
        lease_id,
        credential_id,
        now_ms,
        lease_ms,
    )
}

pub(crate) fn pool_release(
    store: &SqliteRouteStateStore,
    provider: &str,
    session_id: &str,
    lease_id: Option<&str>,
    end_session: bool,
    now_ms: i64,
) -> RouteStateResult<usize> {
    store.pool_release(provider, session_id, lease_id, end_session, now_ms)
}

pub(crate) fn v1_record_selection(
    store: &SqliteRouteStateStore,
    decision: &V1Decision,
    selection_json: Option<&str>,
) -> RouteStateResult<()> {
    store.v1_record_selection(decision, selection_json)
}

pub(crate) fn bind_fence_probe_owner(
    store: &SqliteRouteStateStore,
    from: &str,
    to: &str,
) -> RouteStateResult<()> {
    store.bind_fence_probe_owner(from, to)
}

pub(crate) fn prune_account_admissions(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    store.prune_account_admissions(before, limit)
}

pub(crate) fn prune_account_affinities(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<AffinityRetentionBatch> {
    store.prune_account_affinities(before, limit)
}

pub(crate) fn prune_account_observations(
    store: &SqliteRouteStateStore,
    received_before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    store.prune_account_observations(received_before, limit)
}

pub(crate) fn prune_observation_events(
    store: &SqliteRouteStateStore,
    received_before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    store.prune_observation_events(received_before, limit)
}

pub(crate) fn prune_v1_decisions(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<V1DecisionRetentionBatch> {
    store.prune_v1_decisions(before, limit)
}

pub(crate) fn prune_v1_admissions(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    store.prune_v1_admissions(before, limit)
}

pub(crate) fn prune_v1_affinities(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    store.prune_v1_affinities(before, limit)
}

pub(crate) fn prune_account_continuations(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    store.prune_account_continuations(before, limit)
}

pub(crate) fn prune_retired_fence_origins(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    store.prune_retired_fence_origins(before, limit)
}
