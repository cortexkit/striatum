//! Owned account work submitted to the store's single group-commit engine.
//! Preparing a roster stays on the caller; commands only use transaction data
//! and the immutable inputs captured when the request was prepared.

use crate::account_state::AccountCommand;
use striatum_core::route_state_store::RouteStateResult;
use striatum_store::{AccountCommandKind, AccountTx, SqliteRouteStateStore};

pub(crate) fn repair<T: Send + 'static>(
    store: &SqliteRouteStateStore,
    command: impl FnMut(&mut AccountCommand<'_>) -> RouteStateResult<T> + Send + 'static,
) -> RouteStateResult<T> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter("module_adapter::repair");
    execute(store, AccountCommandKind::Repair, command)
}

pub(crate) fn tick(store: &SqliteRouteStateStore, now_ms: i64) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter("module_adapter::tick");
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
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter("module_adapter::pool_wall");
    store.pool_wall(provider, credential, kind, until_ms, reason, now_ms)
}

pub(crate) fn pool_key_states(
    store: &SqliteRouteStateStore,
    provider: &str,
    now_ms: i64,
) -> RouteStateResult<BTreeMap<String, striatum_core::key_pool::PoolKeyState>> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::pool_key_states");
    let published = crate::account_state::published(store, now_ms)?;
    store.pool_key_states_from(provider, now_ms, published.block_state())
}

pub(crate) fn prune_unserved_unpicked_facts(
    store: &SqliteRouteStateStore,
) -> RouteStateResult<Vec<String>> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_unserved_unpicked_facts");
    store.prune_unserved_unpicked_facts()
}

/// `command` may run more than once: if a command in its batch finds the
/// in-memory state disagreeing with SQL, the writer rolls the batch back,
/// reloads the state and runs every command again. A command must therefore
/// compute only from its captured inputs, the transaction and the state.
pub(crate) fn execute<T: Send + 'static>(
    store: &SqliteRouteStateStore,
    kind: AccountCommandKind,
    command: impl FnMut(&mut AccountCommand<'_>) -> RouteStateResult<T> + Send + 'static,
) -> RouteStateResult<T> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter("module_adapter::execute");
    execute_with_error(store, kind, command)
}

pub(crate) fn execute_with_error<T: Send + 'static, E: striatum_store::AccountCommandError>(
    store: &SqliteRouteStateStore,
    kind: AccountCommandKind,
    mut command: impl FnMut(&mut AccountCommand<'_>) -> Result<T, E> + Send + 'static,
) -> Result<T, E> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::execute_with_error");
    if !store.is_private_account_copy() {
        crate::account_state::initialize(store)?;
    }
    #[cfg(test)]
    let inputs = crate::TestThreadState::capture();
    let mut command =
        move |tx: &AccountTx<'_>, state: Option<&mut (dyn AccountWorkingState + 'static)>| {
            #[cfg(test)]
            let _inputs = inputs.clone().install();
            command(&mut AccountCommand::new(tx, state))
        };
    if store.is_private_account_copy() {
        store.account_command_with_error(kind, move |tx| command(tx, None))
    } else {
        store
            .enqueue_account_state_command_with_error(kind, command)?
            .wait()
    }
}

use std::collections::{BTreeMap, HashSet};
use striatum_core::route_state_store::*;
use striatum_core::{FactSource, ModelFact, RouteStateStore};
use striatum_store::*;

pub(crate) fn clear_route_cooldowns(
    store: &SqliteRouteStateStore,
    requested: &str,
) -> RouteStateResult<Option<ClearedRouteCooldowns>> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::clear_route_cooldowns");
    store.clear_route_cooldowns(requested)
}
pub(crate) fn apply_fusiform_catalog_snapshot_for_providers(
    store: &SqliteRouteStateStore,
    facts: &[FusiformFactUpdate],
    cursor: FusiformCatalogCursor,
    mapped_providers: &HashSet<String>,
    retained: &HashSet<(String, String)>,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter(
        "module_adapter::apply_fusiform_catalog_snapshot_for_providers",
    );
    store.apply_fusiform_catalog_snapshot_for_providers(facts, cursor, mapped_providers, retained)
}

pub(crate) fn put_account_vault_roster(
    store: &SqliteRouteStateStore,
    roster: &PersistedVaultRoster,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::put_account_vault_roster");
    store.put_account_vault_roster(roster)
}

pub(crate) fn record_credential_entitlement_fence(
    store: &SqliteRouteStateStore,
    refusal: &striatum_core::route_state_store::CredentialRefusal,
    listing_fingerprint: Option<&str>,
    observed_at_ms: i64,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter(
        "module_adapter::record_credential_entitlement_fence",
    );
    store.record_credential_entitlement_fence(refusal, listing_fingerprint, observed_at_ms)
}

pub(crate) fn clear_credential_entitlement_fences_for_credential(
    store: &SqliteRouteStateStore,
    credential_id: &str,
) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter(
        "module_adapter::clear_credential_entitlement_fences_for_credential",
    );
    store.clear_credential_entitlement_fences_for_credential(credential_id)
}

pub(crate) fn upsert_fact<S: RouteStateStore>(
    store: &S,
    fact: &ModelFact,
    source: FactSource,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter("module_adapter::upsert_fact");
    store.upsert_fact(fact, source)
}

pub(crate) fn record_provider_cooldown(
    store: &SqliteRouteStateStore,
    provider_key: &str,
    route_key: &str,
    capacity_key: &str,
    update: &RouteStateUpdate,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::record_provider_cooldown");
    store.record_provider_cooldown(provider_key, route_key, capacity_key, update)
}

pub(crate) fn update_route_state(
    store: &SqliteRouteStateStore,
    route_key: &str,
    capacity_key: &str,
    update: &RouteStateUpdate,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::update_route_state");
    store.update_route_state(route_key, capacity_key, update)
}

pub(crate) fn clear_failure_backoff(
    store: &SqliteRouteStateStore,
    route_key: &str,
) -> RouteStateResult<Option<FailureBackoffState>> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::clear_failure_backoff");
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
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::clear_account_size_refusal");
    store.clear_account_size_refusal(provider, credential, model, size_class, observed_at)
}

pub(crate) fn record_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::record_account_route_fence");
    store.record_account_route_fence(fence)
}

pub(crate) fn record_transient_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter(
        "module_adapter::record_transient_account_route_fence",
    );
    store.record_transient_account_route_fence(fence)
}

pub(crate) fn record_billing_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter(
        "module_adapter::record_billing_account_route_fence",
    );
    store.record_billing_account_route_fence(fence)
}

pub(crate) fn reconcile_account_route_fence(
    store: &SqliteRouteStateStore,
    fence: &AccountRouteFence,
    until_ms: i64,
    last_reported_at: i64,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::reconcile_account_route_fence");
    store.reconcile_account_route_fence(fence, until_ms, last_reported_at)
}

pub(crate) fn clear_all_account_route_fences_for_credential(
    store: &SqliteRouteStateStore,
    credential_id: &str,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter(
        "module_adapter::clear_all_account_route_fences_for_credential",
    );
    store.clear_all_account_route_fences_for_credential(credential_id)
}

pub(crate) fn pool_acquire_for_route(
    store: &SqliteRouteStateStore,
    request: &PoolAcquire<'_>,
    providers: &[&str],
    model: &str,
    recovered: &[striatum_store::RecoveredOrigin],
    accounts: &BTreeMap<String, String>,
) -> RouteStateResult<PoolAcquireOutcome> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::pool_acquire_for_route");
    // A module command, so expired probes are released before the lease is
    // decided, as the readers' view already shows them.
    let provider = request.provider.to_owned();
    let session = request.session_id.to_owned();
    let keys = request.keys.to_vec();
    let (now_ms, lease_ms, affinity_idle_ms) =
        (request.now_ms, request.lease_ms, request.affinity_idle_ms);
    let providers: Vec<String> = providers.iter().map(|p| (*p).to_owned()).collect();
    let model = model.to_owned();
    let recovered = recovered.to_vec();
    let accounts = accounts.clone();
    execute(store, AccountCommandKind::Maintenance, move |tx| {
        tx.release_expired(now_ms)?;
        let providers: Vec<&str> = providers.iter().map(String::as_str).collect();
        tx.pool_acquire_for_route(
            &PoolAcquire {
                provider: &provider,
                session_id: &session,
                keys: &keys,
                now_ms,
                lease_ms,
                affinity_idle_ms,
            },
            &providers,
            &model,
            &recovered,
            &accounts,
        )
    })
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
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter("module_adapter::pool_renew");
    let (provider, session_id, lease_id) = (
        provider.to_owned(),
        session_id.to_owned(),
        lease_id.to_owned(),
    );
    let credential_id = credential_id.map(str::to_owned);
    execute(store, AccountCommandKind::Maintenance, move |tx| {
        tx.release_expired(now_ms)?;
        tx.pool_renew(
            &provider,
            &session_id,
            &lease_id,
            credential_id.as_deref(),
            now_ms,
            lease_ms,
        )
    })
}

pub(crate) fn pool_release(
    store: &SqliteRouteStateStore,
    provider: &str,
    session_id: &str,
    lease_id: Option<&str>,
    end_session: bool,
    now_ms: i64,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry = striatum_store::writer_entry_trace::enter("module_adapter::pool_release");
    store.pool_release(provider, session_id, lease_id, end_session, now_ms)
}

pub(crate) fn v1_record_selection(
    store: &SqliteRouteStateStore,
    decision: &V1Decision,
    selection_json: Option<&str>,
) -> RouteStateResult<()> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::v1_record_selection");
    store.v1_record_selection(decision, selection_json)
}

pub(crate) fn prune_account_admissions(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
    now_ms: i64,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_account_admissions");
    let keys = store.plan_account_admissions(before, limit, now_ms)?;
    if keys.is_empty() {
        return Ok(0);
    }
    // Planning stays off the writer; keyed rechecks and state updates are atomic.
    execute(store, AccountCommandKind::Maintenance, move |tx| {
        tx.delete_account_admissions(before, &keys, now_ms)
    })
}

pub(crate) fn prune_account_pins(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_account_pins");
    let keys = store.plan_account_pins(before, limit)?;
    if keys.is_empty() {
        return Ok(0);
    }
    execute(store, AccountCommandKind::HistoryMaintenance, move |tx| {
        tx.delete_account_pins(before, &keys)
    })
}

pub(crate) fn prune_account_affinities(
    store: &SqliteRouteStateStore,
    keys: Vec<AffinityRetentionKey>,
    before: i64,
    lease_before: i64,
) -> RouteStateResult<AffinityRetentionBatch> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_account_affinities");
    // Pruning is a writer command so the admission state drops the same rows.
    execute(store, AccountCommandKind::HistoryMaintenance, move |tx| {
        tx.prune_account_affinities(&keys, before, lease_before)
    })
}

pub(crate) fn prune_account_observations(
    store: &SqliteRouteStateStore,
    received_before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_account_observations");
    store.prune_account_observations(received_before, limit)
}

pub(crate) fn prune_observation_events(
    store: &SqliteRouteStateStore,
    received_before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_observation_events");
    store.prune_observation_events(received_before, limit)
}

pub(crate) fn prune_v1_decisions(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<V1DecisionRetentionBatch> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_v1_decisions");
    store.prune_v1_decisions(before, limit)
}

pub(crate) fn prune_v1_admissions(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_v1_admissions");
    store.prune_v1_admissions(before, limit)
}

pub(crate) fn prune_v1_affinities(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_v1_affinities");
    store.prune_v1_affinities(before, limit)
}

pub(crate) fn prune_account_continuations(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_account_continuations");
    // Pruning is a writer command so the admission state drops the same rows.
    execute(store, AccountCommandKind::Maintenance, move |tx| {
        tx.prune_account_continuations(before, limit)
    })
}

pub(crate) fn prune_retired_fence_origins(
    store: &SqliteRouteStateStore,
    before: i64,
    limit: usize,
) -> RouteStateResult<usize> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::prune_retired_fence_origins");
    store.prune_retired_fence_origins(before, limit)
}

/// Apply the shared repair decision on the live writer transaction.
pub(crate) fn apply_block_repair(
    tx: &AccountTx<'_>,
    repair: DryRunRepair,
    now: i64,
) -> RouteStateResult<bool> {
    #[cfg(test)]
    let _writer_entry =
        striatum_store::writer_entry_trace::enter("module_adapter::apply_block_repair");
    match repair {
        DryRunRepair::ClearFence(id, reason) => {
            tx.clear_fence(&id, now, &reason)?;
        }
        DryRunRepair::ClearHeld(row, reason) => {
            tx.clear_held_fence(&row, now, &reason)?;
        }
        DryRunRepair::ClearCredential(provider, credential) => {
            tx.clear_credential_entitlement_fence(&provider, &credential)?;
        }
        DryRunRepair::Classify(provider, credential, quota) => {
            return tx.classify_credential_quota(&provider, &credential, quota)
        }
        DryRunRepair::InsertFence(fence) => {
            tx.insert_pool_fence(&fence)?;
        }
    }
    Ok(false)
}
