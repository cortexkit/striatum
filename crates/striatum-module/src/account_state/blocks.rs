//! Request-path reads of the block tables.
//!
//! A writer command answers them from its working state, which holds every
//! earlier change of its batch; the rows the command itself has written so
//! far are read back by rowid and laid over a copy first, so a command reads
//! its own writes. A request outside the writer answers them from the
//! published state and sends a command only when the answer shows work to do.
//! A private SQL projection holds no state, and a store opened without the
//! block schema holds none either: both read SQL.
//!
//! [`BlockReads`] lets one helper serve the writer, the published state and
//! the SQL oracle. `AccountCommand` and [`Published`] also carry each read as
//! an inherent method, so a call on them reaches the state even where the
//! trait is not imported, rather than falling through `Deref` to the
//! `AccountTx` method of the same name, which reads SQL.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use striatum_core::model_router::ModelRouteStateSnapshot;
use striatum_core::route_state_store::{
    CredentialEntitlementFence, RouteStateResult, RouteStateStoreError,
};
use striatum_core::ProviderCooldownState;
use striatum_store::{
    AccountEligibilityFact, AccountExclusionRow, AccountFenceRow, AccountPoolFenceRow,
    AccountRouteFence, AccountSizeRefusal, AccountTx, BlockState, FenceRecheck,
    SqliteRouteStateStore,
};

use super::{AccountCommand, AccountState};

/// The time `SqliteRouteStateStore::fence_rechecks` evaluates expiry at: the
/// store's clock when the read runs (the wall clock, or a replayed request's
/// recorded time in test builds), not the request's clock.
fn wall_now() -> i64 {
    striatum_store::store_now_ms()
}

/// The block reads a helper may make, answered by whoever holds the rows.
pub(crate) trait BlockReads {
    fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>>;
    fn active_fences(&self, provider: &str) -> RouteStateResult<Vec<AccountFenceRow>>;
    fn active_pool_fences(&self, provider: &str) -> RouteStateResult<Vec<AccountPoolFenceRow>>;
    fn route_fence_latest_reports(&self) -> RouteStateResult<BTreeMap<String, i64>>;
    /// The quota route fences.
    fn report_route_fences(&self, now: i64) -> RouteStateResult<Vec<AccountRouteFence>>;
    fn credential_quota_classification(
        &self,
        provider: &str,
        credential: &str,
        proposed: bool,
    ) -> RouteStateResult<(bool, bool)>;
    fn admission_exclusions(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Vec<AccountExclusionRow>>;
    /// The writer transaction, for a helper that writes what it decided.
    /// `None` outside a command.
    fn writer(&self) -> Option<&AccountTx<'_>>;
}

/// The writer transaction of a helper that only writes inside a command.
pub(crate) fn writer<R: BlockReads + ?Sized>(reads: &R) -> RouteStateResult<&AccountTx<'_>> {
    reads
        .writer()
        .ok_or_else(|| RouteStateStoreError("block write outside an account command".into()))
}

/// Implements [`BlockReads`] by calling the type's method of the same name.
macro_rules! block_reads_by {
    ($type:ty, |$this:ident| $writer:expr) => {
        impl BlockReads for $type {
            fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
                <$type>::fence_rechecks(self)
            }
            fn active_fences(&self, provider: &str) -> RouteStateResult<Vec<AccountFenceRow>> {
                <$type>::active_fences(self, provider)
            }
            fn active_pool_fences(
                &self,
                provider: &str,
            ) -> RouteStateResult<Vec<AccountPoolFenceRow>> {
                <$type>::active_pool_fences(self, provider)
            }
            fn route_fence_latest_reports(&self) -> RouteStateResult<BTreeMap<String, i64>> {
                <$type>::route_fence_latest_reports(self)
            }
            fn report_route_fences(&self, now: i64) -> RouteStateResult<Vec<AccountRouteFence>> {
                <$type>::report_route_fences(self, now)
            }
            fn credential_quota_classification(
                &self,
                provider: &str,
                credential: &str,
                proposed: bool,
            ) -> RouteStateResult<(bool, bool)> {
                <$type>::credential_quota_classification(self, provider, credential, proposed)
            }
            fn admission_exclusions(
                &self,
                principal: &str,
                admission_id: &str,
            ) -> RouteStateResult<Vec<AccountExclusionRow>> {
                <$type>::admission_exclusions(self, principal, admission_id)
            }
            fn writer(&self) -> Option<&AccountTx<'_>> {
                let $this = self;
                $writer
            }
        }
    };
}

// `AccountTx` reads SQL, the store's own readers; the other two read state.
block_reads_by!(AccountTx<'_>, |tx| Some(tx));
block_reads_by!(AccountCommand<'_>, |command| Some(command.sql));
block_reads_by!(Published<'_>, |published| {
    let _ = published;
    None
});

/// Answer from the block state when there is one, else from SQL.
macro_rules! read {
    ($blocks:expr, $sql:expr, |$state:ident| $memory:expr) => {
        match $blocks {
            Some($state) => Ok($memory),
            None => $sql,
        }
    };
}

impl AccountCommand<'_> {
    /// The working state's block tables with this command's own writes so
    /// far laid over a copy, or `None` to read SQL.
    fn working_blocks(&self) -> RouteStateResult<Option<Cow<'_, BlockState>>> {
        // The writer gives the command's transaction the same block tables,
        // with the command's own writes kept laid over them between reads.
        if let Some(blocks) = self.sql.working_blocks()? {
            return Ok(Some(blocks));
        }
        let Some(state) = self.state.as_deref() else {
            return Ok(None);
        };
        if !state.blocks.enabled() {
            return Ok(None);
        }
        let pending = self.sql.pending_block_rows()?;
        if pending.is_empty() {
            return Ok(Some(Cow::Borrowed(&state.blocks)));
        }
        let mut blocks = state.blocks.clone();
        blocks.apply(&pending);
        Ok(Some(Cow::Owned(blocks)))
    }

    pub(crate) fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        match self.working_blocks()? {
            Some(blocks) => blocks.fence_rechecks(wall_now()),
            None => self.sql.fence_rechecks(),
        }
    }

    pub(crate) fn active_fences(&self, provider: &str) -> RouteStateResult<Vec<AccountFenceRow>> {
        read!(
            self.working_blocks()?,
            self.sql.active_fences(provider),
            |blocks| blocks.active_fences(provider)
        )
    }

    pub(crate) fn active_pool_fences(
        &self,
        provider: &str,
    ) -> RouteStateResult<Vec<AccountPoolFenceRow>> {
        match self.working_blocks()? {
            Some(blocks) => blocks.active_pool_fences(provider),
            None => self.sql.active_pool_fences(provider),
        }
    }

    pub(crate) fn active_fence_providers(&self) -> RouteStateResult<Vec<String>> {
        read!(
            self.working_blocks()?,
            self.sql.active_fence_providers(),
            |blocks| blocks.active_fence_providers()
        )
    }

    pub(crate) fn route_fence_latest_reports(&self) -> RouteStateResult<BTreeMap<String, i64>> {
        read!(
            self.working_blocks()?,
            self.sql.route_fence_latest_reports(),
            |blocks| blocks.route_fence_latest_reports()
        )
    }

    pub(crate) fn report_route_fences(&self, now: i64) -> RouteStateResult<Vec<AccountRouteFence>> {
        read!(
            self.working_blocks()?,
            self.sql.report_route_fences(now),
            |blocks| blocks.route_fences("quota")
        )
    }

    pub(crate) fn report_credential_refusals(
        &self,
    ) -> RouteStateResult<Vec<CredentialEntitlementFence>> {
        read!(
            self.working_blocks()?,
            self.sql.report_credential_refusals(),
            |blocks| blocks.entitlement_fences()
        )
    }

    pub(crate) fn credential_entitlement_fence(
        &self,
        provider: &str,
        credential: &str,
    ) -> RouteStateResult<Option<CredentialEntitlementFence>> {
        read!(
            self.working_blocks()?,
            self.sql.credential_entitlement_fence(provider, credential),
            |blocks| blocks.entitlement_fence(provider, credential)
        )
    }

    pub(crate) fn credential_quota_classification(
        &self,
        provider: &str,
        credential: &str,
        proposed: bool,
    ) -> RouteStateResult<(bool, bool)> {
        read!(
            self.working_blocks()?,
            self.sql
                .credential_quota_classification(provider, credential, proposed),
            |blocks| blocks.credential_quota_classification(provider, credential, proposed)
        )
    }

    pub(crate) fn admission_exclusions(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Vec<AccountExclusionRow>> {
        read!(
            self.working_blocks()?,
            self.sql.admission_exclusions(principal, admission_id),
            |blocks| blocks.admission_exclusions(principal, admission_id)
        )
    }

    pub(crate) fn exclusion_count(
        &self,
        principal: &str,
        credential_id: &str,
    ) -> RouteStateResult<i64> {
        read!(
            self.working_blocks()?,
            self.sql.exclusion_count(principal, credential_id),
            |blocks| blocks.exclusion_count(principal, credential_id)
        )
    }

    /// The owners of every probe authorization.
    pub(crate) fn probe_authorization_owners(&self) -> RouteStateResult<Vec<String>> {
        read!(
            self.working_blocks()?,
            self.sql.probe_authorization_owners(),
            |blocks| blocks.authorization_owners()
        )
    }
}

/// The block tables as a request outside the writer sees them: the
/// published state, shared with every other reader, or SQL where there is no
/// state.
pub(crate) enum Published<'s> {
    State(Arc<AccountState>),
    Sql(&'s SqliteRouteStateStore),
}

/// The published block tables of `store`.
pub(crate) fn published(store: &SqliteRouteStateStore) -> RouteStateResult<Published<'_>> {
    if store.is_account_evaluation_projection() {
        return Ok(Published::Sql(store));
    }
    // Expired probes and offers read as released: the next command
    // releases them before it decides (`AccountCommand::release_expired`).
    let state = super::published_at(store, crate::account_selection::now_ms())?;
    Ok(if state.blocks.enabled() {
        Published::State(state)
    } else {
        Published::Sql(store)
    })
}

impl Published<'_> {
    /// The published block state, or `None` where SQL answers.
    pub(crate) fn block_state(&self) -> Option<&BlockState> {
        self.blocks()
    }

    fn blocks(&self) -> Option<&BlockState> {
        match self {
            Self::State(state) => Some(&state.blocks),
            Self::Sql(_) => None,
        }
    }

    fn sql<T>(
        &self,
        read: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T>,
    ) -> RouteStateResult<T> {
        match self {
            Self::Sql(store) => store.account_read(read),
            Self::State(_) => unreachable!("the state answers"),
        }
    }

    fn store(&self) -> &SqliteRouteStateStore {
        match self {
            Self::Sql(store) => store,
            Self::State(_) => unreachable!("the state answers"),
        }
    }

    pub(crate) fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        match self.blocks() {
            Some(blocks) => blocks.fence_rechecks(wall_now()),
            None => self.store().fence_rechecks(),
        }
    }

    /// The route-state snapshot at `now`. Its probe clocks are evaluated at
    /// the wall clock, as the store's SQL `build_snapshot` evaluates them.
    pub(crate) fn route_state_snapshot(
        &self,
        now: f64,
    ) -> RouteStateResult<ModelRouteStateSnapshot> {
        match self.blocks() {
            Some(blocks) => blocks.route_state_snapshot(now, wall_now()),
            None => striatum_core::RouteStateStore::build_snapshot(self.store(), now),
        }
    }

    /// One provider's cooldown row, for `route.cooldown_status`.
    pub(crate) fn provider_cooldown(
        &self,
        provider_key: &str,
    ) -> RouteStateResult<Option<ProviderCooldownState>> {
        match self.blocks() {
            Some(blocks) => blocks.provider_cooldown(provider_key),
            None => striatum_core::RouteStateStore::get_provider_cooldown(self.store(), provider_key),
        }
    }

    pub(crate) fn active_fences(&self, provider: &str) -> RouteStateResult<Vec<AccountFenceRow>> {
        read!(
            self.blocks(),
            self.sql(|tx| tx.active_fences(provider)),
            |blocks| blocks.active_fences(provider)
        )
    }

    pub(crate) fn active_pool_fences(
        &self,
        provider: &str,
    ) -> RouteStateResult<Vec<AccountPoolFenceRow>> {
        match self.blocks() {
            Some(blocks) => blocks.active_pool_fences(provider),
            None => self.sql(|tx| tx.active_pool_fences(provider)),
        }
    }

    pub(crate) fn route_fence_latest_reports(&self) -> RouteStateResult<BTreeMap<String, i64>> {
        read!(
            self.blocks(),
            self.store().account_route_fence_latest_reports(),
            |blocks| blocks.route_fence_latest_reports()
        )
    }

    pub(crate) fn report_route_fences(&self, now: i64) -> RouteStateResult<Vec<AccountRouteFence>> {
        read!(
            self.blocks(),
            self.store().account_route_fences(now),
            |blocks| blocks.route_fences("quota")
        )
    }

    pub(crate) fn report_credential_refusals(
        &self,
    ) -> RouteStateResult<Vec<CredentialEntitlementFence>> {
        read!(
            self.blocks(),
            self.store().credential_entitlement_fences(),
            |blocks| blocks.entitlement_fences()
        )
    }

    pub(crate) fn credential_quota_classification(
        &self,
        provider: &str,
        credential: &str,
        proposed: bool,
    ) -> RouteStateResult<(bool, bool)> {
        read!(
            self.blocks(),
            self.sql(|tx| tx.credential_quota_classification(provider, credential, proposed)),
            |blocks| blocks.credential_quota_classification(provider, credential, proposed)
        )
    }

    pub(crate) fn admission_exclusions(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Vec<AccountExclusionRow>> {
        read!(
            self.blocks(),
            self.sql(|tx| tx.admission_exclusions(principal, admission_id)),
            |blocks| blocks.admission_exclusions(principal, admission_id)
        )
    }

    pub(crate) fn account_eligibility(&self) -> RouteStateResult<Vec<AccountEligibilityFact>> {
        read!(
            self.blocks(),
            self.store().account_eligibility(),
            |blocks| blocks.eligibility()
        )
    }

    pub(crate) fn account_size_refusals(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<AccountSizeRefusal>> {
        read!(
            self.blocks(),
            self.store().account_size_refusals(now),
            |blocks| blocks.size_refusals()
        )
    }

    pub(crate) fn account_route_fences(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<AccountRouteFence>> {
        self.report_route_fences(now)
    }

    pub(crate) fn transient_account_route_fences(
        &self,
        now: i64,
    ) -> RouteStateResult<Vec<AccountRouteFence>> {
        read!(
            self.blocks(),
            self.store().transient_account_route_fences(now),
            |blocks| blocks.route_fences("transient")
        )
    }
}
