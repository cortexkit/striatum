//! Owned account work submitted to the store's single group-commit engine.
//! Preparing a roster stays on the caller; commands only use transaction data
//! and the immutable inputs captured when the request was prepared.

use striatum_core::route_state_store::RouteStateResult;
use striatum_store::{AccountCommandKind, AccountTx, SqliteRouteStateStore};

pub(crate) fn execute<T: Send + 'static>(
    store: &SqliteRouteStateStore,
    kind: AccountCommandKind,
    command: impl FnOnce(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
) -> RouteStateResult<T> {
    #[cfg(test)]
    let inputs = crate::TestThreadState::capture();
    store.account_command(kind, move |tx| {
        #[cfg(test)]
        let _inputs = inputs.install();
        command(tx)
    })
}
